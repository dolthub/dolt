// Copyright 2022 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nbs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/dolthub/fslock"
	"github.com/sirupsen/logrus"

	"github.com/dolthub/dolt/go/libraries/doltcore/dconfig"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

const (
	chunkJournalName = chunkJournalAddr // todo
)

// reflogDisabled indicates whether access to the reflog has been disabled and if so, no chunk journal root references
// should be kept in memory. This is controlled by the DOLT_DISABLE_REFLOG env var and this var is ONLY written to
// during initialization. All access after initialization is read-only, so no additional locking is needed.
var reflogDisabled = false

// defaultReflogBufferSize controls how many of the most recent root references for root updates are kept in-memory.
// This default can be overridden by setting the DOLT_REFLOG_RECORD_LIMIT before Dolt starts.
const defaultReflogBufferSize = 5_000

func init() {
	if os.Getenv(dconfig.EnvDisableReflog) != "" {
		reflogDisabled = true
	}
}

// ChunkJournal is a persistence abstraction for a NomsBlockStore.
// It implements both manifest and tablePersister, durably writing
// both memTable persists and manifest updates to a single file.
type ChunkJournal struct {
	wr   *journalWriter
	path string

	contents  manifestContents
	backing   *journalManifest
	persister *fsTablePersister

	// reflogRingBuffer holds the most recent roots written to the chunk journal so that they can be
	// quickly loaded for reflog queries without having to re-read the journal file from disk.
	reflogRingBuffer *reflogRingBuffer
}

var _ tablePersister = &ChunkJournal{}
var _ tableFilePersister = &ChunkJournal{}
var _ manifest = &ChunkJournal{}
var _ manifestGCGenUpdater = &ChunkJournal{}
var _ io.Closer = &ChunkJournal{}

func newChunkJournal(ctx context.Context, nbfVers, dir string, m *journalManifest, p *fsTablePersister) (*ChunkJournal, error) {
	path, err := filepath.Abs(filepath.Join(dir, chunkJournalName))
	if err != nil {
		return nil, err
	}

	j := &ChunkJournal{path: path, backing: m, persister: p}
	j.contents.nbfVers = nbfVers
	j.reflogRingBuffer = newReflogRingBuffer(reflogBufferSize())

	ok, err := fileExists(path)
	if err != nil {
		return nil, err
	} else if ok {
		// only bootstrap journalWriter if the journal file exists,
		// otherwise we wait to open in case we're cloning
		if err = j.bootstrapJournalWriter(ctx); err != nil {
			return nil, err
		}
	}
	return j, nil
}

// reflogBufferSize returns the size of the ring buffer to allocate to store in-memory roots references when
// new roots are written to a chunk journal. If reflog queries have been disabled, this function will return 0.
// If the default buffer size has been overridden via DOLT_REFLOG_RECORD_LIMIT, that value will be returned if
// it can be successfully parsed. Otherwise, the default buffer size will be returned.
func reflogBufferSize() int {
	if reflogDisabled {
		return 0
	}

	reflogBufferSize := defaultReflogBufferSize
	if limit := os.Getenv(dconfig.EnvReflogRecordLimit); limit != "" {
		i, err := strconv.Atoi(limit)
		if err != nil {
			logrus.Warnf("unable to parse integer value for %s from %s: %s",
				dconfig.EnvReflogRecordLimit, limit, err.Error())
		} else {
			if i <= 0 {
				reflogDisabled = true
			} else {
				reflogBufferSize = i
			}
		}
	}

	return reflogBufferSize
}

// bootstrapJournalWriter initializes the journalWriter, which manages access to the
// journal file for this ChunkJournal. The bootstrapping process differs depending
// on whether a journal file exists at startup time.
//
// If a journal file does not exist, we create one and commit a root hash record
// containing the root hash we read from the manifest file.
//
// If a journal file does exist, we process its records to build up an index of its
// resident chunks. Processing journal records is potentially accelerated by an index
// file (see indexRec). The journal file is the source of truth for latest root hash.
// As we process journal records, we keep track of the latest root hash record we see
// and update the manifest file with the last root hash we saw.
func (j *ChunkJournal) bootstrapJournalWriter(ctx context.Context) (err error) {
	var ok bool
	ok, err = fileExists(j.path)
	if err != nil {
		return err
	}

	if !ok { // create new journal file
		j.wr, err = createJournalWriter(ctx, j.path)
		if err != nil {
			return err
		}

		_, err = j.wr.bootstrapJournal(ctx, j.reflogRingBuffer)
		if err != nil {
			return err
		}

		var contents manifestContents
		ok, contents, err = j.backing.ParseIfExists(ctx, &Stats{}, nil)
		if err != nil {
			return err
		}
		if ok {
			// write the current root hash to the journal file
			if err = j.wr.commitRootHash(ctx, contents.root); err != nil {
				return
			}
			j.contents = contents
		}
		return
	}

	j.wr, ok, err = openJournalWriter(ctx, j.path)
	if err != nil {
		return err
	} else if !ok {
		return errors.New("missing chunk journal " + j.path)
	}

	// parse existing journal file
	root, err := j.wr.bootstrapJournal(ctx, j.reflogRingBuffer)
	if err != nil {
		return err
	}

	mc, err := trueUpBackingManifest(ctx, root, j.backing)
	if err != nil {
		return err
	}
	j.contents = mc
	return
}

// the journal file is the source of truth for the root hash, true-up persisted manifest
func trueUpBackingManifest(ctx context.Context, root hash.Hash, backing *journalManifest) (manifestContents, error) {
	ok, mc, err := backing.ParseIfExists(ctx, &Stats{}, nil)
	if err != nil {
		return manifestContents{}, err
	} else if !ok {
		return manifestContents{}, fmt.Errorf("manifest not found when opening chunk journal")
	}

	// set our in-memory root to match the journal
	mc.root = root
	if backing.readOnly() {
		return mc, nil
	}

	prev := mc.lock
	next := generateLockHash(mc.root, mc.specs, mc.appendix, nil)
	mc.lock = next

	mc, err = backing.Update(ctx, prev, mc, &Stats{}, nil)
	if err != nil {
		return manifestContents{}, err
	} else if mc.lock != next {
		return manifestContents{}, errOptimisticLockFailedTables
	} else if mc.root != root {
		return manifestContents{}, errOptimisticLockFailedRoot
	}
	// true-up succeeded
	return mc, nil
}

// IterateRoots iterates over the in-memory roots tracked by the ChunkJournal, from oldest root to newest root,
// and passes the root and associated timestamp to a callback function, |f|. If |f| returns an error, iteration
// is stopped and the error is returned.
func (j *ChunkJournal) IterateRoots(f func(root string, timestamp *time.Time) error) error {
	return j.reflogRingBuffer.Iterate(func(entry reflogRootHashEntry) error {
		// If we're reading a chunk journal written with an older version of Dolt, the root hash journal record may
		// not have a timestamp value, so we'll have a time.Time instance in its zero value. If we see this, pass
		// nil instead to signal to callers that there is no valid timestamp available.
		var pTimestamp *time.Time = nil
		if time.Time.IsZero(entry.timestamp) == false {
			pTimestamp = &entry.timestamp
		}

		return f(entry.root, pTimestamp)
	})
}

// Persist implements tablePersister.
func (j *ChunkJournal) Persist(ctx context.Context, mt *memTable, haver chunkReader, keeper keeperF, stats *Stats) (chunkSource, gcBehavior, error) {
	if j.backing.readOnly() {
		return nil, gcBehavior_Continue, errReadOnlyManifest
	} else if err := j.maybeInit(ctx); err != nil {
		return nil, gcBehavior_Continue, err
	}

	if haver != nil {
		sort.Sort(hasRecordByPrefix(mt.order)) // hasMany() requires addresses to be sorted.
		if _, gcb, err := haver.hasMany(mt.order, keeper); err != nil {
			return nil, gcBehavior_Continue, err
		} else if gcb != gcBehavior_Continue {
			return nil, gcb, nil
		}
		sort.Sort(hasRecordByOrder(mt.order)) // restore "insertion" order for write
	}

	for _, record := range mt.order {
		if record.has {
			continue
		}
		c := chunks.NewChunkWithHash(hash.Hash(*record.a), mt.chunks[*record.a])
		err := j.wr.writeCompressedChunk(ctx, ChunkToCompressedChunk(c))
		if err != nil {
			return nil, gcBehavior_Continue, err
		}
	}
	return journalChunkSource{journal: j.wr}, gcBehavior_Continue, nil
}

// ConjoinAll implements tablePersister.
func (j *ChunkJournal) ConjoinAll(ctx context.Context, sources chunkSources, stats *Stats) (chunkSource, cleanupFunc, error) {
	if j.backing.readOnly() {
		return nil, nil, errReadOnlyManifest
	}
	return j.persister.ConjoinAll(ctx, sources, stats)
}

// Open implements tablePersister.
func (j *ChunkJournal) Open(ctx context.Context, name hash.Hash, chunkCount uint32, stats *Stats) (chunkSource, error) {
	if name == journalAddr {
		if err := j.maybeInit(ctx); err != nil {
			return nil, err
		}
		return journalChunkSource{journal: j.wr}, nil
	}
	return j.persister.Open(ctx, name, chunkCount, stats)
}

// Exists implements tablePersister.
func (j *ChunkJournal) Exists(ctx context.Context, name hash.Hash, chunkCount uint32, stats *Stats) (bool, error) {
	return j.persister.Exists(ctx, name, chunkCount, stats)
}

// PruneTableFiles implements tablePersister.
func (j *ChunkJournal) PruneTableFiles(ctx context.Context, keeper func() []hash.Hash, mtime time.Time) error {
	if j.backing.readOnly() {
		return errReadOnlyManifest
	}
	// sanity check that we're not deleting the journal
	var keepJournal bool
	for _, a := range keeper() {
		if a == journalAddr {
			keepJournal = true
		}
	}
	if j.wr != nil && !keepJournal {
		return errors.New("cannot drop chunk journal through tablePersister.PruneTableFiles()")
	}
	return j.persister.PruneTableFiles(ctx, keeper, mtime)
}

func (j *ChunkJournal) Path() string {
	return filepath.Dir(j.path)
}

func (j *ChunkJournal) CopyTableFile(ctx context.Context, r io.Reader, fileId string, fileSz uint64, chunkCount uint32) error {
	if j.backing.readOnly() {
		return errReadOnlyManifest
	}
	return j.persister.CopyTableFile(ctx, r, fileId, fileSz, chunkCount)
}

// Name implements manifest.
func (j *ChunkJournal) Name() string {
	return j.path
}

// Update implements manifest.
func (j *ChunkJournal) Update(ctx context.Context, lastLock hash.Hash, next manifestContents, stats *Stats, writeHook func() error) (manifestContents, error) {
	if j.backing.readOnly() {
		return j.contents, errReadOnlyManifest
	}

	if j.wr == nil {
		// pass the update to |j.backing| if the journal is not initialized
		return j.backing.Update(ctx, lastLock, next, stats, writeHook)
	}

	if j.contents.gcGen != next.gcGen {
		return manifestContents{}, errors.New("use UpdateGCGen to update GC generation")
	} else if j.contents.lock != lastLock {
		return j.contents, nil // |next| is stale
	}

	if writeHook != nil {
		if err := writeHook(); err != nil {
			return manifestContents{}, err
		}
	}

	// if |next| has a different table file set, flush to |j.backing|
	if !equalSpecs(j.contents.specs, next.specs) {
		if err := j.flushToBackingManifest(ctx, next, stats); err != nil {
			return manifestContents{}, err
		}
	}

	if err := j.wr.commitRootHash(ctx, next.root); err != nil {
		return manifestContents{}, err
	}
	j.contents = next

	// Update the in-memory structures so that the ChunkJournal can be queried for reflog data
	if !reflogDisabled {
		j.reflogRingBuffer.Push(reflogRootHashEntry{
			root:      next.root.String(),
			timestamp: time.Now(),
		})
	}

	return j.contents, nil
}

// UpdateGCGen implements manifestGCGenUpdater.
func (j *ChunkJournal) UpdateGCGen(ctx context.Context, lastLock hash.Hash, next manifestContents, stats *Stats, writeHook func() error) (manifestContents, error) {
	if j.backing.readOnly() {
		return j.contents, errReadOnlyManifest
	} else if j.wr == nil {
		// pass the update to |j.backing| if the journal is not initialized
		return j.backing.UpdateGCGen(ctx, lastLock, next, stats, writeHook)
	} else if j.contents.lock != lastLock {
		return j.contents, nil // |next| is stale
	}

	// UpdateGCGen below cannot update the root hash, only the GC generation
	// flush |j.contents| with the latest root hash here
	if err := j.flushToBackingManifest(ctx, j.contents, stats); err != nil {
		return manifestContents{}, err
	}

	latest, err := j.backing.UpdateGCGen(ctx, j.contents.lock, next, stats, writeHook)
	if err != nil {
		return manifestContents{}, err
	} else if latest.root == next.root {
		j.contents = next // success
	}

	// if we're landing a new manifest without the chunk journal
	// then physically delete the journal here and cleanup |j.wr|
	if !containsJournalSpec(latest.specs) {
		if err = j.dropJournalWriter(ctx); err != nil {
			return manifestContents{}, err
		}
	}

	// Truncate the in-memory root and root timestamp metadata
	if !reflogDisabled {
		j.reflogRingBuffer.Truncate()
	}

	return latest, nil
}

// flushToBackingManifest attempts to update the backing file manifest with |next|. This is necessary
// when making manifest updates other than root hash updates (adding new table files, updating GC gen, etc).
func (j *ChunkJournal) flushToBackingManifest(ctx context.Context, next manifestContents, stats *Stats) error {
	_, prev, err := j.backing.ParseIfExists(ctx, stats, nil)
	if err != nil {
		return err
	}
	var mc manifestContents
	mc, err = j.backing.Update(ctx, prev.lock, next, stats, nil)
	if err != nil {
		return err
	} else if mc.lock != next.lock {
		return errOptimisticLockFailedTables
	}
	return nil
}

func (j *ChunkJournal) dropJournalWriter(ctx context.Context) error {
	curr := j.wr
	if j.wr == nil {
		return nil
	}
	j.wr = nil
	if err := curr.Close(); err != nil {
		return err
	}
	return deleteJournalAndIndexFiles(ctx, curr.path)
}

// ParseIfExists implements manifest.
func (j *ChunkJournal) ParseIfExists(ctx context.Context, stats *Stats, readHook func() error) (ok bool, mc manifestContents, err error) {
	if j.wr == nil {
		// parse contents from |j.backing| if the journal is not initialized
		return j.backing.ParseIfExists(ctx, stats, readHook)
	}
	if readHook != nil {
		if err = readHook(); err != nil {
			return false, manifestContents{}, err
		}
	}
	ok, mc = true, j.contents
	return
}

func (j *ChunkJournal) maybeInit(ctx context.Context) (err error) {
	if j.wr == nil {
		err = j.bootstrapJournalWriter(ctx)
	}
	return
}

// Close implements io.Closer
func (j *ChunkJournal) Close() (err error) {
	if j.wr != nil {
		err = j.wr.Close()
		// flush the latest root to the backing manifest
		if !j.backing.readOnly() {
			cerr := j.flushToBackingManifest(context.Background(), j.contents, &Stats{})
			if err == nil {
				err = cerr
			}
		}
	}
	// close the journal manifest to release the file lock
	if cerr := j.backing.Close(); err == nil {
		err = cerr // keep first error
	}

	return err
}

func (j *ChunkJournal) AccessMode() chunks.ExclusiveAccessMode {
	if j.backing.readOnly() {
		return chunks.ExclusiveAccessMode_ReadOnly
	}
	return chunks.ExclusiveAccessMode_Exclusive
}

type journalConjoiner struct {
	child conjoinStrategy
}

func (c journalConjoiner) conjoinRequired(ts tableSet) bool {
	return c.child.conjoinRequired(ts)
}

func (c journalConjoiner) chooseConjoinees(upstream []tableSpec) (conjoinees, keepers []tableSpec, err error) {
	var stash tableSpec // don't conjoin journal
	pruned := make([]tableSpec, 0, len(upstream))
	for _, ts := range upstream {
		if isJournalAddr(ts.name) {
			stash = ts
		} else {
			pruned = append(pruned, ts)
		}
	}
	conjoinees, keepers, err = c.child.chooseConjoinees(pruned)
	if err != nil {
		return nil, nil, err
	}
	if !hash.Hash(stash.name).IsEmpty() {
		keepers = append(keepers, stash)
	}
	return
}

func AcquireManifestLock(dir string) (lock *fslock.Lock, err error) {
	lock = fslock.New(filepath.Join(dir, lockFileName))
	// try to take the file lock. if we fail, make the manifest read-only.
	// if we succeed, hold the file lock until we close the journalManifest
	err = lock.LockWithTimeout(lockFileTimeout)
	if errors.Is(err, fslock.ErrTimeout) {
		lock, err = nil, nil // read only
	}
	if err != nil {
		return nil, err
	}
	return lock, nil
}

// newJournalManifest makes a new file manifest.
func newJournalManifest(ctx context.Context, lock *fslock.Lock, dir string) (m *journalManifest, err error) {
	m = &journalManifest{dir: dir, lock: lock}

	var f *os.File
	f, err = openIfExists(filepath.Join(dir, manifestFileName))
	if err != nil {
		if lock != nil {
			_ = lock.Unlock()
		}
		return nil, err
	} else if f == nil {
		return m, nil
	}
	defer func() {
		if cerr := f.Close(); err == nil {
			err = cerr // keep first error
		}
		if err != nil {
			if lock != nil {
				_ = lock.Unlock()
			}
		}
	}()

	var ok bool
	ok, _, err = m.ParseIfExists(ctx, &Stats{}, nil)
	if err != nil {
		if lock != nil {
			_ = lock.Unlock()
		}
		return nil, err
	} else if !ok {
		if lock != nil {
			_ = lock.Unlock()
		}
		return nil, ErrUnreadableManifest
	}
	return
}

type journalManifest struct {
	dir  string
	lock *fslock.Lock
}

func (jm *journalManifest) readOnly() bool {
	return jm.lock == nil
}

// Name implements manifest.
func (jm *journalManifest) Name() string {
	return jm.dir
}

// ParseIfExists implements manifest.
func (jm *journalManifest) ParseIfExists(ctx context.Context, stats *Stats, readHook func() error) (exists bool, contents manifestContents, err error) {
	t1 := time.Now()
	defer func() { stats.ReadManifestLatency.SampleTimeSince(t1) }()
	return parseIfExists(ctx, jm.dir, readHook)
}

// Update implements manifest.
func (jm *journalManifest) Update(ctx context.Context, lastLock hash.Hash, newContents manifestContents, stats *Stats, writeHook func() error) (mc manifestContents, err error) {
	if jm.readOnly() {
		_, mc, err = jm.ParseIfExists(ctx, stats, nil)
		if err != nil {
			return manifestContents{}, err
		}
		// return current contents and sentinel error
		return mc, errReadOnlyManifest
	}

	t1 := time.Now()
	defer func() { stats.WriteManifestLatency.SampleTimeSince(t1) }()
	checker := func(upstream, contents manifestContents) error {
		if contents.gcGen != upstream.gcGen {
			return chunks.ErrGCGenerationExpired
		}
		return nil
	}
	return updateWithChecker(ctx, jm.dir, syncFlush, checker, lastLock, newContents, writeHook)
}

// UpdateGCGen implements manifest.
func (jm *journalManifest) UpdateGCGen(ctx context.Context, lastLock hash.Hash, newContents manifestContents, stats *Stats, writeHook func() error) (mc manifestContents, err error) {
	if jm.readOnly() {
		_, mc, err = jm.ParseIfExists(ctx, stats, nil)
		if err != nil {
			return manifestContents{}, err
		}
		// return current contents and sentinel error
		return mc, errReadOnlyManifest
	}

	t1 := time.Now()
	defer func() { stats.WriteManifestLatency.SampleTimeSince(t1) }()
	checker := func(upstream, contents manifestContents) error {
		if contents.gcGen == upstream.gcGen {
			return errors.New("UpdateGCGen() must update the garbage collection generation")
		} else if contents.root != upstream.root {
			return errors.New("UpdateGCGen() cannot update the root")
		}
		return nil
	}
	return updateWithChecker(ctx, jm.dir, syncFlush, checker, lastLock, newContents, writeHook)
}

func (jm *journalManifest) Close() (err error) {
	if jm.lock != nil {
		err = jm.lock.Unlock()
		jm.lock = nil
	}
	return
}

func containsJournalSpec(specs []tableSpec) (ok bool) {
	for _, spec := range specs {
		if spec.name == journalAddr {
			ok = true
			break
		}
	}
	return
}
