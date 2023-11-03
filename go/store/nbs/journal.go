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
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/dolthub/dolt/go/libraries/doltcore/dconfig"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/fslock"
)

const (
	chunkJournalName = chunkJournalAddr // todo
)

var reflogDisabled = false
var reflogRecordLimit = 100_000
var loggedReflogMaxSizeWarning = false

func init() {
	if os.Getenv(dconfig.EnvDisableReflog) != "" {
		reflogDisabled = true
	}

	if limit := os.Getenv(dconfig.EnvReflogRecordLimit); limit != "" {
		i, err := strconv.Atoi(limit)
		if err != nil {
			logrus.Warnf("unable to parse integer value for %s: %s", dconfig.EnvReflogRecordLimit, err.Error())
		} else {
			if i <= 0 {
				reflogDisabled = true
			} else {
				reflogRecordLimit = i
			}
		}
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

	// mu locks access to the in-memory roots and root timestamp information that is queried by Dolt's reflog
	mu sync.Mutex
	// roots holds an in-memory representation of the root hashes that have been written to the ChunkJournal
	roots []string
	// rootTimestamps holds a timestamp for each of the root hashes that have been written to the ChunkJournal
	rootTimestamps []time.Time
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

		_, _, _, err = j.wr.bootstrapJournal(ctx)
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
			if err = j.wr.commitRootHash(contents.root); err != nil {
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
	root, roots, rootTimestamps, err := j.wr.bootstrapJournal(ctx)
	if err != nil {
		return err
	}

	j.mu.Lock()
	j.roots = roots
	j.rootTimestamps = rootTimestamps
	j.mu.Unlock()

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
	next := generateLockHash(mc.root, mc.specs, mc.appendix)
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
	roots, rootTimestamps, length, err := j.readCurrentRootsAndTimestamps()
	if err != nil {
		return err
	}

	// journal.roots are stored in chronological order. We need to process them in that order so that we can
	// accurately detect the root where a ref was first set to a commit. Note that we are careful to not iterate
	// beyond |length| in the slice, otherwise we risk a race condition that would read inconsistent data.
	for i := 0; i < length; i++ {
		// If we're reading a chunk journal written with an older version of Dolt, the root hash journal record may
		// not have a timestamp value, so we'll have a time.Time instance in its zero value. If we see this, pass
		// nil instead to signal to callers that there is no valid timestamp available.
		var timestamp *time.Time = nil
		if time.Time.IsZero(rootTimestamps[i]) == false {
			timestamp = &rootTimestamps[i]
		}
		err := f(roots[i], timestamp)
		if err != nil {
			return err
		}
	}

	return nil
}

// readCurrentRootsAndTimestamps grabs the mutex that protects the in-memory root and root timestamps that represent the
// root hash updates in the chunk journal and returns the references to the roots and root timestamps slices, as well as
// the length that can be safely read from them. Callers MUST honor this length and NOT read beyond it in the returned
// slices, otherwise they risk getting inconsistent data (since the chunk journal continues to append entries to these
// slices as new root update journal records are saved).
func (j *ChunkJournal) readCurrentRootsAndTimestamps() (roots []string, rootTimestamps []time.Time, length int, err error) {
	j.mu.Lock()
	defer j.mu.Unlock()

	roots = j.roots
	rootTimestamps = j.rootTimestamps
	length = len(roots)
	if len(roots) != len(rootTimestamps) {
		return nil, nil, -1, fmt.Errorf(
			"different number of roots and root timestamps encountered in ChunkJournal")
	}

	return roots, rootTimestamps, length, nil
}

// Persist implements tablePersister.
func (j *ChunkJournal) Persist(ctx context.Context, mt *memTable, haver chunkReader, stats *Stats) (chunkSource, error) {
	if j.backing.readOnly() {
		return nil, errReadOnlyManifest
	} else if err := j.maybeInit(ctx); err != nil {
		return nil, err
	}

	if haver != nil {
		sort.Sort(hasRecordByPrefix(mt.order)) // hasMany() requires addresses to be sorted.
		if _, err := haver.hasMany(mt.order); err != nil {
			return nil, err
		}
		sort.Sort(hasRecordByOrder(mt.order)) // restore "insertion" order for write
	}

	for _, record := range mt.order {
		if record.has {
			continue
		}
		c := chunks.NewChunkWithHash(hash.Hash(*record.a), mt.chunks[*record.a])
		err := j.wr.writeCompressedChunk(ChunkToCompressedChunk(c))
		if err != nil {
			return nil, err
		}
	}
	return journalChunkSource{journal: j.wr}, nil
}

// ConjoinAll implements tablePersister.
func (j *ChunkJournal) ConjoinAll(ctx context.Context, sources chunkSources, stats *Stats) (chunkSource, cleanupFunc, error) {
	if j.backing.readOnly() {
		return nil, nil, errReadOnlyManifest
	}
	return j.persister.ConjoinAll(ctx, sources, stats)
}

// Open implements tablePersister.
func (j *ChunkJournal) Open(ctx context.Context, name addr, chunkCount uint32, stats *Stats) (chunkSource, error) {
	if name == journalAddr {
		if err := j.maybeInit(ctx); err != nil {
			return nil, err
		}
		return journalChunkSource{journal: j.wr}, nil
	}
	return j.persister.Open(ctx, name, chunkCount, stats)
}

// Exists implements tablePersister.
func (j *ChunkJournal) Exists(ctx context.Context, name addr, chunkCount uint32, stats *Stats) (bool, error) {
	return j.persister.Exists(ctx, name, chunkCount, stats)
}

// PruneTableFiles implements tablePersister.
func (j *ChunkJournal) PruneTableFiles(ctx context.Context, keeper func() []addr, mtime time.Time) error {
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
func (j *ChunkJournal) Update(ctx context.Context, lastLock addr, next manifestContents, stats *Stats, writeHook func() error) (manifestContents, error) {
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

	if err := j.wr.commitRootHash(next.root); err != nil {
		return manifestContents{}, err
	}
	j.contents = next

	// Update the in-memory structures so that the ChunkJournal can be queried for reflog data
	if !reflogDisabled {
		j.mu.Lock()
		defer j.mu.Unlock()

		if len(j.roots) < reflogRecordLimit {
			j.roots = append(j.roots, next.root.String())
			j.rootTimestamps = append(j.rootTimestamps, time.Now())
		} else if !loggedReflogMaxSizeWarning {
			loggedReflogMaxSizeWarning = true
			logrus.Warnf("exceeded reflog record limit (%d)", reflogRecordLimit)
		}
	}

	return j.contents, nil
}

// UpdateGCGen implements manifestGCGenUpdater.
func (j *ChunkJournal) UpdateGCGen(ctx context.Context, lastLock addr, next manifestContents, stats *Stats, writeHook func() error) (manifestContents, error) {
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

	// Truncate the in-memory root and root timestamp metadata to the most recent
	// entry, and double check that it matches the root stored in the manifest.
	if !reflogDisabled {
		j.mu.Lock()
		defer j.mu.Unlock()

		if len(j.roots) == 0 {
			return manifestContents{}, fmt.Errorf(
				"ChunkJournal roots not intialized; no roots in memory")
		}
		j.roots = j.roots[len(j.roots)-1:]
		j.rootTimestamps = j.rootTimestamps[len(j.rootTimestamps)-1:]
		if j.roots[0] != latest.root.String() {
			return manifestContents{}, fmt.Errorf(
				"ChunkJournal root doesn't match manifest root")
		}
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

// newJournalManifest makes a new file manifest.
func newJournalManifest(ctx context.Context, dir string) (m *journalManifest, err error) {
	lock := fslock.New(filepath.Join(dir, lockFileName))
	// try to take the file lock. if we fail, make the manifest read-only.
	// if we succeed, hold the file lock until we close the journalManifest
	err = lock.LockWithTimeout(lockFileTimeout)
	if errors.Is(err, fslock.ErrTimeout) {
		lock, err = nil, nil // read only
	} else if err != nil {
		return nil, err
	}
	m = &journalManifest{dir: dir, lock: lock}

	var f *os.File
	f, err = openIfExists(filepath.Join(dir, manifestFileName))
	if err != nil {
		_ = lock.Unlock()
		return nil, err
	} else if f == nil {
		return m, nil
	}
	defer func() {
		if cerr := f.Close(); err == nil {
			err = cerr // keep first error
		}
		if err != nil {
			_ = lock.Unlock()
		}
	}()

	var ok bool
	ok, _, err = m.ParseIfExists(ctx, &Stats{}, nil)
	if err != nil {
		_ = lock.Unlock()
		return nil, err
	} else if !ok {
		_ = lock.Unlock()
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
func (jm *journalManifest) Update(ctx context.Context, lastLock addr, newContents manifestContents, stats *Stats, writeHook func() error) (mc manifestContents, err error) {
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
func (jm *journalManifest) UpdateGCGen(ctx context.Context, lastLock addr, newContents manifestContents, stats *Stats, writeHook func() error) (mc manifestContents, err error) {
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
		}
	}
	return
}
