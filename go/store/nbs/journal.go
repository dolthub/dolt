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
	"path/filepath"
	"sort"
	"time"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

const (
	chunkJournalName = chunkJournalAddr // todo
)

// chunkJournal is a persistence abstraction for a NomsBlockStore.
// It implements both manifest and tablePersister, durably writing
// both memTable persists and manifest updates to a single file.
type chunkJournal struct {
	wr   *journalWriter
	path string

	contents  manifestContents
	backing   manifest
	persister *fsTablePersister
}

var _ tablePersister = &chunkJournal{}
var _ tableFilePersister = &chunkJournal{}
var _ manifest = &chunkJournal{}
var _ manifestGCGenUpdater = &chunkJournal{}
var _ io.Closer = &chunkJournal{}

func newChunkJournal(ctx context.Context, nbfVers, dir string, m manifest, p *fsTablePersister) (*chunkJournal, error) {
	path, err := filepath.Abs(filepath.Join(dir, chunkJournalName))
	if err != nil {
		return nil, err
	}

	j := &chunkJournal{path: path, backing: m, persister: p}
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
// journal file for this chunkJournal. The bootstrapping process differs depending
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
func (j *chunkJournal) bootstrapJournalWriter(ctx context.Context) (err error) {
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

		_, err = j.wr.bootstrapJournal(ctx)
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
	root, err := j.wr.bootstrapJournal(ctx)
	if err != nil {
		return err
	}

	var contents manifestContents
	ok, contents, err = j.backing.ParseIfExists(ctx, &Stats{}, nil)
	if err != nil {
		return err
	}

	if ok {
		// the journal file is the source of truth for the root hash, true-up persisted manifest
		contents.root = root
		contents, err = j.backing.Update(ctx, contents.lock, contents, &Stats{}, nil)
		if err != nil {
			return err
		}
	} else {
		return fmt.Errorf("manifest not found when opening chunk journal")
	}
	j.contents = contents
	return
}

// Persist implements tablePersister.
func (j *chunkJournal) Persist(ctx context.Context, mt *memTable, haver chunkReader, stats *Stats) (chunkSource, error) {
	if err := j.maybeInit(ctx); err != nil {
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
func (j *chunkJournal) ConjoinAll(ctx context.Context, sources chunkSources, stats *Stats) (chunkSource, cleanupFunc, error) {
	return j.persister.ConjoinAll(ctx, sources, stats)
}

// Open implements tablePersister.
func (j *chunkJournal) Open(ctx context.Context, name addr, chunkCount uint32, stats *Stats) (chunkSource, error) {
	if name == journalAddr {
		if err := j.maybeInit(ctx); err != nil {
			return nil, err
		}
		return journalChunkSource{journal: j.wr}, nil
	}
	return j.persister.Open(ctx, name, chunkCount, stats)
}

// Exists implements tablePersister.
func (j *chunkJournal) Exists(ctx context.Context, name addr, chunkCount uint32, stats *Stats) (bool, error) {
	return j.persister.Exists(ctx, name, chunkCount, stats)
}

// PruneTableFiles implements tablePersister.
func (j *chunkJournal) PruneTableFiles(ctx context.Context, keeper func() []addr, mtime time.Time) error {
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

func (j *chunkJournal) Path() string {
	return filepath.Dir(j.path)
}

func (j *chunkJournal) CopyTableFile(ctx context.Context, r io.ReadCloser, fileId string, fileSz uint64, chunkCount uint32) error {
	return j.persister.CopyTableFile(ctx, r, fileId, fileSz, chunkCount)
}

// Name implements manifest.
func (j *chunkJournal) Name() string {
	return j.path
}

// Update implements manifest.
func (j *chunkJournal) Update(ctx context.Context, lastLock addr, next manifestContents, stats *Stats, writeHook func() error) (manifestContents, error) {
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

	return j.contents, nil
}

// UpdateGCGen implements manifestGCGenUpdater.
func (j *chunkJournal) UpdateGCGen(ctx context.Context, lastLock addr, next manifestContents, stats *Stats, writeHook func() error) (manifestContents, error) {
	updater, ok := j.backing.(manifestGCGenUpdater)
	if !ok {
		return manifestContents{}, fmt.Errorf("manifest (%s) does not support garbage collection", j.backing.Name())
	} else if j.contents.lock != lastLock {
		return j.contents, nil // |next| is stale
	}

	// UpdateGCGen below cannot update the root hash, only the GC generation
	// flush |j.contents| with the latest root hash here
	if err := j.flushToBackingManifest(ctx, j.contents, stats); err != nil {
		return manifestContents{}, err
	}

	latest, err := updater.UpdateGCGen(ctx, j.contents.lock, next, stats, writeHook)
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
	return latest, nil
}

// flushToBackingManifest attempts to update the backing file manifest with |next|. This is necessary
// when making manifest updates other than root hash updates (adding new table files, updating GC gen, etc).
func (j *chunkJournal) flushToBackingManifest(ctx context.Context, next manifestContents, stats *Stats) error {
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

func (j *chunkJournal) dropJournalWriter(ctx context.Context) error {
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
func (j *chunkJournal) ParseIfExists(ctx context.Context, stats *Stats, readHook func() error) (ok bool, mc manifestContents, err error) {
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

func (j *chunkJournal) maybeInit(ctx context.Context) (err error) {
	if j.wr == nil {
		err = j.bootstrapJournalWriter(ctx)
	}
	return
}

// Close implements io.Closer
func (j *chunkJournal) Close() (err error) {
	ctx := context.Background()
	_, last, cerr := j.backing.ParseIfExists(ctx, &Stats{}, nil)
	if cerr != nil {
		err = cerr
	} else if !emptyAddr(j.contents.lock) {
		// best effort update to |backing|, this will
		// fail if the database has been deleted.
		// if we spuriously fail, we'll update |backing|
		// next time we open this chunkJournal
		_, _ = j.backing.Update(ctx, last.lock, j.contents, &Stats{}, nil)
	}
	if j.wr != nil {
		err = j.wr.Close()
	}
	return
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
	keepers = append(keepers, stash)
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
