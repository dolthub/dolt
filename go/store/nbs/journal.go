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
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

var chunkJournalFeatureFlag = false

func init() {
	if os.Getenv("DOLT_ENABLE_CHUNK_JOURNAL") != "" {
		chunkJournalFeatureFlag = true
	}
}

func UseJournalStore(path string) bool {
	if chunkJournalFeatureFlag {
		return true
	}
	ok, err := journalFileExists(filepath.Join(path, chunkJournalAddr))
	if err != nil {
		panic(err)
	}
	return ok
}

const (
	chunkJournalName = chunkJournalAddr // todo
)

// chunkJournal is a persistence abstraction for a NomsBlockStore.
// It implemented both manifest and tablePersister, durably writing
// both memTable persists and manifest updates to a single file.
type chunkJournal struct {
	wr   *journalWriter
	src  journalChunkSource
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

	ok, err := journalFileExists(path)
	if err != nil {
		return nil, err
	} else if ok {
		// only open a journalWriter if the journal file exists,
		// otherwise we wait to open in case we're cloning
		if err = j.openJournal(ctx); err != nil {
			return nil, err
		}
	}
	return j, nil
}

func (j *chunkJournal) openJournal(ctx context.Context) (err error) {
	var ok bool
	ok, err = journalFileExists(j.path)
	if err != nil {
		return err
	}

	if !ok { // create new journal file
		j.wr, err = createJournalWriter(ctx, j.path)
		if err != nil {
			return err
		}

		_, j.src, err = j.wr.ProcessJournal(ctx)
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
			if err = j.wr.WriteRootHash(contents.root); err != nil {
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
	var root hash.Hash
	root, j.src, err = j.wr.ProcessJournal(ctx)
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
		cc := ChunkToCompressedChunk(c)
		lookup, err := j.wr.WriteChunk(cc)
		if err != nil {
			return nil, err
		}
		j.src.lookups.put(*record.a, lookup)
		j.src.compressedSz += uint64(cc.CompressedSize())
	}
	return j.src, nil
}

// ConjoinAll implements tablePersister.
func (j *chunkJournal) ConjoinAll(ctx context.Context, sources chunkSources, stats *Stats) (chunkSource, error) {
	return j.persister.ConjoinAll(ctx, sources, stats)
}

// Open implements tablePersister.
func (j *chunkJournal) Open(ctx context.Context, name addr, chunkCount uint32, stats *Stats) (chunkSource, error) {
	if name == journalAddr {
		if err := j.maybeInit(ctx); err != nil {
			return nil, err
		}
		return j.src, nil
	}
	return j.persister.Open(ctx, name, chunkCount, stats)
}

// Exists implements tablePersister.
func (j *chunkJournal) Exists(ctx context.Context, name addr, chunkCount uint32, stats *Stats) (bool, error) {
	return j.persister.Exists(ctx, name, chunkCount, stats)
}

// PruneTableFiles implements tablePersister.
func (j *chunkJournal) PruneTableFiles(ctx context.Context, contents manifestContents, mtime time.Time) error {
	return j.persister.PruneTableFiles(ctx, contents, mtime)
}

func (j *chunkJournal) Path() string {
	return filepath.Dir(j.path)
}

// Name implements manifest.
func (j *chunkJournal) Name() string {
	return j.path
}

// Update implements manifest.
func (j *chunkJournal) Update(ctx context.Context, lastLock addr, next manifestContents, stats *Stats, writeHook func() error) (manifestContents, error) {
	if j.wr == nil {
		// pass the update to |j.backing| if the journals is not initialized
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
		_, mc, err := j.backing.ParseIfExists(ctx, stats, nil)
		if err != nil {
			return manifestContents{}, err
		}
		lastLock = mc.lock

		mc, err = j.backing.Update(ctx, lastLock, next, stats, nil)
		if err != nil {
			return manifestContents{}, err
		} else if mc.lock != next.lock {
			return manifestContents{}, errOptimisticLockFailedTables
		}
	}

	if err := j.wr.WriteRootHash(next.root); err != nil {
		return manifestContents{}, err
	}
	j.contents = next

	return j.contents, nil
}

// UpdateGCGen implements manifestGCGenUpdater.
func (j *chunkJournal) UpdateGCGen(ctx context.Context, lastLock addr, next manifestContents, stats *Stats, writeHook func() error) (manifestContents, error) {
	updater, ok := j.backing.(manifestGCGenUpdater)
	if !ok {
		err := fmt.Errorf("backing manifest (%s) does not support garbage collection", j.backing.Name())
		return manifestContents{}, err
	}

	latest, err := updater.UpdateGCGen(ctx, lastLock, next, stats, writeHook)
	if err != nil {
		return manifestContents{}, err
	} else if latest.root == next.root {
		j.contents = next // success
	}
	return latest, nil
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
		err = j.openJournal(ctx)
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

// recLookup contains journalRec lookup metadata.
type recLookup struct {
	// journalOff is the file offset of the journalRec.
	journalOff int64

	// recordLen is the length of the journalRec.
	recordLen uint32

	// payloadOff is the offset of the payload within the
	// journalRec, it's used for converting to a Range.
	payloadOff uint32
}

// rangeFromLookup converts a recLookup to a Range,
// used when computing GetDownloadLocs.
func rangeFromLookup(l recLookup) Range {
	return Range{
		// see journalRec for serialization format
		Offset: uint64(l.journalOff) + uint64(l.payloadOff),
		Length: l.recordLen - (l.payloadOff + recChecksumSz),
	}
}

// lookupMap is a thread-safe collection of recLookups.
type lookupMap struct {
	data map[addr]recLookup
	lock *sync.RWMutex
}

func newLookupMap() lookupMap {
	return lookupMap{
		data: make(map[addr]recLookup),
		lock: new(sync.RWMutex),
	}
}

func (m lookupMap) get(a addr) (l recLookup, ok bool) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	l, ok = m.data[a]
	return
}

func (m lookupMap) put(a addr, l recLookup) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.data[a] = l
	return
}

func (m lookupMap) count() int {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return len(m.data)
}

// journalChunkSource is a chunkSource that reads chunks
// from a chunkJournal. Unlike other NBS chunkSources,
// it is not immutable and its set of chunks grows as
// more commits are made to the chunkJournal.
type journalChunkSource struct {
	address      addr
	journal      snapshotReader
	lookups      lookupMap
	compressedSz uint64
}

var _ chunkSource = journalChunkSource{}

func (s journalChunkSource) has(h addr) (bool, error) {
	_, ok := s.lookups.get(h)
	return ok, nil
}

func (s journalChunkSource) hasMany(addrs []hasRecord) (missing bool, err error) {
	for i := range addrs {
		a := addrs[i].a
		if _, ok := s.lookups.get(*a); ok {
			addrs[i].has = true
		} else {
			missing = true
		}
	}
	return
}

func (s journalChunkSource) getCompressed(_ context.Context, h addr, _ *Stats) (CompressedChunk, error) {
	l, ok := s.lookups.get(h)
	if !ok {
		return CompressedChunk{}, nil
	}

	buf := make([]byte, l.recordLen)
	if _, err := s.journal.ReadAt(buf, l.journalOff); err != nil {
		return CompressedChunk{}, nil
	}

	rec, err := readJournalRecord(buf)
	if err != nil {
		return CompressedChunk{}, err
	} else if h != rec.address {
		return CompressedChunk{}, fmt.Errorf("chunk record hash does not match lookup hash (%s != %s)",
			h.String(), rec.address.String())
	}
	return NewCompressedChunk(hash.Hash(h), rec.payload)
}

func (s journalChunkSource) get(ctx context.Context, h addr, stats *Stats) ([]byte, error) {
	cc, err := s.getCompressed(ctx, h, stats)
	if err != nil {
		return nil, err
	} else if cc.IsEmpty() {
		return nil, nil
	}
	ch, err := cc.ToChunk()
	if err != nil {
		return nil, err
	}
	return ch.Data(), nil
}

func (s journalChunkSource) getMany(ctx context.Context, _ *errgroup.Group, reqs []getRecord, found func(context.Context, *chunks.Chunk), stats *Stats) (bool, error) {
	var remaining bool
	// todo: read planning
	for i := range reqs {
		data, err := s.get(ctx, *reqs[i].a, stats)
		if err != nil {
			return false, err
		} else if data != nil {
			ch := chunks.NewChunkWithHash(hash.Hash(*reqs[i].a), data)
			found(ctx, &ch)
		} else {
			remaining = true
		}
	}
	return remaining, nil
}

func (s journalChunkSource) getManyCompressed(ctx context.Context, _ *errgroup.Group, reqs []getRecord, found func(context.Context, CompressedChunk), stats *Stats) (bool, error) {
	var remaining bool
	// todo: read planning
	for i := range reqs {
		cc, err := s.getCompressed(ctx, *reqs[i].a, stats)
		if err != nil {
			return false, err
		} else if cc.IsEmpty() {
			remaining = true
		} else {
			found(ctx, cc)
		}
	}
	return remaining, nil
}

func (s journalChunkSource) count() (uint32, error) {
	return uint32(s.lookups.count()), nil
}

func (s journalChunkSource) uncompressedLen() (uint64, error) {
	// todo(andy)
	return s.compressedSz, nil
}

func (s journalChunkSource) hash() addr {
	return s.address
}

// reader implements chunkSource.
func (s journalChunkSource) reader(context.Context) (io.Reader, uint64, error) {
	rdr, sz, err := s.journal.Snapshot()
	return rdr, uint64(sz), err
}

func (s journalChunkSource) getRecordRanges(requests []getRecord) (map[hash.Hash]Range, error) {
	ranges := make(map[hash.Hash]Range, len(requests))
	for _, req := range requests {
		if req.found {
			continue
		}
		l, ok := s.lookups.get(*req.a)
		if !ok {
			continue
		}
		req.found = true // update |requests|
		ranges[hash.Hash(*req.a)] = rangeFromLookup(l)
	}
	return ranges, nil
}

// size implements chunkSource.
// size returns the total size of the chunkSource: chunks, index, and footer
func (s journalChunkSource) currentSize() uint64 {
	return uint64(s.journal.CurrentSize())
}

// index implements chunkSource.
func (s journalChunkSource) index() (tableIndex, error) {
	return nil, fmt.Errorf("journalChunkSource cannot be conjoined")
}

func (s journalChunkSource) clone() (chunkSource, error) {
	return s, nil
}

func (s journalChunkSource) close() error {
	return nil
}

func equalSpecs(left, right []tableSpec) bool {
	if len(left) != len(right) {
		return false
	}
	l := make(map[addr]struct{}, len(left))
	for _, s := range left {
		l[s.name] = struct{}{}
	}
	for _, s := range right {
		if _, ok := l[s.name]; !ok {
			return false
		}
	}
	return true
}

func emptyAddr(a addr) bool {
	var b addr
	return a == b
}
