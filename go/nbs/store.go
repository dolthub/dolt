// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/constants"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
	"github.com/jpillora/backoff"
)

// The root of a Noms Chunk Store is stored in a 'manifest', along with the
// names of the tables that hold all the chunks in the store. The number of
// chunks in each table is also stored in the manifest.

const (
	// StorageVersion is the version of the on-disk Noms Chunks Store data format.
	StorageVersion = "4"

	defaultMemTableSize uint64 = (1 << 20) * 128 // 128MB
	defaultMaxTables           = 256

	defaultIndexCacheSize = (1 << 20) * 8 // 8MB
)

var (
	cacheOnce        = sync.Once{}
	globalIndexCache *indexCache
	globalFDCache    *fdCache
	globalConjoiner  conjoiner
)

func makeGlobalCaches() {
	globalIndexCache = newIndexCache(defaultIndexCacheSize)
	globalFDCache = newFDCache(defaultMaxTables)
	globalConjoiner = newAsyncConjoiner(defaultMaxTables)
}

type NomsBlockStore struct {
	mm           manifest
	p            tablePersister
	c            conjoiner
	manifestLock addr
	nomsVersion  string

	mu     sync.RWMutex // protects the following state
	mt     *memTable
	tables tableSet
	root   hash.Hash

	mtSize   uint64
	putCount uint64

	stats *Stats
}

func NewAWSStore(table, ns, bucket string, s3 s3svc, ddb ddbsvc, memTableSize uint64) *NomsBlockStore {
	cacheOnce.Do(makeGlobalCaches)
	p := &s3TablePersister{
		s3,
		bucket,
		defaultS3PartSize,
		minS3PartSize,
		maxS3PartSize,
		globalIndexCache,
		make(chan struct{}, 32),
	}
	return newAWSStore(table, ns, ddb, p, globalConjoiner, memTableSize)
}

func newAWSStore(table, ns string, ddb ddbsvc, p tablePersister, c conjoiner, memTableSize uint64) *NomsBlockStore {
	d.PanicIfTrue(ns == "")
	mm := newDynamoManifest(table, ns, ddb)
	return newNomsBlockStore(mm, p, c, memTableSize)
}

func NewLocalStore(dir string, memTableSize uint64) *NomsBlockStore {
	cacheOnce.Do(makeGlobalCaches)
	return newLocalStore(dir, memTableSize, globalFDCache, globalIndexCache, globalConjoiner)
}

func newLocalStore(dir string, memTableSize uint64, fc *fdCache, indexCache *indexCache, c conjoiner) *NomsBlockStore {
	err := checkDir(dir)
	d.PanicIfError(err)
	p := newFSTablePersister(dir, fc, indexCache)
	return newNomsBlockStore(fileManifest{dir}, p, c, memTableSize)
}

func newNomsBlockStore(mm manifest, p tablePersister, c conjoiner, memTableSize uint64) *NomsBlockStore {
	if memTableSize == 0 {
		memTableSize = defaultMemTableSize
	}
	nbs := &NomsBlockStore{
		mm:          mm,
		p:           p,
		c:           c,
		tables:      newTableSet(p),
		nomsVersion: constants.NomsVersion,
		mtSize:      memTableSize,
		stats:       NewStats(),
	}

	if exists, vers, lock, root, tableSpecs := nbs.mm.ParseIfExists(nbs.stats, nil); exists {
		nbs.nomsVersion, nbs.manifestLock, nbs.root = vers, lock, root
		nbs.tables = nbs.tables.Rebase(tableSpecs)
	}

	return nbs
}

func (nbs *NomsBlockStore) Put(c chunks.Chunk) {
	t1 := time.Now()
	a := addr(c.Hash())
	d.PanicIfFalse(nbs.addChunk(a, c.Data()))
	nbs.putCount++

	dur := time.Since(t1) // This actually was 0 sometimes: see attic BUG 1787
	if dur == 0 {
		dur = time.Duration(1)
	}
	nbs.stats.PutLatency.SampleTime(dur)
}

// TODO: figure out if there's a non-error reason for this to return false. If not, get rid of return value.
func (nbs *NomsBlockStore) addChunk(h addr, data []byte) bool {
	nbs.mu.Lock()
	defer nbs.mu.Unlock()
	if nbs.mt == nil {
		nbs.mt = newMemTable(nbs.mtSize)
	}
	if !nbs.mt.addChunk(h, data) {
		nbs.tables = nbs.tables.Prepend(nbs.mt, nbs.stats)
		nbs.mt = newMemTable(nbs.mtSize)
		return nbs.mt.addChunk(h, data)
	}
	return true
}

func (nbs *NomsBlockStore) Get(h hash.Hash) chunks.Chunk {
	t1 := time.Now()
	defer func() {
		nbs.stats.GetLatency.SampleTime(time.Since(t1))
		nbs.stats.ChunksPerGet.Sample(1)
	}()

	a := addr(h)
	data, tables := func() (data []byte, tables chunkReader) {
		nbs.mu.RLock()
		defer nbs.mu.RUnlock()
		if nbs.mt != nil {
			data = nbs.mt.get(a, nbs.stats)
		}
		return data, nbs.tables
	}()
	if data != nil {
		return chunks.NewChunkWithHash(h, data)
	}
	if data := tables.get(a, nbs.stats); data != nil {
		return chunks.NewChunkWithHash(h, data)
	}

	return chunks.EmptyChunk
}

func (nbs *NomsBlockStore) GetMany(hashes hash.HashSet, foundChunks chan *chunks.Chunk) {
	t1 := time.Now()
	reqs := toGetRecords(hashes)

	defer func() {
		if len(hashes) > 0 {
			nbs.stats.GetLatency.SampleTime(time.Since(t1))
			nbs.stats.ChunksPerGet.Sample(uint64(len(reqs)))
		}
	}()

	wg := &sync.WaitGroup{}

	tables, remaining := func() (tables chunkReader, remaining bool) {
		nbs.mu.RLock()
		defer nbs.mu.RUnlock()
		tables = nbs.tables
		remaining = true
		if nbs.mt != nil {
			remaining = nbs.mt.getMany(reqs, foundChunks, nil, nbs.stats)
		}

		return
	}()

	if remaining {
		tables.getMany(reqs, foundChunks, wg, nbs.stats)
		wg.Wait()
	}

}

func toGetRecords(hashes hash.HashSet) []getRecord {
	reqs := make([]getRecord, len(hashes))
	idx := 0
	for h := range hashes {
		a := addr(h)
		reqs[idx] = getRecord{
			a:      &a,
			prefix: a.Prefix(),
		}
		idx++
	}

	sort.Sort(getRecordByPrefix(reqs))
	return reqs
}

func (nbs *NomsBlockStore) CalcReads(hashes hash.HashSet, blockSize uint64) (reads int, split bool) {
	reqs := toGetRecords(hashes)
	tables := func() (tables tableSet) {
		nbs.mu.RLock()
		defer nbs.mu.RUnlock()
		tables = nbs.tables

		return
	}()

	reads, split, remaining := tables.calcReads(reqs, blockSize)
	d.Chk.False(remaining)
	return
}

func (nbs *NomsBlockStore) extractChunks(chunkChan chan<- *chunks.Chunk) {
	ch := make(chan extractRecord, 1)
	go func() {
		defer close(ch)
		nbs.mu.RLock()
		defer nbs.mu.RUnlock()
		// Chunks in nbs.tables were inserted before those in nbs.mt, so extract chunks there _first_
		nbs.tables.extract(ch)
		if nbs.mt != nil {
			nbs.mt.extract(ch)
		}
	}()
	for rec := range ch {
		c := chunks.NewChunkWithHash(hash.Hash(rec.a), rec.data)
		chunkChan <- &c
	}
}

func (nbs *NomsBlockStore) Count() uint32 {
	count, tables := func() (count uint32, tables chunkReader) {
		nbs.mu.RLock()
		defer nbs.mu.RUnlock()
		if nbs.mt != nil {
			count = nbs.mt.count()
		}
		return count, nbs.tables
	}()
	return count + tables.count()
}

func (nbs *NomsBlockStore) Has(h hash.Hash) bool {
	t1 := time.Now()
	defer func() {
		nbs.stats.HasLatency.SampleTime(time.Since(t1))
		nbs.stats.AddressesPerHas.Sample(1)
	}()

	a := addr(h)
	has, tables := func() (bool, chunkReader) {
		nbs.mu.RLock()
		defer nbs.mu.RUnlock()
		return nbs.mt != nil && nbs.mt.has(a), nbs.tables
	}()
	has = has || tables.has(a)

	return has
}

func (nbs *NomsBlockStore) HasMany(hashes hash.HashSet) hash.HashSet {
	t1 := time.Now()

	reqs := toHasRecords(hashes)

	tables, remaining := func() (tables chunkReader, remaining bool) {
		nbs.mu.RLock()
		defer nbs.mu.RUnlock()
		tables = nbs.tables

		remaining = true
		if nbs.mt != nil {
			remaining = nbs.mt.hasMany(reqs)
		}

		return
	}()

	if remaining {
		tables.hasMany(reqs)
	}

	if len(hashes) > 0 {
		nbs.stats.HasLatency.SampleTime(time.Since(t1))
		nbs.stats.AddressesPerHas.SampleLen(len(reqs))
	}

	absent := hash.HashSet{}
	for _, r := range reqs {
		if !r.has {
			absent.Insert(hash.New(r.a[:]))
		}
	}
	return absent
}

func toHasRecords(hashes hash.HashSet) []hasRecord {
	reqs := make([]hasRecord, len(hashes))
	idx := 0
	for h := range hashes {
		a := addr(h)
		reqs[idx] = hasRecord{
			a:      &a,
			prefix: a.Prefix(),
			order:  idx,
		}
		idx++
	}

	sort.Sort(hasRecordByPrefix(reqs))
	return reqs
}

func (nbs *NomsBlockStore) Rebase() {
	if exists, vers, lock, root, tableSpecs := nbs.mm.ParseIfExists(nbs.stats, nil); exists {
		nbs.mu.Lock()
		defer nbs.mu.Unlock()
		nbs.nomsVersion, nbs.manifestLock, nbs.root = vers, lock, root
		nbs.tables = nbs.tables.Rebase(tableSpecs)
	}
}

func (nbs *NomsBlockStore) Root() hash.Hash {
	nbs.mu.RLock()
	defer nbs.mu.RUnlock()
	return nbs.root
}

func (nbs *NomsBlockStore) Commit(current, last hash.Hash) bool {
	anyPossiblyNovelChunks := func() bool {
		nbs.mu.Lock()
		defer nbs.mu.Unlock()
		return nbs.mt != nil || len(nbs.tables.novel) > 0
	}

	if !anyPossiblyNovelChunks() && current == last {
		nbs.Rebase()
		return true
	}

	b := &backoff.Backoff{
		Min:    128 * time.Microsecond,
		Max:    10 * time.Second,
		Factor: 2,
		Jitter: true,
	}
	for {
		if err := nbs.updateManifest(current, last); err == nil {
			return true
		} else if err == errOptimisticLockFailedRoot || err == errLastRootMismatch {
			return false
		}
		time.Sleep(b.Duration())
	}
}

var (
	errLastRootMismatch           = fmt.Errorf("last does not match nbs.Root()")
	errOptimisticLockFailedRoot   = fmt.Errorf("Root moved")
	errOptimisticLockFailedTables = fmt.Errorf("Tables changed")
)

func (nbs *NomsBlockStore) updateManifest(current, last hash.Hash) error {
	nbs.mu.Lock()
	defer nbs.mu.Unlock()
	if nbs.root != last {
		return errLastRootMismatch
	}

	if nbs.mt != nil && nbs.mt.count() > 0 {
		nbs.tables = nbs.tables.Prepend(nbs.mt, nbs.stats)
		nbs.mt = nil
	}

	if nbs.c.ConjoinRequired(nbs.tables) {
		nbs.c.Conjoin(nbs.mm, nbs.p, nbs.tables.Novel(), nbs.stats)
		exists, _, lock, actual, upstream := nbs.mm.ParseIfExists(nbs.stats, nil)
		d.PanicIfFalse(exists)

		nbs.manifestLock = lock
		nbs.root = actual
		nbs.tables = nbs.tables.Rebase(upstream)
		return errOptimisticLockFailedTables
	}

	specs := nbs.tables.ToSpecs()
	nl := generateLockHash(current, specs)
	lock, actual, tableNames := nbs.mm.Update(nbs.manifestLock, nl, specs, current, nbs.stats, nil)
	if nl != lock {
		// Optimistic lock failure. Someone else moved to the root, the set of tables, or both out from under us.
		nbs.manifestLock = lock
		nbs.root = actual
		nbs.tables = nbs.tables.Rebase(tableNames)

		if last != actual {
			return errOptimisticLockFailedRoot
		}
		return errOptimisticLockFailedTables
	}

	nbs.tables = nbs.tables.Flatten()
	nbs.nomsVersion, nbs.manifestLock, nbs.root = constants.NomsVersion, lock, current
	return nil
}

func (nbs *NomsBlockStore) Version() string {
	return nbs.nomsVersion
}

func (nbs *NomsBlockStore) Close() (err error) {
	return
}

func (nbs *NomsBlockStore) Stats() interface{} {
	return *nbs.stats
}
