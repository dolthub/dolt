// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"path"
	"sort"
	"sync"
	"time"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/constants"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/jpillora/backoff"
)

// The root of a Noms Chunk Store is stored in a 'manifest', along with the
// names of the tables that hold all the chunks in the store. The number of
// chunks in each table is also stored in the manifest.

const (
	// StorageVersion is the version of the on-disk Noms Chunks Store data format.
	StorageVersion = "4"

	defaultMemTableSize uint64 = (1 << 20) * 128 // 128MB
	defaultAWSReadLimit        = 1024
	defaultMaxTables           = 128

	defaultIndexCacheSize = (1 << 20) * 8 // 8MB
)

var (
	indexCacheOnce   = sync.Once{}
	globalIndexCache *indexCache
)

func makeGlobalIndexCache() { globalIndexCache = newIndexCache(defaultIndexCacheSize) }

type NomsBlockStore struct {
	mm           manifest
	manifestLock addr
	nomsVersion  string

	mu     sync.RWMutex // protects the following state
	mt     *memTable
	tables tableSet
	root   hash.Hash

	mtSize    uint64
	maxTables int
	putCount  uint64
}

type AWSStoreFactory struct {
	s3            s3svc
	ddb           ddbsvc
	table, bucket string
	indexCache    *indexCache
	readRl        chan struct{}
}

func NewAWSStoreFactory(sess *session.Session, table, bucket string, indexCacheSize uint64) chunks.Factory {
	var indexCache *indexCache
	if indexCacheSize > 0 {
		indexCache = newIndexCache(indexCacheSize)
	}
	return &AWSStoreFactory{s3.New(sess), dynamodb.New(sess), table, bucket, indexCache, make(chan struct{}, defaultAWSReadLimit)}
}

func (asf *AWSStoreFactory) CreateStore(ns string) chunks.ChunkStore {
	return newAWSStore(asf.table, ns, asf.bucket, asf.s3, asf.ddb, defaultMemTableSize, asf.indexCache, asf.readRl)
}

func (asf *AWSStoreFactory) Shutter() {
}

type LocalStoreFactory struct {
	dir        string
	indexCache *indexCache
	maxTables  int
}

func CheckDir(dir string) error {
	stat, err := os.Stat(dir)
	if err != nil {
		return err
	}
	if !stat.IsDir() {
		return fmt.Errorf("Path is not a directory: %s", dir)
	}
	return nil
}

func NewLocalStoreFactory(dir string, indexCacheSize uint64, maxTables int) chunks.Factory {
	err := CheckDir(dir)
	d.PanicIfError(err)

	var indexCache *indexCache
	if indexCacheSize > 0 {
		indexCache = newIndexCache(indexCacheSize)
	}
	return &LocalStoreFactory{dir, indexCache, maxTables}
}

func (lsf *LocalStoreFactory) CreateStore(ns string) chunks.ChunkStore {
	path := path.Join(lsf.dir, ns)
	err := os.MkdirAll(path, 0777)
	d.PanicIfError(err)
	return newLocalStore(path, defaultMemTableSize, lsf.indexCache, lsf.maxTables)
}

func (lsf *LocalStoreFactory) Shutter() {
}

func NewAWSStore(table, ns, bucket string, s3 s3svc, ddb ddbsvc, memTableSize uint64) *NomsBlockStore {
	indexCacheOnce.Do(makeGlobalIndexCache)
	return newAWSStore(table, ns, bucket, s3, ddb, memTableSize, globalIndexCache, make(chan struct{}, 32))
}

func newAWSStore(table, ns, bucket string, s3 s3svc, ddb ddbsvc, memTableSize uint64, indexCache *indexCache, readRl chan struct{}) *NomsBlockStore {
	d.PanicIfTrue(ns == "")
	mm := newDynamoManifest(table, ns, ddb)
	ts := newS3TableSet(s3, bucket, indexCache, readRl)
	return newNomsBlockStore(mm, ts, memTableSize, defaultMaxTables)
}

func NewLocalStore(dir string, memTableSize uint64) *NomsBlockStore {
	indexCacheOnce.Do(makeGlobalIndexCache)
	return newLocalStore(dir, memTableSize, globalIndexCache, defaultMaxTables)
}

func newLocalStore(dir string, memTableSize uint64, indexCache *indexCache, maxTables int) *NomsBlockStore {
	err := CheckDir(dir)
	d.PanicIfError(err)
	return newNomsBlockStore(fileManifest{dir}, newFSTableSet(dir, indexCache), memTableSize, maxTables)
}

func newNomsBlockStore(mm manifest, ts tableSet, memTableSize uint64, maxTables int) *NomsBlockStore {
	if memTableSize == 0 {
		memTableSize = defaultMemTableSize
	}
	nbs := &NomsBlockStore{
		mm:          mm,
		tables:      ts,
		nomsVersion: constants.NomsVersion,
		mtSize:      memTableSize,
		maxTables:   maxTables,
	}

	if exists, vers, lock, root, tableSpecs := nbs.mm.ParseIfExists(nil); exists {
		nbs.nomsVersion, nbs.manifestLock, nbs.root = vers, lock, root
		nbs.tables, _ = nbs.tables.Rebase(tableSpecs)
	}

	return nbs
}

func (nbs *NomsBlockStore) Put(c chunks.Chunk) {
	a := addr(c.Hash())
	d.PanicIfFalse(nbs.addChunk(a, c.Data()))
	nbs.putCount++
}

func (nbs *NomsBlockStore) SchedulePut(c chunks.Chunk) {
	nbs.Put(c)
}

func (nbs *NomsBlockStore) PutMany(chunx []chunks.Chunk) {
	for _, c := range chunx {
		nbs.Put(c)
	}
}

// TODO: figure out if there's a non-error reason for this to return false. If not, get rid of return value.
func (nbs *NomsBlockStore) addChunk(h addr, data []byte) bool {
	nbs.mu.Lock()
	defer nbs.mu.Unlock()
	if nbs.mt == nil {
		nbs.mt = newMemTable(nbs.mtSize)
	}
	if !nbs.mt.addChunk(h, data) {
		nbs.tables = nbs.tables.Prepend(nbs.mt)
		nbs.mt = newMemTable(nbs.mtSize)
		return nbs.mt.addChunk(h, data)
	}
	return true
}

func (nbs *NomsBlockStore) Get(h hash.Hash) chunks.Chunk {
	a := addr(h)
	data, tables := func() (data []byte, tables chunkReader) {
		nbs.mu.RLock()
		defer nbs.mu.RUnlock()
		if nbs.mt != nil {
			data = nbs.mt.get(a)
		}
		return data, nbs.tables
	}()
	if data != nil {
		return chunks.NewChunkWithHash(h, data)
	}
	if data := tables.get(a); data != nil {
		return chunks.NewChunkWithHash(h, data)
	}
	return chunks.EmptyChunk
}

func (nbs *NomsBlockStore) GetMany(hashes hash.HashSet, foundChunks chan *chunks.Chunk) {
	reqs := toGetRecords(hashes)

	wg := &sync.WaitGroup{}

	tables, remaining := func() (tables chunkReader, remaining bool) {
		nbs.mu.RLock()
		defer nbs.mu.RUnlock()
		tables = nbs.tables
		remaining = true
		if nbs.mt != nil {
			remaining = nbs.mt.getMany(reqs, foundChunks, nil)
		}

		return
	}()

	if remaining {
		tables.getMany(reqs, foundChunks, wg)
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
	a := addr(h)
	has, tables := func() (bool, chunkReader) {
		nbs.mu.RLock()
		defer nbs.mu.RUnlock()
		return nbs.mt != nil && nbs.mt.has(a), nbs.tables
	}()
	return has || tables.has(a)
}

func (nbs *NomsBlockStore) HasMany(hashes hash.HashSet) hash.HashSet {
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
	return fromHasRecords(reqs)
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

func fromHasRecords(reqs []hasRecord) hash.HashSet {
	present := hash.HashSet{}
	for _, r := range reqs {
		if r.has {
			present.Insert(hash.New(r.a[:]))
		}
	}
	return present
}

func (nbs *NomsBlockStore) Rebase() {
	if exists, vers, lock, root, tableSpecs := nbs.mm.ParseIfExists(nil); exists {
		nbs.mu.Lock()
		defer nbs.mu.Unlock()
		nbs.nomsVersion, nbs.manifestLock, nbs.root = vers, lock, root
		var dropped chunkSources
		nbs.tables, dropped = nbs.tables.Rebase(tableSpecs)
		dropped.close()
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

var compactThreshRand = rand.New(rand.NewSource(time.Now().UnixNano()))

func (nbs *NomsBlockStore) updateManifest(current, last hash.Hash) error {
	nbs.mu.Lock()
	defer nbs.mu.Unlock()
	if nbs.root != last {
		return errLastRootMismatch
	}

	if nbs.mt != nil && nbs.mt.count() > 0 {
		nbs.tables = nbs.tables.Prepend(nbs.mt)
		nbs.mt = nil
	}

	candidate := nbs.tables
	var compactees chunkSources

	shouldCompact := func() bool {
		// As the number of tables grows from 1 to maxTables, the probability of compacting, grows from 0 to 1
		thresh := float64(math.Max(0, float64(len(candidate.upstream)-1))) / float64(nbs.maxTables-1)
		return compactThreshRand.Float64() < thresh
	}

	if shouldCompact() {
		candidate, compactees = candidate.Compact() // Compact() must only compact upstream tables (BUG 3142)
	}

	specs := candidate.ToSpecs()
	nl := generateLockHash(current, specs)
	lock, actual, tableNames := nbs.mm.Update(nbs.manifestLock, nl, specs, current, nil)
	if nl != lock {
		// Optimistic lock failure. Someone else moved to the root, the set of tables, or both out from under us.
		// Regardless of what happened, we're going to start fresh by re-opening all the new tables from upstream, and re-calculating which tables to compact, so close all the compactees as well as any chunkSources that are dropped during Rebase().
		compactees.close()
		var dropped chunkSources
		nbs.manifestLock = lock
		nbs.root = actual
		nbs.tables, dropped = candidate.Rebase(tableNames)
		dropped.close()

		if last != actual {
			return errOptimisticLockFailedRoot
		}
		return errOptimisticLockFailedTables
	}

	nbs.tables = candidate.Flatten()
	compactees.close()
	nbs.nomsVersion, nbs.manifestLock, nbs.root = constants.NomsVersion, lock, current
	return nil
}

func (nbs *NomsBlockStore) Version() string {
	return nbs.nomsVersion
}

func (nbs *NomsBlockStore) Close() (err error) {
	nbs.mu.Lock()
	defer nbs.mu.Unlock()
	return nbs.tables.Close()
}
