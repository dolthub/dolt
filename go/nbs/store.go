// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"os"
	"path"
	"sort"
	"sync"
	"time"
	"fmt"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/constants"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/types"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/jpillora/backoff"
)

// The root of a Noms Chunk Store is stored in a 'manifest', along with the
// names of the tables that hold all the chunks in the store. The number of
// chunks in each table is also stored in the manifest.

type EnumerationOrder uint8

const (
	// StorageVersion is the version of the on-disk Noms Chunks Store data format.
	StorageVersion = "2"

	defaultMemTableSize uint64 = (1 << 20) * 128 // 128MB
	defaultAWSReadLimit        = 1024
	defaultMaxTables           = 128

	defaultIndexCacheSize = (1 << 20) * 8 // 8MB

	InsertOrder EnumerationOrder = iota
	ReverseOrder
)

var (
	indexCacheOnce   = sync.Once{}
	globalIndexCache *indexCache
)

func makeGlobalIndexCache() { globalIndexCache = newIndexCache(defaultIndexCacheSize) }

type NomsBlockStore struct {
	mm          manifest
	nomsVersion string

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
	return newLocalStore(path.Join(lsf.dir, ns), defaultMemTableSize, lsf.indexCache, lsf.maxTables)
}

func (lsf *LocalStoreFactory) Shutter() {
}

func NewAWSStore(table, ns, bucket string, s3 s3svc, ddb ddbsvc, memTableSize uint64) *NomsBlockStore {
	indexCacheOnce.Do(makeGlobalIndexCache)
	return newAWSStore(table, ns, bucket, s3, ddb, memTableSize, globalIndexCache, make(chan struct{}, 32))
}

func newAWSStore(table, ns, bucket string, s3 s3svc, ddb ddbsvc, memTableSize uint64, indexCache *indexCache, readRl chan struct{}) *NomsBlockStore {
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

	if exists, vers, root, tableSpecs := nbs.mm.ParseIfExists(nil); exists {
		nbs.nomsVersion, nbs.root = vers, root
		nbs.tables, _ = nbs.tables.Rebase(tableSpecs)
	}

	return nbs
}

func (nbs *NomsBlockStore) Put(c chunks.Chunk) {
	a := addr(c.Hash())
	d.PanicIfFalse(nbs.addChunk(a, c.Data()))
	nbs.putCount++
}

func (nbs *NomsBlockStore) SchedulePut(c chunks.Chunk, refHeight uint64, hints types.Hints) {
	nbs.Put(c)
}

func (nbs *NomsBlockStore) PutMany(chunx []chunks.Chunk) (err chunks.BackpressureError) {
	for ; len(chunx) > 0; chunx = chunx[1:] {
		c := chunx[0]
		a := addr(c.Hash())
		if !nbs.addChunk(a, c.Data()) {
			break
		}
		nbs.putCount++
	}
	for _, c := range chunx {
		err = append(err, c.Hash())
	}

	return err
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

		if nbs.mt != nil {
			remaining = nbs.mt.getMany(reqs, foundChunks, &sync.WaitGroup{})
			wg.Wait()
		} else {
			remaining = true
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

func (nbs *NomsBlockStore) extractChunks(order EnumerationOrder, chunkChan chan<- *chunks.Chunk) {
	ch := make(chan extractRecord, 1)
	go func() {
		nbs.mu.RLock()
		defer nbs.mu.RUnlock()
		// Chunks in nbs.tables were inserted before those in nbs.mt, so extract chunks there _first_ if we're doing InsertOrder...
		if order == InsertOrder {
			nbs.tables.extract(order, ch)
		}
		if nbs.mt != nil {
			nbs.mt.extract(order, ch)
		}
		// ...and do them _second_ if we're doing ReverseOrder
		if order == ReverseOrder {
			nbs.tables.extract(order, ch)
		}

		close(ch)
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

func (nbs *NomsBlockStore) Root() hash.Hash {
	nbs.mu.RLock()
	defer nbs.mu.RUnlock()
	return nbs.root
}

func (nbs *NomsBlockStore) UpdateRoot(current, last hash.Hash) bool {
	nbs.mu.Lock()
	defer nbs.mu.Unlock()
	if nbs.root != last {
		return false
	}

	if nbs.mt != nil && nbs.mt.count() > 0 {
		nbs.tables = nbs.tables.Prepend(nbs.mt)
		nbs.mt = nil
	}

	candidate := nbs.tables
	var compactees chunkSources
	if candidate.Size() > nbs.maxTables {
		candidate, compactees = candidate.Compact() // Compact() must only compact upstream tables (BUG 3142)
	}

	actual, tableNames := nbs.mm.Update(candidate.ToSpecs(), nbs.root, current, nil)

	if current != actual {
		// Optimistic lock failure. Since we're going to start fresh, re-opening all the new tables from upstream, and re-calculate which tables to compact, close all the compactees as well as the chunkSources that are dropped during Rebase().
		compactees.close()
		var dropped chunkSources
		nbs.root = actual
		nbs.tables, dropped = candidate.Rebase(tableNames)
		dropped.close()
		return false
	}
	nbs.tables = candidate.Flatten()
	compactees.close()
	nbs.nomsVersion, nbs.root = constants.NomsVersion, current
	return true
}

func (nbs *NomsBlockStore) Version() string {
	return nbs.nomsVersion
}

func (nbs *NomsBlockStore) Close() (err error) {
	nbs.mu.Lock()
	defer nbs.mu.Unlock()
	return nbs.tables.Close()
}

// types.BatchStore
func (nbs *NomsBlockStore) AddHints(hints types.Hints) {
	// noop
}

func (nbs *NomsBlockStore) Flush() {
	b := &backoff.Backoff{
		Min:    128 * time.Microsecond,
		Max:    10 * time.Second,
		Factor: 2,
		Jitter: true,
	}
	for !nbs.UpdateRoot(nbs.root, nbs.root) {
		time.Sleep(b.Duration())
	}
}
