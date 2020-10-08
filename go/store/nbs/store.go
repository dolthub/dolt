// Copyright 2019 Liquidata, Inc.
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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/dustin/go-humanize"
	"github.com/pkg/errors"

	"github.com/dolthub/dolt/go/store/atomicerr"
	"github.com/dolthub/dolt/go/store/blobstore"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

var ErrFetchFailure = errors.New("fetch failed")

// The root of a Noms Chunk Store is stored in a 'manifest', along with the
// names of the tables that hold all the chunks in the store. The number of
// chunks in each table is also stored in the manifest.

const (
	// StorageVersion is the version of the on-disk Noms Chunks Store data format.
	// todo: how to handle discrepancies between file manifest and dynamo manifest
	StorageVersion = "5"

	defaultMemTableSize uint64 = (1 << 20) * 128 // 128MB
	defaultMaxTables           = 256

	defaultIndexCacheSize    = (1 << 20) * 64 // 64MB
	defaultManifestCacheSize = 1 << 23        // 8MB
	preflushChunkCount       = 8
)

var (
	cacheOnce           = sync.Once{}
	globalIndexCache    *indexCache
	makeManifestManager func(manifest) manifestManager
	globalFDCache       *fdCache
)

func makeGlobalCaches() {
	globalIndexCache = newIndexCache(defaultIndexCacheSize)
	globalFDCache = newFDCache(defaultMaxTables)

	manifestCache := newManifestCache(defaultManifestCacheSize)
	manifestLocks := newManifestLocks()
	makeManifestManager = func(m manifest) manifestManager { return manifestManager{m, manifestCache, manifestLocks} }
}

type NomsBlockStore struct {
	mm manifestManager
	p  tablePersister
	c  conjoiner

	mu       sync.RWMutex // protects the following state
	mt       *memTable
	tables   tableSet
	upstream manifestContents

	mtSize   uint64
	putCount uint64

	stats *Stats
}

var _ TableFileStore = &NomsBlockStore{}

type Range struct {
	Offset uint64
	Length uint32
}

func (nbs *NomsBlockStore) GetChunkLocations(hashes hash.HashSet) (map[hash.Hash]map[hash.Hash]Range, error) {
	gr := toGetRecords(hashes)

	ranges := make(map[hash.Hash]map[hash.Hash]Range)
	f := func(css chunkSources) error {
		for _, cs := range css {
			switch tr := cs.(type) {
			case *mmapTableReader:
				offsetRecSlice, _ := tr.findOffsets(gr)
				if len(offsetRecSlice) > 0 {
					y, ok := ranges[hash.Hash(tr.h)]

					if !ok {
						y = make(map[hash.Hash]Range)
					}

					for _, offsetRec := range offsetRecSlice {
						h := hash.Hash(*offsetRec.a)
						y[h] = Range{Offset: offsetRec.offset, Length: offsetRec.length}

						delete(hashes, h)
					}

					if len(offsetRecSlice) > 0 {
						gr = toGetRecords(hashes)
					}

					ranges[hash.Hash(tr.h)] = y
				}
			case *chunkSourceAdapter:
				y, ok := ranges[hash.Hash(tr.h)]

				if !ok {
					y = make(map[hash.Hash]Range)
				}

				tableIndex, err := tr.index()

				if err != nil {
					return err
				}

				var foundHashes []hash.Hash
				for h := range hashes {
					a := addr(h)
					e, ok := tableIndex.Lookup(&a)
					if ok {
						foundHashes = append(foundHashes, h)
						y[h] = Range{Offset: e.Offset(), Length: e.Length()}
					}
				}

				ranges[hash.Hash(tr.h)] = y

				for _, h := range foundHashes {
					delete(hashes, h)
				}

			default:
				panic(reflect.TypeOf(cs))
			}

		}

		return nil
	}

	err := f(nbs.tables.upstream)

	if err != nil {
		return nil, err
	}

	err = f(nbs.tables.novel)

	if err != nil {
		return nil, err
	}

	return ranges, nil
}

func (nbs *NomsBlockStore) UpdateManifest(ctx context.Context, updates map[hash.Hash]uint32) (mi ManifestInfo, err error) {
	nbs.mm.LockForUpdate()
	defer func() {
		unlockErr := nbs.mm.UnlockForUpdate()

		if err == nil {
			err = unlockErr
		}
	}()

	nbs.mu.Lock()
	defer nbs.mu.Unlock()

	var stats Stats
	var ok bool
	var contents manifestContents
	ok, contents, err = nbs.mm.Fetch(ctx, &stats)

	if err != nil {
		return manifestContents{}, err
	} else if !ok {
		contents = manifestContents{vers: nbs.upstream.vers}
	}

	currSpecs := contents.getSpecSet()

	var addCount int
	for h, count := range updates {
		a := addr(h)

		if _, ok := currSpecs[a]; !ok {
			addCount++
			contents.specs = append(contents.specs, tableSpec{a, count})
		}
	}

	if addCount == 0 {
		return contents, nil
	}

	var updatedContents manifestContents
	updatedContents, err = nbs.mm.Update(ctx, contents.lock, contents, &stats, nil)

	if err != nil {
		return manifestContents{}, err
	}

	newTables, err := nbs.tables.Rebase(ctx, contents.specs, nbs.stats)

	if err != nil {
		return manifestContents{}, err
	}

	nbs.upstream = updatedContents
	oldTables := nbs.tables
	nbs.tables = newTables
	err = oldTables.Close()
	if err != nil {
		return manifestContents{}, err
	}

	return updatedContents, nil
}

func NewAWSStoreWithMMapIndex(ctx context.Context, nbfVerStr string, table, ns, bucket string, s3 s3svc, ddb ddbsvc, memTableSize uint64) (*NomsBlockStore, error) {
	cacheOnce.Do(makeGlobalCaches)
	readRateLimiter := make(chan struct{}, 32)
	p := &awsTablePersister{
		s3,
		bucket,
		readRateLimiter,
		nil,
		&ddbTableStore{ddb, table, readRateLimiter, nil},
		awsLimits{defaultS3PartSize, minS3PartSize, maxS3PartSize, maxDynamoItemSize, maxDynamoChunks},
		globalIndexCache,
		ns,
		func(bs []byte) (tableIndex, error) {
			ohi, err := parseTableIndex(bs)
			if err != nil {
				return nil, err
			}
			return newMmapTableIndex(ohi, nil)
		},
	}
	mm := makeManifestManager(newDynamoManifest(table, ns, ddb))
	return newNomsBlockStore(ctx, nbfVerStr, mm, p, inlineConjoiner{defaultMaxTables}, memTableSize)
}

func NewAWSStore(ctx context.Context, nbfVerStr string, table, ns, bucket string, s3 s3svc, ddb ddbsvc, memTableSize uint64) (*NomsBlockStore, error) {
	cacheOnce.Do(makeGlobalCaches)
	readRateLimiter := make(chan struct{}, 32)
	p := &awsTablePersister{
		s3,
		bucket,
		readRateLimiter,
		nil,
		&ddbTableStore{ddb, table, readRateLimiter, nil},
		awsLimits{defaultS3PartSize, minS3PartSize, maxS3PartSize, maxDynamoItemSize, maxDynamoChunks},
		globalIndexCache,
		ns,
		func(bs []byte) (tableIndex, error) {
			return parseTableIndex(bs)
		},
	}
	mm := makeManifestManager(newDynamoManifest(table, ns, ddb))
	return newNomsBlockStore(ctx, nbfVerStr, mm, p, inlineConjoiner{defaultMaxTables}, memTableSize)
}

// NewGCSStore returns an nbs implementation backed by a GCSBlobstore
func NewGCSStore(ctx context.Context, nbfVerStr string, bucketName, path string, gcs *storage.Client, memTableSize uint64) (*NomsBlockStore, error) {
	cacheOnce.Do(makeGlobalCaches)

	bs := blobstore.NewGCSBlobstore(gcs, bucketName, path)
	return NewBSStore(ctx, nbfVerStr, bs, memTableSize)
}

// NewBSStore returns an nbs implementation backed by a Blobstore
func NewBSStore(ctx context.Context, nbfVerStr string, bs blobstore.Blobstore, memTableSize uint64) (*NomsBlockStore, error) {
	cacheOnce.Do(makeGlobalCaches)

	mm := makeManifestManager(blobstoreManifest{"manifest", bs})

	p := &blobstorePersister{bs, s3BlockSize, globalIndexCache}
	return newNomsBlockStore(ctx, nbfVerStr, mm, p, inlineConjoiner{defaultMaxTables}, memTableSize)
}

func NewLocalStore(ctx context.Context, nbfVerStr string, dir string, memTableSize uint64) (*NomsBlockStore, error) {
	return newLocalStore(ctx, nbfVerStr, dir, memTableSize, defaultMaxTables)
}

func newLocalStore(ctx context.Context, nbfVerStr string, dir string, memTableSize uint64, maxTables int) (*NomsBlockStore, error) {
	cacheOnce.Do(makeGlobalCaches)
	err := checkDir(dir)

	if err != nil {
		return nil, err
	}

	m, err := getFileManifest(ctx, dir)

	if err != nil {
		return nil, err
	}

	mm := makeManifestManager(m)
	p := newFSTablePersister(dir, globalFDCache, globalIndexCache)
	nbs, err := newNomsBlockStore(ctx, nbfVerStr, mm, p, inlineConjoiner{maxTables}, memTableSize)

	if err != nil {
		return nil, err
	}

	return nbs, nil
}

func checkDir(dir string) error {
	stat, err := os.Stat(dir)
	if err != nil {
		return err
	}
	if !stat.IsDir() {
		return fmt.Errorf("path is not a directory: %s", dir)
	}
	return nil
}

func newNomsBlockStore(ctx context.Context, nbfVerStr string, mm manifestManager, p tablePersister, c conjoiner, memTableSize uint64) (*NomsBlockStore, error) {
	if memTableSize == 0 {
		memTableSize = defaultMemTableSize
	}

	nbs := &NomsBlockStore{
		mm:       mm,
		p:        p,
		c:        c,
		tables:   newTableSet(p),
		upstream: manifestContents{vers: nbfVerStr},
		mtSize:   memTableSize,
		stats:    NewStats(),
	}

	t1 := time.Now()
	defer nbs.stats.OpenLatency.SampleTimeSince(t1)

	exists, contents, err := nbs.mm.Fetch(ctx, nbs.stats)

	if err != nil {
		return nil, err
	}

	if exists {
		newTables, err := nbs.tables.Rebase(ctx, contents.specs, nbs.stats)

		if err != nil {
			return nil, err
		}

		nbs.upstream = contents
		oldTables := nbs.tables
		nbs.tables = newTables
		err = oldTables.Close()
		if err != nil {
			return nil, err
		}
	}

	return nbs, nil
}

func (nbs *NomsBlockStore) Put(ctx context.Context, c chunks.Chunk) error {
	t1 := time.Now()
	a := addr(c.Hash())
	success := nbs.addChunk(ctx, a, c.Data())

	if !success {
		return errors.New("failed to add chunk")
	}

	nbs.putCount++

	nbs.stats.PutLatency.SampleTimeSince(t1)

	return nil
}

func (nbs *NomsBlockStore) addChunk(ctx context.Context, h addr, data []byte) bool {
	nbs.mu.Lock()
	defer nbs.mu.Unlock()
	if nbs.mt == nil {
		nbs.mt = newMemTable(nbs.mtSize)
	}
	if !nbs.mt.addChunk(h, data) {
		nbs.tables = nbs.tables.Prepend(ctx, nbs.mt, nbs.stats)
		nbs.mt = newMemTable(nbs.mtSize)
		return nbs.mt.addChunk(h, data)
	}
	return true
}

func (nbs *NomsBlockStore) Get(ctx context.Context, h hash.Hash) (chunks.Chunk, error) {
	t1 := time.Now()
	defer func() {
		nbs.stats.GetLatency.SampleTimeSince(t1)
		nbs.stats.ChunksPerGet.Sample(1)
	}()

	a := addr(h)
	data, tables, err := func() ([]byte, chunkReader, error) {
		var data []byte
		nbs.mu.RLock()
		defer nbs.mu.RUnlock()
		if nbs.mt != nil {
			var err error
			data, err = nbs.mt.get(ctx, a, nbs.stats)

			if err != nil {
				return nil, nil, err
			}
		}
		return data, nbs.tables, nil
	}()

	if err != nil {
		return chunks.EmptyChunk, err
	}

	if data != nil {
		return chunks.NewChunkWithHash(h, data), nil
	}

	data, err = tables.get(ctx, a, nbs.stats)

	if err != nil {
		return chunks.EmptyChunk, err
	}

	if data != nil {
		return chunks.NewChunkWithHash(h, data), nil
	}

	return chunks.EmptyChunk, nil
}

func (nbs *NomsBlockStore) GetMany(ctx context.Context, hashes hash.HashSet, foundChunks chan<- *chunks.Chunk) error {
	return nbs.getManyWithFunc(ctx, hashes, func(ctx context.Context, cr chunkReader, reqs []getRecord, wg *sync.WaitGroup, ae *atomicerr.AtomicError, stats *Stats) bool {
		return cr.getMany(ctx, reqs, foundChunks, wg, ae, nbs.stats)
	})
}

func (nbs *NomsBlockStore) GetManyCompressed(ctx context.Context, hashes hash.HashSet, foundCmpChunks chan<- CompressedChunk) error {
	return nbs.getManyWithFunc(ctx, hashes, func(ctx context.Context, cr chunkReader, reqs []getRecord, wg *sync.WaitGroup, ae *atomicerr.AtomicError, stats *Stats) bool {
		return cr.getManyCompressed(ctx, reqs, foundCmpChunks, wg, ae, nbs.stats)
	})
}

func (nbs *NomsBlockStore) getManyWithFunc(
	ctx context.Context,
	hashes hash.HashSet,
	getManyFunc func(ctx context.Context, cr chunkReader, reqs []getRecord, wg *sync.WaitGroup, ae *atomicerr.AtomicError, stats *Stats) bool,
) error {
	t1 := time.Now()
	reqs := toGetRecords(hashes)

	defer func() {
		if len(hashes) > 0 {
			nbs.stats.GetLatency.SampleTimeSince(t1)
			nbs.stats.ChunksPerGet.Sample(uint64(len(reqs)))
		}
	}()

	ae := atomicerr.New()
	wg := &sync.WaitGroup{}

	tables, remaining := func() (tables chunkReader, remaining bool) {
		nbs.mu.RLock()
		defer nbs.mu.RUnlock()
		tables = nbs.tables
		remaining = true
		if nbs.mt != nil {
			remaining = getManyFunc(ctx, nbs.mt, reqs, nil, ae, nbs.stats)
		}

		return
	}()

	if err := ae.Get(); err != nil {
		return err
	}

	if remaining {
		getManyFunc(ctx, tables, reqs, wg, ae, nbs.stats)
		wg.Wait()
	}

	return ae.Get()
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

func (nbs *NomsBlockStore) CalcReads(hashes hash.HashSet, blockSize uint64) (reads int, split bool, err error) {
	reqs := toGetRecords(hashes)
	tables := func() (tables tableSet) {
		nbs.mu.RLock()
		defer nbs.mu.RUnlock()
		tables = nbs.tables

		return
	}()

	reads, split, remaining, err := tables.calcReads(reqs, blockSize)

	if err != nil {
		return 0, false, err
	}

	if remaining {
		return 0, false, errors.New("failed to find all chunks")
	}

	return
}

func (nbs *NomsBlockStore) Count() (uint32, error) {
	count, tables, err := func() (count uint32, tables chunkReader, err error) {
		nbs.mu.RLock()
		defer nbs.mu.RUnlock()
		if nbs.mt != nil {
			count, err = nbs.mt.count()
		}

		if err != nil {
			return 0, nil, err
		}

		return count, nbs.tables, nil
	}()

	if err != nil {
		return 0, err
	}

	tablesCount, err := tables.count()

	if err != nil {
		return 0, err
	}

	return count + tablesCount, nil
}

func (nbs *NomsBlockStore) Has(ctx context.Context, h hash.Hash) (bool, error) {
	t1 := time.Now()
	defer func() {
		nbs.stats.HasLatency.SampleTimeSince(t1)
		nbs.stats.AddressesPerHas.Sample(1)
	}()

	a := addr(h)
	has, tables, err := func() (bool, chunkReader, error) {
		nbs.mu.RLock()
		defer nbs.mu.RUnlock()

		if nbs.mt != nil {
			has, err := nbs.mt.has(a)

			if err != nil {
				return false, nil, err
			}

			return has, nbs.tables, nil
		}

		return false, nbs.tables, nil
	}()

	if err != nil {
		return false, err
	}

	if !has {
		has, err = tables.has(a)

		if err != nil {
			return false, err
		}
	}

	return has, nil
}

func (nbs *NomsBlockStore) HasMany(ctx context.Context, hashes hash.HashSet) (hash.HashSet, error) {
	t1 := time.Now()

	reqs := toHasRecords(hashes)

	tables, remaining, err := func() (tables chunkReader, remaining bool, err error) {
		nbs.mu.RLock()
		defer nbs.mu.RUnlock()
		tables = nbs.tables

		remaining = true
		if nbs.mt != nil {
			remaining, err = nbs.mt.hasMany(reqs)

			if err != nil {
				return nil, false, err
			}
		}

		return tables, remaining, nil
	}()

	if err != nil {
		return nil, err
	}

	if remaining {
		_, err := tables.hasMany(reqs)

		if err != nil {
			return nil, err
		}
	}

	if len(hashes) > 0 {
		nbs.stats.HasLatency.SampleTimeSince(t1)
		nbs.stats.AddressesPerHas.SampleLen(len(reqs))
	}

	absent := hash.HashSet{}
	for _, r := range reqs {
		if !r.has {
			absent.Insert(hash.New(r.a[:]))
		}
	}
	return absent, nil
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

func (nbs *NomsBlockStore) Rebase(ctx context.Context) error {
	nbs.mu.Lock()
	defer nbs.mu.Unlock()
	exists, contents, err := nbs.mm.Fetch(ctx, nbs.stats)

	if err != nil {
		return err
	}

	if exists {
		newTables, err := nbs.tables.Rebase(ctx, contents.specs, nbs.stats)

		if err != nil {
			return err
		}

		nbs.upstream = contents
		oldTables := nbs.tables
		nbs.tables = newTables
		err = oldTables.Close()
		if err != nil {
			return err
		}
	}

	return nil
}

func (nbs *NomsBlockStore) Root(ctx context.Context) (hash.Hash, error) {
	nbs.mu.RLock()
	defer nbs.mu.RUnlock()
	return nbs.upstream.root, nil
}

func (nbs *NomsBlockStore) Commit(ctx context.Context, current, last hash.Hash) (success bool, err error) {
	t1 := time.Now()
	defer nbs.stats.CommitLatency.SampleTimeSince(t1)

	anyPossiblyNovelChunks := func() bool {
		nbs.mu.Lock()
		defer nbs.mu.Unlock()
		return nbs.mt != nil || nbs.tables.Novel() > 0
	}

	if !anyPossiblyNovelChunks() && current == last {
		err := nbs.Rebase(ctx)

		if err != nil {
			return false, err
		}

		return true, nil
	}

	err = func() error {
		// This is unfortunate. We want to serialize commits to the same store
		// so that we avoid writing a bunch of unreachable small tables which result
		// from optismistic lock failures. However, this means that the time to
		// write tables is included in "commit" time and if all commits are
		// serialized, it means alot more waiting.
		// "non-trivial" tables are persisted here, outside of the commit-lock.
		// all other tables are persisted in updateManifest()
		nbs.mu.Lock()
		defer nbs.mu.Unlock()

		if nbs.mt != nil {
			cnt, err := nbs.mt.count()

			if err != nil {
				return err
			}

			if cnt > preflushChunkCount {
				nbs.tables = nbs.tables.Prepend(ctx, nbs.mt, nbs.stats)
				nbs.mt = nil
			}
		}

		return nil
	}()

	if err != nil {
		return false, err
	}

	nbs.mm.LockForUpdate()
	defer func() {
		unlockErr := nbs.mm.UnlockForUpdate()

		if err == nil {
			err = unlockErr
		}
	}()

	nbs.mu.Lock()
	defer nbs.mu.Unlock()
	for {
		if err := nbs.updateManifest(ctx, current, last); err == nil {
			return true, nil
		} else if err == errOptimisticLockFailedRoot || err == errLastRootMismatch {
			return false, nil
		} else if err != errOptimisticLockFailedTables {
			return false, err
		}

		// I guess this thing infinitely retries without backoff in the case off errOptimisticLockFailedTables
	}
}

var (
	errLastRootMismatch           = fmt.Errorf("last does not match nbs.Root()")
	errOptimisticLockFailedRoot   = fmt.Errorf("root moved")
	errOptimisticLockFailedTables = fmt.Errorf("tables changed")
)

// callers must acquire lock |nbs.mu|
func (nbs *NomsBlockStore) updateManifest(ctx context.Context, current, last hash.Hash) error {
	if nbs.upstream.root != last {
		return errLastRootMismatch
	}

	handleOptimisticLockFailure := func(upstream manifestContents) error {
		newTables, err := nbs.tables.Rebase(ctx, upstream.specs, nbs.stats)
		if err != nil {
			return err
		}

		nbs.upstream = upstream
		oldTables := nbs.tables
		nbs.tables = newTables
		err = oldTables.Close()

		if last != upstream.root {
			return errOptimisticLockFailedRoot
		}

		if err != nil {
			return err
		}

		return errOptimisticLockFailedTables
	}

	if cached, doomed := nbs.mm.updateWillFail(nbs.upstream.lock); doomed {
		// Pre-emptive optimistic lock failure. Someone else in-process moved to the root, the set of tables, or both out from under us.
		return handleOptimisticLockFailure(cached)
	}

	if nbs.mt != nil {
		cnt, err := nbs.mt.count()

		if err != nil {
			return err
		}

		if cnt > 0 {
			nbs.tables = nbs.tables.Prepend(ctx, nbs.mt, nbs.stats)
			nbs.mt = nil
		}
	}

	if nbs.c.ConjoinRequired(nbs.tables) {
		var err error
		newUpstream, err := nbs.c.Conjoin(ctx, nbs.upstream, nbs.mm, nbs.p, nbs.stats)

		if err != nil {
			return err
		}

		newTables, err := nbs.tables.Rebase(ctx, newUpstream.specs, nbs.stats)

		if err != nil {
			return err
		}

		nbs.upstream = newUpstream
		oldTables := nbs.tables
		nbs.tables = newTables
		err = oldTables.Close()
		if err != nil {
			return err
		}

		return errOptimisticLockFailedTables
	}

	specs, err := nbs.tables.ToSpecs()

	if err != nil {
		return err
	}

	newContents := manifestContents{
		vers:  nbs.upstream.vers,
		root:  current,
		lock:  generateLockHash(current, specs),
		gcGen: nbs.upstream.gcGen,
		specs: specs,
	}

	upstream, err := nbs.mm.Update(ctx, nbs.upstream.lock, newContents, nbs.stats, nil)
	if err != nil {
		return err
	}

	if newContents.lock != upstream.lock {
		// Optimistic lock failure. Someone else moved to the root, the set of tables, or both out from under us.
		return handleOptimisticLockFailure(upstream)
	}

	newTables, err := nbs.tables.Flatten()

	if err != nil {
		return nil
	}

	nbs.upstream = newContents
	nbs.tables = newTables

	return nil
}

func (nbs *NomsBlockStore) Version() string {
	return nbs.upstream.vers
}

func (nbs *NomsBlockStore) Close() error {
	return nbs.tables.Close()
}

func (nbs *NomsBlockStore) Stats() interface{} {
	return *nbs.stats
}

func (nbs *NomsBlockStore) StatsSummary() string {
	nbs.mu.Lock()
	defer nbs.mu.Unlock()
	cnt, _ := nbs.tables.count()
	physLen, _ := nbs.tables.physicalLen()
	return fmt.Sprintf("Root: %s; Chunk Count %d; Physical Bytes %s", nbs.upstream.root, cnt, humanize.Bytes(physLen))
}

// tableFile is our implementation of TableFile.
type tableFile struct {
	info TableSpecInfo
	open func(ctx context.Context) (io.ReadCloser, error)
}

// FileID gets the id of the file
func (tf tableFile) FileID() string {
	return tf.info.GetName()
}

// NumChunks returns the number of chunks in a table file
func (tf tableFile) NumChunks() int {
	return int(tf.info.GetChunkCount())
}

// Open returns an io.ReadCloser which can be used to read the bytes of a table file.
func (tf tableFile) Open(ctx context.Context) (io.ReadCloser, error) {
	return tf.open(ctx)
}

// Sources retrieves the current root hash, and a list of all the table files
func (nbs *NomsBlockStore) Sources(ctx context.Context) (hash.Hash, []TableFile, error) {
	nbs.mu.Lock()
	defer nbs.mu.Unlock()

	stats := &Stats{}
	exists, contents, err := nbs.mm.m.ParseIfExists(ctx, stats, nil)

	if err != nil {
		return hash.Hash{}, nil, err
	}

	if !exists {
		return hash.Hash{}, nil, nil
	}

	css, err := nbs.chunkSourcesByAddr()
	if err != nil {
		return hash.Hash{}, nil, err
	}

	numSpecs := contents.NumTableSpecs()

	var tableFiles []TableFile
	for i := 0; i < numSpecs; i++ {
		info := contents.getSpec(i)
		cs, ok := css[info.name]
		if !ok {
			return hash.Hash{}, nil, errors.New("manifest referenced table file for which there is no chunkSource.")
		}
		tf := tableFile{
			info: info,
			open: func(ctx context.Context) (io.ReadCloser, error) {
				r, err := cs.reader(ctx)
				if err != nil {
					return nil, err
				}

				return ioutil.NopCloser(r), nil
			},
		}
		tableFiles = append(tableFiles, tf)
	}

	return contents.GetRoot(), tableFiles, nil
}

func (nbs *NomsBlockStore) Size(ctx context.Context) (uint64, error) {
	nbs.mu.Lock()
	defer nbs.mu.Unlock()

	stats := &Stats{}
	exists, contents, err := nbs.mm.m.ParseIfExists(ctx, stats, nil)

	if err != nil {
		return uint64(0), err
	}

	if !exists {
		return uint64(0), nil
	}

	css, err := nbs.chunkSourcesByAddr()
	if err != nil {
		return uint64(0), err
	}

	numSpecs := contents.NumTableSpecs()

	size := uint64(0)
	for i := 0; i < numSpecs; i++ {
		info := contents.getSpec(i)
		cs, ok := css[info.name]
		if !ok {
			return uint64(0), errors.New("manifest referenced table file for which there is no chunkSource.")
		}
		ti, err := cs.index()
		if err != nil {
			return uint64(0), fmt.Errorf("error getting table file index for chunkSource. %w", err)
		}
		size += ti.TableFileSize()
	}
	return size, nil
}

func (nbs *NomsBlockStore) chunkSourcesByAddr() (map[addr]chunkSource, error) {
	css := make(map[addr]chunkSource, len(nbs.tables.upstream)+len(nbs.tables.novel))
	for _, cs := range nbs.tables.upstream {
		a, err := cs.hash()
		if err != nil {
			return nil, err
		}
		css[a] = cs
	}
	for _, cs := range nbs.tables.novel {
		a, err := cs.hash()
		if err != nil {
			return nil, err
		}
		css[a] = cs
	}
	return css, nil

}

func (nbs *NomsBlockStore) SupportedOperations() TableFileStoreOps {
	_, ok := nbs.p.(*fsTablePersister)
	return TableFileStoreOps{
		CanRead:  true,
		CanWrite: ok,
		CanPrune: ok,
		CanGC:    ok,
	}
}

// WriteTableFile will read a table file from the provided reader and write it to the TableFileStore
func (nbs *NomsBlockStore) WriteTableFile(ctx context.Context, fileId string, numChunks int, rd io.Reader, contentLength uint64, contentHash []byte) error {
	fsPersister, ok := nbs.p.(*fsTablePersister)

	if !ok {
		return errors.New("Not implemented")
	}

	path := filepath.Join(fsPersister.dir, fileId)

	err := func() (err error) {
		var f *os.File
		f, err = os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, os.ModePerm)

		if err != nil {
			return err
		}

		defer func() {
			closeErr := f.Close()

			if err == nil {
				err = closeErr
			}
		}()

		_, err = io.Copy(f, rd)

		return err
	}()

	if err != nil {
		return err
	}

	fileIdHash, ok := hash.MaybeParse(fileId)

	if !ok {
		return errors.New("invalid base32 encoded hash: " + fileId)
	}

	_, err = nbs.UpdateManifest(ctx, map[hash.Hash]uint32{fileIdHash: uint32(numChunks)})

	return err
}

// PruneTableFiles deletes old table files that are no longer referenced in the manifest.
func (nbs *NomsBlockStore) PruneTableFiles(ctx context.Context) (err error) {
	nbs.mu.Lock()
	defer nbs.mu.Unlock()

	nbs.mm.LockForUpdate()
	defer func() {
		unlockErr := nbs.mm.UnlockForUpdate()

		if err == nil {
			err = unlockErr
		}
	}()

	for {
		// flush all tables and update manifest
		err = nbs.updateManifest(ctx, nbs.upstream.root, nbs.upstream.root)

		if err == nil {
			break
		} else if err == errOptimisticLockFailedTables {
			continue
		} else {
			return err
		}

		// Same behavior as Commit
		// infinitely retries without backoff in the case off errOptimisticLockFailedTables
	}

	ok, contents, err := nbs.mm.Fetch(ctx, &Stats{})
	if err != nil {
		return err
	}
	if !ok {
		return nil // no manifest exists
	}

	return nbs.p.PruneTableFiles(ctx, contents)
}

func (nbs *NomsBlockStore) MarkAndSweepChunks(ctx context.Context, last hash.Hash, keepChunks <-chan hash.Hash, errChan chan<- error) (err error) {
	//todo: error chan closing

	ops := nbs.SupportedOperations()
	if !ops.CanGC || !ops.CanPrune {
		close(errChan)
		return chunks.ErrUnsupportedOperation
	}

	if nbs.upstream.root != last {
		close(errChan)
		return errLastRootMismatch
	}

	// todo: acquire manifest lock

	nbs.mu.RLock()
	drainAndClose := func() {
		defer nbs.mu.RUnlock()
		defer close(errChan)

		for range keepChunks {
			// drain the channel
		}

		err := nbs.gcUnlock(ctx)

		if err != nil {
			errChan <- err
		}
	}

	go func() {
		defer drainAndClose()

		specs, err := nbs.copyMarkedChunks(ctx, keepChunks)

		if err != nil {
			errChan <- err
			return
		}

		err = nbs.swapTables(ctx, specs)

		if err != nil {
			errChan <- err
			return
		}

		ok, contents, err := nbs.mm.Fetch(ctx, &Stats{})
		if err != nil {
			errChan <- err
			return
		}
		if !ok {
			panic("no manifest")
		}

		err = nbs.p.PruneTableFiles(ctx, contents)

		if err != nil {
			errChan <- err
			return
		}
	}()

	return nil
}

func (nbs *NomsBlockStore) gcLock(ctx context.Context) error {
	return nil
}

func (nbs *NomsBlockStore) gcUnlock(ctx context.Context) error {
	return nil
}

func (nbs *NomsBlockStore) copyMarkedChunks(ctx context.Context, keepChunks <-chan hash.Hash) ([]tableSpec, error) {
	s, err := nbs.copyTableSize()

	if err != nil {
		return nil, err
	}

	gcc, err := newGarbageCollectionCopier(s)

	if err != nil {
		return nil, err
	}

	var h hash.Hash
	ok := true
	for ok {
		select {
		case h, ok = <-keepChunks:
			if !ok {
				break
			}

			// todo: batch calls to nbs.GetMany()
			c, err := nbs.Get(ctx, h)

			if err != nil {
				return nil, err
			}

			gcc.addChunk(ctx, addr(h), c.Data())
		}
	}

	nomsDir := nbs.p.(*fsTablePersister).dir

	return gcc.copyTablesToDir(ctx, nomsDir)
}

// todo: what's the optimal table size to copy to?
func (nbs *NomsBlockStore) copyTableSize() (uint64, error) {
	total, err := nbs.tables.physicalLen()

	if err != nil {
		return 0, err
	}

	avgTableSize := total / uint64(nbs.tables.Upstream()+nbs.tables.Novel()+1)

	// max(avgTableSize, defaultMemTableSize)
	if avgTableSize > nbs.mtSize {
		return avgTableSize, nil
	}
	return nbs.mtSize, nil
}

func (nbs *NomsBlockStore) swapTables(ctx context.Context, specs []tableSpec) error {
	newContents := manifestContents{
		vers:  nbs.upstream.vers,
		root:  nbs.upstream.root,
		lock:  generateLockHash(nbs.upstream.root, specs),
		specs: specs,
	}

	// todo: lock outside of Update
	upstream, err := nbs.mm.Update(ctx, nbs.upstream.lock, newContents, nbs.stats, nil)
	if err != nil {
		return err
	}
	if newContents.lock != upstream.lock {
		panic("manifest was changed outside of the LOCK")
	}

	// clear memTable
	nbs.mt = newMemTable(nbs.mtSize)

	// clear nbs.tables.novel
	nbs.tables, err = nbs.tables.Flatten()

	if err != nil {
		return nil
	}

	// replace nbs.tables.upstream with gc compacted tables
	nbs.upstream = upstream
	nbs.tables, err = nbs.tables.Rebase(ctx, specs, nbs.stats)

	if err != nil {
		return nil
	}

	return nil
}

// SetRootChunk changes the root chunk hash from the previous value to the new root.
func (nbs *NomsBlockStore) SetRootChunk(ctx context.Context, root, previous hash.Hash) error {
	nbs.mu.Lock()
	defer nbs.mu.Unlock()
	for {
		err := nbs.updateManifest(ctx, root, previous)

		if err == nil {
			return nil
		} else if err == errOptimisticLockFailedTables {
			continue
		} else {
			return err
		}

		// Same behavior as Commit
		// I guess this thing infinitely retries without backoff in the case off errOptimisticLockFailedTables
	}
}
