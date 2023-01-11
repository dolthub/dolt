// Copyright 2019 Dolthub, Inc.
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
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/storage"
	"github.com/dustin/go-humanize"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/libraries/utils/file"
	"github.com/dolthub/dolt/go/store/blobstore"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/util/tempfiles"
)

var (
	ErrFetchFailure                           = errors.New("fetch failed")
	ErrSpecWithoutChunkSource                 = errors.New("manifest referenced table file for which there is no chunkSource.")
	ErrConcurrentManifestWriteDuringOverwrite = errors.New("concurrent manifest write during manifest overwrite")
)

// The root of a Noms Chunk Store is stored in a 'manifest', along with the
// names of the tables that hold all the chunks in the store. The number of
// chunks in each table is also stored in the manifest.

const (
	// StorageVersion is the version of the on-disk Noms Chunks Store data format.
	StorageVersion = "5"

	defaultMemTableSize uint64 = (1 << 20) * 128 // 128MB
	defaultMaxTables           = 256

	defaultManifestCacheSize = 1 << 23 // 8MB
	preflushChunkCount       = 8
)

var (
	cacheOnce           = sync.Once{}
	makeManifestManager func(manifest) manifestManager
	globalFDCache       *fdCache
)

var tracer = otel.Tracer("github.com/dolthub/dolt/go/store/nbs")

func makeGlobalCaches() {
	globalFDCache = newFDCache(defaultMaxTables)

	manifestCache := newManifestCache(defaultManifestCacheSize)
	manifestLocks := newManifestLocks()
	makeManifestManager = func(m manifest) manifestManager { return manifestManager{m, manifestCache, manifestLocks} }
}

type NBSCompressedChunkStore interface {
	chunks.ChunkStore
	GetManyCompressed(context.Context, hash.HashSet, func(context.Context, CompressedChunk)) error
}

type NomsBlockStore struct {
	mm manifestManager
	p  tablePersister
	c  conjoinStrategy

	mu       sync.RWMutex // protects the following state
	mt       *memTable
	tables   tableSet
	upstream manifestContents

	mtSize   uint64
	putCount uint64

	stats *Stats
}

var _ TableFileStore = &NomsBlockStore{}
var _ chunks.ChunkStoreGarbageCollector = &NomsBlockStore{}

type Range struct {
	Offset uint64
	Length uint32
}

func (nbs *NomsBlockStore) GetChunkLocationsWithPaths(hashes hash.HashSet) (map[string]map[hash.Hash]Range, error) {
	locs, err := nbs.GetChunkLocations(hashes)
	if err != nil {
		return nil, err
	}
	toret := make(map[string]map[hash.Hash]Range, len(locs))
	for k, v := range locs {
		toret[k.String()] = v
	}
	return toret, nil
}

func (nbs *NomsBlockStore) GetChunkLocations(hashes hash.HashSet) (map[hash.Hash]map[hash.Hash]Range, error) {
	gr := toGetRecords(hashes)
	ranges := make(map[hash.Hash]map[hash.Hash]Range)

	fn := func(css chunkSourceSet) error {
		for _, cs := range css {
			rng, err := cs.getRecordRanges(gr)
			if err != nil {
				return err
			}

			h := hash.Hash(cs.hash())
			if m, ok := ranges[h]; ok {
				for k, v := range rng {
					m[k] = v
				}
			} else {
				ranges[h] = rng
			}
		}
		return nil
	}

	tables := func() tableSet {
		nbs.mu.RLock()
		defer nbs.mu.RUnlock()
		return nbs.tables
	}()

	if err := fn(tables.upstream); err != nil {
		return nil, err
	}
	if err := fn(tables.novel); err != nil {
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

	var updatedContents manifestContents
	for {
		ok, contents, _, ferr := nbs.mm.Fetch(ctx, nbs.stats)
		if ferr != nil {
			return manifestContents{}, ferr
		} else if !ok {
			contents = manifestContents{nbfVers: nbs.upstream.nbfVers}
		}

		originalLock := contents.lock

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

		contents.lock = generateLockHash(contents.root, contents.specs, contents.appendix)

		// ensure we don't drop existing appendices
		if contents.appendix != nil && len(contents.appendix) > 0 {
			contents, err = fromManifestAppendixOptionNewContents(contents, contents.appendix, ManifestAppendixOption_Set)
			if err != nil {
				return manifestContents{}, err
			}
		}

		err = nbs.tables.checkAllTablesExist(ctx, contents.specs, nbs.stats)
		if err != nil {
			return manifestContents{}, err
		}

		updatedContents, err = nbs.mm.Update(ctx, originalLock, contents, nbs.stats, nil)
		if err != nil {
			return manifestContents{}, err
		}

		if updatedContents.lock == contents.lock {
			break
		}
	}

	newTables, err := nbs.tables.rebase(ctx, updatedContents.specs, nbs.stats)
	if err != nil {
		return manifestContents{}, err
	}

	nbs.upstream = updatedContents
	oldTables := nbs.tables
	nbs.tables = newTables
	err = oldTables.close()
	if err != nil {
		return manifestContents{}, err
	}

	return updatedContents, nil
}

func (nbs *NomsBlockStore) UpdateManifestWithAppendix(ctx context.Context, updates map[hash.Hash]uint32, option ManifestAppendixOption) (mi ManifestInfo, err error) {
	nbs.mm.LockForUpdate()
	defer func() {
		unlockErr := nbs.mm.UnlockForUpdate()

		if err == nil {
			err = unlockErr
		}
	}()

	nbs.mu.Lock()
	defer nbs.mu.Unlock()

	var updatedContents manifestContents
	for {
		ok, contents, _, ferr := nbs.mm.Fetch(ctx, nbs.stats)

		if ferr != nil {
			return manifestContents{}, ferr
		} else if !ok {
			contents = manifestContents{nbfVers: nbs.upstream.nbfVers}
		}

		originalLock := contents.lock

		currAppendixSpecs := contents.getAppendixSet()

		appendixSpecs := make([]tableSpec, 0)
		var addCount int
		for h, count := range updates {
			a := addr(h)

			if option == ManifestAppendixOption_Set {
				appendixSpecs = append(appendixSpecs, tableSpec{a, count})
			} else {
				if _, ok := currAppendixSpecs[a]; !ok {
					addCount++
					appendixSpecs = append(appendixSpecs, tableSpec{a, count})
				}
			}
		}

		if addCount == 0 && option != ManifestAppendixOption_Set {
			return contents, nil
		}

		contents, err = fromManifestAppendixOptionNewContents(contents, appendixSpecs, option)
		if err != nil {
			return manifestContents{}, err
		}

		err = nbs.tables.checkAllTablesExist(ctx, contents.specs, nbs.stats)
		if err != nil {
			return manifestContents{}, err
		}

		updatedContents, err = nbs.mm.Update(ctx, originalLock, contents, nbs.stats, nil)
		if err != nil {
			return manifestContents{}, err
		}

		if updatedContents.lock == contents.lock {
			break
		}
	}

	newTables, err := nbs.tables.rebase(ctx, updatedContents.specs, nbs.stats)
	if err != nil {
		return manifestContents{}, err
	}

	nbs.upstream = updatedContents
	oldTables := nbs.tables
	nbs.tables = newTables
	err = oldTables.close()
	if err != nil {
		return manifestContents{}, err
	}
	return updatedContents, nil
}

func fromManifestAppendixOptionNewContents(upstream manifestContents, appendixSpecs []tableSpec, option ManifestAppendixOption) (manifestContents, error) {
	contents, upstreamAppendixSpecs := upstream.removeAppendixSpecs()
	switch option {
	case ManifestAppendixOption_Append:
		// append all appendix specs to contents.specs
		specs := append([]tableSpec{}, appendixSpecs...)
		specs = append(specs, upstreamAppendixSpecs...)
		contents.specs = append(specs, contents.specs...)

		// append all appendix specs to contents.appendix
		newAppendixSpecs := append([]tableSpec{}, upstreamAppendixSpecs...)
		contents.appendix = append(newAppendixSpecs, appendixSpecs...)

		contents.lock = generateLockHash(contents.root, contents.specs, contents.appendix)
		return contents, nil
	case ManifestAppendixOption_Set:
		if len(appendixSpecs) < 1 {
			return contents, nil
		}

		// append new appendix specs to contents.specs
		// dropping all upstream appendix specs
		specs := append([]tableSpec{}, appendixSpecs...)
		contents.specs = append(specs, contents.specs...)

		// append new appendix specs to contents.appendix
		contents.appendix = append([]tableSpec{}, appendixSpecs...)

		contents.lock = generateLockHash(contents.root, contents.specs, contents.appendix)
		return contents, nil
	default:
		return manifestContents{}, ErrUnsupportedManifestAppendixOption
	}
}

// OverwriteStoreManifest is a low level interface to completely replace the manifest contents
// of |store| with the supplied |root|, |tableFiles| and |appendixTableFiles|. It performs concurrency
// control on the existing |store| manifest, and can fail with |ErrConcurrentManifestWriteDuringOverwrite|
// if the |store|'s view is stale. If contents should be unconditionally replaced without regard for the existing
// contents, run this in a loop, rebasing |store| after each failure.
//
// Regardless of success or failure, |OverwriteStoreManifest| does *not* Rebase the |store|. The persisted
// manifest contents will have been updated, but nothing about the in-memory view of the |store| will reflect
// those updates. If |store| is Rebase'd, then the new upstream contents will be picked up.
//
// Extreme care should be taken when updating manifest contents through this interface. Logic typically
// assumes that stores grow monotonically unless the |gcGen| of a manifest changes. Since this interface
// cannot set |gcGen|, callers must ensure that calls to this function grow the store monotonically.
func OverwriteStoreManifest(ctx context.Context, store *NomsBlockStore, root hash.Hash, tableFiles map[hash.Hash]uint32, appendixTableFiles map[hash.Hash]uint32) (err error) {
	contents := manifestContents{
		root:    root,
		nbfVers: store.upstream.nbfVers,
	}
	// Appendix table files should come first in specs
	for h, c := range appendixTableFiles {
		s := tableSpec{name: addr(h), chunkCount: c}
		contents.appendix = append(contents.appendix, s)
		contents.specs = append(contents.specs, s)
	}
	for h, c := range tableFiles {
		s := tableSpec{name: addr(h), chunkCount: c}
		contents.specs = append(contents.specs, s)
	}
	contents.lock = generateLockHash(contents.root, contents.specs, contents.appendix)

	store.mm.LockForUpdate()
	defer func() {
		unlockErr := store.mm.UnlockForUpdate()

		if err == nil {
			err = unlockErr
		}
	}()
	store.mu.Lock()
	defer store.mu.Unlock()
	updatedContents, err := store.mm.Update(ctx, store.upstream.lock, contents, store.stats, nil)
	if err != nil {
		return err
	}
	if updatedContents.lock != contents.lock {
		return ErrConcurrentManifestWriteDuringOverwrite
	}
	// We don't update |nbs.upstream| here since the tables have not been rebased
	return nil
}

func NewAWSStoreWithMMapIndex(ctx context.Context, nbfVerStr string, table, ns, bucket string, s3 s3svc, ddb ddbsvc, memTableSize uint64, q MemoryQuotaProvider) (*NomsBlockStore, error) {
	cacheOnce.Do(makeGlobalCaches)
	readRateLimiter := make(chan struct{}, 32)
	p := &awsTablePersister{
		s3,
		bucket,
		readRateLimiter,
		&ddbTableStore{ddb, table, readRateLimiter, nil},
		awsLimits{defaultS3PartSize, minS3PartSize, maxS3PartSize, maxDynamoItemSize, maxDynamoChunks},
		ns,
		q,
	}
	mm := makeManifestManager(newDynamoManifest(table, ns, ddb))
	return newNomsBlockStore(ctx, nbfVerStr, mm, p, q, inlineConjoiner{defaultMaxTables}, memTableSize)
}

func NewAWSStore(ctx context.Context, nbfVerStr string, table, ns, bucket string, s3 s3svc, ddb ddbsvc, memTableSize uint64, q MemoryQuotaProvider) (*NomsBlockStore, error) {
	cacheOnce.Do(makeGlobalCaches)
	readRateLimiter := make(chan struct{}, 32)
	p := &awsTablePersister{
		s3,
		bucket,
		readRateLimiter,
		&ddbTableStore{ddb, table, readRateLimiter, nil},
		awsLimits{defaultS3PartSize, minS3PartSize, maxS3PartSize, maxDynamoItemSize, maxDynamoChunks},
		ns,
		q,
	}
	mm := makeManifestManager(newDynamoManifest(table, ns, ddb))
	return newNomsBlockStore(ctx, nbfVerStr, mm, p, q, inlineConjoiner{defaultMaxTables}, memTableSize)
}

// NewGCSStore returns an nbs implementation backed by a GCSBlobstore
func NewGCSStore(ctx context.Context, nbfVerStr string, bucketName, path string, gcs *storage.Client, memTableSize uint64, q MemoryQuotaProvider) (*NomsBlockStore, error) {
	cacheOnce.Do(makeGlobalCaches)

	bs := blobstore.NewGCSBlobstore(gcs, bucketName, path)
	return NewBSStore(ctx, nbfVerStr, bs, memTableSize, q)
}

// NewBSStore returns an nbs implementation backed by a Blobstore
func NewBSStore(ctx context.Context, nbfVerStr string, bs blobstore.Blobstore, memTableSize uint64, q MemoryQuotaProvider) (*NomsBlockStore, error) {
	cacheOnce.Do(makeGlobalCaches)

	mm := makeManifestManager(blobstoreManifest{bs})

	p := &blobstorePersister{bs, s3BlockSize, q}
	return newNomsBlockStore(ctx, nbfVerStr, mm, p, q, noopConjoiner{}, memTableSize)
}

func NewLocalStore(ctx context.Context, nbfVerStr string, dir string, memTableSize uint64, q MemoryQuotaProvider) (*NomsBlockStore, error) {
	return newLocalStore(ctx, nbfVerStr, dir, memTableSize, defaultMaxTables, q)
}

func newLocalStore(ctx context.Context, nbfVerStr string, dir string, memTableSize uint64, maxTables int, q MemoryQuotaProvider) (*NomsBlockStore, error) {
	cacheOnce.Do(makeGlobalCaches)
	if err := checkDir(dir); err != nil {
		return nil, err
	}

	m, err := getFileManifest(ctx, dir, asyncFlush)
	if err != nil {
		return nil, err
	}
	p := newFSTablePersister(dir, globalFDCache, q)
	c := conjoinStrategy(inlineConjoiner{maxTables})

	return newNomsBlockStore(ctx, nbfVerStr, makeManifestManager(m), p, q, c, memTableSize)
}

func NewLocalJournalingStore(ctx context.Context, nbfVers, dir string, q MemoryQuotaProvider) (*NomsBlockStore, error) {
	cacheOnce.Do(makeGlobalCaches)
	if err := checkDir(dir); err != nil {
		return nil, err
	}

	m, err := getFileManifest(ctx, dir, syncFlush)
	if err != nil {
		return nil, err
	}
	p := newFSTablePersister(dir, globalFDCache, q)

	journal, err := newChunkJournal(ctx, nbfVers, dir, m, p.(*fsTablePersister))
	if err != nil {
		return nil, err
	}

	mm := makeManifestManager(journal)
	c := journalConjoiner{child: inlineConjoiner{defaultMaxTables}}

	// |journal| serves as the manifest and tablePersister
	return newNomsBlockStore(ctx, nbfVers, mm, journal, q, c, defaultMemTableSize)
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

func newNomsBlockStore(ctx context.Context, nbfVerStr string, mm manifestManager, p tablePersister, q MemoryQuotaProvider, c conjoinStrategy, memTableSize uint64) (*NomsBlockStore, error) {
	if memTableSize == 0 {
		memTableSize = defaultMemTableSize
	}

	nbs := &NomsBlockStore{
		mm:       mm,
		p:        p,
		c:        c,
		tables:   newTableSet(p, q),
		upstream: manifestContents{nbfVers: nbfVerStr},
		mtSize:   memTableSize,
		stats:    NewStats(),
	}

	t1 := time.Now()
	defer nbs.stats.OpenLatency.SampleTimeSince(t1)

	exists, contents, _, err := nbs.mm.Fetch(ctx, nbs.stats)

	if err != nil {
		return nil, err
	}

	if exists {
		newTables, err := nbs.tables.rebase(ctx, contents.specs, nbs.stats)

		if err != nil {
			return nil, err
		}

		nbs.upstream = contents
		oldTables := nbs.tables
		nbs.tables = newTables
		err = oldTables.close()
		if err != nil {
			return nil, err
		}
	}

	return nbs, nil
}

// WithoutConjoiner returns a new *NomsBlockStore instance that will not
// conjoin table files during manifest updates. Used in some server-side
// contexts when things like table file maintenance is done out-of-process. Not
// safe for use outside of NomsBlockStore construction.
func (nbs *NomsBlockStore) WithoutConjoiner() *NomsBlockStore {
	return &NomsBlockStore{
		mm:       nbs.mm,
		p:        nbs.p,
		c:        noopConjoiner{},
		mu:       sync.RWMutex{},
		mt:       nbs.mt,
		tables:   nbs.tables,
		upstream: nbs.upstream,
		mtSize:   nbs.mtSize,
		putCount: nbs.putCount,
		stats:    nbs.stats,
	}
}

func (nbs *NomsBlockStore) Put(ctx context.Context, c chunks.Chunk) error {
	t1 := time.Now()
	a := addr(c.Hash())
	success, err := nbs.addChunk(ctx, a, c.Data())
	if err != nil {
		return err
	} else if !success {
		return errors.New("failed to add chunk")
	}
	atomic.AddUint64(&nbs.putCount, 1)

	nbs.stats.PutLatency.SampleTimeSince(t1)

	return nil
}

func (nbs *NomsBlockStore) addChunk(ctx context.Context, h addr, data []byte) (bool, error) {
	nbs.mu.Lock()
	defer nbs.mu.Unlock()
	if nbs.mt == nil {
		nbs.mt = newMemTable(nbs.mtSize)
	}
	if !nbs.mt.addChunk(h, data) {
		ts, err := nbs.tables.append(ctx, nbs.mt, nbs.stats)
		if err != nil {
			return false, err
		}
		nbs.tables = ts
		nbs.mt = newMemTable(nbs.mtSize)
		return nbs.mt.addChunk(h, data), nil
	}
	return true, nil
}

func (nbs *NomsBlockStore) Get(ctx context.Context, h hash.Hash) (chunks.Chunk, error) {
	ctx, span := tracer.Start(ctx, "nbs.Get")
	defer span.End()

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

func (nbs *NomsBlockStore) GetMany(ctx context.Context, hashes hash.HashSet, found func(context.Context, *chunks.Chunk)) error {
	ctx, span := tracer.Start(ctx, "nbs.GetMany", trace.WithAttributes(attribute.Int("num_hashes", len(hashes))))
	span.End()
	return nbs.getManyWithFunc(ctx, hashes, func(ctx context.Context, cr chunkReader, eg *errgroup.Group, reqs []getRecord, stats *Stats) (bool, error) {
		return cr.getMany(ctx, eg, reqs, found, nbs.stats)
	})
}

func (nbs *NomsBlockStore) GetManyCompressed(ctx context.Context, hashes hash.HashSet, found func(context.Context, CompressedChunk)) error {
	ctx, span := tracer.Start(ctx, "nbs.GetManyCompressed", trace.WithAttributes(attribute.Int("num_hashes", len(hashes))))
	defer span.End()
	return nbs.getManyWithFunc(ctx, hashes, func(ctx context.Context, cr chunkReader, eg *errgroup.Group, reqs []getRecord, stats *Stats) (bool, error) {
		return cr.getManyCompressed(ctx, eg, reqs, found, nbs.stats)
	})
}

func (nbs *NomsBlockStore) getManyWithFunc(
	ctx context.Context,
	hashes hash.HashSet,
	getManyFunc func(ctx context.Context, cr chunkReader, eg *errgroup.Group, reqs []getRecord, stats *Stats) (bool, error),
) error {
	t1 := time.Now()
	reqs := toGetRecords(hashes)

	defer func() {
		if len(hashes) > 0 {
			nbs.stats.GetLatency.SampleTimeSince(t1)
			nbs.stats.ChunksPerGet.Sample(uint64(len(reqs)))
		}
	}()

	eg, ctx := errgroup.WithContext(ctx)

	tables, remaining, err := func() (tables chunkReader, remaining bool, err error) {
		nbs.mu.RLock()
		defer nbs.mu.RUnlock()
		tables = nbs.tables
		remaining = true
		if nbs.mt != nil {
			remaining, err = getManyFunc(ctx, nbs.mt, eg, reqs, nbs.stats)
		}
		return
	}()
	if err != nil {
		return err
	}

	if remaining {
		_, err = getManyFunc(ctx, tables, eg, reqs, nbs.stats)
	}

	if err != nil {
		eg.Wait()
		return err
	}
	return eg.Wait()
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
	exists, contents, _, err := nbs.mm.Fetch(ctx, nbs.stats)
	if err != nil {
		return err
	}

	if exists {
		if contents.lock == nbs.upstream.lock {
			// short-circuit if manifest is unchanged
			return nil
		}

		newTables, err := nbs.tables.rebase(ctx, contents.specs, nbs.stats)
		if err != nil {
			return err
		}

		nbs.upstream = contents
		oldTables := nbs.tables
		nbs.tables = newTables
		err = oldTables.close()
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
		return nbs.mt != nil || len(nbs.tables.novel) > 0
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
		// from optimistic lock failures. However, this means that the time to
		// write tables is included in "commit" time and if all commits are
		// serialized, it means a lot more waiting.
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
				ts, err := nbs.tables.append(ctx, nbs.mt, nbs.stats)
				if err != nil {
					return err
				}
				nbs.tables, nbs.mt = ts, nil
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
		newTables, err := nbs.tables.rebase(ctx, upstream.specs, nbs.stats)
		if err != nil {
			return err
		}

		nbs.upstream = upstream
		oldTables := nbs.tables
		nbs.tables = newTables
		err = oldTables.close()

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
			ts, err := nbs.tables.append(ctx, nbs.mt, nbs.stats)
			if err != nil {
				return err
			}
			nbs.tables, nbs.mt = ts, nil
		}
	}

	if nbs.c.conjoinRequired(nbs.tables) {
		newUpstream, err := conjoin(ctx, nbs.c, nbs.upstream, nbs.mm, nbs.p, nbs.stats)
		if err != nil {
			return err
		}

		newTables, err := nbs.tables.rebase(ctx, newUpstream.specs, nbs.stats)
		if err != nil {
			return err
		}

		nbs.upstream = newUpstream
		oldTables := nbs.tables
		nbs.tables = newTables
		err = oldTables.close()
		if err != nil {
			return err
		}
		return errOptimisticLockFailedTables
	}

	specs, err := nbs.tables.toSpecs()
	if err != nil {
		return err
	}

	// ensure we don't drop appendices on commit
	var appendixSpecs []tableSpec
	if nbs.upstream.appendix != nil && len(nbs.upstream.appendix) > 0 {
		appendixSet := nbs.upstream.getAppendixSet()

		filtered := make([]tableSpec, 0, len(specs))
		for _, s := range specs {
			if _, present := appendixSet[s.name]; !present {
				filtered = append(filtered, s)
			}
		}

		_, appendixSpecs = nbs.upstream.removeAppendixSpecs()
		prepended := append([]tableSpec{}, appendixSpecs...)
		specs = append(prepended, filtered...)
	}

	newContents := manifestContents{
		nbfVers:  nbs.upstream.nbfVers,
		root:     current,
		lock:     generateLockHash(current, specs, appendixSpecs),
		gcGen:    nbs.upstream.gcGen,
		specs:    specs,
		appendix: appendixSpecs,
	}

	upstream, err := nbs.mm.Update(ctx, nbs.upstream.lock, newContents, nbs.stats, nil)
	if err != nil {
		return err
	}

	if newContents.lock != upstream.lock {
		// Optimistic lock failure. Someone else moved to the root, the set of tables, or both out from under us.
		return handleOptimisticLockFailure(upstream)
	}

	newTables, err := nbs.tables.flatten(ctx)

	if err != nil {
		return nil
	}

	nbs.upstream = newContents
	nbs.tables = newTables

	return nil
}

func (nbs *NomsBlockStore) Version() string {
	nbs.mu.RLock()
	defer nbs.mu.RUnlock()
	return nbs.upstream.nbfVers
}

func (nbs *NomsBlockStore) Close() (err error) {
	if cerr := nbs.p.Close(); cerr != nil {
		err = cerr
	}
	if cerr := nbs.tables.close(); cerr != nil {
		err = cerr
	}
	if cerr := nbs.mm.Close(); cerr != nil {
		err = cerr
	}
	return
}

func (nbs *NomsBlockStore) Stats() interface{} {
	return nbs.stats.Clone()
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
	open func(ctx context.Context) (io.ReadCloser, uint64, error)
}

// FileID gets the id of the file
func (tf tableFile) FileID() string {
	return tf.info.GetName()
}

// NumChunks returns the number of chunks in a table file
func (tf tableFile) NumChunks() int {
	return int(tf.info.GetChunkCount())
}

// Open returns an io.ReadCloser which can be used to read the bytes of a table file and the content length in bytes.
func (tf tableFile) Open(ctx context.Context) (io.ReadCloser, uint64, error) {
	return tf.open(ctx)
}

// Sources retrieves the current root hash, a list of all table files (which may include appendix tablefiles),
// and a second list of only the appendix table files
func (nbs *NomsBlockStore) Sources(ctx context.Context) (hash.Hash, []TableFile, []TableFile, error) {
	nbs.mu.Lock()
	defer nbs.mu.Unlock()

	exists, contents, err := nbs.mm.m.ParseIfExists(ctx, nbs.stats, nil)

	if err != nil {
		return hash.Hash{}, nil, nil, err
	}

	if !exists {
		return hash.Hash{}, nil, nil, nil
	}

	css, err := nbs.chunkSourcesByAddr()
	if err != nil {
		return hash.Hash{}, nil, nil, err
	}

	appendixTableFiles, err := getTableFiles(css, contents, contents.NumAppendixSpecs(), func(mc manifestContents, idx int) tableSpec {
		return mc.getAppendixSpec(idx)
	})
	if err != nil {
		return hash.Hash{}, nil, nil, err
	}

	allTableFiles, err := getTableFiles(css, contents, contents.NumTableSpecs(), func(mc manifestContents, idx int) tableSpec {
		return mc.getSpec(idx)
	})
	if err != nil {
		return hash.Hash{}, nil, nil, err
	}

	return contents.GetRoot(), allTableFiles, appendixTableFiles, nil
}

func getTableFiles(css map[addr]chunkSource, contents manifestContents, numSpecs int, specFunc func(mc manifestContents, idx int) tableSpec) ([]TableFile, error) {
	tableFiles := make([]TableFile, 0)
	if numSpecs == 0 {
		return tableFiles, nil
	}
	for i := 0; i < numSpecs; i++ {
		info := specFunc(contents, i)
		cs, ok := css[info.name]
		if !ok {
			return nil, ErrSpecWithoutChunkSource
		}
		tableFiles = append(tableFiles, newTableFile(cs, info))
	}
	return tableFiles, nil
}

func newTableFile(cs chunkSource, info tableSpec) tableFile {
	return tableFile{
		info: info,
		open: func(ctx context.Context) (io.ReadCloser, uint64, error) {
			r, s, err := cs.reader(ctx)
			if err != nil {
				return nil, 0, err
			}
			return io.NopCloser(r), s, nil
		},
	}
}

func (nbs *NomsBlockStore) Size(ctx context.Context) (uint64, error) {
	nbs.mu.Lock()
	defer nbs.mu.Unlock()

	exists, contents, err := nbs.mm.m.ParseIfExists(ctx, nbs.stats, nil)

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
		size += cs.currentSize()
	}
	return size, nil
}

func (nbs *NomsBlockStore) chunkSourcesByAddr() (map[addr]chunkSource, error) {
	css := make(map[addr]chunkSource, len(nbs.tables.upstream)+len(nbs.tables.novel))
	for _, cs := range nbs.tables.upstream {
		css[cs.hash()] = cs
	}
	for _, cs := range nbs.tables.novel {
		css[cs.hash()] = cs
	}
	return css, nil

}

func (nbs *NomsBlockStore) SupportedOperations() TableFileStoreOps {
	var ok bool
	switch nbs.p.(type) {
	case *fsTablePersister, *chunkJournal:
		ok = true
	}
	return TableFileStoreOps{
		CanRead:  true,
		CanWrite: ok,
		CanPrune: ok,
		CanGC:    ok,
	}
}

func (nbs *NomsBlockStore) Path() (string, bool) {
	if tfp, ok := nbs.p.(tableFilePersister); ok {
		return tfp.Path(), true
	}
	return "", false
}

// WriteTableFile will read a table file from the provided reader and write it to the TableFileStore
func (nbs *NomsBlockStore) WriteTableFile(ctx context.Context, fileId string, numChunks int, contentHash []byte, getRd func() (io.ReadCloser, uint64, error)) error {
	var fsPersister *fsTablePersister
	switch t := nbs.p.(type) {
	case *fsTablePersister:
		fsPersister = t
	case *chunkJournal:
		fsPersister = t.persister
	default:
		return errors.New("Not implemented")
	}

	tn, err := func() (n string, err error) {
		var r io.ReadCloser
		r, _, err = getRd()
		if err != nil {
			return "", err
		}
		defer func() {
			cerr := r.Close()
			if err == nil {
				err = cerr
			}
		}()

		var temp *os.File
		temp, err = tempfiles.MovableTempFileProvider.NewFile(fsPersister.dir, tempTablePrefix)
		if err != nil {
			return "", err
		}

		defer func() {
			cerr := temp.Close()
			if err == nil {
				err = cerr
			}
		}()

		_, err = io.Copy(temp, r)
		if err != nil {
			return "", err
		}

		return temp.Name(), nil
	}()
	if err != nil {
		return err
	}

	path := filepath.Join(fsPersister.dir, fileId)
	return file.Rename(tn, path)
}

// AddTableFilesToManifest adds table files to the manifest
func (nbs *NomsBlockStore) AddTableFilesToManifest(ctx context.Context, fileIdToNumChunks map[string]int) error {
	var totalChunks int
	fileIdHashToNumChunks := make(map[hash.Hash]uint32)
	for fileId, numChunks := range fileIdToNumChunks {
		fileIdHash, ok := hash.MaybeParse(fileId)

		if !ok {
			return errors.New("invalid base32 encoded hash: " + fileId)
		}

		fileIdHashToNumChunks[fileIdHash] = uint32(numChunks)
		totalChunks += numChunks
	}

	if totalChunks == 0 {
		return nil
	}

	_, err := nbs.UpdateManifest(ctx, fileIdHashToNumChunks)
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

	ok, contents, t, err := nbs.mm.Fetch(ctx, &Stats{})
	if err != nil {
		return err
	}
	if !ok {
		return nil // no manifest exists
	}

	return nbs.p.PruneTableFiles(ctx, contents, t)
}

func (nbs *NomsBlockStore) MarkAndSweepChunks(ctx context.Context, last hash.Hash, keepChunks <-chan []hash.Hash, dest chunks.ChunkStore) error {
	ops := nbs.SupportedOperations()
	if !ops.CanGC || !ops.CanPrune {
		return chunks.ErrUnsupportedOperation
	}

	precheck := func() error {
		nbs.mu.RLock()
		defer nbs.mu.RUnlock()

		if nbs.upstream.root != last {
			return errLastRootMismatch
		}

		// check to see if the specs have changed since last gc.  If they haven't bail early.
		gcGenCheck := generateLockHash(last, nbs.upstream.specs, nbs.upstream.appendix)
		if nbs.upstream.gcGen == gcGenCheck {
			return chunks.ErrNothingToCollect
		}

		return nil
	}
	err := precheck()
	if err != nil {
		return err
	}

	destNBS := nbs
	if dest != nil {
		switch typed := dest.(type) {
		case *NomsBlockStore:
			destNBS = typed
		case NBSMetricWrapper:
			destNBS = typed.nbs
		}
	}

	specs, err := nbs.copyMarkedChunks(ctx, keepChunks, destNBS)
	if err != nil {
		return err
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}

	if destNBS == nbs {
		err = nbs.swapTables(ctx, specs)
		if err != nil {
			return err
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}

		currentContents := func() manifestContents {
			nbs.mu.RLock()
			defer nbs.mu.RUnlock()
			return nbs.upstream
		}()

		t := time.Now()
		return nbs.p.PruneTableFiles(ctx, currentContents, t)
	} else {
		fileIdToNumChunks := tableSpecsToMap(specs)
		err = destNBS.AddTableFilesToManifest(ctx, fileIdToNumChunks)

		if err != nil {
			return err
		}
		return nil
	}
}

func (nbs *NomsBlockStore) copyMarkedChunks(ctx context.Context, keepChunks <-chan []hash.Hash, dest *NomsBlockStore) ([]tableSpec, error) {
	gcc, err := newGarbageCollectionCopier()
	if err != nil {
		return nil, err
	}

	tfp, ok := dest.p.(tableFilePersister)
	if !ok {
		return nil, fmt.Errorf("NBS does not support copying garbage collection")
	}
	path := tfp.Path()

LOOP:
	for {
		select {
		case hs, ok := <-keepChunks:
			if !ok {
				break LOOP
			}
			var addErr error
			mu := new(sync.Mutex)
			hashset := hash.NewHashSet(hs...)
			err := nbs.GetManyCompressed(ctx, hashset, func(ctx context.Context, c CompressedChunk) {
				mu.Lock()
				defer mu.Unlock()
				if addErr != nil {
					return
				}
				addErr = gcc.addChunk(ctx, c)
			})
			if err != nil {
				return nil, err
			}
			if addErr != nil {
				return nil, addErr
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return gcc.copyTablesToDir(ctx, path)
}

// todo: what's the optimal table size to copy to?
func (nbs *NomsBlockStore) gcTableSize() (uint64, error) {
	total, err := nbs.tables.physicalLen()

	if err != nil {
		return 0, err
	}

	avgTableSize := total / uint64(nbs.tables.Size()+1)

	// max(avgTableSize, defaultMemTableSize)
	if avgTableSize > nbs.mtSize {
		return avgTableSize, nil
	}
	return nbs.mtSize, nil
}

func (nbs *NomsBlockStore) swapTables(ctx context.Context, specs []tableSpec) (err error) {
	nbs.mm.LockForUpdate()
	defer func() {
		unlockErr := nbs.mm.UnlockForUpdate()
		if err == nil {
			err = unlockErr
		}
	}()

	nbs.mu.Lock()
	defer nbs.mu.Unlock()

	newLock := generateLockHash(nbs.upstream.root, specs, []tableSpec{})
	newContents := manifestContents{
		nbfVers: nbs.upstream.nbfVers,
		root:    nbs.upstream.root,
		lock:    newLock,
		gcGen:   newLock,
		specs:   specs,
	}

	// nothing has changed.  Bail early
	if newContents.gcGen == nbs.upstream.gcGen {
		return nil
	}

	upstream, uerr := nbs.mm.UpdateGCGen(ctx, nbs.upstream.lock, newContents, nbs.stats, nil)
	if uerr != nil {
		return uerr
	}

	if upstream.lock != newContents.lock {
		return errors.New("concurrent manifest edit during GC, before swapTables. GC failed.")
	}

	// clear memTable
	nbs.mt = newMemTable(nbs.mtSize)

	// clear nbs.tables.novel
	nbs.tables, err = nbs.tables.flatten(ctx)
	if err != nil {
		return err
	}

	// replace nbs.tables.upstream with gc compacted tables
	nbs.upstream = upstream
	nbs.tables, err = nbs.tables.rebase(ctx, upstream.specs, nbs.stats)
	if err != nil {
		return err
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

// CalcReads computes the number of IO operations necessary to fetch |hashes|.
func CalcReads(nbs *NomsBlockStore, hashes hash.HashSet, blockSize uint64) (reads int, split bool, err error) {
	reqs := toGetRecords(hashes)
	tables := func() (tables tableSet) {
		nbs.mu.RLock()
		defer nbs.mu.RUnlock()
		tables = nbs.tables

		return
	}()

	reads, split, remaining, err := tableSetCalcReads(tables, reqs, blockSize)

	if err != nil {
		return 0, false, err
	}

	if remaining {
		return 0, false, errors.New("failed to find all chunks")
	}

	return
}
