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
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/dustin/go-humanize"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/store/blobstore"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
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
)

var tracer = otel.Tracer("github.com/dolthub/dolt/go/store/nbs")

func makeGlobalCaches() {
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

	cond         *sync.Cond
	gcInProgress bool
	// keeperFunc is set when |gcInProgress| and appends to the GC sweep queue
	// or blocks on GC finalize
	keeperFunc func(hash.Hash) bool

	mtSize   uint64
	putCount uint64

	hasCache *lru.TwoQueueCache[hash.Hash, struct{}]

	stats *Stats
}

func (nbs *NomsBlockStore) PersistGhostHashes(ctx context.Context, refs hash.HashSet) error {
	return fmt.Errorf("runtime error: PersistGhostHashes should never be called on the NomsBlockStore")
}

var _ chunks.TableFileStore = &NomsBlockStore{}
var _ chunks.ChunkStoreGarbageCollector = &NomsBlockStore{}

// 20-byte keys, ~2MB of key data.
//
// Likely big enough to keep common top of DAG references in the scan resistant
// portion for most databases.
const hasCacheSize = 100000

type Range struct {
	Offset uint64
	Length uint32
}

// ChunkJournal returns the ChunkJournal in use by this NomsBlockStore, or nil if no ChunkJournal is being used.
func (nbs *NomsBlockStore) ChunkJournal() *ChunkJournal {
	if cj, ok := nbs.p.(*ChunkJournal); ok {
		return cj
	}
	return nil
}

func (nbs *NomsBlockStore) GetChunkLocationsWithPaths(ctx context.Context, hashes hash.HashSet) (map[string]map[hash.Hash]Range, error) {
	locs, err := nbs.GetChunkLocations(ctx, hashes)
	if err != nil {
		return nil, err
	}
	toret := make(map[string]map[hash.Hash]Range, len(locs))
	for k, v := range locs {
		toret[k.String()] = v
	}
	return toret, nil
}

func (nbs *NomsBlockStore) GetChunkLocations(ctx context.Context, hashes hash.HashSet) (map[hash.Hash]map[hash.Hash]Range, error) {
	gr := toGetRecords(hashes)
	ranges := make(map[hash.Hash]map[hash.Hash]Range)

	fn := func(css chunkSourceSet) error {
		for _, cs := range css {
			rng, err := cs.getRecordRanges(ctx, gr)
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

func (nbs *NomsBlockStore) conjoinIfRequired(ctx context.Context) (bool, error) {
	if nbs.c.conjoinRequired(nbs.tables) {
		newUpstream, cleanup, err := conjoin(ctx, nbs.c, nbs.upstream, nbs.mm, nbs.p, nbs.stats)
		if err != nil {
			return false, err
		}

		newTables, err := nbs.tables.rebase(ctx, newUpstream.specs, nbs.stats)
		if err != nil {
			return false, err
		}

		nbs.upstream = newUpstream
		oldTables := nbs.tables
		nbs.tables = newTables
		err = oldTables.close()
		if err != nil {
			return true, err
		}
		cleanup()
		return true, nil
	} else {
		return false, nil
	}
}

func (nbs *NomsBlockStore) UpdateManifest(ctx context.Context, updates map[hash.Hash]uint32) (mi ManifestInfo, err error) {
	nbs.mu.Lock()
	defer nbs.mu.Unlock()
	err = nbs.waitForGC(ctx)
	if err != nil {
		return
	}

	err = nbs.checkAllManifestUpdatesExist(ctx, updates)
	if err != nil {
		return
	}

	nbs.mm.LockForUpdate()
	defer func() {
		unlockErr := nbs.mm.UnlockForUpdate()

		if err == nil {
			err = unlockErr
		}
	}()

	_, err = nbs.conjoinIfRequired(ctx)
	if err != nil {
		return manifestContents{}, err
	}

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
			if _, ok := currSpecs[h]; !ok {
				addCount++
				contents.specs = append(contents.specs, tableSpec{h, count})
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
	nbs.mu.Lock()
	defer nbs.mu.Unlock()
	err = nbs.waitForGC(ctx)
	if err != nil {
		return
	}

	err = nbs.checkAllManifestUpdatesExist(ctx, updates)
	if err != nil {
		return
	}

	nbs.mm.LockForUpdate()
	defer func() {
		unlockErr := nbs.mm.UnlockForUpdate()

		if err == nil {
			err = unlockErr
		}
	}()

	_, err = nbs.conjoinIfRequired(ctx)
	if err != nil {
		return manifestContents{}, err
	}

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
			if option == ManifestAppendixOption_Set {
				appendixSpecs = append(appendixSpecs, tableSpec{h, count})
			} else {
				if _, ok := currAppendixSpecs[h]; !ok {
					addCount++
					appendixSpecs = append(appendixSpecs, tableSpec{h, count})
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

func (nbs *NomsBlockStore) checkAllManifestUpdatesExist(ctx context.Context, updates map[hash.Hash]uint32) error {
	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(128)
	for h, c := range updates {
		h := h
		c := c
		eg.Go(func() error {
			ok, err := nbs.p.Exists(ctx, h, c, nbs.stats)
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("missing table file referenced in UpdateManifest call: %v", h)
			}
			return nil
		})
	}
	return eg.Wait()
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
	store.mu.Lock()
	defer store.mu.Unlock()
	err = store.waitForGC(ctx)
	if err != nil {
		return
	}

	contents := manifestContents{
		root:    root,
		nbfVers: store.upstream.nbfVers,
	}
	// Appendix table files should come first in specs
	for h, c := range appendixTableFiles {
		s := tableSpec{name: h, chunkCount: c}
		contents.appendix = append(contents.appendix, s)
		contents.specs = append(contents.specs, s)
	}
	for h, c := range tableFiles {
		s := tableSpec{name: h, chunkCount: c}
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

func NewAWSStoreWithMMapIndex(ctx context.Context, nbfVerStr string, table, ns, bucket string, s3 s3iface.S3API, ddb ddbsvc, memTableSize uint64, q MemoryQuotaProvider) (*NomsBlockStore, error) {
	cacheOnce.Do(makeGlobalCaches)
	readRateLimiter := make(chan struct{}, 32)
	p := &awsTablePersister{
		s3,
		bucket,
		readRateLimiter,
		awsLimits{defaultS3PartSize, minS3PartSize, maxS3PartSize},
		ns,
		q,
	}
	mm := makeManifestManager(newDynamoManifest(table, ns, ddb))
	return newNomsBlockStore(ctx, nbfVerStr, mm, p, q, inlineConjoiner{defaultMaxTables}, memTableSize)
}

func NewAWSStore(ctx context.Context, nbfVerStr string, table, ns, bucket string, s3 s3iface.S3API, ddb ddbsvc, memTableSize uint64, q MemoryQuotaProvider) (*NomsBlockStore, error) {
	cacheOnce.Do(makeGlobalCaches)
	readRateLimiter := make(chan struct{}, 32)
	p := &awsTablePersister{
		s3,
		bucket,
		readRateLimiter,
		awsLimits{defaultS3PartSize, minS3PartSize, maxS3PartSize},
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

// NewGCSStore returns an nbs implementation backed by a GCSBlobstore
func NewOCISStore(ctx context.Context, nbfVerStr string, bucketName, path string, provider common.ConfigurationProvider, client objectstorage.ObjectStorageClient, memTableSize uint64, q MemoryQuotaProvider) (*NomsBlockStore, error) {
	cacheOnce.Do(makeGlobalCaches)

	bs, err := blobstore.NewOCIBlobstore(ctx, provider, client, bucketName, path)
	if err != nil {
		return nil, err
	}

	return NewNoConjoinBSStore(ctx, nbfVerStr, bs, memTableSize, q)
}

// NewBSStore returns an nbs implementation backed by a Blobstore
func NewBSStore(ctx context.Context, nbfVerStr string, bs blobstore.Blobstore, memTableSize uint64, q MemoryQuotaProvider) (*NomsBlockStore, error) {
	cacheOnce.Do(makeGlobalCaches)

	mm := makeManifestManager(blobstoreManifest{bs})

	p := &blobstorePersister{bs, s3BlockSize, q}
	return newNomsBlockStore(ctx, nbfVerStr, mm, p, q, inlineConjoiner{defaultMaxTables}, memTableSize)
}

// NewNoConjoinBSStore returns a nbs implementation backed by a Blobstore
func NewNoConjoinBSStore(ctx context.Context, nbfVerStr string, bs blobstore.Blobstore, memTableSize uint64, q MemoryQuotaProvider) (*NomsBlockStore, error) {
	cacheOnce.Do(makeGlobalCaches)

	mm := makeManifestManager(blobstoreManifest{bs})

	p := &noConjoinBlobstorePersister{bs, s3BlockSize, q}
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
	ok, err := fileExists(filepath.Join(dir, chunkJournalAddr))
	if err != nil {
		return nil, err
	} else if ok {
		return nil, fmt.Errorf("cannot create NBS store for directory containing chunk journal: %s", dir)
	}

	m, err := getFileManifest(ctx, dir, asyncFlush)
	if err != nil {
		return nil, err
	}
	p := newFSTablePersister(dir, q)
	c := conjoinStrategy(inlineConjoiner{maxTables})

	return newNomsBlockStore(ctx, nbfVerStr, makeManifestManager(m), p, q, c, memTableSize)
}

func NewLocalJournalingStore(ctx context.Context, nbfVers, dir string, q MemoryQuotaProvider) (*NomsBlockStore, error) {
	cacheOnce.Do(makeGlobalCaches)
	if err := checkDir(dir); err != nil {
		return nil, err
	}

	m, err := newJournalManifest(ctx, dir)
	if err != nil {
		return nil, err
	}
	p := newFSTablePersister(dir, q)

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

	hasCache, err := lru.New2Q[hash.Hash, struct{}](hasCacheSize)
	if err != nil {
		return nil, err
	}

	nbs := &NomsBlockStore{
		mm:       mm,
		p:        p,
		c:        c,
		tables:   newTableSet(p, q),
		upstream: manifestContents{nbfVers: nbfVerStr},
		mtSize:   memTableSize,
		hasCache: hasCache,
		stats:    NewStats(),
	}
	nbs.cond = sync.NewCond(&nbs.mu)

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
		hasCache: nbs.hasCache,
		stats:    nbs.stats,
	}
}

// Wait for GC to complete to continue with writes
func (nbs *NomsBlockStore) waitForGC(ctx context.Context) error {
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			nbs.cond.Broadcast()
		case <-stop:
		}
	}()
	for nbs.gcInProgress && ctx.Err() == nil {
		nbs.cond.Wait()
	}
	return ctx.Err()
}

func (nbs *NomsBlockStore) Put(ctx context.Context, c chunks.Chunk, getAddrs chunks.GetAddrsCurry) error {
	return nbs.putChunk(ctx, c, getAddrs, nbs.hasMany)
}

func (nbs *NomsBlockStore) putChunk(ctx context.Context, c chunks.Chunk, getAddrs chunks.GetAddrsCurry, checker refCheck) error {
	t1 := time.Now()

	success, err := nbs.addChunk(ctx, c, getAddrs, checker)
	if err != nil {
		return err
	} else if !success {
		return errors.New("failed to add chunk")
	}
	atomic.AddUint64(&nbs.putCount, 1)

	nbs.stats.PutLatency.SampleTimeSince(t1)

	return nil
}

// When we have chunks with dangling references in our memtable, we have to
// throw away the entire memtable.
func (nbs *NomsBlockStore) handlePossibleDanglingRefError(err error) {
	if errors.Is(err, ErrDanglingRef) {
		nbs.mt = nil
	}
}

// Writes to a Dolt database typically involve mutating some tuple maps and
// then mutating the top-level address map which points to all the branch heads
// and working sets. Each internal node of the address map can have many
// references and many of them typically change quite slowly. We keep a cache
// of recently written references which we know are in the database so that we
// don't have to check the table file indexes for these  chunks when we write
// references to them again in the near future.
//
// This cache needs to be treated in a principled manner. The integrity checks
// that we run against the a set of chunks we are attempting to write consider
// the to-be-written chunks themselves as also being in the database. This is
// correct, assuming that all the chunks are written at the same time. However,
// we should not add the results of those presence checks to the cache until
// those chunks actually land in the database.
func (nbs *NomsBlockStore) addPendingRefsToHasCache() {
	for _, e := range nbs.mt.pendingRefs {
		if e.has {
			nbs.hasCache.Add(*e.a, struct{}{})
		}
	}
}

func (nbs *NomsBlockStore) addChunk(ctx context.Context, ch chunks.Chunk, getAddrs chunks.GetAddrsCurry, checker refCheck) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	nbs.mu.Lock()
	defer nbs.mu.Unlock()

	retry := true
	var addChunkRes addChunkResult
	for retry {
		retry = false
		if nbs.mt == nil {
			nbs.mt = newMemTable(nbs.mtSize)
		}

		addChunkRes = nbs.mt.addChunk(ch.Hash(), ch.Data())
		if addChunkRes == chunkNotAdded {
			ts, err := nbs.tables.append(ctx, nbs.mt, checker, nbs.hasCache, nbs.stats)
			if err != nil {
				nbs.handlePossibleDanglingRefError(err)
				return false, err
			}
			nbs.addPendingRefsToHasCache()
			nbs.tables = ts
			nbs.mt = newMemTable(nbs.mtSize)
			addChunkRes = nbs.mt.addChunk(ch.Hash(), ch.Data())
		}
		if addChunkRes == chunkAdded || addChunkRes == chunkExists {
			if nbs.keeperFunc != nil && nbs.keeperFunc(ch.Hash()) {
				retry = true
				if err := nbs.waitForGC(ctx); err != nil {
					return false, err
				}
				continue
			}
		}
		if addChunkRes == chunkAdded {
			nbs.mt.addGetChildRefs(getAddrs(ch))
		}
	}

	return addChunkRes == chunkAdded || addChunkRes == chunkExists, nil
}

// refCheck checks that no dangling references are being committed.
type refCheck func(reqs []hasRecord) (hash.HashSet, error)

func (nbs *NomsBlockStore) errorIfDangling(root hash.Hash, checker refCheck) error {
	if !root.IsEmpty() {
		if _, ok := nbs.hasCache.Get(root); !ok {
			var hr [1]hasRecord
			hr[0].a = &root
			hr[0].prefix = root.Prefix()
			absent, err := checker(hr[:])
			if err != nil {
				return err
			} else if absent.Size() > 0 {
				return fmt.Errorf("%w: found dangling references to %s", ErrDanglingRef, absent.String())
			}
			nbs.hasCache.Add(root, struct{}{})
		}
	}
	return nil
}

func (nbs *NomsBlockStore) Get(ctx context.Context, h hash.Hash) (chunks.Chunk, error) {
	ctx, span := tracer.Start(ctx, "nbs.Get")
	defer span.End()

	t1 := time.Now()
	defer func() {
		nbs.stats.GetLatency.SampleTimeSince(t1)
		nbs.stats.ChunksPerGet.Sample(1)
	}()

	data, tables, err := func() ([]byte, chunkReader, error) {
		var data []byte
		nbs.mu.RLock()
		defer nbs.mu.RUnlock()
		if nbs.mt != nil {
			var err error
			data, err = nbs.mt.get(ctx, h, nbs.stats)

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

	data, err = tables.get(ctx, h, nbs.stats)

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
	const ioParallelism = 16
	eg.SetLimit(ioParallelism)

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
		h := h
		reqs[idx] = getRecord{
			a:      &h,
			prefix: h.Prefix(),
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

	has, tables, err := func() (bool, chunkReader, error) {
		nbs.mu.RLock()
		defer nbs.mu.RUnlock()

		if nbs.mt != nil {
			has, err := nbs.mt.has(h)

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
		has, err = tables.has(h)

		if err != nil {
			return false, err
		}
	}

	return has, nil
}

func (nbs *NomsBlockStore) HasMany(ctx context.Context, hashes hash.HashSet) (hash.HashSet, error) {
	if hashes.Size() == 0 {
		return nil, nil
	}

	t1 := time.Now()
	defer nbs.stats.HasLatency.SampleTimeSince(t1)
	nbs.stats.AddressesPerHas.SampleLen(hashes.Size())

	nbs.mu.RLock()
	defer nbs.mu.RUnlock()
	return nbs.hasMany(toHasRecords(hashes))
}

func (nbs *NomsBlockStore) hasMany(reqs []hasRecord) (hash.HashSet, error) {
	tables, remaining, err := func() (tables chunkReader, remaining bool, err error) {
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

	absent := hash.HashSet{}
	for _, r := range reqs {
		if !r.has {
			absent.Insert(*r.a)
		}
	}
	return absent, nil
}

func toHasRecords(hashes hash.HashSet) []hasRecord {
	reqs := make([]hasRecord, len(hashes))
	idx := 0
	for h := range hashes {
		h := h
		reqs[idx] = hasRecord{
			a:      &h,
			prefix: h.Prefix(),
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
	return nbs.rebase(ctx)
}

func (nbs *NomsBlockStore) rebase(ctx context.Context) error {
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
	return nbs.commit(ctx, current, last, nbs.hasMany)
}

func (nbs *NomsBlockStore) commit(ctx context.Context, current, last hash.Hash, checker refCheck) (success bool, err error) {
	t1 := time.Now()
	defer nbs.stats.CommitLatency.SampleTimeSince(t1)

	nbs.mu.Lock()
	defer nbs.mu.Unlock()

	if nbs.keeperFunc != nil {
		if nbs.keeperFunc(current) {
			err = nbs.waitForGC(ctx)
			if err != nil {
				return false, err
			}
		}
	}

	anyPossiblyNovelChunks := nbs.mt != nil || len(nbs.tables.novel) > 0

	if !anyPossiblyNovelChunks && current == last {
		err := nbs.rebase(ctx)
		if err != nil {
			return false, err
		}
		return true, nil
	}

	nbs.mm.LockForUpdate()
	defer func() {
		unlockErr := nbs.mm.UnlockForUpdate()

		if err == nil {
			err = unlockErr
		}
	}()

	for {
		if err := nbs.updateManifest(ctx, current, last, checker); err == nil {
			return true, nil
		} else if err == errOptimisticLockFailedRoot || err == errLastRootMismatch {
			return false, nil
		} else if err != errOptimisticLockFailedTables {
			return false, err
		}
	}
}

var (
	errLastRootMismatch           = fmt.Errorf("last does not match nbs.Root()")
	errOptimisticLockFailedRoot   = fmt.Errorf("root moved")
	errOptimisticLockFailedTables = fmt.Errorf("tables changed")
	errReadOnlyManifest           = fmt.Errorf("cannot update manifest: database is read only")
)

// callers must acquire lock |nbs.mu|
func (nbs *NomsBlockStore) updateManifest(ctx context.Context, current, last hash.Hash, checker refCheck) error {
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
			ts, err := nbs.tables.append(ctx, nbs.mt, checker, nbs.hasCache, nbs.stats)
			if err != nil {
				nbs.handlePossibleDanglingRefError(err)
				return err
			}
			nbs.addPendingRefsToHasCache()
			nbs.tables, nbs.mt = ts, nil
		}
	}

	didConjoin, err := nbs.conjoinIfRequired(ctx)
	if err != nil {
		return err
	}
	if didConjoin {
		return errOptimisticLockFailedTables
	}

	// check for dangling reference to the new root
	if err = nbs.errorIfDangling(current, checker); err != nil {
		nbs.handlePossibleDanglingRefError(err)
		return err
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
		return err
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

func (nbs *NomsBlockStore) AccessMode() chunks.ExclusiveAccessMode {
	return nbs.p.AccessMode()
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

// LocationPrefix
func (tf tableFile) LocationPrefix() string {
	return ""
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
func (nbs *NomsBlockStore) Sources(ctx context.Context) (hash.Hash, []chunks.TableFile, []chunks.TableFile, error) {
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

func getTableFiles(css map[hash.Hash]chunkSource, contents manifestContents, numSpecs int, specFunc func(mc manifestContents, idx int) tableSpec) ([]chunks.TableFile, error) {
	tableFiles := make([]chunks.TableFile, 0)
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
			return r, s, nil
		},
	}
}

func (nbs *NomsBlockStore) Size(ctx context.Context) (uint64, error) {
	nbs.mu.Lock()
	defer nbs.mu.Unlock()

	size := uint64(0)
	for _, cs := range nbs.tables.upstream {
		size += cs.currentSize()
	}
	for _, cs := range nbs.tables.novel {
		size += cs.currentSize()
	}

	return size, nil
}

func (nbs *NomsBlockStore) chunkSourcesByAddr() (map[hash.Hash]chunkSource, error) {
	css := make(map[hash.Hash]chunkSource, len(nbs.tables.upstream)+len(nbs.tables.novel))
	for _, cs := range nbs.tables.upstream {
		css[cs.hash()] = cs
	}
	for _, cs := range nbs.tables.novel {
		css[cs.hash()] = cs
	}
	return css, nil

}

func (nbs *NomsBlockStore) SupportedOperations() chunks.TableFileStoreOps {
	var ok bool
	_, ok = nbs.p.(tableFilePersister)

	return chunks.TableFileStoreOps{
		CanRead:  true,
		CanWrite: ok,
		CanPrune: ok,
		CanGC:    ok,
	}
}

func (nbs *NomsBlockStore) Path() (string, bool) {
	if tfp, ok := nbs.p.(tableFilePersister); ok {
		switch p := tfp.(type) {
		case *fsTablePersister, *ChunkJournal:
			return p.Path(), true
		default:
			return "", false
		}
	}
	return "", false
}

// WriteTableFile will read a table file from the provided reader and write it to the TableFileStore
func (nbs *NomsBlockStore) WriteTableFile(ctx context.Context, fileId string, numChunks int, contentHash []byte, getRd func() (io.ReadCloser, uint64, error)) error {
	tfp, ok := nbs.p.(tableFilePersister)
	if !ok {
		return errors.New("Not implemented")
	}

	r, sz, err := getRd()
	if err != nil {
		return err
	}
	defer r.Close()
	return tfp.CopyTableFile(ctx, r, fileId, sz, uint32(numChunks))
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
	return nbs.pruneTableFiles(ctx, nbs.hasMany)
}

func (nbs *NomsBlockStore) pruneTableFiles(ctx context.Context, checker refCheck) (err error) {
	mtime := time.Now()

	return nbs.p.PruneTableFiles(ctx, func() []hash.Hash {
		nbs.mu.Lock()
		defer nbs.mu.Unlock()
		keepers := make([]hash.Hash, 0, len(nbs.tables.novel)+len(nbs.tables.upstream))
		for a, _ := range nbs.tables.novel {
			keepers = append(keepers, a)
		}
		for a, _ := range nbs.tables.upstream {
			keepers = append(keepers, a)
		}
		return keepers
	}, mtime)
}

func (nbs *NomsBlockStore) BeginGC(keeper func(hash.Hash) bool) error {
	nbs.cond.L.Lock()
	defer nbs.cond.L.Unlock()
	if nbs.gcInProgress {
		return errors.New("gc already in progress")
	}
	nbs.gcInProgress = true
	nbs.keeperFunc = keeper
	nbs.cond.Broadcast()
	return nil
}

func (nbs *NomsBlockStore) EndGC() {
	nbs.cond.L.Lock()
	defer nbs.cond.L.Unlock()
	if !nbs.gcInProgress {
		panic("EndGC called when gc was not in progress")
	}
	nbs.gcInProgress = false
	nbs.keeperFunc = nil
	nbs.cond.Broadcast()
}

func (nbs *NomsBlockStore) MarkAndSweepChunks(ctx context.Context, hashes <-chan []hash.Hash, dest chunks.ChunkStore) error {
	ops := nbs.SupportedOperations()
	if !ops.CanGC || !ops.CanPrune {
		return chunks.ErrUnsupportedOperation
	}

	precheck := func() error {
		nbs.mu.RLock()
		defer nbs.mu.RUnlock()

		// check to see if the specs have changed since last gc. If they haven't bail early.
		gcGenCheck := generateLockHash(nbs.upstream.root, nbs.upstream.specs, nbs.upstream.appendix)
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

	specs, err := nbs.copyMarkedChunks(ctx, hashes, destNBS)
	if err != nil {
		return err
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}

	if destNBS == nbs {
		return nbs.swapTables(ctx, specs)
	} else {
		fileIdToNumChunks := tableSpecsToMap(specs)
		return destNBS.AddTableFilesToManifest(ctx, fileIdToNumChunks)
	}
}

func (nbs *NomsBlockStore) copyMarkedChunks(ctx context.Context, keepChunks <-chan []hash.Hash, dest *NomsBlockStore) ([]tableSpec, error) {
	tfp, ok := dest.p.(tableFilePersister)
	if !ok {
		return nil, fmt.Errorf("NBS does not support copying garbage collection")
	}

	gcc, err := newGarbageCollectionCopier()
	if err != nil {
		return nil, err
	}

	// TODO: We should clean up gcc on error.

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
			found := 0
			err := nbs.GetManyCompressed(ctx, hashset, func(ctx context.Context, c CompressedChunk) {
				mu.Lock()
				defer mu.Unlock()
				if addErr != nil {
					return
				}
				found += 1
				addErr = gcc.addChunk(ctx, c)
			})
			if err != nil {
				return nil, err
			}
			if addErr != nil {
				return nil, addErr
			}
			if found != len(hashset) {
				return nil, fmt.Errorf("dangling references requested during GC. GC not successful. %v", hashset)
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return gcc.copyTablesToDir(ctx, tfp)
}

func (nbs *NomsBlockStore) GetChunkHashes(ctx context.Context, hashes chan<- hash.Hash, wg *sync.WaitGroup) int {
	chunkCount := 0
	for _, v := range nbs.tables.novel {
		chunkCount += v.getAllChunkHashes(ctx, hashes, wg)

	}
	for _, v := range nbs.tables.upstream {
		chunkCount += v.getAllChunkHashes(ctx, hashes, wg)
	}
	return chunkCount
}

func (nbs *NomsBlockStore) swapTables(ctx context.Context, specs []tableSpec) (err error) {
	nbs.mu.Lock()
	defer nbs.mu.Unlock()

	nbs.mm.LockForUpdate()
	defer func() {
		unlockErr := nbs.mm.UnlockForUpdate()
		if err == nil {
			err = unlockErr
		}
	}()

	newLock := generateLockHash(nbs.upstream.root, specs, []tableSpec{})
	newContents := manifestContents{
		nbfVers: nbs.upstream.nbfVers,
		root:    nbs.upstream.root,
		lock:    newLock,
		gcGen:   newLock,
		specs:   specs,
	}

	// Nothing has changed. Bail early.
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

	// We purge the hasCache here, since |swapTables| is the only place where
	// chunks can actually be removed from the block store. Other times when
	// we update the table set, we are appending new table files to it, or
	// replacing table files in it with a file into which they were conjoined.
	nbs.hasCache.Purge()

	// replace nbs.tables.upstream with gc compacted tables
	ts, err := nbs.tables.rebase(ctx, upstream.specs, nbs.stats)
	if err != nil {
		return err
	}
	oldTables := nbs.tables
	nbs.tables, nbs.upstream = ts, upstream
	err = oldTables.close()
	if err != nil {
		return err
	}

	// When this is called, we are at a safepoint in the GC process.
	// We clear novel and the memtable, which are not coming with us
	// into the new store.
	oldNovel := nbs.tables.novel
	nbs.tables.novel = make(chunkSourceSet)
	for _, css := range oldNovel {
		err = css.close()
		if err != nil {
			return err
		}
	}
	if nbs.mt != nil {
		var thrown []string
		for a := range nbs.mt.chunks {
			thrown = append(thrown, a.String())
		}
	}
	nbs.mt = nil
	return nil
}

// SetRootChunk changes the root chunk hash from the previous value to the new root.
func (nbs *NomsBlockStore) SetRootChunk(ctx context.Context, root, previous hash.Hash) error {
	return nbs.setRootChunk(ctx, root, previous, nbs.hasMany)
}

func (nbs *NomsBlockStore) setRootChunk(ctx context.Context, root, previous hash.Hash, checker refCheck) error {
	nbs.mu.Lock()
	defer nbs.mu.Unlock()
	err := nbs.waitForGC(ctx)
	if err != nil {
		return err
	}
	for {
		err := nbs.updateManifest(ctx, root, previous, checker)

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
