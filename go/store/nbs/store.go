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
	"errors"
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
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/libraries/utils/valctx"
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
	GetManyCompressed(context.Context, hash.HashSet, func(context.Context, ToChunker)) error
}

type gcDependencyMode int

const (
	gcDependencyMode_TakeDependency gcDependencyMode = iota
	gcDependencyMode_NoDependency
)

type CompressedChunkStoreForGC interface {
	getManyCompressed(context.Context, hash.HashSet, func(context.Context, ToChunker), gcDependencyMode) error
}

type NomsBlockStore struct {
	manifestMgr manifestManager
	persister   tablePersister
	conjoiner   conjoinStrategy

	mu       sync.RWMutex // protects the following state
	memtable *memTable
	tables   tableSet
	upstream manifestContents

	conjoinOp     *conjoinOperation
	conjoinOpCond *sync.Cond

	// Guarded by |mu|. Notified on gcInProgress and gcOutstandingReads changes.
	// Used to implement |waitForGC|.
	gcCond *sync.Cond
	// |true| after BeginGC is called, and false once the corresponding EndGC call returns.
	gcInProgress bool
	// When unlocked read operations are occurring against the
	// block store, and they started when |gcInProgress == true|,
	// this variable is incremented. EndGC will not return until
	// no outstanding reads are in progress.
	gcOutstandingReads int
	// keeperFunc is set when |gcInProgress| and appends to the GC sweep queue
	// or blocks on GC finalize
	keeperFunc func(hash.Hash) bool

	memtableSz uint64
	putCount   uint64

	logger *logrus.Entry

	hasCache *lru.TwoQueueCache[hash.Hash, struct{}]

	stats *Stats
}

func (nbs *NomsBlockStore) PersistGhostHashes(ctx context.Context, refs hash.HashSet) error {
	return fmt.Errorf("runtime error: PersistGhostHashes should never be called on the NomsBlockStore")
}

var _ chunks.TableFileStore = (*NomsBlockStore)(nil)
var _ chunks.ChunkStoreGarbageCollector = (*NomsBlockStore)(nil)

// 20-byte keys, ~2MB of key data.
//
// Likely big enough to keep common top of DAG references in the scan resistant
// portion for most databases.
const hasCacheSize = 100000

type Range struct {
	Offset     uint64
	Length     uint32
	DictOffset uint64
	DictLength uint32
}

// ChunkJournal returns the ChunkJournal in use by this NomsBlockStore, or nil if no ChunkJournal is being used.
func (nbs *NomsBlockStore) ChunkJournal() *ChunkJournal {
	if cj, ok := nbs.persister.(*ChunkJournal); ok {
		return cj
	}
	return nil
}

func (nbs *NomsBlockStore) GetChunkLocationsWithPaths(ctx context.Context, hashes hash.HashSet) (map[string]map[hash.Hash]Range, error) {
	valctx.ValidateContext(ctx)
	sourcesToRanges, err := nbs.getChunkLocations(ctx, hashes)
	if err != nil {
		return nil, err
	}

	res := make(map[string]map[hash.Hash]Range, len(sourcesToRanges))
	for csP, ranges := range sourcesToRanges {
		cs := *csP
		res[cs.hash().String()+cs.suffix()] = ranges
		for h := range ranges {
			delete(hashes, h)
		}
	}
	return res, nil
}

func (nbs *NomsBlockStore) getChunkLocations(ctx context.Context, hashes hash.HashSet) (map[*chunkSource]map[hash.Hash]Range, error) {
	fn := func(css chunkSourceSet, gr []getRecord, ranges map[*chunkSource]map[hash.Hash]Range, keeper keeperF) (gcBehavior, error) {
		for _, cs := range css {
			rng, gcb, err := cs.getRecordRanges(ctx, gr, keeper)
			if err != nil {
				return gcBehavior_Continue, err
			}
			if gcb != gcBehavior_Continue {
				return gcb, nil
			}
			if len(rng) == 0 {
				continue
			}

			if m, ok := ranges[&cs]; ok {
				for k, v := range rng {
					m[k] = v
				}
			} else {
				ranges[&cs] = rng
			}
		}
		return gcBehavior_Continue, nil
	}

	for {
		nbs.mu.Lock()
		tables, keeper, endRead := nbs.tables, nbs.keeperFunc, nbs.beginRead()
		nbs.mu.Unlock()

		gr := toGetRecords(hashes)
		ranges := make(map[*chunkSource]map[hash.Hash]Range)

		gcb, err := fn(tables.upstream, gr, ranges, keeper)
		if needsContinue, err := nbs.handleUnlockedRead(ctx, gcb, false, endRead, err); err != nil {
			return nil, err
		} else if needsContinue {
			continue
		}

		gcb, err = fn(tables.novel, gr, ranges, keeper)
		if needsContinue, err := nbs.handleUnlockedRead(ctx, gcb, true, endRead, err); err != nil {
			return nil, err
		} else if needsContinue {
			continue
		}

		return ranges, nil
	}

}

func (nbs *NomsBlockStore) GetChunkLocations(ctx context.Context, hashes hash.HashSet) (map[hash.Hash]map[hash.Hash]Range, error) {
	valctx.ValidateContext(ctx)
	sourcesToRanges, err := nbs.getChunkLocations(ctx, hashes)
	if err != nil {
		return nil, err
	}
	res := make(map[hash.Hash]map[hash.Hash]Range, len(hashes))
	for csP, ranges := range sourcesToRanges {
		cs := *csP
		res[cs.hash()] = ranges
		for h := range ranges {
			delete(hashes, h)
		}
	}
	return res, nil
}

func (nbs *NomsBlockStore) handleUnlockedRead(ctx context.Context, gcb gcBehavior, endReadOnSuccess bool, endRead func(), err error) (bool, error) {
	if err != nil {
		if endRead != nil {
			nbs.mu.Lock()
			endRead()
			nbs.mu.Unlock()
		}
		return false, err
	}
	if gcb == gcBehavior_Block {
		nbs.mu.Lock()
		if endRead != nil {
			endRead()
		}
		err := nbs.waitForGC(ctx)
		nbs.mu.Unlock()
		return true, err
	} else {
		if endRead != nil && endReadOnSuccess {
			nbs.mu.Lock()
			endRead()
			nbs.mu.Unlock()
		}
		return false, nil
	}
}

func (nbs *NomsBlockStore) startConjoinIfRequired(ctx context.Context) error {
	if nbs.conjoinOp != nil {
		return nil
	}
	if nbs.conjoiner.conjoinRequired(nbs.tables) {
		nbs.logger.WithField("upstream_len", len(nbs.tables.upstream)).Info("beginning conjoin of database")
		var op = &conjoinOperation{}
		err := op.prepareConjoin(ctx, nbs.conjoiner, nbs.upstream)
		if err != nil {
			return err
		}
		nbs.conjoinOp = op
		go func(ctx context.Context) {
			// We use context.Background(), since this context will outlive the caller
			// and it does not access NomsBlockStore storage directly, instead operating
			// only on tablePersister and manifestUpdater.
			err := op.conjoin(ctx, nbs.persister, nbs.stats)
			nbs.finalizeConjoin(ctx, err)
		}(context.Background())
	}
	return nil
}

// Called in an asynchronous context from the goroutine that |startConjoinIfRequired| kicks off.
//
// Responsible for calling conjoinOp.updateManifest under lock and dealing with its results.
func (nbs *NomsBlockStore) finalizeConjoin(ctx context.Context, err error) {
	nbs.mu.Lock()
	defer nbs.mu.Unlock()

	defer func() {
		nbs.conjoinOp = nil
		nbs.conjoinOpCond.Broadcast()
	}()

	if err != nil {
		nbs.logger.WithError(err).Warn("conjoin of database failed with error")
		return
	}

	nbs.manifestMgr.LockForUpdate()
	defer func() {
		err := nbs.manifestMgr.UnlockForUpdate()
		if err != nil {
			nbs.logger.WithError(err).Warn("during conjoin, unlocking manifest manager for update failed with error")
		}
	}()

	newUpstream, cleanup, err := nbs.conjoinOp.updateManifest(ctx, nbs.upstream, nbs.manifestMgr, nbs.stats)
	if err != nil {
		nbs.logger.WithError(err).Warn("during conjoin, updating database manifest with new table files failed")
	}

	newTables, err := nbs.tables.rebase(ctx, newUpstream.specs, nil, nbs.stats)
	if err != nil {
		nbs.logger.WithError(err).Warn("during conjoin, updating database with new table files failed")
		return
	}

	nbs.upstream = newUpstream
	oldTables := nbs.tables
	nbs.tables = newTables
	nbs.logger.WithField("new_upstream_len", len(nbs.tables.upstream)).Info("conjoin completed successfully")
	err = oldTables.close()
	if err != nil {
		nbs.logger.WithError(err).Warn("during conjoin, closing old table files failed with error")
		return
	}

	cleanup()
}

func (nbs *NomsBlockStore) UpdateManifest(ctx context.Context, updates map[hash.Hash]uint32) (ManifestInfo, error) {
	valctx.ValidateContext(ctx)
	sources, err := nbs.openChunkSourcesForAddTableFiles(ctx, updates)
	if err != nil {
		return manifestContents{}, err
	}
	// If these sources get added to the store, they will get cloned.
	// Either way, we want to close these instances when we are done.
	defer sources.sources.close()
	mi, _, err := nbs.updateManifestAddFiles(ctx, updates, nil, nil, sources.sources)
	return mi, err
}

func (nbs *NomsBlockStore) updateManifestAddFiles(ctx context.Context, updates map[hash.Hash]uint32, appendixOption *ManifestAppendixOption, gcGen *hash.Hash, sources chunkSourceSet) (mi ManifestInfo, gcGenDifferent bool, err error) {
	nbs.mu.Lock()
	defer nbs.mu.Unlock()

	nbs.manifestMgr.LockForUpdate()
	defer func() {
		err = errors.Join(err, nbs.manifestMgr.UnlockForUpdate())
	}()

	err = nbs.startConjoinIfRequired(ctx)
	if err != nil {
		return manifestContents{}, false, err
	}

	var updatedContents manifestContents
	for {
		ok, contents, _, ferr := nbs.manifestMgr.Fetch(ctx, nbs.stats)
		if ferr != nil {
			return manifestContents{}, false, ferr
		} else if !ok {
			contents = manifestContents{nbfVers: nbs.upstream.nbfVers}
		}

		if gcGen != nil && *gcGen != contents.gcGen {
			return manifestContents{}, true, nil
		}

		originalLock := contents.lock

		// Behavior:
		// If appendix == nil, we are appending to currSpecs and keeping the current appendix.
		// If *appendix == ManifestAppendixOption_Set, we are setting appendix to updates.
		// If *appendix == ManifestAppendixOption_Append, we are appending updates to appendix.

		currSpecs := contents.getSpecSet()
		currAppendixSpecs := contents.getAppendixSet()
		appendixSpecs := make([]tableSpec, 0)

		hasWork := (appendixOption != nil && *appendixOption == ManifestAppendixOption_Set)
		for h, count := range updates {
			if appendixOption == nil {
				if _, ok := currSpecs[h]; !ok {
					hasWork = true
					contents.specs = append(contents.specs, tableSpec{h, count})
				}
			} else if *appendixOption == ManifestAppendixOption_Set {
				hasWork = true
				appendixSpecs = append(appendixSpecs, tableSpec{h, count})
			} else if *appendixOption == ManifestAppendixOption_Append {
				if _, ok := currAppendixSpecs[h]; !ok {
					hasWork = true
					appendixSpecs = append(appendixSpecs, tableSpec{h, count})
				}
			} else {
				return manifestContents{}, false, ErrUnsupportedManifestAppendixOption
			}
		}

		if !hasWork {
			return contents, false, nil
		}

		if appendixOption == nil {
			// ensure we don't drop existing appendices
			if contents.appendix != nil && len(contents.appendix) > 0 {
				contents, err = fromManifestAppendixOptionNewContents(contents, contents.appendix, ManifestAppendixOption_Set)
				if err != nil {
					return manifestContents{}, false, err
				}
			}
		} else {
			contents, err = fromManifestAppendixOptionNewContents(contents, appendixSpecs, *appendixOption)
			if err != nil {
				return manifestContents{}, false, err
			}
		}

		contents.lock = generateLockHash(contents.root, contents.specs, contents.appendix, nil)

		updatedContents, err = nbs.manifestMgr.Update(ctx, originalLock, contents, nbs.stats, nil)
		if err != nil {
			return manifestContents{}, false, err
		}

		if updatedContents.lock == contents.lock {
			break
		}
	}

	var newTables tableSet
	newTables, err = nbs.tables.rebase(ctx, updatedContents.specs, sources, nbs.stats)
	if err != nil {
		return manifestContents{}, false, err
	}

	nbs.upstream = updatedContents
	oldTables := nbs.tables
	nbs.tables = newTables
	err = oldTables.close()
	if err != nil {
		return manifestContents{}, false, err
	}

	return updatedContents, false, nil
}

func (nbs *NomsBlockStore) UpdateManifestWithAppendix(ctx context.Context, updates map[hash.Hash]uint32, option ManifestAppendixOption) (ManifestInfo, error) {
	valctx.ValidateContext(ctx)
	sources, err := nbs.openChunkSourcesForAddTableFiles(ctx, updates)
	if err != nil {
		return manifestContents{}, err
	}
	// If these sources get added to the store, they will get cloned.
	// Either way, we want to close these instances when we are done.
	defer sources.sources.close()
	mi, _, err := nbs.updateManifestAddFiles(ctx, updates, &option, nil, sources.sources)
	return mi, err
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

		contents.lock = generateLockHash(contents.root, contents.specs, contents.appendix, nil)
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

		contents.lock = generateLockHash(contents.root, contents.specs, contents.appendix, nil)
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
	contents.lock = generateLockHash(contents.root, contents.specs, contents.appendix, nil)

	store.manifestMgr.LockForUpdate()
	defer func() {
		unlockErr := store.manifestMgr.UnlockForUpdate()

		if err == nil {
			err = unlockErr
		}
	}()
	updatedContents, err := store.manifestMgr.Update(ctx, store.upstream.lock, contents, store.stats, nil)
	if err != nil {
		return err
	}
	if updatedContents.lock != contents.lock {
		return ErrConcurrentManifestWriteDuringOverwrite
	}
	// We don't update |nbs.upstream| here since the tables have not been rebased
	return nil
}

func NewAWSStore(ctx context.Context, nbfVerStr string, table, ns, bucket string, s3 S3APIV2, ddb DynamoDBAPIV2, memTableSize uint64, q MemoryQuotaProvider) (*NomsBlockStore, error) {
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
		manifestMgr: mm,
		persister:   p,
		conjoiner:   c,
		tables:      newTableSet(p, q),
		upstream:    manifestContents{nbfVers: nbfVerStr},
		memtableSz:  memTableSize,
		hasCache:    hasCache,
		stats:       NewStats(),
		logger:      logrus.StandardLogger().WithField("pkg", "store.noms"),
	}
	nbs.gcCond = sync.NewCond(&nbs.mu)
	nbs.conjoinOpCond = sync.NewCond(&nbs.mu)

	t1 := time.Now()
	defer nbs.stats.OpenLatency.SampleTimeSince(t1)

	exists, contents, _, err := nbs.manifestMgr.Fetch(ctx, nbs.stats)

	if err != nil {
		return nil, err
	}

	if exists {
		newTables, err := nbs.tables.rebase(ctx, contents.specs, nil, nbs.stats)

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

// Sets logging fields for the logger used by this store.
func (nbs *NomsBlockStore) AppendLoggerFields(fields logrus.Fields) {
	nbs.logger = nbs.logger.WithFields(fields)
}

// Wait for GC to complete to continue with ongoing operations.
// Called with nbs.mu held. When this function returns with a nil
// error, gcInProgress will be false.
func (nbs *NomsBlockStore) waitForGC(ctx context.Context) error {
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			nbs.gcCond.Broadcast()
		case <-stop:
		}
	}()
	for nbs.gcInProgress && ctx.Err() == nil {
		nbs.gcCond.Wait()
	}
	return ctx.Err()
}

func (nbs *NomsBlockStore) Put(ctx context.Context, c chunks.Chunk, getAddrs chunks.GetAddrsCurry) error {
	valctx.ValidateContext(ctx)
	return nbs.putChunk(ctx, c, getAddrs, nbs.refCheck)
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
		nbs.memtable = nil
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
	for _, e := range nbs.memtable.pendingRefs {
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
		if nbs.memtable == nil {
			nbs.memtable = newMemTable(nbs.memtableSz)
		}

		addChunkRes = nbs.memtable.addChunk(ch.Hash(), ch.Data())
		if addChunkRes == chunkNotAdded {
			ts, gcb, err := nbs.tables.append(ctx, nbs.memtable, checker, nbs.keeperFunc, nbs.hasCache, nbs.stats)
			if err != nil {
				nbs.handlePossibleDanglingRefError(err)
				return false, err
			}
			if gcb == gcBehavior_Block {
				retry = true
				if err := nbs.waitForGC(ctx); err != nil {
					return false, err
				}
				continue
			}
			nbs.addPendingRefsToHasCache()
			nbs.tables = ts
			nbs.memtable = newMemTable(nbs.memtableSz)
			addChunkRes = nbs.memtable.addChunk(ch.Hash(), ch.Data())
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
			nbs.memtable.addGetChildRefs(getAddrs(ch))
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
	valctx.ValidateContext(ctx)
	ctx, span := tracer.Start(ctx, "nbs.Get")
	defer span.End()

	t1 := time.Now()
	defer func() {
		nbs.stats.GetLatency.SampleTimeSince(t1)
		nbs.stats.ChunksPerGet.Sample(1)
	}()

	for {
		nbs.mu.Lock()
		if nbs.memtable != nil {
			data, gcb, err := nbs.memtable.get(ctx, h, nbs.keeperFunc, nbs.stats)
			if err != nil {
				nbs.mu.Unlock()
				return chunks.EmptyChunk, err
			}
			if gcb == gcBehavior_Block {
				err = nbs.waitForGC(ctx)
				nbs.mu.Unlock()
				if err != nil {
					return chunks.EmptyChunk, err
				}
				continue
			}
			if data != nil {
				nbs.mu.Unlock()
				return chunks.NewChunkWithHash(h, data), nil
			}
		}
		tables, keeper, endRead := nbs.tables, nbs.keeperFunc, nbs.beginRead()
		nbs.mu.Unlock()

		data, gcb, err := tables.get(ctx, h, keeper, nbs.stats)
		needContinue, err := nbs.handleUnlockedRead(ctx, gcb, true, endRead, err)
		if err != nil {
			return chunks.EmptyChunk, err
		}
		if needContinue {
			continue
		}

		if data != nil {
			return chunks.NewChunkWithHash(h, data), nil
		}
		return chunks.EmptyChunk, nil
	}
}

func (nbs *NomsBlockStore) GetMany(ctx context.Context, hashes hash.HashSet, found func(context.Context, *chunks.Chunk)) error {
	valctx.ValidateContext(ctx)
	ctx, span := tracer.Start(ctx, "nbs.GetMany", trace.WithAttributes(attribute.Int("num_hashes", len(hashes))))
	defer span.End()
	return nbs.getManyWithFunc(ctx, hashes, gcDependencyMode_TakeDependency,
		func(ctx context.Context, cr chunkReader, eg *errgroup.Group, reqs []getRecord, keeper keeperF, stats *Stats) (bool, gcBehavior, error) {
			return cr.getMany(ctx, eg, reqs, found, keeper, nbs.stats)
		},
	)
}

func (nbs *NomsBlockStore) GetManyCompressed(ctx context.Context, hashes hash.HashSet, found func(context.Context, ToChunker)) error {
	valctx.ValidateContext(ctx)
	return nbs.getManyCompressed(ctx, hashes, found, gcDependencyMode_TakeDependency)
}

func (nbs *NomsBlockStore) getManyCompressed(ctx context.Context, hashes hash.HashSet, found func(context.Context, ToChunker), gcDepMode gcDependencyMode) error {
	ctx, span := tracer.Start(ctx, "nbs.GetManyCompressed", trace.WithAttributes(attribute.Int("num_hashes", len(hashes))))
	defer span.End()
	return nbs.getManyWithFunc(ctx, hashes, gcDepMode,
		func(ctx context.Context, cr chunkReader, eg *errgroup.Group, reqs []getRecord, keeper keeperF, stats *Stats) (bool, gcBehavior, error) {
			return cr.getManyCompressed(ctx, eg, reqs, found, keeper, nbs.stats)
		},
	)
}

func (nbs *NomsBlockStore) getManyWithFunc(
	ctx context.Context,
	hashes hash.HashSet,
	gcDepMode gcDependencyMode,
	getManyFunc func(ctx context.Context, cr chunkReader, eg *errgroup.Group, reqs []getRecord, keeper keeperF, stats *Stats) (bool, gcBehavior, error),
) error {
	if len(hashes) == 0 {
		return nil
	}

	t1 := time.Now()
	defer func() {
		nbs.stats.GetLatency.SampleTimeSince(t1)
		nbs.stats.ChunksPerGet.Sample(uint64(len(hashes)))
	}()

	reqs := toGetRecords(hashes)

	const ioParallelism = 16
	for {
		nbs.mu.Lock()
		keeper := nbs.keeperFunc
		if gcDepMode == gcDependencyMode_NoDependency {
			keeper = nil
		}
		if nbs.memtable != nil {
			// nbs.mt does not use the errgroup parameter, which we pass at |nil| here.
			remaining, gcb, err := getManyFunc(ctx, nbs.memtable, nil, reqs, keeper, nbs.stats)
			if err != nil {
				nbs.mu.Unlock()
				return err
			}
			if gcb == gcBehavior_Block {
				err = nbs.waitForGC(ctx)
				nbs.mu.Unlock()
				if err != nil {
					return err
				}
				continue
			}
			if !remaining {
				nbs.mu.Unlock()
				return nil
			}
		}
		tables, endRead := nbs.tables, nbs.beginRead()
		nbs.mu.Unlock()

		gcb, err := func() (gcBehavior, error) {
			eg, ctx := errgroup.WithContext(ctx)
			eg.SetLimit(ioParallelism)
			_, gcb, err := getManyFunc(ctx, tables, eg, reqs, keeper, nbs.stats)
			return gcb, errors.Join(err, eg.Wait())
		}()
		needContinue, err := nbs.handleUnlockedRead(ctx, gcb, true, endRead, err)
		if err != nil {
			return err
		}
		if needContinue {
			continue
		}

		return nil
	}
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
		if nbs.memtable != nil {
			count, err = nbs.memtable.count()
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
	valctx.ValidateContext(ctx)
	t1 := time.Now()
	defer func() {
		nbs.stats.HasLatency.SampleTimeSince(t1)
		nbs.stats.AddressesPerHas.Sample(1)
	}()

	for {
		nbs.mu.Lock()
		if nbs.memtable != nil {
			has, gcb, err := nbs.memtable.has(h, nbs.keeperFunc)
			if err != nil {
				nbs.mu.Unlock()
				return false, err
			}
			if gcb == gcBehavior_Block {
				err = nbs.waitForGC(ctx)
				nbs.mu.Unlock()
				if err != nil {
					return false, err
				}
				continue
			}
			if has {
				nbs.mu.Unlock()
				return true, nil
			}
		}
		tables, keeper, endRead := nbs.tables, nbs.keeperFunc, nbs.beginRead()
		nbs.mu.Unlock()

		has, gcb, err := tables.has(h, keeper)
		needsContinue, err := nbs.handleUnlockedRead(ctx, gcb, true, endRead, err)
		if err != nil {
			return false, err
		}
		if needsContinue {
			continue
		}

		return has, nil
	}
}

func (nbs *NomsBlockStore) HasMany(ctx context.Context, hashes hash.HashSet) (hash.HashSet, error) {
	valctx.ValidateContext(ctx)
	return nbs.hasManyDep(ctx, hashes, gcDependencyMode_TakeDependency)
}

func (nbs *NomsBlockStore) hasManyDep(ctx context.Context, hashes hash.HashSet, gcDepMode gcDependencyMode) (hash.HashSet, error) {
	if hashes.Size() == 0 {
		return nil, nil
	}

	t1 := time.Now()
	defer func() {
		nbs.stats.HasLatency.SampleTimeSince(t1)
		nbs.stats.AddressesPerHas.SampleLen(hashes.Size())
	}()

	for {
		reqs := toHasRecords(hashes)

		nbs.mu.Lock()
		if nbs.memtable != nil {
			keeper := nbs.keeperFunc
			if gcDepMode == gcDependencyMode_NoDependency {
				keeper = nil
			}
			remaining, gcb, err := nbs.memtable.hasMany(reqs, keeper)
			if err != nil {
				nbs.mu.Unlock()
				return nil, err
			}
			if gcb == gcBehavior_Block {
				err = nbs.waitForGC(ctx)
				nbs.mu.Unlock()
				if err != nil {
					return nil, err
				}
				continue
			}
			if !remaining {
				nbs.mu.Unlock()
				return hash.HashSet{}, nil
			}
		}
		tables, keeper, endRead := nbs.tables, nbs.keeperFunc, nbs.beginRead()
		if gcDepMode == gcDependencyMode_NoDependency {
			keeper = nil
		}
		nbs.mu.Unlock()

		remaining, gcb, err := tables.hasMany(reqs, keeper)
		needContinue, err := nbs.handleUnlockedRead(ctx, gcb, true, endRead, err)
		if err != nil {
			return nil, err
		}
		if needContinue {
			continue
		}

		if !remaining {
			return hash.HashSet{}, nil
		}

		absent := hash.HashSet{}
		for _, r := range reqs {
			if !r.has {
				absent.Insert(*r.a)
			}
		}
		return absent, nil
	}
}

// Operates a lot like |hasMany|, but without locking and without
// taking read dependencies on the checked references. Should only be
// used for the sanity checking on references for written chunks.
func (nbs *NomsBlockStore) refCheck(reqs []hasRecord) (hash.HashSet, error) {
	if nbs.memtable != nil {
		remaining, _, err := nbs.memtable.hasMany(reqs, nil)
		if err != nil {
			return nil, err
		}
		if !remaining {
			return hash.HashSet{}, nil
		}
	}

	remaining, _, err := nbs.tables.hasMany(reqs, nil)
	if err != nil {
		return nil, err
	}
	if !remaining {
		return hash.HashSet{}, nil
	}

	absent := hash.HashSet{}
	for _, r := range reqs {
		if !r.has {
			absent.Insert(*r.a)
		}
	}
	return absent, nil
}

// Only used for a generational full GC, where the table files are
// added to the store and are then used to filter which chunks need to
// make it to the new generation. In this context, we do not need to
// worry about taking read dependencies on the requested chunks. Hence
// our handling of keeperFunc and gcBehavior below.
func (nbs *NomsBlockStore) hasManyInSources(srcs []hash.Hash, hashes hash.HashSet) (hash.HashSet, error) {
	if hashes.Size() == 0 {
		return nil, nil
	}

	t1 := time.Now()
	defer nbs.stats.HasLatency.SampleTimeSince(t1)
	nbs.stats.AddressesPerHas.SampleLen(hashes.Size())

	nbs.mu.RLock()
	defer nbs.mu.RUnlock()

	records := toHasRecords(hashes)

	_, _, err := nbs.tables.hasManyInSources(srcs, records, nil)
	if err != nil {
		return nil, err
	}

	absent := hash.HashSet{}
	for _, r := range records {
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
	valctx.ValidateContext(ctx)
	nbs.mu.Lock()
	defer nbs.mu.Unlock()
	return nbs.rebase(ctx)
}

func (nbs *NomsBlockStore) rebase(ctx context.Context) error {
	exists, contents, _, err := nbs.manifestMgr.Fetch(ctx, nbs.stats)
	if err != nil {
		return err
	}

	if exists {
		if contents.lock == nbs.upstream.lock {
			// short-circuit if manifest is unchanged
			return nil
		}

		newTables, err := nbs.tables.rebase(ctx, contents.specs, nil, nbs.stats)
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
	valctx.ValidateContext(ctx)
	nbs.mu.RLock()
	defer nbs.mu.RUnlock()
	return nbs.upstream.root, nil
}

func (nbs *NomsBlockStore) Commit(ctx context.Context, current, last hash.Hash) (success bool, err error) {
	valctx.ValidateContext(ctx)
	return nbs.commit(ctx, current, last, nbs.refCheck)
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

	anyPossiblyNovelChunks := nbs.memtable != nil || len(nbs.tables.novel) > 0

	if !anyPossiblyNovelChunks && current == last {
		err := nbs.rebase(ctx)
		if err != nil {
			return false, err
		}
		return true, nil
	}

	nbs.manifestMgr.LockForUpdate()
	defer func() {
		unlockErr := nbs.manifestMgr.UnlockForUpdate()

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
		newTables, err := nbs.tables.rebase(ctx, upstream.specs, nil, nbs.stats)
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

	if cached, doomed := nbs.manifestMgr.updateWillFail(nbs.upstream.lock); doomed {
		// Pre-emptive optimistic lock failure. Someone else in-process moved to the root, the set of tables, or both out from under us.
		return handleOptimisticLockFailure(cached)
	}

	for {
		if nbs.memtable != nil {
			cnt, err := nbs.memtable.count()
			if err != nil {
				return err
			}
			if cnt > 0 {
				ts, gcb, err := nbs.tables.append(ctx, nbs.memtable, checker, nbs.keeperFunc, nbs.hasCache, nbs.stats)
				if err != nil {
					nbs.handlePossibleDanglingRefError(err)
					return err
				}
				if gcb == gcBehavior_Block {
					err = nbs.waitForGC(ctx)
					if err != nil {
						return err
					}
					continue
				}
				nbs.addPendingRefsToHasCache()
				nbs.tables, nbs.memtable = ts, nil
			}
		}
		break
	}

	err := nbs.startConjoinIfRequired(ctx)
	if err != nil {
		return err
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
		lock:     generateLockHash(current, specs, appendixSpecs, nil),
		gcGen:    nbs.upstream.gcGen,
		specs:    specs,
		appendix: appendixSpecs,
	}

	upstream, err := nbs.manifestMgr.Update(ctx, nbs.upstream.lock, newContents, nbs.stats, nil)
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
	return nbs.persister.AccessMode()
}

func (nbs *NomsBlockStore) Close() error {
	nbs.mu.Lock()
	defer nbs.mu.Unlock()
	for nbs.conjoinOp != nil {
		nbs.conjoinOpCond.Wait()
	}
	err := nbs.persister.Close()
	err = errors.Join(err, nbs.tables.close())
	err = errors.Join(err, nbs.manifestMgr.Close())
	return err
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
	info   TableSpecInfo
	open   func(ctx context.Context) (io.ReadCloser, uint64, error)
	suffix string
}

// LocationPrefix
func (tf tableFile) LocationPrefix() string {
	return ""
}

func (tf tableFile) LocationSuffix() string {
	return tf.suffix
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
	valctx.ValidateContext(ctx)
	nbs.mu.Lock()
	defer nbs.mu.Unlock()

	exists, contents, err := nbs.manifestMgr.m.ParseIfExists(ctx, nbs.stats, nil)

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
	s := ""
	if _, ok := cs.(archiveChunkSource); ok {
		s = ArchiveFileSuffix
	}

	return tableFile{
		info: info,
		open: func(ctx context.Context) (io.ReadCloser, uint64, error) {
			r, s, err := cs.reader(ctx)
			if err != nil {
				return nil, 0, err
			}
			return r, s, nil
		},
		suffix: s,
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
	_, ok = nbs.persister.(tableFilePersister)

	return chunks.TableFileStoreOps{
		CanRead:  true,
		CanWrite: ok,
		CanPrune: ok,
		CanGC:    ok,
	}
}

func (nbs *NomsBlockStore) Path() (string, bool) {
	if tfp, ok := nbs.persister.(tableFilePersister); ok {
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
func (nbs *NomsBlockStore) WriteTableFile(ctx context.Context, fileName string, numChunks int, contentHash []byte, getRd func() (io.ReadCloser, uint64, error)) error {
	valctx.ValidateContext(ctx)
	tfp, ok := nbs.persister.(tableFilePersister)
	if !ok {
		return errors.New("Not implemented")
	}

	r, sz, err := getRd()
	if err != nil {
		return err
	}
	defer r.Close()
	return tfp.CopyTableFile(ctx, r, fileName, sz, uint32(numChunks))
}

// AddTableFilesToManifest adds table files to the manifest
func (nbs *NomsBlockStore) AddTableFilesToManifest(ctx context.Context, fileIdToNumChunks map[string]int, getAddrs chunks.GetAddrsCurry) error {
	valctx.ValidateContext(ctx)
	return nbs.addTableFilesToManifest(ctx, fileIdToNumChunks, getAddrs, nbs.refCheck)
}

// A small helper which returns a composite refCheck function combining |base| and a |hasMany| call against each source in |css|.
func getRefCheck(nbs *NomsBlockStore, base func([]hasRecord) (hash.HashSet, error), css chunkSourceSet) func([]hasRecord) (hash.HashSet, error) {
	if base == nil {
		return nil
	}
	return func(recs []hasRecord) (hash.HashSet, error) {
		nbs.mu.Lock()
		absent, err := base(recs)
		nbs.mu.Unlock()
		if err != nil {
			return nil, err
		}
		if len(absent) == 0 {
			return nil, nil
		}

		// Still some missing. Check the chunkSourceSet.
		for _, cs := range css {
			remaining, _, err := cs.hasMany(recs, nil)
			if err != nil {
				return nil, err
			}
			if !remaining {
				return nil, nil
			}
		}

		ret := hash.HashSet{}
		for _, r := range recs {
			if !r.has {
				ret.Insert(*r.a)
			}
		}
		return ret, nil
	}
}

// For each chunk in sources, walk all its addresses with |getAddrs| and ensure that none are missing from a call to |refCheck|.
func refCheckAllSources(ctx context.Context, getAddrs chunks.GetAddrsCurry, refCheck func([]hasRecord) (hash.HashSet, error), sources chunkSourceSet, stats *Stats) error {
	if refCheck == nil {
		return nil
	}
	var checkErr error
	// TODO: Bigger batching.
	checkDeps := func(c chunks.Chunk) {
		if checkErr != nil {
			return
		}
		addrs := make(hash.HashSet)
		getAddrs(c)(ctx, addrs, func(hash.Hash) bool { return false })
		remaining, err := refCheck(toHasRecords(addrs))
		if err != nil {
			checkErr = err
		}
		if len(remaining) > 0 {
			checkErr = fmt.Errorf("cannot add table files: %w, missing: %v", ErrTableFileNotFound, remaining)
		}
	}
	for _, source := range sources {
		err := source.iterateAllChunks(ctx, func(c chunks.Chunk) {
			if checkErr != nil {
				return
			}
			checkDeps(c)
		}, stats)
		if err != nil {
			checkErr = errors.Join(err, checkErr)
		}
		if checkErr != nil {
			break
		}
	}
	return checkErr
}

func (nbs *NomsBlockStore) addTableFilesToManifest(ctx context.Context, fileIdToNumChunks map[string]int, getAddrs chunks.GetAddrsCurry, genRefCheck func([]hasRecord) (hash.HashSet, error)) error {
	fileIdHashToNumChunks := make(map[hash.Hash]uint32)
	for fileId, numChunks := range fileIdToNumChunks {
		fileIdHash, ok := hash.MaybeParse(fileId)
		if !ok {
			return errors.New("invalid base32 encoded hash: " + fileId)
		}
		if numChunks > 0 {
			fileIdHashToNumChunks[fileIdHash] = uint32(numChunks)
		}
	}

	if len(fileIdHashToNumChunks) == 0 {
		return nil
	}

	for {
		// Open chunk sources for each of the fileIdHashToNumChunks files.
		// This is done with the lock held. We also inspect |gcGen| at this
		// time so that we can ensure our view of the store is still
		// accurate when we add these files to the store if all the sanity
		// checks pass.
		sources, err := nbs.openChunkSourcesForAddTableFiles(ctx, fileIdHashToNumChunks)
		if err != nil {
			return fmt.Errorf("addTableFiles, openChunkSources: %w", err)
		}
		// If these sources get added to the store, they will get cloned.
		// Either way, we want to close these instances when we are done.
		defer sources.sources.close()

		// If we are an uninitialized store, we do not perform
		// this ref check.  This is a hascky solve for the
		// case where there is a push or clone into a remote
		// newly initialized store with no existing contents.
		//
		// For example, call dolt_backup(sync-url, aws://.../new_backup_path_here
		// needs to be efficient.
		//
		// We should make these ref checks more efficient and
		// more performant as well, so that incremental
		// backups are more credible for example.
		if !sources.root.IsEmpty() {
			refCheck := getRefCheck(nbs, genRefCheck, sources.sources)
			err = refCheckAllSources(ctx, getAddrs, refCheck, sources.sources, nbs.stats)
			if err != nil {
				// There was an error checking all references.
				return fmt.Errorf("addTableFiles, refCheckAllSources: %w", err)
			}
		}

		// At this point, the added files are consistent with our view of the store.
		// We add them to the set of table files in the store.
		_, gcGenMismatch, err := nbs.updateManifestAddFiles(ctx, fileIdHashToNumChunks, nil, &sources.gcGen, sources.sources)
		if err != nil {
			return fmt.Errorf("addTableFiles, updateManifestAddFiles: %w", err)
		} else if gcGenMismatch {
			// A gcGenMismatch means that the store has changed out from under
			// us as we were running these checks. We want to retry.
			continue
		}

		// At this point, the files have been added to the
		// store and the store was in a consistent state when
		// we added them.
		//
		// We currently do not take any chunk dependencies
		// for a GC process on the chunks that are added in
		// these files. In practice, that means that all the
		// dependencies are taken at once, in the |Commit|
		// call setting the new root.
		return nil
	}
}

type openChunkSourcesForAddTableFilesResult struct {
	sources chunkSourceSet
	gcGen   hash.Hash
	root    hash.Hash
}

func (nbs *NomsBlockStore) openChunkSourcesForAddTableFiles(ctx context.Context, files map[hash.Hash]uint32) (openChunkSourcesForAddTableFilesResult, error) {
	nbs.mu.Lock()
	defer nbs.mu.Unlock()
	sources, err := nbs.tables.openForAdd(ctx, files, nbs.stats)
	if err != nil {
		return openChunkSourcesForAddTableFilesResult{}, err
	}
	return openChunkSourcesForAddTableFilesResult{
		sources: sources,
		gcGen:   nbs.upstream.gcGen,
		root:    nbs.upstream.root,
	}, nil
}

// PruneTableFiles deletes old table files that are no longer referenced in the manifest.
func (nbs *NomsBlockStore) PruneTableFiles(ctx context.Context) (err error) {
	valctx.ValidateContext(ctx)
	return nbs.pruneTableFiles(ctx)
}

func (nbs *NomsBlockStore) pruneTableFiles(ctx context.Context) (err error) {
	mtime := time.Now()
	return nbs.persister.PruneTableFiles(ctx, func() []hash.Hash {
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

func (nbs *NomsBlockStore) BeginGC(keeper func(hash.Hash) bool, _ chunks.GCMode) error {
	nbs.mu.Lock()
	defer nbs.mu.Unlock()
	// Block until there is no ongoing conjoin...
	for nbs.conjoinOp != nil {
		nbs.conjoinOpCond.Wait()
	}
	if nbs.gcInProgress {
		return errors.New("gc already in progress")
	}
	nbs.gcInProgress = true
	nbs.keeperFunc = keeper
	nbs.gcCond.Broadcast()
	return nil
}

func (nbs *NomsBlockStore) EndGC(_ chunks.GCMode) {
	nbs.gcCond.L.Lock()
	defer nbs.gcCond.L.Unlock()
	if !nbs.gcInProgress {
		panic("EndGC called when gc was not in progress")
	}
	for nbs.gcOutstandingReads > 0 {
		nbs.gcCond.Wait()
	}
	nbs.gcInProgress = false
	nbs.keeperFunc = nil
	nbs.gcCond.Broadcast()
}

// beginRead() is called with |nbs.mu| held. It signals an ongoing
// read operation which will be operating against the existing table
// files without |nbs.mu| held. The read should be bracket with a call
// to the returned |endRead|, which must be called with |nbs.mu| held
// if it is non-|nil|, and should not be called otherwise.
//
// If there is an ongoing GC operation which this call is made, it is
// guaranteed not to complete until the corresponding |endRead| call.
func (nbs *NomsBlockStore) beginRead() (endRead func()) {
	if nbs.gcInProgress {
		nbs.gcOutstandingReads += 1
		return func() {
			nbs.gcOutstandingReads -= 1
			if nbs.gcOutstandingReads < 0 {
				panic("impossible")
			}
			nbs.gcCond.Broadcast()
		}
	}
	return nil
}

func (nbs *NomsBlockStore) MarkAndSweepChunks(ctx context.Context, getAddrs chunks.GetAddrsCurry, filter chunks.HasManyFunc, dest chunks.ChunkStore, mode chunks.GCMode, cmp chunks.GCArchiveLevel) (chunks.MarkAndSweeper, error) {
	valctx.ValidateContext(ctx)
	return markAndSweepChunks(ctx, nbs, nbs, dest, getAddrs, filter, mode, cmp)
}

// Returns true if this NomsBlockStore instance is carrying local
// changes which wouldn't be reflected in a GCGen.
func (nbs *NomsBlockStore) hasLocalGCNovelty() bool {
	if nbs.memtable != nil {
		return true
	}
	if len(nbs.tables.novel) != 0 {
		return true
	}
	if cj := nbs.ChunkJournal(); cj != nil && cj.wr != nil {
		return true
	}
	return false
}

func markAndSweepChunks(_ context.Context, nbs *NomsBlockStore, src CompressedChunkStoreForGC, dest chunks.ChunkStore, getAddrs chunks.GetAddrsCurry, filter chunks.HasManyFunc, mode chunks.GCMode, cmp chunks.GCArchiveLevel) (chunks.MarkAndSweeper, error) {
	ops := nbs.SupportedOperations()
	if !ops.CanGC || !ops.CanPrune {
		return nil, chunks.ErrUnsupportedOperation
	}

	precheck := func() error {
		nbs.mu.RLock()
		defer nbs.mu.RUnlock()

		sameSpecs := func(upstreamSrcs chunkSourceSet, contents manifestContents) bool {
			upstreams := make(map[hash.Hash]struct{})
			for k := range upstreamSrcs {
				upstreams[k] = struct{}{}
			}
			for _, spec := range contents.specs {
				delete(upstreams, spec.name)
			}
			return len(upstreams) == 0
		}

		if nbs.hasLocalGCNovelty() || !sameSpecs(nbs.tables.upstream, nbs.upstream) {
			// If we have any novelty or if we have different table files than
			// upstream, the precheck always succeeds and we go ahead and run
			// a GC.
			return nil
		}

		// Check to see if the specs have changed since last gc. If they haven't bail early.
		gcGenCheck := generateLockHash(nbs.upstream.root, nbs.upstream.specs, nbs.upstream.appendix, []byte("full"))
		if nbs.upstream.gcGen == gcGenCheck {
			return chunks.ErrNothingToCollect
		}
		if mode != chunks.GCMode_Full {
			// Allow a non-full GC to match the no-op work check as well.
			gcGenCheck := generateLockHash(nbs.upstream.root, nbs.upstream.specs, nbs.upstream.appendix, nil)
			if nbs.upstream.gcGen == gcGenCheck {
				return chunks.ErrNothingToCollect
			}
		}
		return nil
	}
	err := precheck()
	if err != nil {
		return nil, err
	}

	var destNBS *NomsBlockStore
	if dest != nil {
		switch typed := dest.(type) {
		case *NomsBlockStore:
			destNBS = typed
		case NBSMetricWrapper:
			destNBS = typed.nbs
		default:
			return nil, fmt.Errorf("cannot MarkAndSweep into a non-NomsBlockStore ChunkStore: %w", chunks.ErrUnsupportedOperation)
		}
	} else {
		destNBS = nbs
	}

	tfp, ok := destNBS.persister.(tableFilePersister)
	if !ok {
		return nil, fmt.Errorf("NBS does not support copying garbage collection")
	}

	gcc, err := newGarbageCollectionCopier(cmp, tfp)
	if err != nil {
		return nil, err
	}

	return &markAndSweeper{
		src:      src,
		dest:     destNBS,
		getAddrs: getAddrs,
		filter:   filter,
		visited:  make(hash.HashSet),
		tfp:      tfp,
		gcc:      gcc,
		mode:     mode,
	}, nil
}

type markAndSweeper struct {
	src      CompressedChunkStoreForGC
	dest     *NomsBlockStore
	getAddrs chunks.GetAddrsCurry
	filter   chunks.HasManyFunc

	visited hash.HashSet

	tfp  tableFilePersister
	gcc  *gcCopier
	mode chunks.GCMode
}

func (i *markAndSweeper) SaveHashes(ctx context.Context, hashes []hash.Hash) error {
	valctx.ValidateContext(ctx)
	toVisit := make(hash.HashSet, len(hashes))
	for _, h := range hashes {
		if _, ok := i.visited[h]; !ok {
			toVisit.Insert(h)
		}
	}
	var err error
	var mu sync.Mutex
	first := true
	for {
		// We manually check context here, because in some cases
		// the work we are doing here does not result in a timely
		// error once the context is canceled.
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}

		if !first {
			copy := toVisit.Copy()
			for h := range toVisit {
				if _, ok := i.visited[h]; ok {
					delete(copy, h)
				}
			}
			toVisit = copy
		}

		toVisit, err = i.filter(ctx, toVisit)
		if err != nil {
			return err
		}
		if len(toVisit) == 0 {
			break
		}

		first = false
		nextToVisit := make(hash.HashSet)

		found := 0
		var addErr error
		err = i.src.getManyCompressed(ctx, toVisit, func(ctx context.Context, tc ToChunker) {
			mu.Lock()
			defer mu.Unlock()
			if addErr != nil {
				return
			}
			found += 1
			if tc.IsGhost() {
				// Ghost chunks encountered on the walk can be left alone --- they
				// do not bring their dependencies, and because of how generational
				// store works, they will still be ghost chunks
				// in the store after the GC is finished.
				return
			}
			addErr = i.gcc.addChunk(ctx, tc)
			if addErr != nil {
				return
			}
			c, err := tc.ToChunk()
			if err != nil {
				addErr = err
				return
			}
			addErr = i.getAddrs(c)(ctx, nextToVisit, func(h hash.Hash) bool { return false })
		}, gcDependencyMode_NoDependency)
		if err != nil {
			return fmt.Errorf("SaveHashes, error calling getManyCompressed: %w", err)
		}
		if addErr != nil {
			return fmt.Errorf("SaveHashes, error calling gcc.addChunk: %w", addErr)
		}
		if found != len(toVisit) {
			return fmt.Errorf("dangling references requested during GC. GC not successful. %v", toVisit)
		}

		i.visited.InsertAll(toVisit)

		toVisit = nextToVisit
	}
	return nil
}

func (i *markAndSweeper) Finalize(ctx context.Context) (chunks.GCFinalizer, error) {
	valctx.ValidateContext(ctx)
	specs, err := i.gcc.copyTablesToDir(ctx)
	if err != nil {
		return nil, err
	}
	i.gcc = nil

	return gcFinalizer{
		nbs:   i.dest,
		specs: specs,
		mode:  i.mode,
	}, nil
}

func (i *markAndSweeper) Close(ctx context.Context) error {
	if i.gcc != nil {
		return i.gcc.cancel(ctx)
	}
	return nil
}

type gcFinalizer struct {
	nbs   *NomsBlockStore
	specs []tableSpec
	mode  chunks.GCMode
}

func (gcf gcFinalizer) AddChunksToStore(ctx context.Context) (chunks.HasManyFunc, error) {
	fileIdToNumChunks := tableSpecsToMap(gcf.specs)
	var addrs []hash.Hash
	for _, spec := range gcf.specs {
		addrs = append(addrs, spec.name)
	}
	f := func(ctx context.Context, hashes hash.HashSet) (hash.HashSet, error) {
		return gcf.nbs.hasManyInSources(addrs, hashes)
	}
	// Passing |nil| for getAddrs and |refCheck| means ref checking on
	// this add is off.
	return f, gcf.nbs.addTableFilesToManifest(ctx, fileIdToNumChunks, nil, nil)
}

func (gcf gcFinalizer) SwapChunksInStore(ctx context.Context) error {
	return gcf.nbs.swapTables(ctx, gcf.specs, gcf.mode)
}

func (nbs *NomsBlockStore) IterateAllChunks(ctx context.Context, cb func(chunk chunks.Chunk)) error {
	for _, v := range nbs.tables.novel {
		err := v.iterateAllChunks(ctx, cb, nbs.stats)
		if err != nil {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	for _, v := range nbs.tables.upstream {
		err := v.iterateAllChunks(ctx, cb, nbs.stats)
		if err != nil {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	return nil
}

func (nbs *NomsBlockStore) swapTables(ctx context.Context, specs []tableSpec, mode chunks.GCMode) (err error) {
	nbs.mu.Lock()
	defer nbs.mu.Unlock()

	nbs.manifestMgr.LockForUpdate()
	defer func() {
		unlockErr := nbs.manifestMgr.UnlockForUpdate()
		if err == nil {
			err = unlockErr
		}
	}()

	newLock := generateLockHash(nbs.upstream.root, specs, []tableSpec{}, nil)
	var extra []byte
	if mode == chunks.GCMode_Full {
		extra = []byte("full")
	}
	newGCGen := generateLockHash(nbs.upstream.root, specs, []tableSpec{}, extra)

	newContents := manifestContents{
		nbfVers: nbs.upstream.nbfVers,
		root:    nbs.upstream.root,
		lock:    newLock,
		gcGen:   newGCGen,
		specs:   specs,
	}

	sameGcGen := newContents.gcGen == nbs.upstream.gcGen
	hasLocalNovelty := nbs.hasLocalGCNovelty()
	if sameGcGen && !hasLocalNovelty {
		// Nothing has changed. Bail early.
		return nil
	}

	upstream, err := nbs.manifestMgr.UpdateGCGen(ctx, nbs.upstream.lock, newContents, nbs.stats, nil)
	if err != nil {
		return err
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
	ts, err := nbs.tables.rebase(ctx, upstream.specs, nil, nbs.stats)
	if err != nil {
		return fmt.Errorf("swapTables, rebase: %w", err)
	}
	oldTables := nbs.tables
	nbs.tables, nbs.upstream = ts, upstream
	err = oldTables.close()
	if err != nil {
		return fmt.Errorf("swapTables, oldTables.close(): %w", err)
	}

	// When this is called, we are at a safepoint in the GC process.
	// We clear novel and the memtable, which are not coming with us
	// into the new store.
	oldNovel := nbs.tables.novel
	nbs.tables.novel = make(chunkSourceSet)
	for _, css := range oldNovel {
		err = css.close()
		if err != nil {
			return fmt.Errorf("swapTables, oldNovel css.close(), err: %w", err)
		}
	}
	nbs.memtable = nil
	return nil
}

// CalcReads computes the number of IO operations necessary to fetch |hashes|.
func CalcReads(nbs *NomsBlockStore, hashes hash.HashSet, blockSize uint64, keeper keeperF) (int, bool, gcBehavior, error) {
	reqs := toGetRecords(hashes)
	tables := func() (tables tableSet) {
		nbs.mu.RLock()
		defer nbs.mu.RUnlock()
		tables = nbs.tables

		return
	}()

	reads, split, remaining, gcb, err := tableSetCalcReads(tables, reqs, blockSize, keeper)
	if err != nil {
		return 0, false, gcb, err
	}
	if gcb != gcBehavior_Continue {
		return 0, false, gcb, nil
	}

	if remaining {
		return 0, false, gcBehavior_Continue, errors.New("failed to find all chunks")
	}

	return reads, split, gcb, err
}

// ConjoinTableFiles conjoins the specified table files into a single new table file.
// The storageIds slice contains the hash IDs of the table files to conjoin.
// Returns the hash of the newly created conjoined table file.
func (nbs *NomsBlockStore) ConjoinTableFiles(ctx context.Context, storageIds []hash.Hash) (hash.Hash, error) {
	nbs.mu.RLock()
	defer nbs.mu.RUnlock()

	// If no storageIds provided, collect all table files from the current table set
	if len(storageIds) == 0 {
		for _, tf := range nbs.tables.upstream {
			storageIds = append(storageIds, tf.hash())
		}

		if len(storageIds) == 0 {
			return hash.Hash{}, errors.New("no table files to conjoin")
		}
	}

	// Convert storage IDs to chunkSources
	var sources chunkSources
	stats := &Stats{}

	for _, storageId := range storageIds {
		// Find the table spec to get chunk count
		tableSpec, found := nbs.findTableSpec(storageId)
		if !found {
			return hash.Hash{}, errors.New("storage file not found: " + storageId.String())
		}

		// Open the chunkSource for this storage ID
		cs, err := nbs.persister.Open(ctx, storageId, tableSpec.chunkCount, stats)
		if err != nil {
			return hash.Hash{}, err
		}
		sources = append(sources, cs)
	}

	// Store the original manifest for comparison
	originalUpstream := nbs.upstream

	// Use the existing conjoin infrastructure to properly update the manifest
	strategy := &specificFilesConjoiner{targetStorageIds: storageIds}
	newUpstream, finalCleanup, err := conjoin(ctx, strategy, nbs.upstream, nbs.manifestMgr, nbs.persister, stats)
	if err != nil {
		return hash.Hash{}, err
	}

	// Update the in-memory state
	nbs.upstream = newUpstream
	newTables, err := nbs.tables.rebase(ctx, newUpstream.specs, nil, nbs.stats)
	if err != nil {
		return hash.Hash{}, err
	}
	nbs.tables = newTables

	// Call the final cleanup
	finalCleanup()

	// Find the new conjoined table file hash from the updated manifest
	// Since we removed the old files and added one new file, we need to find which one is new
	oldIdSet := make(map[hash.Hash]bool)
	for _, id := range storageIds {
		oldIdSet[id] = true
	}

	// Create a set of original spec names for comparison
	originalSpecSet := make(map[hash.Hash]bool)
	for _, spec := range originalUpstream.specs {
		originalSpecSet[spec.name] = true
	}

	var conjoinedHash hash.Hash
	for _, spec := range newUpstream.specs {
		// If this spec is not in the original manifest and not one of the old files we removed,
		// it must be the new conjoined file
		if !originalSpecSet[spec.name] && !oldIdSet[spec.name] {
			conjoinedHash = spec.name
			break
		}
	}

	return conjoinedHash, nil
}

// findTableSpec finds a table spec by hash in the current manifest. This will ignore novel tables, as it's not
// needed currently.
func (nbs *NomsBlockStore) findTableSpec(storageId hash.Hash) (tableSpec, bool) {
	for _, spec := range nbs.upstream.specs {
		if spec.name == storageId {
			return spec, true
		}
	}
	for _, tf := range nbs.tables.upstream {
		if tf.hash() == storageId {
			count, err := tf.count()
			if err != nil {
				return tableSpec{name: storageId, chunkCount: 0}, false
			}
			return tableSpec{name: storageId, chunkCount: count}, true
		}
	}
	return tableSpec{name: storageId, chunkCount: 0}, false
}
