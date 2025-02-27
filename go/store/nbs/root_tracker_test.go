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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/constants"
	"github.com/dolthub/dolt/go/store/hash"
)

func TestChunkStoreZeroValue(t *testing.T) {
	assert := assert.New(t)
	_, _, _, store := makeStoreWithFakes(t)
	defer func() {
		require.NoError(t, store.Close())
	}()

	// No manifest file gets written until the first call to Commit(). Prior to that, Root() will simply return hash.Hash{}.
	h, err := store.Root(context.Background())
	require.NoError(t, err)
	assert.Equal(hash.Hash{}, h)
	assert.Equal(constants.FormatLD1String, store.Version())
}

func TestChunkStoreVersion(t *testing.T) {
	assert := assert.New(t)
	_, _, _, store := makeStoreWithFakes(t)
	defer func() {
		require.NoError(t, store.Close())
	}()

	assert.Equal(constants.FormatLD1String, store.Version())
	newChunk := chunks.NewChunk([]byte("new root"))
	require.NoError(t, store.Put(context.Background(), newChunk, noopGetAddrs))
	newRoot := newChunk.Hash()

	if assert.True(store.Commit(context.Background(), newRoot, hash.Hash{})) {
		assert.Equal(constants.FormatLD1String, store.Version())
	}
}

func TestChunkStoreRebase(t *testing.T) {
	assert := assert.New(t)
	fm, p, q, store := makeStoreWithFakes(t)
	defer func() {
		require.NoError(t, store.Close())
		require.EqualValues(t, 0, q.Usage())
	}()

	h, err := store.Root(context.Background())
	require.NoError(t, err)
	assert.Equal(hash.Hash{}, h)
	assert.Equal(constants.FormatLD1String, store.Version())

	// Simulate another process writing a manifest behind store's back.
	newRoot, chunks, err := interloperWrite(fm, p, []byte("new root"), []byte("hello2"), []byte("goodbye2"), []byte("badbye2"))
	require.NoError(t, err)

	// state in store shouldn't change
	h, err = store.Root(context.Background())
	require.NoError(t, err)
	assert.Equal(hash.Hash{}, h)
	assert.Equal(constants.FormatLD1String, store.Version())

	err = store.Rebase(context.Background())
	require.NoError(t, err)

	// NOW it should
	h, err = store.Root(context.Background())
	require.NoError(t, err)
	assert.Equal(newRoot, h)
	assert.Equal(constants.FormatLD1String, store.Version())
	assertDataInStore(chunks, store, assert)
}

func TestChunkStoreCommit(t *testing.T) {
	assert := assert.New(t)
	_, _, q, store := makeStoreWithFakes(t)
	defer func() {
		require.NoError(t, store.Close())
		require.EqualValues(t, 0, q.Usage())
	}()

	h, err := store.Root(context.Background())
	require.NoError(t, err)
	assert.Equal(hash.Hash{}, h)

	newRootChunk := chunks.NewChunk([]byte("new root"))
	newRoot := newRootChunk.Hash()
	err = store.Put(context.Background(), newRootChunk, noopGetAddrs)
	require.NoError(t, err)
	success, err := store.Commit(context.Background(), newRoot, hash.Hash{})
	require.NoError(t, err)
	if assert.True(success) {
		has, err := store.Has(context.Background(), newRoot)
		require.NoError(t, err)
		assert.True(has)
		h, err := store.Root(context.Background())
		require.NoError(t, err)
		assert.Equal(newRoot, h)
	}

	secondRootChunk := chunks.NewChunk([]byte("newer root"))
	secondRoot := secondRootChunk.Hash()
	err = store.Put(context.Background(), secondRootChunk, noopGetAddrs)
	require.NoError(t, err)
	success, err = store.Commit(context.Background(), secondRoot, newRoot)
	require.NoError(t, err)
	if assert.True(success) {
		h, err := store.Root(context.Background())
		require.NoError(t, err)
		assert.Equal(secondRoot, h)
		has, err := store.Has(context.Background(), newRoot)
		require.NoError(t, err)
		assert.True(has)
		has, err = store.Has(context.Background(), secondRoot)
		require.NoError(t, err)
		assert.True(has)
	}
}

func TestChunkStoreManifestAppearsAfterConstruction(t *testing.T) {
	assert := assert.New(t)
	fm, p, q, store := makeStoreWithFakes(t)
	defer func() {
		require.NoError(t, store.Close())
		require.EqualValues(t, 0, q.Usage())
	}()

	h, err := store.Root(context.Background())
	require.NoError(t, err)
	assert.Equal(hash.Hash{}, h)
	assert.Equal(constants.FormatLD1String, store.Version())

	// Simulate another process writing a manifest behind store's back.
	interloperWrite(fm, p, []byte("new root"), []byte("hello2"), []byte("goodbye2"), []byte("badbye2"))

	// state in store shouldn't change
	h, err = store.Root(context.Background())
	require.NoError(t, err)
	assert.Equal(hash.Hash{}, h)
	assert.Equal(constants.FormatLD1String, store.Version())
}

func TestChunkStoreManifestFirstWriteByOtherProcess(t *testing.T) {
	assert := assert.New(t)
	fm := &fakeManifest{}
	mm := manifestManager{fm, newManifestCache(0), newManifestLocks()}
	q := NewUnlimitedMemQuotaProvider()
	defer func() {
		require.EqualValues(t, 0, q.Usage())
	}()
	p := newFakeTablePersister(q)

	// Simulate another process writing a manifest behind store's back.
	newRoot, chunks, err := interloperWrite(fm, p, []byte("new root"), []byte("hello2"), []byte("goodbye2"), []byte("badbye2"))
	require.NoError(t, err)

	store, err := newNomsBlockStore(context.Background(), constants.FormatLD1String, mm, p, q, inlineConjoiner{defaultMaxTables}, defaultMemTableSize)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
	}()

	h, err := store.Root(context.Background())
	require.NoError(t, err)
	assert.Equal(newRoot, h)
	assert.Equal(constants.FormatLD1String, store.Version())
	assertDataInStore(chunks, store, assert)
}

func TestChunkStoreCommitOptimisticLockFail(t *testing.T) {
	assert := assert.New(t)
	fm, p, q, store := makeStoreWithFakes(t)
	defer func() {
		require.NoError(t, store.Close())
		require.EqualValues(t, 0, q.Usage())
	}()

	// Simulate another process writing a manifest behind store's back.
	newRoot, chks, err := interloperWrite(fm, p, []byte("new root"), []byte("hello2"), []byte("goodbye2"), []byte("badbye2"))
	require.NoError(t, err)

	newChunk := chunks.NewChunk([]byte("new root 2"))
	require.NoError(t, store.Put(context.Background(), newChunk, noopGetAddrs))
	newRoot2 := newChunk.Hash()
	success, err := store.Commit(context.Background(), newRoot2, hash.Hash{})
	require.NoError(t, err)
	assert.False(success)
	assertDataInStore(chks, store, assert)
	success, err = store.Commit(context.Background(), newRoot2, newRoot)
	require.NoError(t, err)
	assert.True(success)
}

func TestChunkStoreManifestPreemptiveOptimisticLockFail(t *testing.T) {
	assert := assert.New(t)
	fm := &fakeManifest{}
	mm := manifestManager{fm, newManifestCache(defaultManifestCacheSize), newManifestLocks()}
	q := NewUnlimitedMemQuotaProvider()
	p := newFakeTablePersister(q)

	c := inlineConjoiner{defaultMaxTables}

	store, err := newNomsBlockStore(context.Background(), constants.FormatLD1String, mm, p, q, c, defaultMemTableSize)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
		require.EqualValues(t, 0, q.Usage())
	}()

	// Simulate another goroutine writing a manifest behind store's back.
	interloper, err := newNomsBlockStore(context.Background(), constants.FormatLD1String, mm, p, q, c, defaultMemTableSize)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, interloper.Close())
	}()

	chunk := chunks.NewChunk([]byte("hello"))
	err = interloper.Put(context.Background(), chunk, noopGetAddrs)
	require.NoError(t, err)
	assert.True(interloper.Commit(context.Background(), chunk.Hash(), hash.Hash{}))

	// Try to land a new chunk in store, which should fail AND not persist the contents of store.mt
	chunk = chunks.NewChunk([]byte("goodbye"))
	err = store.Put(context.Background(), chunk, noopGetAddrs)
	require.NoError(t, err)
	assert.NotNil(store.mt)
	assert.False(store.Commit(context.Background(), chunk.Hash(), hash.Hash{}))
	assert.NotNil(store.mt)

	h, err := store.Root(context.Background())
	require.NoError(t, err)
	success, err := store.Commit(context.Background(), chunk.Hash(), h)
	require.NoError(t, err)
	assert.True(success)
	assert.Nil(store.mt)

	h, err = store.Root(context.Background())
	require.NoError(t, err)
	assert.Equal(chunk.Hash(), h)
	assert.Equal(constants.FormatLD1String, store.Version())
}

func TestChunkStoreCommitLocksOutFetch(t *testing.T) {
	assert := assert.New(t)
	fm := &fakeManifest{name: "foo"}
	upm := &updatePreemptManifest{manifest: fm}
	mm := manifestManager{upm, newManifestCache(defaultManifestCacheSize), newManifestLocks()}
	q := NewUnlimitedMemQuotaProvider()
	p := newFakeTablePersister(q)
	c := inlineConjoiner{defaultMaxTables}

	store, err := newNomsBlockStore(context.Background(), constants.FormatLD1String, mm, p, q, c, defaultMemTableSize)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
		require.EqualValues(t, 0, q.Usage())
	}()

	// store.Commit() should lock out calls to mm.Fetch()
	wg := sync.WaitGroup{}
	fetched := manifestContents{}
	upm.preUpdate = func() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var err error
			_, fetched, _, err = mm.Fetch(context.Background(), nil)
			require.NoError(t, err)
		}()
	}

	rootChunk := chunks.NewChunk([]byte("new root"))
	err = store.Put(context.Background(), rootChunk, noopGetAddrs)
	require.NoError(t, err)
	h, err := store.Root(context.Background())
	require.NoError(t, err)
	success, err := store.Commit(context.Background(), rootChunk.Hash(), h)
	require.NoError(t, err)
	assert.True(success)

	wg.Wait()
	h, err = store.Root(context.Background())
	require.NoError(t, err)
	assert.Equal(h, fetched.root)
}

func TestChunkStoreSerializeCommits(t *testing.T) {
	assert := assert.New(t)
	fm := &fakeManifest{name: "foo"}
	upm := &updatePreemptManifest{manifest: fm}
	mc := newManifestCache(defaultManifestCacheSize)
	l := newManifestLocks()
	q := NewUnlimitedMemQuotaProvider()
	p := newFakeTablePersister(q)

	c := inlineConjoiner{defaultMaxTables}

	store, err := newNomsBlockStore(context.Background(), constants.FormatLD1String, manifestManager{upm, mc, l}, p, q, c, defaultMemTableSize)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
		require.EqualValues(t, 0, q.Usage())
	}()

	storeChunk := chunks.NewChunk([]byte("store"))
	interloperChunk := chunks.NewChunk([]byte("interloper"))
	updateCount := 0

	interloper, err := newNomsBlockStore(
		context.Background(),
		constants.FormatLD1String,
		manifestManager{
			updatePreemptManifest{fm, func() { updateCount++ }}, mc, l,
		},
		p,
		q,
		c,
		defaultMemTableSize)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, interloper.Close())
	}()

	wg := sync.WaitGroup{}
	upm.preUpdate = func() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := interloper.Put(context.Background(), interloperChunk, noopGetAddrs)
			require.NoError(t, err)
			h, err := interloper.Root(context.Background())
			require.NoError(t, err)
			success, err := interloper.Commit(context.Background(), h, h)
			require.NoError(t, err)
			assert.True(success)
		}()

		updateCount++
	}

	err = store.Put(context.Background(), storeChunk, noopGetAddrs)
	require.NoError(t, err)
	h, err := store.Root(context.Background())
	require.NoError(t, err)
	success, err := store.Commit(context.Background(), h, h)
	require.NoError(t, err)
	assert.True(success)

	wg.Wait()
	assert.Equal(2, updateCount)
	assert.True(interloper.Has(context.Background(), storeChunk.Hash()))
	assert.True(interloper.Has(context.Background(), interloperChunk.Hash()))
}

func makeStoreWithFakes(t *testing.T) (fm *fakeManifest, p tablePersister, q MemoryQuotaProvider, store *NomsBlockStore) {
	fm = &fakeManifest{}
	mm := manifestManager{fm, newManifestCache(0), newManifestLocks()}
	q = NewUnlimitedMemQuotaProvider()
	p = newFakeTablePersister(q)
	store, err := newNomsBlockStore(context.Background(), constants.FormatLD1String, mm, p, q, inlineConjoiner{defaultMaxTables}, 0)
	require.NoError(t, err)
	return
}

// Simulate another process writing a manifest behind store's back.
func interloperWrite(fm *fakeManifest, p tablePersister, rootChunk []byte, chunks ...[]byte) (newRoot hash.Hash, persisted [][]byte, err error) {
	newLock, newRoot := computeAddr([]byte("locker")), hash.Of(rootChunk)
	persisted = append(chunks, rootChunk)

	var src chunkSource
	src, _, err = p.Persist(context.Background(), createMemTable(persisted), nil, nil, &Stats{})
	if err != nil {
		return hash.Hash{}, nil, err
	}

	fm.set(constants.FormatLD1String, newLock, newRoot, []tableSpec{{src.hash(), uint32(len(chunks) + 1)}}, nil)

	if err = src.close(); err != nil {
		return [20]byte{}, nil, err
	}
	return
}

func createMemTable(chunks [][]byte) *memTable {
	mt := newMemTable(1 << 10)
	for _, c := range chunks {
		mt.addChunk(computeAddr(c), c)
	}
	return mt
}

func assertDataInStore(slices [][]byte, store chunks.ChunkStore, assert *assert.Assertions) {
	for _, data := range slices {
		ok, err := store.Has(context.Background(), chunks.NewChunk(data).Hash())
		assert.NoError(err)
		assert.True(ok)
	}
}

// fakeManifest simulates a fileManifest without touching disk.
type fakeManifest struct {
	name     string
	contents manifestContents
	mu       sync.RWMutex
}

func (fm *fakeManifest) Name() string { return fm.name }

// ParseIfExists returns any fake manifest data the caller has injected using
// Update() or set(). It treats an empty |fm.lock| as a non-existent manifest.
func (fm *fakeManifest) ParseIfExists(ctx context.Context, stats *Stats, readHook func() error) (bool, manifestContents, error) {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	if !fm.contents.lock.IsEmpty() {
		return true, fm.contents, nil
	}

	return false, manifestContents{}, nil
}

// Update checks whether |lastLock| == |fm.lock| and, if so, updates internal
// fake manifest state as per the manifest.Update() contract: |fm.lock| is set
// to |newLock|, |fm.root| is set to |newRoot|, and the contents of |specs|
// replace |fm.tableSpecs|. If |lastLock| != |fm.lock|, then the update
// fails. Regardless of success or failure, the current state is returned.
func (fm *fakeManifest) Update(ctx context.Context, lastLock hash.Hash, newContents manifestContents, stats *Stats, writeHook func() error) (manifestContents, error) {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	if fm.contents.lock == lastLock {
		fm.contents = manifestContents{
			manifestVers: StorageVersion,
			nbfVers:      newContents.nbfVers,
			lock:         newContents.lock,
			root:         newContents.root,
			gcGen:        hash.Hash{},
		}
		fm.contents.specs = make([]tableSpec, len(newContents.specs))
		copy(fm.contents.specs, newContents.specs)
		if newContents.appendix != nil && len(newContents.appendix) > 0 {
			fm.contents.appendix = make([]tableSpec, len(newContents.appendix))
			copy(fm.contents.appendix, newContents.appendix)
		}
	}
	return fm.contents, nil
}

func (fm *fakeManifest) set(version string, lock hash.Hash, root hash.Hash, specs, appendix []tableSpec) {
	fm.contents = manifestContents{
		manifestVers: StorageVersion,
		nbfVers:      version,
		lock:         lock,
		root:         root,
		gcGen:        hash.Hash{},
		specs:        specs,
		appendix:     appendix,
	}
}

func newFakeTableSet(q MemoryQuotaProvider) tableSet {
	return tableSet{p: newFakeTablePersister(q), q: q, rl: make(chan struct{}, 1)}
}

func newFakeTablePersister(q MemoryQuotaProvider) fakeTablePersister {
	return fakeTablePersister{q, map[hash.Hash][]byte{}, map[hash.Hash]bool{}, map[hash.Hash]bool{}, &sync.RWMutex{}}
}

type fakeTablePersister struct {
	q             MemoryQuotaProvider
	sources       map[hash.Hash][]byte
	sourcesToFail map[hash.Hash]bool
	opened        map[hash.Hash]bool
	mu            *sync.RWMutex
}

var _ tablePersister = fakeTablePersister{}

func (ftp fakeTablePersister) Persist(ctx context.Context, mt *memTable, haver chunkReader, keeper keeperF, stats *Stats) (chunkSource, gcBehavior, error) {
	if mustUint32(mt.count()) == 0 {
		return emptyChunkSource{}, gcBehavior_Continue, nil
	}

	name, data, chunkCount, gcb, err := mt.write(haver, keeper, stats)
	if err != nil {
		return emptyChunkSource{}, gcBehavior_Continue, err
	} else if gcb != gcBehavior_Continue {
		return emptyChunkSource{}, gcb, nil
	} else if chunkCount == 0 {
		return emptyChunkSource{}, gcBehavior_Continue, nil
	}

	ftp.mu.Lock()
	ftp.sources[name] = data
	ftp.mu.Unlock()

	ti, err := parseTableIndexByCopy(ctx, data, ftp.q)
	if err != nil {
		return nil, gcBehavior_Continue, err
	}

	cs, err := newTableReader(ti, tableReaderAtFromBytes(data), fileBlockSize)
	if err != nil {
		return emptyChunkSource{}, gcBehavior_Continue, err
	}
	return chunkSourceAdapter{cs, name}, gcBehavior_Continue, nil
}

func (ftp fakeTablePersister) ConjoinAll(ctx context.Context, sources chunkSources, stats *Stats) (chunkSource, cleanupFunc, error) {
	name, data, chunkCount, err := compactSourcesToBuffer(sources)
	if err != nil {
		return nil, nil, err
	} else if chunkCount == 0 {
		return emptyChunkSource{}, func() {}, nil
	}

	ftp.mu.Lock()
	defer ftp.mu.Unlock()
	ftp.sources[name] = data

	ti, err := parseTableIndexByCopy(ctx, data, ftp.q)
	if err != nil {
		return nil, nil, err
	}

	cs, err := newTableReader(ti, tableReaderAtFromBytes(data), fileBlockSize)
	if err != nil {
		return nil, nil, err
	}
	return chunkSourceAdapter{cs, name}, func() {}, nil
}

func compactSourcesToBuffer(sources chunkSources) (name hash.Hash, data []byte, chunkCount uint32, err error) {
	totalData := uint64(0)
	for _, src := range sources {
		chunkCount += mustUint32(src.count())
		totalData += mustUint64(src.uncompressedLen())
	}
	if chunkCount == 0 {
		return
	}

	maxSize := maxTableSize(uint64(chunkCount), totalData)
	buff := make([]byte, maxSize) // This can blow up RAM
	tw := newTableWriter(buff, nil)
	errString := ""

	ctx := context.Background()
	for _, src := range sources {
		ch := make(chan extractRecord)
		go func() {
			defer close(ch)
			err = extractAllChunks(ctx, src, func(rec extractRecord) {
				ch <- rec
			})
			if err != nil {
				ch <- extractRecord{a: src.hash(), err: err}
			}
		}()

		for rec := range ch {
			if rec.err != nil {
				errString += fmt.Sprintf("Failed to extract %s:\n %v\n******\n\n", rec.a, rec.err)
				continue
			}
			tw.addChunk(rec.a, rec.data)
		}
	}

	if errString != "" {
		return hash.Hash{}, nil, 0, fmt.Errorf("%s", errString)
	}

	tableSize, name, err := tw.finish()

	if err != nil {
		return hash.Hash{}, nil, 0, err
	}

	return name, buff[:tableSize], chunkCount, nil
}

func (ftp fakeTablePersister) Open(ctx context.Context, name hash.Hash, chunkCount uint32, stats *Stats) (chunkSource, error) {
	ftp.mu.Lock()
	defer ftp.mu.Unlock()

	if _, ok := ftp.sourcesToFail[name]; ok {
		return nil, errors.New("intentional failure")
	}
	data := ftp.sources[name]
	ftp.opened[name] = true

	ti, err := parseTableIndexByCopy(ctx, data, ftp.q)
	if err != nil {
		return nil, err
	}

	cs, err := newTableReader(ti, tableReaderAtFromBytes(data), fileBlockSize)
	if err != nil {
		return emptyChunkSource{}, err
	}
	return chunkSourceAdapter{cs, name}, nil
}

func (ftp fakeTablePersister) Exists(ctx context.Context, name string, chunkCount uint32, stats *Stats) (bool, error) {
	h, ok := hash.MaybeParse(name)
	if !ok {
		panic("object store name expected to be a hash in test")
	}

	if _, ok := ftp.sourcesToFail[h]; ok {
		return false, errors.New("intentional failure")
	}
	return true, nil
}

func (ftp fakeTablePersister) PruneTableFiles(_ context.Context, _ func() []hash.Hash, _ time.Time) error {
	return chunks.ErrUnsupportedOperation
}

func (ftp fakeTablePersister) Close() error {
	return nil
}

func (ftp fakeTablePersister) AccessMode() chunks.ExclusiveAccessMode {
	return chunks.ExclusiveAccessMode_Shared
}

func extractAllChunks(ctx context.Context, src chunkSource, cb func(rec extractRecord)) (err error) {
	var index tableIndex
	if index, err = src.index(); err != nil {
		return err
	}

	for i := uint32(0); i < index.chunkCount(); i++ {
		var h hash.Hash
		_, err = index.indexEntry(i, &h)
		if err != nil {
			return err
		}

		data, _, err := src.get(ctx, h, nil, nil)
		if err != nil {
			return err
		}
		cb(extractRecord{a: h, data: data})
	}
	return
}
