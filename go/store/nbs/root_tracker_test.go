// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/liquidata-inc/ld/dolt/go/store/chunks"
	"github.com/liquidata-inc/ld/dolt/go/store/constants"
	"github.com/liquidata-inc/ld/dolt/go/store/hash"
	"github.com/stretchr/testify/assert"
)

func TestChunkStoreZeroValue(t *testing.T) {
	assert := assert.New(t)
	_, _, store := makeStoreWithFakes(t)
	defer store.Close()

	// No manifest file gets written until the first call to Commit(). Prior to that, Root() will simply return hash.Hash{}.
	assert.Equal(hash.Hash{}, store.Root(context.Background()))
	assert.Equal(constants.NomsVersion, store.Version())
}

func TestChunkStoreVersion(t *testing.T) {
	assert := assert.New(t)
	_, _, store := makeStoreWithFakes(t)
	defer store.Close()

	assert.Equal(constants.NomsVersion, store.Version())
	newRoot := hash.Of([]byte("new root"))
	if assert.True(store.Commit(context.Background(), newRoot, hash.Hash{})) {
		assert.Equal(constants.NomsVersion, store.Version())
	}
}

func TestChunkStoreRebase(t *testing.T) {
	assert := assert.New(t)
	fm, p, store := makeStoreWithFakes(t)
	defer store.Close()

	assert.Equal(hash.Hash{}, store.Root(context.Background()))
	assert.Equal(constants.NomsVersion, store.Version())

	// Simulate another process writing a manifest behind store's back.
	newRoot, chunks := interloperWrite(fm, p, []byte("new root"), []byte("hello2"), []byte("goodbye2"), []byte("badbye2"))

	// state in store shouldn't change
	assert.Equal(hash.Hash{}, store.Root(context.Background()))
	assert.Equal(constants.NomsVersion, store.Version())

	store.Rebase(context.Background())

	// NOW it should
	assert.Equal(newRoot, store.Root(context.Background()))
	assert.Equal(constants.NomsVersion, store.Version())
	assertDataInStore(chunks, store, assert)
}

func TestChunkStoreCommit(t *testing.T) {
	assert := assert.New(t)
	_, _, store := makeStoreWithFakes(t)
	defer store.Close()

	assert.Equal(hash.Hash{}, store.Root(context.Background()))

	newRootChunk := chunks.NewChunk([]byte("new root"))
	newRoot := newRootChunk.Hash()
	store.Put(context.Background(), newRootChunk)
	if assert.True(store.Commit(context.Background(), newRoot, hash.Hash{})) {
		assert.True(store.Has(context.Background(), newRoot))
		assert.Equal(newRoot, store.Root(context.Background()))
	}

	secondRootChunk := chunks.NewChunk([]byte("newer root"))
	secondRoot := secondRootChunk.Hash()
	store.Put(context.Background(), secondRootChunk)
	if assert.True(store.Commit(context.Background(), secondRoot, newRoot)) {
		assert.Equal(secondRoot, store.Root(context.Background()))
		assert.True(store.Has(context.Background(), newRoot))
		assert.True(store.Has(context.Background(), secondRoot))
	}
}

func TestChunkStoreManifestAppearsAfterConstruction(t *testing.T) {
	assert := assert.New(t)
	fm, p, store := makeStoreWithFakes(t)
	defer store.Close()

	assert.Equal(hash.Hash{}, store.Root(context.Background()))
	assert.Equal(constants.NomsVersion, store.Version())

	// Simulate another process writing a manifest behind store's back.
	interloperWrite(fm, p, []byte("new root"), []byte("hello2"), []byte("goodbye2"), []byte("badbye2"))

	// state in store shouldn't change
	assert.Equal(hash.Hash{}, store.Root(context.Background()))
	assert.Equal(constants.NomsVersion, store.Version())
}

func TestChunkStoreManifestFirstWriteByOtherProcess(t *testing.T) {
	assert := assert.New(t)
	fm := &fakeManifest{}
	mm := manifestManager{fm, newManifestCache(0), newManifestLocks()}
	p := newFakeTablePersister()

	// Simulate another process writing a manifest behind store's back.
	newRoot, chunks := interloperWrite(fm, p, []byte("new root"), []byte("hello2"), []byte("goodbye2"), []byte("badbye2"))

	store := newNomsBlockStore(context.Background(), mm, p, inlineConjoiner{defaultMaxTables}, defaultMemTableSize)
	defer store.Close()

	assert.Equal(newRoot, store.Root(context.Background()))
	assert.Equal(constants.NomsVersion, store.Version())
	assertDataInStore(chunks, store, assert)
}

func TestChunkStoreCommitOptimisticLockFail(t *testing.T) {
	assert := assert.New(t)
	fm, p, store := makeStoreWithFakes(t)
	defer store.Close()

	// Simulate another process writing a manifest behind store's back.
	newRoot, chunks := interloperWrite(fm, p, []byte("new root"), []byte("hello2"), []byte("goodbye2"), []byte("badbye2"))

	newRoot2 := hash.Of([]byte("new root 2"))
	assert.False(store.Commit(context.Background(), newRoot2, hash.Hash{}))
	assertDataInStore(chunks, store, assert)
	assert.True(store.Commit(context.Background(), newRoot2, newRoot))
}

func TestChunkStoreManifestPreemptiveOptimisticLockFail(t *testing.T) {
	assert := assert.New(t)
	fm := &fakeManifest{}
	mm := manifestManager{fm, newManifestCache(defaultManifestCacheSize), newManifestLocks()}
	p := newFakeTablePersister()
	c := inlineConjoiner{defaultMaxTables}

	store := newNomsBlockStore(context.Background(), mm, p, c, defaultMemTableSize)
	defer store.Close()

	// Simulate another goroutine writing a manifest behind store's back.
	interloper := newNomsBlockStore(context.Background(), mm, p, c, defaultMemTableSize)
	defer interloper.Close()

	chunk := chunks.NewChunk([]byte("hello"))
	interloper.Put(context.Background(), chunk)
	assert.True(interloper.Commit(context.Background(), chunk.Hash(), hash.Hash{}))

	// Try to land a new chunk in store, which should fail AND not persist the contents of store.mt
	chunk = chunks.NewChunk([]byte("goodbye"))
	store.Put(context.Background(), chunk)
	assert.NotNil(store.mt)
	assert.False(store.Commit(context.Background(), chunk.Hash(), hash.Hash{}))
	assert.NotNil(store.mt)

	assert.True(store.Commit(context.Background(), chunk.Hash(), store.Root(context.Background())))
	assert.Nil(store.mt)
	assert.Equal(chunk.Hash(), store.Root(context.Background()))
	assert.Equal(constants.NomsVersion, store.Version())
}

func TestChunkStoreCommitLocksOutFetch(t *testing.T) {
	assert := assert.New(t)
	fm := &fakeManifest{name: "foo"}
	upm := &updatePreemptManifest{manifest: fm}
	mm := manifestManager{upm, newManifestCache(defaultManifestCacheSize), newManifestLocks()}
	p := newFakeTablePersister()
	c := inlineConjoiner{defaultMaxTables}

	store := newNomsBlockStore(context.Background(), mm, p, c, defaultMemTableSize)
	defer store.Close()

	// store.Commit() should lock out calls to mm.Fetch()
	wg := sync.WaitGroup{}
	fetched := manifestContents{}
	upm.preUpdate = func() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, fetched = mm.Fetch(context.Background(), nil)
		}()
	}

	rootChunk := chunks.NewChunk([]byte("new root"))
	store.Put(context.Background(), rootChunk)
	assert.True(store.Commit(context.Background(), rootChunk.Hash(), store.Root(context.Background())))

	wg.Wait()
	assert.Equal(store.Root(context.Background()), fetched.root)
}

func TestChunkStoreSerializeCommits(t *testing.T) {
	assert := assert.New(t)
	fm := &fakeManifest{name: "foo"}
	upm := &updatePreemptManifest{manifest: fm}
	mc := newManifestCache(defaultManifestCacheSize)
	l := newManifestLocks()
	p := newFakeTablePersister()
	c := inlineConjoiner{defaultMaxTables}

	store := newNomsBlockStore(context.Background(), manifestManager{upm, mc, l}, p, c, defaultMemTableSize)
	defer store.Close()

	storeChunk := chunks.NewChunk([]byte("store"))
	interloperChunk := chunks.NewChunk([]byte("interloper"))
	updateCount := 0

	interloper := newNomsBlockStore(
		context.Background(),
		manifestManager{
			updatePreemptManifest{fm, func() { updateCount++ }}, mc, l,
		},
		p,
		c,
		defaultMemTableSize)
	defer interloper.Close()

	wg := sync.WaitGroup{}
	upm.preUpdate = func() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			interloper.Put(context.Background(), interloperChunk)
			assert.True(interloper.Commit(context.Background(), interloper.Root(context.Background()), interloper.Root(context.Background())))
		}()

		updateCount++
	}

	store.Put(context.Background(), storeChunk)
	assert.True(store.Commit(context.Background(), store.Root(context.Background()), store.Root(context.Background())))

	wg.Wait()
	assert.Equal(2, updateCount)
	assert.True(interloper.Has(context.Background(), storeChunk.Hash()))
	assert.True(interloper.Has(context.Background(), interloperChunk.Hash()))
}

func makeStoreWithFakes(t *testing.T) (fm *fakeManifest, p tablePersister, store *NomsBlockStore) {
	fm = &fakeManifest{}
	mm := manifestManager{fm, newManifestCache(0), newManifestLocks()}
	p = newFakeTablePersister()
	store = newNomsBlockStore(context.Background(), mm, p, inlineConjoiner{defaultMaxTables}, 0)
	return
}

// Simulate another process writing a manifest behind store's back.
func interloperWrite(fm *fakeManifest, p tablePersister, rootChunk []byte, chunks ...[]byte) (newRoot hash.Hash, persisted [][]byte) {
	newLock, newRoot := computeAddr([]byte("locker")), hash.Of(rootChunk)
	persisted = append(chunks, rootChunk)
	src := p.Persist(context.Background(), createMemTable(persisted), nil, &Stats{})
	fm.set(constants.NomsVersion, newLock, newRoot, []tableSpec{{src.hash(), uint32(len(chunks))}})
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
		assert.True(store.Has(context.Background(), chunks.NewChunk(data).Hash()))
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
func (fm *fakeManifest) ParseIfExists(ctx context.Context, stats *Stats, readHook func()) (exists bool, contents manifestContents) {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	if fm.contents.lock != (addr{}) {
		return true, fm.contents
	}
	return false, manifestContents{}
}

// Update checks whether |lastLock| == |fm.lock| and, if so, updates internal
// fake manifest state as per the manifest.Update() contract: |fm.lock| is set
// to |newLock|, |fm.root| is set to |newRoot|, and the contents of |specs|
// replace |fm.tableSpecs|. If |lastLock| != |fm.lock|, then the update
// fails. Regardless of success or failure, the current state is returned.
func (fm *fakeManifest) Update(ctx context.Context, lastLock addr, newContents manifestContents, stats *Stats, writeHook func()) manifestContents {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	if fm.contents.lock == lastLock {
		fm.contents = manifestContents{newContents.vers, newContents.lock, newContents.root, nil}
		fm.contents.specs = make([]tableSpec, len(newContents.specs))
		copy(fm.contents.specs, newContents.specs)
	}
	return fm.contents
}

func (fm *fakeManifest) set(version string, lock addr, root hash.Hash, specs []tableSpec) {
	fm.contents = manifestContents{version, lock, root, specs}
}

func newFakeTableSet() tableSet {
	return tableSet{p: newFakeTablePersister(), rl: make(chan struct{}, 1)}
}

func newFakeTablePersister() tablePersister {
	return fakeTablePersister{map[addr]tableReader{}, &sync.RWMutex{}}
}

type fakeTablePersister struct {
	sources map[addr]tableReader
	mu      *sync.RWMutex
}

func (ftp fakeTablePersister) Persist(ctx context.Context, mt *memTable, haver chunkReader, stats *Stats) chunkSource {
	if mt.count() > 0 {
		name, data, chunkCount := mt.write(haver, stats)
		if chunkCount > 0 {
			ftp.mu.Lock()
			defer ftp.mu.Unlock()
			ftp.sources[name] = newTableReader(parseTableIndex(data), tableReaderAtFromBytes(data), fileBlockSize)
			return chunkSourceAdapter{ftp.sources[name], name}
		}
	}
	return emptyChunkSource{}
}

func (ftp fakeTablePersister) ConjoinAll(ctx context.Context, sources chunkSources, stats *Stats) chunkSource {
	name, data, chunkCount := compactSourcesToBuffer(sources)
	if chunkCount > 0 {
		ftp.mu.Lock()
		defer ftp.mu.Unlock()
		ftp.sources[name] = newTableReader(parseTableIndex(data), tableReaderAtFromBytes(data), fileBlockSize)
		return chunkSourceAdapter{ftp.sources[name], name}
	}
	return emptyChunkSource{}
}

func compactSourcesToBuffer(sources chunkSources) (name addr, data []byte, chunkCount uint32) {
	totalData := uint64(0)
	for _, src := range sources {
		chunkCount += src.count()
		totalData += src.uncompressedLen()
	}
	if chunkCount == 0 {
		return
	}

	maxSize := maxTableSize(uint64(chunkCount), totalData)
	buff := make([]byte, maxSize) // This can blow up RAM
	tw := newTableWriter(buff, nil)
	errString := ""

	for _, src := range sources {
		chunks := make(chan extractRecord)
		go func() {
			defer close(chunks)
			defer func() {
				if r := recover(); r != nil {
					chunks <- extractRecord{a: src.hash(), err: r}
				}
			}()
			src.extract(context.Background(), chunks)
		}()
		for rec := range chunks {
			if rec.err != nil {
				errString += fmt.Sprintf("Failed to extract %s:\n %v\n******\n\n", rec.a, rec.err)
				continue
			}
			tw.addChunk(rec.a, rec.data)
		}
	}

	if errString != "" {
		panic(fmt.Errorf(errString))
	}
	tableSize, name := tw.finish()
	return name, buff[:tableSize], chunkCount
}

func (ftp fakeTablePersister) Open(ctx context.Context, name addr, chunkCount uint32, stats *Stats) chunkSource {
	ftp.mu.RLock()
	defer ftp.mu.RUnlock()
	return chunkSourceAdapter{ftp.sources[name], name}
}
