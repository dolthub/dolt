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
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/constants"
	"github.com/dolthub/dolt/go/store/hash"
)

func TestChunkStoreZeroValue(t *testing.T) {
	assert := assert.New(t)
	_, _, store := makeStoreWithFakes(t)
	defer store.Close()

	// No manifest file gets written until the first call to Commit(). Prior to that, Root() will simply return hash.Hash{}.
	h, err := store.Root(context.Background())
	assert.NoError(err)
	assert.Equal(hash.Hash{}, h)
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

	h, err := store.Root(context.Background())
	assert.NoError(err)
	assert.Equal(hash.Hash{}, h)
	assert.Equal(constants.NomsVersion, store.Version())

	// Simulate another process writing a manifest behind store's back.
	newRoot, chunks, err := interloperWrite(fm, p, []byte("new root"), []byte("hello2"), []byte("goodbye2"), []byte("badbye2"))
	assert.NoError(err)

	// state in store shouldn't change
	h, err = store.Root(context.Background())
	assert.NoError(err)
	assert.Equal(hash.Hash{}, h)
	assert.Equal(constants.NomsVersion, store.Version())

	err = store.Rebase(context.Background())
	assert.NoError(err)

	// NOW it should
	h, err = store.Root(context.Background())
	assert.NoError(err)
	assert.Equal(newRoot, h)
	assert.Equal(constants.NomsVersion, store.Version())
	assertDataInStore(chunks, store, assert)
}

func TestChunkStoreCommit(t *testing.T) {
	assert := assert.New(t)
	_, _, store := makeStoreWithFakes(t)
	defer store.Close()

	h, err := store.Root(context.Background())
	assert.NoError(err)
	assert.Equal(hash.Hash{}, h)

	newRootChunk := chunks.NewChunk([]byte("new root"))
	newRoot := newRootChunk.Hash()
	err = store.Put(context.Background(), newRootChunk)
	assert.NoError(err)
	success, err := store.Commit(context.Background(), newRoot, hash.Hash{})
	assert.NoError(err)
	if assert.True(success) {
		has, err := store.Has(context.Background(), newRoot)
		assert.NoError(err)
		assert.True(has)
		h, err := store.Root(context.Background())
		assert.NoError(err)
		assert.Equal(newRoot, h)
	}

	secondRootChunk := chunks.NewChunk([]byte("newer root"))
	secondRoot := secondRootChunk.Hash()
	err = store.Put(context.Background(), secondRootChunk)
	assert.NoError(err)
	success, err = store.Commit(context.Background(), secondRoot, newRoot)
	assert.NoError(err)
	if assert.True(success) {
		h, err := store.Root(context.Background())
		assert.NoError(err)
		assert.Equal(secondRoot, h)
		has, err := store.Has(context.Background(), newRoot)
		assert.NoError(err)
		assert.True(has)
		has, err = store.Has(context.Background(), secondRoot)
		assert.NoError(err)
		assert.True(has)
	}
}

func TestChunkStoreManifestAppearsAfterConstruction(t *testing.T) {
	assert := assert.New(t)
	fm, p, store := makeStoreWithFakes(t)
	defer store.Close()

	h, err := store.Root(context.Background())
	assert.NoError(err)
	assert.Equal(hash.Hash{}, h)
	assert.Equal(constants.NomsVersion, store.Version())

	// Simulate another process writing a manifest behind store's back.
	interloperWrite(fm, p, []byte("new root"), []byte("hello2"), []byte("goodbye2"), []byte("badbye2"))

	// state in store shouldn't change
	h, err = store.Root(context.Background())
	assert.NoError(err)
	assert.Equal(hash.Hash{}, h)
	assert.Equal(constants.NomsVersion, store.Version())
}

func TestChunkStoreManifestFirstWriteByOtherProcess(t *testing.T) {
	assert := assert.New(t)
	fm := &fakeManifest{}
	mm := manifestManager{fm, newManifestCache(0), newManifestLocks()}
	p := newFakeTablePersister()

	// Simulate another process writing a manifest behind store's back.
	newRoot, chunks, err := interloperWrite(fm, p, []byte("new root"), []byte("hello2"), []byte("goodbye2"), []byte("badbye2"))
	assert.NoError(err)

	store, err := newNomsBlockStore(context.Background(), constants.Format718String, mm, p, inlineConjoiner{defaultMaxTables}, defaultMemTableSize)
	assert.NoError(err)
	defer store.Close()

	h, err := store.Root(context.Background())
	assert.NoError(err)
	assert.Equal(newRoot, h)
	assert.Equal(constants.NomsVersion, store.Version())
	assertDataInStore(chunks, store, assert)
}

func TestChunkStoreCommitOptimisticLockFail(t *testing.T) {
	assert := assert.New(t)
	fm, p, store := makeStoreWithFakes(t)
	defer store.Close()

	// Simulate another process writing a manifest behind store's back.
	newRoot, chunks, err := interloperWrite(fm, p, []byte("new root"), []byte("hello2"), []byte("goodbye2"), []byte("badbye2"))
	assert.NoError(err)

	newRoot2 := hash.Of([]byte("new root 2"))
	success, err := store.Commit(context.Background(), newRoot2, hash.Hash{})
	assert.NoError(err)
	assert.False(success)
	assertDataInStore(chunks, store, assert)
	success, err = store.Commit(context.Background(), newRoot2, newRoot)
	assert.NoError(err)
	assert.True(success)
}

func TestChunkStoreManifestPreemptiveOptimisticLockFail(t *testing.T) {
	assert := assert.New(t)
	fm := &fakeManifest{}
	mm := manifestManager{fm, newManifestCache(defaultManifestCacheSize), newManifestLocks()}
	p := newFakeTablePersister()
	c := inlineConjoiner{defaultMaxTables}

	store, err := newNomsBlockStore(context.Background(), constants.Format718String, mm, p, c, defaultMemTableSize)
	assert.NoError(err)
	defer store.Close()

	// Simulate another goroutine writing a manifest behind store's back.
	interloper, err := newNomsBlockStore(context.Background(), constants.Format718String, mm, p, c, defaultMemTableSize)
	assert.NoError(err)
	defer interloper.Close()

	chunk := chunks.NewChunk([]byte("hello"))
	err = interloper.Put(context.Background(), chunk)
	assert.NoError(err)
	assert.True(interloper.Commit(context.Background(), chunk.Hash(), hash.Hash{}))

	// Try to land a new chunk in store, which should fail AND not persist the contents of store.mt
	chunk = chunks.NewChunk([]byte("goodbye"))
	err = store.Put(context.Background(), chunk)
	assert.NoError(err)
	assert.NotNil(store.mt)
	assert.False(store.Commit(context.Background(), chunk.Hash(), hash.Hash{}))
	assert.NotNil(store.mt)

	h, err := store.Root(context.Background())
	assert.NoError(err)
	success, err := store.Commit(context.Background(), chunk.Hash(), h)
	assert.NoError(err)
	assert.True(success)
	assert.Nil(store.mt)

	h, err = store.Root(context.Background())
	assert.NoError(err)
	assert.Equal(chunk.Hash(), h)
	assert.Equal(constants.NomsVersion, store.Version())
}

func TestChunkStoreCommitLocksOutFetch(t *testing.T) {
	assert := assert.New(t)
	fm := &fakeManifest{name: "foo"}
	upm := &updatePreemptManifest{manifest: fm}
	mm := manifestManager{upm, newManifestCache(defaultManifestCacheSize), newManifestLocks()}
	p := newFakeTablePersister()
	c := inlineConjoiner{defaultMaxTables}

	store, err := newNomsBlockStore(context.Background(), constants.Format718String, mm, p, c, defaultMemTableSize)
	assert.NoError(err)
	defer store.Close()

	// store.Commit() should lock out calls to mm.Fetch()
	wg := sync.WaitGroup{}
	fetched := manifestContents{}
	upm.preUpdate = func() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var err error
			_, fetched, err = mm.Fetch(context.Background(), nil)
			assert.NoError(err)
		}()
	}

	rootChunk := chunks.NewChunk([]byte("new root"))
	err = store.Put(context.Background(), rootChunk)
	assert.NoError(err)
	h, err := store.Root(context.Background())
	assert.NoError(err)
	success, err := store.Commit(context.Background(), rootChunk.Hash(), h)
	assert.NoError(err)
	assert.True(success)

	wg.Wait()
	h, err = store.Root(context.Background())
	assert.NoError(err)
	assert.Equal(h, fetched.root)
}

func TestChunkStoreSerializeCommits(t *testing.T) {
	assert := assert.New(t)
	fm := &fakeManifest{name: "foo"}
	upm := &updatePreemptManifest{manifest: fm}
	mc := newManifestCache(defaultManifestCacheSize)
	l := newManifestLocks()
	p := newFakeTablePersister()
	c := inlineConjoiner{defaultMaxTables}

	store, err := newNomsBlockStore(context.Background(), constants.Format718String, manifestManager{upm, mc, l}, p, c, defaultMemTableSize)
	assert.NoError(err)
	defer store.Close()

	storeChunk := chunks.NewChunk([]byte("store"))
	interloperChunk := chunks.NewChunk([]byte("interloper"))
	updateCount := 0

	interloper, err := newNomsBlockStore(
		context.Background(),
		constants.Format718String,
		manifestManager{
			updatePreemptManifest{fm, func() { updateCount++ }}, mc, l,
		},
		p,
		c,
		defaultMemTableSize)
	assert.NoError(err)
	defer interloper.Close()

	wg := sync.WaitGroup{}
	upm.preUpdate = func() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := interloper.Put(context.Background(), interloperChunk)
			assert.NoError(err)
			h, err := interloper.Root(context.Background())
			assert.NoError(err)
			success, err := interloper.Commit(context.Background(), h, h)
			assert.NoError(err)
			assert.True(success)
		}()

		updateCount++
	}

	err = store.Put(context.Background(), storeChunk)
	assert.NoError(err)
	h, err := store.Root(context.Background())
	assert.NoError(err)
	success, err := store.Commit(context.Background(), h, h)
	assert.NoError(err)
	assert.True(success)

	wg.Wait()
	assert.Equal(2, updateCount)
	assert.True(interloper.Has(context.Background(), storeChunk.Hash()))
	assert.True(interloper.Has(context.Background(), interloperChunk.Hash()))
}

func makeStoreWithFakes(t *testing.T) (fm *fakeManifest, p tablePersister, store *NomsBlockStore) {
	fm = &fakeManifest{}
	mm := manifestManager{fm, newManifestCache(0), newManifestLocks()}
	p = newFakeTablePersister()
	store, err := newNomsBlockStore(context.Background(), constants.Format718String, mm, p, inlineConjoiner{defaultMaxTables}, 0)
	assert.NoError(t, err)
	return
}

// Simulate another process writing a manifest behind store's back.
func interloperWrite(fm *fakeManifest, p tablePersister, rootChunk []byte, chunks ...[]byte) (newRoot hash.Hash, persisted [][]byte, err error) {
	newLock, newRoot := computeAddr([]byte("locker")), hash.Of(rootChunk)
	persisted = append(chunks, rootChunk)

	var src chunkSource
	src, err = p.Persist(context.Background(), createMemTable(persisted), nil, &Stats{})

	if err != nil {
		return hash.Hash{}, nil, err
	}

	fm.set(constants.NomsVersion, newLock, newRoot, []tableSpec{{mustAddr(src.hash()), uint32(len(chunks))}})
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
	if fm.contents.lock != (addr{}) {
		return true, fm.contents, nil
	}

	return false, manifestContents{}, nil
}

// Update checks whether |lastLock| == |fm.lock| and, if so, updates internal
// fake manifest state as per the manifest.Update() contract: |fm.lock| is set
// to |newLock|, |fm.root| is set to |newRoot|, and the contents of |specs|
// replace |fm.tableSpecs|. If |lastLock| != |fm.lock|, then the update
// fails. Regardless of success or failure, the current state is returned.
func (fm *fakeManifest) Update(ctx context.Context, lastLock addr, newContents manifestContents, stats *Stats, writeHook func() error) (manifestContents, error) {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	if fm.contents.lock == lastLock {
		fm.contents = manifestContents{newContents.vers, newContents.lock, newContents.root, addr(hash.Hash{}), nil}
		fm.contents.specs = make([]tableSpec, len(newContents.specs))
		copy(fm.contents.specs, newContents.specs)
	}
	return fm.contents, nil
}

func (fm *fakeManifest) set(version string, lock addr, root hash.Hash, specs []tableSpec) {
	fm.contents = manifestContents{version, lock, root, addr(hash.Hash{}), specs}
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

var _ tablePersister = fakeTablePersister{}

func (ftp fakeTablePersister) Persist(ctx context.Context, mt *memTable, haver chunkReader, stats *Stats) (chunkSource, error) {
	if mustUint32(mt.count()) > 0 {
		name, data, chunkCount, err := mt.write(haver, stats)

		if err != nil {
			return emptyChunkSource{}, err
		}

		if chunkCount > 0 {
			ftp.mu.Lock()
			defer ftp.mu.Unlock()
			ti, err := parseTableIndex(data)

			if err != nil {
				return nil, err
			}

			ftp.sources[name] = newTableReader(ti, tableReaderAtFromBytes(data), fileBlockSize)
			return chunkSourceAdapter{ftp.sources[name], name}, nil
		}
	}
	return emptyChunkSource{}, nil
}

func (ftp fakeTablePersister) ConjoinAll(ctx context.Context, sources chunkSources, stats *Stats) (chunkSource, error) {
	name, data, chunkCount, err := compactSourcesToBuffer(sources)

	if err != nil {
		return nil, err
	}

	if chunkCount > 0 {
		ftp.mu.Lock()
		defer ftp.mu.Unlock()
		ti, err := parseTableIndex(data)

		if err != nil {
			return nil, err
		}

		ftp.sources[name] = newTableReader(ti, tableReaderAtFromBytes(data), fileBlockSize)
		return chunkSourceAdapter{ftp.sources[name], name}, nil
	}
	return emptyChunkSource{}, nil
}

func compactSourcesToBuffer(sources chunkSources) (name addr, data []byte, chunkCount uint32, err error) {
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

	for _, src := range sources {
		chunks := make(chan extractRecord)
		go func() {
			defer close(chunks)
			err := src.extract(context.Background(), chunks)

			if err != nil {
				chunks <- extractRecord{a: mustAddr(src.hash()), err: err}
			}
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
		return addr{}, nil, 0, fmt.Errorf(errString)
	}

	tableSize, name, err := tw.finish()

	if err != nil {
		return addr{}, nil, 0, err
	}

	return name, buff[:tableSize], chunkCount, nil
}

func (ftp fakeTablePersister) Open(ctx context.Context, name addr, chunkCount uint32, stats *Stats) (chunkSource, error) {
	ftp.mu.RLock()
	defer ftp.mu.RUnlock()
	return chunkSourceAdapter{ftp.sources[name], name}, nil
}

func (ftp fakeTablePersister) PruneTableFiles(_ context.Context, _ manifestContents) error {
	return chunks.ErrUnsupportedOperation
}
