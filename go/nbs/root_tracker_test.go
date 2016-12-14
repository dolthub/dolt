// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"bytes"
	"sync"
	"testing"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/constants"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/testify/assert"
)

func TestChunkStoreZeroValue(t *testing.T) {
	assert := assert.New(t)
	_, _, store := makeStoreWithFakes(t)
	defer store.Close()

	// No manifest file gets written until the first call to UpdateRoot(). Prior to that, Root() will simply return hash.Hash{}.
	assert.Equal(hash.Hash{}, store.Root())
	assert.Equal(constants.NomsVersion, store.Version())
}

func TestChunkStoreVersion(t *testing.T) {
	assert := assert.New(t)
	_, _, store := makeStoreWithFakes(t)
	defer store.Close()

	assert.Equal(constants.NomsVersion, store.Version())
	newRoot := hash.Of([]byte("new root"))
	if assert.True(store.UpdateRoot(newRoot, hash.Hash{})) {
		assert.Equal(constants.NomsVersion, store.Version())
	}
}

func TestChunkStoreUpdateRoot(t *testing.T) {
	assert := assert.New(t)
	_, _, store := makeStoreWithFakes(t)
	defer store.Close()

	assert.Equal(hash.Hash{}, store.Root())

	newRootChunk := chunks.NewChunk([]byte("new root"))
	newRoot := newRootChunk.Hash()
	store.Put(newRootChunk)
	if assert.True(store.UpdateRoot(newRoot, hash.Hash{})) {
		assert.True(store.Has(newRoot))
		assert.Equal(newRoot, store.Root())
	}

	secondRootChunk := chunks.NewChunk([]byte("newer root"))
	secondRoot := secondRootChunk.Hash()
	store.Put(secondRootChunk)
	if assert.True(store.UpdateRoot(secondRoot, newRoot)) {
		assert.Equal(secondRoot, store.Root())
		assert.True(store.Has(newRoot))
		assert.True(store.Has(secondRoot))
	}
}

func TestChunkStoreManifestAppearsAfterConstruction(t *testing.T) {
	assert := assert.New(t)
	fm, tt, store := makeStoreWithFakes(t)
	defer store.Close()

	assert.Equal(hash.Hash{}, store.Root())
	assert.Equal(constants.NomsVersion, store.Version())

	// Simulate another process writing a manifest after construction.
	chunks := [][]byte{[]byte("hello2"), []byte("goodbye2"), []byte("badbye2")}
	newRoot := hash.Of([]byte("new root"))
	src := tt.p.Compact(createMemTable(chunks), nil)
	fm.set(constants.NomsVersion, newRoot, []tableSpec{{src.hash(), uint32(len(chunks))}})

	// state in store shouldn't change
	assert.Equal(hash.Hash{}, store.Root())
	assert.Equal(constants.NomsVersion, store.Version())
}

func TestChunkStoreManifestFirstWriteByOtherProcess(t *testing.T) {
	assert := assert.New(t)
	fm := &fakeManifest{}
	tt := newFakeTableSet()

	// Simulate another process having already written a manifest.
	chunks := [][]byte{[]byte("hello2"), []byte("goodbye2"), []byte("badbye2")}
	newRoot := hash.Of([]byte("new root"))
	src := tt.p.Compact(createMemTable(chunks), nil)
	fm.set(constants.NomsVersion, newRoot, []tableSpec{{src.hash(), uint32(len(chunks))}})

	store := newNomsBlockStore(fm, tt, defaultMemTableSize)
	defer store.Close()

	assert.Equal(newRoot, store.Root())
	assert.Equal(constants.NomsVersion, store.Version())
	assertDataInStore(chunks, store, assert)
}

func TestChunkStoreUpdateRootOptimisticLockFail(t *testing.T) {
	assert := assert.New(t)
	fm, tt, store := makeStoreWithFakes(t)
	defer store.Close()

	// Simulate another process writing a manifest behind store's back.
	chunks := [][]byte{[]byte("hello2"), []byte("goodbye2"), []byte("badbye2")}
	newRoot := hash.Of([]byte("new root"))
	src := tt.p.Compact(createMemTable(chunks), nil)
	fm.set(constants.NomsVersion, newRoot, []tableSpec{{src.hash(), uint32(len(chunks))}})

	newRoot2 := hash.Of([]byte("new root 2"))
	assert.False(store.UpdateRoot(newRoot2, hash.Hash{}))
	assertDataInStore(chunks, store, assert)
	assert.True(store.UpdateRoot(newRoot2, newRoot))
}

func makeStoreWithFakes(t *testing.T) (fm *fakeManifest, tt tableSet, store *NomsBlockStore) {
	fm = &fakeManifest{}
	tt = newFakeTableSet()
	store = newNomsBlockStore(fm, tt, 0)
	return
}

func createMemTable(chunks [][]byte) *memTable {
	mt := newMemTable(1 << 10)
	for _, c := range chunks {
		mt.addChunk(computeAddr(c), c)
	}
	return mt
}

func assertDataInStore(slices [][]byte, store chunks.ChunkSource, assert *assert.Assertions) {
	for _, data := range slices {
		assert.True(store.Has(chunks.NewChunk(data).Hash()))
	}
}

// fakeManifest simulates a fileManifest without touching disk.
type fakeManifest struct {
	version    string
	root       hash.Hash
	tableSpecs []tableSpec
	mu         sync.RWMutex
}

// ParseIfExists returns any fake manifest data the caller has injected using Update() or set(). It treats an empty |fm.root| as a non-existent manifest.
func (fm *fakeManifest) ParseIfExists(readHook func()) (exists bool, vers string, root hash.Hash, tableSpecs []tableSpec) {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	if fm.root != (hash.Hash{}) {
		return true, fm.version, fm.root, fm.tableSpecs
	}
	return false, "", hash.Hash{}, nil
}

// Update checks whether |root| == |fm.root| and, if so, updates internal fake manifest state as per the manifest.Update() contract: |fm.root| is set to |newRoot|, and the contents of |specs| are merged into |fm.tableSpecs|. If |root| != |fm.root|, then the update fails. Regardless of success or failure, the current state is returned.
func (fm *fakeManifest) Update(specs []tableSpec, root, newRoot hash.Hash, writeHook func()) (actual hash.Hash, tableSpecs []tableSpec) {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	if fm.root != root {
		return fm.root, fm.tableSpecs
	}
	fm.version = constants.NomsVersion
	fm.root = newRoot

	known := map[addr]struct{}{}
	for _, t := range fm.tableSpecs {
		known[t.name] = struct{}{}
	}

	for _, t := range specs {
		if _, present := known[t.name]; !present {
			fm.tableSpecs = append(fm.tableSpecs, t)
		}
	}
	return fm.root, fm.tableSpecs
}

func (fm *fakeManifest) set(version string, root hash.Hash, specs []tableSpec) {
	fm.version, fm.root, fm.tableSpecs = version, root, specs
}

func newFakeTableSet() tableSet {
	return tableSet{p: newFakeTablePersister(), rl: make(chan struct{}, 1)}
}

func newFakeTablePersister() tablePersister {
	return fakeTablePersister{map[addr]tableReader{}}
}

type fakeTablePersister struct {
	sources map[addr]tableReader
}

func (ftp fakeTablePersister) Compact(mt *memTable, haver chunkReader) chunkSource {
	if mt.count() > 0 {
		var data []byte
		name, data, _ := mt.write(haver)
		ftp.sources[name] = newTableReader(parseTableIndex(data), bytes.NewReader(data), fileReadAmpThresh)
		return chunkSourceAdapter{ftp.sources[name], name}
	}
	return emptyChunkSource{}
}

func (ftp fakeTablePersister) Open(name addr, chunkCount uint32) chunkSource {
	return chunkSourceAdapter{ftp.sources[name], name}
}

type chunkSourceAdapter struct {
	tableReader
	h addr
}

func (csa chunkSourceAdapter) close() error {
	return nil
}

func (csa chunkSourceAdapter) hash() addr {
	return csa.h
}
