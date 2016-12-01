// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/constants"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/testify/assert"
)

func makeStoreInTempDir(t *testing.T) (dir string, store *NomsBlockStore) {
	dir, err := ioutil.TempDir("", "")
	assert.NoError(t, err)
	store = NewBlockStore(dir, defaultMemTableSize)
	return
}

func TestChunkStoreZeroValue(t *testing.T) {
	assert := assert.New(t)
	dir, store := makeStoreInTempDir(t)
	defer os.RemoveAll(dir)
	defer store.Close()

	// No manifest file gets written until the first call to UpdateRoot(). Prior to that, Root() will simply return hash.Hash{}.
	assert.Equal(hash.Hash{}, store.Root())
	assert.Equal(constants.NomsVersion, store.Version())
}

func TestChunkStoreVersion(t *testing.T) {
	assert := assert.New(t)
	dir, store := makeStoreInTempDir(t)
	defer os.RemoveAll(dir)
	defer store.Close()

	assert.Equal(constants.NomsVersion, store.Version())
	newRoot := hash.FromData([]byte("new root"))
	if assert.True(store.UpdateRoot(newRoot, hash.Hash{})) {
		assert.Equal(constants.NomsVersion, store.Version())
	}
}

func TestChunkStoreUpdateRoot(t *testing.T) {
	assert := assert.New(t)
	dir, store := makeStoreInTempDir(t)
	defer os.RemoveAll(dir)
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
	dir, store := makeStoreInTempDir(t)
	defer os.RemoveAll(dir)
	defer store.Close()

	assert.Equal(hash.Hash{}, store.Root())
	assert.Equal(constants.NomsVersion, store.Version())

	// Simulate another process writing a manifest (with an old Noms version) after construction.
	chunks := [][]byte{[]byte("hello2"), []byte("goodbye2"), []byte("badbye2")}
	newRoot := hash.FromData([]byte("new root"))
	h := createOnDiskTable(dir, chunks)
	b, err := clobberManifest(dir, strings.Join([]string{StorageVersion, "0", newRoot.String(), h.String(), "3"}, ":"))
	assert.NoError(err, string(b))

	// Creating another store should reflect the manifest written above.
	store2 := NewBlockStore(dir, defaultMemTableSize)
	defer store2.Close()

	assert.Equal(newRoot, store2.Root())
	assert.Equal("0", store2.Version())
	assertDataInStore(chunks, store2, assert)
}

func createOnDiskTable(dir string, chunks [][]byte) addr {
	tableData, h := buildTable(chunks)
	d.PanicIfError(ioutil.WriteFile(filepath.Join(dir, h.String()), tableData, 0666))
	return h
}

func assertDataInStore(slices [][]byte, store chunks.ChunkSource, assert *assert.Assertions) {
	for _, data := range slices {
		assert.True(store.Has(chunks.NewChunk(data).Hash()))
	}
}

func TestChunkStoreManifestFirstWriteByOtherProcess(t *testing.T) {
	assert := assert.New(t)
	dir, err := ioutil.TempDir(os.TempDir(), "")
	assert.NoError(err)
	defer os.RemoveAll(dir)

	// Simulate another process having already written a manifest (with an old Noms version).
	chunks := [][]byte{[]byte("hello2"), []byte("goodbye2"), []byte("badbye2")}
	h := createOnDiskTable(dir, chunks)
	newRoot := hash.FromData([]byte("new root"))
	b, err := tryClobberManifest(dir, strings.Join([]string{StorageVersion, "0", newRoot.String(), h.String(), "3"}, ":"))
	assert.NoError(err, string(b))

	store := hookedNewNomsBlockStore(dir, defaultMemTableSize, func() {
		// This should fail to get the lock, and therefore _not_ clobber the manifest.
		badRoot := hash.FromData([]byte("bad root"))
		b, err := tryClobberManifest(dir, strings.Join([]string{StorageVersion, "0", badRoot.String(), h.String(), "3"}, ":"))
		assert.NoError(err, string(b))
	})
	defer store.Close()

	assert.Equal(newRoot, store.Root())
	assert.Equal("0", store.Version())
	assertDataInStore(chunks, store, assert)
}
