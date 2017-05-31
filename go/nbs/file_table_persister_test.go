// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"crypto/rand"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/attic-labs/testify/assert"
)

func TestFSTableCacheOnOpen(t *testing.T) {
	assert := assert.New(t)
	dir := makeTempDir(t)
	defer os.RemoveAll(dir)

	names := []addr{}
	cacheSize := 2
	fc := newFDCache(cacheSize)
	defer fc.Drop()
	fts := newFSTablePersister(dir, fc, nil)

	// Create some tables manually, load them into the cache, and then blow them away
	func() {
		for i := 0; i < cacheSize; i++ {
			name, err := writeTableData(dir, []byte{byte(i)})
			assert.NoError(err)
			names = append(names, name)
		}
		for _, name := range names {
			fts.Open(name, 1)
		}
		removeTables(dir, names...)
	}()

	// Tables should still be cached, even though they're gone from disk
	for i, name := range names {
		src := fts.Open(name, 1)
		h := computeAddr([]byte{byte(i)})
		assert.True(src.has(h))
	}

	// Kick a table out of the cache
	name, err := writeTableData(dir, []byte{0xff})
	assert.NoError(err)
	fts.Open(name, 1)

	present := fc.reportEntries()
	// Since 0 refcount entries are evicted randomly, the only thing we can validate is that fc remains at its target size
	assert.Len(present, cacheSize)
}

func makeTempDir(t *testing.T) string {
	dir, err := ioutil.TempDir("", "")
	assert.NoError(t, err)
	return dir
}

func writeTableData(dir string, chunx ...[]byte) (name addr, err error) {
	var tableData []byte
	tableData, name = buildTable(chunx)
	err = ioutil.WriteFile(filepath.Join(dir, name.String()), tableData, 0666)
	return
}

func removeTables(dir string, names ...addr) error {
	for _, name := range names {
		if err := os.Remove(filepath.Join(dir, name.String())); err != nil {
			return err
		}
	}
	return nil
}

func contains(s sort.StringSlice, e string) bool {
	for _, c := range s {
		if c == e {
			return true
		}
	}
	return false
}

func TestFSTablePersisterPersist(t *testing.T) {
	assert := assert.New(t)
	dir := makeTempDir(t)
	defer os.RemoveAll(dir)
	fc := newFDCache(defaultMaxTables)
	defer fc.Drop()
	fts := newFSTablePersister(dir, fc, nil)

	src, err := persistTableData(fts, testChunks...)
	assert.NoError(err)
	if assert.True(src.count() > 0) {
		buff, err := ioutil.ReadFile(filepath.Join(dir, src.hash().String()))
		assert.NoError(err)
		tr := newTableReader(parseTableIndex(buff), tableReaderAtFromBytes(buff), fileBlockSize)
		assertChunksInReader(testChunks, tr, assert)
	}
}

func persistTableData(p tablePersister, chunx ...[]byte) (src chunkSource, err error) {
	mt := newMemTable(testMemTableSize)
	for _, c := range chunx {
		if !mt.addChunk(computeAddr(c), c) {
			return nil, fmt.Errorf("memTable too full to add %s", computeAddr(c))
		}
	}
	return p.Persist(mt, nil, &Stats{}), nil
}

func TestFSTablePersisterPersistNoData(t *testing.T) {
	assert := assert.New(t)
	mt := newMemTable(testMemTableSize)
	existingTable := newMemTable(testMemTableSize)

	for _, c := range testChunks {
		assert.True(mt.addChunk(computeAddr(c), c))
		assert.True(existingTable.addChunk(computeAddr(c), c))
	}

	dir := makeTempDir(t)
	defer os.RemoveAll(dir)
	fc := newFDCache(defaultMaxTables)
	defer fc.Drop()
	fts := newFSTablePersister(dir, fc, nil)

	src := fts.Persist(mt, existingTable, &Stats{})
	assert.True(src.count() == 0)

	_, err := os.Stat(filepath.Join(dir, src.hash().String()))
	assert.True(os.IsNotExist(err), "%v", err)
}

func TestFSTablePersisterCacheOnPersist(t *testing.T) {
	assert := assert.New(t)
	dir := makeTempDir(t)
	fc := newFDCache(1)
	defer fc.Drop()
	fts := newFSTablePersister(dir, fc, nil)
	defer os.RemoveAll(dir)

	var name addr
	func() {
		src, err := persistTableData(fts, testChunks...)
		assert.NoError(err)
		name = src.hash()
		removeTables(dir, name)
	}()

	// Table should still be cached, even though it's gone from disk
	src := fts.Open(name, uint32(len(testChunks)))
	assertChunksInReader(testChunks, src, assert)

	// Evict |name| from cache
	src, err := persistTableData(fts, []byte{0xff})
	assert.NoError(err)

	present := fc.reportEntries()
	// Since 0 refcount entries are evicted randomly, the only thing we can validate is that fc remains at its target size
	assert.Len(present, 1)
}

func TestFSTablePersisterConjoinAll(t *testing.T) {
	assert := assert.New(t)
	assert.True(len(testChunks) > 1, "Whoops, this test isn't meaningful")
	sources := make(chunkSources, len(testChunks))

	dir := makeTempDir(t)
	defer os.RemoveAll(dir)
	fc := newFDCache(len(sources))
	defer fc.Drop()
	fts := newFSTablePersister(dir, fc, nil)

	for i, c := range testChunks {
		randChunk := make([]byte, (i+1)*13)
		_, err := rand.Read(randChunk)
		assert.NoError(err)
		name, err := writeTableData(dir, c, randChunk)
		assert.NoError(err)
		sources[i] = fts.Open(name, 2)
	}

	src := fts.ConjoinAll(sources, &Stats{})

	if assert.True(src.count() > 0) {
		buff, err := ioutil.ReadFile(filepath.Join(dir, src.hash().String()))
		assert.NoError(err)
		tr := newTableReader(parseTableIndex(buff), tableReaderAtFromBytes(buff), fileBlockSize)
		assertChunksInReader(testChunks, tr, assert)
	}

	present := fc.reportEntries()
	// Since 0 refcount entries are evicted randomly, the only thing we can validate is that fc remains at its target size
	assert.Len(present, len(sources))
}

func TestFSTablePersisterConjoinAllDups(t *testing.T) {
	assert := assert.New(t)
	dir := makeTempDir(t)
	defer os.RemoveAll(dir)
	fc := newFDCache(defaultMaxTables)
	defer fc.Drop()
	fts := newFSTablePersister(dir, fc, nil)

	reps := 3
	sources := make(chunkSources, reps)
	for i := 0; i < reps; i++ {
		mt := newMemTable(1 << 10)
		for _, c := range testChunks {
			mt.addChunk(computeAddr(c), c)
		}
		sources[i] = fts.Persist(mt, nil, &Stats{})
	}
	src := fts.ConjoinAll(sources, &Stats{})

	if assert.True(src.count() > 0) {
		buff, err := ioutil.ReadFile(filepath.Join(dir, src.hash().String()))
		assert.NoError(err)
		tr := newTableReader(parseTableIndex(buff), tableReaderAtFromBytes(buff), fileBlockSize)
		assertChunksInReader(testChunks, tr, assert)
		assert.EqualValues(reps*len(testChunks), tr.count())
	}
}
