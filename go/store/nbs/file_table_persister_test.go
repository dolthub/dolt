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
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/dolthub/dolt/go/libraries/utils/file"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFSTableCacheOnOpen(t *testing.T) {
	assert := assert.New(t)
	dir := makeTempDir(t)
	defer file.RemoveAll(dir)

	names := []addr{}
	cacheSize := 2
	fc := newFDCache(cacheSize)
	defer fc.Drop()
	fts := newFSTablePersister(dir, fc, nil)

	// Create some tables manually, load them into the cache
	func() {
		for i := 0; i < cacheSize; i++ {
			name, err := writeTableData(dir, []byte{byte(i)})
			require.NoError(t, err)
			names = append(names, name)
		}
		for _, name := range names {
			_, err := fts.Open(context.Background(), name, 1, nil)
			require.NoError(t, err)
		}
	}()

	// Tables should still be cached and on disk
	for i, name := range names {
		src, err := fts.Open(context.Background(), name, 1, nil)
		require.NoError(t, err)
		h := computeAddr([]byte{byte(i)})
		assert.True(src.has(h))
	}

	// Kick a table out of the cache
	name, err := writeTableData(dir, []byte{0xff})
	require.NoError(t, err)
	_, err = fts.Open(context.Background(), name, 1, nil)
	require.NoError(t, err)

	present := fc.reportEntries()
	// Since 0 refcount entries are evicted randomly, the only thing we can validate is that fc remains at its target size
	assert.Len(present, cacheSize)

	err = fc.ShrinkCache()
	require.NoError(t, err)
	err = removeTables(dir, names...)
	require.NoError(t, err)
}

func makeTempDir(t *testing.T) string {
	dir, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	return dir
}

func writeTableData(dir string, chunx ...[]byte) (addr, error) {
	tableData, name, err := buildTable(chunx)

	if err != nil {
		return addr{}, err
	}

	err = os.WriteFile(filepath.Join(dir, name.String()), tableData, 0666)

	if err != nil {
		return addr{}, err
	}

	return name, nil
}

func removeTables(dir string, names ...addr) error {
	for _, name := range names {
		if err := file.Remove(filepath.Join(dir, name.String())); err != nil {
			return err
		}
	}
	return nil
}

func TestFSTablePersisterPersist(t *testing.T) {
	assert := assert.New(t)
	dir := makeTempDir(t)
	defer file.RemoveAll(dir)
	fc := newFDCache(defaultMaxTables)
	defer fc.Drop()
	fts := newFSTablePersister(dir, fc, nil)

	src, err := persistTableData(fts, testChunks...)
	require.NoError(t, err)
	if assert.True(mustUint32(src.count()) > 0) {
		buff, err := os.ReadFile(filepath.Join(dir, mustAddr(src.hash()).String()))
		require.NoError(t, err)
		ti, err := parseTableIndexByCopy(buff)
		require.NoError(t, err)
		tr, err := newTableReader(ti, tableReaderAtFromBytes(buff), fileBlockSize)
		require.NoError(t, err)
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
	return p.Persist(context.Background(), mt, nil, &Stats{})
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
	defer file.RemoveAll(dir)
	fc := newFDCache(defaultMaxTables)
	defer fc.Drop()
	fts := newFSTablePersister(dir, fc, nil)

	src, err := fts.Persist(context.Background(), mt, existingTable, &Stats{})
	require.NoError(t, err)
	assert.True(mustUint32(src.count()) == 0)

	_, err = os.Stat(filepath.Join(dir, mustAddr(src.hash()).String()))
	assert.True(os.IsNotExist(err), "%v", err)
}

func TestFSTablePersisterCacheOnPersist(t *testing.T) {
	assert := assert.New(t)
	dir := makeTempDir(t)
	fc := newFDCache(1)
	defer fc.Drop()
	fts := newFSTablePersister(dir, fc, nil)
	defer file.RemoveAll(dir)

	var name addr
	func() {
		src, err := persistTableData(fts, testChunks...)
		require.NoError(t, err)
		name = mustAddr(src.hash())
	}()

	// Table should still be cached
	src, err := fts.Open(context.Background(), name, uint32(len(testChunks)), nil)
	require.NoError(t, err)
	assertChunksInReader(testChunks, src, assert)

	// Evict |name| from cache
	_, err = persistTableData(fts, []byte{0xff})
	require.NoError(t, err)

	present := fc.reportEntries()
	// Since 0 refcount entries are evicted randomly, the only thing we can validate is that fc remains at its target size
	assert.Len(present, 1)

	err = removeTables(dir, name)
	require.NoError(t, err)
}

func TestFSTablePersisterConjoinAll(t *testing.T) {
	assert := assert.New(t)
	assert.True(len(testChunks) > 1, "Whoops, this test isn't meaningful")
	sources := make(chunkSources, len(testChunks))

	dir := makeTempDir(t)
	defer file.RemoveAll(dir)
	fc := newFDCache(len(sources))
	defer fc.Drop()
	fts := newFSTablePersister(dir, fc, nil)

	for i, c := range testChunks {
		randChunk := make([]byte, (i+1)*13)
		_, err := rand.Read(randChunk)
		require.NoError(t, err)
		name, err := writeTableData(dir, c, randChunk)
		require.NoError(t, err)
		sources[i], err = fts.Open(context.Background(), name, 2, nil)
		require.NoError(t, err)
	}

	src, err := fts.ConjoinAll(context.Background(), sources, &Stats{})
	require.NoError(t, err)

	if assert.True(mustUint32(src.count()) > 0) {
		buff, err := os.ReadFile(filepath.Join(dir, mustAddr(src.hash()).String()))
		require.NoError(t, err)
		ti, err := parseTableIndexByCopy(buff)
		require.NoError(t, err)
		tr, err := newTableReader(ti, tableReaderAtFromBytes(buff), fileBlockSize)
		require.NoError(t, err)
		assertChunksInReader(testChunks, tr, assert)
	}

	present := fc.reportEntries()
	// Since 0 refcount entries are evicted randomly, the only thing we can validate is that fc remains at its target size
	assert.Len(present, len(sources))
}

func TestFSTablePersisterConjoinAllDups(t *testing.T) {
	assert := assert.New(t)
	dir := makeTempDir(t)
	defer file.RemoveAll(dir)
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

		var err error
		sources[i], err = fts.Persist(context.Background(), mt, nil, &Stats{})
		require.NoError(t, err)
	}

	src, err := fts.ConjoinAll(context.Background(), sources, &Stats{})
	require.NoError(t, err)

	if assert.True(mustUint32(src.count()) > 0) {
		buff, err := os.ReadFile(filepath.Join(dir, mustAddr(src.hash()).String()))
		require.NoError(t, err)
		ti, err := parseTableIndexByCopy(buff)
		require.NoError(t, err)
		tr, err := newTableReader(ti, tableReaderAtFromBytes(buff), fileBlockSize)
		require.NoError(t, err)
		assertChunksInReader(testChunks, tr, assert)
		assert.EqualValues(reps*len(testChunks), mustUint32(tr.count()))
	}
}
