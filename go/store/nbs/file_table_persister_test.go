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
	"github.com/dolthub/dolt/go/store/hash"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeTempDir(t *testing.T) string {
	dir, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	return dir
}

func writeTableData(dir string, chunx ...[]byte) (hash.Hash, error) {
	tableData, name, err := buildTable(chunx)

	if err != nil {
		return hash.Hash{}, err
	}

	err = os.WriteFile(filepath.Join(dir, name.String()), tableData, 0666)

	if err != nil {
		return hash.Hash{}, err
	}

	return name, nil
}

func removeTables(dir string, names ...hash.Hash) error {
	for _, name := range names {
		if err := file.Remove(filepath.Join(dir, name.String())); err != nil {
			return err
		}
	}
	return nil
}

func TestFSTablePersisterPersist(t *testing.T) {
	ctx := context.Background()
	assert := assert.New(t)
	dir := makeTempDir(t)
	defer file.RemoveAll(dir)
	fts := newFSTablePersister(dir, &UnlimitedQuotaProvider{}, false)

	src, err := persistTableData(fts, testChunks...)
	require.NoError(t, err)
	defer src.close()
	if assert.True(mustUint32(src.count()) > 0) {
		buff, err := os.ReadFile(filepath.Join(dir, src.hash().String()))
		require.NoError(t, err)
		ti, err := parseTableIndexByCopy(ctx, buff, &UnlimitedQuotaProvider{})
		require.NoError(t, err)
		tr, err := newTableReader(ti, tableReaderAtFromBytes(buff), fileBlockSize)
		require.NoError(t, err)
		defer tr.close()
		assertChunksInReader(testChunks, tr, assert)
	}
}

func persistTableData(p tablePersister, chunx ...[]byte) (src chunkSource, err error) {
	mt := newMemTable(testMemTableSize)
	for _, c := range chunx {
		if mt.addChunk(computeAddr(c), c) == chunkNotAdded {
			return nil, fmt.Errorf("memTable too full to add %s", computeAddr(c))
		}
	}
	src, _, err = p.Persist(context.Background(), mt, nil, nil, &Stats{})
	return src, err
}

func TestFSTablePersisterPersistNoData(t *testing.T) {
	assert := assert.New(t)
	mt := newMemTable(testMemTableSize)
	existingTable := newMemTable(testMemTableSize)

	for _, c := range testChunks {
		assert.Equal(mt.addChunk(computeAddr(c), c), chunkAdded)
		assert.Equal(existingTable.addChunk(computeAddr(c), c), chunkAdded)
	}

	dir := makeTempDir(t)
	defer file.RemoveAll(dir)
	fts := newFSTablePersister(dir, &UnlimitedQuotaProvider{}, false)

	src, _, err := fts.Persist(context.Background(), mt, existingTable, nil, &Stats{})
	require.NoError(t, err)
	assert.True(mustUint32(src.count()) == 0)

	_, err = os.Stat(filepath.Join(dir, src.hash().String()))
	assert.True(os.IsNotExist(err), "%v", err)
}

func TestFSTablePersisterConjoinAll(t *testing.T) {
	ctx := context.Background()
	assert := assert.New(t)
	assert.True(len(testChunks) > 1, "Whoops, this test isn't meaningful")
	sources := make(chunkSources, len(testChunks))

	dir := makeTempDir(t)
	defer file.RemoveAll(dir)
	fts := newFSTablePersister(dir, &UnlimitedQuotaProvider{}, false)

	for i, c := range testChunks {
		randChunk := make([]byte, (i+1)*13)
		_, err := rand.Read(randChunk)
		require.NoError(t, err)
		name, err := writeTableData(dir, c, randChunk)
		require.NoError(t, err)
		sources[i], err = fts.Open(ctx, name, 2, nil)
		require.NoError(t, err)
	}
	defer func() {
		for _, s := range sources {
			s.close()
		}
	}()

	src, _, err := fts.ConjoinAll(ctx, sources, &Stats{})
	require.NoError(t, err)
	defer src.close()

	if assert.True(mustUint32(src.count()) > 0) {
		buff, err := os.ReadFile(filepath.Join(dir, src.hash().String()))
		require.NoError(t, err)
		ti, err := parseTableIndexByCopy(ctx, buff, &UnlimitedQuotaProvider{})
		require.NoError(t, err)
		tr, err := newTableReader(ti, tableReaderAtFromBytes(buff), fileBlockSize)
		require.NoError(t, err)
		defer tr.close()
		assertChunksInReader(testChunks, tr, assert)
	}
}

func TestFSTablePersisterConjoinAllDups(t *testing.T) {
	ctx := context.Background()
	assert := assert.New(t)
	dir := makeTempDir(t)
	defer file.RemoveAll(dir)
	fts := newFSTablePersister(dir, &UnlimitedQuotaProvider{}, false)

	reps := 3
	sources := make(chunkSources, reps)
	mt := newMemTable(1 << 10)
	for _, c := range testChunks {
		mt.addChunk(computeAddr(c), c)
	}

	var err error
	sources[0], _, err = fts.Persist(ctx, mt, nil, nil, &Stats{})
	require.NoError(t, err)
	sources[1], err = sources[0].clone()
	require.NoError(t, err)
	sources[2], err = sources[0].clone()
	require.NoError(t, err)

	src, cleanup, err := fts.ConjoinAll(ctx, sources, &Stats{})
	require.NoError(t, err)
	defer src.close()

	// After ConjoinAll runs, we can close the sources and
	// call the cleanup func.
	for _, s := range sources {
		s.close()
	}
	cleanup()

	if assert.True(mustUint32(src.count()) > 0) {
		buff, err := os.ReadFile(filepath.Join(dir, src.hash().String()))
		require.NoError(t, err)
		ti, err := parseTableIndexByCopy(ctx, buff, &UnlimitedQuotaProvider{})
		require.NoError(t, err)
		tr, err := newTableReader(ti, tableReaderAtFromBytes(buff), fileBlockSize)
		require.NoError(t, err)
		defer tr.close()
		assertChunksInReader(testChunks, tr, assert)
		assert.EqualValues(reps*len(testChunks), mustUint32(tr.count()))
	}
}
