// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"bytes"
	"crypto/rand"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/attic-labs/testify/assert"
)

func TestFSTablePersisterPersist(t *testing.T) {
	assert := assert.New(t)
	mt := newMemTable(testMemTableSize)

	for _, c := range testChunks {
		assert.True(mt.addChunk(computeAddr(c), c))
	}

	dir := makeTempDir(assert)
	defer os.RemoveAll(dir)
	fts := fsTablePersister{dir: dir}

	src := fts.Persist(mt, nil)
	if assert.True(src.count() > 0) {
		buff, err := ioutil.ReadFile(filepath.Join(dir, src.hash().String()))
		assert.NoError(err)
		tr := newTableReader(parseTableIndex(buff), bytes.NewReader(buff), fileBlockSize)
		assertChunksInReader(testChunks, tr, assert)
	}
}

func TestFSTablePersisterPersistNoData(t *testing.T) {
	assert := assert.New(t)
	mt := newMemTable(testMemTableSize)
	existingTable := newMemTable(testMemTableSize)

	for _, c := range testChunks {
		assert.True(mt.addChunk(computeAddr(c), c))
		assert.True(existingTable.addChunk(computeAddr(c), c))
	}

	dir := makeTempDir(assert)
	defer os.RemoveAll(dir)
	fts := fsTablePersister{dir: dir}

	src := fts.Persist(mt, existingTable)
	assert.True(src.count() == 0)

	_, err := os.Stat(filepath.Join(dir, src.hash().String()))
	assert.True(os.IsNotExist(err), "%v", err)
}

func TestFSTablePersisterCompactAll(t *testing.T) {
	assert := assert.New(t)
	assert.True(len(testChunks) > 1, "Whoops, this test isn't meaningful")
	sources := make(chunkSources, len(testChunks))

	for i, c := range testChunks {
		randChunk := make([]byte, (i+1)*13)
		_, err := rand.Read(randChunk)
		assert.NoError(err)
		sources[i] = bytesToChunkSource(c, randChunk)
	}

	dir := makeTempDir(assert)
	defer os.RemoveAll(dir)
	fts := fsTablePersister{dir: dir}
	src := fts.CompactAll(sources)

	if assert.True(src.count() > 0) {
		buff, err := ioutil.ReadFile(filepath.Join(dir, src.hash().String()))
		assert.NoError(err)
		tr := newTableReader(parseTableIndex(buff), bytes.NewReader(buff), fileBlockSize)
		assertChunksInReader(testChunks, tr, assert)
	}
}

func TestFSTablePersisterCompactAllDups(t *testing.T) {
	assert := assert.New(t)
	dir := makeTempDir(assert)
	defer os.RemoveAll(dir)
	fts := fsTablePersister{dir: dir}

	reps := 3
	sources := make(chunkSources, reps)
	for i := 0; i < reps; i++ {
		mt := newMemTable(1 << 10)
		for _, c := range testChunks {
			mt.addChunk(computeAddr(c), c)
		}
		sources[i] = fts.Persist(mt, nil)
	}
	src := fts.CompactAll(sources)

	if assert.True(src.count() > 0) {
		buff, err := ioutil.ReadFile(filepath.Join(dir, src.hash().String()))
		assert.NoError(err)
		tr := newTableReader(parseTableIndex(buff), bytes.NewReader(buff), fileBlockSize)
		assertChunksInReader(testChunks, tr, assert)
		assert.EqualValues(reps*len(testChunks), tr.count())
	}
}
