// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/attic-labs/testify/assert"
)

func TestFSTablePersisterCompact(t *testing.T) {
	assert := assert.New(t)
	mt := newMemTable(testMemTableSize)

	for _, c := range testChunks {
		assert.True(mt.addChunk(computeAddr(c), c))
	}

	dir := makeTempDir(assert)
	defer os.RemoveAll(dir)
	fts := fsTablePersister{dir: dir}

	src := fts.Compact(mt, nil)
	if assert.True(src.count() > 0) {
		buff, err := ioutil.ReadFile(filepath.Join(dir, src.hash().String()))
		assert.NoError(err)
		tr := newTableReader(parseTableIndex(buff), bytes.NewReader(buff), fileBlockSize)
		assertChunksInReader(testChunks, tr, assert)
	}
}

func TestFSTablePersisterCompactNoData(t *testing.T) {
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

	src := fts.Compact(mt, existingTable)
	assert.True(src.count() == 0)

	_, err := os.Stat(filepath.Join(dir, src.hash().String()))
	assert.True(os.IsNotExist(err), "%v", err)
}

func TestFSTablePersisterCompactAll(t *testing.T) {
	assert := assert.New(t)
	assert.True(len(testChunks) > 1, "Whoops, this test isn't meaningful")
	sources := make(chunkSources, len(testChunks))

	for i, c := range testChunks {
		sources[i] = bytesToChunkSource(c)
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
	assert.True(len(testChunks) > 1, "Whoops, this test isn't meaningful")
	sources := make(chunkSources, len(testChunks))

	for i := range testChunks {
		sources[i] = bytesToChunkSource(testChunks...)
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
		assert.EqualValues(len(testChunks), tr.count())
	}
}
