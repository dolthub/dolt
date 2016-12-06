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

var testChunks = [][]byte{[]byte("hello2"), []byte("goodbye2"), []byte("badbye2")}

func TestTableSetPrependEmpty(t *testing.T) {
	ts := newFakeTableSet().Prepend(newMemTable(testMemTableSize))
	assert.Empty(t, ts.ToSpecs())
}

func TestTableSetPrepend(t *testing.T) {
	assert := assert.New(t)
	ts := newFakeTableSet()
	assert.Empty(ts.ToSpecs())
	mt := newMemTable(testMemTableSize)
	mt.addChunk(computeAddr(testChunks[0]), testChunks[0])

	ts = ts.Prepend(mt)
	firstSpecs := ts.ToSpecs()
	assert.Len(firstSpecs, 1)

	mt = newMemTable(testMemTableSize)
	mt.addChunk(computeAddr(testChunks[1]), testChunks[1])
	mt.addChunk(computeAddr(testChunks[2]), testChunks[2])
	ts = ts.Prepend(mt)
	secondSpecs := ts.ToSpecs()
	assert.Len(secondSpecs, 2)
	assert.Equal(firstSpecs, secondSpecs[1:])
}

func makeTempDir(assert *assert.Assertions) string {
	dir, err := ioutil.TempDir("", "")
	assert.NoError(err)
	return dir
}

func TestTableSetUnion(t *testing.T) {
	assert := assert.New(t)
	dir := makeTempDir(assert)
	defer os.RemoveAll(dir)

	insert := func(ts tableSet, chunks ...[]byte) tableSet {
		for _, c := range chunks {
			mt := newMemTable(testMemTableSize)
			mt.addChunk(computeAddr(c), c)
			ts = ts.Prepend(mt)
		}
		return ts
	}
	fullTS := newFSTableSet(dir)
	assert.Empty(fullTS.ToSpecs())
	fullTS = insert(fullTS, testChunks...)

	ts := newFSTableSet(dir)
	ts = insert(ts, testChunks[0])
	assert.Len(ts.ToSpecs(), 1)

	ts = ts.Union(fullTS.ToSpecs())
	assert.Len(ts.ToSpecs(), 3)
}

func TestS3TablePersisterCompact(t *testing.T) {
	assert := assert.New(t)
	mt := newMemTable(testMemTableSize)

	for _, c := range testChunks {
		assert.True(mt.addChunk(computeAddr(c), c))
	}

	s3svc := makeFakeS3(assert)
	s3p := s3TablePersister{s3svc, "bucket"}

	tableAddr, chunkCount := s3p.Compact(mt, nil)
	if assert.True(chunkCount > 0) {
		buff, present := s3svc.data[tableAddr.String()]
		assert.True(present)
		tr := newTableReader(buff, bytes.NewReader(buff))
		for _, c := range testChunks {
			assert.True(tr.has(computeAddr(c)))
		}
	}
}

func TestS3TablePersisterCompactNoData(t *testing.T) {
	assert := assert.New(t)
	mt := newMemTable(testMemTableSize)
	existingTable := newMemTable(testMemTableSize)

	for _, c := range testChunks {
		assert.True(mt.addChunk(computeAddr(c), c))
		assert.True(existingTable.addChunk(computeAddr(c), c))
	}

	s3svc := makeFakeS3(assert)
	s3p := s3TablePersister{s3svc, "bucket"}

	tableAddr, chunkCount := s3p.Compact(mt, existingTable)
	assert.True(chunkCount == 0)

	_, present := s3svc.data[tableAddr.String()]
	assert.False(present)
}

func TestFSTablePersisterCompact(t *testing.T) {
	assert := assert.New(t)
	mt := newMemTable(testMemTableSize)

	for _, c := range testChunks {
		assert.True(mt.addChunk(computeAddr(c), c))
	}

	dir := makeTempDir(assert)
	defer os.RemoveAll(dir)
	fts := fsTablePersister{dir}

	tableAddr, chunkCount := fts.Compact(mt, nil)
	if assert.True(chunkCount > 0) {
		buff, err := ioutil.ReadFile(filepath.Join(dir, tableAddr.String()))
		assert.NoError(err)
		tr := newTableReader(buff, bytes.NewReader(buff))
		for _, c := range testChunks {
			assert.True(tr.has(computeAddr(c)))
		}
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
	fts := fsTablePersister{dir}

	tableAddr, chunkCount := fts.Compact(mt, existingTable)
	assert.True(chunkCount == 0)

	_, err := os.Stat(filepath.Join(dir, tableAddr.String()))
	assert.True(os.IsNotExist(err), "%v", err)
}
