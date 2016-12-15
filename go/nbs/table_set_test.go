// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/attic-labs/testify/assert"
	"github.com/aws/aws-sdk-go/service/s3"
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
	ts.Close()
}

func TestTableToSpecsExcludesEmptyTable(t *testing.T) {
	assert := assert.New(t)
	ts := newFakeTableSet()
	assert.Empty(ts.ToSpecs())
	mt := newMemTable(testMemTableSize)
	mt.addChunk(computeAddr(testChunks[0]), testChunks[0])
	ts = ts.Prepend(mt)

	mt = newMemTable(testMemTableSize)
	ts = ts.Prepend(mt)

	mt = newMemTable(testMemTableSize)
	mt.addChunk(computeAddr(testChunks[1]), testChunks[1])
	mt.addChunk(computeAddr(testChunks[2]), testChunks[2])
	ts = ts.Prepend(mt)

	specs := ts.ToSpecs()
	assert.Len(specs, 2)
	ts.Close()
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
	ts.Close()
	fullTS.Close()
}

func TestS3TablePersisterCompact(t *testing.T) {
	assert := assert.New(t)
	mt := newMemTable(testMemTableSize)

	for _, c := range testChunks {
		assert.True(mt.addChunk(computeAddr(c), c))
	}

	s3svc := makeFakeS3(assert)
	cache := newS3IndexCache(1024)
	s3p := s3TablePersister{s3: s3svc, bucket: "bucket", partSize: calcPartSize(mt, 3), indexCache: cache}

	src := s3p.Compact(mt, nil)
	assert.NotNil(cache.get(src.hash()))

	if assert.True(src.count() > 0) {
		if r := s3svc.readerForTable(src.hash()); assert.NotNil(r) {
			assertChunksInReader(testChunks, r, assert)
		}
	}
}

func calcPartSize(mt *memTable, maxPartNum int) int {
	return int(maxTableSize(uint64(mt.count()), mt.totalData)) / maxPartNum
}

func TestS3TablePersisterCompactSinglePart(t *testing.T) {
	assert := assert.New(t)
	mt := newMemTable(testMemTableSize)

	for _, c := range testChunks {
		assert.True(mt.addChunk(computeAddr(c), c))
	}

	s3svc := makeFakeS3(assert)
	s3p := s3TablePersister{s3: s3svc, bucket: "bucket", partSize: calcPartSize(mt, 1)}

	src := s3p.Compact(mt, nil)
	if assert.True(src.count() > 0) {
		if r := s3svc.readerForTable(src.hash()); assert.NotNil(r) {
			assertChunksInReader(testChunks, r, assert)
		}
	}
}

func TestS3TablePersisterCompactAbort(t *testing.T) {
	assert := assert.New(t)
	mt := newMemTable(testMemTableSize)

	for _, c := range testChunks {
		assert.True(mt.addChunk(computeAddr(c), c))
	}

	numParts := 4
	s3svc := &failingFakeS3{makeFakeS3(assert), sync.Mutex{}, 1}
	s3p := s3TablePersister{s3: s3svc, bucket: "bucket", partSize: calcPartSize(mt, numParts)}

	assert.Panics(func() { s3p.Compact(mt, nil) })
}

type failingFakeS3 struct {
	*fakeS3
	mu           sync.Mutex
	numSuccesses int
}

func (m *failingFakeS3) UploadPart(input *s3.UploadPartInput) (*s3.UploadPartOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.numSuccesses > 0 {
		m.numSuccesses--
		return m.fakeS3.UploadPart(input)
	}
	return nil, mockAWSError("MalformedXML")
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
	s3p := s3TablePersister{s3: s3svc, bucket: "bucket", partSize: 1 << 10}

	src := s3p.Compact(mt, existingTable)
	assert.True(src.count() == 0)

	_, present := s3svc.data[src.hash().String()]
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

	src := fts.Compact(mt, nil)
	if assert.True(src.count() > 0) {
		buff, err := ioutil.ReadFile(filepath.Join(dir, src.hash().String()))
		assert.NoError(err)
		tr := newTableReader(parseTableIndex(buff), bytes.NewReader(buff), fileReadAmpThresh)
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

	src := fts.Compact(mt, existingTable)
	assert.True(src.count() == 0)

	_, err := os.Stat(filepath.Join(dir, src.hash().String()))
	assert.True(os.IsNotExist(err), "%v", err)
}
