// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"io/ioutil"
	"os"
	"sort"
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
	ts.Close()
}

func TestTableSetToSpecsExcludesEmptyTable(t *testing.T) {
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

func TestTableSetFlattenExcludesEmptyTable(t *testing.T) {
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

	ts = ts.Flatten()
	assert.EqualValues(ts.Size(), 2)
	ts.Close()
}

func TestTableSetExtract(t *testing.T) {
	assert := assert.New(t)
	ts := newFakeTableSet()
	assert.Empty(ts.ToSpecs())

	// Put in one table
	mt := newMemTable(testMemTableSize)
	mt.addChunk(computeAddr(testChunks[0]), testChunks[0])
	ts = ts.Prepend(mt)

	// Put in a second
	mt = newMemTable(testMemTableSize)
	mt.addChunk(computeAddr(testChunks[1]), testChunks[1])
	mt.addChunk(computeAddr(testChunks[2]), testChunks[2])
	ts = ts.Prepend(mt)

	chunkChan := make(chan extractRecord)
	go func() { defer close(chunkChan); ts.extract(chunkChan) }()
	i := 0
	for rec := range chunkChan {
		a := computeAddr(testChunks[i])
		assert.NotNil(rec.data, "Nothing for", a)
		assert.Equal(testChunks[i], rec.data, "Item %d: %s != %s", i, string(testChunks[i]), string(rec.data))
		assert.Equal(a, rec.a)
		i++
	}

	ts.Close()
}

func TestTableSetCompact(t *testing.T) {
	assert := assert.New(t)
	ts := newFakeTableSet()
	defer ts.Close()
	assert.Empty(ts.ToSpecs())

	moreChunks := append(testChunks, []byte("booboo"))
	for _, c := range moreChunks {
		mt := newMemTable(testMemTableSize)
		mt.addChunk(computeAddr(c), c)
		ts = ts.Prepend(mt)
	}

	// Put in one larger table, to be sure that it's not selected for compaction
	extraChunks := [][]byte{[]byte("fubu"), []byte("fubar")}
	mt := newMemTable(testMemTableSize)
	mt.addChunk(computeAddr(extraChunks[0]), extraChunks[0])
	mt.addChunk(computeAddr(extraChunks[1]), extraChunks[1])
	ts = ts.Prepend(mt)
	ts = ts.Flatten()
	bigTable := ts.upstream[0]
	assert.Equal(5, ts.Size())

	var compacted chunkSources
	ts, compacted = ts.Compact() // Should compact len/2 smallest (7/2 == 3) tables into 1, leaving 4 tables
	assert.NoError(compacted.close())

	assert.Equal(4, ts.Size())
	assert.Contains(ts.upstream, bigTable)
	assertChunksInReader(moreChunks, ts, assert)
	assertChunksInReader(extraChunks, ts, assert)

	ts, compacted = ts.Compact() // Should compact len/2 smallest (4/2 == 2) tables into 1, leaving 3 tables
	assert.NoError(compacted.close())

	// After two waves of compaction on a set of tables of size 2, 1, 1, 1, 1 there should be 3 tables, each with 2 chunks in it.
	assert.Equal(3, ts.Size())
	for _, source := range ts.upstream {
		assert.EqualValues(2, source.count())
	}
	assertChunksInReader(moreChunks, ts, assert)
	assertChunksInReader(extraChunks, ts, assert)

	ts, compacted = ts.Compact() // Should compact max(2, len/2) smallest (2 > 3/2 == 2) tables into 1
	assert.NoError(compacted.close())
	// After one last compaction there should be 2 tables, one of size 4 and one of size 2.
	if assert.Equal(2, ts.Size()) {
		sources := make(chunkSources, 2)
		copy(sources, ts.upstream)
		sort.Sort(chunkSourcesByDescendingCount(sources))
		assert.EqualValues(4, sources[0].count())
		assert.EqualValues(2, sources[1].count())
		assertChunksInReader(moreChunks, ts, assert)
		assertChunksInReader(extraChunks, ts, assert)
	}
}

func makeTempDir(assert *assert.Assertions) string {
	dir, err := ioutil.TempDir("", "")
	assert.NoError(err)
	return dir
}

func TestTableSetRebase(t *testing.T) {
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
	fullTS := newFSTableSet(dir, nil)
	assert.Empty(fullTS.ToSpecs())
	fullTS = insert(fullTS, testChunks...)
	fullTS = fullTS.Flatten()

	ts := newFSTableSet(dir, nil)
	ts = insert(ts, testChunks[0])
	assert.Equal(1, ts.Size())
	ts = ts.Flatten()
	ts = insert(ts, []byte("novel"))

	ts, dropped := ts.Rebase(fullTS.ToSpecs())
	assert.Len(dropped, 1)
	assert.Equal(4, ts.Size())
	dropped.close()
	ts.Close()
	fullTS.Close()
}
