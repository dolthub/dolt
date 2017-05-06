// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"io/ioutil"
	"os"
	"sort"
	"testing"

	"encoding/binary"

	"github.com/attic-labs/testify/assert"
)

var testChunks = [][]byte{[]byte("hello2"), []byte("goodbye2"), []byte("badbye2")}

func TestTableSetPrependEmpty(t *testing.T) {
	ts := newFakeTableSet().Prepend(newMemTable(testMemTableSize), &Stats{})
	assert.Empty(t, ts.ToSpecs())
}

func TestTableSetPrepend(t *testing.T) {
	assert := assert.New(t)
	ts := newFakeTableSet()
	assert.Empty(ts.ToSpecs())
	mt := newMemTable(testMemTableSize)
	mt.addChunk(computeAddr(testChunks[0]), testChunks[0])
	ts = ts.Prepend(mt, &Stats{})

	firstSpecs := ts.ToSpecs()
	assert.Len(firstSpecs, 1)

	mt = newMemTable(testMemTableSize)
	mt.addChunk(computeAddr(testChunks[1]), testChunks[1])
	mt.addChunk(computeAddr(testChunks[2]), testChunks[2])
	ts = ts.Prepend(mt, &Stats{})

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
	ts = ts.Prepend(mt, &Stats{})

	mt = newMemTable(testMemTableSize)
	ts = ts.Prepend(mt, &Stats{})

	mt = newMemTable(testMemTableSize)
	mt.addChunk(computeAddr(testChunks[1]), testChunks[1])
	mt.addChunk(computeAddr(testChunks[2]), testChunks[2])
	ts = ts.Prepend(mt, &Stats{})

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
	ts = ts.Prepend(mt, &Stats{})

	mt = newMemTable(testMemTableSize)
	ts = ts.Prepend(mt, &Stats{})

	mt = newMemTable(testMemTableSize)
	mt.addChunk(computeAddr(testChunks[1]), testChunks[1])
	mt.addChunk(computeAddr(testChunks[2]), testChunks[2])
	ts = ts.Prepend(mt, &Stats{})

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
	ts = ts.Prepend(mt, &Stats{})

	// Put in a second
	mt = newMemTable(testMemTableSize)
	mt.addChunk(computeAddr(testChunks[1]), testChunks[1])
	mt.addChunk(computeAddr(testChunks[2]), testChunks[2])
	ts = ts.Prepend(mt, &Stats{})

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
	// Makes a tableSet with len(tableSizes) upstream tables containing tableSizes[N] unique chunks
	makeTestTableSet := func(tableSizes []uint32) tableSet {
		count := uint32(0)
		nextChunk := func() (chunk []byte) {
			chunk = make([]byte, 4)
			binary.BigEndian.PutUint32(chunk, count)
			count++
			return chunk
		}

		ts := newFakeTableSet()
		for _, s := range tableSizes {
			mt := newMemTable(testMemTableSize)
			for i := uint32(0); i < s; i++ {
				c := nextChunk()
				mt.addChunk(computeAddr(c), c)
			}
			ts = ts.Prepend(mt, &Stats{})
		}
		return ts
	}

	// Returns the set of and chunk count upstream tables
	getSortedSizes := func(ts tableSet) (sorted []uint32) {
		sort.Sort(chunkSourcesByAscendingCount(ts.upstream))
		sorted = make([]uint32, len(ts.upstream))
		for i := 0; i < len(sorted); i++ {
			sorted[i] = ts.upstream[i].count()
		}
		return
	}

	tc := []struct {
		name        string
		precompact  []uint32
		postcompact []uint32
	}{
		{"uniform", []uint32{1, 1, 1, 1, 1}, []uint32{5}},
		{"all but last", []uint32{1, 1, 1, 1, 5}, []uint32{4, 5}},
		{"all", []uint32{5, 5, 10}, []uint32{10, 10}},
		{"first four", []uint32{5, 6, 10, 11, 35, 64}, []uint32{32, 35, 64}},
		{"log, first two", []uint32{1, 2, 4, 8, 16, 32, 64}, []uint32{3, 4, 8, 16, 32, 64}},
		{"log, all", []uint32{2, 3, 4, 8, 16, 32, 64}, []uint32{129}},
	}

	for _, c := range tc {
		t.Run(c.name, func(t *testing.T) {
			assert := assert.New(t)
			ts := makeTestTableSet(c.precompact)
			ts2, _ := ts.Flatten().Compact(&Stats{})
			assert.Equal(c.postcompact, getSortedSizes(ts2))
			assertContainAll(t, ts, ts2)
			ts.Close()
			ts2.Close()
		})
	}
}

func assertContainAll(t *testing.T, expect, actual tableSet) {
	chunkChan := make(chan extractRecord, expect.count())
	expect.extract(chunkChan)
	close(chunkChan)

	for rec := range chunkChan {
		assert.True(t, actual.has(rec.a))
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
			ts = ts.Prepend(mt, &Stats{})
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
