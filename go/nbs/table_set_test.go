// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"io/ioutil"
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
}

func TestTableSetCompact(t *testing.T) {
	// Returns the chunk counts of the tables in ts.compacted & ts.upstream in ascending order
	getSortedSizes := func(ts tableSet) (sorted []uint32) {
		all := append(chunkSources{}, ts.compacted...)
		all = append(all, ts.upstream...)
		sort.Sort(chunkSourcesByAscendingCount(all))
		sorted = make([]uint32, len(all))
		for i := 0; i < len(sorted); i++ {
			sorted[i] = all[i].count()
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
			ts2 := ts.Compact(&Stats{})
			assert.Equal(c.postcompact, getSortedSizes(ts2))
			assertContainAll(t, ts, ts2)
		})
	}
}

// Makes a tableSet with len(tableSizes) upstream tables containing tableSizes[N] unique chunks
func makeTestTableSet(tableSizes []uint32) tableSet {
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
	return ts.Flatten()
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
	insert := func(ts tableSet, chunks ...[]byte) tableSet {
		for _, c := range chunks {
			mt := newMemTable(testMemTableSize)
			mt.addChunk(computeAddr(c), c)
			ts = ts.Prepend(mt, &Stats{})
		}
		return ts
	}
	upstream := makeTestTableSet([]uint32{1, 1, 3, 7})

	t.Run("NoCompactions", func(t *testing.T) {
		assert := assert.New(t)

		// Inject an upstream table
		ts := newFakeTableSet()
		ts = insert(ts, testChunks[0])
		assert.Equal(1, ts.Size())
		ts = ts.Flatten()
		// Add a novel table
		ts = insert(ts, []byte("novel"))

		ts = ts.Rebase(upstream.ToSpecs())
		assert.Equal(upstream.Size()+1, ts.Size())
	})

	t.Run("WithCompactions", func(t *testing.T) {
		validate := func(rebased, prebased tableSet, crashers chunkSources, t *testing.T) {
			assert := assert.New(t)
			specs := rebased.ToSpecs()
			for _, novel := range prebased.novel {
				assert.Contains(specs, tableSpec{novel.hash(), novel.count()})
			}
			for _, compacted := range prebased.compacted {
				assert.Contains(specs, tableSpec{compacted.hash(), compacted.count()})
			}
			for _, upstream := range prebased.upstream {
				assert.Contains(specs, tableSpec{upstream.hash(), upstream.count()})
			}
			for _, compactee := range prebased.compactees {
				assert.NotContains(specs, tableSpec{compactee.hash(), compactee.count()})
			}
			for _, crasher := range crashers {
				assert.Contains(specs, tableSpec{crasher.hash(), crasher.count()})
			}
		}
		t.Run("KeepSingle", func(t *testing.T) {
			// Start from upstream, do a compaction and add a novel table
			local := upstream.Flatten()
			local = local.Compact(&Stats{})
			assert.True(t, local.Size() < upstream.Size())
			local = insert(local, []byte("novel"))

			// Mimic some other committer landing additional novel tables upstream
			interloper := insert(upstream, []byte("party crasher"))
			crashers := interloper.novel

			rebased := local.Rebase(interloper.ToSpecs())

			// Since interloper didn't drop any of local's compactees, Rebase should retain the compacted table created above.
			validate(rebased, local, crashers, t)
		})

		t.Run("KeepMultiple", func(t *testing.T) {
			// Start from upstream, do a couple of compactions
			stats := &Stats{}
			local := upstream.Flatten()
			local = local.Compact(stats)

			assert.True(t, local.Size() >= 2)
			local = local.Compact(stats)
			local = insert(local, []byte("novel"))

			// Mimic some other committer landing additional novel tables upstream
			interloper := insert(upstream, []byte("party crasher"))
			crashers := interloper.novel

			rebased := local.Rebase(interloper.ToSpecs())

			// Since interloper didn't drop any of local's compactees, Rebase should retain the compacted tables created above.
			validate(rebased, local, crashers, t)
		})

		t.Run("KeepAcrossMultipleRebases", func(t *testing.T) {
			// Start from upstream, do a compaction and add a novel table
			local := upstream.Flatten()
			local = local.Compact(&Stats{})
			assert.True(t, local.Size() < upstream.Size())
			local = insert(local, []byte("novel"))

			// Mimic some other committer landing additional novel tables upstream
			interloper := insert(upstream, []byte("party crasher"))
			crashers := interloper.novel

			rebased := local.Rebase(interloper.ToSpecs())

			// Since interloper didn't drop any of local's compactees, Rebase should retain the compacted table created above.
			validate(rebased, local, crashers, t)

			interloper = insert(interloper, []byte("party crasher 2: electric boogaloo"))
			crashers = append(crashers, interloper.novel...)

			rebased = local.Rebase(interloper.ToSpecs())

			// Since interloper STILL didn't drop any of local's compactees, Rebase should retain the compacted table created way back before the first rebase.
			validate(rebased, local, crashers, t)
		})

		t.Run("Drop", func(t *testing.T) {
			assert := assert.New(t)
			// Start from upstream, do a compaction and add a novel table
			local := upstream.Flatten()
			local = local.Compact(&Stats{})
			assert.True(local.Size() < upstream.Size())
			local = insert(local, []byte("novel"))

			// Mimic some other committer dropping tables upstream (due to e.g. compaction or garbage collection)
			interloper := insert(upstream, []byte("party crasher")).Flatten()
			for i, up := range interloper.upstream {
				if up.hash() == local.compactees[0].hash() {
					if i == len(interloper.upstream)-1 {
						interloper.upstream = interloper.upstream[:i]
					} else {
						interloper.upstream = append(interloper.upstream[:i], interloper.upstream[i+1:]...)
					}
					break
				}
			}

			rebased := local.Rebase(interloper.ToSpecs())

			// rebased should retain all novel tables...
			specs := rebased.ToSpecs()
			for _, novel := range local.novel {
				assert.Contains(specs, tableSpec{novel.hash(), novel.count()})
			}
			// ...but drop the compacted tables, and take upstream from interloper.
			assert.Empty(rebased.compacted)
			for _, upstream := range interloper.upstream {
				assert.Contains(specs, tableSpec{upstream.hash(), upstream.count()})
			}
		})
	})
}
