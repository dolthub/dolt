// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"bytes"
	"encoding/binary"
	"sort"
	"sync"
	"testing"

	"github.com/attic-labs/noms/go/constants"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/testify/assert"
)

func TestAsyncConjoinerAwait(t *testing.T) {
	runTest := func(t *testing.T, names ...string) {
		c := newAsyncConjoiner(defaultMaxTables)

		// mu protects |conjoins|
		mu := sync.Mutex{}
		conjoins := map[string]int{}

		// |trigger| ensures the goroutines will all await() concurrently
		trigger := &sync.WaitGroup{}
		trigger.Add(len(names))

		// |wg| allows the test to wait for all the goroutines started below
		wg := sync.WaitGroup{}
		for _, n := range names {
			wg.Add(1)
			go func(db string) {
				defer wg.Done()
				c.await(db, func() {
					trigger.Wait()
					mu.Lock()
					defer mu.Unlock()
					cnt := conjoins[db]
					cnt++
					conjoins[db] = cnt
				}, trigger)
			}(n)
		}
		wg.Wait()

		for _, n := range names {
			assert.EqualValues(t, 1, conjoins[n], "Wrong num conjoins for %s", n)
		}
	}

	t.Run("AllDifferent", func(t *testing.T) {
		runTest(t, "foo", "bar", "baz")
	})

	t.Run("Concurrent", func(t *testing.T) {
		runTest(t, "foo", "foo", "bar", "foo", "baz")
	})
}

type tableSpecsByAscendingCount []tableSpec

func (ts tableSpecsByAscendingCount) Len() int { return len(ts) }
func (ts tableSpecsByAscendingCount) Less(i, j int) bool {
	tsI, tsJ := ts[i], ts[j]
	if tsI.chunkCount == tsJ.chunkCount {
		return bytes.Compare(tsI.name[:], tsJ.name[:]) < 0
	}
	return tsI.chunkCount < tsJ.chunkCount
}
func (ts tableSpecsByAscendingCount) Swap(i, j int) { ts[i], ts[j] = ts[j], ts[i] }

func makeTestSrcs(tableSizes []uint32, p tablePersister) (srcs chunkSources) {
	count := uint32(0)
	nextChunk := func() (chunk []byte) {
		chunk = make([]byte, 4)
		binary.BigEndian.PutUint32(chunk, count)
		count++
		return chunk
	}

	for _, s := range tableSizes {
		mt := newMemTable(testMemTableSize)
		for i := uint32(0); i < s; i++ {
			c := nextChunk()
			mt.addChunk(computeAddr(c), c)
		}
		srcs = append(srcs, p.Persist(mt, nil, &Stats{}))
	}
	return
}

func TestConjoin(t *testing.T) {
	// Makes a tableSet with len(tableSizes) upstream tables containing tableSizes[N] unique chunks
	makeTestTableSpecs := func(tableSizes []uint32, p tablePersister) (specs []tableSpec) {
		for _, src := range makeTestSrcs(tableSizes, p) {
			specs = append(specs, tableSpec{src.hash(), src.count()})
		}
		return
	}

	// Returns the chunk counts of the tables in ts.compacted & ts.upstream in ascending order
	getSortedSizes := func(specs []tableSpec) (sorted []uint32) {
		all := append([]tableSpec{}, specs...)
		sort.Sort(tableSpecsByAscendingCount(all))
		for _, ts := range all {
			sorted = append(sorted, ts.chunkCount)
		}
		return
	}

	assertContainAll := func(t *testing.T, p tablePersister, expect, actual []tableSpec) {
		open := func(specs []tableSpec) (srcs chunkReaderGroup) {
			for _, sp := range specs {
				srcs = append(srcs, p.Open(sp.name, sp.chunkCount))
			}
			return
		}
		expectSrcs, actualSrcs := open(expect), open(actual)
		chunkChan := make(chan extractRecord, expectSrcs.count())
		expectSrcs.extract(chunkChan)
		close(chunkChan)

		for rec := range chunkChan {
			assert.True(t, actualSrcs.has(rec.a))
		}
	}

	setup := func(lock addr, root hash.Hash, sizes []uint32) (fm *fakeManifest, p tablePersister, upstream []tableSpec) {
		p = newFakeTablePersister()
		upstream = makeTestTableSpecs(sizes, p)
		fm = &fakeManifest{}
		fm.set(constants.NomsVersion, lock, root, upstream)
		return
	}

	tc := []struct {
		name        string
		precompact  []uint32
		postcompact []uint32
	}{
		{"uniform", []uint32{1, 1, 1, 1, 1}, []uint32{5}},
		{"all but last", []uint32{1, 1, 1, 1, 5}, []uint32{4, 5}},
		{"all", []uint32{5, 5, 5}, []uint32{15}},
		{"first four", []uint32{5, 6, 10, 11, 35, 64}, []uint32{32, 35, 64}},
		{"log, first two", []uint32{1, 2, 4, 8, 16, 32, 64}, []uint32{3, 4, 8, 16, 32, 64}},
		{"log, all", []uint32{2, 3, 4, 8, 16, 32, 64}, []uint32{129}},
	}

	stats := &Stats{}
	alwaysConjoin := func(int) bool { return true }
	startLock, startRoot := computeAddr([]byte("lock")), hash.Of([]byte("root"))
	t.Run("Success", func(t *testing.T) {
		// Compact some tables, no one interrupts
		for _, c := range tc {
			t.Run(c.name, func(t *testing.T) {
				fm, p, upstream := setup(startLock, startRoot, c.precompact)

				conjoin(fm, p, alwaysConjoin, stats)
				exists, _, _, _, newUpstream := fm.ParseIfExists(nil)
				assert.True(t, exists)
				assert.Equal(t, c.postcompact, getSortedSizes(newUpstream))
				assertContainAll(t, p, upstream, newUpstream)
			})
		}
	})

	t.Run("Retry", func(t *testing.T) {
		// Compact some tables, interloper slips in a new table
		makeExtra := func(p tablePersister) tableSpec {
			mt := newMemTable(testMemTableSize)
			data := []byte{0xde, 0xad}
			mt.addChunk(computeAddr(data), data)
			src := p.Persist(mt, nil, &Stats{})
			return tableSpec{src.hash(), src.count()}
		}
		for _, c := range tc {
			t.Run(c.name, func(t *testing.T) {
				fm, p, upstream := setup(startLock, startRoot, c.precompact)

				newTable := makeExtra(p)
				u := updatePreemptManifest{fm, func() {
					fm.set(constants.NomsVersion, computeAddr([]byte("lock2")), startRoot, append(upstream, newTable))
				}}
				conjoin(u, p, alwaysConjoin, stats)
				exists, _, _, _, newUpstream := fm.ParseIfExists(nil)
				assert.True(t, exists)
				assert.Equal(t, append([]uint32{1}, c.postcompact...), getSortedSizes(newUpstream))
				assertContainAll(t, p, append(upstream, newTable), newUpstream)
			})
		}
	})

	t.Run("TablesDroppedUpstream", func(t *testing.T) {
		// Interloper drops some compactees
		for _, c := range tc {
			t.Run(c.name, func(t *testing.T) {
				fm, p, upstream := setup(startLock, startRoot, c.precompact)

				u := updatePreemptManifest{fm, func() {
					fm.set(constants.NomsVersion, computeAddr([]byte("lock2")), startRoot, upstream[1:])
				}}
				conjoin(u, p, alwaysConjoin, stats)
				exists, _, _, _, newUpstream := fm.ParseIfExists(nil)
				assert.True(t, exists)
				assert.Equal(t, c.precompact[1:], getSortedSizes(newUpstream))
			})
		}
	})

	neverConjoin := func(int) bool { return false }
	t.Run("ExitEarly", func(t *testing.T) {
		// conjoin called with out-of-date manifest; no longer needs a conjoin
		for _, c := range tc {
			t.Run(c.name, func(t *testing.T) {
				fm, p, upstream := setup(startLock, startRoot, c.precompact)

				conjoin(fm, p, neverConjoin, stats)
				exists, _, _, _, newUpstream := fm.ParseIfExists(nil)
				assert.True(t, exists)
				assert.Equal(t, c.precompact, getSortedSizes(newUpstream))
				assertContainAll(t, p, upstream, newUpstream)
			})
		}
	})
}

type updatePreemptManifest struct {
	manifest
	preUpdate func()
}

func (u updatePreemptManifest) Update(lastLock, newLock addr, specs []tableSpec, newRoot hash.Hash, writeHook func()) (lock addr, actual hash.Hash, tableSpecs []tableSpec) {
	if u.preUpdate != nil {
		u.preUpdate()
	}
	return u.manifest.Update(lastLock, newLock, specs, newRoot, writeHook)
}
