// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"bytes"
	"context"
	"encoding/binary"
	"github.com/liquidata-inc/ld/dolt/go/store/d"
	"sort"
	"testing"

	"github.com/liquidata-inc/ld/dolt/go/store/constants"
	"github.com/liquidata-inc/ld/dolt/go/store/hash"
	"github.com/stretchr/testify/assert"
)

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
		cs, err := p.Persist(context.Background(), mt, nil, &Stats{})
		d.PanicIfError(err)
		srcs = append(srcs, cs)
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
				cs, err := p.Open(context.Background(), sp.name, sp.chunkCount, nil)

				if err != nil {
					assert.NoError(t, err)
				}

				srcs = append(srcs, cs)
			}
			return
		}
		expectSrcs, actualSrcs := open(expect), open(actual)
		chunkChan := make(chan extractRecord, expectSrcs.count())
		expectSrcs.extract(context.Background(), chunkChan)
		close(chunkChan)

		for rec := range chunkChan {
			has, err := actualSrcs.has(rec.a)
			assert.NoError(t, err)
			assert.True(t, has)
		}
	}

	setup := func(lock addr, root hash.Hash, sizes []uint32) (fm *fakeManifest, p tablePersister, upstream manifestContents) {
		p = newFakeTablePersister()
		fm = &fakeManifest{}
		fm.set(constants.NomsVersion, lock, root, makeTestTableSpecs(sizes, p))

		var err error
		_, upstream, err = fm.ParseIfExists(context.Background(), nil, nil)
		assert.NoError(t, err)

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
	startLock, startRoot := computeAddr([]byte("lock")), hash.Of([]byte("root"))
	t.Run("Success", func(t *testing.T) {
		// Compact some tables, no one interrupts
		for _, c := range tc {
			t.Run(c.name, func(t *testing.T) {
				fm, p, upstream := setup(startLock, startRoot, c.precompact)

				conjoin(context.Background(), upstream, fm, p, stats)
				exists, newUpstream, err := fm.ParseIfExists(context.Background(), stats, nil)
				assert.NoError(t, err)
				assert.True(t, exists)
				assert.Equal(t, c.postcompact, getSortedSizes(newUpstream.specs))
				assertContainAll(t, p, upstream.specs, newUpstream.specs)
			})
		}
	})

	t.Run("Retry", func(t *testing.T) {
		// Compact some tables, interloper slips in a new table
		makeExtra := func(p tablePersister) tableSpec {
			mt := newMemTable(testMemTableSize)
			data := []byte{0xde, 0xad}
			mt.addChunk(computeAddr(data), data)
			src, err := p.Persist(context.Background(), mt, nil, &Stats{})
			assert.NoError(t, err)
			return tableSpec{src.hash(), src.count()}
		}
		for _, c := range tc {
			t.Run(c.name, func(t *testing.T) {
				fm, p, upstream := setup(startLock, startRoot, c.precompact)

				newTable := makeExtra(p)
				u := updatePreemptManifest{fm, func() {
					specs := append([]tableSpec{}, upstream.specs...)
					fm.set(constants.NomsVersion, computeAddr([]byte("lock2")), startRoot, append(specs, newTable))
				}}
				conjoin(context.Background(), upstream, u, p, stats)
				exists, newUpstream, err := fm.ParseIfExists(context.Background(), stats, nil)
				assert.NoError(t, err)
				assert.True(t, exists)
				assert.Equal(t, append([]uint32{1}, c.postcompact...), getSortedSizes(newUpstream.specs))
				assertContainAll(t, p, append(upstream.specs, newTable), newUpstream.specs)
			})
		}
	})

	t.Run("TablesDroppedUpstream", func(t *testing.T) {
		// Interloper drops some compactees
		for _, c := range tc {
			t.Run(c.name, func(t *testing.T) {
				fm, p, upstream := setup(startLock, startRoot, c.precompact)

				u := updatePreemptManifest{fm, func() {
					fm.set(constants.NomsVersion, computeAddr([]byte("lock2")), startRoot, upstream.specs[1:])
				}}
				conjoin(context.Background(), upstream, u, p, stats)
				exists, newUpstream, err := fm.ParseIfExists(context.Background(), stats, nil)
				assert.NoError(t, err)
				assert.True(t, exists)
				assert.Equal(t, c.precompact[1:], getSortedSizes(newUpstream.specs))
			})
		}
	})
}

type updatePreemptManifest struct {
	manifest
	preUpdate func()
}

func (u updatePreemptManifest) Update(ctx context.Context, lastLock addr, newContents manifestContents, stats *Stats, writeHook func() error) (manifestContents, error) {
	if u.preUpdate != nil {
		u.preUpdate()
	}
	return u.manifest.Update(ctx, lastLock, newContents, stats, writeHook)
}
