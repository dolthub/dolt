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
// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"bytes"
	"context"
	"encoding/binary"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/blobstore"
	"github.com/dolthub/dolt/go/store/constants"
	"github.com/dolthub/dolt/go/store/hash"
)

type tableSpecsByAscendingCount []tableSpec

func (ts tableSpecsByAscendingCount) Len() int { return len(ts) }
func (ts tableSpecsByAscendingCount) Less(i, j int) bool {
	tsI, tsJ := ts[i], ts[j]
	if tsI.chunkCount == tsJ.chunkCount {
		return bytes.Compare(tsI.hash[:], tsJ.hash[:]) < 0
	}
	return tsI.chunkCount < tsJ.chunkCount
}
func (ts tableSpecsByAscendingCount) Swap(i, j int) { ts[i], ts[j] = ts[j], ts[i] }

func makeTestSrcs(t *testing.T, tableSizes []uint32, p tablePersister) (srcs chunkSources) {
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
		cs, _, err := p.Persist(context.Background(), mt, nil, nil, &Stats{})
		require.NoError(t, err)
		c, err := cs.clone()
		require.NoError(t, err)
		srcs = append(srcs, c)
		cs.close()
	}
	return
}

// Makes a tableSet with len(tableSizes) upstream tables containing tableSizes[N] unique chunks
func makeTestTableSpecs(t *testing.T, tableSizes []uint32, p tablePersister) (specs []tableSpec) {
	for _, src := range makeTestSrcs(t, tableSizes, p) {
		specs = append(specs, tableSpec{typeNoms, src.hash(), mustUint32(src.count())})
		err := src.close()
		require.NoError(t, err)
	}
	return
}

func TestConjoin(t *testing.T) {
	t.Run("fake table persister", func(t *testing.T) {
		testConjoin(t, func(*testing.T) tablePersister {
			return newFakeTablePersister(&UnlimitedQuotaProvider{})
		})
	})
	t.Run("in-memory blobstore persister", func(t *testing.T) {
		testConjoin(t, func(*testing.T) tablePersister {
			return &blobstorePersister{
				bs:        blobstore.NewInMemoryBlobstore(""),
				blockSize: 4096,
				q:         &UnlimitedQuotaProvider{},
			}
		})
	})
	t.Run("local fs blobstore persister", func(t *testing.T) {
		testConjoin(t, func(*testing.T) tablePersister {
			return &blobstorePersister{
				bs:        blobstore.NewLocalBlobstore(t.TempDir()),
				blockSize: 4096,
				q:         &UnlimitedQuotaProvider{},
			}
		})
	})
}

func testConjoin(t *testing.T, factory func(t *testing.T) tablePersister) {
	stats := &Stats{}
	setup := func(lock hash.Hash, root hash.Hash, sizes []uint32) (fm *fakeManifest, p tablePersister, upstream manifestContents) {
		p = factory(t)
		fm = &fakeManifest{}
		fm.set(constants.FormatLD1String, lock, root, makeTestTableSpecs(t, sizes, p), nil)
		var err error
		_, upstream, err = fm.ParseIfExists(context.Background(), nil, nil)
		require.NoError(t, err)
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
		open := func(specs []tableSpec) (sources chunkSources) {
			for _, sp := range specs {
				cs, err := p.Open(context.Background(), sp.hash, sp.chunkCount, stats)
				if err != nil {
					require.NoError(t, err)
				}
				sources = append(sources, cs)
			}
			return
		}

		expectSrcs, actualSrcs := open(expect), open(actual)
		defer func() {
			for _, s := range expectSrcs {
				s.close()
			}
			for _, s := range actualSrcs {
				s.close()
			}
		}()

		ctx := context.Background()
		for _, src := range expectSrcs {
			err := extractAllChunks(ctx, src, func(rec extractRecord) {
				var ok bool
				for _, act := range actualSrcs {
					var err error
					ok, _, err = act.has(rec.a, nil)
					require.NoError(t, err)
					var buf []byte
					if ok {
						buf, _, err = act.get(ctx, rec.a, nil, stats)
						require.NoError(t, err)
						assert.Equal(t, rec.data, buf)
						break
					}
				}
				assert.True(t, ok)
			})
			require.NoError(t, err)
		}
	}

	// Compact some tables, interloper slips in a new table
	makeExtra := func(p tablePersister) tableSpec {
		mt := newMemTable(testMemTableSize)
		data := []byte{0xde, 0xad}
		mt.addChunk(computeAddr(data), data)
		src, _, err := p.Persist(context.Background(), mt, nil, nil, &Stats{})
		require.NoError(t, err)
		defer src.close()
		return tableSpec{typeNoms, src.hash(), mustUint32(src.count())}
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

	startLock, startRoot := computeAddr([]byte("lock")), hash.Of([]byte("root"))
	t.Run("Success", func(t *testing.T) {
		// Compact some tables, no one interrupts
		for _, c := range tc {
			t.Run(c.name, func(t *testing.T) {
				fm, p, upstream := setup(startLock, startRoot, c.precompact)

				_, _, err := conjoin(context.Background(), inlineConjoiner{}, upstream, fm, p, stats)
				require.NoError(t, err)
				exists, newUpstream, err := fm.ParseIfExists(context.Background(), stats, nil)
				require.NoError(t, err)
				assert.True(t, exists)
				assert.Equal(t, c.postcompact, getSortedSizes(newUpstream.specs))
				assertContainAll(t, p, upstream.specs, newUpstream.specs)
			})
		}
	})

	t.Run("Retry", func(t *testing.T) {
		for _, c := range tc {
			t.Run(c.name, func(t *testing.T) {
				fm, p, upstream := setup(startLock, startRoot, c.precompact)

				newTable := makeExtra(p)
				u := updatePreemptManifest{fm, func() {
					specs := append([]tableSpec{}, upstream.specs...)
					fm.set(constants.FormatLD1String, computeAddr([]byte("lock2")), startRoot, append(specs, newTable), nil)
				}}
				_, _, err := conjoin(context.Background(), inlineConjoiner{}, upstream, u, p, stats)
				require.NoError(t, err)
				exists, newUpstream, err := fm.ParseIfExists(context.Background(), stats, nil)
				require.NoError(t, err)
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
					fm.set(constants.FormatLD1String, computeAddr([]byte("lock2")), startRoot, upstream.specs[1:], nil)
				}}
				_, _, err := conjoin(context.Background(), inlineConjoiner{}, upstream, u, p, stats)
				require.NoError(t, err)
				exists, newUpstream, err := fm.ParseIfExists(context.Background(), stats, nil)
				require.NoError(t, err)
				assert.True(t, exists)
				assert.Equal(t, c.precompact[1:], getSortedSizes(newUpstream.specs))
			})
		}
	})

	setupAppendix := func(lock hash.Hash, root hash.Hash, specSizes, appendixSizes []uint32) (fm *fakeManifest, p tablePersister, upstream manifestContents) {
		p = newFakeTablePersister(&UnlimitedQuotaProvider{})
		fm = &fakeManifest{}
		fm.set(constants.FormatLD1String, lock, root, makeTestTableSpecs(t, specSizes, p), makeTestTableSpecs(t, appendixSizes, p))

		var err error
		_, upstream, err = fm.ParseIfExists(context.Background(), nil, nil)
		require.NoError(t, err)

		return
	}

	tca := []struct {
		name        string
		appendix    []uint32
		precompact  []uint32
		postcompact []uint32
	}{
		{"uniform", []uint32{1}, []uint32{1, 1, 1, 1, 1}, []uint32{1, 4}},
		{"all but last", []uint32{2}, []uint32{2, 1, 1, 1, 1, 5}, []uint32{2, 4, 5}},
		{"all", []uint32{1, 2, 3}, []uint32{1, 2, 3, 5, 5, 5}, []uint32{1, 2, 3, 15}},
		{"first four", []uint32{8, 9, 10}, []uint32{8, 9, 10, 5, 6, 10, 11, 35, 64}, []uint32{8, 9, 10, 32, 35, 64}},
		{"log, first two", nil, []uint32{1, 2, 4, 8, 16, 32, 64}, []uint32{3, 4, 8, 16, 32, 64}},
		{"log, all", []uint32{9, 10, 11, 12}, []uint32{9, 10, 11, 12, 2, 3, 4, 8, 16, 32, 64}, []uint32{9, 10, 11, 12, 129}},
	}

	t.Run("SuccessAppendix", func(t *testing.T) {
		// Compact some tables, no one interrupts
		for _, c := range tca {
			t.Run(c.name, func(t *testing.T) {
				fm, p, upstream := setupAppendix(startLock, startRoot, c.precompact, c.appendix)

				_, _, err := conjoin(context.Background(), inlineConjoiner{}, upstream, fm, p, stats)
				require.NoError(t, err)
				exists, newUpstream, err := fm.ParseIfExists(context.Background(), stats, nil)
				require.NoError(t, err)
				assert.True(t, exists)
				assert.Equal(t, c.postcompact, getSortedSizes(newUpstream.specs))
				assert.Equal(t, c.appendix, getSortedSizes(newUpstream.appendix))
				assertContainAll(t, p, upstream.specs, newUpstream.specs)
				assertContainAll(t, p, upstream.appendix, newUpstream.appendix)
			})
		}
	})

	t.Run("RetryAppendixSpecsChange", func(t *testing.T) {
		for _, c := range tca {
			t.Run(c.name, func(t *testing.T) {
				fm, p, upstream := setupAppendix(startLock, startRoot, c.precompact, c.appendix)

				newTable := makeExtra(p)
				u := updatePreemptManifest{fm, func() {
					specs := append([]tableSpec{}, upstream.specs...)
					fm.set(constants.FormatLD1String, computeAddr([]byte("lock2")), startRoot, append(specs, newTable), upstream.appendix)
				}}

				_, _, err := conjoin(context.Background(), inlineConjoiner{}, upstream, u, p, stats)
				require.NoError(t, err)
				exists, newUpstream, err := fm.ParseIfExists(context.Background(), stats, nil)
				require.NoError(t, err)
				assert.True(t, exists)
				assert.Equal(t, append([]uint32{1}, c.postcompact...), getSortedSizes(newUpstream.specs))
				assert.Equal(t, c.appendix, getSortedSizes(newUpstream.appendix))
				assertContainAll(t, p, append(upstream.specs, newTable), newUpstream.specs)
				assertContainAll(t, p, upstream.appendix, newUpstream.appendix)
			})
		}
	})

	t.Run("RetryAppendixAppendixChange", func(t *testing.T) {
		for _, c := range tca {
			t.Run(c.name, func(t *testing.T) {
				fm, p, upstream := setupAppendix(startLock, startRoot, c.precompact, c.appendix)

				newTable := makeExtra(p)
				u := updatePreemptManifest{fm, func() {
					app := append([]tableSpec{}, upstream.appendix...)
					specs := append([]tableSpec{}, newTable)
					fm.set(constants.FormatLD1String, computeAddr([]byte("lock2")), startRoot, append(specs, upstream.specs...), append(app, newTable))
				}}

				_, _, err := conjoin(context.Background(), inlineConjoiner{}, upstream, u, p, stats)
				require.NoError(t, err)
				exists, newUpstream, err := fm.ParseIfExists(context.Background(), stats, nil)
				require.NoError(t, err)
				assert.True(t, exists)
				if newUpstream.appendix != nil {
					assert.Equal(t, append([]uint32{1}, c.appendix...), getSortedSizes(newUpstream.appendix))
					assertContainAll(t, p, append(upstream.appendix, newTable), newUpstream.appendix)
				} else {
					assert.Equal(t, upstream.appendix, newUpstream.appendix)
				}
			})
		}
	})

	t.Run("TablesDroppedUpstreamAppendixSpecChanges", func(t *testing.T) {
		// Interloper drops some compactees
		for _, c := range tca {
			t.Run(c.name, func(t *testing.T) {
				fm, p, upstream := setupAppendix(startLock, startRoot, c.precompact, c.appendix)

				u := updatePreemptManifest{fm, func() {
					fm.set(constants.FormatLD1String, computeAddr([]byte("lock2")), startRoot, upstream.specs[len(c.appendix)+1:], upstream.appendix[:])
				}}
				_, _, err := conjoin(context.Background(), inlineConjoiner{}, upstream, u, p, stats)
				require.NoError(t, err)
				exists, newUpstream, err := fm.ParseIfExists(context.Background(), stats, nil)
				require.NoError(t, err)
				assert.True(t, exists)
				assert.Equal(t, c.precompact[len(c.appendix)+1:], getSortedSizes(newUpstream.specs))
				assert.Equal(t, c.appendix, getSortedSizes(newUpstream.appendix))
			})
		}
	})

	t.Run("TablesDroppedUpstreamAppendixAppendixChanges", func(t *testing.T) {
		// Interloper drops some compactees
		for _, c := range tca {
			t.Run(c.name, func(t *testing.T) {
				fm, p, upstream := setupAppendix(startLock, startRoot, c.precompact, c.appendix)

				newTable := makeExtra(p)
				u := updatePreemptManifest{fm, func() {
					specs := append([]tableSpec{}, newTable)
					specs = append(specs, upstream.specs[len(c.appendix)+1:]...)
					fm.set(constants.FormatLD1String, computeAddr([]byte("lock2")), startRoot, specs, append([]tableSpec{}, newTable))
				}}

				_, _, err := conjoin(context.Background(), inlineConjoiner{}, upstream, u, p, stats)
				require.NoError(t, err)
				exists, newUpstream, err := fm.ParseIfExists(context.Background(), stats, nil)
				require.NoError(t, err)
				assert.True(t, exists)
				assert.Equal(t, append([]uint32{1}, c.precompact[len(c.appendix)+1:]...), getSortedSizes(newUpstream.specs))
				assert.Equal(t, []uint32{1}, getSortedSizes(newUpstream.appendix))
			})
		}
	})
}

type updatePreemptManifest struct {
	manifest
	preUpdate func()
}

func (u updatePreemptManifest) Update(ctx context.Context, lastLock hash.Hash, newContents manifestContents, stats *Stats, writeHook func() error) (manifestContents, error) {
	if u.preUpdate != nil {
		u.preUpdate()
	}
	return u.manifest.Update(ctx, lastLock, newContents, stats, writeHook)
}
