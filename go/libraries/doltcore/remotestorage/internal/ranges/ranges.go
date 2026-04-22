// Copyright 2024 Dolthub, Inc.
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

package ranges

import (
	"container/heap"
	"fmt"
	"math/rand/v2"
	"os"

	"github.com/google/btree"
)

// GetRange represents a range of remote data that has semantic meaning to the
// ChunkFetcher. These ranges are currently either Chunks, or Dictionaries.
// They can be fetched from the remote URL with an HTTP Range request.
// For a chunk range, the chunk with hash |Hash| can be fetched using
// the |Url| with a Range request starting at |Offset| and reading |Length|
// bytes. A Dictionary does not have a meaningful Hash, but its identity is
// unique for a Url and Offset.
//
// A |GetRange| struct is a member of a |Region| in the |RegionHeap|.
//
// Chunk |GetRange|s which depend on Dictionaries can be constructed with
// some state which allows them to fetch those dictionaries from a shared
// chache when they need them. That is their GetDict callback.
type GetRange struct {
	Url        string
	Hash       []byte
	Offset     uint64
	Length     uint32
	DictOffset uint64
	DictLength uint32
	Region     *Region
}

// A |Region| represents a continuous range of bytes within in a Url.
// |ranges.Tree| maintains |Region| instances that cover every |GetRange|
// within the tree. As entries are inserted into the Tree, their Regions can
// coalesce with Regions which come before or after them in the same Url,
// based on the |coalesceLimit|.
//
// |Region|s are maintained in a |RegionHeap| so that the |Tree| can quickly
// return a large download to get started on when a download worker is
// available.
type Region struct {
	Url          string
	StartOffset  uint64
	EndOffset    uint64
	MatchedBytes uint64
	HeapIndex    int
	Score        int
}

type RegionHeap []*Region

func (rh RegionHeap) Len() int {
	return len(rh)
}

const (
	HeapStrategy_smallest = iota
	HeapStrategy_largest
	HeapStrategy_random
	// HeapStrategy_largestGoodput orders by Region.MatchedBytes — the
	// sum of chunk lengths within the region — rather than total span.
	// Dispatches the region carrying the most wanted bytes first,
	// leaving sparser regions in the tree longer where they may still
	// coalesce with newly arriving chunks.
	HeapStrategy_largestGoodput
)

// strategy is read once at package init from DOLT_HEAP_STRATEGY.
// It is deliberately not mutated afterward: container/heap requires
// Less to be consistent across invocations.
var strategy = initStrategy()

func initStrategy() int {
	switch os.Getenv("DOLT_HEAP_STRATEGY") {
	case "", "largest":
		return HeapStrategy_largest
	case "smallest":
		return HeapStrategy_smallest
	case "random":
		return HeapStrategy_random
	case "goodput", "largest_goodput":
		return HeapStrategy_largestGoodput
	default:
		fmt.Fprintf(os.Stderr, "ranges: ignoring unknown DOLT_HEAP_STRATEGY=%q; using largest\n",
			os.Getenv("DOLT_HEAP_STRATEGY"))
		return HeapStrategy_largest
	}
}

func (rh RegionHeap) Less(i, j int) bool {
	switch strategy {
	case HeapStrategy_largest:
		return (rh[i].EndOffset - rh[i].StartOffset) > (rh[j].EndOffset - rh[j].StartOffset)
	case HeapStrategy_smallest:
		return (rh[i].EndOffset - rh[i].StartOffset) < (rh[j].EndOffset - rh[j].StartOffset)
	case HeapStrategy_largestGoodput:
		return rh[i].MatchedBytes > rh[j].MatchedBytes
	default:
		// HeapStrategy_random: stable random-ish order via the per-Region score.
		return rh[i].Score < rh[j].Score
	}
}

func (rh RegionHeap) Swap(i, j int) {
	rh[i], rh[j] = rh[j], rh[i]
	rh[i].HeapIndex = i
	rh[j].HeapIndex = j
}

func (rh *RegionHeap) Push(x any) {
	r := x.(*Region)
	*rh = append(*rh, r)
	r.HeapIndex = len(*rh) - 1
}

func (rh *RegionHeap) Pop() any {
	old := *rh
	n := len(old)
	r := old[n-1]
	*rh = old[0 : n-1]
	return r
}

// A ranges.Tree is a tree data structure designed to support efficient
// coalescing of non-overlapping ranges inserted into it.
type Tree struct {
	t             *btree.BTreeG[*GetRange]
	regions       *RegionHeap
	coalesceLimit int
}

func GetRangeLess(a, b *GetRange) bool {
	if a.Url == b.Url {
		return a.Offset < b.Offset
	} else {
		return a.Url < b.Url
	}
}

func NewTree(coalesceLimit int) *Tree {
	return &Tree{
		t:             btree.NewG[*GetRange](64, GetRangeLess),
		regions:       &RegionHeap{},
		coalesceLimit: coalesceLimit,
	}
}

// |intern| will deduplicate strings that are stored in the |ranges.Tree|, so
// that all equal values share the same heap memory. The context is that URLs
// stored in the |Tree| can be very long, since they can be pre-signed S3 URLs,
// for example. And in general a Tree will have a large number of |GetRange|
// entries, that contain the same |Url|.
func (t *Tree) intern(s string) string {
	t.t.AscendGreaterOrEqual(&GetRange{Url: s}, func(gr *GetRange) bool {
		if gr.Url == s {
			s = gr.Url
		}
		return false
	})
	return s
}

func (t *Tree) Len() int {
	return t.t.Len()
}

func (t *Tree) Insert(url string, hash []byte, offset uint64, length uint32, dictOffset uint64, dictLength uint32) {
	ins := &GetRange{
		Url:        t.intern(url),
		Hash:       hash,
		Offset:     offset,
		Length:     length,
		DictOffset: dictOffset,
		DictLength: dictLength,
	}
	t.t.ReplaceOrInsert(ins)

	// Check for coalesce with the range of the entry before the new one...
	t.t.DescendLessOrEqual(ins, func(gr *GetRange) bool {
		if gr == ins {
			return true
		}
		// If we coalesce...
		if ins.Url == gr.Url {
			regionEnd := gr.Region.EndOffset
			if regionEnd > ins.Offset {
				// Inserted entry is already contained in the prior region.
				ins.Region = gr.Region
				ins.Region.MatchedBytes += uint64(ins.Length)
				heap.Fix(t.regions, ins.Region.HeapIndex)
			} else if (ins.Offset - regionEnd) < uint64(t.coalesceLimit) {
				// Inserted entry is within the limit to coalesce with the prior one.
				ins.Region = gr.Region
				ins.Region.MatchedBytes += uint64(ins.Length)
				ins.Region.EndOffset = ins.Offset + uint64(ins.Length)
				heap.Fix(t.regions, ins.Region.HeapIndex)
			}
		}
		return false
	})

	// And for the the range of the entry after the new one...
	t.t.AscendGreaterOrEqual(ins, func(gr *GetRange) bool {
		if gr == ins {
			return true
		}
		// If we coalesce...
		if ins.Url == gr.Url && gr.Region != ins.Region {
			regionStart := gr.Region.StartOffset
			if regionStart < (ins.Offset + uint64(ins.Length) + uint64(t.coalesceLimit)) {
				if ins.Region == nil {
					ins.Region = gr.Region
					ins.Region.MatchedBytes += uint64(ins.Length)
					ins.Region.StartOffset = ins.Offset
					heap.Fix(t.regions, ins.Region.HeapIndex)
				} else {
					// TODO: Would be more efficient with union find...
					// Can be N^2 if we have an insert
					// pattern where we insert a bunch of
					// middle things in descending order
					// which merge with the region before
					// them and also merge with the region
					// after them.
					heap.Remove(t.regions, gr.Region.HeapIndex)
					ins.Region.EndOffset = gr.Region.EndOffset
					ins.Region.MatchedBytes += gr.Region.MatchedBytes
					start := &GetRange{Url: ins.Url, Offset: gr.Offset}
					end := &GetRange{Url: ins.Url, Offset: gr.Region.EndOffset}
					t.t.AscendRange(start, end, func(gr *GetRange) bool {
						gr.Region = ins.Region
						return true
					})
					heap.Fix(t.regions, ins.Region.HeapIndex)
				}
			}
		}
		return false
	})

	// We didn't coalesce with any existing Regions. Insert a new Region
	// covering just this GetRange.
	if ins.Region == nil {
		ins.Region = &Region{
			Url:          ins.Url,
			StartOffset:  ins.Offset,
			EndOffset:    ins.Offset + uint64(ins.Length),
			MatchedBytes: uint64(ins.Length),
			Score:        rand.Int(),
		}
		heap.Push(t.regions, ins.Region)
	}
}

// Returns all the |*GetRange| entries in the tree that are encompassed by the
// current top entry in our |RegionHeap|. For |HeapStrategy_largest|, this will
// be the largest possible download we can currently start, given our
// |coalesceLimit|. Also returns the popped region (so callers can record its
// URL/offset span) and the number of dark bytes in the dispatched region —
// bytes inside [StartOffset, EndOffset) that are not covered by any returned
// GetRange and that will be downloaded but discarded.
func (t *Tree) DeleteMaxRegion() ([]*GetRange, *Region, uint64) {
	if t.regions.Len() == 0 {
		return nil, nil, 0
	}
	region := heap.Pop(t.regions).(*Region)
	start := &GetRange{Url: region.Url, Offset: region.StartOffset}
	end := &GetRange{Url: region.Url, Offset: region.EndOffset}
	iter := t.t.Clone()
	var ret []*GetRange
	iter.AscendRange(start, end, func(gr *GetRange) bool {
		ret = append(ret, gr)
		t.t.Delete(gr)
		return true
	})
	dark := (region.EndOffset - region.StartOffset) - region.MatchedBytes
	return ret, region, dark
}
