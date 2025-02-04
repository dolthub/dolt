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
	"math/rand/v2"

	"github.com/google/btree"
)

// GetRange represents a way to get the contents for a Chunk from a given Url
// with an HTTP Range request. The chunk with hash |Hash| can be fetched using
// the |Url| with a Range request starting at |Offset| and reading |Length|
// bytes.
//
// A |GetRange| struct is a member of a |Region| in the |RegionHeap|.
type GetRange struct {
	Url    string
	Hash   []byte
	Offset uint64
	Length uint32
	Region *Region

	// Archive file format requires the url/dictionary offset/length to be carried through to fully resolve the chunk.
	// This information is not used withing the range calculations at all, as the range is not related to the chunk content.
	DictionaryOffset uint64
	DictionaryLength uint32
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
)

var strategy = HeapStrategy_largest

func (rh RegionHeap) Less(i, j int) bool {
	leni := rh[i].EndOffset - rh[i].StartOffset
	lenj := rh[j].EndOffset - rh[j].StartOffset
	if strategy == HeapStrategy_largest {
		// This makes us track the largest region...
		return leni > lenj
	} else if strategy == HeapStrategy_smallest {
		// This makes us track the smallest...
		return leni < lenj
	} else {
		// This makes us track a random order...
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
		Url:              t.intern(url),
		Hash:             hash,
		Offset:           offset,
		Length:           length,
		DictionaryOffset: dictOffset,
		DictionaryLength: dictLength,
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
// |coalesceLimit|.
func (t *Tree) DeleteMaxRegion() []*GetRange {
	if t.regions.Len() == 0 {
		return nil
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
	return ret
}
