package ranges

import (
	"container/heap"

	"github.com/google/btree"
)

type interned string

type GetRange struct {
	Url    interned
	Hash   []byte
	Offset uint64
	Length uint32
	Region *Region
}

type Region struct {
	StartOffset  uint64
	EndOffset    uint64
	MatchedBytes uint64
	HeapIndex    int
}

type RegionHeap []*Region

func (rh RegionHeap) Len() int {
	return len(rh)
}

func (rh RegionHeap) Less(i, j int) bool {
	leni := rh[i].EndOffset - rh[i].StartOffset
	lenj := rh[j].EndOffset - rh[j].StartOffset
	return leni > lenj
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
// coallescing of non-overlapping ranges inserted into it.
type Tree struct {
	t *btree.BTreeG[*GetRange]
	regions *RegionHeap
	coallesceLimit int
}

func GetRangeLess(a, b *GetRange) bool {
	if a.Url == b.Url {
		return a.Offset < b.Offset
	} else {
		return a.Url < b.Url
	}
}

func NewTree(coallesceLimit int) *Tree {
	return &Tree{
		t: btree.NewG[*GetRange](64, GetRangeLess),
		regions: &RegionHeap{},
		coallesceLimit: coallesceLimit,
	}
}

func (t *Tree) intern(s string) interned {
	i := interned(s)
	t.t.AscendGreaterOrEqual(&GetRange{Url: i}, func(gr *GetRange) bool {
		if gr.Url == i {
			i = gr.Url
		}
		return false
	})
	return i
}

func (t *Tree) Insert(url string, hash []byte, offset uint64, length uint32) {
	ins := &GetRange{
		Url:    t.intern(url),
		Hash:   hash,
		Offset: offset,
		Length: length,
	}
	t.t.ReplaceOrInsert(ins)

	// Check for coallesce with the range of the entry before the new one...
	t.t.DescendLessOrEqual(ins, func(gr *GetRange) bool {
		if gr == ins {
			return true
		}
		// If we coallesce...
		if ins.Url == gr.Url {
			regionEnd := gr.Region.EndOffset
			if regionEnd > ins.Offset {
				// Inserted entry is already contained in the prior region.
				ins.Region = gr.Region
				ins.Region.MatchedBytes += uint64(ins.Length)
				heap.Fix(t.regions, ins.Region.HeapIndex)
			} else if (ins.Offset - regionEnd) < uint64(t.coallesceLimit) {
				// Inserted entry is within the limit to coallesce with the prior one.
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
		// If we coallesce...
		if ins.Url == gr.Url && gr.Region != ins.Region {
			regionStart := gr.Region.StartOffset
			if regionStart < (ins.Offset + uint64(ins.Length) + uint64(t.coallesceLimit)) {
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

	if ins.Region == nil {
		ins.Region = &Region{
			StartOffset:  ins.Offset,
			EndOffset:    ins.Offset + uint64(ins.Length),
			MatchedBytes: uint64(ins.Length),
		}
		heap.Push(t.regions, ins.Region)
	}
}

// Get Interface needs to take regions[0], our largest region, and return a
// getter based on it. Maybe slice it up if we don't want too large of a
// download... Remove all the corresponding entries from tree.t. Pop it from Heap.

//
// func NewTree[K any](less btree.LessFunc[K]) *Tree[K] {
// 	return &Tree[K]{
// 		t: btree.NewG[keyRange[K]](64, keyRangeLess(less)),
// 	}
// }
//
// type KeyRange[K any] struct {
// 	Begin K
// 	End   K
// }
//
// func keyRangeLess[K any](less btree.LessFunc[K]) func(a, b keyRange[K]) bool {
// 	return func(a, b keyRange[K]) bool {
// 		return less(a.begin, b.begin)
// 	}
// }
//
// // To insert a given *GetRange, we check the entry directly below where it would be inserted.
// //
// // * If it is within the colleasce range of that entry, we append the ranges onto the end of the existing entry.
// //
// // * Otherwise, we insert a new entry.
// //
// // After inserting or modifying an existing entry with the GetRange, we check the
// // entry directly after the inserted or modified range. If the end of the
// // inserted/modified range coallesces with the next entry, we delete the next
// // entry and we append its ranges onto the just inserted/modified entry.
// //
// // A (max)heap of the (byte range) sizes of the current ranges is kept, separate
// // of the sorted entries themselves. If we modify an existing entry, we heap.Fix
// // it. If we insert a new entry, we heap.Push it. When we merge an entry with the
// // entry which comes after it, we heap.Remove() the old entry and we heap.Fix the
// // updated entry.
// //
// // Ranges are allowed to grow arbitrarily large. When we pick the max range to
// // download, we might want to limit the size of the actual download. If we do
// // that, we take the current max range and we cut off the number of bytes we're
// // willing to download from the end. We remove those ranges from the end of the
// // entry in the tree and we heap.Fix the entry's heap entry.
//
// // TODO: Take goodput vs byte range length into account. If we transit 16KB to download 8KB, is that better than downloading 12KB of known chunks?
//
// // TODO: Count and account for gaps we download without knowing the address and then we end up downloading them again...
