package types

import (
	"sort"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
)

// compoundList represents a list of lists
// compoundList implements the List interface
// compoundList implements the Value interface
type compoundList struct {
	offsets []uint64
	lists   []Future
	ref     *ref.Ref
	cs      chunks.ChunkSource
}

func (cl compoundList) Len() uint64 {
	return cl.offsets[len(cl.offsets)-1]
}

func (cl compoundList) Empty() bool {
	return cl.Len() == uint64(0)
}

func findSubIndex(idx uint64, offsets []uint64) int {
	return sort.Search(len(offsets), func(i int) bool {
		return offsets[i] > idx
	})
}

func (cl compoundList) Get(idx uint64) Value {
	li := findSubIndex(idx, cl.offsets)
	f := cl.lists[li]
	l := f.Deref(cl.cs).(List)
	if li > 0 {
		idx -= cl.offsets[li-1]
	}
	return l.Get(idx)
}

func (cl compoundList) Slice(start uint64, end uint64) List {
	// TODO: Implement
	return cl
}

func (cl compoundList) Set(idx uint64, v Value) List {
	// TODO: Implement
	return cl
}

func (cl compoundList) Append(v ...Value) List {
	// TODO: Implement
	return cl
}

func (cl compoundList) Insert(idx uint64, v ...Value) List {
	// TODO: Implement
	return cl
}

func (cl compoundList) Remove(start uint64, end uint64) List {
	// TODO: Implement
	return cl
}

func (cl compoundList) RemoveAt(idx uint64) List {
	// TODO: Implement
	return cl
}

func (cl compoundList) Ref() ref.Ref {
	return ensureRef(cl.ref, cl)
}

func (cl compoundList) Release() {
	for _, f := range cl.lists {
		f.Release()
	}
}

func (cl compoundList) Equals(other Value) bool {
	if other == nil {
		return false
	}
	return cl.Ref() == other.Ref()
}

func (cl compoundList) Chunks() (futures []Future) {
	for _, f := range cl.lists {
		if f, ok := f.(*unresolvedFuture); ok {
			futures = append(futures, f)
		}
	}
	return
}
