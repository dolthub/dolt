package types

import (
	"github.com/attic-labs/noms/chunks"
	. "github.com/attic-labs/noms/dbg"
	"github.com/attic-labs/noms/ref"
)

// flatList is a quick 'n easy implementation of List.
// It should eventually be replaced by a chunking implementation.
type flatList struct {
	list []Future
	cr   *cachedRef
	cs 	 chunks.ChunkSource
}

func valuesToFutures(list []Value) []Future {
	f := []Future{}
	for _, v := range list {
		f = append(f, FutureFromValue(v))
	}
	return f
}

func newFlatList(list []Future, cs chunks.ChunkSource) List {
	return flatList{list, &cachedRef{}, cs}
}

func (l flatList) Len() uint64 {
	return uint64(len(l.list))
}

func (l flatList) Get(idx uint64) Value {
	v, err := l.list[idx].Deref(l.cs)
	// TODO: blech
	Chk.NoError(err)
	return v
}

func (l flatList) Slice(start uint64, end uint64) List {
	return newFlatList(l.list[start:end], l.cs)
}

func (l flatList) Set(idx uint64, v Value) List {
	b := make([]Future, len(l.list))
	copy(b, l.list)
	b[idx] = FutureFromValue(v)
	return newFlatList(b, l.cs)
}

func (l flatList) Append(v ...Value) List {
	return newFlatList(append(l.list, valuesToFutures(v)...), l.cs)
}

func (l flatList) Insert(idx uint64, v ...Value) List {
	b := make([]Future, len(l.list)+len(v))
	copy(b, l.list[:idx])
	copy(b[idx:], valuesToFutures(v))
	copy(b[idx+uint64(len(v)):], l.list[idx:])
	return newFlatList(b, l.cs)
}

func (l flatList) Remove(start uint64, end uint64) List {
	b := make([]Future, uint64(len(l.list))-(end-start))
	copy(b, l.list[:start])
	copy(b[start:], l.list[end:])
	return newFlatList(b, l.cs)
}

func (l flatList) RemoveAt(idx uint64) List {
	return l.Remove(idx, idx+1)
}

func (l flatList) Ref() ref.Ref {
	return l.cr.Ref(l)
}

func (l flatList) Equals(other Value) bool {
	if other == nil {
		return false
	} else {
		return l.Ref() == other.Ref()
	}
}
