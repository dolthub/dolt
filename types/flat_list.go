package types

import (
	"github.com/attic-labs/noms/ref"
)

// flatList is a quick 'n easy implementation of List.
// It should eventually be replaced by a chunking implementation.
type flatList struct {
	list []Value
	cr   *cachedRef
}

func newFlatList(list []Value) List {
	return flatList{list, &cachedRef{}}
}

func (l flatList) Len() uint64 {
	return uint64(len(l.list))
}

func (l flatList) Get(idx uint64) Value {
	return l.list[idx]
}

func (l flatList) Slice(start uint64, end uint64) List {
	return newFlatList(l.list[start:end])
}

func (l flatList) Set(idx uint64, v Value) List {
	b := make([]Value, len(l.list))
	copy(b, l.list)
	b[idx] = v
	return newFlatList(b)
}

func (l flatList) Append(v ...Value) List {
	return newFlatList(append(l.list, v...))
}

func (l flatList) Insert(idx uint64, v ...Value) List {
	b := make([]Value, len(l.list)+len(v))
	copy(b, l.list[:idx])
	copy(b[idx:], v)
	copy(b[idx+uint64(len(v)):], l.list[idx:])
	return newFlatList(b)
}

func (l flatList) Remove(start uint64, end uint64) List {
	b := make([]Value, uint64(len(l.list))-(end-start))
	copy(b, l.list[:start])
	copy(b[start:], l.list[end:])
	return newFlatList(b)
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
