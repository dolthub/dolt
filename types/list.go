package types

import "github.com/attic-labs/noms/d"

type List interface {
	Collection
	Get(idx uint64) Value
	Iter(f listIterFunc)
	IterAll(f listIterAllFunc)
	IterAllP(concurrency int, f listIterAllFunc)
	Filter(cb listFilterCallback) List
	Map(mf MapFunc) []interface{}
	MapP(concurrency int, mf MapFunc) []interface{}
	Slice(start uint64, end uint64) List
	Set(idx uint64, v Value) List
	Append(v ...Value) List
	Insert(idx uint64, v ...Value) List
	Remove(start uint64, end uint64) List
	RemoveAt(idx uint64) List
}

type listIterFunc func(v Value, index uint64) (stop bool)
type listIterAllFunc func(v Value, index uint64)
type listFilterCallback func(v Value, index uint64) (keep bool)
type MapFunc func(v Value, index uint64) interface{}

var listType = MakeListType(ValueType)

// NewList creates a new untyped List, populated with values, chunking if and when needed.
func NewList(v ...Value) List {
	return NewTypedList(listType, v...)
}

// NewTypedList creates a new List with type t, populated with values, chunking if and when needed.
func NewTypedList(t *Type, values ...Value) List {
	d.Chk.Equal(ListKind, t.Kind(), "Invalid type. Expected: ListKind, found: %s", t.Describe())
	seq := newEmptySequenceChunker(makeListLeafChunkFn(t, nil), newIndexedMetaSequenceChunkFn(t, nil, nil), newListLeafBoundaryChecker(), newIndexedMetaSequenceBoundaryChecker)
	for _, v := range values {
		seq.Append(v)
	}
	return seq.Done().(List)
}

// NewStreamingTypedList creates a new List with type t, populated with values, chunking if and when needed. As chunks are created, they're written to vrw -- including the root chunk of the list. Once the caller has closed values, she can read the completed List from the returned channel.
func NewStreamingTypedList(t *Type, vrw ValueReadWriter, values <-chan Value) <-chan List {
	out := make(chan List)
	go func() {
		seq := newEmptySequenceChunker(makeListLeafChunkFn(t, vrw), newIndexedMetaSequenceChunkFn(t, vrw, vrw), newListLeafBoundaryChecker(), newIndexedMetaSequenceBoundaryChecker)
		for v := range values {
			seq.Append(v)
		}
		out <- seq.Done().(List)
		close(out)
	}()
	return out
}
