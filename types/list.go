package types

import (
	"github.com/attic-labs/noms/chunks"
)

type List interface {
	Value
	Len() uint64
	Empty() bool
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

var listType = MakeCompoundType(ListKind, MakePrimitiveType(ValueKind))

func NewList(cs chunks.ChunkStore, v ...Value) List {
	return NewTypedList(cs, listType, v...)
}

func NewTypedList(cs chunks.ChunkStore, t Type, values ...Value) List {
	seq := newEmptySequenceChunker(makeListLeafChunkFn(t, cs), newMetaSequenceChunkFn(t, cs), newListLeafBoundaryChecker(), newMetaSequenceBoundaryChecker)
	for _, v := range values {
		seq.Append(v)
	}

	list := seq.Done()
	return internalValueFromType(list, list.Type()).(List)
}
