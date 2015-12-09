package types

import "github.com/attic-labs/noms/chunks"

type Set interface {
	Value
	First() Value
	Len() uint64
	Empty() bool
	Has(key Value) bool
	Insert(values ...Value) Set
	Remove(values ...Value) Set
	Union(others ...Set) Set
	Subtract(others ...Set) Set
	Iter(cb setIterCallback)
	IterAll(cb setIterAllCallback)
	IterAllP(concurrency int, f setIterAllCallback)
	Filter(cb setFilterCallback) Set
}

type indexOfSetFn func(m setData, v Value) int
type setIterCallback func(v Value) bool
type setIterAllCallback func(v Value)
type setFilterCallback func(v Value) (keep bool)

var setType = MakeCompoundType(SetKind, MakePrimitiveType(ValueKind))

func NewSet(cs chunks.ChunkStore, v ...Value) Set {
	return NewTypedSet(cs, setType, v...)
}

func NewTypedSet(cs chunks.ChunkStore, t Type, v ...Value) Set {
	return newTypedSet(cs, t, buildSetData(setData{}, v, t)...)
}

func newTypedSet(cs chunks.ChunkStore, t Type, data ...Value) Set {
	seq := newEmptySequenceChunker(makeSetLeafChunkFn(t, cs), newSetMetaSequenceChunkFn(t, cs), newSetLeafBoundaryChecker(), newOrderedMetaSequenceBoundaryChecker)

	for _, v := range data {
		seq.Append(v)
	}

	s := seq.Done()
	return internalValueFromType(s, s.Type()).(Set)
}
