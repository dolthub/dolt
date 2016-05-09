package types

import "github.com/attic-labs/noms/d"

type Set interface {
	Collection
	First() Value
	Has(key Value) bool
	Insert(values ...Value) Set
	Remove(values ...Value) Set
	Iter(cb setIterCallback)
	IterAll(cb setIterAllCallback)
	Filter(cb setFilterCallback) Set
	elemType() *Type
	valueReader() ValueReader
}

type indexOfSetFn func(m setData, v Value) int
type setIterCallback func(v Value) bool
type setIterAllCallback func(v Value)
type setFilterCallback func(v Value) (keep bool)

var setType = MakeSetType(ValueType)

func NewSet(v ...Value) Set {
	return NewTypedSet(setType, v...)
}

func NewTypedSet(t *Type, v ...Value) Set {
	d.Chk.Equal(SetKind, t.Kind(), "Invalid type. Expected:SetKind, found: %s", t.Describe())
	return newTypedSet(t, buildSetData(setData{}, v, t)...)
}

func newTypedSet(t *Type, data ...Value) Set {
	seq := newEmptySequenceChunker(makeSetLeafChunkFn(t, nil), newOrderedMetaSequenceChunkFn(t, nil), newSetLeafBoundaryChecker(), newOrderedMetaSequenceBoundaryChecker)

	for _, v := range data {
		seq.Append(v)
	}

	return seq.Done().(Set)
}
