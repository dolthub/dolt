package types

import "github.com/attic-labs/noms/chunks"

type Map interface {
	Value
	First() (Value, Value)
	Len() uint64
	Empty() bool
	Has(key Value) bool
	Get(key Value) Value
	MaybeGet(key Value) (v Value, ok bool)
	Set(key Value, val Value) Map
	SetM(kv ...Value) Map
	Remove(k Value) Map
	Iter(cb mapIterCallback)
	IterAll(cb mapIterAllCallback)
	IterAllP(concurrency int, f mapIterAllCallback)
	Filter(cb mapFilterCallback) Map
}

type indexOfMapFn func(m mapData, v Value) int
type mapIterCallback func(key, value Value) (stop bool)
type mapIterAllCallback func(key, value Value)
type mapFilterCallback func(key, value Value) (keep bool)

var mapType = MakeCompoundType(MapKind, MakePrimitiveType(ValueKind), MakePrimitiveType(ValueKind))

func NewMap(cs chunks.ChunkStore, kv ...Value) Map {
	return NewTypedMap(cs, mapType, kv...)
}

func NewTypedMap(cs chunks.ChunkStore, t Type, kv ...Value) Map {
	return newTypedMap(cs, t, buildMapData(mapData{}, kv, t)...)
}

func newTypedMap(cs chunks.ChunkStore, t Type, entries ...mapEntry) Map {
	seq := newEmptySequenceChunker(makeMapLeafChunkFn(t, cs), newMapMetaSequenceChunkFn(t, cs), newMapLeafBoundaryChecker(), newOrderedMetaSequenceBoundaryChecker)

	for _, entry := range entries {
		seq.Append(entry)
	}

	return seq.Done().(Map)
}
