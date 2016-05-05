package types

import "github.com/attic-labs/noms/d"

type Map interface {
	Collection
	First() (Value, Value)
	Has(key Value) bool
	Get(key Value) Value
	MaybeGet(key Value) (v Value, ok bool)
	Set(key Value, val Value) Map
	SetM(kv ...Value) Map
	Remove(k Value) Map
	Iter(cb mapIterCallback)
	IterAll(cb mapIterAllCallback)
	Filter(cb mapFilterCallback) Map
	elemTypes() []*Type
}

type indexOfMapFn func(m mapData, v Value) int
type mapIterCallback func(key, value Value) (stop bool)
type mapIterAllCallback func(key, value Value)
type mapFilterCallback func(key, value Value) (keep bool)

var mapType = MakeMapType(ValueType, ValueType)

func NewMap(kv ...Value) Map {
	return NewTypedMap(mapType, kv...)
}

func NewTypedMap(t *Type, kv ...Value) Map {
	d.Chk.Equal(MapKind, t.Kind(), "Invalid type. Expected: MapKind, found: %s", t.Describe())
	return newTypedMap(t, buildMapData(mapData{}, kv, t)...)
}

func newTypedMap(t *Type, entries ...mapEntry) Map {
	seq := newEmptySequenceChunker(makeMapLeafChunkFn(t, nil), newOrderedMetaSequenceChunkFn(t, nil), newMapLeafBoundaryChecker(), newOrderedMetaSequenceBoundaryChecker)

	for _, entry := range entries {
		seq.Append(entry)
	}

	return seq.Done().(Map)
}
