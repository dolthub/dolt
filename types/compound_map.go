package types

import (
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

const (
	mapWindowSize = 1
	mapPattern    = uint32(1<<6 - 1) // Average size of 64 elements
)

type compoundMap struct {
	metaSequenceObject
	ref *ref.Ref
	cs  chunks.ChunkStore
}

func buildCompoundMap(tuples metaSequenceData, t Type, cs chunks.ChunkStore) Value {
	cm := compoundMap{metaSequenceObject{tuples, t}, &ref.Ref{}, cs}
	return valueFromType(cs, cm, t)
}

func init() {
	registerMetaValue(MapKind, buildCompoundMap)
}

func (cm compoundMap) Equals(other Value) bool {
	return other != nil && cm.t.Equals(other.Type()) && cm.Ref() == other.Ref()
}

func (cm compoundMap) Ref() ref.Ref {
	return EnsureRef(cm.ref, cm)
}

func (cm compoundMap) Len() uint64 {
	panic("not implemented")
}

func (cm compoundMap) Empty() bool {
	d.Chk.True(cm.Len() > 0) // A compound object should never be empty.
	return false
}

// TODO: seek should return false if it failed to find the value
func (cm compoundMap) findLeaf(key Value) (*sequenceCursor, mapLeaf) {
	cursor, leaf := newMetaSequenceCursor(cm, cm.cs)

	var seekFn sequenceCursorSeekBinaryCompareFn
	if orderedSequenceByIndexedType(cm.t) {
		orderedKey := key.(OrderedValue)

		seekFn = func(mt sequenceItem) bool {
			return !mt.(metaTuple).value.(OrderedValue).Less(orderedKey)
		}
	} else {
		seekFn = func(mt sequenceItem) bool {
			return !mt.(metaTuple).value.(Ref).TargetRef().Less(key.Ref())
		}
	}

	cursor.seekBinary(seekFn)

	current := cursor.current().(metaTuple)
	if current.ref != leaf.Ref() {
		leaf = readMetaTupleValue(cursor.current(), cm.cs)
	}

	return cursor, leaf.(mapLeaf)
}

func (cm compoundMap) First() (Value, Value) {
	_, leaf := newMetaSequenceCursor(cm, cm.cs)
	return leaf.(mapLeaf).First()
}

func (cm compoundMap) MaybeGet(key Value) (v Value, ok bool) {
	_, leaf := cm.findLeaf(key)
	return leaf.MaybeGet(key)
}

func (cm compoundMap) Set(key Value, val Value) Map {
	panic("Not implemented")
}

func (cm compoundMap) SetM(kv ...Value) Map {
	panic("Not implemented")
}
func (cm compoundMap) Remove(k Value) Map {
	panic("Not implemented")
}

func (cm compoundMap) IterAllP(concurrency int, f mapIterAllCallback) {
	// TODO: Improve
	iterateMetaSequenceLeaf(cm, cm.cs, func(v Value) bool {
		v.(mapLeaf).IterAllP(concurrency, f)
		return false
	})
}

func (cm compoundMap) Filter(cb mapFilterCallback) Map {
	panic("Not implemented")
}

func (cm compoundMap) Has(key Value) bool {
	_, leaf := cm.findLeaf(key)
	return leaf.Has(key)
}

func (cm compoundMap) Get(key Value) Value {
	_, leaf := cm.findLeaf(key)
	return leaf.Get(key)
}

func (cm compoundMap) Iter(cb mapIterCallback) {
	iterateMetaSequenceLeaf(cm, cm.cs, func(v Value) bool {
		m := v.(mapLeaf)
		for _, entry := range m.data {
			if cb(entry.key, entry.value) {
				return true
			}
		}
		return false
	})
}

func (cm compoundMap) IterAll(cb mapIterAllCallback) {
	iterateMetaSequenceLeaf(cm, cm.cs, func(v Value) bool {
		v.(mapLeaf).IterAll(cb)
		return false
	})
}

func newMapMetaSequenceChunkFn(t Type, cs chunks.ChunkStore) makeChunkFn {
	return func(items []sequenceItem) (sequenceItem, Value) {
		tuples := make(metaSequenceData, len(items))

		for i, v := range items {
			tuples[i] = v.(metaTuple)
		}

		lastIndex := tuples.last().value
		meta := newMetaSequenceFromData(tuples, t, cs)
		ref := WriteValue(meta, cs)
		return metaTuple{ref, lastIndex}, meta
	}
}
