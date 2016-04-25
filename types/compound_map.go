package types

import (
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

const (
	mapWindowSize = 1
	mapPattern    = uint32(1<<6 - 1) // Average size of 64 elements
)

type compoundMap struct {
	metaSequenceObject
	numLeaves uint64
	ref       *ref.Ref
	vr        ValueReader
}

func buildCompoundMap(tuples metaSequenceData, t *Type, vr ValueReader) Value {
	return compoundMap{metaSequenceObject{tuples, t}, tuples.numLeavesSum(), &ref.Ref{}, vr}
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
	return cm.numLeaves
}

func (cm compoundMap) Empty() bool {
	d.Chk.True(cm.Len() > 0) // A compound object should never be empty.
	return false
}

func (cm compoundMap) findLeaf(key Value) (*sequenceCursor, mapLeaf, int) {
	cursor, leaf, idx := findLeafInOrderedSequence(cm, cm.t, key, func(v Value) []Value {
		entries := v.(mapLeaf).data
		res := make([]Value, len(entries))
		for i, entry := range entries {
			res[i] = entry.key
		}
		return res
	}, cm.vr)
	return cursor, leaf.(mapLeaf), idx
}

func (cm compoundMap) First() (Value, Value) {
	_, leaf := newMetaSequenceCursor(cm, cm.vr)
	return leaf.(mapLeaf).First()
}

func (cm compoundMap) MaybeGet(key Value) (v Value, ok bool) {
	_, leaf, _ := cm.findLeaf(key)
	return leaf.MaybeGet(key)
}

func (cm compoundMap) Set(key Value, val Value) Map {
	return cm.SetM(key, val)
}

func (cm compoundMap) SetM(kv ...Value) Map {
	if len(kv) == 0 {
		return cm
	}
	d.Chk.True(len(kv)%2 == 0)

	assertMapElemTypes(cm, kv...)

	k, v, tail := kv[0], kv[1], kv[2:]

	seq, found := cm.sequenceChunkerAtKey(k)
	if found {
		seq.Skip()
	}
	seq.Append(mapEntry{k, v})
	return seq.Done().(Map).SetM(tail...)
}

func (cm compoundMap) Remove(k Value) Map {
	if seq, found := cm.sequenceChunkerAtKey(k); found {
		seq.Skip()
		return seq.Done().(Map)
	}
	return cm
}

func (cm compoundMap) sequenceChunkerAtKey(k Value) (*sequenceChunker, bool) {
	metaCur, leaf, idx := cm.findLeaf(k)

	cur := &sequenceCursor{metaCur, leaf, idx, len(leaf.data), func(otherLeaf sequenceItem, idx int) sequenceItem {
		return otherLeaf.(mapLeaf).data[idx]
	}, func(mt sequenceItem) (sequenceItem, int) {
		otherLeaf := readMetaTupleValue(mt, cm.vr).(mapLeaf)
		return otherLeaf, len(otherLeaf.data)
	}}

	seq := newSequenceChunker(cur, makeMapLeafChunkFn(cm.t, cm.vr), newOrderedMetaSequenceChunkFn(cm.t, cm.vr), newMapLeafBoundaryChecker(), newOrderedMetaSequenceBoundaryChecker)
	found := idx < len(leaf.data) && leaf.data[idx].key.Equals(k)
	return seq, found
}

func (cm compoundMap) IterAllP(concurrency int, f mapIterAllCallback) {
	// TODO: Improve
	iterateMetaSequenceLeaf(cm, cm.vr, func(v Value) bool {
		v.(mapLeaf).IterAllP(concurrency, f)
		return false
	})
}

func (cm compoundMap) Filter(cb mapFilterCallback) Map {
	seq := newEmptySequenceChunker(makeMapLeafChunkFn(cm.t, cm.vr), newOrderedMetaSequenceChunkFn(cm.t, cm.vr), newMapLeafBoundaryChecker(), newOrderedMetaSequenceBoundaryChecker)

	cm.IterAll(func(k, v Value) {
		if cb(k, v) {
			seq.Append(mapEntry{k, v})
		}
	})

	return seq.Done().(Map)
}

func (cm compoundMap) Has(key Value) bool {
	_, leaf, _ := cm.findLeaf(key)
	return leaf.Has(key)
}

func (cm compoundMap) Get(key Value) Value {
	_, leaf, _ := cm.findLeaf(key)
	return leaf.Get(key)
}

func (cm compoundMap) Iter(cb mapIterCallback) {
	iterateMetaSequenceLeaf(cm, cm.vr, func(v Value) bool {
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
	iterateMetaSequenceLeaf(cm, cm.vr, func(v Value) bool {
		v.(mapLeaf).IterAll(cb)
		return false
	})
}

func (cm compoundMap) elemTypes() []*Type {
	return cm.Type().Desc.(CompoundDesc).ElemTypes
}
