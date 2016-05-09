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
	orderedMetaSequence
	numLeaves uint64
	ref       *ref.Ref
}

func buildCompoundMap(tuples metaSequenceData, t *Type, vr ValueReader) metaSequence {
	return compoundMap{orderedMetaSequence{metaSequenceObject{tuples, t, vr}}, tuples.numLeavesSum(), &ref.Ref{}}
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

func (cm compoundMap) First() (Value, Value) {
	cur := newCursorAtKey(cm, nil, false, false)
	entry := cur.current().(mapEntry)
	return entry.key, entry.value
}

func (cm compoundMap) MaybeGet(key Value) (v Value, ok bool) {
	cur := newCursorAtKey(cm, key, false, false)
	if !cur.valid() {
		return nil, false
	}
	entry := cur.current().(mapEntry)
	if !entry.key.Equals(key) {
		return nil, false
	}

	return entry.value, true
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
	cur := newCursorAtKey(cm, k, true, false)
	found := cur.idx < cur.seq.seqLen() && cur.current().(mapEntry).key.Equals(k)
	seq := newSequenceChunker(cur, makeMapLeafChunkFn(cm.t, cm.vr), newOrderedMetaSequenceChunkFn(cm.t, cm.vr), newMapLeafBoundaryChecker(), newOrderedMetaSequenceBoundaryChecker)
	return seq, found
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
	cur := newCursorAtKey(cm, key, false, false)
	if !cur.valid() {
		return false
	}
	entry := cur.current().(mapEntry)
	return entry.key.Equals(key)
}

func (cm compoundMap) Get(key Value) Value {
	v, ok := cm.MaybeGet(key)
	d.Chk.True(ok)
	return v
}

func (cm compoundMap) Iter(cb mapIterCallback) {
	cur := newCursorAtKey(cm, nil, false, false)
	cur.iter(func(v interface{}) bool {
		entry := v.(mapEntry)
		return cb(entry.key, entry.value)
	})
}

func (cm compoundMap) IterAll(cb mapIterAllCallback) {
	cur := newCursorAtKey(cm, nil, false, false)
	cur.iter(func(v interface{}) bool {
		entry := v.(mapEntry)
		cb(entry.key, entry.value)
		return false
	})
}

func (cm compoundMap) elemTypes() []*Type {
	return cm.Type().Desc.(CompoundDesc).ElemTypes
}
