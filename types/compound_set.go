package types

import (
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

const (
	// The window size to use for computing the rolling hash.
	setWindowSize = 1
	setPattern    = uint32(1<<6 - 1) // Average size of 64 elements
)

type compoundSet struct {
	metaSequenceObject
	ref *ref.Ref
	cs  chunks.ChunkStore
}

func buildCompoundSet(tuples metaSequenceData, t Type, cs chunks.ChunkStore) Value {
	s := compoundSet{metaSequenceObject{tuples, t}, &ref.Ref{}, cs}
	return valueFromType(cs, s, t)
}

func init() {
	registerMetaValue(SetKind, buildCompoundSet)
}

func (cs compoundSet) Equals(other Value) bool {
	return other != nil && cs.t.Equals(other.Type()) && cs.Ref() == other.Ref()
}

func (cs compoundSet) Ref() ref.Ref {
	return EnsureRef(cs.ref, cs)
}

func (cs compoundSet) Len() uint64 {
	panic("not implemented")
}

func (cs compoundSet) Empty() bool {
	d.Chk.True(cs.Len() > 0) // A compound object should never be empty.
	return false
}

func (cs compoundSet) First() Value {
	panic("not implemented")
}

func (cs compoundSet) Insert(values ...Value) Set {
	panic("not implemented")
}

func (cs compoundSet) Remove(values ...Value) Set {
	panic("not implemented")
}

func (cs compoundSet) Union(others ...Set) Set {
	panic("not implemented")
}

func (cs compoundSet) Subtract(others ...Set) Set {
	panic("not implemented")
}

func (cs compoundSet) Filter(cb setFilterCallback) Set {
	panic("not implemented")
}

// TODO: seek should return false if it failed to find the value
func (cs compoundSet) findLeaf(key Value) (*sequenceCursor, setLeaf) {
	cursor, leaf := newMetaSequenceCursor(cs, cs.cs)

	var seekFn sequenceCursorSeekBinaryCompareFn
	if orderedSequenceByIndexedType(cs.t) {
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
		leaf = readMetaTupleValue(cursor.current(), cs.cs)
	}

	return cursor, leaf.(setLeaf)
}

func (cs compoundSet) Has(key Value) bool {
	_, leaf := cs.findLeaf(key)
	return leaf.Has(key)
}

func (cs compoundSet) Iter(cb setIterCallback) {
	iterateMetaSequenceLeaf(cs, cs.cs, func(v Value) bool {
		s := v.(setLeaf)
		for _, v := range s.data {
			if cb(v) {
				return true
			}
		}
		return false
	})
}

func (cs compoundSet) IterAll(cb setIterAllCallback) {
	iterateMetaSequenceLeaf(cs, cs.cs, func(v Value) bool {
		v.(setLeaf).IterAll(cb)
		return false
	})
}

func (cs compoundSet) IterAllP(concurrency int, f setIterAllCallback) {
	iterateMetaSequenceLeaf(cs, cs.cs, func(v Value) bool {
		v.(setLeaf).IterAllP(concurrency, f)
		return false
	})
}

func orderedSequenceByIndexedType(t Type) bool {
	return t.Desc.(CompoundDesc).ElemTypes[0].IsOrdered()
}

func newSetMetaSequenceChunkFn(t Type, cs chunks.ChunkStore) makeChunkFn {
	return func(items []sequenceItem) (sequenceItem, Value) {
		tuples := make(metaSequenceData, len(items))

		for i, v := range items {
			tuples[i] = v.(metaTuple)
		}

		lastIndex := tuples[len(tuples)-1].value
		meta := newMetaSequenceFromData(tuples, t, cs)
		ref := WriteValue(meta, cs)
		return metaTuple{ref, lastIndex}, meta
	}
}
