package types

import (
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
	numLeaves uint64
	ref       *ref.Ref
	vr        ValueReader
}

func buildCompoundSet(tuples metaSequenceData, t *Type, vr ValueReader) Value {
	return compoundSet{metaSequenceObject{tuples, t}, tuples.numLeavesSum(), &ref.Ref{}, vr}
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
	return cs.numLeaves
}

func (cs compoundSet) Empty() bool {
	d.Chk.True(cs.Len() > 0) // A compound object should never be empty.
	return false
}

func (cs compoundSet) First() Value {
	_, leaf := newMetaSequenceCursor(cs, cs.vr)
	return leaf.(setLeaf).First()
}

func (cs compoundSet) Insert(values ...Value) Set {
	if len(values) == 0 {
		return cs
	}

	assertType(cs.elemType(), values...)

	head, tail := values[0], values[1:]

	var res Set
	if seq, found := cs.sequenceChunkerAtValue(head); !found {
		seq.Append(head)
		res = seq.Done().(Set)
	} else {
		res = cs
	}

	return res.Insert(tail...)
}

func (cs compoundSet) Remove(values ...Value) Set {
	if len(values) == 0 {
		return cs
	}

	head, tail := values[0], values[1:]

	var res Set
	if seq, found := cs.sequenceChunkerAtValue(head); found {
		seq.Skip()
		res = seq.Done().(Set)
	} else {
		res = cs
	}

	return res.Remove(tail...)
}

func (cs compoundSet) sequenceCursorAtValue(v Value) (*sequenceCursor, bool) {
	metaCur, leaf, idx := cs.findLeaf(v)
	cur := newSetSequenceCursorAtPosition(metaCur, leaf, idx, cs.vr)
	found := idx < len(leaf.data) && leaf.data[idx].Equals(v)
	return cur, found
}

func newSetSequenceCursorAtPosition(metaCur *sequenceCursor, leaf setLeaf, idx int, cs ValueReader) *sequenceCursor {
	return &sequenceCursor{metaCur, leaf, idx, len(leaf.data), func(otherLeaf sequenceItem, idx int) sequenceItem {
		return otherLeaf.(setLeaf).data[idx]
	}, func(mt sequenceItem) (sequenceItem, int) {
		otherLeaf := readMetaTupleValue(mt, cs).(setLeaf)
		return otherLeaf, len(otherLeaf.data)
	}}
}

func (cs compoundSet) sequenceChunkerAtValue(v Value) (*sequenceChunker, bool) {
	cur, found := cs.sequenceCursorAtValue(v)
	seq := newSequenceChunker(cur, makeSetLeafChunkFn(cs.t, cs.vr), newOrderedMetaSequenceChunkFn(cs.t, cs.vr), newSetLeafBoundaryChecker(), newOrderedMetaSequenceBoundaryChecker)
	return seq, found
}

func (cs compoundSet) Union(others ...Set) Set {
	return setUnion(cs, others)
}

func (cs compoundSet) Subtract(others ...Set) Set {
	panic("not implemented")
}

func (cs compoundSet) Filter(cb setFilterCallback) Set {
	seq := newEmptySequenceChunker(makeSetLeafChunkFn(cs.t, cs.vr), newOrderedMetaSequenceChunkFn(cs.t, cs.vr), newSetLeafBoundaryChecker(), newOrderedMetaSequenceBoundaryChecker)

	cs.IterAll(func(v Value) {
		if cb(v) {
			seq.Append(v)
		}
	})

	return seq.Done().(Set)
}

func (cs compoundSet) findLeaf(key Value) (*sequenceCursor, setLeaf, int) {
	cursor, leaf, idx := findLeafInOrderedSequence(cs, cs.t, key, func(v Value) []Value {
		return v.(setLeaf).data
	}, cs.vr)
	return cursor, leaf.(setLeaf), idx
}

func (cs compoundSet) Has(key Value) bool {
	_, leaf, _ := cs.findLeaf(key)
	return leaf.Has(key)
}

func (cs compoundSet) Iter(cb setIterCallback) {
	iterateMetaSequenceLeaf(cs, cs.vr, func(v Value) bool {
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
	iterateMetaSequenceLeaf(cs, cs.vr, func(v Value) bool {
		v.(setLeaf).IterAll(cb)
		return false
	})
}

func (cs compoundSet) elemType() *Type {
	return cs.t.Desc.(CompoundDesc).ElemTypes[0]
}

func (cs compoundSet) sequenceCursorAtFirst() *sequenceCursor {
	metaCur, leaf := newMetaSequenceCursor(cs, cs.vr)
	return newSetSequenceCursorAtPosition(metaCur, leaf.(setLeaf), 0, cs.vr)
}

func (cs compoundSet) valueReader() ValueReader {
	return cs.vr
}
