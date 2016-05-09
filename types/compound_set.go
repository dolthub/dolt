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
	orderedMetaSequence
	numLeaves uint64
	ref       *ref.Ref
}

func buildCompoundSet(tuples metaSequenceData, t *Type, vr ValueReader) metaSequence {
	return compoundSet{orderedMetaSequence{metaSequenceObject{tuples, t, vr}}, tuples.numLeavesSum(), &ref.Ref{}}
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
	cur := newCursorAtKey(cs, nil, false, false)
	return cur.current().(Value)
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

func (cs compoundSet) sequenceChunkerAtValue(v Value) (*sequenceChunker, bool) {
	cur := newCursorAtKey(cs, v, true, false)
	found := cur.idx < cur.seq.seqLen() && cur.current().(Value).Equals(v)
	seq := newSequenceChunker(cur, makeSetLeafChunkFn(cs.t, cs.vr), newOrderedMetaSequenceChunkFn(cs.t, cs.vr), newSetLeafBoundaryChecker(), newOrderedMetaSequenceBoundaryChecker)
	return seq, found
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

func (cs compoundSet) Has(key Value) bool {
	cur := newCursorAtKey(cs, key, false, false)
	return cur.valid() && cur.current().(Value).Equals(key)
}

func (cs compoundSet) Iter(cb setIterCallback) {
	cur := newCursorAtKey(cs, nil, false, false)
	cur.iter(func(v interface{}) bool {
		return cb(v.(Value))
	})
}

func (cs compoundSet) IterAll(cb setIterAllCallback) {
	cur := newCursorAtKey(cs, nil, false, false)
	cur.iter(func(v interface{}) bool {
		cb(v.(Value))
		return false
	})
}

func (cs compoundSet) elemType() *Type {
	return cs.t.Desc.(CompoundDesc).ElemTypes[0]
}

func (cs compoundSet) valueReader() ValueReader {
	return cs.vr
}
