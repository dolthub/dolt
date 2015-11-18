package types

import (
	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/attic-labs/buzhash"
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

const (
	// The window size to use for computing the rolling hash.
	listWindowSize = 64
	listPattern    = uint32(1<<6 - 1) // Average size of 64 elements
)

type compoundList struct {
	metaSequenceObject
	ref *ref.Ref
	cs  chunks.ChunkSource
}

func buildCompoundList(tuples metaSequenceData, t Type, cs chunks.ChunkSource) Value {
	return compoundList{metaSequenceObject{tuples, t}, &ref.Ref{}, cs}
}

func getListSequenceData(v Value) metaSequenceData {
	return v.(compoundList).tuples
}

func init() {
	registerMetaValue(ListKind, buildCompoundList, getListSequenceData)
}

func (cl compoundList) Equals(other Value) bool {
	return other != nil && cl.t.Equals(other.Type()) && cl.Ref() == other.Ref()
}

func (cl compoundList) Ref() ref.Ref {
	return EnsureRef(cl.ref, cl)
}

func (cl compoundList) Len() uint64 {
	return cl.tuples[len(cl.tuples)-1].uint64Value()
}

func (cl compoundList) Empty() bool {
	d.Chk.True(cl.Len() > 0) // A compound object should never be empty.
	return false
}

func (cl compoundList) cursorAt(idx uint64) (cursor *metaSequenceCursor, listLeaf List, start uint64) {
	d.Chk.True(idx < cl.Len())
	cursor, leaf := newMetaSequenceCursor(cl, cl.cs)

	chunkStart := cursor.seek(func(v, parent Value) bool {
		d.Chk.NotNil(v)
		d.Chk.NotNil(parent)

		return idx < uint64(parent.(UInt64))+uint64(v.(UInt64))
	}, func(parent, prev, current Value) Value {
		pv := uint64(0)
		if prev != nil {
			pv = uint64(prev.(UInt64))
		}

		return UInt64(uint64(parent.(UInt64)) + pv)
	}, UInt64(0))

	if cursor.currentRef() != leaf.Ref() {
		leaf = cursor.current()
	}

	listLeaf = leaf.(List)
	start = uint64(chunkStart.(UInt64))
	return
}

func (cl compoundList) Get(idx uint64) Value {
	_, l, start := cl.cursorAt(idx)
	return l.Get(idx - start)
}

func (cl compoundList) Iter(f listIterFunc) {
	start := uint64(0)

	iterateMetaSequenceLeaf(cl, cl.cs, func(l Value) bool {
		list := l.(List)
		for i, v := range list.values {
			if f(v, start+uint64(i)) {
				return true
			}
		}
		start += list.Len()
		return false
	})

}

func (cl compoundList) IterAll(f listIterAllFunc) {
	start := uint64(0)

	iterateMetaSequenceLeaf(cl, cl.cs, func(l Value) bool {
		list := l.(List)
		for i, v := range list.values {
			f(v, start+uint64(i))
		}
		start += list.Len()
		return false
	})
}

func listLeafIsBoundaryFn() isBoundaryFn {
	h := buzhash.NewBuzHash(listWindowSize)

	return func(item sequenceItem) bool {
		v, ok := item.(Value)
		d.Chk.True(ok)
		digest := v.Ref().Digest()
		b := digest[0]
		return h.HashByte(b)&listPattern == listPattern
	}
}

func makeListLeafChunkFn(t Type, cs chunks.ChunkStore) makeChunkFn {
	return func(items []sequenceItem) (sequenceItem, interface{}) {
		values := make([]Value, len(items))

		for i, v := range items {
			value, ok := v.(Value)
			d.Chk.True(ok)
			values[i] = value
		}

		concreteType := t.Desc.(CompoundDesc).ElemTypes[0]
		list := List{values, concreteType, &ref.Ref{}}
		ref := WriteValue(list, cs)
		return metaTuple{ref, UInt64(uint64(len(values)))}, list
	}
}

func NewCompoundList(t Type, cs chunks.ChunkStore, values ...Value) Value {
	seq := newSequenceChunker(makeListLeafChunkFn(t, cs), newMetaSequenceChunkFn(t, cs), listLeafIsBoundaryFn(), metaSequenceIsBoundaryFn())
	for _, v := range values {
		seq.Append(v)
	}
	_, l := seq.Done()
	return l.(Value)
}
