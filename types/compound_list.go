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

func (cl compoundList) cursorAt(idx uint64) (cursor *metaSequenceCursor, start uint64) {
	d.Chk.True(idx < cl.Len())
	cursor = newMetaSequenceCursor(cl, cl.cs)

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

	return cursor, uint64(chunkStart.(UInt64))
}

func (cl compoundList) Get(idx uint64) Value {
	cursor, start := cl.cursorAt(idx)
	l := ReadValue(cursor.current().ref, cl.cs).(List)
	return l.Get(idx - start)
}

func (cl compoundList) Iter(f listIterFunc) {
	cursor := newMetaSequenceCursor(cl, cl.cs)
	start := uint64(0)
	first := true

	for first || cursor.advance() {
		l := ReadValue(cursor.current().ref, cl.cs).(List)
		for i, v := range l.values {
			if f(v, start+uint64(i)) {
				return
			}
		}
		start += l.Len()
		first = false
	}
}

func (cl compoundList) IterAll(f listIterAllFunc) {
	cursor := newMetaSequenceCursor(cl, cl.cs)
	start := uint64(0)
	first := true

	for first || cursor.advance() {
		l := ReadValue(cursor.current().ref, cl.cs).(List)
		for i, v := range l.values {
			f(v, start+uint64(i))
		}
		start += l.Len()
		first = false
	}
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
