package types

import (
	"crypto/sha1"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

const (
	// The window size to use for computing the rolling hash.
	listWindowSize = 64
	listPattern    = uint32(1<<6 - 1) // Average size of 64 elements
)

type compoundList struct {
	indexedMetaSequence
	length uint64
	ref    *ref.Ref
}

func buildCompoundList(tuples metaSequenceData, t *Type, vr ValueReader) metaSequence {
	return compoundList{
		indexedMetaSequence{
			metaSequenceObject{tuples, t, vr},
			computeIndexedSequenceOffsets(tuples),
		},
		tuples.uint64ValuesSum(),
		&ref.Ref{},
	}
}

func listAsSequenceItems(ls listLeaf) []sequenceItem {
	items := make([]sequenceItem, len(ls.values))
	for i, v := range ls.values {
		items[i] = v
	}
	return items
}

func init() {
	registerMetaValue(ListKind, buildCompoundList)
}

func (cl compoundList) Equals(other Value) bool {
	return other != nil && cl.t.Equals(other.Type()) && cl.Ref() == other.Ref()
}

func (cl compoundList) Ref() ref.Ref {
	return EnsureRef(cl.ref, cl)
}

func (cl compoundList) Len() uint64 {
	return cl.length
}

func (cl compoundList) Empty() bool {
	d.Chk.True(cl.Len() > 0) // A compound object should never be empty.
	return false
}

func (cl compoundList) Get(idx uint64) Value {
	cur := newCursorAtIndex(cl, idx)
	v, ok := cur.maybeCurrent()
	d.Chk.True(ok)
	return v.(Value)
}

func (cl compoundList) Slice(start uint64, end uint64) List {
	// See https://github.com/attic-labs/noms/issues/744 for a better Slice implementation.
	cur := newCursorAtIndex(cl, start)
	slice := make([]Value, 0, end-start)
	for i := start; i < end; i++ {
		if value, ok := cur.maybeCurrent(); ok {
			slice = append(slice, value.(Value))
		} else {
			break
		}
		cur.advance()
	}
	return NewTypedList(cl.t, slice...)
}

func (cl compoundList) Map(mf MapFunc) []interface{} {
	idx := uint64(0)
	cur := newCursorAtIndex(cl, idx)

	results := make([]interface{}, 0, cl.Len())
	cur.iter(func(v interface{}) bool {
		res := mf(v.(Value), uint64(idx))
		results = append(results, res)
		idx++
		return false
	})
	return results
}

func (cl compoundList) elemType() *Type {
	return cl.Type().Desc.(CompoundDesc).ElemTypes[0]
}

func (cl compoundList) Set(idx uint64, v Value) List {
	assertType(cl.elemType(), v)
	seq := cl.sequenceChunkerAtIndex(idx)
	seq.Skip()
	seq.Append(v)
	return seq.Done().(List)
}

func (cl compoundList) Append(vs ...Value) List {
	return cl.Insert(cl.Len(), vs...)
}

func (cl compoundList) Insert(idx uint64, vs ...Value) List {
	if len(vs) == 0 {
		return cl
	}

	assertType(cl.elemType(), vs...)

	seq := cl.sequenceChunkerAtIndex(idx)
	for _, v := range vs {
		seq.Append(v)
	}
	return seq.Done().(List)
}

func (cl compoundList) sequenceChunkerAtIndex(idx uint64) *sequenceChunker {
	cur := newCursorAtIndex(cl, idx)
	return newSequenceChunker(cur, makeListLeafChunkFn(cl.t, nil), newIndexedMetaSequenceChunkFn(cl.t, cl.vr, nil), newListLeafBoundaryChecker(), newIndexedMetaSequenceBoundaryChecker)
}

func (cl compoundList) Filter(cb listFilterCallback) List {
	seq := newEmptySequenceChunker(makeListLeafChunkFn(cl.t, nil), newIndexedMetaSequenceChunkFn(cl.t, cl.vr, nil), newListLeafBoundaryChecker(), newIndexedMetaSequenceBoundaryChecker)
	cl.IterAll(func(v Value, idx uint64) {
		if cb(v, idx) {
			seq.Append(v)
		}
	})
	return seq.Done().(List)
}

func (cl compoundList) Remove(start uint64, end uint64) List {
	if start == end {
		return cl
	}
	d.Chk.True(end > start)
	seq := cl.sequenceChunkerAtIndex(start)
	for i := start; i < end; i++ {
		seq.Skip()
	}
	return seq.Done().(List)
}

func (cl compoundList) RemoveAt(idx uint64) List {
	return cl.Remove(idx, idx+1)
}

func (cl compoundList) Iter(f listIterFunc) {
	idx := uint64(0)
	cur := newCursorAtIndex(cl, idx)
	cur.iter(func(v interface{}) bool {
		if f(v.(Value), idx) {
			return true
		}
		idx++
		return false
	})
}

func (cl compoundList) IterAll(f listIterAllFunc) {
	idx := uint64(0)
	cur := newCursorAtIndex(cl, idx)
	cur.iter(func(v interface{}) bool {
		f(v.(Value), uint64(idx))
		idx++
		return false
	})
}

func newListLeafBoundaryChecker() boundaryChecker {
	return newBuzHashBoundaryChecker(listWindowSize, sha1.Size, listPattern, func(item sequenceItem) []byte {
		digest := item.(Value).Ref().Digest()
		return digest[:]
	})
}

// If |sink| is not nil, chunks will be eagerly written as they're created. Otherwise they are
// written when the root is written.
func makeListLeafChunkFn(t *Type, sink ValueWriter) makeChunkFn {
	return func(items []sequenceItem) (sequenceItem, Value) {
		values := make([]Value, len(items))

		for i, v := range items {
			values[i] = v.(Value)
		}

		list := newListLeaf(t, values...)
		if sink != nil {
			return newMetaTuple(Number(len(values)), nil, sink.WriteValue(list), uint64(len(values))), list
		}
		return newMetaTuple(Number(len(values)), list, NewTypedRefFromValue(list), uint64(len(values))), list
	}
}
