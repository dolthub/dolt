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

type List struct {
	seq indexedSequence
	ref *ref.Ref
}

var listType = MakeListType(ValueType)

func newList(seq indexedSequence) List {
	return List{seq, &ref.Ref{}}
}

// NewList creates a new untyped List, populated with values, chunking if and when needed.
func NewList(v ...Value) List {
	return NewTypedList(listType, v...)
}

// NewTypedList creates a new List with type t, populated with values, chunking if and when needed.
func NewTypedList(t *Type, values ...Value) List {
	d.Chk.Equal(ListKind, t.Kind(), "Invalid type. Expected: ListKind, found: %s", t.Describe())
	seq := newEmptySequenceChunker(makeListLeafChunkFn(t, nil, nil), newIndexedMetaSequenceChunkFn(t, nil, nil), newListLeafBoundaryChecker(), newIndexedMetaSequenceBoundaryChecker)
	for _, v := range values {
		seq.Append(v)
	}
	return seq.Done().(List)
}

// NewStreamingTypedList creates a new List with type t, populated with values, chunking if and when needed. As chunks are created, they're written to vrw -- including the root chunk of the list. Once the caller has closed values, she can read the completed List from the returned channel.
func NewStreamingTypedList(t *Type, vrw ValueReadWriter, values <-chan Value) <-chan List {
	out := make(chan List)
	go func() {
		seq := newEmptySequenceChunker(makeListLeafChunkFn(t, vrw, vrw), newIndexedMetaSequenceChunkFn(t, vrw, vrw), newListLeafBoundaryChecker(), newIndexedMetaSequenceBoundaryChecker)
		for v := range values {
			seq.Append(v)
		}
		out <- seq.Done().(List)
		close(out)
	}()
	return out
}

func (l List) Type() *Type {
	return l.seq.Type()
}

func (l List) Equals(other Value) bool {
	return other != nil && l.Ref() == other.Ref()
}

func (l List) Ref() ref.Ref {
	return EnsureRef(l.ref, l)
}

func (l List) Len() uint64 {
	return l.seq.numLeaves()
}

func (l List) Empty() bool {
	return l.Len() == 0
}

func (l List) ChildValues() (values []Value) {
	l.IterAll(func(v Value, idx uint64) {
		values = append(values, v)
	})
	return
}

func (l List) Chunks() []Ref {
	return l.seq.Chunks()
}

func (l List) sequence() sequence {
	return l.seq
}

func (l List) Get(idx uint64) Value {
	cur := newCursorAtIndex(l.seq, idx)
	return cur.current().(Value)
}

func (l List) Slice(start uint64, end uint64) List {
	// See https://github.com/attic-labs/noms/issues/744 for a better Slice implementation.
	cur := newCursorAtIndex(l.seq, start)
	slice := make([]Value, 0, end-start)
	for i := start; i < end; i++ {
		if !cur.valid() {
			break
		}

		slice = append(slice, cur.current().(Value))
		cur.advance()
	}
	return NewTypedList(l.seq.Type(), slice...)
}

type MapFunc func(v Value, index uint64) interface{}

func (l List) Map(mf MapFunc) []interface{} {
	idx := uint64(0)
	cur := newCursorAtIndex(l.seq, idx)

	results := make([]interface{}, 0, l.Len())
	cur.iter(func(v interface{}) bool {
		res := mf(v.(Value), uint64(idx))
		results = append(results, res)
		idx++
		return false
	})
	return results
}

func (l List) elemType() *Type {
	return l.seq.Type().Desc.(CompoundDesc).ElemTypes[0]
}

func (l List) Set(idx uint64, v Value) List {
	assertType(l.elemType(), v)
	seq := listSequenceChunkerAtIndex(l.seq, idx)
	seq.Skip()
	seq.Append(v)
	return seq.Done().(List)
}

func (l List) Append(vs ...Value) List {
	return l.Insert(l.Len(), vs...)
}

func (l List) Insert(idx uint64, vs ...Value) List {
	if len(vs) == 0 {
		return l
	}

	assertType(l.elemType(), vs...)

	seq := listSequenceChunkerAtIndex(l.seq, idx)
	for _, v := range vs {
		seq.Append(v)
	}
	return seq.Done().(List)
}

func listSequenceChunkerAtIndex(seq indexedSequence, idx uint64) *sequenceChunker {
	cur := newCursorAtIndex(seq, idx)
	return newSequenceChunker(cur, makeListLeafChunkFn(seq.Type(), seq.valueReader(), nil), newIndexedMetaSequenceChunkFn(seq.Type(), seq.valueReader(), nil), newListLeafBoundaryChecker(), newIndexedMetaSequenceBoundaryChecker)
}

type listFilterCallback func(v Value, index uint64) (keep bool)

func (l List) Filter(cb listFilterCallback) List {
	seq := l.seq
	ch := newEmptySequenceChunker(makeListLeafChunkFn(seq.Type(), l.seq.valueReader(), nil), newIndexedMetaSequenceChunkFn(seq.Type(), seq.valueReader(), nil), newListLeafBoundaryChecker(), newIndexedMetaSequenceBoundaryChecker)
	l.IterAll(func(v Value, idx uint64) {
		if cb(v, idx) {
			ch.Append(v)
		}
	})
	return ch.Done().(List)
}

func (l List) Remove(start uint64, end uint64) List {
	if start == end {
		return l
	}
	d.Chk.True(end > start)
	d.Chk.True(start < l.Len() && end <= l.Len())
	seq := listSequenceChunkerAtIndex(l.seq, start)
	for i := start; i < end; i++ {
		seq.Skip()
	}
	return seq.Done().(List)
}

func (l List) RemoveAt(idx uint64) List {
	return l.Remove(idx, idx+1)
}

type listIterFunc func(v Value, index uint64) (stop bool)

func (l List) Iter(f listIterFunc) {
	idx := uint64(0)
	cur := newCursorAtIndex(l.seq, idx)
	cur.iter(func(v interface{}) bool {
		if f(v.(Value), uint64(idx)) {
			return true
		}
		idx++
		return false
	})
}

type listIterAllFunc func(v Value, index uint64)

func (l List) IterAll(f listIterAllFunc) {
	idx := uint64(0)
	cur := newCursorAtIndex(l.seq, idx)
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
func makeListLeafChunkFn(t *Type, vr ValueReader, sink ValueWriter) makeChunkFn {
	return func(items []sequenceItem) (sequenceItem, Value) {
		values := make([]Value, len(items))

		for i, v := range items {
			values[i] = v.(Value)
		}

		list := newList(newListLeafSequence(t, vr, values...))
		if sink != nil {
			return newMetaTuple(Number(len(values)), nil, sink.WriteValue(list), uint64(len(values))), list
		}
		return newMetaTuple(Number(len(values)), list, NewTypedRefFromValue(list), uint64(len(values))), list
	}
}
