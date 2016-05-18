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

func newList(seq indexedSequence) List {
	return List{seq, &ref.Ref{}}
}

// NewList creates a new List where the type is computed from the elements in the list, populated with values, chunking if and when needed.
func NewList(values ...Value) List {
	seq := newEmptySequenceChunker(makeListLeafChunkFn(nil, nil), newIndexedMetaSequenceChunkFn(ListKind, nil, nil), newListLeafBoundaryChecker(), newIndexedMetaSequenceBoundaryChecker)
	for _, v := range values {
		seq.Append(v)
	}
	return seq.Done().(List)
}

// NewStreamingList creates a new List with type t, populated with values, chunking if and when needed. As chunks are created, they're written to vrw -- including the root chunk of the list. Once the caller has closed values, she can read the completed List from the returned channel.
func NewStreamingList(vrw ValueReadWriter, values <-chan Value) <-chan List {
	out := make(chan List)
	go func() {
		seq := newEmptySequenceChunker(makeListLeafChunkFn(vrw, vrw), newIndexedMetaSequenceChunkFn(ListKind, vrw, vrw), newListLeafBoundaryChecker(), newIndexedMetaSequenceBoundaryChecker)
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

func (l List) Less(other Value) bool {
	return valueLess(l, other)
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
	d.Chk.True(idx < l.Len())
	cur := newCursorAtIndex(l.seq, idx)
	return cur.current().(Value)
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
	d.Chk.True(idx < l.Len())
	return l.Splice(idx, 1, v)
}

func (l List) Append(vs ...Value) List {
	return l.Splice(l.Len(), 0, vs...)
}

func (l List) Splice(idx uint64, deleteCount uint64, vs ...Value) List {
	if deleteCount == 0 && len(vs) == 0 {
		return l
	}

	d.Chk.True(idx <= l.Len())
	d.Chk.True(idx+deleteCount <= l.Len())

	cur := newCursorAtIndex(l.seq, idx)
	ch := newSequenceChunker(cur, makeListLeafChunkFn(l.seq.valueReader(), nil), newIndexedMetaSequenceChunkFn(ListKind, l.seq.valueReader(), nil), newListLeafBoundaryChecker(), newIndexedMetaSequenceBoundaryChecker)
	for deleteCount > 0 {
		ch.Skip()
		deleteCount--
	}

	for _, v := range vs {
		ch.Append(v)
	}
	return ch.Done().(List)
}

func (l List) Insert(idx uint64, vs ...Value) List {
	return l.Splice(idx, 0, vs...)
}

func (l List) Remove(start uint64, end uint64) List {
	return l.Splice(start, end-start)
}

func (l List) RemoveAt(idx uint64) List {
	return l.Splice(idx, 1)
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
func makeListLeafChunkFn(vr ValueReader, sink ValueWriter) makeChunkFn {
	return func(items []sequenceItem) (metaTuple, Collection) {
		values := make([]Value, len(items))

		for i, v := range items {
			values[i] = v.(Value)
		}

		list := newList(newListLeafSequence(vr, values...))
		if sink != nil {
			return newMetaTuple(Number(len(values)), nil, sink.WriteValue(list), uint64(len(values))), list
		}
		return newMetaTuple(Number(len(values)), list, NewRef(list), uint64(len(values))), list
	}
}
