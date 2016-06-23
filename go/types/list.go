// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"crypto/sha1"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
)

const (
	// The window size to use for computing the rolling hash.
	listWindowSize = 64
	listPattern    = uint32(1<<6 - 1) // Average size of 64 elements
)

type List struct {
	seq indexedSequence
	h   *hash.Hash
}

func newList(seq indexedSequence) List {
	return List{seq, &hash.Hash{}}
}

// NewList creates a new List where the type is computed from the elements in the list, populated with values, chunking if and when needed.
func NewList(values ...Value) List {
	seq := newEmptySequenceChunker(makeListLeafChunkFn(nil, nil), newIndexedMetaSequenceChunkFn(ListKind, nil, nil), newListLeafBoundaryChecker(), newIndexedMetaSequenceBoundaryChecker)
	for _, v := range values {
		seq.Append(v)
	}
	return newList(seq.Done().(indexedSequence))
}

// NewStreamingList creates a new List with type t, populated with values, chunking if and when needed. As chunks are created, they're written to vrw -- including the root chunk of the list. Once the caller has closed values, she can read the completed List from the returned channel.
func NewStreamingList(vrw ValueReadWriter, values <-chan Value) <-chan List {
	out := make(chan List)
	go func() {
		seq := newEmptySequenceChunker(makeListLeafChunkFn(vrw, vrw), newIndexedMetaSequenceChunkFn(ListKind, vrw, vrw), newListLeafBoundaryChecker(), newIndexedMetaSequenceBoundaryChecker)
		for v := range values {
			seq.Append(v)
		}
		out <- newList(seq.Done().(indexedSequence))
		close(out)
	}()
	return out
}

// Collection interface
func (l List) Len() uint64 {
	return l.seq.numLeaves()
}

func (l List) Empty() bool {
	return l.Len() == 0
}

func (l List) sequence() sequence {
	return l.seq
}

func (l List) hashPointer() *hash.Hash {
	return l.h
}

// Value interface
func (l List) Equals(other Value) bool {
	return other != nil && l.Hash() == other.Hash()
}

func (l List) Less(other Value) bool {
	return valueLess(l, other)
}

func (l List) Hash() hash.Hash {
	if l.h.IsEmpty() {
		*l.h = getHash(l)
	}

	return *l.h
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

func (l List) Type() *Type {
	return l.seq.Type()
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
	return newList(ch.Done().(indexedSequence))
}

func (l List) Insert(idx uint64, vs ...Value) List {
	return l.Splice(idx, 0, vs...)
}

func (l List) Remove(start uint64, end uint64) List {
	d.Chk.True(start <= end)
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

func (l List) Diff(last List) []Splice {
	return l.DiffWithLimit(last, DEFAULT_MAX_SPLICE_MATRIX_SIZE)
}

func (l List) DiffWithLimit(last List, maxSpliceMatrixSize uint64) []Splice {
	if l.Equals(last) {
		return []Splice{} // nothing changed
	}
	lLen, lastLen := l.Len(), last.Len()
	if lLen == 0 {
		return []Splice{Splice{0, lastLen, 0, 0}} // everything removed
	}
	if lastLen == 0 {
		return []Splice{Splice{0, 0, lLen, 0}} // everything added
	}
	lastCur := newCursorAtIndex(last.seq, 0)
	lCur := newCursorAtIndex(l.seq, 0)
	return indexedSequenceDiff(last.seq, lastCur.depth(), 0, l.seq, lCur.depth(), 0, maxSpliceMatrixSize)
}

func newListLeafBoundaryChecker() boundaryChecker {
	return newBuzHashBoundaryChecker(listWindowSize, sha1.Size, listPattern, func(item sequenceItem) []byte {
		digest := item.(Value).Hash().Digest()
		return digest[:]
	})
}

// If |sink| is not nil, chunks will be eagerly written as they're created. Otherwise they are
// written when the root is written.
func makeListLeafChunkFn(vr ValueReader, sink ValueWriter) makeChunkFn {
	return func(items []sequenceItem) (metaTuple, sequence) {
		values := make([]Value, len(items))

		for i, v := range items {
			values[i] = v.(Value)
		}

		seq := newListLeafSequence(vr, values...)
		list := newList(seq)

		var ref Ref
		var child Collection
		if sink != nil {
			// Eagerly write chunks
			ref = sink.WriteValue(list)
			child = nil
		} else {
			ref = NewRef(list)
			child = list
		}

		return newMetaTuple(ref, orderedKeyFromInt(len(values)), uint64(len(values)), child), seq
	}
}
