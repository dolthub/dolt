// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
)

// List represents a list or an array of Noms values. A list can contain zero or more values of zero
// or more types. The type of the list will reflect the type of the elements in the list. For
// example:
//
//  l := NewList(Number(1), Bool(true))
//  fmt.Println(l.Type().Describe())
//  // outputs List<Bool | Number>
//
// Lists, like all Noms values are immutable so the "mutation" methods return a new list.
type List struct {
	seq sequence
	h   *hash.Hash
}

func newList(seq sequence) List {
	return List{seq, &hash.Hash{}}
}

// NewList creates a new List where the type is computed from the elements in the list, populated
// with values, chunking if and when needed.
func NewList(values ...Value) List {
	ch := newEmptyListSequenceChunker(nil, nil)
	for _, v := range values {
		ch.Append(v)
	}
	return newList(ch.Done())
}

// NewStreamingList creates a new List, populated with values, chunking if and when needed. As
// chunks are created, they're written to vrw -- including the root chunk of the list. Once the
// caller has closed values, the caller can read the completed List from the returned channel.
func NewStreamingList(vrw ValueReadWriter, values <-chan Value) <-chan List {
	out := make(chan List)
	go func() {
		ch := newEmptyListSequenceChunker(vrw, vrw)
		for v := range values {
			ch.Append(v)
		}
		out <- newList(ch.Done())
		close(out)
	}()
	return out
}

// Collection interface

// Len returns the number of elements in the list.
func (l List) Len() uint64 {
	return l.seq.numLeaves()
}

// Empty returns true if the list is empty (length is zero).
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
	return l.Hash() == other.Hash()
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

func (l List) WalkValues(cb ValueCallback) {
	l.IterAll(func(v Value, idx uint64) {
		cb(v)
	})
}

func (l List) WalkRefs(cb RefCallback) {
	l.seq.WalkRefs(cb)
}

func (l List) Type() *Type {
	return l.seq.Type()
}

// Get returns the value at the given index. If this list has been chunked then this will have to
// descend into the prolly-tree which leads to Get being O(depth).
func (l List) Get(idx uint64) Value {
	d.PanicIfFalse(idx < l.Len())
	cur := newCursorAtIndex(l.seq, idx)
	return cur.current().(Value)
}

type MapFunc func(v Value, index uint64) interface{}

// Deprecated: This API may change in the future. Use IterAll or Iterator instead.
func (l List) Map(mf MapFunc) []interface{} {
	// TODO: This is bad API. It should have returned another List.
	// https://github.com/attic-labs/noms/issues/2557
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

// Set returns a new list where the valie at the given index have been replaced with v. If idx is
// out bounds then this panics.
func (l List) Set(idx uint64, v Value) List {
	d.PanicIfFalse(idx < l.Len())
	return l.Splice(idx, 1, v)
}

// Append returns a new list where vs have been appended to the resulting list.
func (l List) Append(vs ...Value) List {
	return l.Splice(l.Len(), 0, vs...)
}

// Splice returns a new list where deleteCount values have been removed at idx and vs have been
// inserted instead.
// This function panics if idx or deleteCount is out of bounds.
func (l List) Splice(idx uint64, deleteCount uint64, vs ...Value) List {
	if deleteCount == 0 && len(vs) == 0 {
		return l
	}

	d.PanicIfFalse(idx <= l.Len())
	d.PanicIfFalse(idx+deleteCount <= l.Len())

	cur := newCursorAtIndex(l.seq, idx)
	ch := l.newChunker(cur, l.seq.valueReader())
	for deleteCount > 0 {
		ch.Skip()
		deleteCount--
	}

	for _, v := range vs {
		ch.Append(v)
	}
	return newList(ch.Done())
}

// Insert returns a new list where vs values have been inserted at idx.
func (l List) Insert(idx uint64, vs ...Value) List {
	return l.Splice(idx, 0, vs...)
}

// Concat returns a new List comprised of this joined with other. It only needs
// to visit the rightmost prolly tree chunks of this List, and the leftmost
// prolly tree chunks of other, so it's efficient.
func (l List) Concat(other List) List {
	seq := concat(l.seq, other.seq, func(cur *sequenceCursor, vr ValueReader) *sequenceChunker {
		return l.newChunker(cur, vr)
	})
	return newList(seq)
}

// Remove returns a new list where the items at index start (inclusive) through end (exclusive) have
// been removed. This panics if end is smaller than start.
func (l List) Remove(start uint64, end uint64) List {
	d.PanicIfFalse(start <= end)
	return l.Splice(start, end-start)
}

// RemoveAt returns a new list where a single element at index idx have been removed.
func (l List) RemoveAt(idx uint64) List {
	return l.Splice(idx, 1)
}

type listIterFunc func(v Value, index uint64) (stop bool)

// Iter iterates over the list and calls f for every element in the list. If f returns true then the
// iteration stops.
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

// IterAll iterates over the list and calls f for every element in the list. Unlike Iter there is no
// way to stop the iteration and all elements are visited.
func (l List) IterAll(f listIterAllFunc) {
	// TODO: Consider removing this and have Iter behave like IterAll.
	// https://github.com/attic-labs/noms/issues/2558
	idx := uint64(0)
	cur := newCursorAtIndex(l.seq, idx)
	cur.iter(func(v interface{}) bool {
		f(v.(Value), uint64(idx))
		idx++
		return false
	})
}

// Iterator returns a ListIterator which can be used to iterate efficiently over a list.
func (l List) Iterator() ListIterator {
	return l.IteratorAt(0)
}

// IteratorAt returns a ListIterator starting at index. If index is out of bound the iterator will
// have reached its end on creation.
func (l List) IteratorAt(index uint64) ListIterator {
	return ListIterator{newCursorAtIndex(l.seq, index)}
}

// Diff streams the diff from last to the current list to the changes channel. Caller can close
// closeChan to cancel the diff operation.
func (l List) Diff(last List, changes chan<- Splice, closeChan <-chan struct{}) {
	l.DiffWithLimit(last, changes, closeChan, DEFAULT_MAX_SPLICE_MATRIX_SIZE)
}

// DiffWithLimit streams the diff from last to the current list to the changes channel. Caller can
// close closeChan to cancel the diff operation.
// The maxSpliceMatrixSize determines the how big of an edit distance matrix we are willing to
// compute versus just saying the thing changed.
func (l List) DiffWithLimit(last List, changes chan<- Splice, closeChan <-chan struct{}, maxSpliceMatrixSize uint64) {
	if l.Equals(last) {
		return
	}
	lLen, lastLen := l.Len(), last.Len()
	if lLen == 0 {
		changes <- Splice{0, lastLen, 0, 0} // everything removed
		return
	}
	if lastLen == 0 {
		changes <- Splice{0, 0, lLen, 0} // everything added
		return
	}

	lastCur := newCursorAtIndex(last.seq, 0)
	lCur := newCursorAtIndex(l.seq, 0)
	indexedSequenceDiff(last.seq, lastCur.depth(), 0, l.seq, lCur.depth(), 0, changes, closeChan, maxSpliceMatrixSize)
}

func (l List) newChunker(cur *sequenceCursor, vr ValueReader) *sequenceChunker {
	return newSequenceChunker(cur, vr, nil, makeListLeafChunkFn(vr), newIndexedMetaSequenceChunkFn(ListKind, vr), hashValueBytes)
}

// If |sink| is not nil, chunks will be eagerly written as they're created. Otherwise they are
// written when the root is written.
func makeListLeafChunkFn(vr ValueReader) makeChunkFn {
	return func(items []sequenceItem) (Collection, orderedKey, uint64) {
		values := make([]Value, len(items))

		for i, v := range items {
			values[i] = v.(Value)
		}

		list := newList(newListLeafSequence(vr, values...))
		return list, orderedKeyFromInt(len(values)), uint64(len(values))
	}
}

func newEmptyListSequenceChunker(vr ValueReader, vw ValueWriter) *sequenceChunker {
	return newEmptySequenceChunker(vr, vw, makeListLeafChunkFn(vr), newIndexedMetaSequenceChunkFn(ListKind, vr), hashValueBytes)
}
