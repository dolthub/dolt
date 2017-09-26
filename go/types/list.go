// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"sync/atomic"

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
}

func newList(seq sequence) List {
	return List{seq}
}

// NewList creates a new List where the type is computed from the elements in the list, populated
// with values, chunking if and when needed.
func NewList(vrw ValueReadWriter, values ...Value) List {
	ch := newEmptyListSequenceChunker(vrw)
	for _, v := range values {
		ch.Append(v)
	}
	return newList(ch.Done())
}

// NewStreamingList creates a new List, populated with values, chunking if and when needed. As
// chunks are created, they're written to vrw -- including the root chunk of the list. Once the
// caller has closed values, the caller can read the completed List from the returned channel.
func NewStreamingList(vrw ValueReadWriter, values <-chan Value) <-chan List {
	out := make(chan List, 1)
	go func() {
		defer close(out)
		ch := newEmptyListSequenceChunker(vrw)
		for v := range values {
			ch.Append(v)
		}
		out <- newList(ch.Done())
	}()
	return out
}

func (l List) Edit() *ListEditor {
	return NewListEditor(l)
}

// Collection interface

// Len returns the number of elements in the list.
func (l List) Len() uint64 {
	return l.seq.numLeaves()
}

// Empty returns true if the list is empty (length is zero).
func (l List) Empty() bool {
	// TODO: l.Len() is not free, use l.seq.seqLen()?
	return l.Len() == 0
}

func (l List) sequence() sequence {
	return l.seq
}

// Value interface
func (l List) Value() Value {
	return l
}

func (l List) Equals(other Value) bool {
	if otherList, ok := other.(List); ok {
		return l.sequence().equals(otherList.sequence())
	}
	return false
}

func (l List) Less(other Value) bool {
	return valueLess(l, other)
}

func (l List) Hash() hash.Hash {
	return l.sequence().hash()
}

func (l List) WalkValues(cb ValueCallback) {
	l.IterAll(func(v Value, idx uint64) {
		cb(v)
	})
}

func (l List) WalkRefs(cb RefCallback) {
	l.seq.WalkRefs(cb)
}

func (l List) typeOf() *Type {
	return l.seq.typeOf()
}

func (l List) Kind() NomsKind {
	return ListKind
}

// Get returns the value at the given index. If this list has been chunked then this will have to
// descend into the prolly-tree which leads to Get being O(depth).
func (l List) Get(idx uint64) Value {
	d.PanicIfFalse(idx < l.Len())
	cur := newCursorAtIndex(l.seq, idx, false)
	return cur.current().(Value)
}

// Concat returns a new List comprised of this joined with other. It only needs
// to visit the rightmost prolly tree chunks of this List, and the leftmost
// prolly tree chunks of other, so it's efficient.
func (l List) Concat(other List) List {
	seq := concat(l.seq, other.seq, func(cur *sequenceCursor, vrw ValueReadWriter) *sequenceChunker {
		return l.newChunker(cur, vrw)
	})
	return newList(seq)
}

type listIterFunc func(v Value, index uint64) (stop bool)

// Iter iterates over the list and calls f for every element in the list. If f returns true then the
// iteration stops.
func (l List) Iter(f listIterFunc) {
	idx := uint64(0)
	cur := newCursorAtIndex(l.seq, idx, false)
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
	concurrency := 6
	vcChan := make(chan chan []Value, concurrency)

	// Target reading data in |targetBatchBytes| per thread. We don't know how
	// many bytes each value is, so update |estimatedNumValues| as data is read.
	targetBatchBytes := 1 << 23 // 8MB
	estimatedNumValues := uint64(1000)

	go func() {
		for idx, llen := uint64(0), l.Len(); idx < llen; {
			numValues := atomic.LoadUint64(&estimatedNumValues)

			start := idx
			blockLength := llen - start
			if blockLength > numValues {
				blockLength = numValues
			}
			idx += blockLength

			vc := make(chan []Value)
			vcChan <- vc

			go func() {
				values := make([]Value, blockLength)
				numBytes := l.copyReadAhead(values, start)

				// Adjust the estimated number of values to try to read
				// |targetBatchBytes| next time.
				if numValues == blockLength {
					scale := float64(targetBatchBytes) / float64(numBytes)
					atomic.StoreUint64(&estimatedNumValues, uint64(float64(numValues)*scale))
				}

				// Send |values| to |vc| last so that adjusting |estimatedNumValues|
				// doesn't block.
				vc <- values
			}()
		}
		close(vcChan)
	}()

	// Ensure read-ahead goroutines can exit, because the `range` below might not
	// finish if an |f| callback panics.
	defer func() {
		for range vcChan {
		}
	}()

	i := uint64(0)
	for vc := range vcChan {
		for _, v := range <-vc {
			f(v, i)
			i++
		}
	}
}

func (l List) copyReadAhead(out []Value, startIdx uint64) (numBytes uint64) {
	llen := l.Len()
	d.PanicIfFalse(startIdx < llen)

	endIdx := startIdx + uint64(len(out))
	if endIdx > llen {
		endIdx = llen
	}

	if startIdx == endIdx {
		return
	}

	leaves, localStart := LoadLeafNodes([]Collection{l}, startIdx, endIdx)
	endIdx = localStart + endIdx - startIdx
	startIdx = localStart

	for _, leaf := range leaves {
		ls := leaf.sequence().(listLeafSequence)

		values := ls.valuesSlice(startIdx, endIdx)
		copy(out, values)
		out = out[len(values):]

		endIdx = endIdx - uint64(len(values)) - startIdx
		startIdx = 0
		numBytes += uint64(len(ls.buff))
	}
	return
}

// Iterator returns a ListIterator which can be used to iterate efficiently over a list.
func (l List) Iterator() ListIterator {
	return l.IteratorAt(0)
}

// IteratorAt returns a ListIterator starting at index. If index is out of bound the iterator will
// have reached its end on creation.
func (l List) IteratorAt(index uint64) ListIterator {
	return ListIterator{
		newCursorAtIndex(l.seq, index, false),
	}
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

	indexedSequenceDiff(last.seq, 0, l.seq, 0, changes, closeChan, maxSpliceMatrixSize)
}

func (l List) newChunker(cur *sequenceCursor, vrw ValueReadWriter) *sequenceChunker {
	return newSequenceChunker(cur, 0, vrw, makeListLeafChunkFn(vrw), newIndexedMetaSequenceChunkFn(ListKind, vrw), hashValueBytes)
}

func makeListLeafChunkFn(vrw ValueReadWriter) makeChunkFn {
	return func(level uint64, items []sequenceItem) (Collection, orderedKey, uint64) {
		d.PanicIfFalse(level == 0)
		values := make([]Value, len(items))

		for i, v := range items {
			values[i] = v.(Value)
		}

		list := newList(newListLeafSequence(vrw, values...))
		return list, orderedKeyFromInt(len(values)), uint64(len(values))
	}
}

func newEmptyListSequenceChunker(vrw ValueReadWriter) *sequenceChunker {
	return newEmptySequenceChunker(vrw, makeListLeafChunkFn(vrw), newIndexedMetaSequenceChunkFn(ListKind, vrw), hashValueBytes)
}

func (l List) valueReadWriter() ValueReadWriter {
	return l.seq.valueReadWriter()
}

func (l List) writeTo(w nomsWriter) {
	l.seq.writeTo(w)
}
