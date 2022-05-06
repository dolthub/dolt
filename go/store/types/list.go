// Copyright 2019 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"context"
	"io"
	"sync/atomic"

	"github.com/dolthub/dolt/go/store/atomicerr"

	"github.com/dolthub/dolt/go/store/d"
)

var EmptyList List

// List represents a list or an array of Noms values. A list can contain zero or more values of zero
// or more types. The type of the list will reflect the type of the elements in the list. For
// example:
//
//  l := NewList(Float(1), Bool(true))
//  fmt.Println(l.Type().Describe())
//  // outputs List<Bool | Float>
//
// Lists, like all Noms values are immutable so the "mutation" methods return a new list.
type List struct {
	sequence
}

func newList(seq sequence) List {
	return List{seq}
}

// NewList creates a new List where the type is computed from the elements in the list, populated
// with values, chunking if and when needed.
func NewList(ctx context.Context, vrw ValueReadWriter, values ...Value) (List, error) {
	ch, err := newEmptyListSequenceChunker(ctx, vrw)

	if err != nil {
		return EmptyList, err
	}

	for _, v := range values {
		_, err := ch.Append(ctx, v)

		if err != nil {
			return EmptyList, err
		}
	}
	seq, err := ch.Done(ctx)

	if err != nil {
		return EmptyList, err
	}

	return newList(seq), nil
}

func (l List) ToSet(ctx context.Context) (Set, error) {
	s, err := NewSet(ctx, l.valueReadWriter())
	if err != nil {
		return Set{}, err
	}
	e := s.Edit()
	err = l.IterAll(ctx, func(v Value, idx uint64) error {
		se, err := e.Insert(v)
		e = se
		return err
	})
	if err != nil {
		return Set{}, err
	}
	return e.Set(ctx)
}

// NewStreamingList creates a new List, populated with values, chunking if and when needed. As
// chunks are created, they're written to vrw -- including the root chunk of the list. Once the
// caller has closed values, the caller can read the completed List from the returned channel.
func NewStreamingList(ctx context.Context, vrw ValueReadWriter, ae *atomicerr.AtomicError, values <-chan Value) <-chan List {
	out := make(chan List, 1)
	go func() {
		defer close(out)
		ch, err := newEmptyListSequenceChunker(ctx, vrw)

		if ae.SetIfError(err) {
			return
		}

		for v := range values {
			_, err := ch.Append(ctx, v)

			if ae.SetIfError(err) {
				return
			}
		}

		seq, err := ch.Done(ctx)

		if ae.SetIfError(err) {
			return
		}

		out <- newList(seq)
	}()
	return out
}

func (l List) Edit() *ListEditor {
	return NewListEditor(l)
}

// Collection interface

func (l List) asSequence() sequence {
	return l.sequence
}

// Value interface
func (l List) Value(ctx context.Context) (Value, error) {
	return l, nil
}

// Get returns the value at the given index. If this list has been chunked then this will have to
// descend into the prolly-tree which leads to Get being O(depth).
func (l List) Get(ctx context.Context, idx uint64) (Value, error) {
	d.PanicIfFalse(idx < l.Len())
	cur, err := newSequenceIteratorAtIndex(ctx, l.sequence, idx)

	if err != nil {
		return nil, err
	}

	currItem, err := cur.current()

	if err != nil {
		return nil, err
	}

	return currItem.(Value), nil
}

// Concat returns a new List comprised of this joined with other. It only needs
// to visit the rightmost prolly tree chunks of this List, and the leftmost
// prolly tree chunks of other, so it's efficient.
func (l List) Concat(ctx context.Context, other List) (List, error) {
	seq, err := concat(ctx, l.sequence, other.sequence, func(cur *sequenceCursor, vrw ValueReadWriter) (*sequenceChunker, error) {
		return l.newChunker(ctx, cur, vrw)
	})

	if err != nil {
		return EmptyList, err
	}

	return newList(seq), nil
}

func (l List) isPrimitive() bool {
	return false
}

// Iter iterates over the list and calls f for every element in the list. If f returns true then the
// iteration stops.
func (l List) Iter(ctx context.Context, f func(v Value, index uint64) (stop bool, err error)) error {
	idx := uint64(0)
	cur, err := newSequenceIteratorAtIndex(ctx, l.sequence, idx)

	if err != nil {
		return err
	}

	err = cur.iter(ctx, func(v interface{}) (bool, error) {
		stop, err := f(v.(Value), idx)
		idx++
		return stop, err
	})

	return err
}

func (l List) IterRange(ctx context.Context, startIdx, endIdx uint64, f func(v Value, idx uint64) error) error {
	idx := uint64(startIdx)
	cb := func(v Value) error {
		err := f(v, idx)

		if err != nil {
			return err
		}

		idx++
		return nil
	}

	_, err := iterRange(ctx, l, startIdx, endIdx, cb)

	if err != nil {
		return err
	}

	return nil
}

// IterAll iterates over the list and calls f for every element in the list. Unlike Iter there is no
// way to stop the iteration and all elements are visited.
func (l List) IterAll(ctx context.Context, f func(v Value, index uint64) error) error {
	return iterAll(ctx, l, f)
}

func iterAll(ctx context.Context, col Collection, f func(v Value, index uint64) error) error {
	concurrency := 6
	vcChan := make(chan chan Value, concurrency)

	// Target reading data in |targetBatchBytes| per thread. We don't know how
	// many bytes each value is, so update |estimatedNumValues| as data is read.
	targetBatchBytes := 1 << 23 // 8MB
	estimatedNumValues := uint64(1000)

	ae := atomicerr.New()
	go func() {
		defer close(vcChan)
		for idx, l := uint64(0), col.Len(); idx < l; {
			if ae.IsSet() {
				return
			}

			numValues := atomic.LoadUint64(&estimatedNumValues)

			start := idx
			blockLength := l - start
			if blockLength > numValues {
				blockLength = numValues
			}
			idx += blockLength

			vc := make(chan Value)
			vcChan <- vc

			go func() {
				defer close(vc)

				numBytes, iterErr := iterRange(ctx, col, start, start+blockLength, func(v Value) error {
					vc <- v
					return nil
				})

				if ae.SetIfError(iterErr) {
					return
				}

				// Adjust the estimated number of values to try to read
				// |targetBatchBytes| next time.
				if numValues == blockLength {
					scale := float64(targetBatchBytes) / float64(numBytes)
					atomic.StoreUint64(&estimatedNumValues, uint64(float64(numValues)*scale))
				}
			}()
		}
	}()

	i := uint64(0)
	for vc := range vcChan {
		for v := range vc {
			if ae.IsSet() {
				continue // drain
			}

			err := f(v, i)
			ae.SetIfError(err)

			i++
		}
	}

	return ae.Get()
}

type collTupleRangeIter struct {
	leaves         []Collection
	currLeaf       int
	startIdx       uint64
	endIdx         uint64
	valsPerIdx     uint64
	currLeafValues []Tuple
	leafValPos     int
	nbf            *NomsBinFormat
	tupleBuffer    []Tuple
}

func (itr *collTupleRangeIter) Next() (Tuple, error) {
	var err error
	if itr.currLeafValues == nil {
		if itr.currLeaf >= len(itr.leaves) {
			// reached the end
			return Tuple{}, io.EOF
		}

		currLeaf := itr.leaves[itr.currLeaf]
		itr.currLeaf++

		seq := currLeaf.asSequence()
		itr.tupleBuffer, err = seq.kvTuples(itr.startIdx, itr.endIdx, itr.tupleBuffer)

		if err != nil {
			return Tuple{}, err
		}

		itr.currLeafValues = itr.tupleBuffer
		itr.leafValPos = 0
	}

	v := itr.currLeafValues[itr.leafValPos]
	itr.leafValPos++

	if itr.leafValPos >= len(itr.currLeafValues) {
		itr.endIdx = itr.endIdx - uint64(len(itr.currLeafValues))/itr.valsPerIdx - itr.startIdx
		itr.startIdx = 0
		itr.currLeafValues = nil
	}

	return v, nil
}

func newCollRangeIter(ctx context.Context, col Collection, startIdx, endIdx uint64) (*collTupleRangeIter, error) {
	l := col.Len()
	d.PanicIfTrue(startIdx > endIdx || endIdx > l)
	if startIdx == endIdx {
		return nil, nil
	}

	leaves, localStart, err := LoadLeafNodes(ctx, []Collection{col}, startIdx, endIdx)

	if err != nil {
		return nil, err
	}

	endIdx = localStart + endIdx - startIdx
	startIdx = localStart
	valuesPerIdx := uint64(getValuesPerIdx(col.Kind()))

	return &collTupleRangeIter{
		leaves:      leaves,
		startIdx:    startIdx,
		endIdx:      endIdx,
		valsPerIdx:  valuesPerIdx,
		tupleBuffer: make([]Tuple, 32),
		nbf:         col.asSequence().format(),
	}, nil
}

func iterRange(ctx context.Context, col Collection, startIdx, endIdx uint64, cb func(v Value) error) (uint64, error) {
	l := col.Len()
	d.PanicIfTrue(startIdx > endIdx || endIdx > l)
	if startIdx == endIdx {
		return 0, nil
	}

	leaves, localStart, err := LoadLeafNodes(ctx, []Collection{col}, startIdx, endIdx)

	if err != nil {
		return 0, err
	}

	endIdx = localStart + endIdx - startIdx
	startIdx = localStart
	numValues := 0
	valuesPerIdx := uint64(getValuesPerIdx(col.Kind()))

	var numBytes uint64
	for _, leaf := range leaves {
		seq := leaf.asSequence()
		values, err := seq.valuesSlice(startIdx, endIdx)

		if err != nil {
			return 0, err
		}

		numValues += len(values)

		for _, v := range values {
			err := cb(v)

			if err != nil {
				return 0, err
			}
		}

		endIdx = endIdx - uint64(len(values))/valuesPerIdx - startIdx
		startIdx = 0

		w := binaryNomsWriter{make([]byte, 4), 0}
		err = seq.writeTo(&w, seq.format())

		if err != nil {
			return 0, err
		}

		numBytes += uint64(w.offset) // note: should really only include |values|
	}

	return numBytes, nil
}

// Iterator returns a ListIterator which can be used to iterate efficiently over a list.
func (l List) Iterator(ctx context.Context) (ListIterator, error) {
	return l.IteratorAt(ctx, 0)
}

// IteratorAt returns a ListIterator starting at index. If index is out of bound the iterator will
// have reached its end on creation.
func (l List) IteratorAt(ctx context.Context, index uint64) (ListIterator, error) {
	cur, err := newSequenceIteratorAtIndex(ctx, l.sequence, index)

	if err != nil {
		return ListIterator{}, err
	}

	return ListIterator{cur}, err
}

func (l List) Format() *NomsBinFormat {
	return l.format()
}

// Diff streams the diff from last to the current list to the changes channel. Caller can close
// closeChan to cancel the diff operation.
func (l List) Diff(ctx context.Context, last List, changes chan<- Splice) error {
	return l.DiffWithLimit(ctx, last, changes, DEFAULT_MAX_SPLICE_MATRIX_SIZE)
}

// DiffWithLimit streams the diff from last to the current list to the changes channel. Caller can
// close closeChan to cancel the diff operation.
// The maxSpliceMatrixSize determines the how big of an edit distance matrix we are willing to
// compute versus just saying the thing changed.
func (l List) DiffWithLimit(ctx context.Context, last List, changes chan<- Splice, maxSpliceMatrixSize uint64) error {
	if l.Equals(last) {
		return nil
	}
	lLen, lastLen := l.Len(), last.Len()
	if lLen == 0 {
		changes <- Splice{0, lastLen, 0, 0} // everything removed
		return nil
	}
	if lastLen == 0 {
		changes <- Splice{0, 0, lLen, 0} // everything added
		return nil
	}

	return indexedSequenceDiff(ctx, last.sequence, 0, l.sequence, 0, changes, maxSpliceMatrixSize)
}

func (l List) newChunker(ctx context.Context, cur *sequenceCursor, vrw ValueReadWriter) (*sequenceChunker, error) {
	makeChunk := makeListLeafChunkFn(vrw)
	makeParentChunk := newIndexedMetaSequenceChunkFn(ListKind, vrw)
	return newSequenceChunker(ctx, cur, 0, vrw, makeChunk, makeParentChunk, newListChunker, hashValueBytes)
}

func newListChunker(nbf *NomsBinFormat, salt byte) sequenceSplitter {
	return newRollingValueHasher(nbf, salt)
}

func makeListLeafChunkFn(vrw ValueReadWriter) makeChunkFn {
	return func(level uint64, items []sequenceItem) (Collection, orderedKey, uint64, error) {
		d.PanicIfFalse(level == 0)
		values := make([]Value, len(items))

		for i, v := range items {
			values[i] = v.(Value)
		}

		listSeq, err := newListLeafSequence(vrw, values...)

		if err != nil {
			return nil, orderedKey{}, 0, err
		}

		ordKey, err := orderedKeyFromInt(len(values), vrw.Format())

		if err != nil {
			return nil, orderedKey{}, 0, err
		}

		return newList(listSeq), ordKey, uint64(len(values)), nil
	}
}

func newEmptyListSequenceChunker(ctx context.Context, vrw ValueReadWriter) (*sequenceChunker, error) {
	return newEmptySequenceChunker(ctx, vrw, makeListLeafChunkFn(vrw), newIndexedMetaSequenceChunkFn(ListKind, vrw), newListChunker, hashValueBytes)
}

func (l List) readFrom(nbf *NomsBinFormat, b *binaryNomsReader) (Value, error) {
	panic("unreachable")
}

func (l List) skip(nbf *NomsBinFormat, b *binaryNomsReader) {
	panic("unreachable")
}

func (l List) String() string {
	panic("unreachable")
}

func (l List) HumanReadableString() string {
	panic("unreachable")
}
