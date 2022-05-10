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
	"fmt"

	"github.com/dolthub/dolt/go/store/atomicerr"
	"github.com/dolthub/dolt/go/store/d"
)

var EmptySet Set

type Set struct {
	orderedSequence
}

func newSet(seq orderedSequence) Set {
	return Set{seq}
}

func NewSet(ctx context.Context, vrw ValueReadWriter, v ...Value) (Set, error) {
	data := buildSetData(vrw.Format(), v)
	ch, err := newEmptySetSequenceChunker(ctx, vrw)

	if err != nil {
		return EmptySet, err
	}

	for _, v := range data {
		_, err := ch.Append(ctx, v)

		if err != nil {
			return EmptySet, err
		}
	}

	seq, err := ch.Done(ctx)

	if err != nil {
		return EmptySet, err
	}

	return newSet(seq.(orderedSequence)), nil
}

// NewStreamingSet takes an input channel of values and returns a output
// channel that will produce a finished Set. Values that are sent to the input
// channel must be in Noms sortorder, adding values to the input channel
// out of order will result in a panic. Once the input channel is closed
// by the caller, a finished Set will be sent to the output channel. See
// graph_builder.go for building collections with values that are not in order.
func NewStreamingSet(ctx context.Context, vrw ValueReadWriter, ae *atomicerr.AtomicError, vChan <-chan Value) <-chan Set {
	return newStreamingSet(vrw, vChan, func(vrw ValueReadWriter, vChan <-chan Value, outChan chan<- Set) {
		go readSetInput(ctx, vrw, ae, vChan, outChan)
	})
}

type streamingSetReadFunc func(vrw ValueReadWriter, vChan <-chan Value, outChan chan<- Set)

func newStreamingSet(vrw ValueReadWriter, vChan <-chan Value, readFunc streamingSetReadFunc) <-chan Set {
	d.PanicIfTrue(vrw == nil)
	outChan := make(chan Set, 1)
	readFunc(vrw, vChan, outChan)
	return outChan
}

func readSetInput(ctx context.Context, vrw ValueReadWriter, ae *atomicerr.AtomicError, vChan <-chan Value, outChan chan<- Set) {
	defer close(outChan)

	ch, err := newEmptySetSequenceChunker(ctx, vrw)
	if ae.SetIfError(err) {
		return
	}

	var lastV Value
	for v := range vChan {
		if lastV != nil {
			isLess, err := lastV.Less(vrw.Format(), v)

			if ae.SetIfErrAndCheck(err) {
				return
			}

			d.PanicIfFalse(isLess)
		}
		lastV = v
		_, err := ch.Append(ctx, v)

		if ae.SetIfError(err) {
			return
		}
	}
	seq, err := ch.Done(ctx)

	if ae.SetIfError(err) {
		return
	}

	outChan <- newSet(seq.(orderedSequence))
}

// Diff computes the diff from |last| to |m| using the top-down algorithm,
// which completes as fast as possible while taking longer to return early
// results than left-to-right.
func (s Set) Diff(ctx context.Context, last Set, changes chan<- ValueChanged) error {
	if s.Equals(last) {
		return nil
	}
	return orderedSequenceDiffLeftRight(ctx, last.orderedSequence, s.orderedSequence, changes)
}

// DiffLeftRight computes the diff from |last| to |s| using a left-to-right
// streaming approach, optimised for returning results early, but not
// completing quickly.
func (s Set) DiffLeftRight(ctx context.Context, last Set, changes chan<- ValueChanged) error {
	if s.Equals(last) {
		return nil
	}
	return orderedSequenceDiffLeftRight(ctx, last.orderedSequence, s.orderedSequence, changes)
}

func (s Set) asSequence() sequence {
	return s.orderedSequence
}

// Value interface
func (s Set) Value(ctx context.Context) (Value, error) {
	return s, nil
}

func (s Set) First(ctx context.Context) (Value, error) {
	cur, err := newCursorAt(ctx, s.orderedSequence, emptyKey, false, false)

	if err != nil {
		return nil, err
	}

	if !cur.valid() {
		return nil, nil
	}

	item, err := cur.current()

	if err != nil {
		return nil, err
	}

	return item.(Value), nil
}

func (s Set) At(ctx context.Context, idx uint64) (Value, error) {
	if idx >= s.Len() {
		panic(fmt.Errorf("out of bounds: %d >= %d", idx, s.Len()))
	}

	cur, err := newSequenceIteratorAtIndex(ctx, s.orderedSequence, idx)

	if err != nil {
		return nil, err
	}

	item, err := cur.current()

	if err != nil {
		return nil, err
	}

	return item.(Value), nil
}

func (s Set) Has(ctx context.Context, v Value) (bool, error) {
	cur, err := newCursorAtValue(ctx, s.orderedSequence, v, false, false)

	if err != nil {
		return false, err
	}

	if !cur.valid() {
		return false, nil
	}

	item, err := cur.current()

	if err != nil {
		return false, err
	}

	return item.(Value).Equals(v), nil
}

type setIterCallback func(v Value) (bool, error)

func (s Set) isPrimitive() bool {
	return false
}

func (s Set) Iter(ctx context.Context, cb setIterCallback) error {
	cur, err := newCursorAt(ctx, s.orderedSequence, emptyKey, false, false)

	if err != nil {
		return err
	}

	return cur.iter(ctx, func(v interface{}) (bool, error) {
		return cb(v.(Value))
	})
}

type setIterAllCallback func(v Value) error

func (s Set) IterAll(ctx context.Context, cb setIterAllCallback) error {
	return iterAll(ctx, s, func(v Value, idx uint64) error {
		return cb(v)
	})
}

func (s Set) Iterator(ctx context.Context) (SetIterator, error) {
	return s.IteratorAt(ctx, 0)
}

func (s Set) IteratorAt(ctx context.Context, idx uint64) (SetIterator, error) {
	cur, err := newSequenceIteratorAtIndex(ctx, s.orderedSequence, idx)

	if err != nil {
		return nil, err
	}

	return &setIterator{
		sequenceIter: cur,
		s:            s,
	}, nil
}

func (s Set) IteratorFrom(ctx context.Context, val Value) (SetIterator, error) {
	cur, err := newCursorAtValue(ctx, s.orderedSequence, val, false, false)

	if err != nil {
		return nil, err
	}

	return &setIterator{sequenceIter: cur, s: s}, nil
}

func (s Set) Format() *NomsBinFormat {
	return s.format()
}

func (s Set) Edit() *SetEditor {
	return NewSetEditor(s)
}

func buildSetData(nbf *NomsBinFormat, values ValueSlice) ValueSlice {
	if len(values) == 0 {
		return ValueSlice{}
	}

	SortWithErroringLess(ValueSort{values, nbf})

	uniqueSorted := make(ValueSlice, 0, len(values))
	last := values[0]
	for i := 1; i < len(values); i++ {
		v := values[i]
		if !v.Equals(last) {
			uniqueSorted = append(uniqueSorted, last)
		}
		last = v
	}

	return append(uniqueSorted, last)
}

func makeSetLeafChunkFn(vrw ValueReadWriter) makeChunkFn {
	return func(level uint64, items []sequenceItem) (Collection, orderedKey, uint64, error) {
		d.PanicIfFalse(level == 0)
		setData := make([]Value, len(items))

		var lastValue Value
		for i, item := range items {
			v := item.(Value)

			if lastValue != nil {
				isLess, err := lastValue.Less(vrw.Format(), v)

				if err != nil {
					return nil, orderedKey{}, 0, err
				}

				d.PanicIfFalse(isLess)
			}
			lastValue = v
			setData[i] = v
		}

		seq, err := newSetLeafSequence(vrw, setData...)

		if err != nil {
			return nil, orderedKey{}, 0, err
		}

		set := newSet(seq)
		var key orderedKey
		if len(setData) > 0 {
			var err error
			key, err = newOrderedKey(setData[len(setData)-1], vrw.Format())

			if err != nil {
				return nil, orderedKey{}, 0, err
			}
		}

		return set, key, uint64(len(items)), nil
	}
}

func newEmptySetSequenceChunker(ctx context.Context, vrw ValueReadWriter) (*sequenceChunker, error) {
	return newEmptySequenceChunker(ctx, vrw, makeSetLeafChunkFn(vrw), newOrderedMetaSequenceChunkFn(SetKind, vrw), newSetChunker, hashValueBytes)
}

func newSetChunker(nbf *NomsBinFormat, salt byte) sequenceSplitter {
	return newRollingValueHasher(nbf, salt)
}

func (s Set) readFrom(nbf *NomsBinFormat, b *binaryNomsReader) (Value, error) {
	panic("unreachable")
}

func (s Set) skip(nbf *NomsBinFormat, b *binaryNomsReader) {
	panic("unreachable")
}

func (s Set) String() string {
	panic("unreachable")
}

func (s Set) HumanReadableString() string {
	panic("unreachable")
}
