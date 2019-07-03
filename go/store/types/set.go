// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"context"
	"fmt"
	"sort"

	"github.com/liquidata-inc/ld/dolt/go/store/d"
)

var EmptySet Set

type Set struct {
	orderedSequence
	format *Format
}

func newSet(f *Format, seq orderedSequence) Set {
	return Set{seq, f}
}

func NewSet(ctx context.Context, vrw ValueReadWriter, v ...Value) Set {
	data := buildSetData(vrw.Format(), v)
	ch := newEmptySetSequenceChunker(ctx, vrw.Format(), vrw)

	for _, v := range data {
		ch.Append(ctx, v)
	}

	return newSet(vrw.Format(), ch.Done(ctx).(orderedSequence))
}

// NewStreamingSet takes an input channel of values and returns a output
// channel that will produce a finished Set. Values that are sent to the input
// channel must be in Noms sortorder, adding values to the input channel
// out of order will result in a panic. Once the input channel is closed
// by the caller, a finished Set will be sent to the output channel. See
// graph_builder.go for building collections with values that are not in order.
func NewStreamingSet(ctx context.Context, vrw ValueReadWriter, vChan <-chan Value) <-chan Set {
	return newStreamingSet(vrw, vChan, func(vrw ValueReadWriter, vChan <-chan Value, outChan chan<- Set) {
		go readSetInput(ctx, vrw.Format(), vrw, vChan, outChan)
	})
}

type streamingSetReadFunc func(vrw ValueReadWriter, vChan <-chan Value, outChan chan<- Set)

func newStreamingSet(vrw ValueReadWriter, vChan <-chan Value, readFunc streamingSetReadFunc) <-chan Set {
	d.PanicIfTrue(vrw == nil)
	outChan := make(chan Set, 1)
	readFunc(vrw, vChan, outChan)
	return outChan
}

func readSetInput(ctx context.Context, f *Format, vrw ValueReadWriter, vChan <-chan Value, outChan chan<- Set) {
	defer close(outChan)
	ch := newEmptySetSequenceChunker(ctx, f, vrw)
	var lastV Value
	for v := range vChan {
		if lastV != nil {
			d.PanicIfFalse(lastV.Less(f, v))
		}
		lastV = v
		ch.Append(ctx, v)
	}
	outChan <- newSet(f, ch.Done(ctx).(orderedSequence))
}

// Diff computes the diff from |last| to |m| using the top-down algorithm,
// which completes as fast as possible while taking longer to return early
// results than left-to-right.
func (s Set) Diff(ctx context.Context, last Set, changes chan<- ValueChanged, closeChan <-chan struct{}) {
	if s.Equals(s.format, last) {
		return
	}
	orderedSequenceDiffTopDown(ctx, s.format, last.orderedSequence, s.orderedSequence, changes, closeChan)
}

// DiffHybrid computes the diff from |last| to |s| using a hybrid algorithm
// which balances returning results early vs completing quickly, if possible.
func (s Set) DiffHybrid(ctx context.Context, last Set, changes chan<- ValueChanged, closeChan <-chan struct{}) {
	if s.Equals(s.format, last) {
		return
	}
	orderedSequenceDiffBest(ctx, s.format, last.orderedSequence, s.orderedSequence, changes, closeChan)
}

// DiffLeftRight computes the diff from |last| to |s| using a left-to-right
// streaming approach, optimised for returning results early, but not
// completing quickly.
func (s Set) DiffLeftRight(ctx context.Context, last Set, changes chan<- ValueChanged, closeChan <-chan struct{}) {
	if s.Equals(s.format, last) {
		return
	}
	orderedSequenceDiffLeftRight(ctx, s.format, last.orderedSequence, s.orderedSequence, changes, closeChan)
}

func (s Set) asSequence() sequence {
	return s.orderedSequence
}

// Value interface
func (s Set) Value(ctx context.Context) Value {
	return s
}

func (s Set) WalkValues(ctx context.Context, cb ValueCallback) {
	iterAll(ctx, s.format, s, func(v Value, idx uint64) {
		cb(v)
	})
}

func (s Set) First(ctx context.Context) Value {
	cur := newCursorAt(ctx, s.format, s.orderedSequence, emptyKey, false, false)
	if !cur.valid() {
		return nil
	}
	return cur.current().(Value)
}

func (s Set) At(ctx context.Context, idx uint64) Value {
	if idx >= s.Len() {
		panic(fmt.Errorf("out of bounds: %d >= %d", idx, s.Len()))
	}

	cur := newCursorAtIndex(ctx, s.orderedSequence, idx, s.format)
	return cur.current().(Value)
}

func (s Set) Has(ctx context.Context, v Value) bool {
	cur := newCursorAtValue(ctx, s.format, s.orderedSequence, v, false, false)
	return cur.valid() && cur.current().(Value).Equals(s.format, v)
}

type setIterCallback func(v Value) bool

func (s Set) Iter(ctx context.Context, cb setIterCallback) {
	cur := newCursorAt(ctx, s.format, s.orderedSequence, emptyKey, false, false)
	cur.iter(ctx, func(v interface{}) bool {
		return cb(v.(Value))
	})
}

type setIterAllCallback func(v Value)

func (s Set) IterAll(ctx context.Context, cb setIterAllCallback) {
	iterAll(ctx, s.format, s, func(v Value, idx uint64) {
		cb(v)
	})
}

func (s Set) Iterator(ctx context.Context) SetIterator {
	return s.IteratorAt(ctx, 0)
}

func (s Set) IteratorAt(ctx context.Context, idx uint64) SetIterator {
	return &setIterator{
		cursor: newCursorAtIndex(ctx, s.orderedSequence, idx, s.format),
		s:      s,
	}
}

func (s Set) IteratorFrom(ctx context.Context, val Value) SetIterator {
	return &setIterator{
		cursor: newCursorAtValue(ctx, s.format, s.orderedSequence, val, false, false),
		s:      s,
	}
}

func (s Set) Edit() *SetEditor {
	return NewSetEditor(s)
}

func buildSetData(f *Format, values ValueSlice) ValueSlice {
	if len(values) == 0 {
		return ValueSlice{}
	}

	sort.Stable(ValueSort{values, f})

	uniqueSorted := make(ValueSlice, 0, len(values))
	last := values[0]
	for i := 1; i < len(values); i++ {
		v := values[i]
		if !v.Equals(f, last) {
			uniqueSorted = append(uniqueSorted, last)
		}
		last = v
	}

	return append(uniqueSorted, last)
}

func makeSetLeafChunkFn(f *Format, vrw ValueReadWriter) makeChunkFn {
	return func(level uint64, items []sequenceItem) (Collection, orderedKey, uint64) {
		d.PanicIfFalse(level == 0)
		setData := make([]Value, len(items))

		var lastValue Value
		for i, item := range items {
			v := item.(Value)
			d.PanicIfFalse(lastValue == nil || lastValue.Less(f, v))
			lastValue = v
			setData[i] = v
		}

		set := newSet(f, newSetLeafSequence(f, vrw, setData...))
		var key orderedKey
		if len(setData) > 0 {
			key = newOrderedKey(setData[len(setData)-1], f)
		}

		return set, key, uint64(len(items))
	}
}

func newEmptySetSequenceChunker(ctx context.Context, f *Format, vrw ValueReadWriter) *sequenceChunker {
	return newEmptySequenceChunker(ctx, vrw, makeSetLeafChunkFn(f, vrw), newOrderedMetaSequenceChunkFn(SetKind, f, vrw), hashValueBytes)
}
