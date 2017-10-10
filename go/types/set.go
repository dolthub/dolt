// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"fmt"
	"sort"

	"github.com/attic-labs/noms/go/d"
)

type Set struct {
	orderedSequence
}

func newSet(seq orderedSequence) Set {
	return Set{seq}
}

func NewSet(vrw ValueReadWriter, v ...Value) Set {
	data := buildSetData(v)
	ch := newEmptySetSequenceChunker(vrw)

	for _, v := range data {
		ch.Append(v)
	}

	return newSet(ch.Done().(orderedSequence))
}

// NewStreamingSet takes an input channel of values and returns a output
// channel that will produce a finished Set. Values that are sent to the input
// channel must be in Noms sortorder, adding values to the input channel
// out of order will result in a panic. Once the input channel is closed
// by the caller, a finished Set will be sent to the output channel. See
// graph_builder.go for building collections with values that are not in order.
func NewStreamingSet(vrw ValueReadWriter, vChan <-chan Value) <-chan Set {
	return newStreamingSet(vrw, vChan, func(vrw ValueReadWriter, vChan <-chan Value, outChan chan<- Set) {
		go readSetInput(vrw, vChan, outChan)
	})
}

type streamingSetReadFunc func(vrw ValueReadWriter, vChan <-chan Value, outChan chan<- Set)

func newStreamingSet(vrw ValueReadWriter, vChan <-chan Value, readFunc streamingSetReadFunc) <-chan Set {
	d.PanicIfTrue(vrw == nil)
	outChan := make(chan Set, 1)
	readFunc(vrw, vChan, outChan)
	return outChan
}

func readSetInput(vrw ValueReadWriter, vChan <-chan Value, outChan chan<- Set) {
	defer close(outChan)
	ch := newEmptySetSequenceChunker(vrw)
	var lastV Value
	for v := range vChan {
		d.PanicIfTrue(v == nil)
		if lastV != nil {
			d.PanicIfFalse(lastV.Less(v))
		}
		lastV = v
		ch.Append(v)
	}
	outChan <- newSet(ch.Done().(orderedSequence))
}

// Diff computes the diff from |last| to |m| using the top-down algorithm,
// which completes as fast as possible while taking longer to return early
// results than left-to-right.
func (s Set) Diff(last Set, changes chan<- ValueChanged, closeChan <-chan struct{}) {
	if s.Equals(last) {
		return
	}
	orderedSequenceDiffTopDown(last.orderedSequence, s.orderedSequence, changes, closeChan)
}

// DiffHybrid computes the diff from |last| to |s| using a hybrid algorithm
// which balances returning results early vs completing quickly, if possible.
func (s Set) DiffHybrid(last Set, changes chan<- ValueChanged, closeChan <-chan struct{}) {
	if s.Equals(last) {
		return
	}
	orderedSequenceDiffBest(last.orderedSequence, s.orderedSequence, changes, closeChan)
}

// DiffLeftRight computes the diff from |last| to |s| using a left-to-right
// streaming approach, optimised for returning results early, but not
// completing quickly.
func (s Set) DiffLeftRight(last Set, changes chan<- ValueChanged, closeChan <-chan struct{}) {
	if s.Equals(last) {
		return
	}
	orderedSequenceDiffLeftRight(last.orderedSequence, s.orderedSequence, changes, closeChan)
}

func (s Set) asSequence() sequence {
	return s.orderedSequence
}

// Value interface
func (s Set) Value() Value {
	return s
}

func (s Set) WalkValues(cb ValueCallback) {
	iterAll(s, func(v Value, idx uint64) {
		cb(v)
	})
}

func (s Set) First() Value {
	cur := newCursorAt(s.orderedSequence, emptyKey, false, false)
	if !cur.valid() {
		return nil
	}
	return cur.current().(Value)
}

func (s Set) At(idx uint64) Value {
	if idx >= s.Len() {
		panic(fmt.Errorf("Out of bounds: %d >= %d", idx, s.Len()))
	}

	cur := newCursorAtIndex(s.orderedSequence, idx)
	return cur.current().(Value)
}

func (s Set) Has(v Value) bool {
	cur := newCursorAtValue(s.orderedSequence, v, false, false)
	return cur.valid() && cur.current().(Value).Equals(v)
}

type setIterCallback func(v Value) bool

func (s Set) Iter(cb setIterCallback) {
	cur := newCursorAt(s.orderedSequence, emptyKey, false, false)
	cur.iter(func(v interface{}) bool {
		return cb(v.(Value))
	})
}

type setIterAllCallback func(v Value)

func (s Set) IterAll(cb setIterAllCallback) {
	iterAll(s, func(v Value, idx uint64) {
		cb(v)
	})
}

func (s Set) Iterator() SetIterator {
	return s.IteratorAt(0)
}

func (s Set) IteratorAt(idx uint64) SetIterator {
	return &setIterator{
		cursor: newCursorAtIndex(s.orderedSequence, idx),
		s:      s,
	}
}

func (s Set) IteratorFrom(val Value) SetIterator {
	return &setIterator{
		cursor: newCursorAtValue(s.orderedSequence, val, false, false),
		s:      s,
	}
}

func (s Set) Edit() *SetEditor {
	return NewSetEditor(s)
}

func buildSetData(values ValueSlice) ValueSlice {
	if len(values) == 0 {
		return ValueSlice{}
	}

	uniqueSorted := make(ValueSlice, 0, len(values))
	sort.Stable(values)
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
	return func(level uint64, items []sequenceItem) (Collection, orderedKey, uint64) {
		d.PanicIfFalse(level == 0)
		setData := make([]Value, len(items), len(items))

		var lastValue Value
		for i, item := range items {
			v := item.(Value)
			d.PanicIfFalse(lastValue == nil || lastValue.Less(v))
			lastValue = v
			setData[i] = v
		}

		set := newSet(newSetLeafSequence(vrw, setData...))
		var key orderedKey
		if len(setData) > 0 {
			key = newOrderedKey(setData[len(setData)-1])
		}

		return set, key, uint64(len(items))
	}
}

func newEmptySetSequenceChunker(vrw ValueReadWriter) *sequenceChunker {
	return newEmptySequenceChunker(vrw, makeSetLeafChunkFn(vrw), newOrderedMetaSequenceChunkFn(SetKind, vrw), hashValueBytes)
}
