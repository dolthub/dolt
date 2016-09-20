// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"sort"

	"github.com/attic-labs/noms/go/hash"
)

type Set struct {
	seq orderedSequence
	h   *hash.Hash
}

func newSet(seq orderedSequence) Set {
	return Set{seq, &hash.Hash{}}
}

func NewSet(v ...Value) Set {
	data := buildSetData(v)
	ch := newEmptySetSequenceChunker(nil, nil)

	for _, v := range data {
		ch.Append(v)
	}

	return newSet(ch.Done().(orderedSequence))
}

func NewStreamingSet(vrw ValueReadWriter, vals <-chan Value) <-chan Set {
	outChan := make(chan Set)
	go func() {
		gb := NewGraphBuilder(vrw, SetKind, false)
		for v := range vals {
			gb.SetInsert(nil, v)
		}
		outChan <- gb.Build().(Set)
	}()
	return outChan
}

// Computes the diff from |last| to |s| using "best" algorithm, which balances returning results early vs completing quickly.
func (s Set) Diff(last Set, changes chan<- ValueChanged, closeChan <-chan struct{}) {
	if s.Equals(last) {
		return
	}
	orderedSequenceDiffBest(last.seq, s.seq, changes, closeChan)
}

// Like Diff() but uses a left-to-right streaming approach, optimised for returning results early, but not completing quickly.
func (s Set) DiffLeftRight(last Set, changes chan<- ValueChanged, closeChan <-chan struct{}) {
	if s.Equals(last) {
		return
	}
	orderedSequenceDiffLeftRight(last.seq, s.seq, changes, closeChan)
}

// Collection interface
func (s Set) Len() uint64 {
	return s.seq.numLeaves()
}

func (s Set) Empty() bool {
	return s.Len() == 0
}

func (s Set) sequence() sequence {
	return s.seq
}

func (s Set) hashPointer() *hash.Hash {
	return s.h
}

// Value interface
func (s Set) Equals(other Value) bool {
	return s.Hash() == other.Hash()
}

func (s Set) Less(other Value) bool {
	return valueLess(s, other)
}

func (s Set) Hash() hash.Hash {
	if s.h.IsEmpty() {
		*s.h = getHash(s)
	}

	return *s.h
}

func (s Set) ChildValues() (values []Value) {
	s.IterAll(func(v Value) {
		values = append(values, v)
	})
	return
}

func (s Set) Chunks() []Ref {
	return s.seq.Chunks()
}

func (s Set) Type() *Type {
	return s.seq.Type()
}

func (s Set) First() Value {
	cur := newCursorAt(s.seq, emptyKey, false, false)
	if !cur.valid() {
		return nil
	}
	return cur.current().(Value)
}

func (s Set) Insert(values ...Value) Set {
	if len(values) == 0 {
		return s
	}

	head, tail := values[0], values[1:]

	var res Set
	if cur, found := s.getCursorAtValue(head); !found {
		res = s.splice(cur, 0, head)
	} else {
		res = s
	}

	return res.Insert(tail...)
}

func (s Set) Remove(values ...Value) Set {
	if len(values) == 0 {
		return s
	}

	head, tail := values[0], values[1:]

	var res Set
	if cur, found := s.getCursorAtValue(head); found {
		res = s.splice(cur, 1)
	} else {
		res = s
	}

	return res.Remove(tail...)
}

func (s Set) splice(cur *sequenceCursor, deleteCount uint64, vs ...Value) Set {
	ch := newSequenceChunker(cur, s.seq.valueReader(), nil, makeSetLeafChunkFn(s.seq.valueReader()), newOrderedMetaSequenceChunkFn(SetKind, s.seq.valueReader()), hashValueBytes)
	for deleteCount > 0 {
		ch.Skip()
		deleteCount--
	}

	for _, v := range vs {
		ch.Append(v)
	}

	ns := newSet(ch.Done().(orderedSequence))
	return ns
}

func (s Set) getCursorAtValue(v Value) (cur *sequenceCursor, found bool) {
	cur = newCursorAtValue(s.seq, v, true, false)
	found = cur.idx < cur.seq.seqLen() && cur.current().(Value).Equals(v)
	return
}

func (s Set) Has(v Value) bool {
	cur := newCursorAtValue(s.seq, v, false, false)
	return cur.valid() && cur.current().(Value).Equals(v)
}

type setIterCallback func(v Value) bool

func (s Set) Iter(cb setIterCallback) {
	cur := newCursorAt(s.seq, emptyKey, false, false)
	cur.iter(func(v interface{}) bool {
		return cb(v.(Value))
	})
}

type setIterAllCallback func(v Value)

func (s Set) IterAll(cb setIterAllCallback) {
	cur := newCursorAt(s.seq, emptyKey, false, false)
	cur.iter(func(v interface{}) bool {
		cb(v.(Value))
		return false
	})
}

func (s Set) Iterator() SetIterator {
	return &setIterator{s: s, cursor: nil}
}

func (s Set) elemType() *Type {
	return s.Type().Desc.(CompoundDesc).ElemTypes[0]
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

func makeSetLeafChunkFn(vr ValueReader) makeChunkFn {
	return func(items []sequenceItem) (Collection, orderedKey, uint64) {
		setData := make([]Value, len(items), len(items))

		for i, v := range items {
			setData[i] = v.(Value)
		}

		set := newSet(newSetLeafSequence(vr, setData...))
		var key orderedKey
		if len(setData) > 0 {
			key = newOrderedKey(setData[len(setData)-1])
		}

		return set, key, uint64(len(items))
	}
}

func newEmptySetSequenceChunker(vr ValueReader, vw ValueWriter) *sequenceChunker {
	return newEmptySequenceChunker(vr, vw, makeSetLeafChunkFn(vr), newOrderedMetaSequenceChunkFn(SetKind, vr), hashValueBytes)
}
