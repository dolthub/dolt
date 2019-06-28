// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"context"

	"github.com/liquidata-inc/ld/dolt/go/store/d"
)

// SetIterator defines methods that can be used to efficiently iterate through a set in 'Noms-defined'
// sorted order.
type SetIterator interface {
	// Next returns subsequent values from a set. It returns nil, when no objects remain.
	Next(ctx context.Context) Value

	// SkipTo(v) advances to and returns the next value in the iterator >= v.
	// Note: if the iterator has already returned the value being skipped to, it will return the next
	// value (just as if Next() was called). For example, given the following set:
	//   s = Set{ 0, 3, 6, 9, 12, 15, 18 }
	// An iterator on the set would return:
	//   i := s.Iterator()
	//   i.Next()  return 0
	//   i.SkipTo(4) -- returns 6
	//   i.skipTo(3) -- returns 9 (this is the next value in the iterator >= 3)
	//   i.skipTo(12) -- returns 12
	//   i.skipTo(12) -- return 15 (this is the next value in the iterator >= 12)
	//   i.skipTo(20) -- returns nil
	// If there are no values left in the iterator that are >= v,
	// the iterator will skip to the end of the sequence and return nil.
	SkipTo(ctx context.Context, v Value) Value
}

type setIterator struct {
	s            Set
	cursor       *sequenceCursor
	currentValue Value
}

func (si *setIterator) Next(ctx context.Context) Value {
	if si.cursor.valid() {
		si.currentValue = si.cursor.current().(Value)
		si.cursor.advance(ctx)
	} else {
		si.currentValue = nil
	}
	return si.currentValue
}

func (si *setIterator) SkipTo(ctx context.Context, v Value) Value {
	d.PanicIfTrue(v == nil)
	if si.cursor.valid() {
		if compareValue(v, si.currentValue) <= 0 {
			return si.Next(ctx)
		}

		// TODO(binformat)
		si.cursor = newCursorAtValue(ctx, Format_7_18, si.s.orderedSequence, v, true, false)
		if si.cursor.valid() {
			si.currentValue = si.cursor.current().(Value)
			si.cursor.advance(ctx)
		} else {
			si.currentValue = nil
		}
	} else {
		si.currentValue = nil
	}
	return si.currentValue
}

// iterState contains iterator and it's current value
type iterState struct {
	i SetIterator
	v Value
}

func (st *iterState) Next(ctx context.Context) Value {
	if st.v == nil {
		return nil
	}
	v := st.v
	st.v = st.i.Next(ctx)
	return v
}

func (st *iterState) SkipTo(ctx context.Context, v Value) Value {
	if st.v == nil || v == nil {
		st.v = nil
		return nil
	}
	st.v = st.i.SkipTo(ctx, v)
	return st.v
}

// UnionIterator combines the results from two other iterators. The values from Next() are returned in
// noms-defined order with all duplicates removed.
type UnionIterator struct {
	aState iterState
	bState iterState
}

// NewUnionIterator creates a union iterator from two other SetIterators.
func NewUnionIterator(ctx context.Context, iterA, iterB SetIterator) SetIterator {
	d.PanicIfTrue(iterA == nil)
	d.PanicIfTrue(iterB == nil)
	a := iterState{i: iterA, v: iterA.Next(ctx)}
	b := iterState{i: iterB, v: iterB.Next(ctx)}
	return &UnionIterator{aState: a, bState: b}
}

func (u *UnionIterator) Next(ctx context.Context) Value {
	switch compareValue(u.aState.v, u.bState.v) {
	case -1:
		return u.aState.Next(ctx)
	case 0:
		u.aState.Next(ctx)
		return u.bState.Next(ctx)
	case 1:
		return u.bState.Next(ctx)
	}
	panic("Unreachable")
}

func (u *UnionIterator) SkipTo(ctx context.Context, v Value) Value {
	d.PanicIfTrue(v == nil)
	didAdvance := false
	if compareValue(u.aState.v, v) < 0 {
		didAdvance = true
		u.aState.SkipTo(ctx, v)
	}
	if compareValue(u.bState.v, v) < 0 {
		didAdvance = true
		u.bState.SkipTo(ctx, v)
	}
	if !didAdvance {
		return u.Next(ctx)
	}
	switch compareValue(u.aState.v, u.bState.v) {
	case -1:
		return u.aState.Next(ctx)
	case 0:
		u.aState.Next(ctx)
		return u.bState.Next(ctx)
	case 1:
		return u.bState.Next(ctx)
	}
	panic("Unreachable")
}

// IntersectionIterator only returns values that are returned in both of its child iterators.
// The values from Next() are returned in noms-defined order with all duplicates removed.
type IntersectionIterator struct {
	aState iterState
	bState iterState
}

// NewIntersectionIterator creates a intersect iterator from two other SetIterators.
func NewIntersectionIterator(ctx context.Context, iterA, iterB SetIterator) SetIterator {
	d.PanicIfTrue(iterA == nil)
	d.PanicIfTrue(iterB == nil)
	a := iterState{i: iterA, v: iterA.Next(ctx)}
	b := iterState{i: iterB, v: iterB.Next(ctx)}
	return &IntersectionIterator{aState: a, bState: b}
}

func (i *IntersectionIterator) Next(ctx context.Context) Value {
	for cont := true; cont; {
		switch compareValue(i.aState.v, i.bState.v) {
		case -1:
			i.aState.SkipTo(ctx, i.bState.v)
		case 0:
			cont = false
		case 1:
			i.bState.SkipTo(ctx, i.aState.v)
		}
	}
	// we only get here if aState and bState are equal
	res := i.aState.v
	i.aState.Next(ctx)
	i.bState.Next(ctx)
	return res
}

func (i *IntersectionIterator) SkipTo(ctx context.Context, v Value) Value {
	d.PanicIfTrue(v == nil)
	if compareValue(v, i.aState.v) >= 0 {
		i.aState.SkipTo(ctx, v)
	}
	if compareValue(v, i.bState.v) >= 0 {
		i.bState.SkipTo(ctx, v)
	}
	return i.Next(ctx)
}

// considers nil max value, return -1 if v1 < v2, 0 if v1 == v2, 1 if v1 > v2
func compareValue(v1, v2 Value) int {
	if v1 == nil && v2 == nil {
		return 0
	}
	// TODO(binformat)
	if v2 == nil || (v1 != nil && v1.Less(Format_7_18, v2)) {
		return -1
	}
	if v1 == nil || (v2 != nil && v2.Less(Format_7_18, v1)) {
		return 1
	}
	return 0
}
