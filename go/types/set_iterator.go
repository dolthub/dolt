// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"github.com/attic-labs/noms/go/d"
)

// SetIterator defines methods that can be used to efficiently iterate through a set in 'Noms-defined'
// sorted order.
type SetIterator interface {
	// Next returns subsequent values from a set. It returns nil, when no objects remain
	Next() Value

	// NextFrom(v) advances the cursor to a new position based on the following rules and returns the value at that position.
	// * if v <= curValue, advances to the next value in the iteration.
	// * if there are no values >= v, advances to end of the set and returns nil.
	// * if there are values > v but but no values == v, advances to the first value > v.
	NextFrom(v Value) Value
}

type setIterator struct {
	s      Set
	cursor *sequenceCursor
}

func (si *setIterator) Next() Value {
	if si.cursor == nil {
		si.cursor = newCursorAt(si.s.seq, emptyKey, false, false)
	} else {
		si.cursor.advance()
	}
	if si.cursor.valid() {
		return si.cursor.current().(Value)
	}
	return nil
}

func (si *setIterator) NextFrom(v Value) Value {
	d.Chk.NotNil(v, "setIterator.NextFrom() called with nil value")
	first := false
	if si.cursor == nil {
		first = true
		si.cursor, _ = si.s.getCursorAtValue(v)
	}

	if !si.cursor.valid() {
		return nil
	}

	curValue := si.cursor.current().(Value)
	if first {
		return curValue
	}

	if compareValue(v, curValue) <= 0 {
		return si.Next()
	}

	si.cursor, _ = si.s.getCursorAtValue(v)
	if si.cursor.valid() {
		return si.cursor.current().(Value)
	}
	return nil
}

// iterState contains iterator and it's current value
type iterState struct {
	i SetIterator
	v Value
}

func (st *iterState) Next() Value {
	if st.v == nil {
		return nil
	}
	v := st.v
	st.v = st.i.Next()
	return v
}

func (st *iterState) NextFrom(v Value) Value {
	if st.v == nil || v == nil {
		st.v = nil
		return nil
	}
	st.v = st.i.NextFrom(v)
	return st.v
}

// UnionIterator combines the results from two other iterators. The values from Next() are returned in
// noms-defined order with all duplicates removed.
type UnionIterator struct {
	aState iterState
	bState iterState
}

// NewUnionIterator creates a union iterator from two other SetIterators.
func NewUnionIterator(iterA, iterB SetIterator) SetIterator {
	d.Chk.NotNil(iterA)
	d.Chk.NotNil(iterB)
	a := iterState{i: iterA, v: iterA.Next()}
	b := iterState{i: iterB, v: iterB.Next()}
	return &UnionIterator{aState: a, bState: b}
}

func (u *UnionIterator) Next() Value {
	switch compareValue(u.aState.v, u.bState.v) {
	case -1:
		return u.aState.Next()
	case 0:
		u.aState.Next()
		return u.bState.Next()
	case 1:
		return u.bState.Next()
	}
	panic("Unreachable")
	return nil
}

func (u *UnionIterator) NextFrom(v Value) Value {
	d.Chk.NotNil(v)
	didAdvance := false
	if compareValue(u.aState.v, v) < 0 {
		didAdvance = true
		u.aState.NextFrom(v)
	}
	if compareValue(u.bState.v, v) < 0 {
		didAdvance = true
		u.bState.NextFrom(v)
	}
	if !didAdvance {
		return u.Next()
	}
	switch compareValue(u.aState.v, u.bState.v) {
	case -1:
		return u.aState.Next()
	case 0:
		u.aState.Next()
		return u.bState.Next()
	case 1:
		return u.bState.Next()
	}
	panic("Unreachable")
	return nil
}

// IntersectionIterator only returns values that are returned in both of its child iterators.
// The values from Next() are returned in noms-defined order with all duplicates removed.
type IntersectionIterator struct {
	aState iterState
	bState iterState
}

// NewIntersectionIterator creates a intersect iterator from two other SetIterators.
func NewIntersectionIterator(iterA, iterB SetIterator) SetIterator {
	d.Chk.NotNil(iterA)
	d.Chk.NotNil(iterB)
	a := iterState{i: iterA, v: iterA.Next()}
	b := iterState{i: iterB, v: iterB.Next()}
	return &IntersectionIterator{aState: a, bState: b}
}

func (i *IntersectionIterator) Next() Value {
	for cont := true; cont; {
		switch compareValue(i.aState.v, i.bState.v) {
		case -1:
			i.aState.NextFrom(i.bState.v)
		case 0:
			cont = false
		case 1:
			i.bState.NextFrom(i.aState.v)
		}
	}
	// we only get here if aState and bState are equal
	res := i.aState.v
	i.aState.Next()
	i.bState.Next()
	return res
}

func (i *IntersectionIterator) NextFrom(v Value) Value {
	d.Chk.NotNil(v)
	if compareValue(v, i.aState.v) >= 0 {
		i.aState.NextFrom(v)
	}
	if compareValue(v, i.bState.v) >= 0 {
		i.bState.NextFrom(v)
	}
	return i.Next()
}

// considers nil max value, return -1 if v1 < v2, 0 if v1 == v2, 1 if v1 > v2
func compareValue(v1, v2 Value) int {
	if v1 == nil && v2 == nil {
		return 0
	}
	if v2 == nil || (v1 != nil && v1.Less(v2)) {
		return -1
	}
	if v1 == nil || (v2 != nil && v2.Less(v1)) {
		return 1
	}
	return 0
}
