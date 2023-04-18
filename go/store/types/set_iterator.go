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

	"github.com/dolthub/dolt/go/store/d"
)

// SetIterator defines methods that can be used to efficiently iterate through a set in 'Noms-defined'
// sorted order.
type SetIterator interface {
	// Next returns subsequent values from a set. It returns nil, when no objects remain.
	Next(ctx context.Context) (Value, error)

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
	SkipTo(ctx context.Context, v Value) (Value, error)
}

type setIterator struct {
	s            Set
	sequenceIter sequenceIterator
	currentValue Value
}

func (si *setIterator) Next(ctx context.Context) (Value, error) {
	if si.sequenceIter.valid() {
		item, err := si.sequenceIter.current()

		if err != nil {
			return nil, err
		}

		si.currentValue = item.(Value)
		_, err = si.sequenceIter.advance(ctx)

		if err != nil {
			return nil, err
		}
	} else {
		si.currentValue = nil
	}
	return si.currentValue, nil
}

func (si *setIterator) SkipTo(ctx context.Context, v Value) (Value, error) {
	d.PanicIfTrue(v == nil)
	if si.sequenceIter.valid() {
		if cmp, err := compareValue(ctx, si.s.valueReadWriter(), v, si.currentValue); err != nil {
			return nil, err
		} else if cmp <= 0 {
			return si.Next(ctx)
		}

		var err error
		si.sequenceIter, err = newCursorAtValue(ctx, si.s.orderedSequence, v, true, false)

		if err != nil {
			return nil, err
		}

		if si.sequenceIter.valid() {
			item, err := si.sequenceIter.current()

			if err != nil {
				return nil, err
			}

			si.currentValue = item.(Value)
			_, err = si.sequenceIter.advance(ctx)

			if err != nil {
				return nil, err
			}
		} else {
			si.currentValue = nil
		}
	} else {
		si.currentValue = nil
	}
	return si.currentValue, nil
}

// iterState contains iterator and it's current value
type iterState struct {
	i SetIterator
	v Value
}

func (st *iterState) Next(ctx context.Context) (Value, error) {
	if st.v == nil {
		return nil, nil
	}

	v := st.v
	var err error
	st.v, err = st.i.Next(ctx)

	if err != nil {
		return nil, err
	}

	return v, nil
}

func (st *iterState) SkipTo(ctx context.Context, v Value) (Value, error) {
	if st.v == nil || v == nil {
		st.v = nil
		return nil, nil
	}
	var err error
	st.v, err = st.i.SkipTo(ctx, v)

	if err != nil {
		return nil, err
	}

	return st.v, nil
}

// UnionIterator combines the results from two other iterators. The values from Next() are returned in
// noms-defined order with all duplicates removed.
type UnionIterator struct {
	aState iterState
	bState iterState
	vr     ValueReader
}

// NewUnionIterator creates a union iterator from two other SetIterators.
func NewUnionIterator(ctx context.Context, vr ValueReader, iterA, iterB SetIterator) (SetIterator, error) {
	d.PanicIfTrue(iterA == nil)
	d.PanicIfTrue(iterB == nil)
	aVal, err := iterA.Next(ctx)
	if err != nil {
		return nil, err
	}

	bVal, err := iterB.Next(ctx)
	if err != nil {
		return nil, err
	}

	a := iterState{i: iterA, v: aVal}
	b := iterState{i: iterB, v: bVal}
	return &UnionIterator{aState: a, bState: b, vr: vr}, nil
}

func (u *UnionIterator) Next(ctx context.Context) (Value, error) {
	cmp, err := compareValue(ctx, u.vr, u.aState.v, u.bState.v)

	if err != nil {
		return nil, err
	}

	switch cmp {
	case -1:
		return u.aState.Next(ctx)
	case 0:
		_, err := u.aState.Next(ctx)

		if err != nil {
			return nil, err
		}

		return u.bState.Next(ctx)
	case 1:
		return u.bState.Next(ctx)
	}
	panic("Unreachable")
}

func (u *UnionIterator) SkipTo(ctx context.Context, v Value) (Value, error) {
	d.PanicIfTrue(v == nil)
	didAdvance := false
	if cmp, err := compareValue(ctx, u.vr, u.aState.v, v); err != nil {
		return nil, err
	} else if cmp < 0 {
		didAdvance = true
		_, err := u.aState.SkipTo(ctx, v)

		if err != nil {
			return nil, err
		}
	}
	if cmp, err := compareValue(ctx, u.vr, u.bState.v, v); err != nil {
		return nil, err
	} else if cmp < 0 {
		didAdvance = true
		_, err := u.bState.SkipTo(ctx, v)

		if err != nil {
			return nil, err
		}
	}
	if !didAdvance {
		return u.Next(ctx)
	}
	cmp, err := compareValue(ctx, u.vr, u.aState.v, u.bState.v)

	if err != nil {
		return nil, err
	}

	switch cmp {
	case -1:
		return u.aState.Next(ctx)
	case 0:
		_, err := u.aState.Next(ctx)

		if err != nil {
			return nil, err
		}

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
	vr     ValueReader
}

// NewIntersectionIterator creates a intersect iterator from two other SetIterators.
func NewIntersectionIterator(ctx context.Context, vr ValueReader, iterA, iterB SetIterator) (SetIterator, error) {
	d.PanicIfTrue(iterA == nil)
	d.PanicIfTrue(iterB == nil)
	aVal, err := iterA.Next(ctx)

	if err != nil {
		return nil, err
	}

	bVal, err := iterB.Next(ctx)

	if err != nil {
		return nil, err
	}

	a := iterState{i: iterA, v: aVal}
	b := iterState{i: iterB, v: bVal}
	return &IntersectionIterator{aState: a, bState: b, vr: vr}, nil
}

func (i *IntersectionIterator) Next(ctx context.Context) (Value, error) {
	for cont := true; cont; {
		cmp, err := compareValue(ctx, i.vr, i.aState.v, i.bState.v)

		if err != nil {
			return nil, err
		}

		switch cmp {
		case -1:
			_, err := i.aState.SkipTo(ctx, i.bState.v)

			if err != nil {
				return nil, err
			}
		case 0:
			cont = false
		case 1:
			_, err := i.bState.SkipTo(ctx, i.aState.v)

			if err != nil {
				return nil, err
			}
		}
	}
	// we only get here if aState and bState are equal
	res := i.aState.v
	_, err := i.aState.Next(ctx)

	if err != nil {
		return nil, err
	}

	_, err = i.bState.Next(ctx)

	if err != nil {
		return nil, err
	}

	return res, nil
}

func (i *IntersectionIterator) SkipTo(ctx context.Context, v Value) (Value, error) {
	d.PanicIfTrue(v == nil)
	if cmp, err := compareValue(ctx, i.vr, v, i.aState.v); err != nil {
		return nil, err
	} else if cmp >= 0 {
		_, err := i.aState.SkipTo(ctx, v)

		if err != nil {
			return nil, err
		}
	}

	if cmp, err := compareValue(ctx, i.vr, v, i.bState.v); err != nil {
		return nil, err
	} else if cmp >= 0 {
		_, err := i.bState.SkipTo(ctx, v)

		if err != nil {
			return nil, err
		}
	}

	return i.Next(ctx)
}

// considers nil max value, return -1 if v1 < v2, 0 if v1 == v2, 1 if v1 > v2
func compareValue(ctx context.Context, vr ValueReader, v1, v2 Value) (int, error) {
	if v1 == nil && v2 == nil {
		return 0, nil
	}

	if v2 == nil {
		return -1, nil
	}

	if v1 != nil {
		if isLess, err := v1.Less(ctx, vr.Format(), v2); err != nil {
			return 0, err
		} else if isLess {
			return -1, nil
		}
	}

	if v1 == nil {
		return 1, nil
	}

	if isLess, err := v2.Less(ctx, vr.Format(), v1); err != nil {
		return 0, err
	} else if isLess {
		return 1, nil
	}

	return 0, nil
}
