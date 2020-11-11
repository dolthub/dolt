// Copyright 2020 Dolthub, Inc.
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

package lookup

import (
	"fmt"

	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/dolthub/dolt/go/store/types"
)

// Range represents the contiguous set of values that a lookup operation covers.
type Range struct {
	LowerBound Cut
	UpperBound Cut
}

// OpenRange returns a range representing {l < x < u}.
func OpenRange(lower, upper types.Tuple) (Range, error) {
	var err error
	// If partial key, moves the start to after all tuples matching the given. This will be ignored if it's a full key.
	lower, err = lower.Append(types.Uint(uint64(0xffffffffffffffff)))
	if err != nil {
		return Range{}, err
	}
	return Range{
		Above{key: lower},
		Below{key: upper},
	}, nil
}

// MustOpenRange is the same as OpenRange except that it panics on errors.
func MustOpenRange(lower, upper types.Tuple) Range {
	r, err := OpenRange(lower, upper)
	if err != nil {
		panic(err)
	}
	return r
}

// ClosedRange returns a range representing {l <= x <= u}.
func ClosedRange(lower, upper types.Tuple) (Range, error) {
	var err error
	// If partial key, moves the end to after all tuples matching the given. This will be ignored if it's a full key.
	upper, err = upper.Append(types.Uint(uint64(0xffffffffffffffff)))
	if err != nil {
		return Range{}, err
	}
	return Range{
		Below{key: lower},
		Above{key: upper},
	}, nil
}

// MustClosedRange is the same as ClosedRange except that it panics on errors.
func MustClosedRange(lower, upper types.Tuple) Range {
	r, err := ClosedRange(lower, upper)
	if err != nil {
		panic(err)
	}
	return r
}

// CustomRange returns a range defined by the bounds given.
func CustomRange(lower, upper types.Tuple, lowerBound, upperBound BoundType) (Range, error) {
	var err error
	var lCut Cut
	var uCut Cut
	if lowerBound == Open {
		// If partial key, moves the start to after all tuples matching the given. This will be ignored if it's a full key.
		lower, err = lower.Append(types.Uint(uint64(0xffffffffffffffff)))
		if err != nil {
			return Range{}, err
		}
		lCut = Above{key: lower}
	} else {
		lCut = Below{key: lower}
	}
	if upperBound == Open {
		uCut = Below{key: upper}
	} else {
		// If partial key, moves the end to after all tuples matching the given. This will be ignored if it's a full key.
		upper, err = upper.Append(types.Uint(uint64(0xffffffffffffffff)))
		if err != nil {
			return Range{}, err
		}
		uCut = Above{key: upper}
	}
	return Range{
		lCut,
		uCut,
	}, nil
}

// MustCustomRange is the same as CustomRange except that it panics on errors.
func MustCustomRange(lower, upper types.Tuple, lowerBound, upperBound BoundType) Range {
	r, err := CustomRange(lower, upper, lowerBound, upperBound)
	if err != nil {
		panic(err)
	}
	return r
}

// LessThanRange returns a range representing {x < u}.
func LessThanRange(upper types.Tuple) Range {
	return Range{
		BelowAll{},
		Below{key: upper},
	}
}

// LessOrEqualRange returns a range representing  {x <= u}.
func LessOrEqualRange(upper types.Tuple) (Range, error) {
	var err error
	// If partial key, moves the end to after all tuples matching the given. This will be ignored if it's a full key.
	upper, err = upper.Append(types.Uint(uint64(0xffffffffffffffff)))
	if err != nil {
		return Range{}, err
	}
	return Range{
		BelowAll{},
		Above{key: upper},
	}, nil
}

// MustLessOrEqualRange is the same as LessOrEqualRange except that it panics on errors.
func MustLessOrEqualRange(upper types.Tuple) Range {
	r, err := LessOrEqualRange(upper)
	if err != nil {
		panic(err)
	}
	return r
}

// GreaterThanRange returns a range representing {x > l}.
func GreaterThanRange(lower types.Tuple) (Range, error) {
	var err error
	// If partial key, moves the start to after all tuples matching the given. This will be ignored if it's a full key.
	lower, err = lower.Append(types.Uint(uint64(0xffffffffffffffff)))
	if err != nil {
		return Range{}, err
	}
	return Range{
		Above{key: lower},
		AboveAll{},
	}, nil
}

// MustGreaterThanRange is the same as GreaterThanRange except that it panics on errors.
func MustGreaterThanRange(lower types.Tuple) Range {
	r, err := GreaterThanRange(lower)
	if err != nil {
		panic(err)
	}
	return r
}

// GreaterOrEqualRange returns a range representing {x >= l}.
func GreaterOrEqualRange(lower types.Tuple) Range {
	return Range{
		Below{key: lower},
		AboveAll{},
	}
}

// AllRange returns a range representing all values.
func AllRange() Range {
	return Range{
		BelowAll{},
		AboveAll{},
	}
}

// EmptyRange returns the empty range.
func EmptyRange() Range {
	return Range{
		AboveAll{},
		AboveAll{},
	}
}

// Equals checks for equality with the given range.
func (r Range) Equals(other Range) bool {
	return r.LowerBound.Equals(other.LowerBound) && r.UpperBound.Equals(other.UpperBound)
}

// Format returns the NomsBinFormat.
func (r Range) Format() *types.NomsBinFormat {
	return r.LowerBound.Format()
}

// HasLowerBound returns whether this range has a lower bound.
func (r Range) HasLowerBound() bool {
	return r.LowerBound != BelowAll{}
}

// HasUpperBound returns whether this range has an upper bound.
func (r Range) HasUpperBound() bool {
	return r.UpperBound != AboveAll{}
}

// IsEmpty returns whether this range is empty.
func (r Range) IsEmpty() bool {
	return r.LowerBound.Equals(r.UpperBound)
}

// IsConnected evaluates whether the given range overlaps or is adjacent to the calling range.
func (r Range) IsConnected(other Range) (bool, error) {
	comp, err := r.LowerBound.Compare(other.UpperBound)
	if err != nil {
		return false, err
	}
	if comp > 0 {
		return false, nil
	}
	comp, err = other.LowerBound.Compare(r.UpperBound)
	if err != nil {
		return false, err
	}
	return comp <= 0, nil
}

// String returns Range Cut as a string for debugging purposes. Will panic on errors.
func (r Range) String() string {
	return fmt.Sprintf("Range(%s, %s)", r.LowerBound.String(), r.UpperBound.String())
}

// ToReadRange returns this range as a Noms ReadRange.
func (r Range) ToReadRange() *noms.ReadRange {
	if r.IsEmpty() {
		return &noms.ReadRange{
			Start:     types.EmptyTuple(r.Format()),
			Inclusive: false,
			Reverse:   false,
			Check:     neverContinueRangeCheck,
		}
	}
	if r.Equals(AllRange()) {
		return &noms.ReadRange{
			Start:     types.EmptyTuple(r.Format()),
			Inclusive: true,
			Reverse:   false,
			Check:     alwaysContinueRangeCheck,
		}
	}
	if !r.HasLowerBound() {
		return &noms.ReadRange{
			Start:     GetKey(r.UpperBound),
			Inclusive: r.UpperBound.TypeAsUpperBound().Inclusive(),
			Reverse:   true,
			Check:     alwaysContinueRangeCheck,
		}
	} else if !r.HasUpperBound() {
		return &noms.ReadRange{
			Start:     GetKey(r.LowerBound),
			Inclusive: r.LowerBound.TypeAsLowerBound().Inclusive(),
			Reverse:   false,
			Check:     alwaysContinueRangeCheck,
		}
	}
	return &noms.ReadRange{
		Start:     GetKey(r.LowerBound),
		Inclusive: r.LowerBound.TypeAsLowerBound().Inclusive(),
		Reverse:   false,
		Check: func(tpl types.Tuple) (bool, error) {
			ok, err := r.UpperBound.Less(tpl)
			return !ok, err
		},
	}
}

// TryIntersect attempts to intersect the given range with the calling range.
func (r Range) TryIntersect(other Range) (Range, bool, error) {
	_, l, err := OrderedCuts(r.LowerBound, other.LowerBound)
	if err != nil {
		return Range{}, false, err
	}
	u, _, err := OrderedCuts(r.UpperBound, other.UpperBound)
	if err != nil {
		return Range{}, false, err
	}
	comp, err := l.Compare(u)
	if err != nil {
		return Range{}, false, err
	}
	if comp < 0 {
		return Range{l, u}, true, nil
	}
	return EmptyRange(), false, nil
}

// TryUnion attempts to combine the given range with the calling range.
func (r Range) TryUnion(other Range) (Range, bool, error) {
	if other.IsEmpty() {
		return r, true, nil
	}
	if r.IsEmpty() {
		return other, true, nil
	}
	connected, err := r.IsConnected(other)
	if err != nil {
		return Range{}, false, err
	}
	if !connected {
		return Range{}, false, nil
	}
	l, _, err := OrderedCuts(r.LowerBound, other.LowerBound)
	if err != nil {
		return Range{}, false, err
	}
	_, u, err := OrderedCuts(r.UpperBound, other.UpperBound)
	if err != nil {
		return Range{}, false, err
	}
	return Range{l, u}, true, nil
}

// OrderedCuts returns the given Cuts in order from lowest-touched values to highest-touched values.
func OrderedCuts(l, r Cut) (Cut, Cut, error) {
	comp, err := l.Compare(r)
	if err != nil {
		return nil, nil, err
	}
	if comp <= 0 {
		return l, r, nil
	}
	return r, l, nil
}

// alwaysContinueRangeCheck will allow the range to continue until the end is reached.
func alwaysContinueRangeCheck(types.Tuple) (bool, error) {
	return true, nil
}

// neverContinueRangeCheck will immediately end.
func neverContinueRangeCheck(types.Tuple) (bool, error) {
	return false, nil
}
