// Copyright 2020 Liquidata, Inc.
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

package setalgebra

import "github.com/liquidata-inc/dolt/go/store/types"

// IntervalEndpoint is a value at which an interval starts or ends, and a boolean which indicates whether
// the Interval is open or closed at that endpoint.
type IntervalEndpoint struct {
	// Val is the value at which the interval starts or ends
	Val types.Value
	// Inclusive indicates whether the value itself is included in the interval.  If the value is inclusive
	// we say this is a closed interval.  If it is not, it is an open interval.
	Inclusive bool
}

// Interval is a set which can be written as an inequality such as {n | n > 0} (set of all numbers n such that n > 0)
// or a chained comparison {n | 0.0 <= n <= 1.0 } (set of all floating point values between 0.0 and 1.0)
type Interval struct {
	nbf *types.NomsBinFormat
	// Start is the start of an interval. Start must be less than or equal to Erd. A nil value indicates an interval
	// going to negative infinity
	Start *IntervalEndpoint
	// End is the end of an interval. End must be greater than or equal to Start. A nil value indicates an interval
	// going to positive infinity.
	End *IntervalEndpoint
}

// NewInterval creates a new Interval object with given endpoints where a nil start or end represents an interval
// going to negative finitity or infinity positive respectively.
func NewInterval(nbf *types.NomsBinFormat, start, end *IntervalEndpoint) Interval {
	return Interval{nbf, start, end}
}

// Union takes the current set and another set and returns a set containing all values from both.
func (in Interval) Union(other Set) (Set, error) {
	switch otherTyped := other.(type) {
	case FiniteSet:
		return finiteSetIntervalUnion(otherTyped, in)
	case Interval:
		return intervalUnion(in, otherTyped)
	case CompositeSet:
		return intervalCompositeSetUnion(in, otherTyped)
	case EmptySet:
		return in, nil
	case UniversalSet:
		return otherTyped, nil
	}

	panic("unknown set type")

}

// Interset takes the current set and another set and returns a set containing the values that are in both
func (in Interval) Intersect(other Set) (Set, error) {
	switch otherTyped := other.(type) {
	case FiniteSet:
		return finiteSetIntervalIntersection(otherTyped, in)
	case Interval:
		return intervalIntersection(in, otherTyped)
	case CompositeSet:
		return intervalCompositeSetIntersection(in, otherTyped)
	case EmptySet:
		return EmptySet{}, nil

	case UniversalSet:
		return in, nil
	}

	panic("unknown set type")

}

// Contains returns true if the value falls within the bounds of the interval
func (in Interval) Contains(val types.Value) (bool, error) {
	if in.Start == nil && in.End == nil {
		// interval is open on both sides. full range includes everything
		return true, nil
	}

	// matchesStartCondition returns true if the value is greater than the start.  For a closed
	// interval it will also return true if it is equal to the start value
	var nbf *types.NomsBinFormat
	matchesStartCondition := func() (bool, error) {
		res, err := in.Start.Val.Less(nbf, val)

		if err != nil {
			return false, err
		}

		if res {
			return res, nil
		}

		if in.Start.Inclusive {
			return in.Start.Val.Equals(val), nil
		}

		return false, nil
	}

	// matchesStartCondition returns true if the value is less than the start.  For a closed
	// interval it will also return true if it is equal to the start value
	matchesEndCondition := func() (bool, error) {
		res, err := val.Less(nbf, in.End.Val)

		if err != nil {
			return false, err
		}

		if res {
			return res, nil
		}

		if in.End.Inclusive {
			return in.End.Val.Equals(val), nil
		}

		return false, nil
	}

	// if the interval is finite (has a start and end value) the result is true if both the start and
	// end condition check return true
	if in.Start != nil && in.End != nil {
		st, err := matchesStartCondition()

		if err != nil {
			return false, err
		}

		end, err := matchesEndCondition()

		if err != nil {
			return false, err
		}

		return st && end, nil
	} else if in.End == nil {
		// No end means the interval goes to positive infinity.  All values that match the start
		// condition are in the interval
		return matchesStartCondition()
	} else {
		// No start means the interval goes to negative infinity.  All values that match the end
		// condition are in the interval
		return matchesEndCondition()
	}
}

// intervalComparison contains the results of compareIntervals.
type intervalComparison [4]int

// intervalComparisonIndex is an enum which allows you to access the specific comparison of two
// points compared by a call to compareIntervals(...)
type intervalComparisonIndex int

const (
	start1start2 intervalComparisonIndex = iota
	start1end2
	end1start2
	end1end2
)

// noOverlapLess is the result you will get when comparing 2 intervals (A,B] and [C,D) where
// B is less than C
var noOverlapLess = intervalComparison{-1, -1, -1, -1}

// noOverlapGreater is the result you will get when comparing 2 intervals [A,B) and (C,D] where
// A is greater than D
var noOverlapGreater = intervalComparison{1, 1, 1, 1}

// compareIntervals compares the start and end points of one interval against the start and end
// points of another interval. The resulting intervalComparison is an array of 4 ints where a
// value of -1 means that the point in interval 1 is less than the point in interval 2. A value
// of 0 indicates equality, and a value of 1 indicates the value in interval 1 is greater than
// the value in interval 2.
func compareIntervals(in1, in2 Interval) (intervalComparison, error) {
	var err error
	var comp intervalComparison

	comp[start1start2], err = comparePoints(in1.nbf, in1.Start, in2.Start, false, false)

	if err != nil {
		return intervalComparison{}, nil
	}

	comp[start1end2], err = comparePoints(in1.nbf, in1.Start, in2.End, false, true)

	if err != nil {
		return intervalComparison{}, nil
	}

	comp[end1start2], err = comparePoints(in1.nbf, in1.End, in2.Start, true, false)

	if err != nil {
		return intervalComparison{}, nil
	}

	comp[end1end2], err = comparePoints(in1.nbf, in1.End, in2.End, true, true)

	if err != nil {
		return intervalComparison{}, nil
	}

	return comp, nil
}

// comparePoints compares two points from an interval
func comparePoints(nbf *types.NomsBinFormat, ep1, ep2 *IntervalEndpoint, p1IsEnd, p2IsEnd bool) (int, error) {
	lt, eq := false, false

	if ep1 == nil && ep2 == nil {
		// if both points are null they are only equivalent when comparing start points
		// or end points, and they are less when comparing a start point to an end point
		// but greater when comparing an end point to a start point
		if p1IsEnd == p2IsEnd {
			eq = true
		} else {
			lt = !p1IsEnd
		}
	} else if ep1 == nil {
		// if an intervalEndpoint is nil in the first point it will be less than the
		// second point if it is a nil start point. In all other cases it is greater,
		// and it can never be equal to a non nil intervalEndpoint
		lt = !p1IsEnd
	} else if ep2 == nil {
		// if an intervalEndpoint is nil in the second point then the first point will be less
		// if it is a nil end point. In all other cases it is greater, and it can never
		// be equal to a non nil intervalEndpoint
		lt = p2IsEnd
	} else {
		// compare 2 valid intervalEndpoints
		eq = ep1.Val.Equals(ep2.Val)

		if eq {
			if !ep1.Inclusive && !ep2.Inclusive {
				// If equal, but both intervalEndpoints are open, they are only equal if comparing two
				// start points or to end points. Otherwise they are not equal.
				if p1IsEnd != p2IsEnd {
					eq = false
					lt = p1IsEnd
				}
			} else if !ep1.Inclusive {
				// intervalEndpoints are not equal unless both are open, or both are closed
				eq = false
				lt = p1IsEnd
			} else if !ep2.Inclusive {
				// intervalEndpoints are not equal unless both are open, or both are closed
				eq = false
				lt = !p2IsEnd
			}
		} else {
			// both points are non nil and not equal so simply check to see if the first point is less
			// than the second.
			var err error
			lt, err = ep1.Val.Less(nbf, ep2.Val)

			if err != nil {
				return 0, err
			}
		}
	}

	if lt {
		return -1, nil
	} else if !eq {
		return 1, nil
	}

	return 0, nil
}

// simplifyInterval will return:
//  * a UniversalSet for an equivalent interval defined as: negative infinity < X < positive infinity
//  * a FiniteSet with a single value N for an equivalent interval defined as: N <= X <= N
//  * EmptySet for an interval defined as: N < X < N
//  * EmptySet for an interval where end < start
//  * an unchanged interval will be returned for all other conditions
func simplifyInterval(in Interval) (Set, error) {
	if in.Start == nil && in.End == nil {
		return UniversalSet{}, nil
	} else if in.Start != nil && in.End != nil {
		if in.Start.Val.Equals(in.End.Val) {
			if in.Start.Inclusive || in.End.Inclusive {
				return NewFiniteSet(in.nbf, in.Start.Val)
			} else {
				return EmptySet{}, nil
			}
		}

		endLessThanStart, err := in.End.Val.Less(in.nbf, in.Start.Val)
		if err != nil {
			return nil, err
		}

		if endLessThanStart {
			return EmptySet{}, nil
		}
	}

	return in, nil
}
