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

import (
	"github.com/liquidata-inc/dolt/go/store/hash"
	"github.com/liquidata-inc/dolt/go/store/types"
)

// finiteSetUnion adds all points from both sets to a new FiniteSet
func finiteSetUnion(fs1, fs2 FiniteSet) (FiniteSet, error) {
	hashToVal := make(map[hash.Hash]types.Value, len(fs1.HashToVal)+len(fs2.HashToVal))
	for h, v := range fs1.HashToVal {
		hashToVal[h] = v
	}

	for h, v := range fs2.HashToVal {
		hashToVal[h] = v
	}

	return FiniteSet{HashToVal: hashToVal}, nil
}

// copyIntervalEndpoint makes a copy of an interval endpoint
func copyIntervalEndpoint(ep *IntervalEndpoint) *IntervalEndpoint {
	if ep == nil {
		return nil
	}

	copyOf := *ep

	return &copyOf
}

// finiteSetIntervalUnion will check all the points of the FiniteSet and see which ones are not in the given interval.
// if all points are in the interval then the resulting set is the interval itself, otherwise a CompositeSet containing
// the missing points as a new FiniteSet and the interval is returned.
func finiteSetIntervalUnion(fs FiniteSet, in Interval) (Set, error) {
	inStart := copyIntervalEndpoint(in.Start)
	inEnd := copyIntervalEndpoint(in.End)

	hashToVal := make(map[hash.Hash]types.Value, len(fs.HashToVal))
	for h, v := range fs.HashToVal {
		inRange, err := in.Contains(v)

		if err != nil {
			return nil, err
		}

		if !inRange {
			if inStart != nil && !inStart.Inclusive {
				if inStart.Val.Equals(v) {
					inStart.Inclusive = true
					continue
				}
			}

			if in.End != nil && !in.End.Inclusive {
				if in.End.Val.Equals(v) {
					inEnd.Inclusive = true
					continue
				}
			}

			hashToVal[h] = v
		}
	}

	resInterval := Interval{in.nbf, inStart, inEnd}
	if len(hashToVal) > 0 {
		newSet := FiniteSet{HashToVal: hashToVal}
		return CompositeSet{newSet, []Interval{resInterval}}, nil
	} else {
		return resInterval, nil
	}
}

// finiteSetCompositeSetUnion checks all the points in a FiniteSet against the CompositeSet to find all points not
// represented in the CompositeSet (So not in any of it's intervals and not in it's existing FiniteSet), and adds those
// points to the compositeSet
func finiteSetCompositeSetUnion(fs FiniteSet, cs CompositeSet) (Set, error) {
	hashToVal := make(map[hash.Hash]types.Value, len(fs.HashToVal))
	for h, v := range cs.Set.HashToVal {
		hashToVal[h] = v
	}

	for h, v := range fs.HashToVal {
		var inRange bool
		var err error
		for _, r := range cs.Intervals {
			inRange, err = r.Contains(v)

			if err != nil {
				return nil, err
			}

			if inRange {
				break
			}
		}

		if !inRange {
			hashToVal[h] = v
		}
	}

	return CompositeSet{FiniteSet{hashToVal}, cs.Intervals}, nil
}

// intervalUnion takes two Interval objects and compares them then returns their union
func intervalUnion(in1, in2 Interval) (Set, error) {
	intComparison, err := compareIntervals(in1, in2)

	if err != nil {
		return nil, err
	}

	return intervalUnionWithComparison(in1, in2, intComparison)
}

// intervalUnionWithComparison takes two Interval objects and their comparison and returns a new interval that
// represents all the points in both intervals where possible, and returns a CompositeInterval when the two intervals
// are non-overlapping.
func intervalUnionWithComparison(in1, in2 Interval, intComparison intervalComparison) (Set, error) {
	var resIntervToReduce Interval
	if intComparison == noOverlapLess {
		if in1.End != nil && in2.Start != nil && (in1.End.Inclusive || in2.Start.Inclusive) && in1.End.Val.Equals(in2.Start.Val) {
			// in the case where you have intervals X and Y defined as A < X < B and B <= Y < C the comparison of the
			// end of X and the start of Y will be -1.  But X includes all the points less than B, Y includes B and the
			// points up until C.  So the resulting interval Z would be A < Z < C.
			resIntervToReduce = Interval{in1.nbf, in1.Start, in2.End}
		} else {
			// Non overlapping intervals. Create CompositeSet with intervals in sorted order.
			return CompositeSet{FiniteSet{make(map[hash.Hash]types.Value)}, []Interval{in1, in2}}, nil
		}
	} else if intComparison == noOverlapGreater {
		if in2.End != nil && in1.Start != nil && (in2.End.Inclusive || in1.Start.Inclusive) && in2.End.Val.Equals(in1.Start.Val) {
			// see above for info no this case
			resIntervToReduce = Interval{in1.nbf, in2.Start, in1.End}
		} else {
			// Non overlapping intervals. Create CompositeSet with intervals in sorted order.
			return CompositeSet{FiniteSet{make(map[hash.Hash]types.Value)}, []Interval{in2, in1}}, nil
		}
	} else if intComparison[start1start2] <= 0 {
		if intComparison[end1end2] >= 0 {
			// the first interval wholly contains the second. Return the first.
			return in1, nil
		} else {
			// return an interval with the smallest start point and largest end point
			resIntervToReduce = Interval{in1.nbf, in1.Start, in2.End}
		}
	} else {
		if intComparison[end1end2] <= 0 {
			// the second interval wholly contains the first. Return the second.
			return in2, nil
		} else {
			// return an interval with the smallest start point and largest end point
			resIntervToReduce = Interval{in1.nbf, in2.Start, in1.End}
		}
	}

	return simplifyInterval(resIntervToReduce)
}

// intervalCompositeSetUnion will check the CompositeSet's FiniteSet for points that the new Interval contains
// and exclude those from the resulting composite and then union the Interval with its existing intervals.
func intervalCompositeSetUnion(in Interval, cs CompositeSet) (Set, error) {
	hashToVal := make(map[hash.Hash]types.Value)
	for h, v := range cs.Set.HashToVal {
		contained, err := in.Contains(v)

		if err != nil {
			return nil, err
		}

		if !contained {
			hashToVal[h] = v
		}
	}

	intervals, err := unionWithMultipleIntervals(in, cs.Intervals)

	if err != nil {
		return nil, err
	}

	if len(hashToVal) == 0 && len(intervals) == 1 {
		// could possibly be universal set
		return simplifyInterval(intervals[0])
	} else {
		return CompositeSet{FiniteSet{hashToVal}, intervals}, nil
	}
}

// unionWithMultipleIntervals takes an interval and a slice of intervals and returns a slice of intervals containing
// the minimum number of intervals required to represent the union.  The src []Interval argument must be in sorted
// order and only contain non-overlapping intervals.
func unionWithMultipleIntervals(in Interval, src []Interval) ([]Interval, error) {
	dest := make([]Interval, 0, len(src)+1)

	// iterate in sorted order
	for i, curr := range src {
		intComparison, err := compareIntervals(in, curr)

		if err != nil {
			return nil, err
		}

		if intComparison == noOverlapLess {
			// new interval is wholly less than the curr Interval. Check to see if we a case where we can combine them
			// into a single interval (described in intervalUnionWithComparison)
			if in.End != nil && curr.Start != nil && (in.End.Inclusive || curr.Start.Inclusive) && in.End.Val.Equals(curr.Start.Val) {
				// modify the input Interval object to include the curr interval
				in = Interval{in.nbf, in.Start, curr.End}
				continue
			}

			// current interval is before all remaining intervals.  Add it and then add all the remaining intervals
			dest = append(dest, in)
			in = Interval{}
			dest = append(dest, src[i:]...)
			break
		} else if intComparison == noOverlapGreater {
			// new interval is wholly greater than the curr Interval. Check to see if we a case where we can combine them
			// into a single interval (described in intervalUnionWithComparison)
			if curr.End != nil && in.Start != nil && (curr.End.Inclusive || in.Start.Inclusive) && curr.End.Val.Equals(in.Start.Val) {
				// modify the input Interval object to include the curr interval
				in = Interval{in.nbf, curr.Start, in.End}
				continue
			}

			// add the current interval, and leave the input Interval object unchanged
			dest = append(dest, curr)
		} else {
			// input interval overlaps with the curr interval.  update the input Interval object to be the
			// entire interval
			un, err := intervalUnionWithComparison(in, curr, intComparison)

			if err != nil {
				return nil, err
			}

			switch typedVal := un.(type) {
			case UniversalSet:
				return []Interval{{in.nbf, nil, nil}}, nil
			case Interval:
				in = typedVal
			default:
				panic("Should not be possible.")
			}
		}
	}

	if in.nbf != nil {
		dest = append(dest, in)
	}

	return dest, nil
}

// addIfNotInIntervals adds a value to the provided hashToValue map if a value in the FiniteSet passed in is not in any
// of the intervals.
func addIfNotInIntervals(hashToValue map[hash.Hash]types.Value, fs FiniteSet, intervals []Interval) error {
	var err error
	for h, v := range fs.HashToVal {
		var found bool
		for _, in := range intervals {
			found, err = in.Contains(v)
			if err != nil {
				return err
			}

			if found {
				break
			}
		}

		if !found {
			hashToValue[h] = v
		}
	}

	return nil
}

// findUniqueFiniteSetForComposites takes the values from cs1.Set and adds the ones not contained in cs2.Intervals,
// and then takes the values from cs2.Set and adds the ones not contained in cs1.Intervals then returns the
// resulting FiniteSet
func findUniqueFiniteSetForComposites(cs1, cs2 CompositeSet) (FiniteSet, error) {
	hashToVal := make(map[hash.Hash]types.Value)
	err := addIfNotInIntervals(hashToVal, cs1.Set, cs2.Intervals)

	if err != nil {
		return FiniteSet{}, err
	}

	err = addIfNotInIntervals(hashToVal, cs2.Set, cs1.Intervals)

	if err != nil {
		return FiniteSet{}, err
	}

	return FiniteSet{hashToVal}, nil
}

// compositeUnion returns the union of 2 CompositeSets
func compositeUnion(cs1, cs2 CompositeSet) (Set, error) {
	fs, err := findUniqueFiniteSetForComposites(cs1, cs2)

	if err != nil {
		return nil, err
	}

	intervals := cs1.Intervals
	for _, currInterval := range cs2.Intervals {
		intervals, err = unionWithMultipleIntervals(currInterval, intervals)

		if err != nil {
			return nil, err
		} else if len(intervals) == 1 && intervals[0].Start == nil && intervals[0].End == nil {
			return UniversalSet{}, nil
		}
	}

	if len(intervals) == 1 {
		return intervals[0], nil
	}

	return CompositeSet{fs, intervals}, nil
}
