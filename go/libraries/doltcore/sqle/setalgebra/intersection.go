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
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

// finiteSetIntersection returns the set of points that are in both fs1 and fs2
func finiteSetIntersection(fs1, fs2 FiniteSet) (Set, error) {
	hashToVal := make(map[hash.Hash]types.Value)
	for h, v := range fs1.HashToVal {
		if _, ok := fs2.HashToVal[h]; ok {
			hashToVal[h] = v
		}
	}

	if len(hashToVal) == 0 {
		return EmptySet{}, nil
	} else {
		return FiniteSet{HashToVal: hashToVal}, nil
	}
}

// finiteSetInterval will return the set of points that are in the interval, or an EmptySet instance
func finiteSetIntervalIntersection(fs FiniteSet, in Interval) (Set, error) {
	hashToVal := make(map[hash.Hash]types.Value)
	for h, v := range fs.HashToVal {
		inRange, err := in.Contains(v)

		if err != nil {
			return nil, err
		}

		if inRange {
			hashToVal[h] = v
		}
	}

	if len(hashToVal) == 0 {
		return EmptySet{}, nil
	} else {
		return FiniteSet{HashToVal: hashToVal}, nil
	}
}

// finiteSetInterval will return the set of points that are in the composite set, or an EmptySet instance
func finiteSetCompositeSetIntersection(fs FiniteSet, composite CompositeSet) (Set, error) {
	hashToVal := make(map[hash.Hash]types.Value)
	for h, v := range fs.HashToVal {
		if _, ok := composite.Set.HashToVal[h]; ok {
			hashToVal[h] = v
		}
	}

	for _, r := range composite.Intervals {
		for h, v := range fs.HashToVal {
			inRange, err := r.Contains(v)

			if err != nil {
				return nil, err
			}

			if inRange {
				hashToVal[h] = v
			}
		}
	}

	if len(hashToVal) == 0 {
		return EmptySet{}, nil
	} else {
		return FiniteSet{HashToVal: hashToVal}, nil
	}
}

// intervalIntersection will return the intersection of two intervals.  This will either be the interval where they
// overlap, or an EmptySet instance.
func intervalIntersection(in1, in2 Interval) (Set, error) {
	intComparison, err := compareIntervals(in1, in2)

	if err != nil {
		return nil, err
	}

	var resIntervToReduce Interval
	if intComparison == noOverlapLess || intComparison == noOverlapGreater {
		// No overlap
		return EmptySet{}, nil
	} else if intComparison[start1start2] <= 0 {
		if intComparison[end1end2] >= 0 {
			// in2 fully contained in in1
			return in2, nil
		} else {
			// in1 starts first and in2 ends last.  take the inside points for the intersection
			resIntervToReduce = Interval{in1.nbf, in2.Start, in1.End}
		}
	} else {
		if intComparison[end1end2] <= 0 {
			// in1 fully contained in in2
			return in1, nil
		} else {
			// in2 starts first and in1 ends last.  take the inside points for the intersection
			resIntervToReduce = Interval{in1.nbf, in1.Start, in2.End}
		}
	}

	return simplifyInterval(resIntervToReduce)
}

// intervalCompositeSetIntersection will intersect the interval with all sets in the composite and return the resulting
// set of points and intervals that are in the interval and the CompositeSet
func intervalCompositeSetIntersection(in Interval, cs CompositeSet) (Set, error) {
	hashToVal := make(map[hash.Hash]types.Value)

	// check the existing finite set and eliminate values not in the new interval
	for h, v := range cs.Set.HashToVal {
		contained, err := in.Contains(v)

		if err != nil {
			return nil, err
		}

		if contained {
			hashToVal[h] = v
		}
	}

	// intersect the new interval with all the existing intervals
	intervals, err := intersectIntervalWithMultipleIntervals(in, cs.Intervals, hashToVal)
	if err != nil {
		return nil, err
	}

	if len(hashToVal) == 0 && len(intervals) == 1 {
		// could possibly be universal set
		return simplifyInterval(intervals[0])
	} else if len(hashToVal) > 0 && len(intervals) == 0 {
		return FiniteSet{hashToVal}, nil
	} else if len(hashToVal) > 0 && len(intervals) > 0 {
		return CompositeSet{FiniteSet{hashToVal}, intervals}, nil
	} else {
		return EmptySet{}, nil
	}
}

// intersectIntervalWithMultipleIntervals returns a slice of Interval objects containing all intersections between the
// input interval and the slice of multiple intervals.
func intersectIntervalWithMultipleIntervals(in Interval, multipleIntervals []Interval, hashToVal map[hash.Hash]types.Value) ([]Interval, error) {
	intervals := make([]Interval, 0, len(multipleIntervals))
	for _, curr := range multipleIntervals {
		intersection, err := intervalIntersection(in, curr)

		if err != nil {
			return nil, err
		}

		switch typedSet := intersection.(type) {
		case EmptySet:
			continue
		case FiniteSet:
			for h, v := range typedSet.HashToVal {
				hashToVal[h] = v
			}
		case Interval:
			intervals = append(intervals, typedSet)
		default:
			panic("unexpected set type")
		}
	}
	return intervals, nil
}

// compositeIntersection finds the intersection of two composite sets
func compositeIntersection(cs1, cs2 CompositeSet) (Set, error) {
	// intersect cs1.Set with cs2 and cs2.Set with cs1 to get the discreet values in the resulting intersection.
	temp1, err := finiteSetCompositeSetIntersection(cs1.Set, cs2)

	if err != nil {
		return nil, err
	}

	temp2, err := finiteSetCompositeSetIntersection(cs2.Set, cs1)

	if err != nil {
		return nil, err
	}

	resSet, err := temp1.Union(temp2)

	if err != nil {
		return nil, err
	}

	var hashToVal map[hash.Hash]types.Value
	switch typedResSet := resSet.(type) {
	case EmptySet:
		hashToVal = make(map[hash.Hash]types.Value)
	case FiniteSet:
		hashToVal = typedResSet.HashToVal
	default:
		panic("unexpected set type")
	}

	// intersect the intervals
	var intervals []Interval
	for _, curr := range cs1.Intervals {
		newIntervals, err := intersectIntervalWithMultipleIntervals(curr, cs2.Intervals, hashToVal)

		if err != nil {
			return nil, err
		}

		intervals = append(intervals, newIntervals...)
	}

	// combine the intervals and the discreet values into a result
	if len(hashToVal) == 0 && len(intervals) == 1 {
		return simplifyInterval(intervals[0])
	} else if len(hashToVal) > 0 && len(intervals) == 0 {
		return FiniteSet{hashToVal}, nil
	} else if len(hashToVal) > 0 && len(intervals) > 0 {
		return CompositeSet{FiniteSet{hashToVal}, intervals}, nil
	} else {
		return EmptySet{}, nil
	}
}
