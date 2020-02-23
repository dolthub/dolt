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

// CompositeSet is a set which is made up of a FiniteSet and one or more non overlapping intervals such as
// {n | n < 0 or n > 100} (set of all numbers n below 0 or greater than 100) this set contains 2 non overlapping intervals
// and an empty finite set. Alternatively {n | n < 0 or {5,10,15}} (set of all numbers n below 0 or n equal to 5, 10 or 15)
// which would be represented by one Interval and a FiniteSet containing 5,10, and 15.
type CompositeSet struct {
	// Set contains a set of points in the set.  None of these points should be in the Intervals
	Set FiniteSet
	// Intervals is a slice of non overlapping Interval objects in sorted order.
	Intervals []Interval
}

// Union takes the current set and another set and returns a set containing all values from both.
func (cs CompositeSet) Union(other Set) (Set, error) {
	switch otherTyped := other.(type) {
	case FiniteSet:
		return finiteSetCompositeSetUnion(otherTyped, cs)
	case Interval:
		return intervalCompositeSetUnion(otherTyped, cs)
	case CompositeSet:
		return compositeUnion(cs, otherTyped)
	case EmptySet:
		return cs, nil
	case UniversalSet:
		return otherTyped, nil
	}

	panic("unknown set type")

}

// Interset takes the current set and another set and returns a set containing the values that are in both
func (cs CompositeSet) Intersect(other Set) (Set, error) {
	switch otherTyped := other.(type) {
	case FiniteSet:
		return finiteSetCompositeSetIntersection(otherTyped, cs)
	case Interval:
		return intervalCompositeSetIntersection(otherTyped, cs)
	case CompositeSet:
		return compositeIntersection(otherTyped, cs)
	case EmptySet:
		return EmptySet{}, nil
	case UniversalSet:
		return cs, nil
	}

	panic("unknown set type")
}
