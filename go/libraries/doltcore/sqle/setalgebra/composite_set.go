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

type CompositeSet struct {
	Set       FiniteSet
	Intervals []Interval
}

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
