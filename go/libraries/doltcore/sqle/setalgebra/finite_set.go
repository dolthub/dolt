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

// FiniteSet is your typical computer science set representing a finite number of unique objects stored in a map. An
// example would be the set of strings {"red","blue","green"}, or the set of numbers {5, 73, 127}.
type FiniteSet struct {
	// HashToVal is a map from the noms hash of a value to it's value
	HashToVal map[hash.Hash]types.Value
}

// NewFiniteSet returns a FiniteSet constructed from the provided values
func NewFiniteSet(nbf *types.NomsBinFormat, vals ...types.Value) (FiniteSet, error) {
	hashToVal := make(map[hash.Hash]types.Value, len(vals))

	for _, val := range vals {
		h, err := val.Hash(nbf)

		if err != nil {
			return FiniteSet{}, err
		}

		hashToVal[h] = val
	}

	return FiniteSet{HashToVal: hashToVal}, nil
}

// Union takes the current set and another set and returns a set containing all values from both.
func (fs FiniteSet) Union(other Set) (Set, error) {
	switch otherTyped := other.(type) {
	// set / set union is all the values from both sets
	case FiniteSet:
		return finiteSetUnion(fs, otherTyped)
	case Interval:
		return finiteSetIntervalUnion(fs, otherTyped)
	case CompositeSet:
		return finiteSetCompositeSetUnion(fs, otherTyped)
	case EmptySet:
		return fs, nil
	case UniversalSet:
		return otherTyped, nil
	default:
		panic("unknown set type")
	}
}

// Interset takes the current set and another set and returns a set containing the values that are in both
func (fs FiniteSet) Intersect(other Set) (Set, error) {
	switch otherTyped := other.(type) {
	case FiniteSet:
		return finiteSetIntersection(fs, otherTyped)
	case Interval:
		return finiteSetIntervalIntersection(fs, otherTyped)
	case CompositeSet:
		return finiteSetCompositeSetIntersection(fs, otherTyped)
	case EmptySet:
		return otherTyped, nil
	case UniversalSet:
		return fs, nil
	default:
		panic("unknown set type")
	}
}
