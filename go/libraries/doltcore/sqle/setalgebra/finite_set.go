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

type FiniteSet struct {
	HashToVal map[hash.Hash]types.Value
}

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
	}

	panic("unknown set type")
}

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
	}

	panic("unknown set type")
}
