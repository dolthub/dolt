// Copyright 2019 Liquidata, Inc.
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

package row

import (
	"context"
	"fmt"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

type TaggedValues map[uint64]types.Value

type TupleVals struct {
	vs  []types.Value
	nbf *types.NomsBinFormat
}

func (tvs TupleVals) Kind() types.NomsKind {
	return types.TupleKind
}

func (tvs TupleVals) Value(ctx context.Context) (types.Value, error) {
	return types.NewTuple(tvs.nbf, tvs.vs...)
}

func (tvs TupleVals) Less(nbf *types.NomsBinFormat, other types.LesserValuable) (bool, error) {
	if other.Kind() == types.TupleKind {
		if otherTVs, ok := other.(TupleVals); ok {
			for i, val := range tvs.vs {
				if i == len(otherTVs.vs) {
					// equal up til the end of other. other is shorter, therefore it is less
					return false, nil
				}

				otherVal := otherTVs.vs[i]

				if !val.Equals(otherVal) {
					return val.Less(nbf, otherVal)
				}
			}

			return len(tvs.vs) < len(otherTVs.vs), nil
		} else {
			panic("not supported")
		}
	}

	return types.TupleKind < other.Kind(), nil
}

func (tt TaggedValues) NomsTupleForPKCols(nbf *types.NomsBinFormat, pkCols *schema.ColCollection) TupleVals {
	return tt.nomsTupleForTags(nbf, pkCols.Tags, true)
}

func (tt TaggedValues) NomsTupleForNonPKCols(nbf *types.NomsBinFormat, nonPKCols *schema.ColCollection) TupleVals {
	return tt.nomsTupleForTags(nbf, nonPKCols.SortedTags, false)
}

func (tt TaggedValues) nomsTupleForTags(nbf *types.NomsBinFormat, tags []uint64, encodeNulls bool) TupleVals {
	numVals := 0
	for _, tag := range tags {
		val := tt[tag]

		if val != nil || encodeNulls {
			numVals++
		}
	}

	i := 0
	vals := make([]types.Value, 2*numVals)
	for _, tag := range tags {
		val := tt[tag]

		if val == nil && encodeNulls {
			val = types.NullValue
		}

		if val != nil {
			vals[i*2] = types.Uint(tag)
			vals[i*2+1] = val
			i++
		}
	}

	return TupleVals{vals, nbf}
}

func (tt TaggedValues) Iter(cb func(tag uint64, val types.Value) (stop bool, err error)) (bool, error) {
	stop := false

	var err error
	for tag, val := range tt {
		stop, err = cb(tag, val)

		if stop || err != nil {
			break
		}
	}

	return stop, err
}

func (tt TaggedValues) Get(tag uint64) (types.Value, bool) {
	val, ok := tt[tag]
	return val, ok
}

func (tt TaggedValues) GetWithDefault(tag uint64, def types.Value) types.Value {
	val, ok := tt[tag]

	if !ok {
		return def
	}

	return val
}

func (tt TaggedValues) Set(tag uint64, val types.Value) TaggedValues {
	updated := tt.copy()
	// Setting a nil value removes the mapping for that tag entirely, rather than setting a nil value. The methods to
	// write to noms treat a nil value the same as an absent value.
	if val != nil {
		updated[tag] = val
	} else {
		delete(updated, tag)
	}

	return updated
}

func (tt TaggedValues) copy() TaggedValues {
	newTagToVal := make(TaggedValues, len(tt))
	for tag, val := range tt {
		newTagToVal[tag] = val
	}

	return newTagToVal
}

func ParseTaggedValues(tpl types.Tuple) (TaggedValues, error) {
	vals, err := tpl.AsSlice()

	if err != nil {
		return nil, err
	}

	return TaggedValuesFromTupleValueSlice(vals)
}

func TaggedValuesFromTupleValueSlice(vals types.TupleValueSlice) (TaggedValues, error) {
	valCount := len(vals)
	if valCount%2 != 0 {
		panic("A tagged tuple must have an even column count.")
	}

	taggedTuple := make(TaggedValues, valCount/2)
	for i, j := 0, 0; j < valCount; i, j = i+1, j+2 {
		tag := vals[j]
		val := vals[j+1]

		if tag.Kind() != types.UintKind {
			panic("Invalid tagged tuple must have uint tags.")
		}

		if val != types.NullValue {
			taggedTuple[uint64(tag.(types.Uint))] = val
		}
	}

	return taggedTuple, nil
}

func (tt TaggedValues) String() string {
	str := "{"
	for k, v := range tt {
		encStr, err := types.EncodedValue(context.Background(), v)

		if err != nil {
			return err.Error()
		}

		str += fmt.Sprintf("\n\t%d: %s", k, encStr)
	}

	str += "\n}"
	return str
}
