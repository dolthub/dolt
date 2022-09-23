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

func (tt TaggedValues) ToRow(ctx context.Context, nbf *types.NomsBinFormat, sch schema.Schema) (Row, error) {
	keyVals := tt.NomsTupleForNonPKCols(nbf, sch.GetPKCols())
	valVals := tt.NomsTupleForNonPKCols(nbf, sch.GetNonPKCols())
	key, err := keyVals.Value(ctx)

	if err != nil {
		return nil, err
	}

	val, err := valVals.Value(ctx)

	if err != nil {
		return nil, err
	}

	return FromNoms(sch, key.(types.Tuple), val.(types.Tuple))
}

func (tt TaggedValues) NomsTupleForPKCols(nbf *types.NomsBinFormat, pkCols *schema.ColCollection) TupleVals {
	return tt.nomsTupleForTags(nbf, pkCols.Tags, true)
}

func (tt TaggedValues) NomsTupleForNonPKCols(nbf *types.NomsBinFormat, nonPKCols *schema.ColCollection) TupleVals {
	return tt.nomsTupleForTags(nbf, nonPKCols.SortedTags, false)
}

func (tt TaggedValues) nomsTupleForTags(nbf *types.NomsBinFormat, tags []uint64, encodeNulls bool) TupleVals {
	vals := make([]types.Value, 0, 2*len(tags))
	for _, tag := range tags {
		val := tt[tag]

		if types.IsNull(val) && !encodeNulls {
			continue
		} else if val == nil {
			val = types.NullValue
		}

		vals = append(vals, types.Uint(tag))
		vals = append(vals, val)
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

func TaggedValuesFromTupleKeyAndValue(key, value types.Tuple) (TaggedValues, error) {
	tv := make(TaggedValues)
	err := AddToTaggedVals(tv, key)

	if err != nil {
		return nil, err
	}

	err = AddToTaggedVals(tv, value)

	if err != nil {
		return nil, err
	}

	return tv, nil
}

func AddToTaggedVals(tv TaggedValues, t types.Tuple) error {
	return IterDoltTuple(t, func(tag uint64, val types.Value) error {
		tv[tag] = val
		return nil
	})
}

func IterDoltTuple(t types.Tuple, cb func(tag uint64, val types.Value) error) error {
	itr, err := t.Iterator()

	if err != nil {
		return err
	}

	for itr.HasMore() {
		_, tag, err := itr.NextUint64()

		if err != nil {
			return err
		}

		_, currVal, err := itr.Next()

		if err != nil {
			return err
		}

		err = cb(tag, currVal)

		if err != nil {
			return err
		}
	}

	return nil
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

// CountCellDiffs returns the number of fields that are different between two
// tuples and does not panic if tuples are different lengths.
func CountCellDiffs(from, to types.Tuple, fromSch, toSch schema.Schema) (uint64, error) {
	fromColLen := len(fromSch.GetAllCols().GetColumns())
	toColLen := len(toSch.GetAllCols().GetColumns())
	changed := 0
	f, err := ParseTaggedValues(from)
	if err != nil {
		return 0, err
	}

	t, err := ParseTaggedValues(to)
	if err != nil {
		return 0, err
	}

	for i, v := range f {
		ov, ok := t[i]
		// !ok means t[i] has NULL value, and it is not cell modify if it was from drop column or add column
		if (!ok && fromColLen == toColLen) || (ok && !v.Equals(ov)) {
			changed++
		}
	}

	for i := range t {
		if f[i] == nil {
			changed++
		}
	}

	return uint64(changed), nil
}
