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
	"errors"
	"fmt"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

type nomsRow struct {
	key   TaggedValues
	value TaggedValues
	nbf   *types.NomsBinFormat
}

var _ Row = nomsRow{}

func pkRowFromNoms(sch schema.Schema, nomsKey, nomsVal types.Tuple) (Row, error) {
	keySl, err := nomsKey.AsSlice()
	if err != nil {
		return nil, err
	}

	valSl, err := nomsVal.AsSlice()
	if err != nil {
		return nil, err
	}

	allCols := sch.GetAllCols()

	err = IterPkTuple(keySl, func(tag uint64, val types.Value) (stop bool, err error) {
		// The IsKeyless check in FromNoms misses keyless index schemas, even though
		// the output tuple is a keyless index that contains a KeylessRowIdTag.
		// NomsRangeReader breaks without this.
		// A longer term fix could separate record vs index parsing, each of
		// which is different for keyless vs keyed tables.
		if tag == schema.KeylessRowIdTag {
			return false, nil
		}
		col, ok := allCols.GetByTag(tag)

		if !ok {
			return false, errors.New("Trying to set a value on an unknown tag is a bug for the key.  Validation should happen upstream. col:" + col.Name)
		} else if !col.IsPartOfPK {
			return false, errors.New("writing columns that are not part of the primary key to pk values. col:" + col.Name)
		} else if !types.IsNull(val) && col.Kind != val.Kind() {
			return false, errors.New("bug.  Setting a value to an incorrect kind. col: " + col.Name)
		}

		return false, nil
	})
	if err != nil {
		return nil, err
	}

	filteredVals := make(TaggedValues, len(valSl))
	err = IterPkTuple(valSl, func(tag uint64, val types.Value) (stop bool, err error) {
		col, ok := allCols.GetByTag(tag)
		if !ok {
			return false, nil
		}

		if col.IsPartOfPK {
			return false, errors.New("writing columns that are part of the primary key to non-pk values. col:" + col.Name)
		} else if !types.IsNull(val) {
			// Column is GeometryKind and received PointKind, LinestringKind, or PolygonKind
			if col.Kind == types.GeometryKind && types.IsGeometryKind(val.Kind()) {
				filteredVals[tag] = val
			} else if col.Kind == val.Kind() {
				filteredVals[tag] = val
			} else {
				return false, errors.New("bug.  Setting a value to an incorrect kind. col:" + col.Name)
			}

		}

		return false, nil
	})
	if err != nil {
		return nil, err
	}

	taggedKeyVals, err := TaggedValuesFromTupleValueSlice(keySl)
	if err != nil {
		return nil, err
	}

	return nomsRow{taggedKeyVals, filteredVals, nomsKey.Format()}, nil
}

func (nr nomsRow) IterSchema(sch schema.Schema, cb func(tag uint64, val types.Value) (stop bool, err error)) (bool, error) {
	err := sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (bool, error) {
		value, _ := nr.GetColVal(tag)
		return cb(tag, value)
	})

	return false, err
}

func (nr nomsRow) IterCols(cb func(tag uint64, val types.Value) (bool, error)) (bool, error) {
	stopped, err := nr.key.Iter(cb)

	if err != nil {
		return false, err
	}

	if !stopped {
		stopped, err = nr.value.Iter(cb)
	}

	if err != nil {
		return false, err
	}

	return stopped, nil
}

func (nr nomsRow) GetColVal(tag uint64) (types.Value, bool) {
	val, ok := nr.key.Get(tag)

	if !ok {
		val, ok = nr.value.Get(tag)
	}

	return val, ok
}

func (nr nomsRow) SetColVal(tag uint64, val types.Value, sch schema.Schema) (Row, error) {
	rowKey := nr.key
	rowVal := nr.value

	cols := sch.GetAllCols()
	col, ok := cols.GetByTag(tag)

	if ok {
		if col.IsPartOfPK {
			rowKey = nr.key.Set(tag, val)
		} else {
			rowVal = nr.value.Set(tag, val)
		}

		return nomsRow{rowKey, rowVal, nr.nbf}, nil
	}

	panic("can't set a column whose tag isn't in the schema.  verify before calling this function.")
}

func (nr nomsRow) Format() *types.NomsBinFormat {
	return nr.nbf
}

// TaggedValues implements the Row interface.
func (nr nomsRow) TaggedValues() (TaggedValues, error) {
	tv := make(TaggedValues)
	for k, v := range nr.key {
		tv[k] = v
	}
	for k, v := range nr.value {
		tv[k] = v
	}
	return tv, nil
}

func pkRowFromTaggedValues(nbf *types.NomsBinFormat, sch schema.Schema, colVals TaggedValues) (Row, error) {
	allCols := sch.GetAllCols()

	keyVals := make(TaggedValues)
	nonKeyVals := make(TaggedValues)

	_, err := colVals.Iter(func(tag uint64, val types.Value) (stop bool, err error) {
		col, ok := allCols.GetByTag(tag)

		if !ok {
			return false, errors.New("Trying to set a value on an unknown tag is a bug.  Validation should happen upstream.")
		} else if col.IsPartOfPK {
			keyVals[tag] = val
		} else {
			nonKeyVals[tag] = val
		}
		return false, nil
	})

	if err != nil {
		return nil, err
	}

	return fromTaggedVals(nbf, sch, keyVals, nonKeyVals)
}

// fromTaggedVals will take a schema, a map of tag to value for the key, and a map of tag to value for non key values,
// and generates a row.  When a schema adds or removes columns from the non-key portion of the row, the schema will be
// updated, but the rows will not be touched.  So the non-key portion of the row may contain values that are not in the
// schema (The keys must match the schema though).
func fromTaggedVals(nbf *types.NomsBinFormat, sch schema.Schema, keyVals, nonKeyVals TaggedValues) (Row, error) {
	allCols := sch.GetAllCols()

	_, err := keyVals.Iter(func(tag uint64, val types.Value) (stop bool, err error) {
		col, ok := allCols.GetByTag(tag)

		if !ok {
			return false, errors.New("Trying to set a value on an unknown tag is a bug for the key.  Validation should happen upstream. col:" + col.Name)
		} else if !col.IsPartOfPK {
			return false, errors.New("writing columns that are not part of the primary key to pk values. col:" + col.Name)
		} else if !types.IsNull(val) && col.Kind != val.Kind() {
			if col.Kind == types.GeometryKind && types.IsGeometryKind(val.Kind()) {
				return false, nil
			}
			return false, errors.New("bug.  Setting a value to an incorrect kind. col: " + col.Name)
		}

		return false, nil
	})

	if err != nil {
		return nil, err
	}

	filteredVals := make(TaggedValues, len(nonKeyVals))
	_, err = nonKeyVals.Iter(func(tag uint64, val types.Value) (stop bool, err error) {
		col, ok := allCols.GetByTag(tag)
		if !ok {
			return false, nil
		}

		if col.IsPartOfPK {
			return false, errors.New("writing columns that are part of the primary key to non-pk values. col:" + col.Name)
		} else if !types.IsNull(val) && col.Kind != val.Kind() {
			if col.Kind == types.GeometryKind && types.IsGeometryKind(val.Kind()) {
				filteredVals[tag] = val
				return false, nil
			}
			return false, errors.New("bug.  Setting a value to an incorrect kind. col:" + col.Name)
		} else {
			filteredVals[tag] = val
		}

		return false, nil
	})

	if err != nil {
		return nil, err
	}

	return nomsRow{keyVals, filteredVals, nbf}, nil
}

func (nr nomsRow) NomsMapKey(sch schema.Schema) types.LesserValuable {
	return nr.key.NomsTupleForPKCols(nr.nbf, sch.GetPKCols())
}

func (nr nomsRow) NomsMapValue(sch schema.Schema) types.Valuable {
	return nr.value.NomsTupleForNonPKCols(nr.nbf, sch.GetNonPKCols())
}

func (nr nomsRow) NomsMapKeyTuple(sch schema.Schema, tf *types.TupleFactory) (types.Tuple, error) {
	tv := nr.key.NomsTupleForPKCols(nr.nbf, sch.GetPKCols())

	if tf != nil {
		return tf.Create(tv.vs...)
	} else {
		return types.NewTuple(tv.nbf, tv.vs...)
	}
}

func (nr nomsRow) NomsMapValueTuple(sch schema.Schema, tf *types.TupleFactory) (types.Tuple, error) {
	tv := nr.value.NomsTupleForNonPKCols(nr.nbf, sch.GetNonPKCols())

	if tf != nil {
		return tf.Create(tv.vs...)
	} else {
		return types.NewTuple(tv.nbf, tv.vs...)
	}
}

// ReduceToIndexKeys creates a full key, partial key, and value tuple from the given row (first tuple being the full key). Please
// refer to the note in the index editor for more information regarding partial keys. NomsRows map always
// keys to an empty value tuple.
func (nr nomsRow) ReduceToIndexKeys(idx schema.Index, tf *types.TupleFactory) (types.Tuple, types.Tuple, types.Tuple, error) {
	vals := make([]types.Value, 0, len(idx.AllTags())*2)
	for _, tag := range idx.AllTags() {
		val, ok := nr.GetColVal(tag)
		if !ok {
			val = types.NullValue
		}
		vals = append(vals, types.Uint(tag), val)
	}

	var err error
	var fullKey types.Tuple
	var partialKey types.Tuple
	if tf == nil {
		fullKey, err = types.NewTuple(nr.Format(), vals...)
		if err != nil {
			return types.Tuple{}, types.Tuple{}, types.Tuple{}, err
		}
		partialKey, err = types.NewTuple(nr.Format(), vals[:idx.Count()*2]...)
		if err != nil {
			return types.Tuple{}, types.Tuple{}, types.Tuple{}, err
		}
	} else {
		fullKey, err = tf.Create(vals...)
		if err != nil {
			return types.Tuple{}, types.Tuple{}, types.Tuple{}, err
		}
		partialKey, err = tf.Create(vals[:idx.Count()*2]...)
		if err != nil {
			return types.Tuple{}, types.Tuple{}, types.Tuple{}, err
		}
	}

	return fullKey, partialKey, types.EmptyTuple(nr.Format()), nil
}

func IterPkTuple(tvs types.TupleValueSlice, cb func(tag uint64, val types.Value) (stop bool, err error)) error {
	if len(tvs)%2 != 0 {
		return fmt.Errorf("expected len(TupleValueSlice) to be even, got %d", len(tvs))
	}

	l := len(tvs)
	for i := 0; i < l; i += 2 {
		stop, err := cb(uint64(tvs[i].(types.Uint)), tvs[i+1])

		if err != nil {
			return err
		}

		if stop {
			break
		}
	}

	return nil
}
