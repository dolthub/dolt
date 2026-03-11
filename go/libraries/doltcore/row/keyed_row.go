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

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

type keyedRow struct {
	key   TaggedValues
	value TaggedValues
}

var _ Row = keyedRow{}

func (nr keyedRow) IterSchema(sch schema.Schema, cb func(tag uint64, val types.Value) (stop bool, err error)) (bool, error) {
	err := sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (bool, error) {
		value, _ := nr.GetColVal(tag)
		return cb(tag, value)
	})

	return false, err
}

func (nr keyedRow) GetColVal(tag uint64) (types.Value, bool) {
	val, ok := nr.key.Get(tag)

	if !ok {
		val, ok = nr.value.Get(tag)
	}

	return val, ok
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

	return keyedRow{key: keyVals, value: filteredVals}, nil
}
