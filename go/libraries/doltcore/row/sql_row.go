// Copyright 2020 Dolthub, Inc.
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
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

// SqlRowFromTuples constructs a go-mysql-server/sql.Row from Noms tuples.
func SqlRowFromTuples(sch schema.Schema, key, val types.Tuple) (sql.Row, error) {
	allCols := sch.GetAllCols()
	colVals := make(sql.Row, allCols.Size())

	keySl, err := key.AsSlice()
	if err != nil {
		return nil, err
	}
	valSl, err := val.AsSlice()
	if err != nil {
		return nil, err
	}

	for _, sl := range []types.TupleValueSlice{keySl, valSl} {
		var convErr error
		err := iterPkTuple(sl, func(tag uint64, val types.Value) (stop bool, err error) {
			if idx, ok := allCols.TagToIdx[tag]; ok {
				col := allCols.GetByIndex(idx)
				colVals[idx], convErr = col.TypeInfo.ConvertNomsValueToValue(val)

				if convErr != nil {
					return false, err
				}
			}

			return false, nil
		})

		if err != nil {
			return nil, err
		}
	}

	return sql.NewRow(colVals...), nil
}

// todo: ensure correct column ordering
func EncodeKeylessSqlRows(nbf *types.NomsBinFormat, sch schema.Schema, r sql.Row, count uint64) (key, val types.Tuple, err error) {
	if len(r) != sch.GetAllCols().Size() {
		rl, sl := len(r), sch.GetAllCols().Size()
		return key, val, fmt.Errorf("row length (%d) != schema length (%d)", rl, sl)
	}

	size := 0
	for _, v := range r {
		// skip NULLS
		if v != nil {
			size++
		}
	}

	// { Uint(count), Uint(tag1), Value(val1), ..., Uint(tagN), Value(valN) }
	vals := make([]types.Value, 1+(size*2))
	vals[0] = types.Uint(count)

	idx := 0
	err = sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		v := r[idx]
		if v != nil {
			vals[2*idx+1] = types.Uint(tag)
			vals[2*idx+2], err = col.TypeInfo.ConvertValueToNomsValue(v)
		}
		idx++

		stop = err != nil
		return
	})
	if err != nil {
		return key, val, err
	}

	id, err := types.UUIDHashedFromValues(nbf, vals[1:]...)
	if err != nil {
		return key, val, err
	}

	key, err = types.NewTuple(nbf, id)
	if err != nil {
		return key, val, err
	}

	val, err = types.NewTuple(nbf, vals...)
	if err != nil {
		return key, val, err
	}

	return key, val, nil
}

func DecodeKeylessSqlRow(sch schema.Schema, val types.Tuple) (r sql.Row, count uint64, err error) {
	allCols := sch.GetAllCols()
	colVals := make(sql.Row, allCols.Size())

	sl, err := val.AsSlice()
	if err != nil {
		return r, count, err
	}
	count = uint64(sl[0].(types.Uint))

	err = iterKeylessTuple(sl, func(tag uint64, val types.Value) (stop bool, err error) {
		if idx, ok := allCols.TagToIdx[tag]; ok {
			col := allCols.GetByIndex(idx)
			colVals[idx], err = col.TypeInfo.ConvertNomsValueToValue(val)
			return
		}

		return false, nil
	})
	if err != nil {
		return r, count, err
	}

	return sql.NewRow(colVals...), count, nil
}

// DoltRowToSqlRow constructs a go-mysql-server sql.Row from a Dolt row.Row.
func DoltRowToSqlRow(doltRow Row, sch schema.Schema) (sql.Row, error) {
	colVals := make(sql.Row, sch.GetAllCols().Size())

	i := 0
	err := sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		var innerErr error
		value, _ := doltRow.GetColVal(tag)
		colVals[i], innerErr = col.TypeInfo.ConvertNomsValueToValue(value)
		if innerErr != nil {
			return true, innerErr
		}
		i++
		return false, nil
	})

	if err != nil {
		return nil, err
	}

	return sql.NewRow(colVals...), nil
}

// SqlRowToDoltRow constructs a Dolt row.Row from a go-mysql-server sql.Row.
func SqlRowToDoltRow(nbf *types.NomsBinFormat, r sql.Row, doltSchema schema.Schema) (Row, error) {
	taggedVals := make(TaggedValues)
	allCols := doltSchema.GetAllCols()
	for i, val := range r {
		tag := allCols.Tags[i]
		schCol := allCols.TagToCol[tag]
		if val != nil {
			var err error
			taggedVals[tag], err = schCol.TypeInfo.ConvertValueToNomsValue(val)
			if err != nil {
				return nil, err
			}
		} else if !schCol.IsNullable() {
			return nil, fmt.Errorf("column <%v> received nil but is non-nullable", schCol.Name)
		}
	}
	return New(nbf, doltSchema, taggedVals)
}
