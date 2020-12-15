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
	if schema.IsKeyless(doltSchema) {
		return keylessDoltRowFromSqlRow(nbf, r, doltSchema)
	}
	return pkDoltRowFromSqlRow(nbf, r, doltSchema)
}

func pkDoltRowFromSqlRow(nbf *types.NomsBinFormat, r sql.Row, doltSchema schema.Schema) (Row, error) {
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

func keylessDoltRowFromSqlRow(nbf *types.NomsBinFormat, sqlRow sql.Row, sch schema.Schema) (Row, error) {
	j := 0
	vals := make([]types.Value, sch.GetAllCols().Size()*2)

	for idx, val := range sqlRow {
		if val != nil {
			col := sch.GetAllCols().GetByIndex(idx)
			nv, err := col.TypeInfo.ConvertValueToNomsValue(val)
			if err != nil {
				return nil, err
			}

			vals[j] = types.Uint(col.Tag)
			vals[j+1] = nv
			j += 2
		}
	}

	return KeylessRow(nbf, vals[:j]...)
}
