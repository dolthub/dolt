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

package sqlutil

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

// Returns a SQL row representation for the dolt row given.
func DoltRowToSqlRow(doltRow row.Row, sch schema.Schema) (sql.Row, error) {
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

// Returns a Dolt row representation for SQL row given
func SqlRowToDoltRow(nbf *types.NomsBinFormat, r sql.Row, doltSchema schema.Schema) (row.Row, error) {
	taggedVals := make(row.TaggedValues)
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
	return row.New(nbf, doltSchema, taggedVals)
}
