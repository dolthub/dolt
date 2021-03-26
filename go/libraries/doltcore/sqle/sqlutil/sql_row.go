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
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

// DoltRowToSqlRow constructs a go-mysql-server sql.Row from a Dolt row.Row.
func DoltRowToSqlRow(doltRow row.Row, sch schema.Schema) (sql.Row, error) {
	colVals := make(sql.Row, sch.GetAllCols().Size())
	i := 0

	_, err := doltRow.IterSchema(sch, func(tag uint64, val types.Value) (stop bool, err error) {
		col, _ := sch.GetAllCols().GetByTag(tag)
		colVals[i], err = col.TypeInfo.ConvertNomsValueToValue(val)
		i++

		stop = err != nil
		return
	})
	if err != nil {
		return nil, err
	}

	return sql.NewRow(colVals...), nil
}

// SqlRowToDoltRow constructs a Dolt row.Row from a go-mysql-server sql.Row.
func SqlRowToDoltRow(ctx context.Context, vrw types.ValueReadWriter, r sql.Row, doltSchema schema.Schema) (row.Row, error) {
	if schema.IsKeyless(doltSchema) {
		return keylessDoltRowFromSqlRow(ctx, vrw, r, doltSchema)
	}
	return pkDoltRowFromSqlRow(ctx, vrw, r, doltSchema)
}

// DoltKeyValueAndMappingFromSqlRow converts a sql.Row to key and value tuples and keeps a mapping from tag to value that
// can be used to speed up index key generation for foreign key checks.
func DoltKeyValueAndMappingFromSqlRow(ctx context.Context, vrw types.ValueReadWriter, r sql.Row, doltSchema schema.Schema) (types.Tuple, types.Tuple, map[uint64]types.Value, error) {
	allCols := doltSchema.GetAllCols()
	nonPKCols := doltSchema.GetNonPKCols()

	numCols := allCols.Size()
	vals := make([]types.Value, numCols*2)
	tagToVal := make(map[uint64]types.Value, numCols)

	numNonPKVals := nonPKCols.Size() * 2
	nonPKVals := vals[:numNonPKVals]
	pkVals := vals[numNonPKVals:]

	// values for the pk tuple are in schema order
	pkIdx := 0
	for i := 0; i < numCols; i++ {
		schCol := allCols.GetAtIndex(i)
		val := r[i]
		if val == nil {
			if !schCol.IsNullable() {
				return types.Tuple{}, types.Tuple{}, nil, fmt.Errorf("column <%v> received nil but is non-nullable", schCol.Name)
			}
			continue
		}

		tag := schCol.Tag
		nomsVal, err := schCol.TypeInfo.ConvertValueToNomsValue(ctx, vrw, val)

		if err != nil {
			return types.Tuple{}, types.Tuple{}, nil, err
		}

		tagToVal[tag] = nomsVal

		if schCol.IsPartOfPK {
			pkVals[pkIdx] = types.Uint(tag)
			pkVals[pkIdx+1] = nomsVal
			pkIdx += 2
		}
	}

	// no nulls in keys
	if pkIdx != len(pkVals) {
		return types.Tuple{}, types.Tuple{}, nil, errors.New("not all pk columns have a value")
	}

	// non pk values in tag sorted order
	nonPKIdx := 0
	nonPKTags := len(tagToVal) - (pkIdx / 2)
	for i := 0; i < len(nonPKCols.SortedTags) && nonPKIdx < (nonPKTags*2); i++ {
		tag := nonPKCols.SortedTags[i]
		val, ok := tagToVal[tag]

		if ok {
			nonPKVals[nonPKIdx] = types.Uint(tag)
			nonPKVals[nonPKIdx+1] = val
			nonPKIdx += 2
		}
	}

	nonPKVals = nonPKVals[:nonPKIdx]

	nbf := vrw.Format()
	keyTuple, err := types.NewTuple(nbf, pkVals...)

	if err != nil {
		return types.Tuple{}, types.Tuple{}, nil, err
	}

	valTuple, err := types.NewTuple(nbf, nonPKVals...)

	if err != nil {
		return types.Tuple{}, types.Tuple{}, nil, err
	}

	return keyTuple, valTuple, tagToVal, nil
}

func pkDoltRowFromSqlRow(ctx context.Context, vrw types.ValueReadWriter, r sql.Row, doltSchema schema.Schema) (row.Row, error) {
	taggedVals := make(row.TaggedValues)
	allCols := doltSchema.GetAllCols()
	for i, val := range r {
		tag := allCols.Tags[i]
		schCol := allCols.TagToCol[tag]
		if val != nil {
			var err error
			taggedVals[tag], err = schCol.TypeInfo.ConvertValueToNomsValue(ctx, vrw, val)
			if err != nil {
				return nil, err
			}
		} else if !schCol.IsNullable() {
			// TODO: this isn't an error in the case of result set construction (where non-null columns can indeed be null)
			return nil, fmt.Errorf("column <%v> received nil but is non-nullable", schCol.Name)
		}
	}
	return row.New(vrw.Format(), doltSchema, taggedVals)
}

func keylessDoltRowFromSqlRow(ctx context.Context, vrw types.ValueReadWriter, sqlRow sql.Row, sch schema.Schema) (row.Row, error) {
	j := 0
	vals := make([]types.Value, sch.GetAllCols().Size()*2)

	for idx, val := range sqlRow {
		if val != nil {
			col := sch.GetAllCols().GetByIndex(idx)
			nv, err := col.TypeInfo.ConvertValueToNomsValue(ctx, vrw, val)
			if err != nil {
				return nil, err
			}

			vals[j] = types.Uint(col.Tag)
			vals[j+1] = nv
			j += 2
		}
	}

	return row.KeylessRow(vrw.Format(), vals[:j]...)
}

// SqlColToStr is a utility function for converting a sql column of type interface{} to a string
func SqlColToStr(col interface{}) string {
	if col != nil {
		switch typedCol := col.(type) {
		case int:
			return strconv.FormatInt(int64(typedCol), 10)
		case int32:
			return strconv.FormatInt(int64(typedCol), 10)
		case int64:
			return strconv.FormatInt(int64(typedCol), 10)
		case int16:
			return strconv.FormatInt(int64(typedCol), 10)
		case int8:
			return strconv.FormatInt(int64(typedCol), 10)
		case uint:
			return strconv.FormatUint(uint64(typedCol), 10)
		case uint32:
			return strconv.FormatUint(uint64(typedCol), 10)
		case uint64:
			return strconv.FormatUint(uint64(typedCol), 10)
		case uint16:
			return strconv.FormatUint(uint64(typedCol), 10)
		case uint8:
			return strconv.FormatUint(uint64(typedCol), 10)
		case float64:
			return strconv.FormatFloat(float64(typedCol), 'g', -1, 64)
		case float32:
			return strconv.FormatFloat(float64(typedCol), 'g', -1, 32)
		case string:
			return typedCol
		case bool:
			if typedCol {
				return "true"
			} else {
				return "false"
			}
		case time.Time:
			return typedCol.Format("2006-01-02 15:04:05.999999 -0700 MST")
		}
	}

	return ""
}
