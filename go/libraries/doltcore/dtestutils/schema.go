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

package dtestutils

import (
	"context"
	"math"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/dolthub/dolt/go/store/types"
)

// CreateSchema returns a schema from the columns given, panicking on any errors.
func CreateSchema(columns ...schema.Column) schema.Schema {
	colColl, _ := schema.NewColCollection(columns...)
	return schema.MustSchemaFromCols(colColl)
}

// Creates a row with the schema given, having the values given. Starts at tag 0 and counts up.
func NewRow(sch schema.Schema, values ...types.Value) row.Row {
	taggedVals := make(row.TaggedValues)
	for i := range values {
		taggedVals[uint64(i)] = values[i]
	}
	r, err := row.New(types.Format_7_18, sch, taggedVals)

	if err != nil {
		panic(err)
	}

	return r
}

// AddColumnToSchema returns a new schema by adding the given column to the given schema. Will panic on an invalid
// schema, e.g. tag collision.
func AddColumnToSchema(sch schema.Schema, col schema.Column) schema.Schema {
	columns := sch.GetAllCols()
	columns, err := columns.Append(col)
	if err != nil {
		panic(err)
	}
	return schema.MustSchemaFromCols(columns)
}

// RemoveColumnFromSchema returns a new schema with the given tag missing, but otherwise identical. At least one
// primary column must remain.
func RemoveColumnFromSchema(sch schema.Schema, tagToRemove uint64) schema.Schema {
	var newCols []schema.Column
	err := sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		if tag != tagToRemove {
			newCols = append(newCols, col)
		}
		return false, nil
	})

	if err != nil {
		panic(err)
	}

	columns, err := schema.NewColCollection(newCols...)
	if err != nil {
		panic(err)
	}
	return schema.MustSchemaFromCols(columns)
}

// Compares two noms Floats for approximate equality
var FloatComparer = cmp.Comparer(func(x, y types.Float) bool {
	return math.Abs(float64(x)-float64(y)) < .001
})

var TimestampComparer = cmp.Comparer(func(x, y types.Timestamp) bool {
	return x.Equals(y)
})

// CreateTestTable creates a new test table with the name, schema, and rows given.
func CreateTestTable(t *testing.T, dEnv *env.DoltEnv, tableName string, sch schema.Schema, rs ...row.Row) {
	imt := table.NewInMemTable(sch)

	for _, r := range rs {
		_ = imt.AppendRow(r)
	}

	ctx := context.Background()
	vrw := dEnv.DoltDB.ValueReadWriter()
	rd := table.NewInMemTableReader(imt)
	wr := noms.NewNomsMapCreator(ctx, vrw, sch)

	_, _, err := table.PipeRows(ctx, rd, wr, false)
	require.NoError(t, err)
	err = rd.Close(ctx)
	require.NoError(t, err)
	err = wr.Close(ctx)
	require.NoError(t, err)

	schVal, err := encoding.MarshalSchemaAsNomsValue(ctx, vrw, sch)
	require.NoError(t, err)
	empty, err := types.NewMap(ctx, vrw)
	require.NoError(t, err)
	tbl, err := doltdb.NewTable(ctx, vrw, schVal, wr.GetMap(), empty)
	require.NoError(t, err)
	tbl, err = editor.RebuildAllIndexes(ctx, tbl)
	require.NoError(t, err)

	sch, err = tbl.GetSchema(ctx)
	require.NoError(t, err)
	rows, err := tbl.GetRowData(ctx)
	require.NoError(t, err)
	indexes, err := tbl.GetIndexData(ctx)
	require.NoError(t, err)
	err = dEnv.PutTableToWorking(ctx, sch, rows, indexes, tableName)
	require.NoError(t, err)
}

// MustSchema takes a variable number of columns and returns a schema.
func MustSchema(cols ...schema.Column) schema.Schema {
	hasPKCols := false
	for _, col := range cols {
		if col.IsPartOfPK {
			hasPKCols = true
			break
		}
	}

	colColl, err := schema.NewColCollection(cols...)

	if err != nil {
		panic(err)
	}

	if !hasPKCols {
		return schema.UnkeyedSchemaFromCols(colColl)
	} else {
		return schema.MustSchemaFromCols(colColl)
	}
}
