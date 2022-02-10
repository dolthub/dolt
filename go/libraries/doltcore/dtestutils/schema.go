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
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/store/types"
)

// CreateSchema returns a schema from the columns given, panicking on any errors.
func CreateSchema(columns ...schema.Column) schema.Schema {
	colColl := schema.NewColCollection(columns...)
	return schema.MustSchemaFromCols(colColl)
}

// Creates a row with the schema given, having the values given. Starts at tag 0 and counts up.
func NewRow(sch schema.Schema, values ...types.Value) row.Row {
	taggedVals := make(row.TaggedValues)
	for i := range values {
		taggedVals[uint64(i)] = values[i]
	}
	r, err := row.New(types.Format_Default, sch, taggedVals)

	if err != nil {
		panic(err)
	}

	return r
}

// AddColumnToSchema returns a new schema by adding the given column to the given schema. Will panic on an invalid
// schema, e.g. tag collision.
// Note the AddColumnToSchema relies on being called from the engine (GMS) to correctly update defaults. Directly calling
// this method in Dolt only adds a new column to schema but does not apply the default.
func AddColumnToSchema(sch schema.Schema, col schema.Column) schema.Schema {
	columns := sch.GetAllCols()
	columns = columns.Append(col)
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

	columns := schema.NewColCollection(newCols...)
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

	rowMap, err := types.NewMap(ctx, vrw)
	require.NoError(t, err)
	me := rowMap.Edit()
	for i := 0; i < imt.NumRows(); i++ {
		r, err := imt.GetRow(i)
		require.NoError(t, err)
		k, v := r.NomsMapKey(sch), r.NomsMapValue(sch)
		me.Set(k, v)
	}
	rowMap, err = me.Map(ctx)
	require.NoError(t, err)

	tbl, err := doltdb.NewNomsTable(ctx, vrw, sch, rowMap, nil, nil)
	require.NoError(t, err)
	tbl, err = editor.RebuildAllIndexes(ctx, tbl, editor.TestEditorOptions(vrw))
	require.NoError(t, err)

	sch, err = tbl.GetSchema(ctx)
	require.NoError(t, err)
	rows, err := tbl.GetNomsRowData(ctx)
	require.NoError(t, err)
	indexes, err := tbl.GetIndexSet(ctx)
	require.NoError(t, err)
	err = putTableToWorking(ctx, dEnv, sch, rows, indexes, tableName, nil)
	require.NoError(t, err)
}

func putTableToWorking(ctx context.Context, dEnv *env.DoltEnv, sch schema.Schema, rows types.Map, indexData durable.IndexSet, tableName string, autoVal types.Value) error {
	root, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return doltdb.ErrNomsIO
	}

	vrw := dEnv.DoltDB.ValueReadWriter()
	tbl, err := doltdb.NewNomsTable(ctx, vrw, sch, rows, indexData, autoVal)
	if err != nil {
		return err
	}

	newRoot, err := root.PutTable(ctx, tableName, tbl)
	if err != nil {
		return err
	}

	rootHash, err := root.HashOf()
	if err != nil {
		return err
	}

	newRootHash, err := newRoot.HashOf()
	if err != nil {
		return err
	}
	if rootHash == newRootHash {
		return nil
	}

	return dEnv.UpdateWorkingRoot(ctx, newRoot)
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

	colColl := schema.NewColCollection(cols...)

	if !hasPKCols {
		return schema.UnkeyedSchemaFromCols(colColl)
	} else {
		return schema.MustSchemaFromCols(colColl)
	}
}
