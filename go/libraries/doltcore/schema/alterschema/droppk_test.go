// Copyright 2021 Dolthub, Inc.
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

package alterschema

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

const indexName string = "c1_idx"

var nomsType *types.NomsBinFormat = types.Format_Default

const pkTag uint64 = 100
const c1Tag uint64 = 101

func getTable(ctx context.Context, dEnv *env.DoltEnv, tableName string) (*doltdb.Table, error) {
	wr, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return nil, err
	}

	table, ok, err := wr.GetTable(ctx, tableName)
	if !ok {
		return nil, fmt.Errorf("error: table not found")
	}
	if err != nil {
		return nil, err
	}

	return table, nil
}

// NewRowWithSchema creates a new row with the using the provided schema.
func NewRowWithSchema(sch schema.Schema, vals ...types.Value) row.Row {
	tv := make(row.TaggedValues)
	var i int
	sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		tv[tag] = vals[i]
		i++
		return false, nil
	})

	r, err := row.New(nomsType, sch, tv)
	if err != nil {
		panic(err)
	}

	return r
}

func setupDrop(t *testing.T, dEnv *env.DoltEnv) {
	cc := schema.NewColCollection(
		schema.NewColumn("id", uint64(100), types.IntKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("c1", uint64(101), types.IntKind, false),
	)
	otherSch, err := schema.SchemaFromCols(cc)
	assert.NoError(t, err)

	_, err = otherSch.Indexes().AddIndexByColTags(indexName, []uint64{101}, schema.IndexProperties{IsUnique: false, Comment: ""})
	assert.NoError(t, err)

	rows := []row.Row{
		NewRowWithSchema(otherSch, types.Int(1), types.Int(1)),
		NewRowWithSchema(otherSch, types.Int(2), types.Int(2)),
	}

	dtestutils.CreateTestTable(t, dEnv, "test", otherSch, rows...)
}

func createRow(schema schema.Schema, tags []uint64, vals []types.Value) (row.Row, error) {
	if len(tags) != len(vals) {
		return nil, fmt.Errorf("error: size of tags and vals missaligned")
	}

	var tv = make(row.TaggedValues)
	for i, tag := range tags {
		tv[tag] = vals[i]
	}

	return row.New(nomsType, schema, tv)
}

func TestDropPk(t *testing.T) {
	dEnv := dtestutils.CreateTestEnv()
	ctx := context.Background()

	setupDrop(t, dEnv)

	t.Run("Drop primary key from table with index", func(t *testing.T) {
		table, err := getTable(ctx, dEnv, "test")
		assert.NoError(t, err)

		// Get the original index data
		originalMap, err := table.GetIndexRowData(ctx, indexName)
		assert.NoError(t, err)
		assert.False(t, originalMap.Empty())

		// Drop the Primary Key
		table, err = DropPrimaryKeyFromTable(ctx, table, nomsType)
		assert.NoError(t, err)

		sch, err := table.GetSchema(ctx)
		assert.NoError(t, err)

		// Assert the new index map is not empty
		newMap, err := table.GetIndexRowData(ctx, indexName)
		assert.NoError(t, err)
		assert.False(t, newMap.Empty())
		assert.Equal(t, newMap.Len(), uint64(2))

		// Assert the noms level encoding of the map by ensuring the correct index values are present
		kr1, err := createRow(sch, []uint64{pkTag, c1Tag}, []types.Value{types.Int(1), types.Int(1)})
		assert.NoError(t, err)

		idx, ok := sch.Indexes().GetByNameCaseInsensitive(indexName)
		assert.True(t, ok)

		full, _, _, err := kr1.ReduceToIndexKeys(idx)
		assert.NoError(t, err)
		ok, err = newMap.Has(ctx, full)
		assert.NoError(t, err)
		assert.True(t, ok)

		kr2, err := createRow(sch, []uint64{pkTag, c1Tag}, []types.Value{types.Int(2), types.Int(2)})
		assert.NoError(t, err)

		full, _, _, err = kr2.ReduceToIndexKeys(idx)
		assert.NoError(t, err)
		ok, err = newMap.Has(ctx, full)
		assert.NoError(t, err)
		assert.True(t, ok)
	})
}
