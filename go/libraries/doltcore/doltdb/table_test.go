// Copyright 2019 Liquidata, Inc.
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

package doltdb

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dbfactory"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/liquidata-inc/dolt/go/store/types"
)

var id0, _ = uuid.NewRandom()
var id1, _ = uuid.NewRandom()
var id2, _ = uuid.NewRandom()
var id3, _ = uuid.NewRandom()

func createTestRowData(t *testing.T, vrw types.ValueReadWriter, sch schema.Schema) (types.Map, []row.Row) {
	var err error
	rows := make([]row.Row, 4)
	rows[0], err = row.New(types.Format_7_18, sch, row.TaggedValues{
		idTag: types.UUID(id0), firstTag: types.String("bill"), lastTag: types.String("billerson"), ageTag: types.Uint(53)})
	assert.NoError(t, err)
	rows[1], err = row.New(types.Format_7_18, sch, row.TaggedValues{
		idTag: types.UUID(id1), firstTag: types.String("eric"), lastTag: types.String("ericson"), isMarriedTag: types.Bool(true), ageTag: types.Uint(21)})
	assert.NoError(t, err)
	rows[2], err = row.New(types.Format_7_18, sch, row.TaggedValues{
		idTag: types.UUID(id2), firstTag: types.String("john"), lastTag: types.String("johnson"), isMarriedTag: types.Bool(false), ageTag: types.Uint(53)})
	assert.NoError(t, err)
	rows[3], err = row.New(types.Format_7_18, sch, row.TaggedValues{
		idTag: types.UUID(id3), firstTag: types.String("robert"), lastTag: types.String("robertson"), ageTag: types.Uint(36)})
	assert.NoError(t, err)

	m, err := types.NewMap(context.Background(), vrw)
	assert.NoError(t, err)
	ed := m.Edit()

	for _, r := range rows {
		ed = ed.Set(r.NomsMapKey(sch), r.NomsMapValue(sch))
	}

	m, err = ed.Map(context.Background())
	assert.NoError(t, err)

	return m, rows
}

func createUpdatedTestRowData(t *testing.T, vrw types.ValueReadWriter, sch schema.Schema) (types.Map, []row.Row) {
	var err error
	rows := make([]row.Row, 4)
	rows[0], err = row.New(types.Format_7_18, sch, row.TaggedValues{
		idTag: types.UUID(id0), firstTag: types.String("jack"), lastTag: types.String("space"), ageTag: types.Uint(20)})
	assert.NoError(t, err)
	rows[1], err = row.New(types.Format_7_18, sch, row.TaggedValues{
		idTag: types.UUID(id1), firstTag: types.String("rick"), lastTag: types.String("drive"), isMarriedTag: types.Bool(false), ageTag: types.Uint(21)})
	assert.NoError(t, err)
	rows[2], err = row.New(types.Format_7_18, sch, row.TaggedValues{
		idTag: types.UUID(id2), firstTag: types.String("tyler"), lastTag: types.String("eat"), isMarriedTag: types.Bool(true), ageTag: types.Uint(22)})
	assert.NoError(t, err)
	rows[3], err = row.New(types.Format_7_18, sch, row.TaggedValues{
		idTag: types.UUID(id3), firstTag: types.String("moore"), lastTag: types.String("walk"), ageTag: types.Uint(23)})
	assert.NoError(t, err)

	m, err := types.NewMap(context.Background(), vrw)
	assert.NoError(t, err)
	ed := m.Edit()

	for _, r := range rows {
		ed = ed.Set(r.NomsMapKey(sch), r.NomsMapValue(sch))
	}

	m, err = ed.Map(context.Background())
	assert.NoError(t, err)

	return m, rows
}

func createTestTable(vrw types.ValueReadWriter, tSchema schema.Schema, rowData types.Map) (*Table, error) {
	schemaVal, err := encoding.MarshalSchemaAsNomsValue(context.Background(), vrw, tSchema)

	if err != nil {
		return nil, err
	}

	tbl, err := NewTable(context.Background(), vrw, schemaVal, rowData, nil)

	if err != nil {
		return nil, err
	}

	tbl, err = tbl.RebuildIndexData(context.Background())

	if err != nil {
		return nil, err
	}

	return tbl, nil
}

func TestIsValidTableName(t *testing.T) {
	assert.True(t, IsValidTableName("a"))
	assert.True(t, IsValidTableName("a1"))
	assert.True(t, IsValidTableName("a1_b_c------1"))
	assert.True(t, IsValidTableName("Add-098234_lkjasdf0p98"))
	assert.False(t, IsValidTableName("1"))
	assert.False(t, IsValidTableName("-"))
	assert.False(t, IsValidTableName("-a"))
	assert.False(t, IsValidTableName("__a"))
	assert.False(t, IsValidTableName(""))
	assert.False(t, IsValidTableName("1a"))
	assert.False(t, IsValidTableName("a1-"))
	assert.False(t, IsValidTableName("ab!!c"))
}

func TestTables(t *testing.T) {
	db, _ := dbfactory.MemFactory{}.CreateDB(context.Background(), types.Format_7_18, nil, nil)

	tSchema := createTestSchema(t)
	rowData, rows := createTestRowData(t, db, tSchema)
	tbl, err := createTestTable(db, tSchema, rowData)
	assert.NoError(t, err)

	if err != nil {
		t.Fatal("Failed to create table.")
	}

	badUUID, _ := uuid.NewRandom()
	ids := []types.Value{types.UUID(id0), types.UUID(id1), types.UUID(id2), types.UUID(id3), types.UUID(badUUID)}

	readRow0, ok, err := tbl.GetRowByPKVals(context.Background(), row.TaggedValues{idTag: ids[0]}, tSchema)
	assert.NoError(t, err)

	if !ok {
		t.Error("Could not find row 0 in table")
	} else if !row.AreEqual(readRow0, rows[0], tSchema) {
		t.Error(row.Fmt(context.Background(), readRow0, tSchema), "!=", row.Fmt(context.Background(), rows[0], tSchema))
	}

	_, ok, err = tbl.GetRowByPKVals(context.Background(), row.TaggedValues{idTag: types.UUID(badUUID)}, tSchema)
	assert.NoError(t, err)

	if ok {
		t.Error("GetRow should have returned false.")
	}

	idItr := SingleColPKItr(types.Format_7_18, idTag, ids)
	readRows, missing, err := tbl.GetRows(context.Background(), idItr, -1, tSchema)
	assert.NoError(t, err)

	if len(readRows) != len(rows) {
		t.Error("Did not find all the expected rows")
	} else if len(missing) != 1 {
		t.Error("Expected one missing row for badUUID")
	}

	for i, r := range rows {
		if !row.AreEqual(r, readRows[i], tSchema) {
			t.Error(row.Fmt(context.Background(), readRows[i], tSchema), "!=", row.Fmt(context.Background(), r, tSchema))
		}
	}
}

func TestIndexRebuildingWithZeroIndexes(t *testing.T) {
	db, _ := dbfactory.MemFactory{}.CreateDB(context.Background(), types.Format_7_18, nil, nil)
	tSchema := createTestSchema(t)
	_, err := tSchema.Indexes().RemoveIndex(testSchemaIndexName)
	require.NoError(t, err)
	_, err = tSchema.Indexes().RemoveIndex(testSchemaIndexAge)
	require.NoError(t, err)
	rowData, _ := createTestRowData(t, db, tSchema)
	schemaVal, err := encoding.MarshalSchemaAsNomsValue(context.Background(), db, tSchema)
	require.NoError(t, err)

	originalTable, err := NewTable(context.Background(), db, schemaVal, rowData, nil)
	require.NoError(t, err)

	rebuildAllTable, err := originalTable.RebuildIndexData(context.Background())
	require.NoError(t, err)
	_, err = rebuildAllTable.GetIndexRowData(context.Background(), testSchemaIndexName)
	require.Error(t, err)

	_, err = originalTable.RebuildIndexRowData(context.Background(), testSchemaIndexName)
	require.Error(t, err)
}

func TestIndexRebuildingWithOneIndex(t *testing.T) {
	db, _ := dbfactory.MemFactory{}.CreateDB(context.Background(), types.Format_7_18, nil, nil)
	tSchema := createTestSchema(t)
	_, err := tSchema.Indexes().RemoveIndex(testSchemaIndexAge)
	require.NoError(t, err)
	index := tSchema.Indexes().Get(testSchemaIndexName)
	require.NotNil(t, index)
	indexSch := index.Schema()
	rowData, rows := createTestRowData(t, db, tSchema)
	schemaVal, err := encoding.MarshalSchemaAsNomsValue(context.Background(), db, tSchema)
	require.NoError(t, err)

	indexExpectedRows := make([]row.Row, len(rows))
	for i, r := range rows {
		indexKey := make(row.TaggedValues)
		for _, tag := range index.AllTags() {
			val, ok := r.GetColVal(tag)
			require.True(t, ok)
			indexKey[tag] = val
		}
		indexExpectedRows[i], err = row.New(types.Format_7_18, indexSch, indexKey)
		require.NoError(t, err)
	}

	originalTable, err := NewTable(context.Background(), db, schemaVal, rowData, nil)
	require.NoError(t, err)

	var indexRows []row.Row

	rebuildAllTable, err := originalTable.RebuildIndexData(context.Background())
	require.NoError(t, err)
	indexRowData, err := rebuildAllTable.GetIndexRowData(context.Background(), testSchemaIndexName)
	require.NoError(t, err)
	_ = indexRowData.IterAll(context.Background(), func(key, value types.Value) error {
		indexRow, err := row.FromNoms(indexSch, key.(types.Tuple), value.(types.Tuple))
		require.NoError(t, err)
		indexRows = append(indexRows, indexRow)
		return nil
	})
	assert.ElementsMatch(t, indexExpectedRows, indexRows)

	indexRows = nil
	indexRowData, err = originalTable.RebuildIndexRowData(context.Background(), testSchemaIndexName)
	require.NoError(t, err)
	_ = indexRowData.IterAll(context.Background(), func(key, value types.Value) error {
		indexRow, err := row.FromNoms(indexSch, key.(types.Tuple), value.(types.Tuple))
		require.NoError(t, err)
		indexRows = append(indexRows, indexRow)
		return nil
	})
	assert.ElementsMatch(t, indexExpectedRows, indexRows)
}

func TestIndexRebuildingWithTwoIndexes(t *testing.T) {
	db, _ := dbfactory.MemFactory{}.CreateDB(context.Background(), types.Format_7_18, nil, nil)
	tSchema := createTestSchema(t)

	indexName := tSchema.Indexes().Get(testSchemaIndexName)
	require.NotNil(t, indexName)
	indexAge := tSchema.Indexes().Get(testSchemaIndexAge)
	require.NotNil(t, indexAge)

	indexNameSch := indexName.Schema()
	indexAgeSch := indexAge.Schema()

	rowData, rows := createTestRowData(t, db, tSchema)
	indexNameExpectedRows, indexAgeExpectedRows := rowsToIndexRows(t, rows, indexName, indexAge)

	schemaVal, err := encoding.MarshalSchemaAsNomsValue(context.Background(), db, tSchema)
	require.NoError(t, err)
	originalTable, err := NewTable(context.Background(), db, schemaVal, rowData, nil)
	require.NoError(t, err)

	rebuildAllTable := originalTable
	var indexRows []row.Row

	// do two runs, data should not be different regardless of how many times it's ran
	for i := 0; i < 2; i++ {
		rebuildAllTable, err = rebuildAllTable.RebuildIndexData(context.Background())
		require.NoError(t, err)

		indexNameRowData, err := rebuildAllTable.GetIndexRowData(context.Background(), testSchemaIndexName)
		require.NoError(t, err)
		_ = indexNameRowData.IterAll(context.Background(), func(key, value types.Value) error {
			indexRow, err := row.FromNoms(indexNameSch, key.(types.Tuple), value.(types.Tuple))
			require.NoError(t, err)
			indexRows = append(indexRows, indexRow)
			return nil
		})
		assert.ElementsMatch(t, indexNameExpectedRows, indexRows)
		indexRows = nil

		indexAgeRowData, err := rebuildAllTable.GetIndexRowData(context.Background(), testSchemaIndexAge)
		require.NoError(t, err)
		_ = indexAgeRowData.IterAll(context.Background(), func(key, value types.Value) error {
			indexRow, err := row.FromNoms(indexAgeSch, key.(types.Tuple), value.(types.Tuple))
			require.NoError(t, err)
			indexRows = append(indexRows, indexRow)
			return nil
		})
		assert.ElementsMatch(t, indexAgeExpectedRows, indexRows)
		indexRows = nil

		indexNameRowData, err = originalTable.RebuildIndexRowData(context.Background(), testSchemaIndexName)
		require.NoError(t, err)
		_ = indexNameRowData.IterAll(context.Background(), func(key, value types.Value) error {
			indexRow, err := row.FromNoms(indexNameSch, key.(types.Tuple), value.(types.Tuple))
			require.NoError(t, err)
			indexRows = append(indexRows, indexRow)
			return nil
		})
		assert.ElementsMatch(t, indexNameExpectedRows, indexRows)
		indexRows = nil

		indexAgeRowData, err = originalTable.RebuildIndexRowData(context.Background(), testSchemaIndexAge)
		require.NoError(t, err)
		_ = indexAgeRowData.IterAll(context.Background(), func(key, value types.Value) error {
			indexRow, err := row.FromNoms(indexAgeSch, key.(types.Tuple), value.(types.Tuple))
			require.NoError(t, err)
			indexRows = append(indexRows, indexRow)
			return nil
		})
		assert.ElementsMatch(t, indexAgeExpectedRows, indexRows)
		indexRows = nil
	}

	// change the underlying data and verify that rebuild changes the data as well
	rowData, rows = createUpdatedTestRowData(t, db, tSchema)
	indexNameExpectedRows, indexAgeExpectedRows = rowsToIndexRows(t, rows, indexName, indexAge)
	updatedTable, err := rebuildAllTable.UpdateRows(context.Background(), rowData)
	require.NoError(t, err)
	rebuildAllTable, err = updatedTable.RebuildIndexData(context.Background())
	require.NoError(t, err)

	indexNameRowData, err := rebuildAllTable.GetIndexRowData(context.Background(), testSchemaIndexName)
	require.NoError(t, err)
	_ = indexNameRowData.IterAll(context.Background(), func(key, value types.Value) error {
		indexRow, err := row.FromNoms(indexNameSch, key.(types.Tuple), value.(types.Tuple))
		require.NoError(t, err)
		indexRows = append(indexRows, indexRow)
		return nil
	})
	assert.ElementsMatch(t, indexNameExpectedRows, indexRows)
	indexRows = nil

	indexAgeRowData, err := rebuildAllTable.GetIndexRowData(context.Background(), testSchemaIndexAge)
	require.NoError(t, err)
	_ = indexAgeRowData.IterAll(context.Background(), func(key, value types.Value) error {
		indexRow, err := row.FromNoms(indexAgeSch, key.(types.Tuple), value.(types.Tuple))
		require.NoError(t, err)
		indexRows = append(indexRows, indexRow)
		return nil
	})
	assert.ElementsMatch(t, indexAgeExpectedRows, indexRows)
	indexRows = nil

	indexNameRowData, err = updatedTable.RebuildIndexRowData(context.Background(), testSchemaIndexName)
	require.NoError(t, err)
	_ = indexNameRowData.IterAll(context.Background(), func(key, value types.Value) error {
		indexRow, err := row.FromNoms(indexNameSch, key.(types.Tuple), value.(types.Tuple))
		require.NoError(t, err)
		indexRows = append(indexRows, indexRow)
		return nil
	})
	assert.ElementsMatch(t, indexNameExpectedRows, indexRows)
	indexRows = nil

	indexAgeRowData, err = updatedTable.RebuildIndexRowData(context.Background(), testSchemaIndexAge)
	require.NoError(t, err)
	_ = indexAgeRowData.IterAll(context.Background(), func(key, value types.Value) error {
		indexRow, err := row.FromNoms(indexAgeSch, key.(types.Tuple), value.(types.Tuple))
		require.NoError(t, err)
		indexRows = append(indexRows, indexRow)
		return nil
	})
	assert.ElementsMatch(t, indexAgeExpectedRows, indexRows)
}

func rowsToIndexRows(t *testing.T, rows []row.Row, indexName schema.Index, indexAge schema.Index) (indexNameExpectedRows []row.Row, indexAgeExpectedRows []row.Row) {
	indexNameExpectedRows = make([]row.Row, len(rows))
	indexAgeExpectedRows = make([]row.Row, len(rows))
	indexNameSch := indexName.Schema()
	indexAgeSch := indexAge.Schema()
	var err error
	for i, r := range rows {
		indexNameKey := make(row.TaggedValues)
		for _, tag := range indexName.AllTags() {
			val, ok := r.GetColVal(tag)
			require.True(t, ok)
			indexNameKey[tag] = val
		}
		indexNameExpectedRows[i], err = row.New(types.Format_7_18, indexNameSch, indexNameKey)
		require.NoError(t, err)

		indexAgeKey := make(row.TaggedValues)
		for _, tag := range indexAge.AllTags() {
			val, ok := r.GetColVal(tag)
			require.True(t, ok)
			indexAgeKey[tag] = val
		}
		indexAgeExpectedRows[i], err = row.New(types.Format_7_18, indexAgeSch, indexAgeKey)
		require.NoError(t, err)
	}
	return
}
