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

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/dolthub/dolt/go/store/types"
)

var id0, _ = uuid.NewRandom()
var id1, _ = uuid.NewRandom()
var id2, _ = uuid.NewRandom()
var id3, _ = uuid.NewRandom()

func createTestRowData(t *testing.T, vrw types.ValueReadWriter, sch schema.Schema) (types.Map, []row.Row) {
	return createTestRowDataFromTaggedValues(t, vrw, sch,
		row.TaggedValues{
			idTag: types.UUID(id0), firstTag: types.String("bill"), lastTag: types.String("billerson"), ageTag: types.Uint(53)},
		row.TaggedValues{
			idTag: types.UUID(id1), firstTag: types.String("eric"), lastTag: types.String("ericson"), isMarriedTag: types.Bool(true), ageTag: types.Uint(21)},
		row.TaggedValues{
			idTag: types.UUID(id2), firstTag: types.String("john"), lastTag: types.String("johnson"), isMarriedTag: types.Bool(false), ageTag: types.Uint(53)},
		row.TaggedValues{
			idTag: types.UUID(id3), firstTag: types.String("robert"), lastTag: types.String("robertson"), ageTag: types.Uint(36)},
	)
}

func createUpdatedTestRowData(t *testing.T, vrw types.ValueReadWriter, sch schema.Schema) (types.Map, []row.Row) {
	return createTestRowDataFromTaggedValues(t, vrw, sch,
		row.TaggedValues{
			idTag: types.UUID(id0), firstTag: types.String("jack"), lastTag: types.String("space"), ageTag: types.Uint(20)},
		row.TaggedValues{
			idTag: types.UUID(id1), firstTag: types.String("rick"), lastTag: types.String("drive"), isMarriedTag: types.Bool(false), ageTag: types.Uint(21)},
		row.TaggedValues{
			idTag: types.UUID(id2), firstTag: types.String("tyler"), lastTag: types.String("eat"), isMarriedTag: types.Bool(true), ageTag: types.Uint(22)},
		row.TaggedValues{
			idTag: types.UUID(id3), firstTag: types.String("moore"), lastTag: types.String("walk"), ageTag: types.Uint(23)},
	)
}

func createTestRowDataFromTaggedValues(t *testing.T, vrw types.ValueReadWriter, sch schema.Schema, vals ...row.TaggedValues) (types.Map, []row.Row) {
	var err error
	rows := make([]row.Row, len(vals))

	m, err := types.NewMap(context.Background(), vrw)
	assert.NoError(t, err)
	ed := m.Edit()

	for i, val := range vals {
		r, err := row.New(types.Format_7_18, sch, val)
		require.NoError(t, err)
		rows[i] = r
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

	originalTable, err := createTableWithoutIndexRebuilding(context.Background(), db, schemaVal, rowData)
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
	index := tSchema.Indexes().GetByName(testSchemaIndexName)
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

	originalTable, err := createTableWithoutIndexRebuilding(context.Background(), db, schemaVal, rowData)
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

	indexName := tSchema.Indexes().GetByName(testSchemaIndexName)
	require.NotNil(t, indexName)
	indexAge := tSchema.Indexes().GetByName(testSchemaIndexAge)
	require.NotNil(t, indexAge)

	indexNameSch := indexName.Schema()
	indexAgeSch := indexAge.Schema()

	rowData, rows := createTestRowData(t, db, tSchema)
	indexNameExpectedRows, indexAgeExpectedRows := rowsToIndexRows(t, rows, indexName, indexAge)

	schemaVal, err := encoding.MarshalSchemaAsNomsValue(context.Background(), db, tSchema)
	require.NoError(t, err)
	originalTable, err := createTableWithoutIndexRebuilding(context.Background(), db, schemaVal, rowData)
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

func TestIndexRebuildingUniqueSuccessOneCol(t *testing.T) {
	db, _ := dbfactory.MemFactory{}.CreateDB(context.Background(), types.Format_7_18, nil, nil)
	colColl, _ := schema.NewColCollection(
		schema.NewColumn("pk1", 1, types.IntKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("v1", 2, types.IntKind, false),
		schema.NewColumn("v2", 3, types.IntKind, false),
	)
	sch := schema.SchemaFromCols(colColl)
	rowData, _ := createTestRowDataFromTaggedValues(t, db, sch,
		row.TaggedValues{1: types.Int(1), 2: types.Int(1), 3: types.Int(1)},
		row.TaggedValues{1: types.Int(2), 2: types.Int(2), 3: types.Int(2)},
		row.TaggedValues{1: types.Int(3), 2: types.Int(3), 3: types.Int(3)},
	)
	schVal, err := encoding.MarshalSchemaAsNomsValue(context.Background(), db, sch)
	require.NoError(t, err)
	originalTable, err := createTableWithoutIndexRebuilding(context.Background(), db, schVal, rowData)
	require.NoError(t, err)

	index, err := sch.Indexes().AddIndexByColTags("idx_v1", []uint64{2}, schema.IndexProperties{IsUnique: true, Comment: ""})
	require.NoError(t, err)
	updatedTable, err := originalTable.UpdateSchema(context.Background(), sch)
	require.NoError(t, err)

	_, err = updatedTable.RebuildIndexData(context.Background())
	require.NoError(t, err)

	_, err = updatedTable.RebuildIndexRowData(context.Background(), index.Name())
	require.NoError(t, err)
}

func TestIndexRebuildingUniqueSuccessTwoCol(t *testing.T) {
	db, _ := dbfactory.MemFactory{}.CreateDB(context.Background(), types.Format_7_18, nil, nil)
	colColl, _ := schema.NewColCollection(
		schema.NewColumn("pk1", 1, types.IntKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("v1", 2, types.IntKind, false),
		schema.NewColumn("v2", 3, types.IntKind, false),
	)
	sch := schema.SchemaFromCols(colColl)
	rowData, _ := createTestRowDataFromTaggedValues(t, db, sch,
		row.TaggedValues{1: types.Int(1), 2: types.Int(1), 3: types.Int(1)},
		row.TaggedValues{1: types.Int(2), 2: types.Int(1), 3: types.Int(2)},
		row.TaggedValues{1: types.Int(3), 2: types.Int(2), 3: types.Int(2)},
	)
	schVal, err := encoding.MarshalSchemaAsNomsValue(context.Background(), db, sch)
	require.NoError(t, err)
	originalTable, err := createTableWithoutIndexRebuilding(context.Background(), db, schVal, rowData)
	require.NoError(t, err)

	index, err := sch.Indexes().AddIndexByColTags("idx_v1", []uint64{2, 3}, schema.IndexProperties{IsUnique: true, Comment: ""})
	require.NoError(t, err)
	updatedTable, err := originalTable.UpdateSchema(context.Background(), sch)
	require.NoError(t, err)

	_, err = updatedTable.RebuildIndexData(context.Background())
	require.NoError(t, err)

	_, err = updatedTable.RebuildIndexRowData(context.Background(), index.Name())
	require.NoError(t, err)
}

func TestIndexRebuildingUniqueFailOneCol(t *testing.T) {
	db, _ := dbfactory.MemFactory{}.CreateDB(context.Background(), types.Format_7_18, nil, nil)
	colColl, _ := schema.NewColCollection(
		schema.NewColumn("pk1", 1, types.IntKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("v1", 2, types.IntKind, false),
		schema.NewColumn("v2", 3, types.IntKind, false),
	)
	sch := schema.SchemaFromCols(colColl)
	rowData, _ := createTestRowDataFromTaggedValues(t, db, sch,
		row.TaggedValues{1: types.Int(1), 2: types.Int(1), 3: types.Int(1)},
		row.TaggedValues{1: types.Int(2), 2: types.Int(2), 3: types.Int(2)},
		row.TaggedValues{1: types.Int(3), 2: types.Int(2), 3: types.Int(3)},
	)
	schVal, err := encoding.MarshalSchemaAsNomsValue(context.Background(), db, sch)
	require.NoError(t, err)
	originalTable, err := createTableWithoutIndexRebuilding(context.Background(), db, schVal, rowData)
	require.NoError(t, err)

	index, err := sch.Indexes().AddIndexByColTags("idx_v1", []uint64{2}, schema.IndexProperties{IsUnique: true, Comment: ""})
	require.NoError(t, err)
	updatedTable, err := originalTable.UpdateSchema(context.Background(), sch)
	require.NoError(t, err)

	_, err = updatedTable.RebuildIndexData(context.Background())
	require.Error(t, err)

	_, err = updatedTable.RebuildIndexRowData(context.Background(), index.Name())
	require.Error(t, err)
}

func TestIndexRebuildingUniqueFailTwoCol(t *testing.T) {
	db, _ := dbfactory.MemFactory{}.CreateDB(context.Background(), types.Format_7_18, nil, nil)
	colColl, _ := schema.NewColCollection(
		schema.NewColumn("pk1", 1, types.IntKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("v1", 2, types.IntKind, false),
		schema.NewColumn("v2", 3, types.IntKind, false),
	)
	sch := schema.SchemaFromCols(colColl)
	rowData, _ := createTestRowDataFromTaggedValues(t, db, sch,
		row.TaggedValues{1: types.Int(1), 2: types.Int(1), 3: types.Int(1)},
		row.TaggedValues{1: types.Int(2), 2: types.Int(1), 3: types.Int(2)},
		row.TaggedValues{1: types.Int(3), 2: types.Int(2), 3: types.Int(2)},
		row.TaggedValues{1: types.Int(4), 2: types.Int(1), 3: types.Int(2)},
	)
	schVal, err := encoding.MarshalSchemaAsNomsValue(context.Background(), db, sch)
	require.NoError(t, err)
	originalTable, err := createTableWithoutIndexRebuilding(context.Background(), db, schVal, rowData)
	require.NoError(t, err)

	index, err := sch.Indexes().AddIndexByColTags("idx_v1", []uint64{2, 3}, schema.IndexProperties{IsUnique: true, Comment: ""})
	require.NoError(t, err)
	updatedTable, err := originalTable.UpdateSchema(context.Background(), sch)
	require.NoError(t, err)

	_, err = updatedTable.RebuildIndexData(context.Background())
	require.Error(t, err)

	_, err = updatedTable.RebuildIndexRowData(context.Background(), index.Name())
	require.Error(t, err)
}

func createTableWithoutIndexRebuilding(ctx context.Context, vrw types.ValueReadWriter, schemaVal types.Value, rowData types.Map) (*Table, error) {
	indexData, err := types.NewMap(ctx, vrw)
	if err != nil {
		return nil, err
	}

	schemaRef, err := writeValAndGetRef(ctx, vrw, schemaVal)
	if err != nil {
		return nil, err
	}

	rowDataRef, err := writeValAndGetRef(ctx, vrw, rowData)
	if err != nil {
		return nil, err
	}

	indexesRef, err := writeValAndGetRef(ctx, vrw, indexData)
	if err != nil {
		return nil, err
	}

	sd := types.StructData{
		schemaRefKey: schemaRef,
		tableRowsKey: rowDataRef,
		indexesKey:   indexesRef,
	}

	tableStruct, err := types.NewStruct(vrw.Format(), tableStructName, sd)
	if err != nil {
		return nil, err
	}

	return &Table{vrw, tableStruct}, nil
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

// DO NOT CHANGE THIS TEST
// It is necessary to ensure consistent system table definitions
// for more info: https://github.com/dolthub/dolt/pull/663
func TestSystemTableTags(t *testing.T) {
	var sysTableMin uint64 = 1 << 51

	t.Run("asdf", func(t *testing.T) {
		assert.Equal(t, sysTableMin, SystemTableReservedMin)
	})
	t.Run("dolt_doc tags", func(t *testing.T) {
		docTableMin := sysTableMin + uint64(5)
		assert.Equal(t, docTableMin+0, DocNameTag)
		assert.Equal(t, docTableMin+1, DocTextTag)
	})
	t.Run("dolt_history_ tags", func(t *testing.T) {
		doltHistoryMin := sysTableMin + uint64(1000)
		assert.Equal(t, doltHistoryMin+0, HistoryCommitterTag)
		assert.Equal(t, doltHistoryMin+1, HistoryCommitHashTag)
		assert.Equal(t, doltHistoryMin+2, HistoryCommitDateTag)
	})
	t.Run("dolt_diff_ tags", func(t *testing.T) {
		diffTableMin := sysTableMin + uint64(2000)
		assert.Equal(t, diffTableMin+0, DiffCommitTag)
	})
	t.Run("dolt_query_catalog tags", func(t *testing.T) {
		queryCatalogMin := sysTableMin + uint64(3005)
		assert.Equal(t, queryCatalogMin+0, QueryCatalogIdTag)
		assert.Equal(t, queryCatalogMin+1, QueryCatalogOrderTag)
		assert.Equal(t, queryCatalogMin+2, QueryCatalogNameTag)
		assert.Equal(t, queryCatalogMin+3, QueryCatalogQueryTag)
		assert.Equal(t, queryCatalogMin+4, QueryCatalogDescriptionTag)
	})
	t.Run("dolt_schemas tags", func(t *testing.T) {
		doltSchemasMin := sysTableMin + uint64(4007)
		assert.Equal(t, doltSchemasMin+0, DoltSchemasIdTag)
		assert.Equal(t, doltSchemasMin+1, DoltSchemasTypeTag)
		assert.Equal(t, doltSchemasMin+2, DoltSchemasNameTag)
		assert.Equal(t, doltSchemasMin+3, DoltSchemasFragmentTag)
	})
}
