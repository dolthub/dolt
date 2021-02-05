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

package editor

import (
	"context"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	// The number of times we will loop through the tests to ensure consistent results
	indexEditorConcurrencyIterations = 1000

	// The number of rows we expect the test to end up with
	indexEditorConcurrencyFinalCount = 100

	idTag        = 0
	firstTag     = 1
	lastTag      = 2
	isMarriedTag = 3
	ageTag       = 4
	emptyTag     = 5

	testSchemaIndexName = "idx_name"
	testSchemaIndexAge  = "idx_age"
)

var id0, _ = uuid.NewRandom()
var id1, _ = uuid.NewRandom()
var id2, _ = uuid.NewRandom()
var id3, _ = uuid.NewRandom()

func TestIndexEditorConcurrency(t *testing.T) {
	format := types.Format_7_18
	db, err := dbfactory.MemFactory{}.CreateDB(context.Background(), format, nil, nil)
	require.NoError(t, err)
	colColl := schema.NewColCollection(
		schema.NewColumn("pk", 0, types.IntKind, true),
		schema.NewColumn("v1", 1, types.IntKind, false),
		schema.NewColumn("v2", 2, types.IntKind, false))
	tableSch, err := schema.SchemaFromCols(colColl)
	require.NoError(t, err)
	index, err := tableSch.Indexes().AddIndexByColNames("idx_concurrency", []string{"v1"}, schema.IndexProperties{IsUnique: false, Comment: ""})
	require.NoError(t, err)
	indexSch := index.Schema()
	emptyMap, err := types.NewMap(context.Background(), db)
	require.NoError(t, err)

	for i := 0; i < indexEditorConcurrencyIterations; i++ {
		indexEditor := NewIndexEditor(index, emptyMap)
		wg := &sync.WaitGroup{}

		for j := 0; j < indexEditorConcurrencyFinalCount*2; j++ {
			wg.Add(1)
			go func(val int) {
				dRow, err := row.New(format, indexSch, row.TaggedValues{
					0: types.Int(val),
					1: types.Int(val),
				})
				require.NoError(t, err)
				require.NoError(t, indexEditor.UpdateIndex(context.Background(), nil, dRow))
				wg.Done()
			}(j)
		}
		wg.Wait()

		for j := 0; j < indexEditorConcurrencyFinalCount; j++ {
			wg.Add(1)
			go func(val int) {
				dOldRow, err := row.New(format, indexSch, row.TaggedValues{
					0: types.Int(val),
					1: types.Int(val),
				})
				require.NoError(t, err)
				dNewRow, err := row.New(format, indexSch, row.TaggedValues{
					0: types.Int(val),
					1: types.Int(val + 1),
				})
				require.NoError(t, err)
				require.NoError(t, indexEditor.UpdateIndex(context.Background(), dOldRow, dNewRow))
				wg.Done()
			}(j)
		}

		// We let the Updates and Deletes execute at the same time
		for j := indexEditorConcurrencyFinalCount; j < indexEditorConcurrencyFinalCount*2; j++ {
			wg.Add(1)
			go func(val int) {
				dRow, err := row.New(format, indexSch, row.TaggedValues{
					0: types.Int(val),
					1: types.Int(val),
				})
				require.NoError(t, err)
				require.NoError(t, indexEditor.UpdateIndex(context.Background(), dRow, nil))
				wg.Done()
			}(j)
		}
		wg.Wait()

		newIndexData, err := indexEditor.Map(context.Background())
		require.NoError(t, err)
		if assert.Equal(t, uint64(indexEditorConcurrencyFinalCount), newIndexData.Len()) {
			iterIndex := 0
			_ = newIndexData.IterAll(context.Background(), func(key, value types.Value) error {
				dReadRow, err := row.FromNoms(indexSch, key.(types.Tuple), value.(types.Tuple))
				require.NoError(t, err)
				dReadVals, err := row.GetTaggedVals(dReadRow)
				require.NoError(t, err)
				assert.Equal(t, row.TaggedValues{
					0: types.Int(iterIndex),
					1: types.Int(iterIndex + 1),
				}, dReadVals)
				iterIndex++
				return nil
			})
		}
	}
}

func TestIndexEditorConcurrencyPostInsert(t *testing.T) {
	format := types.Format_7_18
	db, err := dbfactory.MemFactory{}.CreateDB(context.Background(), format, nil, nil)
	require.NoError(t, err)
	colColl := schema.NewColCollection(
		schema.NewColumn("pk", 0, types.IntKind, true),
		schema.NewColumn("v1", 1, types.IntKind, false),
		schema.NewColumn("v2", 2, types.IntKind, false))
	tableSch, err := schema.SchemaFromCols(colColl)
	require.NoError(t, err)
	index, err := tableSch.Indexes().AddIndexByColNames("idx_concurrency", []string{"v1"}, schema.IndexProperties{IsUnique: false, Comment: ""})
	require.NoError(t, err)
	indexSch := index.Schema()
	emptyMap, err := types.NewMap(context.Background(), db)
	require.NoError(t, err)

	indexEditor := NewIndexEditor(index, emptyMap)
	for i := 0; i < indexEditorConcurrencyFinalCount*2; i++ {
		dRow, err := row.New(format, indexSch, row.TaggedValues{
			0: types.Int(i),
			1: types.Int(i),
		})
		require.NoError(t, err)
		require.NoError(t, indexEditor.UpdateIndex(context.Background(), nil, dRow))
	}
	indexData, err := indexEditor.Map(context.Background())
	require.NoError(t, err)

	for i := 0; i < indexEditorConcurrencyIterations; i++ {
		indexEditor := NewIndexEditor(index, indexData)
		wg := &sync.WaitGroup{}

		for j := 0; j < indexEditorConcurrencyFinalCount; j++ {
			wg.Add(1)
			go func(val int) {
				dOldRow, err := row.New(format, indexSch, row.TaggedValues{
					0: types.Int(val),
					1: types.Int(val),
				})
				require.NoError(t, err)
				dNewRow, err := row.New(format, indexSch, row.TaggedValues{
					0: types.Int(val),
					1: types.Int(val + 1),
				})
				require.NoError(t, err)
				require.NoError(t, indexEditor.UpdateIndex(context.Background(), dOldRow, dNewRow))
				wg.Done()
			}(j)
		}

		for j := indexEditorConcurrencyFinalCount; j < indexEditorConcurrencyFinalCount*2; j++ {
			wg.Add(1)
			go func(val int) {
				dRow, err := row.New(format, indexSch, row.TaggedValues{
					0: types.Int(val),
					1: types.Int(val),
				})
				require.NoError(t, err)
				require.NoError(t, indexEditor.UpdateIndex(context.Background(), dRow, nil))
				wg.Done()
			}(j)
		}
		wg.Wait()

		newIndexData, err := indexEditor.Map(context.Background())
		require.NoError(t, err)
		if assert.Equal(t, uint64(indexEditorConcurrencyFinalCount), newIndexData.Len()) {
			iterIndex := 0
			_ = newIndexData.IterAll(context.Background(), func(key, value types.Value) error {
				dReadRow, err := row.FromNoms(indexSch, key.(types.Tuple), value.(types.Tuple))
				require.NoError(t, err)
				dReadVals, err := row.GetTaggedVals(dReadRow)
				require.NoError(t, err)
				assert.Equal(t, row.TaggedValues{
					0: types.Int(iterIndex),
					1: types.Int(iterIndex + 1),
				}, dReadVals)
				iterIndex++
				return nil
			})
		}
	}
}

func TestIndexEditorConcurrencyUnique(t *testing.T) {
	format := types.Format_7_18
	db, err := dbfactory.MemFactory{}.CreateDB(context.Background(), format, nil, nil)
	require.NoError(t, err)
	colColl := schema.NewColCollection(
		schema.NewColumn("pk", 0, types.IntKind, true),
		schema.NewColumn("v1", 1, types.IntKind, false),
		schema.NewColumn("v2", 2, types.IntKind, false))
	tableSch, err := schema.SchemaFromCols(colColl)
	require.NoError(t, err)
	index, err := tableSch.Indexes().AddIndexByColNames("idx_concurrency", []string{"v1"}, schema.IndexProperties{IsUnique: true, Comment: ""})
	require.NoError(t, err)
	indexSch := index.Schema()
	emptyMap, err := types.NewMap(context.Background(), db)
	require.NoError(t, err)

	for i := 0; i < indexEditorConcurrencyIterations; i++ {
		indexEditor := NewIndexEditor(index, emptyMap)
		wg := &sync.WaitGroup{}

		for j := 0; j < indexEditorConcurrencyFinalCount*2; j++ {
			wg.Add(1)
			go func(val int) {
				dRow, err := row.New(format, indexSch, row.TaggedValues{
					0: types.Int(val),
					1: types.Int(val),
				})
				require.NoError(t, err)
				require.NoError(t, indexEditor.UpdateIndex(context.Background(), nil, dRow))
				wg.Done()
			}(j)
		}
		wg.Wait()

		for j := 0; j < indexEditorConcurrencyFinalCount; j++ {
			wg.Add(1)
			go func(val int) {
				dOldRow, err := row.New(format, indexSch, row.TaggedValues{
					0: types.Int(val),
					1: types.Int(val),
				})
				require.NoError(t, err)
				dNewRow, err := row.New(format, indexSch, row.TaggedValues{
					0: types.Int(val),
					1: types.Int(val + 1),
				})
				require.NoError(t, err)
				require.NoError(t, indexEditor.UpdateIndex(context.Background(), dOldRow, dNewRow))
				wg.Done()
			}(j)
		}

		// We let the Updates and Deletes execute at the same time
		for j := indexEditorConcurrencyFinalCount; j < indexEditorConcurrencyFinalCount*2; j++ {
			wg.Add(1)
			go func(val int) {
				dRow, err := row.New(format, indexSch, row.TaggedValues{
					0: types.Int(val),
					1: types.Int(val),
				})
				require.NoError(t, err)
				require.NoError(t, indexEditor.UpdateIndex(context.Background(), dRow, nil))
				wg.Done()
			}(j)
		}
		wg.Wait()

		newIndexData, err := indexEditor.Map(context.Background())
		require.NoError(t, err)
		if assert.Equal(t, uint64(indexEditorConcurrencyFinalCount), newIndexData.Len()) {
			iterIndex := 0
			_ = newIndexData.IterAll(context.Background(), func(key, value types.Value) error {
				dReadRow, err := row.FromNoms(indexSch, key.(types.Tuple), value.(types.Tuple))
				require.NoError(t, err)
				dReadVals, err := row.GetTaggedVals(dReadRow)
				require.NoError(t, err)
				assert.Equal(t, row.TaggedValues{
					0: types.Int(iterIndex),
					1: types.Int(iterIndex + 1),
				}, dReadVals)
				iterIndex++
				return nil
			})
		}
	}
}

func TestIndexEditorUniqueMultipleNil(t *testing.T) {
	format := types.Format_7_18
	db, err := dbfactory.MemFactory{}.CreateDB(context.Background(), format, nil, nil)
	require.NoError(t, err)
	colColl := schema.NewColCollection(
		schema.NewColumn("pk", 0, types.IntKind, true),
		schema.NewColumn("v1", 1, types.IntKind, false))
	tableSch, err := schema.SchemaFromCols(colColl)
	require.NoError(t, err)
	index, err := tableSch.Indexes().AddIndexByColNames("idx_unique", []string{"v1"}, schema.IndexProperties{IsUnique: true, Comment: ""})
	require.NoError(t, err)
	indexSch := index.Schema()
	emptyMap, err := types.NewMap(context.Background(), db)
	require.NoError(t, err)

	indexEditor := NewIndexEditor(index, emptyMap)
	for i := 0; i < 3; i++ {
		dRow, err := row.New(format, indexSch, row.TaggedValues{
			0: types.NullValue,
			1: types.Int(i),
		})
		require.NoError(t, err)
		require.NoError(t, indexEditor.UpdateIndex(context.Background(), nil, dRow))
	}
	newIndexData, err := indexEditor.Map(context.Background())
	require.NoError(t, err)
	if assert.Equal(t, uint64(3), newIndexData.Len()) {
		index := 0
		_ = newIndexData.IterAll(context.Background(), func(key, value types.Value) error {
			dReadRow, err := row.FromNoms(indexSch, key.(types.Tuple), value.(types.Tuple))
			require.NoError(t, err)
			dReadVals, err := row.GetTaggedVals(dReadRow)
			require.NoError(t, err)
			assert.Equal(t, row.TaggedValues{
				1: types.Int(index), // We don't encode NULL values
			}, dReadVals)
			index++
			return nil
		})
	}
}

func TestIndexEditorWriteAfterFlush(t *testing.T) {
	format := types.Format_7_18
	db, err := dbfactory.MemFactory{}.CreateDB(context.Background(), format, nil, nil)
	require.NoError(t, err)
	colColl := schema.NewColCollection(
		schema.NewColumn("pk", 0, types.IntKind, true),
		schema.NewColumn("v1", 1, types.IntKind, false),
		schema.NewColumn("v2", 2, types.IntKind, false))
	tableSch, err := schema.SchemaFromCols(colColl)
	require.NoError(t, err)
	index, err := tableSch.Indexes().AddIndexByColNames("idx_concurrency", []string{"v1"}, schema.IndexProperties{IsUnique: false, Comment: ""})
	require.NoError(t, err)
	indexSch := index.Schema()
	emptyMap, err := types.NewMap(context.Background(), db)
	require.NoError(t, err)

	indexEditor := NewIndexEditor(index, emptyMap)
	require.NoError(t, err)

	for i := 0; i < 20; i++ {
		dRow, err := row.New(format, indexSch, row.TaggedValues{
			0: types.Int(i),
			1: types.Int(i),
		})
		require.NoError(t, err)
		require.NoError(t, indexEditor.UpdateIndex(context.Background(), nil, dRow))
	}

	require.NoError(t, indexEditor.Flush(context.Background()))

	for i := 10; i < 20; i++ {
		dRow, err := row.New(format, indexSch, row.TaggedValues{
			0: types.Int(i),
			1: types.Int(i),
		})
		require.NoError(t, err)
		require.NoError(t, indexEditor.UpdateIndex(context.Background(), dRow, nil))
	}

	newIndexData, err := indexEditor.Map(context.Background())
	require.NoError(t, err)
	if assert.Equal(t, uint64(10), newIndexData.Len()) {
		iterIndex := 0
		_ = newIndexData.IterAll(context.Background(), func(key, value types.Value) error {
			dReadRow, err := row.FromNoms(indexSch, key.(types.Tuple), value.(types.Tuple))
			require.NoError(t, err)
			dReadVals, err := row.GetTaggedVals(dReadRow)
			require.NoError(t, err)
			assert.Equal(t, row.TaggedValues{
				0: types.Int(iterIndex),
				1: types.Int(iterIndex),
			}, dReadVals)
			iterIndex++
			return nil
		})
	}

	sameIndexData, err := indexEditor.Map(context.Background())
	require.NoError(t, err)
	assert.True(t, sameIndexData.Equals(newIndexData))
}

func TestIndexEditorFlushClearsUniqueError(t *testing.T) {
	format := types.Format_7_18
	db, err := dbfactory.MemFactory{}.CreateDB(context.Background(), format, nil, nil)
	require.NoError(t, err)
	colColl := schema.NewColCollection(
		schema.NewColumn("pk", 0, types.IntKind, true),
		schema.NewColumn("v1", 1, types.IntKind, false))
	tableSch, err := schema.SchemaFromCols(colColl)
	require.NoError(t, err)
	index, err := tableSch.Indexes().AddIndexByColNames("idx_unq", []string{"v1"}, schema.IndexProperties{IsUnique: true, Comment: ""})
	require.NoError(t, err)
	indexSch := index.Schema()
	emptyMap, err := types.NewMap(context.Background(), db)
	require.NoError(t, err)

	indexEditor := NewIndexEditor(index, emptyMap)
	dRow, err := row.New(format, indexSch, row.TaggedValues{
		0: types.Int(1),
		1: types.Int(1),
	})
	require.NoError(t, err)
	require.NoError(t, indexEditor.UpdateIndex(context.Background(), nil, dRow))
	dRow, err = row.New(format, indexSch, row.TaggedValues{
		0: types.Int(2),
		1: types.Int(1),
	})
	require.NoError(t, err)
	require.NoError(t, indexEditor.UpdateIndex(context.Background(), nil, dRow))
	err = indexEditor.Flush(context.Background())
	require.Error(t, err)
	err = indexEditor.Flush(context.Background())
	require.NoError(t, err)
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

	rebuildAllTable, err := RebuildAllIndexes(context.Background(), originalTable)
	require.NoError(t, err)
	_, err = rebuildAllTable.GetIndexRowData(context.Background(), testSchemaIndexName)
	require.Error(t, err)

	_, err = RebuildIndex(context.Background(), originalTable, testSchemaIndexName)
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

	rebuildAllTable, err := RebuildAllIndexes(context.Background(), originalTable)
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
	indexRowData, err = RebuildIndex(context.Background(), originalTable, testSchemaIndexName)
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
		rebuildAllTable, err = RebuildAllIndexes(context.Background(), rebuildAllTable)
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

		indexNameRowData, err = RebuildIndex(context.Background(), originalTable, testSchemaIndexName)
		require.NoError(t, err)
		_ = indexNameRowData.IterAll(context.Background(), func(key, value types.Value) error {
			indexRow, err := row.FromNoms(indexNameSch, key.(types.Tuple), value.(types.Tuple))
			require.NoError(t, err)
			indexRows = append(indexRows, indexRow)
			return nil
		})
		assert.ElementsMatch(t, indexNameExpectedRows, indexRows)
		indexRows = nil

		indexAgeRowData, err = RebuildIndex(context.Background(), originalTable, testSchemaIndexAge)
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
	rebuildAllTable, err = RebuildAllIndexes(context.Background(), updatedTable)
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

	indexNameRowData, err = RebuildIndex(context.Background(), updatedTable, testSchemaIndexName)
	require.NoError(t, err)
	_ = indexNameRowData.IterAll(context.Background(), func(key, value types.Value) error {
		indexRow, err := row.FromNoms(indexNameSch, key.(types.Tuple), value.(types.Tuple))
		require.NoError(t, err)
		indexRows = append(indexRows, indexRow)
		return nil
	})
	assert.ElementsMatch(t, indexNameExpectedRows, indexRows)
	indexRows = nil

	indexAgeRowData, err = RebuildIndex(context.Background(), updatedTable, testSchemaIndexAge)
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
	colColl := schema.NewColCollection(
		schema.NewColumn("pk1", 1, types.IntKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("v1", 2, types.IntKind, false),
		schema.NewColumn("v2", 3, types.IntKind, false),
	)
	sch, err := schema.SchemaFromCols(colColl)
	require.NoError(t, err)
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

	_, err = RebuildAllIndexes(context.Background(), updatedTable)
	require.NoError(t, err)

	_, err = RebuildIndex(context.Background(), updatedTable, index.Name())
	require.NoError(t, err)
}

func TestIndexRebuildingUniqueSuccessTwoCol(t *testing.T) {
	db, _ := dbfactory.MemFactory{}.CreateDB(context.Background(), types.Format_7_18, nil, nil)
	colColl := schema.NewColCollection(
		schema.NewColumn("pk1", 1, types.IntKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("v1", 2, types.IntKind, false),
		schema.NewColumn("v2", 3, types.IntKind, false),
	)
	sch, err := schema.SchemaFromCols(colColl)
	require.NoError(t, err)
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

	_, err = RebuildAllIndexes(context.Background(), updatedTable)
	require.NoError(t, err)

	_, err = RebuildIndex(context.Background(), updatedTable, index.Name())
	require.NoError(t, err)
}

func TestIndexRebuildingUniqueFailOneCol(t *testing.T) {
	db, _ := dbfactory.MemFactory{}.CreateDB(context.Background(), types.Format_7_18, nil, nil)
	colColl := schema.NewColCollection(
		schema.NewColumn("pk1", 1, types.IntKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("v1", 2, types.IntKind, false),
		schema.NewColumn("v2", 3, types.IntKind, false),
	)
	sch, err := schema.SchemaFromCols(colColl)
	require.NoError(t, err)
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

	_, err = RebuildAllIndexes(context.Background(), updatedTable)
	require.Error(t, err)

	_, err = RebuildIndex(context.Background(), updatedTable, index.Name())
	require.Error(t, err)
}

func TestIndexRebuildingUniqueFailTwoCol(t *testing.T) {
	db, _ := dbfactory.MemFactory{}.CreateDB(context.Background(), types.Format_7_18, nil, nil)
	colColl := schema.NewColCollection(
		schema.NewColumn("pk1", 1, types.IntKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("v1", 2, types.IntKind, false),
		schema.NewColumn("v2", 3, types.IntKind, false),
	)
	sch, err := schema.SchemaFromCols(colColl)
	require.NoError(t, err)
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

	_, err = RebuildAllIndexes(context.Background(), updatedTable)
	require.Error(t, err)

	_, err = RebuildIndex(context.Background(), updatedTable, index.Name())
	require.Error(t, err)
}

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

func createTestSchema(t *testing.T) schema.Schema {
	colColl := schema.NewColCollection(
		schema.NewColumn("id", idTag, types.UUIDKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("first", firstTag, types.StringKind, false, schema.NotNullConstraint{}),
		schema.NewColumn("last", lastTag, types.StringKind, false, schema.NotNullConstraint{}),
		schema.NewColumn("is_married", isMarriedTag, types.BoolKind, false),
		schema.NewColumn("age", ageTag, types.UintKind, false),
		schema.NewColumn("empty", emptyTag, types.IntKind, false),
	)
	sch, err := schema.SchemaFromCols(colColl)
	require.NoError(t, err)
	_, err = sch.Indexes().AddIndexByColTags(testSchemaIndexName, []uint64{firstTag, lastTag}, schema.IndexProperties{IsUnique: false, Comment: ""})
	require.NoError(t, err)
	_, err = sch.Indexes().AddIndexByColTags(testSchemaIndexAge, []uint64{ageTag}, schema.IndexProperties{IsUnique: false, Comment: ""})
	require.NoError(t, err)
	return sch
}

func createTableWithoutIndexRebuilding(ctx context.Context, vrw types.ValueReadWriter, schemaVal types.Value, rowData types.Map) (*doltdb.Table, error) {
	empty, _ := types.NewMap(ctx, vrw)
	return doltdb.NewTable(ctx, vrw, schemaVal, rowData, empty, nil)
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
