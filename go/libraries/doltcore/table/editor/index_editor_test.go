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
	format := types.Format_Default
	_, vrw, err := dbfactory.MemFactory{}.CreateDB(context.Background(), format, nil, nil)
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
	emptyMap, err := types.NewMap(context.Background(), vrw)
	require.NoError(t, err)

	opts := TestEditorOptions(vrw)
	for i := 0; i < indexEditorConcurrencyIterations; i++ {
		indexEditor := NewIndexEditor(context.Background(), index, emptyMap, tableSch, opts)
		wg := &sync.WaitGroup{}

		for j := 0; j < indexEditorConcurrencyFinalCount*2; j++ {
			wg.Add(1)
			go func(val int) {
				dRow, err := row.New(format, indexSch, row.TaggedValues{
					0: types.Int(val),
					1: types.Int(val),
				})
				require.NoError(t, err)
				fullKey, partialKey, value, err := dRow.ReduceToIndexKeys(index, nil)
				require.NoError(t, err)
				require.NoError(t, indexEditor.InsertRow(context.Background(), fullKey, partialKey, value))
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
				oldFullKey, oldPartialKey, _, err := dOldRow.ReduceToIndexKeys(index, nil)
				require.NoError(t, err)
				require.NoError(t, indexEditor.DeleteRow(context.Background(), oldFullKey, oldPartialKey, types.EmptyTuple(format)))
				newFullKey, newPartialKey, newValue, err := dNewRow.ReduceToIndexKeys(index, nil)
				require.NoError(t, err)
				require.NoError(t, indexEditor.InsertRow(context.Background(), newFullKey, newPartialKey, newValue))
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
				fullKey, partialKey, _, err := dRow.ReduceToIndexKeys(index, nil)
				require.NoError(t, err)
				require.NoError(t, indexEditor.DeleteRow(context.Background(), fullKey, partialKey, types.EmptyTuple(format)))
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
				dReadVals, err := dReadRow.TaggedValues()
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
	format := types.Format_Default
	_, vrw, err := dbfactory.MemFactory{}.CreateDB(context.Background(), format, nil, nil)
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
	emptyMap, err := types.NewMap(context.Background(), vrw)
	require.NoError(t, err)

	opts := TestEditorOptions(vrw)
	indexEditor := NewIndexEditor(context.Background(), index, emptyMap, tableSch, opts)
	for i := 0; i < indexEditorConcurrencyFinalCount*2; i++ {
		dRow, err := row.New(format, indexSch, row.TaggedValues{
			0: types.Int(i),
			1: types.Int(i),
		})
		require.NoError(t, err)
		fullKey, partialKey, value, err := dRow.ReduceToIndexKeys(index, nil)
		require.NoError(t, err)
		require.NoError(t, indexEditor.InsertRow(context.Background(), fullKey, partialKey, value))
	}
	indexData, err := indexEditor.Map(context.Background())
	require.NoError(t, err)

	for i := 0; i < indexEditorConcurrencyIterations; i++ {
		indexEditor := NewIndexEditor(context.Background(), index, indexData, tableSch, opts)
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
				oldFullKey, oldPartialKey, _, err := dOldRow.ReduceToIndexKeys(index, nil)
				require.NoError(t, err)
				require.NoError(t, indexEditor.DeleteRow(context.Background(), oldFullKey, oldPartialKey, types.EmptyTuple(format)))
				newFullKey, newPartialKey, value, err := dNewRow.ReduceToIndexKeys(index, nil)
				require.NoError(t, err)
				require.NoError(t, indexEditor.InsertRow(context.Background(), newFullKey, newPartialKey, value))
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
				fullKey, partialKey, _, err := dRow.ReduceToIndexKeys(index, nil)
				require.NoError(t, err)
				require.NoError(t, indexEditor.DeleteRow(context.Background(), fullKey, partialKey, types.EmptyTuple(format)))
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
				dReadVals, err := dReadRow.TaggedValues()
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
	format := types.Format_Default
	_, vrw, err := dbfactory.MemFactory{}.CreateDB(context.Background(), format, nil, nil)
	require.NoError(t, err)
	colColl := schema.NewColCollection(
		schema.NewColumn("pk", 0, types.IntKind, true),
		schema.NewColumn("v1", 1, types.IntKind, false))
	tableSch, err := schema.SchemaFromCols(colColl)
	require.NoError(t, err)
	index, err := tableSch.Indexes().AddIndexByColNames("idx_unique", []string{"v1"}, schema.IndexProperties{IsUnique: true, Comment: ""})
	require.NoError(t, err)
	indexSch := index.Schema()
	emptyMap, err := types.NewMap(context.Background(), vrw)
	require.NoError(t, err)

	opts := TestEditorOptions(vrw)
	indexEditor := NewIndexEditor(context.Background(), index, emptyMap, tableSch, opts)
	for i := 0; i < 3; i++ {
		dRow, err := row.New(format, indexSch, row.TaggedValues{
			0: types.NullValue,
			1: types.Int(i),
		})
		require.NoError(t, err)
		fullKey, partialKey, value, err := dRow.ReduceToIndexKeys(index, nil)
		require.NoError(t, err)
		require.NoError(t, indexEditor.InsertRow(context.Background(), fullKey, partialKey, value))
	}
	newIndexData, err := indexEditor.Map(context.Background())
	require.NoError(t, err)
	if assert.Equal(t, uint64(3), newIndexData.Len()) {
		index := 0
		_ = newIndexData.IterAll(context.Background(), func(key, value types.Value) error {
			dReadRow, err := row.FromNoms(indexSch, key.(types.Tuple), value.(types.Tuple))
			require.NoError(t, err)
			dReadVals, err := dReadRow.TaggedValues()
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
	format := types.Format_Default
	_, vrw, err := dbfactory.MemFactory{}.CreateDB(context.Background(), format, nil, nil)
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
	emptyMap, err := types.NewMap(context.Background(), vrw)
	require.NoError(t, err)

	opts := TestEditorOptions(vrw)
	indexEditor := NewIndexEditor(context.Background(), index, emptyMap, tableSch, opts)
	require.NoError(t, err)

	for i := 0; i < 20; i++ {
		dRow, err := row.New(format, indexSch, row.TaggedValues{
			0: types.Int(i),
			1: types.Int(i),
		})
		require.NoError(t, err)
		fullKey, partialKey, value, err := dRow.ReduceToIndexKeys(index, nil)
		require.NoError(t, err)
		require.NoError(t, indexEditor.InsertRow(context.Background(), fullKey, partialKey, value))
	}

	_, err = indexEditor.Map(context.Background())
	require.NoError(t, err)

	for i := 10; i < 20; i++ {
		dRow, err := row.New(format, indexSch, row.TaggedValues{
			0: types.Int(i),
			1: types.Int(i),
		})
		require.NoError(t, err)
		fullKey, partialKey, _, err := dRow.ReduceToIndexKeys(index, nil)
		require.NoError(t, err)
		require.NoError(t, indexEditor.DeleteRow(context.Background(), fullKey, partialKey, types.EmptyTuple(format)))
	}

	newIndexData, err := indexEditor.Map(context.Background())
	require.NoError(t, err)
	if assert.Equal(t, uint64(10), newIndexData.Len()) {
		iterIndex := 0
		_ = newIndexData.IterAll(context.Background(), func(key, value types.Value) error {
			dReadRow, err := row.FromNoms(indexSch, key.(types.Tuple), value.(types.Tuple))
			require.NoError(t, err)
			dReadVals, err := dReadRow.TaggedValues()
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

func TestIndexEditorUniqueErrorDoesntPersist(t *testing.T) {
	format := types.Format_Default
	_, vrw, err := dbfactory.MemFactory{}.CreateDB(context.Background(), format, nil, nil)
	require.NoError(t, err)
	colColl := schema.NewColCollection(
		schema.NewColumn("pk", 0, types.IntKind, true),
		schema.NewColumn("v1", 1, types.IntKind, false))
	tableSch, err := schema.SchemaFromCols(colColl)
	require.NoError(t, err)
	index, err := tableSch.Indexes().AddIndexByColNames("idx_unq", []string{"v1"}, schema.IndexProperties{IsUnique: true, Comment: ""})
	require.NoError(t, err)
	indexSch := index.Schema()
	emptyMap, err := types.NewMap(context.Background(), vrw)
	require.NoError(t, err)

	opts := TestEditorOptions(vrw)
	indexEditor := NewIndexEditor(context.Background(), index, emptyMap, tableSch, opts)
	dRow, err := row.New(format, indexSch, row.TaggedValues{
		0: types.Int(1),
		1: types.Int(1),
	})
	require.NoError(t, err)
	fullKey, partialKey, value, err := dRow.ReduceToIndexKeys(index, nil)
	require.NoError(t, err)
	require.NoError(t, indexEditor.InsertRow(context.Background(), fullKey, partialKey, value))
	dRow, err = row.New(format, indexSch, row.TaggedValues{
		0: types.Int(2),
		1: types.Int(1),
	})
	require.NoError(t, err)
	fullKey, partialKey, value, err = dRow.ReduceToIndexKeys(index, nil)
	require.NoError(t, err)
	require.Error(t, indexEditor.InsertRow(context.Background(), fullKey, partialKey, value))
	dRow, err = row.New(format, indexSch, row.TaggedValues{
		0: types.Int(2),
		1: types.Int(2),
	})
	require.NoError(t, err)
	fullKey, partialKey, value, err = dRow.ReduceToIndexKeys(index, nil)
	require.NoError(t, err)
	require.NoError(t, indexEditor.InsertRow(context.Background(), fullKey, partialKey, value))
}

func TestIndexRebuildingWithZeroIndexes(t *testing.T) {
	_, vrw, _ := dbfactory.MemFactory{}.CreateDB(context.Background(), types.Format_Default, nil, nil)
	tSchema := createTestSchema(t)
	_, err := tSchema.Indexes().RemoveIndex(testSchemaIndexName)
	require.NoError(t, err)
	_, err = tSchema.Indexes().RemoveIndex(testSchemaIndexAge)
	require.NoError(t, err)
	rowData, _ := createTestRowData(t, vrw, tSchema)

	originalTable, err := createTableWithoutIndexRebuilding(context.Background(), vrw, tSchema, rowData)
	require.NoError(t, err)

	opts := TestEditorOptions(vrw)
	rebuildAllTable, err := RebuildAllIndexes(context.Background(), originalTable, opts)
	require.NoError(t, err)
	_, err = rebuildAllTable.GetNomsIndexRowData(context.Background(), testSchemaIndexName)
	require.Error(t, err)

	_, err = RebuildIndex(context.Background(), originalTable, testSchemaIndexName, opts)
	require.Error(t, err)
}

func TestIndexRebuildingWithOneIndex(t *testing.T) {
	_, vrw, _ := dbfactory.MemFactory{}.CreateDB(context.Background(), types.Format_Default, nil, nil)
	tSchema := createTestSchema(t)
	_, err := tSchema.Indexes().RemoveIndex(testSchemaIndexAge)
	require.NoError(t, err)
	index := tSchema.Indexes().GetByName(testSchemaIndexName)
	require.NotNil(t, index)
	indexSch := index.Schema()
	rowData, rows := createTestRowData(t, vrw, tSchema)

	indexExpectedRows := make([]row.Row, len(rows))
	for i, r := range rows {
		indexKey := make(row.TaggedValues)
		for _, tag := range index.AllTags() {
			val, ok := r.GetColVal(tag)
			require.True(t, ok)
			indexKey[tag] = val
		}
		indexExpectedRows[i], err = row.New(types.Format_Default, indexSch, indexKey)
		require.NoError(t, err)
	}

	originalTable, err := createTableWithoutIndexRebuilding(context.Background(), vrw, tSchema, rowData)
	require.NoError(t, err)

	var indexRows []row.Row

	opts := TestEditorOptions(vrw)
	rebuildAllTable, err := RebuildAllIndexes(context.Background(), originalTable, opts)
	require.NoError(t, err)
	indexRowData, err := rebuildAllTable.GetNomsIndexRowData(context.Background(), testSchemaIndexName)
	require.NoError(t, err)
	_ = indexRowData.IterAll(context.Background(), func(key, value types.Value) error {
		indexRow, err := row.FromNoms(indexSch, key.(types.Tuple), value.(types.Tuple))
		require.NoError(t, err)
		indexRows = append(indexRows, indexRow)
		return nil
	})
	assert.ElementsMatch(t, indexExpectedRows, indexRows)

	indexRows = nil
	indexRowData, err = RebuildIndex(context.Background(), originalTable, testSchemaIndexName, opts)
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
	_, vrw, _ := dbfactory.MemFactory{}.CreateDB(context.Background(), types.Format_Default, nil, nil)
	tSchema := createTestSchema(t)

	indexName := tSchema.Indexes().GetByName(testSchemaIndexName)
	require.NotNil(t, indexName)
	indexAge := tSchema.Indexes().GetByName(testSchemaIndexAge)
	require.NotNil(t, indexAge)

	indexNameSch := indexName.Schema()
	indexAgeSch := indexAge.Schema()

	rowData, rows := createTestRowData(t, vrw, tSchema)
	indexNameExpectedRows, indexAgeExpectedRows := rowsToIndexRows(t, rows, indexName, indexAge)

	originalTable, err := createTableWithoutIndexRebuilding(context.Background(), vrw, tSchema, rowData)
	require.NoError(t, err)

	opts := TestEditorOptions(vrw)
	rebuildAllTable := originalTable
	var indexRows []row.Row

	// do two runs, data should not be different regardless of how many times it's ran
	for i := 0; i < 2; i++ {
		rebuildAllTable, err = RebuildAllIndexes(context.Background(), rebuildAllTable, opts)
		require.NoError(t, err)

		indexNameRowData, err := rebuildAllTable.GetNomsIndexRowData(context.Background(), testSchemaIndexName)
		require.NoError(t, err)
		_ = indexNameRowData.IterAll(context.Background(), func(key, value types.Value) error {
			indexRow, err := row.FromNoms(indexNameSch, key.(types.Tuple), value.(types.Tuple))
			require.NoError(t, err)
			indexRows = append(indexRows, indexRow)
			return nil
		})
		assert.ElementsMatch(t, indexNameExpectedRows, indexRows)
		indexRows = nil

		indexAgeRowData, err := rebuildAllTable.GetNomsIndexRowData(context.Background(), testSchemaIndexAge)
		require.NoError(t, err)
		_ = indexAgeRowData.IterAll(context.Background(), func(key, value types.Value) error {
			indexRow, err := row.FromNoms(indexAgeSch, key.(types.Tuple), value.(types.Tuple))
			require.NoError(t, err)
			indexRows = append(indexRows, indexRow)
			return nil
		})
		assert.ElementsMatch(t, indexAgeExpectedRows, indexRows)
		indexRows = nil

		indexNameRowData, err = RebuildIndex(context.Background(), originalTable, testSchemaIndexName, opts)
		require.NoError(t, err)
		_ = indexNameRowData.IterAll(context.Background(), func(key, value types.Value) error {
			indexRow, err := row.FromNoms(indexNameSch, key.(types.Tuple), value.(types.Tuple))
			require.NoError(t, err)
			indexRows = append(indexRows, indexRow)
			return nil
		})
		assert.ElementsMatch(t, indexNameExpectedRows, indexRows)
		indexRows = nil

		indexAgeRowData, err = RebuildIndex(context.Background(), originalTable, testSchemaIndexAge, opts)
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
	rowData, rows = createUpdatedTestRowData(t, vrw, tSchema)
	indexNameExpectedRows, indexAgeExpectedRows = rowsToIndexRows(t, rows, indexName, indexAge)
	updatedTable, err := rebuildAllTable.UpdateNomsRows(context.Background(), rowData)
	require.NoError(t, err)
	rebuildAllTable, err = RebuildAllIndexes(context.Background(), updatedTable, opts)
	require.NoError(t, err)

	indexNameRowData, err := rebuildAllTable.GetNomsIndexRowData(context.Background(), testSchemaIndexName)
	require.NoError(t, err)
	_ = indexNameRowData.IterAll(context.Background(), func(key, value types.Value) error {
		indexRow, err := row.FromNoms(indexNameSch, key.(types.Tuple), value.(types.Tuple))
		require.NoError(t, err)
		indexRows = append(indexRows, indexRow)
		return nil
	})
	assert.ElementsMatch(t, indexNameExpectedRows, indexRows)
	indexRows = nil

	indexAgeRowData, err := rebuildAllTable.GetNomsIndexRowData(context.Background(), testSchemaIndexAge)
	require.NoError(t, err)
	_ = indexAgeRowData.IterAll(context.Background(), func(key, value types.Value) error {
		indexRow, err := row.FromNoms(indexAgeSch, key.(types.Tuple), value.(types.Tuple))
		require.NoError(t, err)
		indexRows = append(indexRows, indexRow)
		return nil
	})
	assert.ElementsMatch(t, indexAgeExpectedRows, indexRows)
	indexRows = nil

	indexNameRowData, err = RebuildIndex(context.Background(), updatedTable, testSchemaIndexName, opts)
	require.NoError(t, err)
	_ = indexNameRowData.IterAll(context.Background(), func(key, value types.Value) error {
		indexRow, err := row.FromNoms(indexNameSch, key.(types.Tuple), value.(types.Tuple))
		require.NoError(t, err)
		indexRows = append(indexRows, indexRow)
		return nil
	})
	assert.ElementsMatch(t, indexNameExpectedRows, indexRows)
	indexRows = nil

	indexAgeRowData, err = RebuildIndex(context.Background(), updatedTable, testSchemaIndexAge, opts)
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
	_, vrw, _ := dbfactory.MemFactory{}.CreateDB(context.Background(), types.Format_Default, nil, nil)
	colColl := schema.NewColCollection(
		schema.NewColumn("pk1", 1, types.IntKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("v1", 2, types.IntKind, false),
		schema.NewColumn("v2", 3, types.IntKind, false),
	)
	sch, err := schema.SchemaFromCols(colColl)
	require.NoError(t, err)
	rowData, _ := createTestRowDataFromTaggedValues(t, vrw, sch,
		row.TaggedValues{1: types.Int(1), 2: types.Int(1), 3: types.Int(1)},
		row.TaggedValues{1: types.Int(2), 2: types.Int(2), 3: types.Int(2)},
		row.TaggedValues{1: types.Int(3), 2: types.Int(3), 3: types.Int(3)},
	)
	originalTable, err := createTableWithoutIndexRebuilding(context.Background(), vrw, sch, rowData)
	require.NoError(t, err)

	index, err := sch.Indexes().AddIndexByColTags("idx_v1", []uint64{2}, schema.IndexProperties{IsUnique: true, Comment: ""})
	require.NoError(t, err)
	updatedTable, err := originalTable.UpdateSchema(context.Background(), sch)
	require.NoError(t, err)

	opts := TestEditorOptions(vrw)
	_, err = RebuildAllIndexes(context.Background(), updatedTable, opts)
	require.NoError(t, err)

	_, err = RebuildIndex(context.Background(), updatedTable, index.Name(), opts)
	require.NoError(t, err)
}

func TestIndexRebuildingUniqueSuccessTwoCol(t *testing.T) {
	_, vrw, _ := dbfactory.MemFactory{}.CreateDB(context.Background(), types.Format_Default, nil, nil)
	colColl := schema.NewColCollection(
		schema.NewColumn("pk1", 1, types.IntKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("v1", 2, types.IntKind, false),
		schema.NewColumn("v2", 3, types.IntKind, false),
	)
	sch, err := schema.SchemaFromCols(colColl)
	require.NoError(t, err)
	rowData, _ := createTestRowDataFromTaggedValues(t, vrw, sch,
		row.TaggedValues{1: types.Int(1), 2: types.Int(1), 3: types.Int(1)},
		row.TaggedValues{1: types.Int(2), 2: types.Int(1), 3: types.Int(2)},
		row.TaggedValues{1: types.Int(3), 2: types.Int(2), 3: types.Int(2)},
	)
	originalTable, err := createTableWithoutIndexRebuilding(context.Background(), vrw, sch, rowData)
	require.NoError(t, err)

	index, err := sch.Indexes().AddIndexByColTags("idx_v1", []uint64{2, 3}, schema.IndexProperties{IsUnique: true, Comment: ""})
	require.NoError(t, err)
	updatedTable, err := originalTable.UpdateSchema(context.Background(), sch)
	require.NoError(t, err)

	opts := TestEditorOptions(vrw)
	_, err = RebuildAllIndexes(context.Background(), updatedTable, opts)
	require.NoError(t, err)

	_, err = RebuildIndex(context.Background(), updatedTable, index.Name(), opts)
	require.NoError(t, err)
}

func TestIndexRebuildingUniqueFailOneCol(t *testing.T) {
	_, vrw, _ := dbfactory.MemFactory{}.CreateDB(context.Background(), types.Format_Default, nil, nil)
	colColl := schema.NewColCollection(
		schema.NewColumn("pk1", 1, types.IntKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("v1", 2, types.IntKind, false),
		schema.NewColumn("v2", 3, types.IntKind, false),
	)
	sch, err := schema.SchemaFromCols(colColl)
	require.NoError(t, err)
	rowData, _ := createTestRowDataFromTaggedValues(t, vrw, sch,
		row.TaggedValues{1: types.Int(1), 2: types.Int(1), 3: types.Int(1)},
		row.TaggedValues{1: types.Int(2), 2: types.Int(2), 3: types.Int(2)},
		row.TaggedValues{1: types.Int(3), 2: types.Int(2), 3: types.Int(3)},
	)
	originalTable, err := createTableWithoutIndexRebuilding(context.Background(), vrw, sch, rowData)
	require.NoError(t, err)

	index, err := sch.Indexes().AddIndexByColTags("idx_v1", []uint64{2}, schema.IndexProperties{IsUnique: true, Comment: ""})
	require.NoError(t, err)
	updatedTable, err := originalTable.UpdateSchema(context.Background(), sch)
	require.NoError(t, err)

	opts := TestEditorOptions(vrw)
	_, err = RebuildAllIndexes(context.Background(), updatedTable, opts)
	require.Error(t, err)

	_, err = RebuildIndex(context.Background(), updatedTable, index.Name(), opts)
	require.Error(t, err)
}

func TestIndexRebuildingUniqueFailTwoCol(t *testing.T) {
	_, vrw, _ := dbfactory.MemFactory{}.CreateDB(context.Background(), types.Format_Default, nil, nil)
	colColl := schema.NewColCollection(
		schema.NewColumn("pk1", 1, types.IntKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("v1", 2, types.IntKind, false),
		schema.NewColumn("v2", 3, types.IntKind, false),
	)
	sch, err := schema.SchemaFromCols(colColl)
	require.NoError(t, err)
	rowData, _ := createTestRowDataFromTaggedValues(t, vrw, sch,
		row.TaggedValues{1: types.Int(1), 2: types.Int(1), 3: types.Int(1)},
		row.TaggedValues{1: types.Int(2), 2: types.Int(1), 3: types.Int(2)},
		row.TaggedValues{1: types.Int(3), 2: types.Int(2), 3: types.Int(2)},
		row.TaggedValues{1: types.Int(4), 2: types.Int(1), 3: types.Int(2)},
	)
	originalTable, err := createTableWithoutIndexRebuilding(context.Background(), vrw, sch, rowData)
	require.NoError(t, err)

	index, err := sch.Indexes().AddIndexByColTags("idx_v1", []uint64{2, 3}, schema.IndexProperties{IsUnique: true, Comment: ""})
	require.NoError(t, err)
	updatedTable, err := originalTable.UpdateSchema(context.Background(), sch)
	require.NoError(t, err)

	opts := TestEditorOptions(vrw)
	_, err = RebuildAllIndexes(context.Background(), updatedTable, opts)

	require.Error(t, err)

	_, err = RebuildIndex(context.Background(), updatedTable, index.Name(), opts)
	require.Error(t, err)
}

func TestIndexEditorCapacityExceeded(t *testing.T) {
	// In the event that we reach the iea capacity on Undo, we need to verify that all code paths fail and remain failing
	ctx := context.Background()
	format := types.Format_Default
	_, vrw, err := dbfactory.MemFactory{}.CreateDB(ctx, format, nil, nil)
	require.NoError(t, err)
	colColl := schema.NewColCollection(
		schema.NewColumn("pk", 0, types.IntKind, true),
		schema.NewColumn("v1", 1, types.IntKind, false))
	tableSch, err := schema.SchemaFromCols(colColl)
	require.NoError(t, err)
	index, err := tableSch.Indexes().AddIndexByColNames("idx_cap", []string{"v1"}, schema.IndexProperties{IsUnique: false, Comment: ""})
	require.NoError(t, err)
	indexSch := index.Schema()
	emptyMap, err := types.NewMap(ctx, vrw)
	require.NoError(t, err)

	opts := Options{Deaf: NewInMemDeafWithMaxCapacity(format, 224)}
	indexEditor := NewIndexEditor(ctx, index, emptyMap, tableSch, opts)
	for i := 0; i < 3; i++ {
		dRow, err := row.New(format, indexSch, row.TaggedValues{
			0: types.Int(i),
			1: types.Int(i),
		})
		require.NoError(t, err)
		fullKey, partialKey, value, err := dRow.ReduceToIndexKeys(index, nil)
		require.NoError(t, err)
		require.NoError(t, indexEditor.InsertRow(ctx, fullKey, partialKey, value))
	}

	dRow, err := row.New(format, indexSch, row.TaggedValues{
		0: types.Int(4),
		1: types.Int(4),
	})
	require.NoError(t, err)
	fullKey, partialKey, value, err := dRow.ReduceToIndexKeys(index, nil)
	require.NoError(t, err)
	err = indexEditor.InsertRow(ctx, fullKey, partialKey, value)
	require.Error(t, err)
	require.Equal(t, "capacity exceeded", err.Error())
	indexEditor.Undo(ctx) // This sets the unrecoverable state error, but does not return an error itself

	require.Contains(t, indexEditor.InsertRow(ctx, fullKey, partialKey, value).Error(), "unrecoverable state")
	require.Contains(t, indexEditor.DeleteRow(ctx, fullKey, partialKey, value).Error(), "unrecoverable state")
	require.Contains(t, indexEditor.StatementFinished(ctx, false).Error(), "unrecoverable state")
	require.Contains(t, indexEditor.Close().Error(), "unrecoverable state")
	_, err = indexEditor.HasPartial(ctx, partialKey)
	require.Contains(t, err.Error(), "unrecoverable state")
	_, err = indexEditor.Map(ctx)
	require.Contains(t, err.Error(), "unrecoverable state")
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
		r, err := row.New(types.Format_Default, sch, val)
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

func createTableWithoutIndexRebuilding(ctx context.Context, vrw types.ValueReadWriter, sch schema.Schema, rowData types.Map) (*doltdb.Table, error) {
	return doltdb.NewNomsTable(ctx, vrw, sch, rowData, nil, nil)
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
		indexNameExpectedRows[i], err = row.New(types.Format_Default, indexNameSch, indexNameKey)
		require.NoError(t, err)

		indexAgeKey := make(row.TaggedValues)
		for _, tag := range indexAge.AllTags() {
			val, ok := r.GetColVal(tag)
			require.True(t, ok)
			indexAgeKey[tag] = val
		}
		indexAgeExpectedRows[i], err = row.New(types.Format_Default, indexAgeSch, indexAgeKey)
		require.NoError(t, err)
	}
	return
}
