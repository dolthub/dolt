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
	"errors"
	"sync"
	"testing"

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
	tableEditorConcurrencyIterations = 1000

	// The number of rows we expect the test to end up with
	tableEditorConcurrencyFinalCount = 100

	tableName = "t"
)

func TestTableEditorConcurrency(t *testing.T) {
	format := types.Format_Default
	_, vrw, err := dbfactory.MemFactory{}.CreateDB(context.Background(), format, nil, nil)
	require.NoError(t, err)
	opts := TestEditorOptions(vrw)
	colColl := schema.NewColCollection(
		schema.NewColumn("pk", 0, types.IntKind, true),
		schema.NewColumn("v1", 1, types.IntKind, false),
		schema.NewColumn("v2", 2, types.IntKind, false))
	tableSch, err := schema.SchemaFromCols(colColl)
	require.NoError(t, err)
	emptyMap, err := types.NewMap(context.Background(), vrw)
	require.NoError(t, err)
	table, err := doltdb.NewNomsTable(context.Background(), vrw, tableSch, emptyMap, nil, nil)
	require.NoError(t, err)

	for i := 0; i < tableEditorConcurrencyIterations; i++ {
		tableEditor, err := newPkTableEditor(context.Background(), table, tableSch, tableName, opts)
		require.NoError(t, err)
		wg := &sync.WaitGroup{}

		for j := 0; j < tableEditorConcurrencyFinalCount*2; j++ {
			wg.Add(1)
			go func(val int) {
				dRow, err := row.New(format, tableSch, row.TaggedValues{
					0: types.Int(val),
					1: types.Int(val),
					2: types.Int(val),
				})
				require.NoError(t, err)
				require.NoError(t, tableEditor.InsertRow(context.Background(), dRow, nil))
				wg.Done()
			}(j)
		}
		wg.Wait()

		for j := 0; j < tableEditorConcurrencyFinalCount; j++ {
			wg.Add(1)
			go func(val int) {
				dOldRow, err := row.New(format, tableSch, row.TaggedValues{
					0: types.Int(val),
					1: types.Int(val),
					2: types.Int(val),
				})
				require.NoError(t, err)
				dNewRow, err := row.New(format, tableSch, row.TaggedValues{
					0: types.Int(val),
					1: types.Int(val + 1),
					2: types.Int(val + 1),
				})
				require.NoError(t, err)
				require.NoError(t, tableEditor.UpdateRow(context.Background(), dOldRow, dNewRow, nil))
				wg.Done()
			}(j)
		}

		// We let the Updates and Deletes execute at the same time
		for j := tableEditorConcurrencyFinalCount; j < tableEditorConcurrencyFinalCount*2; j++ {
			wg.Add(1)
			go func(val int) {
				dRow, err := row.New(format, tableSch, row.TaggedValues{
					0: types.Int(val),
					1: types.Int(val),
					2: types.Int(val),
				})
				require.NoError(t, err)
				require.NoError(t, tableEditor.DeleteRow(context.Background(), dRow))
				wg.Done()
			}(j)
		}
		wg.Wait()

		newTable, err := tableEditor.Table(context.Background())
		require.NoError(t, err)
		newTableData, err := newTable.GetNomsRowData(context.Background())
		require.NoError(t, err)
		if assert.Equal(t, uint64(tableEditorConcurrencyFinalCount), newTableData.Len()) {
			iterIndex := 0
			_ = newTableData.IterAll(context.Background(), func(key, value types.Value) error {
				dReadRow, err := row.FromNoms(tableSch, key.(types.Tuple), value.(types.Tuple))
				require.NoError(t, err)
				dReadVals, err := dReadRow.TaggedValues()
				require.NoError(t, err)
				assert.Equal(t, row.TaggedValues{
					0: types.Int(iterIndex),
					1: types.Int(iterIndex + 1),
					2: types.Int(iterIndex + 1),
				}, dReadVals)
				iterIndex++
				return nil
			})
		}
	}
}

func TestTableEditorConcurrencyPostInsert(t *testing.T) {
	format := types.Format_Default
	_, vrw, err := dbfactory.MemFactory{}.CreateDB(context.Background(), format, nil, nil)
	require.NoError(t, err)
	opts := TestEditorOptions(vrw)
	colColl := schema.NewColCollection(
		schema.NewColumn("pk", 0, types.IntKind, true),
		schema.NewColumn("v1", 1, types.IntKind, false),
		schema.NewColumn("v2", 2, types.IntKind, false))
	tableSch, err := schema.SchemaFromCols(colColl)
	require.NoError(t, err)
	emptyMap, err := types.NewMap(context.Background(), vrw)
	require.NoError(t, err)
	table, err := doltdb.NewNomsTable(context.Background(), vrw, tableSch, emptyMap, nil, nil)
	require.NoError(t, err)

	tableEditor, err := newPkTableEditor(context.Background(), table, tableSch, tableName, opts)
	require.NoError(t, err)
	for i := 0; i < tableEditorConcurrencyFinalCount*2; i++ {
		dRow, err := row.New(format, tableSch, row.TaggedValues{
			0: types.Int(i),
			1: types.Int(i),
			2: types.Int(i),
		})
		require.NoError(t, err)
		require.NoError(t, tableEditor.InsertRow(context.Background(), dRow, nil))
	}
	table, err = tableEditor.Table(context.Background())
	require.NoError(t, err)

	for i := 0; i < tableEditorConcurrencyIterations; i++ {
		tableEditor, err := newPkTableEditor(context.Background(), table, tableSch, tableName, opts)
		require.NoError(t, err)
		wg := &sync.WaitGroup{}

		for j := 0; j < tableEditorConcurrencyFinalCount; j++ {
			wg.Add(1)
			go func(val int) {
				dOldRow, err := row.New(format, tableSch, row.TaggedValues{
					0: types.Int(val),
					1: types.Int(val),
					2: types.Int(val),
				})
				require.NoError(t, err)
				dNewRow, err := row.New(format, tableSch, row.TaggedValues{
					0: types.Int(val),
					1: types.Int(val + 1),
					2: types.Int(val + 1),
				})
				require.NoError(t, err)
				require.NoError(t, tableEditor.UpdateRow(context.Background(), dOldRow, dNewRow, nil))
				wg.Done()
			}(j)
		}

		for j := tableEditorConcurrencyFinalCount; j < tableEditorConcurrencyFinalCount*2; j++ {
			wg.Add(1)
			go func(val int) {
				dRow, err := row.New(format, tableSch, row.TaggedValues{
					0: types.Int(val),
					1: types.Int(val),
					2: types.Int(val),
				})
				require.NoError(t, err)
				require.NoError(t, tableEditor.DeleteRow(context.Background(), dRow))
				wg.Done()
			}(j)
		}
		wg.Wait()

		newTable, err := tableEditor.Table(context.Background())
		require.NoError(t, err)
		newTableData, err := newTable.GetNomsRowData(context.Background())
		require.NoError(t, err)
		if assert.Equal(t, uint64(tableEditorConcurrencyFinalCount), newTableData.Len()) {
			iterIndex := 0
			_ = newTableData.IterAll(context.Background(), func(key, value types.Value) error {
				dReadRow, err := row.FromNoms(tableSch, key.(types.Tuple), value.(types.Tuple))
				require.NoError(t, err)
				dReadVals, err := dReadRow.TaggedValues()
				require.NoError(t, err)
				assert.Equal(t, row.TaggedValues{
					0: types.Int(iterIndex),
					1: types.Int(iterIndex + 1),
					2: types.Int(iterIndex + 1),
				}, dReadVals)
				iterIndex++
				return nil
			})
		}
	}
}

func TestTableEditorWriteAfterFlush(t *testing.T) {
	format := types.Format_Default
	_, vrw, err := dbfactory.MemFactory{}.CreateDB(context.Background(), format, nil, nil)
	require.NoError(t, err)
	opts := TestEditorOptions(vrw)
	colColl := schema.NewColCollection(
		schema.NewColumn("pk", 0, types.IntKind, true),
		schema.NewColumn("v1", 1, types.IntKind, false),
		schema.NewColumn("v2", 2, types.IntKind, false))
	tableSch, err := schema.SchemaFromCols(colColl)
	require.NoError(t, err)
	emptyMap, err := types.NewMap(context.Background(), vrw)
	require.NoError(t, err)
	table, err := doltdb.NewNomsTable(context.Background(), vrw, tableSch, emptyMap, nil, nil)
	require.NoError(t, err)

	tableEditor, err := newPkTableEditor(context.Background(), table, tableSch, tableName, opts)
	require.NoError(t, err)

	for i := 0; i < 20; i++ {
		dRow, err := row.New(format, tableSch, row.TaggedValues{
			0: types.Int(i),
			1: types.Int(i),
			2: types.Int(i),
		})
		require.NoError(t, err)
		require.NoError(t, tableEditor.InsertRow(context.Background(), dRow, nil))
	}

	_, err = tableEditor.Table(context.Background())
	require.NoError(t, err)

	for i := 10; i < 20; i++ {
		dRow, err := row.New(format, tableSch, row.TaggedValues{
			0: types.Int(i),
			1: types.Int(i),
			2: types.Int(i),
		})
		require.NoError(t, err)
		require.NoError(t, tableEditor.DeleteRow(context.Background(), dRow))
	}

	newTable, err := tableEditor.Table(context.Background())
	require.NoError(t, err)
	newTableData, err := newTable.GetNomsRowData(context.Background())
	require.NoError(t, err)
	if assert.Equal(t, uint64(10), newTableData.Len()) {
		iterIndex := 0
		_ = newTableData.IterAll(context.Background(), func(key, value types.Value) error {
			dReadRow, err := row.FromNoms(tableSch, key.(types.Tuple), value.(types.Tuple))
			require.NoError(t, err)
			dReadVals, err := dReadRow.TaggedValues()
			require.NoError(t, err)
			assert.Equal(t, row.TaggedValues{
				0: types.Int(iterIndex),
				1: types.Int(iterIndex),
				2: types.Int(iterIndex),
			}, dReadVals)
			iterIndex++
			return nil
		})
	}

	sameTable, err := tableEditor.Table(context.Background())
	require.NoError(t, err)
	sameTableData, err := sameTable.GetNomsRowData(context.Background())
	require.NoError(t, err)
	assert.True(t, sameTableData.Equals(newTableData))
}

func TestTableEditorDuplicateKeyHandling(t *testing.T) {
	format := types.Format_Default
	_, vrw, err := dbfactory.MemFactory{}.CreateDB(context.Background(), format, nil, nil)
	require.NoError(t, err)
	opts := TestEditorOptions(vrw)
	colColl := schema.NewColCollection(
		schema.NewColumn("pk", 0, types.IntKind, true),
		schema.NewColumn("v1", 1, types.IntKind, false),
		schema.NewColumn("v2", 2, types.IntKind, false))
	tableSch, err := schema.SchemaFromCols(colColl)
	require.NoError(t, err)
	emptyMap, err := types.NewMap(context.Background(), vrw)
	require.NoError(t, err)
	table, err := doltdb.NewNomsTable(context.Background(), vrw, tableSch, emptyMap, nil, nil)
	require.NoError(t, err)

	tableEditor, err := newPkTableEditor(context.Background(), table, tableSch, tableName, opts)
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		dRow, err := row.New(format, tableSch, row.TaggedValues{
			0: types.Int(i),
			1: types.Int(i),
			2: types.Int(i),
		})
		require.NoError(t, err)
		require.NoError(t, tableEditor.InsertRow(context.Background(), dRow, nil))
	}

	_, err = tableEditor.Table(context.Background())
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		dRow, err := row.New(format, tableSch, row.TaggedValues{
			0: types.Int(i),
			1: types.Int(i),
			2: types.Int(i),
		})
		require.NoError(t, err)
		err = tableEditor.InsertRow(context.Background(), dRow, nil)
		require.True(t, errors.Is(err, ErrDuplicateKey))
	}

	_, err = tableEditor.Table(context.Background())
	require.NoError(t, err)

	for i := 3; i < 10; i++ {
		dRow, err := row.New(format, tableSch, row.TaggedValues{
			0: types.Int(i),
			1: types.Int(i),
			2: types.Int(i),
		})
		require.NoError(t, err)
		require.NoError(t, tableEditor.InsertRow(context.Background(), dRow, nil))
	}

	newTable, err := tableEditor.Table(context.Background())
	require.NoError(t, err)
	newTableData, err := newTable.GetNomsRowData(context.Background())
	require.NoError(t, err)
	if assert.Equal(t, uint64(10), newTableData.Len()) {
		iterIndex := 0
		_ = newTableData.IterAll(context.Background(), func(key, value types.Value) error {
			dReadRow, err := row.FromNoms(tableSch, key.(types.Tuple), value.(types.Tuple))
			require.NoError(t, err)
			dReadVals, err := dReadRow.TaggedValues()
			require.NoError(t, err)
			assert.Equal(t, row.TaggedValues{
				0: types.Int(iterIndex),
				1: types.Int(iterIndex),
				2: types.Int(iterIndex),
			}, dReadVals)
			iterIndex++
			return nil
		})
	}
}

func TestTableEditorMultipleIndexErrorHandling(t *testing.T) {
	ctx := context.Background()
	format := types.Format_Default
	_, vrw, err := dbfactory.MemFactory{}.CreateDB(ctx, format, nil, nil)
	require.NoError(t, err)
	opts := TestEditorOptions(vrw)
	colColl := schema.NewColCollection(
		schema.NewColumn("pk", 0, types.IntKind, true),
		schema.NewColumn("v1", 1, types.IntKind, false),
		schema.NewColumn("v2", 2, types.IntKind, false))
	tableSch, err := schema.SchemaFromCols(colColl)
	require.NoError(t, err)
	idxv1, err := tableSch.Indexes().AddIndexByColNames("idx_v1", []string{"v1"}, schema.IndexProperties{
		IsUnique: true,
	})
	require.NoError(t, err)
	idxv2, err := tableSch.Indexes().AddIndexByColNames("idx_v2", []string{"v2"}, schema.IndexProperties{
		IsUnique: true,
	})
	require.NoError(t, err)
	emptyMap, err := types.NewMap(ctx, vrw)
	require.NoError(t, err)
	table, err := doltdb.NewNomsTable(ctx, vrw, tableSch, emptyMap, nil, nil)
	require.NoError(t, err)
	table, err = RebuildAllIndexes(ctx, table, opts)
	require.NoError(t, err)
	tableEditor, err := newPkTableEditor(ctx, table, tableSch, tableName, opts)
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		dRow, err := row.New(format, tableSch, row.TaggedValues{
			0: types.Int(i),
			1: types.Int(i),
			2: types.Int(i),
		})
		require.NoError(t, err)
		require.NoError(t, tableEditor.InsertRow(ctx, dRow, nil))
	}

	_, err = tableEditor.Table(ctx)
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		dRow, err := row.New(format, tableSch, row.TaggedValues{
			0: types.Int(i + 10),
			1: types.Int(i),
			2: types.Int(i + 10),
		})
		require.NoError(t, err)
		err = tableEditor.InsertRow(ctx, dRow, nil)
		require.True(t, errors.Is(err, ErrDuplicateKey))
		dRow, err = row.New(format, tableSch, row.TaggedValues{
			0: types.Int(i + 10),
			1: types.Int(i + 10),
			2: types.Int(i),
		})
		require.NoError(t, err)
		err = tableEditor.InsertRow(ctx, dRow, nil)
		require.True(t, errors.Is(err, ErrDuplicateKey))
	}

	table, err = tableEditor.Table(ctx)
	require.NoError(t, err)
	tableData, err := table.GetNomsRowData(ctx)
	require.NoError(t, err)
	if assert.Equal(t, uint64(3), tableData.Len()) {
		iterIndex := 0
		_ = tableData.IterAll(ctx, func(key, value types.Value) error {
			dReadRow, err := row.FromNoms(tableSch, key.(types.Tuple), value.(types.Tuple))
			require.NoError(t, err)
			dReadVals, err := dReadRow.TaggedValues()
			require.NoError(t, err)
			assert.Equal(t, row.TaggedValues{
				0: types.Int(iterIndex),
				1: types.Int(iterIndex),
				2: types.Int(iterIndex),
			}, dReadVals)
			iterIndex++
			return nil
		})
	}

	idxv1Data, err := table.GetNomsIndexRowData(ctx, "idx_v1")
	require.NoError(t, err)
	if assert.Equal(t, uint64(3), idxv1Data.Len()) {
		iterIndex := 0
		_ = idxv1Data.IterAll(ctx, func(key, value types.Value) error {
			dReadRow, err := row.FromNoms(idxv1.Schema(), key.(types.Tuple), value.(types.Tuple))
			require.NoError(t, err)
			dReadVals, err := dReadRow.TaggedValues()
			require.NoError(t, err)
			assert.Equal(t, row.TaggedValues{
				1: types.Int(iterIndex),
				0: types.Int(iterIndex),
			}, dReadVals)
			iterIndex++
			return nil
		})
	}

	idxv2Data, err := table.GetNomsIndexRowData(ctx, "idx_v2")
	require.NoError(t, err)
	if assert.Equal(t, uint64(3), idxv2Data.Len()) {
		iterIndex := 0
		_ = idxv2Data.IterAll(ctx, func(key, value types.Value) error {
			dReadRow, err := row.FromNoms(idxv2.Schema(), key.(types.Tuple), value.(types.Tuple))
			require.NoError(t, err)
			dReadVals, err := dReadRow.TaggedValues()
			require.NoError(t, err)
			assert.Equal(t, row.TaggedValues{
				2: types.Int(iterIndex),
				0: types.Int(iterIndex),
			}, dReadVals)
			iterIndex++
			return nil
		})
	}
}
