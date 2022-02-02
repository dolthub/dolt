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

func TestKeylessTableEditorConcurrency(t *testing.T) {
	format := types.Format_Default
	_, vrw, err := dbfactory.MemFactory{}.CreateDB(context.Background(), format, nil, nil)
	require.NoError(t, err)
	colColl := schema.NewColCollection(
		schema.NewColumn("v0", 0, types.IntKind, false),
		schema.NewColumn("v1", 1, types.IntKind, false),
		schema.NewColumn("v2", 2, types.IntKind, false))
	tableSch, err := schema.SchemaFromCols(colColl)
	require.NoError(t, err)
	emptyMap, err := types.NewMap(context.Background(), vrw)
	require.NoError(t, err)
	table, err := doltdb.NewNomsTable(context.Background(), vrw, tableSch, emptyMap, nil, nil)
	require.NoError(t, err)

	opts := TestEditorOptions(vrw)
	for i := 0; i < tableEditorConcurrencyIterations; i++ {
		tableEditor, err := newKeylessTableEditor(context.Background(), table, tableSch, tableName, opts)
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

		require.Equal(t, newTableData.Len(), uint64(100))

		seen := make([]bool, 100)
		if assert.Equal(t, uint64(tableEditorConcurrencyFinalCount), newTableData.Len()) {
			_ = newTableData.IterAll(context.Background(), func(key, value types.Value) error {
				dReadRow, err := row.FromNoms(tableSch, key.(types.Tuple), value.(types.Tuple))
				require.NoError(t, err)
				dReadVals, err := dReadRow.TaggedValues()
				require.NoError(t, err)

				idx, ok := dReadVals[0].(types.Int)
				assert.Equal(t, true, ok)
				seen[int(idx)] = true

				val1, ok := dReadVals[1].(types.Int)
				assert.Equal(t, true, ok)
				assert.Equal(t, int(idx), int(val1)-1)

				val2, ok := dReadVals[2].(types.Int)
				assert.Equal(t, true, ok)
				assert.Equal(t, int(idx), int(val2)-1)

				return nil
			})
			for _, v := range seen {
				assert.True(t, v)
			}
		}
	}
}

func TestKeylessTableEditorConcurrencyPostInsert(t *testing.T) {
	format := types.Format_Default
	_, vrw, err := dbfactory.MemFactory{}.CreateDB(context.Background(), format, nil, nil)
	require.NoError(t, err)
	colColl := schema.NewColCollection(
		schema.NewColumn("v0", 0, types.IntKind, false),
		schema.NewColumn("v1", 1, types.IntKind, false),
		schema.NewColumn("v2", 2, types.IntKind, false))
	tableSch, err := schema.SchemaFromCols(colColl)
	require.NoError(t, err)
	emptyMap, err := types.NewMap(context.Background(), vrw)
	require.NoError(t, err)
	table, err := doltdb.NewNomsTable(context.Background(), vrw, tableSch, emptyMap, nil, nil)
	require.NoError(t, err)

	opts := TestEditorOptions(vrw)
	tableEditor, err := newKeylessTableEditor(context.Background(), table, tableSch, tableName, opts)
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
		tableEditor, err := newKeylessTableEditor(context.Background(), table, tableSch, tableName, opts)
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

		require.Equal(t, newTableData.Len(), uint64(100))

		seen := make([]bool, 100)

		if assert.Equal(t, uint64(tableEditorConcurrencyFinalCount), newTableData.Len()) {
			_ = newTableData.IterAll(context.Background(), func(key, value types.Value) error {
				dReadRow, err := row.FromNoms(tableSch, key.(types.Tuple), value.(types.Tuple))
				require.NoError(t, err)
				dReadVals, err := dReadRow.TaggedValues()
				require.NoError(t, err)

				idx, ok := dReadVals[0].(types.Int)
				assert.Equal(t, true, ok)
				seen[int(idx)] = true

				val1, ok := dReadVals[1].(types.Int)
				assert.Equal(t, true, ok)
				assert.Equal(t, int(idx), int(val1)-1)

				val2, ok := dReadVals[2].(types.Int)
				assert.Equal(t, true, ok)
				assert.Equal(t, int(idx), int(val2)-1)

				return nil
			})
			for _, v := range seen {
				assert.True(t, v)
			}
		}
	}
}

func TestKeylessTableEditorWriteAfterFlush(t *testing.T) {
	format := types.Format_Default
	_, vrw, err := dbfactory.MemFactory{}.CreateDB(context.Background(), format, nil, nil)
	require.NoError(t, err)
	colColl := schema.NewColCollection(
		schema.NewColumn("v0", 0, types.IntKind, false),
		schema.NewColumn("v1", 1, types.IntKind, false),
		schema.NewColumn("v2", 2, types.IntKind, false))
	tableSch, err := schema.SchemaFromCols(colColl)
	require.NoError(t, err)
	emptyMap, err := types.NewMap(context.Background(), vrw)
	require.NoError(t, err)
	table, err := doltdb.NewNomsTable(context.Background(), vrw, tableSch, emptyMap, nil, nil)
	require.NoError(t, err)

	opts := TestEditorOptions(vrw)
	tableEditor, err := newKeylessTableEditor(context.Background(), table, tableSch, tableName, opts)
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

	seen := make([]bool, 10)
	if assert.Equal(t, uint64(10), newTableData.Len()) {
		_ = newTableData.IterAll(context.Background(), func(key, value types.Value) error {
			dReadRow, err := row.FromNoms(tableSch, key.(types.Tuple), value.(types.Tuple))
			require.NoError(t, err)
			dReadVals, err := dReadRow.TaggedValues()
			require.NoError(t, err)

			idx, ok := dReadVals[0].(types.Int)
			assert.Equal(t, true, ok)
			seen[int(idx)] = true

			val1, ok := dReadVals[1].(types.Int)
			assert.Equal(t, true, ok)
			assert.Equal(t, int(idx), int(val1))

			val2, ok := dReadVals[2].(types.Int)
			assert.Equal(t, true, ok)
			assert.Equal(t, int(idx), int(val2))

			return nil
		})
		for _, v := range seen {
			assert.True(t, v)
		}
	}

	sameTable, err := tableEditor.Table(context.Background())
	require.NoError(t, err)
	sameTableData, err := sameTable.GetNomsRowData(context.Background())
	require.NoError(t, err)
	assert.True(t, sameTableData.Equals(newTableData))
}

func TestKeylessTableEditorDuplicateKeyHandling(t *testing.T) {
	format := types.Format_Default
	_, vrw, err := dbfactory.MemFactory{}.CreateDB(context.Background(), format, nil, nil)
	require.NoError(t, err)
	colColl := schema.NewColCollection(
		schema.NewColumn("v0", 0, types.IntKind, false),
		schema.NewColumn("v1", 1, types.IntKind, false),
		schema.NewColumn("v2", 2, types.IntKind, false))
	tableSch, err := schema.SchemaFromCols(colColl)
	require.NoError(t, err)
	emptyMap, err := types.NewMap(context.Background(), vrw)
	require.NoError(t, err)
	table, err := doltdb.NewNomsTable(context.Background(), vrw, tableSch, emptyMap, nil, nil)
	require.NoError(t, err)

	opts := TestEditorOptions(vrw)
	tableEditor, err := newKeylessTableEditor(context.Background(), table, tableSch, tableName, opts)
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
		require.False(t, errors.Is(err, ErrDuplicateKey))
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

	seen := make([]bool, 10)
	if assert.Equal(t, uint64(10), newTableData.Len()) {
		_ = newTableData.IterAll(context.Background(), func(key, value types.Value) error {
			dReadRow, err := row.FromNoms(tableSch, key.(types.Tuple), value.(types.Tuple))
			require.NoError(t, err)
			dReadVals, err := dReadRow.TaggedValues()
			require.NoError(t, err)

			idx, ok := dReadVals[0].(types.Int)
			assert.Equal(t, true, ok)
			seen[int(idx)] = true

			val1, ok := dReadVals[1].(types.Int)
			assert.Equal(t, true, ok)
			assert.Equal(t, int(idx), int(val1))

			val2, ok := dReadVals[2].(types.Int)
			assert.Equal(t, true, ok)
			assert.Equal(t, int(idx), int(val2))

			return nil
		})
		for _, v := range seen {
			assert.True(t, v)
		}
	}
}

func TestKeylessTableEditorMultipleIndexErrorHandling(t *testing.T) {
	ctx := context.Background()
	format := types.Format_Default
	_, vrw, err := dbfactory.MemFactory{}.CreateDB(ctx, format, nil, nil)
	require.NoError(t, err)
	opts := TestEditorOptions(vrw)
	colColl := schema.NewColCollection(
		schema.NewColumn("v0", 0, types.IntKind, false),
		schema.NewColumn("v1", 1, types.IntKind, false),
		schema.NewColumn("v2", 2, types.IntKind, false))
	tableSch, err := schema.SchemaFromCols(colColl)
	require.NoError(t, err)
	idxv1, err := tableSch.Indexes().AddIndexByColNames("idx_v1", []string{"v1"}, schema.IndexProperties{
		IsUnique: false,
	})
	require.NoError(t, err)
	idxv2, err := tableSch.Indexes().AddIndexByColNames("idx_v2", []string{"v2"}, schema.IndexProperties{
		IsUnique: false,
	})
	require.NoError(t, err)
	emptyMap, err := types.NewMap(ctx, vrw)
	require.NoError(t, err)
	table, err := doltdb.NewNomsTable(ctx, vrw, tableSch, emptyMap, nil, nil)
	require.NoError(t, err)
	table, err = RebuildAllIndexes(ctx, table, opts)
	require.NoError(t, err)
	tableEditor, err := newKeylessTableEditor(ctx, table, tableSch, tableName, opts)
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
		require.False(t, errors.Is(err, ErrDuplicateKey))
		dRow, err = row.New(format, tableSch, row.TaggedValues{
			0: types.Int(i + 10),
			1: types.Int(i + 10),
			2: types.Int(i),
		})
		require.NoError(t, err)
		err = tableEditor.InsertRow(ctx, dRow, nil)
		require.False(t, errors.Is(err, ErrDuplicateKey))
	}

	table, err = tableEditor.Table(ctx)
	require.NoError(t, err)
	tableData, err := table.GetNomsRowData(ctx)
	require.NoError(t, err)

	seen := make([]bool, 13)

	if assert.Equal(t, uint64(9), tableData.Len()) {
		_ = tableData.IterAll(ctx, func(key, value types.Value) error {
			dReadRow, err := row.FromNoms(tableSch, key.(types.Tuple), value.(types.Tuple))
			require.NoError(t, err)
			dReadVals, err := dReadRow.TaggedValues()
			require.NoError(t, err)

			idx, ok := dReadVals[0].(types.Int)
			assert.Equal(t, true, ok)
			seen[int(idx)] = true

			return nil
		})
		for i := 0; i < 3; i++ {
			assert.True(t, seen[i])
		}
		for i := 3; i < 10; i++ {
			assert.False(t, seen[i])
		}
		for i := 10; i < 13; i++ {
			assert.True(t, seen[i])
		}
	}

	idxv1Data, err := table.GetNomsIndexRowData(ctx, "idx_v1")
	require.NoError(t, err)

	seen = make([]bool, 13)

	if assert.Equal(t, uint64(9), idxv1Data.Len()) {
		_ = idxv1Data.IterAll(ctx, func(key, value types.Value) error {
			dReadRow, err := row.FromNoms(idxv1.Schema(), key.(types.Tuple), value.(types.Tuple))
			require.NoError(t, err)
			dReadVals, err := dReadRow.TaggedValues()
			require.NoError(t, err)

			idx, ok := dReadVals[1].(types.Int)
			assert.True(t, ok)
			seen[int(idx)] = true

			_, ok = dReadVals[schema.KeylessRowIdTag].(types.UUID)
			assert.True(t, ok)

			return nil
		})
		for i := 0; i < 3; i++ {
			assert.True(t, seen[i])
		}
		for i := 3; i < 10; i++ {
			assert.False(t, seen[i])
		}
		for i := 10; i < 13; i++ {
			assert.True(t, seen[i])
		}
	}

	idxv2Data, err := table.GetNomsIndexRowData(ctx, "idx_v2")
	require.NoError(t, err)

	seen = make([]bool, 13)

	if assert.Equal(t, uint64(9), idxv2Data.Len()) {
		_ = idxv2Data.IterAll(ctx, func(key, value types.Value) error {
			dReadRow, err := row.FromNoms(idxv2.Schema(), key.(types.Tuple), value.(types.Tuple))
			require.NoError(t, err)
			dReadVals, err := dReadRow.TaggedValues()
			require.NoError(t, err)

			idx, ok := dReadVals[2].(types.Int)
			assert.True(t, ok)
			seen[int(idx)] = true

			_, ok = dReadVals[schema.KeylessRowIdTag].(types.UUID)
			assert.True(t, ok)

			return nil
		})
		for i := 0; i < 3; i++ {
			assert.True(t, seen[i])
		}
		for i := 3; i < 10; i++ {
			assert.False(t, seen[i])
		}
		for i := 10; i < 13; i++ {
			assert.True(t, seen[i])
		}
	}
}

func TestKeylessTableEditorIndexCardinality(t *testing.T) {
	ctx := context.Background()
	format := types.Format_Default
	_, vrw, err := dbfactory.MemFactory{}.CreateDB(ctx, format, nil, nil)
	require.NoError(t, err)
	opts := TestEditorOptions(vrw)
	colColl := schema.NewColCollection(
		schema.NewColumn("v0", 0, types.IntKind, false),
		schema.NewColumn("v1", 1, types.IntKind, false),
		schema.NewColumn("v2", 2, types.IntKind, false))
	tableSch, err := schema.SchemaFromCols(colColl)
	require.NoError(t, err)
	idxv1, err := tableSch.Indexes().AddIndexByColNames("idx_v1", []string{"v1"}, schema.IndexProperties{
		IsUnique: false,
	})
	require.NoError(t, err)
	emptyMap, err := types.NewMap(ctx, vrw)
	require.NoError(t, err)
	table, err := doltdb.NewNomsTable(ctx, vrw, tableSch, emptyMap, nil, nil)
	require.NoError(t, err)
	table, err = RebuildAllIndexes(ctx, table, opts)
	require.NoError(t, err)
	tableEditor, err := newKeylessTableEditor(ctx, table, tableSch, tableName, opts)
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		dRow, err := row.New(format, tableSch, row.TaggedValues{
			0: types.Int(i),
			1: types.Int(i),
			2: types.Int(i),
		})
		require.NoError(t, err)
		require.NoError(t, tableEditor.InsertRow(ctx, dRow, nil))

		for j := 0; j < i; j++ {
			require.NoError(t, tableEditor.InsertRow(ctx, dRow, nil))
		}
	}

	table, err = tableEditor.Table(ctx)
	require.NoError(t, err)

	idxv1Data, err := table.GetNomsIndexRowData(ctx, "idx_v1")
	require.NoError(t, err)

	seen := make([]bool, 3)

	if assert.Equal(t, uint64(3), idxv1Data.Len()) {
		_ = idxv1Data.IterAll(ctx, func(key, value types.Value) error {
			dReadRow, err := row.FromNoms(idxv1.Schema(), key.(types.Tuple), value.(types.Tuple))
			require.NoError(t, err)
			dReadVals, err := dReadRow.TaggedValues()
			require.NoError(t, err)

			idx, ok := dReadVals[1].(types.Int)
			require.True(t, ok)
			seen[int(idx)] = true

			_, ok = dReadVals[schema.KeylessRowIdTag].(types.UUID)
			require.True(t, ok)

			cardTuple := value.(types.Tuple)
			cardVal, err := cardTuple.Get(1)
			require.NoError(t, err)
			card, ok := cardVal.(types.Uint)
			require.True(t, ok)

			assert.Equal(t, int(card), int(idx)+1)

			return nil
		})
	}
}
