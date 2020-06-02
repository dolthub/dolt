// Copyright 2020 Liquidata, Inc.
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
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dbfactory"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/liquidata-inc/dolt/go/store/types"
)

// The number of times we will loop through the tests to ensure consistent results
const tableEditorConcurrencyIterations = 1000

// The number of rows we expect the test to end up with
const tableEditorConcurrencyFinalCount = 100

func TestTableEditorConcurrency(t *testing.T) {
	format := types.Format_7_18
	db, err := dbfactory.MemFactory{}.CreateDB(context.Background(), format, nil, nil)
	require.NoError(t, err)
	colColl, err := schema.NewColCollection(
		schema.NewColumn("pk", 0, types.IntKind, true),
		schema.NewColumn("v1", 1, types.IntKind, false),
		schema.NewColumn("v2", 2, types.IntKind, false))
	require.NoError(t, err)
	tableSch := schema.SchemaFromCols(colColl)
	tableSchVal, err := encoding.MarshalSchemaAsNomsValue(context.Background(), db, tableSch)
	require.NoError(t, err)
	emptyMap, err := types.NewMap(context.Background(), db)
	require.NoError(t, err)
	table, err := NewTable(context.Background(), db, tableSchVal, emptyMap, nil)
	require.NoError(t, err)

	for i := 0; i < tableEditorConcurrencyIterations; i++ {
		tableEditor, err := NewTableEditor(context.Background(), table, tableSch)
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
				require.NoError(t, tableEditor.InsertRow(context.Background(), dRow))
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
				require.NoError(t, tableEditor.UpdateRow(context.Background(), dOldRow, dNewRow))
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

		newTable, err := tableEditor.Table()
		require.NoError(t, err)
		newTableData, err := newTable.GetRowData(context.Background())
		require.NoError(t, err)
		if assert.Equal(t, uint64(tableEditorConcurrencyFinalCount), newTableData.Len()) {
			iterIndex := 0
			_ = newTableData.IterAll(context.Background(), func(key, value types.Value) error {
				dReadRow, err := row.FromNoms(tableSch, key.(types.Tuple), value.(types.Tuple))
				require.NoError(t, err)
				dReadVals, err := row.GetTaggedVals(dReadRow)
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
	format := types.Format_7_18
	db, err := dbfactory.MemFactory{}.CreateDB(context.Background(), format, nil, nil)
	require.NoError(t, err)
	colColl, err := schema.NewColCollection(
		schema.NewColumn("pk", 0, types.IntKind, true),
		schema.NewColumn("v1", 1, types.IntKind, false),
		schema.NewColumn("v2", 2, types.IntKind, false))
	require.NoError(t, err)
	tableSch := schema.SchemaFromCols(colColl)
	tableSchVal, err := encoding.MarshalSchemaAsNomsValue(context.Background(), db, tableSch)
	require.NoError(t, err)
	emptyMap, err := types.NewMap(context.Background(), db)
	require.NoError(t, err)
	table, err := NewTable(context.Background(), db, tableSchVal, emptyMap, nil)
	require.NoError(t, err)

	tableEditor, err := NewTableEditor(context.Background(), table, tableSch)
	require.NoError(t, err)
	for i := 0; i < tableEditorConcurrencyFinalCount*2; i++ {
		dRow, err := row.New(format, tableSch, row.TaggedValues{
			0: types.Int(i),
			1: types.Int(i),
			2: types.Int(i),
		})
		require.NoError(t, err)
		require.NoError(t, tableEditor.InsertRow(context.Background(), dRow))
	}
	table, err = tableEditor.Table()
	require.NoError(t, err)

	for i := 0; i < tableEditorConcurrencyIterations; i++ {
		tableEditor, err := NewTableEditor(context.Background(), table, tableSch)
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
				require.NoError(t, tableEditor.UpdateRow(context.Background(), dOldRow, dNewRow))
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

		newTable, err := tableEditor.Table()
		require.NoError(t, err)
		newTableData, err := newTable.GetRowData(context.Background())
		require.NoError(t, err)
		if assert.Equal(t, uint64(tableEditorConcurrencyFinalCount), newTableData.Len()) {
			iterIndex := 0
			_ = newTableData.IterAll(context.Background(), func(key, value types.Value) error {
				dReadRow, err := row.FromNoms(tableSch, key.(types.Tuple), value.(types.Tuple))
				require.NoError(t, err)
				dReadVals, err := row.GetTaggedVals(dReadRow)
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
	format := types.Format_7_18
	db, err := dbfactory.MemFactory{}.CreateDB(context.Background(), format, nil, nil)
	require.NoError(t, err)
	colColl, err := schema.NewColCollection(
		schema.NewColumn("pk", 0, types.IntKind, true),
		schema.NewColumn("v1", 1, types.IntKind, false),
		schema.NewColumn("v2", 2, types.IntKind, false))
	require.NoError(t, err)
	tableSch := schema.SchemaFromCols(colColl)
	tableSchVal, err := encoding.MarshalSchemaAsNomsValue(context.Background(), db, tableSch)
	require.NoError(t, err)
	emptyMap, err := types.NewMap(context.Background(), db)
	require.NoError(t, err)
	table, err := NewTable(context.Background(), db, tableSchVal, emptyMap, nil)
	require.NoError(t, err)

	tableEditor, err := NewTableEditor(context.Background(), table, tableSch)
	require.NoError(t, err)

	for i := 0; i < 20; i++ {
		dRow, err := row.New(format, tableSch, row.TaggedValues{
			0: types.Int(i),
			1: types.Int(i),
			2: types.Int(i),
		})
		require.NoError(t, err)
		require.NoError(t, tableEditor.InsertRow(context.Background(), dRow))
	}

	_, err = tableEditor.Table()
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

	newTable, err := tableEditor.Table()
	require.NoError(t, err)
	newTableData, err := newTable.GetRowData(context.Background())
	require.NoError(t, err)
	if assert.Equal(t, uint64(10), newTableData.Len()) {
		iterIndex := 0
		_ = newTableData.IterAll(context.Background(), func(key, value types.Value) error {
			dReadRow, err := row.FromNoms(tableSch, key.(types.Tuple), value.(types.Tuple))
			require.NoError(t, err)
			dReadVals, err := row.GetTaggedVals(dReadRow)
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

	sameTable, err := tableEditor.Table()
	require.NoError(t, err)
	sameTableData, err := sameTable.GetRowData(context.Background())
	require.NoError(t, err)
	assert.True(t, sameTableData.Equals(newTableData))
}
