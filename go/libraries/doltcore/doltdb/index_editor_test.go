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
	"github.com/liquidata-inc/dolt/go/store/types"
)

// The number of times we will loop through the tests to ensure consistent results
const indexEditorConcurrencyIterations = 1000

// The number of rows we expect the test to end up with
const indexEditorConcurrencyFinalCount = 100

func TestIndexEditorConcurrency(t *testing.T) {
	format := types.Format_7_18
	db, err := dbfactory.MemFactory{}.CreateDB(context.Background(), format, nil, nil)
	require.NoError(t, err)
	colColl, err := schema.NewColCollection(
		schema.NewColumn("pk", 0, types.IntKind, true),
		schema.NewColumn("v1", 1, types.IntKind, false),
		schema.NewColumn("v2", 2, types.IntKind, false))
	require.NoError(t, err)
	tableSch := schema.SchemaFromCols(colColl)
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
	colColl, err := schema.NewColCollection(
		schema.NewColumn("pk", 0, types.IntKind, true),
		schema.NewColumn("v1", 1, types.IntKind, false),
		schema.NewColumn("v2", 2, types.IntKind, false))
	require.NoError(t, err)
	tableSch := schema.SchemaFromCols(colColl)
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
	colColl, err := schema.NewColCollection(
		schema.NewColumn("pk", 0, types.IntKind, true),
		schema.NewColumn("v1", 1, types.IntKind, false),
		schema.NewColumn("v2", 2, types.IntKind, false))
	require.NoError(t, err)
	tableSch := schema.SchemaFromCols(colColl)
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

func TestIndexEditorWriteAfterFlush(t *testing.T) {
	format := types.Format_7_18
	db, err := dbfactory.MemFactory{}.CreateDB(context.Background(), format, nil, nil)
	require.NoError(t, err)
	colColl, err := schema.NewColCollection(
		schema.NewColumn("pk", 0, types.IntKind, true),
		schema.NewColumn("v1", 1, types.IntKind, false),
		schema.NewColumn("v2", 2, types.IntKind, false))
	require.NoError(t, err)
	tableSch := schema.SchemaFromCols(colColl)
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
