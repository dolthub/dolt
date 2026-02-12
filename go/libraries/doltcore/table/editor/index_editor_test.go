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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	// The number of rows we expect the test to end up with
	indexEditorConcurrencyFinalCount = 100
)

func TestIndexEditorConcurrency(t *testing.T) {
	format := types.Format_LD_1
	_, vrw, _, err := dbfactory.MemFactory{}.CreateDB(context.Background(), format, nil, nil)
	require.Equal(t, format, vrw.Format())
	require.NoError(t, err)
	colColl := schema.NewColCollection(
		schema.NewColumn("pk", 0, types.IntKind, true),
		schema.NewColumn("v1", 1, types.IntKind, false),
		schema.NewColumn("v2", 2, types.IntKind, false))
	tableSch, err := schema.SchemaFromCols(colColl)
	require.NoError(t, err)
	index, err := tableSch.Indexes().AddIndexByColNames("idx_concurrency", []string{"v1"}, nil, schema.IndexProperties{IsUnique: false, Comment: ""})
	require.NoError(t, err)
	indexSch := index.Schema()
	emptyMap, err := types.NewMap(context.Background(), vrw)
	require.NoError(t, err)

	opts := TestEditorOptions(vrw)
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

func TestIndexEditorConcurrencyPostInsert(t *testing.T) {
	format := types.Format_LD_1
	_, vrw, _, err := dbfactory.MemFactory{}.CreateDB(context.Background(), format, nil, nil)
	require.Equal(t, format, vrw.Format())
	require.NoError(t, err)
	colColl := schema.NewColCollection(
		schema.NewColumn("pk", 0, types.IntKind, true),
		schema.NewColumn("v1", 1, types.IntKind, false),
		schema.NewColumn("v2", 2, types.IntKind, false))
	tableSch, err := schema.SchemaFromCols(colColl)
	require.NoError(t, err)
	index, err := tableSch.Indexes().AddIndexByColNames("idx_concurrency", []string{"v1"}, nil, schema.IndexProperties{IsUnique: false, Comment: ""})
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

	indexEditor = NewIndexEditor(context.Background(), index, indexData, tableSch, opts)
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

func TestIndexEditorUniqueMultipleNil(t *testing.T) {
	format := types.Format_LD_1
	_, vrw, _, err := dbfactory.MemFactory{}.CreateDB(context.Background(), format, nil, nil)
	require.Equal(t, format, vrw.Format())
	require.NoError(t, err)
	colColl := schema.NewColCollection(
		schema.NewColumn("pk", 0, types.IntKind, true),
		schema.NewColumn("v1", 1, types.IntKind, false))
	tableSch, err := schema.SchemaFromCols(colColl)
	require.NoError(t, err)
	index, err := tableSch.Indexes().AddIndexByColNames("idx_unique", []string{"v1"}, nil, schema.IndexProperties{IsUnique: true, Comment: ""})
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
	format := types.Format_LD_1
	_, vrw, _, err := dbfactory.MemFactory{}.CreateDB(context.Background(), format, nil, nil)
	require.Equal(t, format, vrw.Format())
	require.NoError(t, err)
	colColl := schema.NewColCollection(
		schema.NewColumn("pk", 0, types.IntKind, true),
		schema.NewColumn("v1", 1, types.IntKind, false),
		schema.NewColumn("v2", 2, types.IntKind, false))
	tableSch, err := schema.SchemaFromCols(colColl)
	require.NoError(t, err)
	index, err := tableSch.Indexes().AddIndexByColNames("idx_concurrency", []string{"v1"}, nil, schema.IndexProperties{IsUnique: false, Comment: ""})
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
	format := types.Format_LD_1
	_, vrw, _, err := dbfactory.MemFactory{}.CreateDB(context.Background(), format, nil, nil)
	require.Equal(t, format, vrw.Format())
	require.NoError(t, err)
	colColl := schema.NewColCollection(
		schema.NewColumn("pk", 0, types.IntKind, true),
		schema.NewColumn("v1", 1, types.IntKind, false))
	tableSch, err := schema.SchemaFromCols(colColl)
	require.NoError(t, err)
	index, err := tableSch.Indexes().AddIndexByColNames("idx_unq", []string{"v1"}, nil, schema.IndexProperties{IsUnique: true, Comment: ""})
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

func TestIndexEditorCapacityExceeded(t *testing.T) {
	// In the event that we reach the iea capacity on Undo, we need to verify that all code paths fail and remain failing
	ctx := context.Background()
	format := types.Format_LD_1
	_, vrw, _, err := dbfactory.MemFactory{}.CreateDB(ctx, format, nil, nil)
	require.Equal(t, format, vrw.Format())
	require.NoError(t, err)
	colColl := schema.NewColCollection(
		schema.NewColumn("pk", 0, types.IntKind, true),
		schema.NewColumn("v1", 1, types.IntKind, false))
	tableSch, err := schema.SchemaFromCols(colColl)
	require.NoError(t, err)
	index, err := tableSch.Indexes().AddIndexByColNames("idx_cap", []string{"v1"}, nil, schema.IndexProperties{IsUnique: false, Comment: ""})
	require.NoError(t, err)
	indexSch := index.Schema()
	emptyMap, err := types.NewMap(ctx, vrw)
	require.NoError(t, err)

	opts := Options{Deaf: NewInMemDeafWithMaxCapacity(vrw, 224)}
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
	require.Contains(t, indexEditor.Close().Error(), "unrecoverable state")
	_, err = indexEditor.HasPartial(ctx, partialKey)
	require.Contains(t, err.Error(), "unrecoverable state")
	_, err = indexEditor.Map(ctx)
	require.Contains(t, err.Error(), "unrecoverable state")
}
