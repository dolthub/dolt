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
	"fmt"
	"io"
	"sync"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

// IndexEditor takes in changes to an index map and returns the updated map if changes have been made.
//
// This type is thread-safe, and may be used in a multi-threaded environment.
type IndexEditor struct {
	keyCount            map[hash.Hash]int64
	ed                  types.EditAccumulator
	data                types.Map
	idx                 schema.Index
	idxSch              schema.Schema // idx.Schema() builds the schema every call, so we cache it here
	numOutstandingEdits uint64        // The number of edits that have been made since the last flush
	updated             bool          // Whether the data has changed since the editor was created

	// This mutex blocks on key count updates
	keyMutex *sync.Mutex
	// This mutex blocks on map edits
	mapMutex *sync.Mutex
	// This mutex ensures that Flush is only called once all current update operations have completed
	flushMutex *sync.RWMutex
}

const indexEditorMaxEdits = 16384

func NewIndexEditor(index schema.Index, indexData types.Map) *IndexEditor {
	return &IndexEditor{
		keyCount:            make(map[hash.Hash]int64),
		ed:                  types.CreateEditAccForMapEdits(indexData.Format()),
		data:                indexData,
		idx:                 index,
		idxSch:              index.Schema(),
		numOutstandingEdits: 0,
		updated:             false,
		keyMutex:            &sync.Mutex{},
		mapMutex:            &sync.Mutex{},
		flushMutex:          &sync.RWMutex{},
	}
}

// Flush applies all current edits to the underlying map.
func (indexEd *IndexEditor) Flush(ctx context.Context) error {
	indexEd.flushMutex.Lock()
	defer indexEd.flushMutex.Unlock()

	// We have to ensure that the edit accumulator is closed, otherwise it will cause a memory leak
	defer indexEd.ed.Close() // current edit accumulator is captured by defer

	if indexEd.idx.IsUnique() {
		for _, numOfKeys := range indexEd.keyCount {
			if numOfKeys > 1 {
				indexEd.reset(indexEd.data)
				return fmt.Errorf("UNIQUE constraint violation on index: %s", indexEd.idx.Name())
			}
		}
	}

	accEdits, err := indexEd.ed.FinishedEditing()
	if err != nil {
		indexEd.reset(indexEd.data)
		return err
	}
	newIndexData, _, err := types.ApplyEdits(ctx, accEdits, indexEd.data)
	if err != nil {
		indexEd.reset(indexEd.data)
		return err
	}
	indexEd.reset(newIndexData)
	return nil
}

// HasChanges returns whether the returned data would be different than the initial data.
func (indexEd *IndexEditor) HasChanges() bool {
	return indexEd.updated
}

// Index returns the index used for this editor.
func (indexEd *IndexEditor) Index() schema.Index {
	return indexEd.idx
}

// Map returns a Map based on the edits given, if any. If Flush() was not called prior, it will be called here.
func (indexEd *IndexEditor) Map(ctx context.Context) (types.Map, error) {
	indexEd.flushMutex.RLock() // if a Flush is ongoing then we need to wait

	needsFlush := false
	indexEd.mapMutex.Lock() // reads and writes to numOutstandingEdits is guarded by mapMutex
	if indexEd.numOutstandingEdits > 0 {
		indexEd.numOutstandingEdits = 0
		needsFlush = true
	}
	indexEd.mapMutex.Unlock()

	if needsFlush {
		indexEd.flushMutex.RUnlock() // Flush locks flushMutex, so we must unlock to prevent deadlock
		err := indexEd.Flush(ctx)    // if this panics and is caught higher up then we are fine since we read unlocked
		if err != nil {
			return types.EmptyMap, err
		}
		indexEd.flushMutex.RLock() // we must read lock again since needsFlush may be false and we unlock in that case
	}
	indexEd.flushMutex.RUnlock()
	return indexEd.data, nil
}

// UpdateIndex updates the index map according to the given reduced index rows.
func (indexEd *IndexEditor) UpdateIndex(ctx context.Context, originalIndexRow row.Row, updatedIndexRow row.Row) (err error) {
	defer indexEd.autoFlush(ctx, &err)
	indexEd.flushMutex.RLock()
	defer indexEd.flushMutex.RUnlock()

	if row.AreEqual(originalIndexRow, updatedIndexRow, indexEd.idxSch) {
		return nil
	}

	if originalIndexRow != nil {
		indexKey, err := originalIndexRow.NomsMapKey(indexEd.idxSch).Value(ctx)
		if err != nil {
			return err
		}
		if indexEd.idx.IsUnique() {
			partialKey, err := row.ReduceToIndexPartialKey(indexEd.idx, originalIndexRow)
			if err != nil {
				return err
			}
			if hasNulls, err := partialKey.Contains(types.NullValue); err != nil {
				return err
			} else if !hasNulls {
				partialKeyHash, err := partialKey.Hash(originalIndexRow.Format())
				if err != nil {
					return err
				}
				indexEd.keyMutex.Lock()
				indexEd.keyCount[partialKeyHash]--
				indexEd.keyMutex.Unlock()
			}
		}
		indexEd.mapMutex.Lock()
		indexEd.ed.AddEdit(indexKey, nil)
		indexEd.updated = true
		indexEd.numOutstandingEdits++
		indexEd.mapMutex.Unlock()
	}
	if updatedIndexRow != nil {
		indexKey, err := updatedIndexRow.NomsMapKey(indexEd.idxSch).Value(ctx)
		if err != nil {
			return err
		}
		if indexEd.idx.IsUnique() {
			partialKey, err := row.ReduceToIndexPartialKey(indexEd.idx, updatedIndexRow)
			if err != nil {
				return err
			}
			if hasNulls, err := partialKey.Contains(types.NullValue); err != nil {
				return err
			} else if !hasNulls {
				partialKeyHash, err := partialKey.Hash(updatedIndexRow.Format())
				if err != nil {
					return err
				}
				var mapIter table.TableReadCloser = noms.NewNomsRangeReader(indexEd.idxSch, indexEd.data,
					[]*noms.ReadRange{{Start: partialKey, Inclusive: true, Reverse: false, Check: func(tuple types.Tuple) (bool, error) {
						return tuple.StartsWith(partialKey), nil
					}}})
				_, err = mapIter.ReadRow(ctx)
				if err == nil { // row exists
					indexEd.keyMutex.Lock()
					indexEd.keyCount[partialKeyHash]++
					indexEd.keyMutex.Unlock()
				} else if err != io.EOF {
					return err
				}
				indexEd.keyMutex.Lock()
				indexEd.keyCount[partialKeyHash]++
				indexEd.keyMutex.Unlock()
			}
		}
		indexEd.mapMutex.Lock()
		indexEd.ed.AddEdit(indexKey, updatedIndexRow.NomsMapValue(indexEd.idxSch))
		indexEd.updated = true
		indexEd.numOutstandingEdits++
		indexEd.mapMutex.Unlock()
	}

	return nil
}

// autoFlush is called at the end of every write call (after all locks have been released) and checks if we need to
// automatically flush the edits.
func (indexEd *IndexEditor) autoFlush(ctx context.Context, err *error) {
	if *err != nil {
		return
	}
	indexEd.flushMutex.RLock()
	indexEd.mapMutex.Lock()
	runFlush := false
	if indexEd.numOutstandingEdits >= indexEditorMaxEdits {
		indexEd.numOutstandingEdits = 0
		runFlush = true
	}
	indexEd.mapMutex.Unlock()
	indexEd.flushMutex.RUnlock()

	if runFlush {
		*err = indexEd.Flush(ctx)
	}
}

func (indexEd *IndexEditor) reset(indexData types.Map) {
	indexEd.keyCount = make(map[hash.Hash]int64)
	indexEd.ed = types.CreateEditAccForMapEdits(indexData.Format())
	indexEd.data = indexData
	indexEd.numOutstandingEdits++
}

func RebuildIndex(ctx context.Context, tbl *doltdb.Table, indexName string) (types.Map, error) {
	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return types.EmptyMap, err
	}

	tableRowData, err := tbl.GetRowData(ctx)
	if err != nil {
		return types.EmptyMap, err
	}

	index := sch.Indexes().GetByName(indexName)
	if index == nil {
		return types.EmptyMap, fmt.Errorf("index `%s` does not exist", indexName)
	}

	rebuiltIndexData, err := rebuildIndexRowData(ctx, tbl.ValueReadWriter(), sch, tableRowData, index)
	if err != nil {
		return types.EmptyMap, err
	}
	return rebuiltIndexData, nil
}

func RebuildAllIndexes(ctx context.Context, t *doltdb.Table) (*doltdb.Table, error) {
	sch, err := t.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	if sch.Indexes().Count() == 0 {
		return t, nil
	}

	tableRowData, err := t.GetRowData(ctx)
	if err != nil {
		return nil, err
	}

	indexesMap, err := t.GetIndexData(ctx)
	if err != nil {
		return nil, err
	}

	for _, index := range sch.Indexes().AllIndexes() {
		rebuiltIndexRowData, err := rebuildIndexRowData(ctx, t.ValueReadWriter(), sch, tableRowData, index)
		if err != nil {
			return nil, err
		}
		rebuiltIndexRowDataRef, err := doltdb.WriteValAndGetRef(ctx, t.ValueReadWriter(), rebuiltIndexRowData)
		if err != nil {
			return nil, err
		}
		indexesMap, err = indexesMap.Edit().Set(types.String(index.Name()), rebuiltIndexRowDataRef).Map(ctx)
		if err != nil {
			return nil, err
		}
	}

	return t.SetIndexData(ctx, indexesMap)
}

func rebuildIndexRowData(ctx context.Context, vrw types.ValueReadWriter, sch schema.Schema, tblRowData types.Map, index schema.Index) (types.Map, error) {
	emptyIndexMap, err := types.NewMap(ctx, vrw)
	if err != nil {
		return types.EmptyMap, err
	}
	indexEditor := NewIndexEditor(index, emptyIndexMap)

	err = tblRowData.IterAll(ctx, func(key, value types.Value) error {
		dRow, err := row.FromNoms(sch, key.(types.Tuple), value.(types.Tuple))
		if err != nil {
			return err
		}
		indexRow, err := row.ReduceToIndex(index, dRow)
		if err != nil {
			return err
		}
		err = indexEditor.UpdateIndex(ctx, nil, indexRow)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return types.EmptyMap, err
	}

	rebuiltIndexMap, err := indexEditor.Map(ctx)
	if err != nil {
		return types.EmptyMap, err
	}
	return rebuiltIndexMap, nil
}
