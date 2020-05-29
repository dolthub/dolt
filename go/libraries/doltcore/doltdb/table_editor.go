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
	"fmt"
	"sync"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/store/hash"
	"github.com/liquidata-inc/dolt/go/store/types"
)

var ErrDuplicatePrimaryKeyFmt = "duplicate primary key given: (%v)"

// TableEditor supports making multiple row edits (inserts, updates, deletes) to a table. It does error checking for key
// collision etc. in the Close() method, as well as during Insert / Update.
//
// This type is thread-safe, and may be used in a multi-threaded environment.
type TableEditor struct {
	t            *Table
	tSch         schema.Schema
	ed           *types.MapEditor
	updated      bool // Whether the table has been updated
	insertedKeys map[hash.Hash]types.Value
	addedKeys    map[hash.Hash]types.Value
	removedKeys  map[hash.Hash]types.Value
	affectedKeys map[hash.Hash]types.Value
	indexEds     []*IndexEditor

	// This mutex blocks on each operation, so that map reads and updates are serialized
	writeMutex *sync.Mutex
	// This mutex ensures that Flush is only called once all current write operations have completed
	flushMutex *sync.RWMutex
}

func NewTableEditor(ctx context.Context, t *Table, tableSch schema.Schema) (*TableEditor, error) {
	// initialize the mutexes here since they're not reset
	te := &TableEditor{
		writeMutex: &sync.Mutex{},
		flushMutex: &sync.RWMutex{},
	}
	err := te.reset(ctx, t, tableSch)
	if err != nil {
		return nil, err
	}
	return te, nil
}

func (te *TableEditor) Insert(ctx context.Context, dRow row.Row) error {
	te.flushMutex.RLock()
	defer te.flushMutex.RUnlock()

	key, err := dRow.NomsMapKey(te.tSch).Value(ctx)
	if err != nil {
		return errhand.BuildDError("failed to get row key").AddCause(err).Build()
	}
	keyHash, err := key.Hash(dRow.Format())
	if err != nil {
		return err
	}

	// We allow each write operation to perform as much work as possible before acquiring the lock. This minimizes the
	// lock time and slightly increases throughput. Although this introduces variability in the amount of time before
	// the lock is acquired, this is a non-issue, which is elaborated on using this example function.
	// func Example(ctx context.Context, te *TableEditor, someRow row.Row) {
	//     go te.Insert(ctx, someRow)
	//     go te.Delete(ctx, someRow)
	// }
	// Let's pretend the table already has someRow. Go will run goroutines in any arbitrary order, sequentially or
	// concurrently, and thus running Example() may see that Delete() executes before Insert(), causing a different
	// result as Insert() executing first would result in an error. Such an issue must be handled above the TableEditor.
	// Since we cannot guarantee any of that here, we can delay our lock acquisition.
	te.writeMutex.Lock()
	defer te.writeMutex.Unlock()

	// If we've already inserted this key as part of this insert operation, that's an error. Inserting a row that
	// already exists in the table will be handled in Close().
	if _, ok := te.addedKeys[keyHash]; ok {
		value, err := types.EncodedValue(ctx, key)
		if err != nil {
			return err
		}
		return fmt.Errorf(ErrDuplicatePrimaryKeyFmt, value)
	}
	te.insertedKeys[keyHash] = key
	te.addedKeys[keyHash] = key
	te.affectedKeys[keyHash] = key

	te.ed = te.ed.Set(key, dRow.NomsMapValue(te.tSch))
	te.updated = true
	return nil
}

func (te *TableEditor) Delete(ctx context.Context, dRow row.Row) error {
	te.flushMutex.RLock()
	defer te.flushMutex.RUnlock()

	key, err := dRow.NomsMapKey(te.tSch).Value(ctx)
	if err != nil {
		return errhand.BuildDError("failed to get row key").AddCause(err).Build()
	}
	keyHash, err := key.Hash(dRow.Format())
	if err != nil {
		return err
	}

	// Regarding the lock's position here, refer to the comment in Insert()
	te.writeMutex.Lock()
	defer te.writeMutex.Unlock()

	delete(te.addedKeys, keyHash)
	te.removedKeys[keyHash] = key
	te.affectedKeys[keyHash] = key

	te.ed = te.ed.Remove(key)
	te.updated = true
	return nil
}

func (te *TableEditor) Update(ctx context.Context, dOldRow row.Row, dNewRow row.Row) error {
	te.flushMutex.RLock()
	defer te.flushMutex.RUnlock()

	dOldKey := dOldRow.NomsMapKey(te.tSch)
	dOldKeyVal, err := dOldKey.Value(ctx)
	if err != nil {
		return err
	}
	dNewKey := dNewRow.NomsMapKey(te.tSch)
	dNewKeyVal, err := dNewKey.Value(ctx)
	if err != nil {
		return err
	}

	newHash, err := dNewKeyVal.Hash(dNewRow.Format())
	if err != nil {
		return err
	}
	oldKeyEqualsNewKey := dOldKeyVal.Equals(dNewKeyVal)

	// Regarding the lock's position here, refer to the comment in Insert()
	te.writeMutex.Lock()
	defer te.writeMutex.Unlock()

	// If the PK is changed then we need to delete the old value and insert the new one
	if !oldKeyEqualsNewKey {
		oldHash, err := dOldKeyVal.Hash(dOldRow.Format())
		if err != nil {
			return err
		}

		// If the old value of the primary key we just updated was previously inserted, then we need to remove it now.
		if _, ok := te.insertedKeys[oldHash]; ok {
			delete(te.insertedKeys, oldHash)
			te.ed.Remove(dOldKeyVal)
		}

		te.addedKeys[newHash] = dNewKeyVal
		te.removedKeys[oldHash] = dOldKeyVal
		te.affectedKeys[oldHash] = dOldKeyVal
	}

	te.affectedKeys[newHash] = dNewKeyVal

	te.ed.Set(dNewKeyVal, dNewRow.NomsMapValue(te.tSch))
	te.updated = true
	return nil
}

// Flush finalizes all of the changes and returns the updated Table.
func (te *TableEditor) Flush(ctx context.Context) (*Table, error) {
	te.flushMutex.Lock()
	defer te.flushMutex.Unlock()

	if !te.updated {
		return te.t, nil
	}

	// For all added keys, check for and report a collision
	for keyHash, addedKey := range te.addedKeys {
		if _, ok := te.removedKeys[keyHash]; !ok {
			_, rowExists, err := te.t.GetRow(ctx, addedKey.(types.Tuple), te.tSch)
			if err != nil {
				return nil, errhand.BuildDError("failed to read table").AddCause(err).Build()
			}
			if rowExists {
				value, err := types.EncodedValue(ctx, addedKey)
				if err != nil {
					return nil, err
				}
				return nil, fmt.Errorf(ErrDuplicatePrimaryKeyFmt, value)
			}
		}
	}
	// For all removed keys, remove the map entries that weren't added elsewhere by other updates
	for keyHash, removedKey := range te.removedKeys {
		if _, ok := te.addedKeys[keyHash]; !ok {
			te.ed.Remove(removedKey)
		}
	}

	updated, err := te.ed.Map(ctx)
	if err != nil {
		return nil, errhand.BuildDError("failed to modify table").AddCause(err).Build()
	}
	originalRowData, err := te.t.GetRowData(ctx)
	if err != nil {
		return nil, errhand.BuildDError("failed to read table").AddCause(err).Build()
	}
	newTable, err := te.t.UpdateRows(ctx, updated)
	if err != nil {
		return nil, errhand.BuildDError("failed to update rows").AddCause(err).Build()
	}
	newTable, err = te.updateIndexes(ctx, newTable, originalRowData, updated)
	if err != nil {
		return nil, errhand.BuildDError("failed to update indexes").AddCause(err).Build()
	}

	// Set the TableEditor to the new table state
	err = te.reset(ctx, newTable, te.tSch)
	if err != nil {
		return nil, err
	}

	return newTable, nil
}

// reset sets the TableEditor to the given table
func (te *TableEditor) reset(ctx context.Context, t *Table, tableSch schema.Schema) error {
	tableData, err := t.GetRowData(ctx)
	if err != nil {
		return errhand.BuildDError("failed to get row data.").AddCause(err).Build()
	}

	te.t = t
	te.tSch = tableSch
	te.ed = tableData.Edit()
	te.updated = false
	te.insertedKeys = make(map[hash.Hash]types.Value)
	te.addedKeys = make(map[hash.Hash]types.Value)
	te.removedKeys = make(map[hash.Hash]types.Value)
	te.affectedKeys = make(map[hash.Hash]types.Value)
	te.indexEds = make([]*IndexEditor, tableSch.Indexes().Count())

	for i, index := range tableSch.Indexes().AllIndexes() {
		indexData, err := t.GetIndexRowData(ctx, index.Name())
		if err != nil {
			panic(err) // should never have an index that does not have data, even an empty index
		}
		te.indexEds[i] = NewIndexEditor(index, indexData)
	}
	return nil
}

func (te *TableEditor) updateIndexes(ctx context.Context, tbl *Table, originalRowData types.Map, updated types.Map) (*Table, error) {
	// We don't call any locks here since this is only called from Flush, which acquires a lock
	if len(te.indexEds) == 0 {
		return tbl, nil
	}

	for _, key := range te.affectedKeys {
		var originalRow row.Row
		var updatedRow row.Row

		if val, ok, err := originalRowData.MaybeGet(ctx, key); err == nil && ok {
			originalRow, err = row.FromNoms(te.tSch, key.(types.Tuple), val.(types.Tuple))
			if err != nil {
				return nil, err
			}
		} else if err != nil {
			return nil, err
		}
		if val, ok, err := updated.MaybeGet(ctx, key); err == nil && ok {
			updatedRow, err = row.FromNoms(te.tSch, key.(types.Tuple), val.(types.Tuple))
			if err != nil {
				return nil, err
			}
		} else if err != nil {
			return nil, err
		}

		for _, indexEd := range te.indexEds {
			var err error
			var originalIndexRow row.Row
			var updatedIndexRow row.Row
			if originalRow != nil {
				originalIndexRow, err = originalRow.ReduceToIndex(indexEd.Index())
				if err != nil {
					return nil, err
				}
			}
			if updatedRow != nil {
				updatedIndexRow, err = updatedRow.ReduceToIndex(indexEd.Index())
				if err != nil {
					return nil, err
				}
			}

			err = indexEd.UpdateIndex(ctx, originalIndexRow, updatedIndexRow)
			if err != nil {
				return nil, err
			}
		}
	}

	for _, indexEd := range te.indexEds {
		if !indexEd.HasChanges() {
			continue
		}
		indexMap, err := indexEd.Map(ctx)
		if err != nil {
			return nil, err
		}
		tbl, err = tbl.SetIndexRowData(ctx, indexEd.Index().Name(), indexMap)
		if err != nil {
			return nil, err
		}
	}

	return tbl, nil
}
