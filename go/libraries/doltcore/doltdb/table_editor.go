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
	"io"
	"strings"
	"sync"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/typed/noms"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/utils/async"
	"github.com/liquidata-inc/dolt/go/store/hash"
	"github.com/liquidata-inc/dolt/go/store/types"
)

var ErrDuplicatePrimaryKeyFmt = "duplicate primary key given: %v"

// TableEditor supports making multiple row edits (inserts, updates, deletes) to a table. It does error checking for key
// collision etc. in the Close() method, as well as during Insert / Update.
//
// This type is thread-safe, and may be used in a multi-threaded environment.
type TableEditor struct {
	t        *Table
	tSch     schema.Schema
	tea      *tableEditAccumulator
	aq       *async.ActionExecutor
	nbf      *types.NomsBinFormat
	indexEds []*IndexEditor

	rowData types.Map // cached for GetRow and ContainsKey operations

	// This mutex blocks on each operation, so that map reads and updates are serialized
	writeMutex *sync.Mutex
	// This mutex ensures that Flush is only called once all current write operations have completed
	flushMutex *sync.RWMutex
}

type tableEditAccumulator struct {
	ed           types.EditAccumulator
	opCount      uint64
	insertedKeys map[hash.Hash]types.Value
	addedKeys    map[hash.Hash]types.Value
	removedKeys  map[hash.Hash]types.Value
	affectedKeys map[hash.Hash]types.Value
}

const tableEditorMaxOps = 16384

func NewTableEditor(ctx context.Context, t *Table, tableSch schema.Schema) (*TableEditor, error) {
	te := &TableEditor{
		t:          t,
		tSch:       tableSch,
		tea:        newTableEditAcc(t.Format()),
		nbf:        t.Format(),
		indexEds:   make([]*IndexEditor, tableSch.Indexes().Count()),
		writeMutex: &sync.Mutex{},
		flushMutex: &sync.RWMutex{},
	}
	var err error
	te.rowData, err = t.GetRowData(ctx)
	if err != nil {
		return nil, err
	}
	te.aq = async.NewActionExecutor(ctx, te.flushEditAccumulator, 1, 1)

	for i, index := range tableSch.Indexes().AllIndexes() {
		indexData, err := t.GetIndexRowData(ctx, index.Name())
		if err != nil {
			return nil, err
		}
		te.indexEds[i] = NewIndexEditor(index, indexData)
	}
	return te, nil
}

func newTableEditAcc(nbf *types.NomsBinFormat) *tableEditAccumulator {
	return &tableEditAccumulator{
		ed:           types.CreateEditAccForMapEdits(nbf),
		insertedKeys: make(map[hash.Hash]types.Value),
		addedKeys:    make(map[hash.Hash]types.Value),
		removedKeys:  make(map[hash.Hash]types.Value),
		affectedKeys: make(map[hash.Hash]types.Value),
	}
}

// Close ensures that all goroutines that may be open are properly disposed of. Attempting to call any other function
// on this editor after calling this function is undefined behavior.
func (te *TableEditor) Close() {
	te.tea.ed.Close()
	for _, indexEd := range te.indexEds {
		indexEd.ed.Close()
	}
}

// ContainsIndexedKey returns whether the given key is contained within the index. The key is assumed to be in the
// format expected of the index, similar to searching on the index map itself.
func (te *TableEditor) ContainsIndexedKey(ctx context.Context, key types.Tuple, indexName string) (bool, error) {
	te.Flush()
	te.flushMutex.RLock()
	defer te.flushMutex.RUnlock()

	err := te.aq.WaitForEmpty()
	if err != nil {
		return false, err
	}

	indexIter, err := te.getIndexIterator(ctx, key, indexName)
	if err != nil {
		return false, err
	}

	_, err = indexIter.ReadRow(ctx)
	if err == nil { // row exists
		return true, nil
	} else if err != io.EOF {
		return false, err
	} else {
		return false, nil
	}
}

// GetIndexedRows returns all matching rows for the given key on the index. The key is assumed to be in the format
// expected of the index, similar to searching on the index map itself.
func (te *TableEditor) GetIndexedRows(ctx context.Context, key types.Tuple, indexName string) ([]row.Row, error) {
	te.Flush()
	te.flushMutex.RLock()
	defer te.flushMutex.RUnlock()

	err := te.aq.WaitForEmpty()
	if err != nil {
		return nil, err
	}

	indexIter, err := te.getIndexIterator(ctx, key, indexName)
	if err != nil {
		return nil, err
	}

	var rows []row.Row
	var r row.Row
	for r, err = indexIter.ReadRow(ctx); err == nil; r, err = indexIter.ReadRow(ctx) {
		indexRowTaggedValues, err := row.GetTaggedVals(r)
		if err != nil {
			return nil, err
		}

		pkTuple := indexRowTaggedValues.NomsTupleForPKCols(te.nbf, te.tSch.GetPKCols())
		pkTupleVal, err := pkTuple.Value(ctx)
		if err != nil {
			return nil, err
		}

		fieldsVal, _, err := te.rowData.MaybeGet(ctx, pkTupleVal)
		if err != nil {
			return nil, err
		}
		if fieldsVal == nil {
			keyStr, err := formatKey(ctx, key)
			if err != nil {
				return nil, err
			}
			return nil, fmt.Errorf("index key `%s` does not have a corresponding entry in table", keyStr)
		}

		tableRow, err := row.FromNoms(te.tSch, pkTupleVal.(types.Tuple), fieldsVal.(types.Tuple))
		rows = append(rows, tableRow)
	}
	if err != io.EOF {
		return nil, err
	}
	return rows, nil
}

// GetRow returns the row matching the key given from the TableEditor. This is equivalent to calling Table and then
// GetRow on the returned table, but a tad faster.
func (te *TableEditor) GetRow(ctx context.Context, key types.Tuple) (row.Row, bool, error) {
	te.Flush()
	te.flushMutex.RLock()
	defer te.flushMutex.RUnlock()

	err := te.aq.WaitForEmpty()
	if err != nil {
		return nil, false, err
	}

	fieldsVal, _, err := te.rowData.MaybeGet(ctx, key)
	if err != nil {
		return nil, false, err
	}
	if fieldsVal == nil {
		return nil, false, nil
	}
	r, err := row.FromNoms(te.tSch, key, fieldsVal.(types.Tuple))
	if err != nil {
		return nil, false, err
	}
	return r, true, nil
}

// InsertRow adds the given row to the table. If the row already exists, use UpdateRow.
func (te *TableEditor) InsertRow(ctx context.Context, dRow row.Row) error {
	defer te.autoFlush()
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
	if _, ok := te.tea.addedKeys[keyHash]; ok {
		keyStr, err := formatKey(ctx, key)
		if err != nil {
			return err
		}
		return fmt.Errorf(ErrDuplicatePrimaryKeyFmt, keyStr)
	}
	te.tea.insertedKeys[keyHash] = key
	te.tea.addedKeys[keyHash] = key
	te.tea.affectedKeys[keyHash] = key

	te.tea.ed.AddEdit(key, dRow.NomsMapValue(te.tSch))
	te.tea.opCount++
	return nil
}

// DeleteKey removes the given key from the table.
func (te *TableEditor) DeleteKey(ctx context.Context, key types.Tuple) error {
	defer te.autoFlush()
	te.flushMutex.RLock()
	defer te.flushMutex.RUnlock()

	return te.delete(key)
}

// DeleteRow removes the given row from the table. This essentially acts as a convenience function for DeleteKey, while
// ensuring proper thread safety.
func (te *TableEditor) DeleteRow(ctx context.Context, dRow row.Row) error {
	defer te.autoFlush()
	te.flushMutex.RLock()
	defer te.flushMutex.RUnlock()

	key, err := dRow.NomsMapKey(te.tSch).Value(ctx)
	if err != nil {
		return errhand.BuildDError("failed to get row key").AddCause(err).Build()
	}

	return te.delete(key.(types.Tuple))
}

// UpdateRow takes the current row and new rows, and updates it accordingly.
func (te *TableEditor) UpdateRow(ctx context.Context, dOldRow row.Row, dNewRow row.Row) error {
	defer te.autoFlush()
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

	// Regarding the lock's position here, refer to the comment in InsertRow
	te.writeMutex.Lock()
	defer te.writeMutex.Unlock()

	// If the PK is changed then we need to delete the old value and insert the new one
	if !oldKeyEqualsNewKey {
		oldHash, err := dOldKeyVal.Hash(dOldRow.Format())
		if err != nil {
			return err
		}

		// If the old value of the primary key we just updated was previously inserted, then we need to remove it now.
		if _, ok := te.tea.insertedKeys[oldHash]; ok {
			delete(te.tea.insertedKeys, oldHash)
			te.tea.ed.AddEdit(dOldKeyVal, nil)
			te.tea.opCount++
		}

		te.tea.addedKeys[newHash] = dNewKeyVal
		te.tea.removedKeys[oldHash] = dOldKeyVal
		te.tea.affectedKeys[oldHash] = dOldKeyVal
	}

	te.tea.affectedKeys[newHash] = dNewKeyVal

	te.tea.ed.AddEdit(dNewKeyVal, dNewRow.NomsMapValue(te.tSch))
	te.tea.opCount++
	return nil
}

// Flush finalizes all of the changes made so far.
func (te *TableEditor) Flush() {
	te.flushMutex.Lock()
	defer te.flushMutex.Unlock()

	if te.tea.opCount > 0 {
		te.aq.Execute(te.tea)
		te.tea = newTableEditAcc(te.nbf)
	}
}

// Table returns a Table based on the edits given, if any. If Flush() was not called prior, it will be called here.
func (te *TableEditor) Table() (*Table, error) {
	te.Flush()
	err := te.aq.WaitForEmpty()
	return te.t, err
}

// autoFlush is called at the end of every write call (after all locks have been released) and checks if we need to
// automatically flush the edits.
func (te *TableEditor) autoFlush() {
	te.flushMutex.RLock()
	runFlush := te.tea.opCount >= tableEditorMaxOps
	te.flushMutex.RUnlock()

	if runFlush {
		te.Flush()
	}
}

func (te *TableEditor) delete(key types.Tuple) error {
	keyHash, err := key.Hash(te.nbf)
	if err != nil {
		return err
	}

	te.writeMutex.Lock()
	defer te.writeMutex.Unlock()

	delete(te.tea.addedKeys, keyHash)
	te.tea.removedKeys[keyHash] = key
	te.tea.affectedKeys[keyHash] = key

	te.tea.ed.AddEdit(key, nil)
	te.tea.opCount++
	return nil
}

func (te *TableEditor) flushEditAccumulator(ctx context.Context, teaInterface interface{}) error {
	// We don't call any locks here since this is called from an ActionExecutor with a concurrency of 1
	tea := teaInterface.(*tableEditAccumulator)
	defer tea.ed.Close()

	// For all added keys, check for and report a collision
	for keyHash, addedKey := range tea.addedKeys {
		if _, ok := tea.removedKeys[keyHash]; !ok {
			_, rowExists, err := te.rowData.MaybeGet(ctx, addedKey)
			if err != nil {
				return errhand.BuildDError("failed to read table").AddCause(err).Build()
			}
			if rowExists {
				keyStr, err := formatKey(ctx, addedKey)
				if err != nil {
					return err
				}
				return fmt.Errorf(ErrDuplicatePrimaryKeyFmt, keyStr)
			}
		}
	}
	// For all removed keys, remove the map entries that weren't added elsewhere by other updates
	for keyHash, removedKey := range tea.removedKeys {
		if _, ok := tea.addedKeys[keyHash]; !ok {
			tea.ed.AddEdit(removedKey, nil)
		}
	}

	accEdits, err := tea.ed.FinishedEditing()
	if err != nil {
		return errhand.BuildDError("failed to finalize table changes").AddCause(err).Build()
	}
	updatedMap, _, err := types.ApplyEdits(ctx, accEdits, te.rowData)
	if err != nil {
		return errhand.BuildDError("failed to modify table").AddCause(err).Build()
	}
	newTable, err := te.t.UpdateRows(ctx, updatedMap)
	if err != nil {
		return errhand.BuildDError("failed to update rows").AddCause(err).Build()
	}
	newTable, err = te.updateIndexes(ctx, tea, newTable, te.rowData, updatedMap)
	if err != nil {
		return errhand.BuildDError("failed to update indexes").AddCause(err).Build()
	}

	te.t = newTable
	te.rowData = updatedMap
	// not sure where it is, but setting these to nil fixes a memory leak
	tea.addedKeys = nil
	tea.affectedKeys = nil
	tea.ed = nil
	tea.insertedKeys = nil
	tea.removedKeys = nil
	return nil
}

// formatKey returns a comma-separated string representation of the key given.
func formatKey(ctx context.Context, key types.Value) (string, error) {
	tuple, ok := key.(types.Tuple)
	if !ok {
		return "", fmt.Errorf("Expected types.Tuple but got %T", key)
	}

	var vals []string
	iter, err := tuple.Iterator()
	if err != nil {
		return "", err
	}

	for iter.HasMore() {
		i, val, err := iter.Next()
		if err != nil {
			return "", err
		}
		if i%2 == 1 {
			str, err := types.EncodedValue(ctx, val)
			if err != nil {
				return "", err
			}
			vals = append(vals, str)
		}
	}

	return fmt.Sprintf("(%s)", strings.Join(vals, ",")), nil
}

func (te *TableEditor) getIndexIterator(ctx context.Context, key types.Tuple, indexName string) (table.TableReadCloser, error) {
	var indexEd *IndexEditor
	for _, ie := range te.indexEds {
		if ie.Index().Name() == indexName {
			indexEd = ie
			break
		}
	}
	if indexEd == nil {
		return nil, fmt.Errorf("could not find referenced index `%s`", indexName)
	}

	indexMap, err := indexEd.Map(ctx)
	if err != nil {
		return nil, err
	}
	return noms.NewNomsRangeReader(indexEd.idxSch, indexMap,
		[]*noms.ReadRange{{Start: key, Inclusive: true, Reverse: false, Check: func(tuple types.Tuple) (bool, error) {
			return tuple.StartsWith(key), nil
		}}},
	), nil
}

func (te *TableEditor) updateIndexes(ctx context.Context, tea *tableEditAccumulator, tbl *Table, originalRowData types.Map, updated types.Map) (*Table, error) {
	// We don't call any locks here since this is called from an ActionExecutor with a concurrency of 1
	if len(te.indexEds) == 0 {
		return tbl, nil
	}

	indexActionQueue := async.NewActionExecutor(ctx, func(_ context.Context, keyInt interface{}) error {
		key := keyInt.(types.Value)

		var originalRow row.Row
		var updatedRow row.Row

		if val, ok, err := originalRowData.MaybeGet(ctx, key); err == nil && ok {
			originalRow, err = row.FromNoms(te.tSch, key.(types.Tuple), val.(types.Tuple))
			if err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
		if val, ok, err := updated.MaybeGet(ctx, key); err == nil && ok {
			updatedRow, err = row.FromNoms(te.tSch, key.(types.Tuple), val.(types.Tuple))
			if err != nil {
				return err
			}
		} else if err != nil {
			return err
		}

		for _, indexEd := range te.indexEds {
			var err error
			var originalIndexRow row.Row
			var updatedIndexRow row.Row
			if originalRow != nil {
				originalIndexRow, err = originalRow.ReduceToIndex(indexEd.Index())
				if err != nil {
					return err
				}
			}
			if updatedRow != nil {
				updatedIndexRow, err = updatedRow.ReduceToIndex(indexEd.Index())
				if err != nil {
					return err
				}
			}

			err = indexEd.UpdateIndex(ctx, originalIndexRow, updatedIndexRow)
			if err != nil {
				return err
			}
		}

		return nil
	}, 4, 0)

	for _, key := range tea.affectedKeys {
		indexActionQueue.Execute(key)
	}

	err := indexActionQueue.WaitForEmpty()
	if err != nil {
		return nil, err
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
