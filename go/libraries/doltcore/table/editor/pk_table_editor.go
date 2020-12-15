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
	"strings"
	"sync"

	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/dolthub/dolt/go/libraries/utils/async"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	tableEditorMaxOps = 16384
)

var ErrDuplicatePrimaryKeyFmt = "duplicate primary key given: %v"

type TableEditor interface {
	InsertRow(ctx context.Context, r row.Row) error
	UpdateRow(ctx context.Context, old, new row.Row) error
	DeleteRow(ctx context.Context, r row.Row) error

	GetAutoIncrementValue() types.Value
	SetAutoIncrementValue(v types.Value) (err error)

	Table(ctx context.Context) (*doltdb.Table, error)
	Schema() schema.Schema
	Name() string
	Format() *types.NomsBinFormat

	Close() error
}

func NewTableEditor(ctx context.Context, t *doltdb.Table, tableSch schema.Schema, name string) (TableEditor, error) {
	if schema.IsKeyless(tableSch) {
		return newKeylessTableEditor(ctx, t, tableSch, name)
	}
	return newPkTableEditor(ctx, t, tableSch, name)
}

// pkTableEditor supports making multiple row edits (inserts, updates, deletes) to a table. It does error checking for key
// collision etc. in the Close() method, as well as during Insert / Update.
//
// This type is thread-safe, and may be used in a multi-threaded environment.
type pkTableEditor struct {
	t    *doltdb.Table
	tSch schema.Schema
	name string

	tea      *tableEditAccumulator
	aq       *async.ActionExecutor
	nbf      *types.NomsBinFormat
	indexEds []*IndexEditor

	rowData types.Map // cached for GetRow and ContainsKey operations

	hasAutoInc bool
	autoIncCol schema.Column
	autoIncVal types.Value

	// This mutex blocks on each operation, so that map reads and updates are serialized
	writeMutex *sync.Mutex
	// This mutex ensures that Flush is only called once all current write operations have completed
	flushMutex *sync.RWMutex
}

var _ TableEditor = &pkTableEditor{}

type tableEditAccumulator struct {
	ed           types.EditAccumulator
	opCount      uint64
	insertedKeys map[hash.Hash]types.Value
	addedKeys    map[hash.Hash]types.Value
	removedKeys  map[hash.Hash]types.Value
	affectedKeys map[hash.Hash]types.Value
}

func newPkTableEditor(ctx context.Context, t *doltdb.Table, tableSch schema.Schema, name string) (*pkTableEditor, error) {
	te := &pkTableEditor{
		t:          t,
		tSch:       tableSch,
		name:       name,
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

	err = tableSch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		if col.AutoIncrement {
			te.autoIncVal, err = t.GetAutoIncrementValue(ctx)
			if err != nil {
				return true, err
			}
			te.hasAutoInc = true
			te.autoIncCol = col
			return true, err
		}
		return false, nil
	})
	if err != nil {
		return nil, err
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

// ContainsIndexedKey returns whether the given key is contained within the index. The key is assumed to be in the
// format expected of the index, similar to searching on the index map itself.
func ContainsIndexedKey(ctx context.Context, te TableEditor, key types.Tuple, indexName string) (bool, error) {
	tbl, err := te.Table(ctx)
	if err != nil {
		return false, err
	}

	idxSch := te.Schema().Indexes().GetByName(indexName)
	idxMap, err := tbl.GetIndexRowData(ctx, indexName)
	if err != nil {
		return false, err
	}

	indexIter := noms.NewNomsRangeReader(idxSch.Schema(), idxMap,
		[]*noms.ReadRange{{Start: key, Inclusive: true, Reverse: false, Check: func(tuple types.Tuple) (bool, error) {
			return tuple.StartsWith(key), nil
		}}},
	)

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
func GetIndexedRows(ctx context.Context, te TableEditor, key types.Tuple, indexName string) ([]row.Row, error) {
	tbl, err := te.Table(ctx)
	if err != nil {
		return nil, err
	}

	idxSch := te.Schema().Indexes().GetByName(indexName)
	idxMap, err := tbl.GetIndexRowData(ctx, indexName)
	if err != nil {
		return nil, err
	}

	indexIter := noms.NewNomsRangeReader(idxSch.Schema(), idxMap,
		[]*noms.ReadRange{{Start: key, Inclusive: true, Reverse: false, Check: func(tuple types.Tuple) (bool, error) {
			return tuple.StartsWith(key), nil
		}}},
	)

	rowData, err := tbl.GetRowData(ctx)
	if err != nil {
		return nil, err
	}

	var rows []row.Row
	for {
		r, err := indexIter.ReadRow(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		indexRowTaggedValues, err := row.GetTaggedVals(r)
		if err != nil {
			return nil, err
		}

		pkTuple := indexRowTaggedValues.NomsTupleForPKCols(te.Format(), te.Schema().GetPKCols())
		pkTupleVal, err := pkTuple.Value(ctx)
		if err != nil {
			return nil, err
		}

		fieldsVal, _, err := rowData.MaybeGet(ctx, pkTupleVal)
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

		tableRow, err := row.FromNoms(te.Schema(), pkTupleVal.(types.Tuple), fieldsVal.(types.Tuple))
		rows = append(rows, tableRow)
	}

	return rows, nil
}

// InsertRow adds the given row to the table. If the row already exists, use UpdateRow.
func (te *pkTableEditor) InsertRow(ctx context.Context, dRow row.Row) error {
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
	// func Example(ctx context.Context, te *pkTableEditor, someRow row.Row) {
	//     go te.Insert(ctx, someRow)
	//     go te.Delete(ctx, someRow)
	// }
	// Let's pretend the table already has someRow. Go will run goroutines in any arbitrary order, sequentially or
	// concurrently, and thus running Example() may see that Delete() executes before Insert(), causing a different
	// result as Insert() executing first would result in an error. Such an issue must be handled above the pkTableEditor.
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

	if te.hasAutoInc {
		// autoIncVal = max(autoIncVal, insertVal)
		insertVal, ok := dRow.GetColVal(te.autoIncCol.Tag)
		if ok {
			less, err := te.autoIncVal.Less(te.nbf, insertVal)
			if err != nil {
				return err
			}
			if less {
				te.autoIncVal = types.Round(insertVal)
			}
			te.autoIncVal = types.Increment(te.autoIncVal)
		}
	}

	te.tea.ed.AddEdit(key, dRow.NomsMapValue(te.tSch))
	te.tea.opCount++
	return nil
}

// DeleteRow removes the given row from the table. This essentially acts as a convenience function for DeleteKey, while
// ensuring proper thread safety.
func (te *pkTableEditor) DeleteRow(ctx context.Context, dRow row.Row) error {
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
func (te *pkTableEditor) UpdateRow(ctx context.Context, dOldRow row.Row, dNewRow row.Row) error {
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

func (te *pkTableEditor) GetAutoIncrementValue() types.Value {
	return te.autoIncVal
}

func (te *pkTableEditor) SetAutoIncrementValue(v types.Value) (err error) {
	te.autoIncVal = v
	te.t, err = te.t.SetAutoIncrementValue(te.autoIncVal)
	return
}

// Table returns a Table based on the edits given, if any. If Flush() was not called prior, it will be called here.
func (te *pkTableEditor) Table(ctx context.Context) (*doltdb.Table, error) {
	te.flush()
	err := te.aq.WaitForEmpty()

	if te.hasAutoInc {
		te.t, err = te.t.SetAutoIncrementValue(te.autoIncVal)
		if err != nil {
			return nil, err
		}
	}

	return te.t, err
}

func (te *pkTableEditor) Schema() schema.Schema {
	return te.tSch
}

func (te *pkTableEditor) Name() string {
	return te.name
}

func (te *pkTableEditor) Format() *types.NomsBinFormat {
	return te.nbf
}

// Close ensures that all goroutines that may be open are properly disposed of. Attempting to call any other function
// on this editor after calling this function is undefined behavior.
func (te *pkTableEditor) Close() error {
	te.tea.ed.Close()
	for _, indexEd := range te.indexEds {
		indexEd.ed.Close()
	}
	return nil
}

// Flush finalizes all of the changes made so far.
func (te *pkTableEditor) flush() {
	te.flushMutex.Lock()
	defer te.flushMutex.Unlock()

	if te.tea.opCount > 0 {
		te.aq.Execute(te.tea)
		te.tea = newTableEditAcc(te.nbf)
	}
}

// autoFlush is called at the end of every write call (after all locks have been released) and checks if we need to
// automatically flush the edits.
func (te *pkTableEditor) autoFlush() {
	te.flushMutex.RLock()
	te.writeMutex.Lock()
	runFlush := te.tea.opCount >= tableEditorMaxOps
	te.writeMutex.Unlock()
	te.flushMutex.RUnlock()

	if runFlush {
		te.flush()
	}
}

func (te *pkTableEditor) delete(key types.Tuple) error {
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

func (te *pkTableEditor) flushEditAccumulator(ctx context.Context, teaInterface interface{}) error {
	// We don'tbl call any locks here since this is called from an ActionExecutor with a concurrency of 1
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
	// For all removed keys, remove the map entries that weren'tbl added elsewhere by other updates
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

func (te *pkTableEditor) updateIndexes(ctx context.Context, tea *tableEditAccumulator, tbl *doltdb.Table, originalRowData types.Map, updated types.Map) (*doltdb.Table, error) {
	// We don'tbl call any locks here since this is called from an ActionExecutor with a concurrency of 1
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
				originalIndexRow, err = row.ReduceToIndex(indexEd.Index(), originalRow)
				if err != nil {
					return err
				}
			}
			if updatedRow != nil {
				updatedIndexRow, err = row.ReduceToIndex(indexEd.Index(), updatedRow)
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
