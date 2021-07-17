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
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/dolthub/dolt/go/libraries/utils/async"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

var (
	tableEditorMaxOps int64 = 16384
	ErrDuplicateKey         = errors.New("duplicate key error")
)

func init() {
	if maxOpsEnv := os.Getenv("DOLT_EDIT_TABLE_BUFFER_ROWS"); maxOpsEnv != "" {
		if v, err := strconv.ParseUint(maxOpsEnv, 10, 63); err == nil {
			tableEditorMaxOps = int64(v)
		}
	}
}

type PKDuplicateErrFunc func(keyString string, k, v types.Tuple, isPk bool) error

type TableEditor interface {
	InsertKeyVal(ctx context.Context, key, val types.Tuple, tagToVal map[uint64]types.Value, errFunc PKDuplicateErrFunc) error

	InsertRow(ctx context.Context, r row.Row, errFunc PKDuplicateErrFunc) error
	UpdateRow(ctx context.Context, old, new row.Row, errFunc PKDuplicateErrFunc) error
	DeleteRow(ctx context.Context, r row.Row) error

	GetAutoIncrementValue() types.Value
	SetAutoIncrementValue(v types.Value) (err error)

	Table(ctx context.Context) (*doltdb.Table, error)
	Schema() schema.Schema
	Name() string
	Format() *types.NomsBinFormat

	StatementStarted(ctx context.Context)
	StatementFinished(ctx context.Context, errored bool) error

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
	savedTea *tableEditAccumulator
	aq       *async.ActionExecutor
	nbf      *types.NomsBinFormat
	indexEds []*IndexEditor

	hasAutoInc bool
	autoIncCol schema.Column
	autoIncVal types.Value

	// This mutex blocks on each operation, so that map reads and updates are serialized
	writeMutex *sync.Mutex
	// This mutex ensures that Flush is only called once all current write operations have completed
	flushMutex *sync.RWMutex
}

type doltKVP struct {
	k types.LesserValuable
	v types.Valuable
}

var _ TableEditor = &pkTableEditor{}

type tableEditAccumulator struct {
	// This is the tableEditAccumulator that is currently processing on the background thread. Once that thread has
	// finished, it updates rowData and sets this to nil.
	prevTea *tableEditAccumulator

	// This is the map equivalent of the previous tableEditAccumulator, represented by prevTea. While the background
	// thread is processing prevTea, this will be an empty map. Once the thread has finished, it will update this map
	// to be equivalent in content to prevTea, and will set prevTea to nil.
	rowData types.Map

	nbf         *types.NomsBinFormat
	opCount     int64
	addedKeys   map[hash.Hash]*doltKVP
	removedKeys map[hash.Hash]types.LesserValuable
}

func newPkTableEditor(ctx context.Context, t *doltdb.Table, tableSch schema.Schema, name string) (*pkTableEditor, error) {
	te := &pkTableEditor{
		t:          t,
		tSch:       tableSch,
		name:       name,
		nbf:        t.Format(),
		indexEds:   make([]*IndexEditor, tableSch.Indexes().Count()),
		writeMutex: &sync.Mutex{},
		flushMutex: &sync.RWMutex{},
	}
	var err error
	rowData, err := t.GetRowData(ctx)
	if err != nil {
		return nil, err
	}
	te.tea = createInitialTableEditAcc(t.Format(), rowData)
	// Warning: changing this from a concurrency of 1 will introduce race conditions, thus much would need to be changed.
	// All of the logic is built upon the assumption that edit accumulators are processed sequentially.
	te.aq = async.NewActionExecutor(ctx, te.flushEditAccumulator, 1, 1)

	for i, index := range tableSch.Indexes().AllIndexes() {
		indexData, err := t.GetIndexRowData(ctx, index.Name())
		if err != nil {
			return nil, err
		}
		te.indexEds[i] = NewIndexEditor(ctx, index, indexData, tableSch)
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

// createInitialTableEditAcc creates the initial tableEditAccumulator. All future teas should use the method
// NewFromCurrent.
func createInitialTableEditAcc(nbf *types.NomsBinFormat, rowData types.Map) *tableEditAccumulator {
	return &tableEditAccumulator{
		prevTea:     nil,
		rowData:     rowData,
		nbf:         nbf,
		addedKeys:   make(map[hash.Hash]*doltKVP),
		removedKeys: make(map[hash.Hash]types.LesserValuable),
	}
}

// NewFromCurrent returns a new tableEditAccumulator that references the current tableEditAccumulator.
func (tea *tableEditAccumulator) NewFromCurrent() *tableEditAccumulator {
	return &tableEditAccumulator{
		prevTea:     tea,
		rowData:     types.EmptyMap,
		nbf:         tea.nbf,
		addedKeys:   make(map[hash.Hash]*doltKVP),
		removedKeys: make(map[hash.Hash]types.LesserValuable),
	}
}

// maybeGet returns a *doltKVP if the current tableEditAccumulator contains the given key, or it exists in the row data.
// This assumes that the given hash is for the given key.
func (tea *tableEditAccumulator) maybeGet(ctx context.Context, keyHash hash.Hash, key types.LesserValuable) (*doltKVP, bool, error) {
	// No locks as all calls and modifications to tea are done from a lock that the caller handles
	if kvp, ok := tea.addedKeys[keyHash]; ok {
		return kvp, true, nil
	}
	if _, ok := tea.removedKeys[keyHash]; !ok {
		// When rowData is updated, prevTea is set to nil. Therefore, if prevTea is non-nil, we use it.
		if tea.prevTea != nil {
			return tea.prevTea.maybeGet(ctx, keyHash, key)
		} else {
			keyVal, err := key.Value(ctx)
			if err != nil {
				return nil, false, err
			}

			keyTup := keyVal.(types.Tuple)
			v, ok, err := tea.rowData.MaybeGetTuple(ctx, keyTup)
			if err != nil {
				return nil, false, err
			}
			if !ok {
				return nil, false, nil
			}

			return &doltKVP{k: keyTup, v: v}, true, err
		}
	}
	return nil, false, nil
}

// ContainsIndexedKey returns whether the given key is contained within the index. The key is assumed to be in the
// format expected of the index, similar to searching on the index map itself.
func ContainsIndexedKey(ctx context.Context, te TableEditor, key types.Tuple, indexName string, idxSch schema.Schema) (bool, error) {
	// If we're working with a pkTableEditor, then we don't need to flush the table nor indexes.
	// TODO : max - do i need to add keyless table editor here?
	if pkTe, ok := te.(*pkTableEditor); ok {
		for _, indexEd := range pkTe.indexEds {
			if indexEd.idx.Name() == indexName {
				return indexEd.HasPartial(ctx, key)
			}
		}
		return false, fmt.Errorf("an index editor for `%s` could not be found", indexName)
	}

	tbl, err := te.Table(ctx)
	if err != nil {
		return false, err
	}

	idxMap, err := tbl.GetIndexRowData(ctx, indexName)
	if err != nil {
		return false, err
	}

	indexIter := noms.NewNomsRangeReader(idxSch, idxMap,
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
func GetIndexedRows(ctx context.Context, te TableEditor, key types.Tuple, indexName string, idxSch schema.Schema) ([]row.Row, error) {
	tbl, err := te.Table(ctx)
	if err != nil {
		return nil, err
	}

	idxMap, err := tbl.GetIndexRowData(ctx, indexName)
	if err != nil {
		return nil, err
	}

	indexIter := noms.NewNomsRangeReader(idxSch, idxMap,
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

		indexRowTaggedValues, err := r.TaggedValues()
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
		if err != nil {
			return nil, err
		}

		rows = append(rows, tableRow)
	}

	return rows, nil
}

func (te *pkTableEditor) keyErrForKVP(ctx context.Context, kvp *doltKVP, isPk bool, errFunc PKDuplicateErrFunc) error {
	kVal, err := kvp.k.Value(ctx)
	if err != nil {
		return err
	}

	vVal, err := kvp.v.Value(ctx)
	if err != nil {
		return err
	}

	keyStr, err := formatKey(ctx, kVal)
	if err != nil {
		return err
	}

	if errFunc != nil {
		return errFunc(keyStr, kVal.(types.Tuple), vVal.(types.Tuple), isPk)
	} else {
		return fmt.Errorf("duplicate key '%s': %w", keyStr, ErrDuplicateKey)
	}
}

// InsertKeyVal adds the given tuples to the table.
func (te *pkTableEditor) InsertKeyVal(ctx context.Context, key, val types.Tuple, tagToVal map[uint64]types.Value, errFunc PKDuplicateErrFunc) (retErr error) {
	defer te.autoFlush()
	te.flushMutex.RLock()
	defer te.flushMutex.RUnlock()

	keyHash, err := key.Hash(te.nbf)
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

	// Run the index editors first, as we can back out of the changes in the event of an error, but can't do that for
	// changes made to the table. We create a slice that matches the number of index editors. For each successful
	// operation, we increment the associated index on the slice, and in the event of an error, we undo that number of
	// operations.
	indexOpsToUndo := make([]int, len(te.indexEds))
	defer func() {
		if retErr != nil {
			for i, opsToUndo := range indexOpsToUndo {
				for undone := 0; undone < opsToUndo; undone++ {
					te.indexEds[i].Undo(ctx)
				}
			}
		}
	}()

	for i, indexEd := range te.indexEds {
		fullKey, partialKey, err := row.ReduceToIndexKeysFromTagMap(te.nbf, indexEd.Index(), tagToVal)
		if err != nil {
			return err
		}
		err = indexEd.InsertRow(ctx, fullKey, partialKey, types.Tuple{})
		if uke, ok := err.(*uniqueKeyErr); ok {
			tableTupleHash, err := uke.TableTuple.Hash(uke.TableTuple.Format())
			if err != nil {
				return err
			}
			kvp, pkExists, err := te.tea.maybeGet(ctx, tableTupleHash, uke.TableTuple)
			if err != nil {
				return err
			}
			if !pkExists {
				keyStr, _ := formatKey(ctx, uke.TableTuple)
				return fmt.Errorf("UNIQUE constraint violation on index '%s', but could not find row with primary key: %s",
					indexEd.Index().Name(), keyStr)
			}
			return te.keyErrForKVP(ctx, kvp, false, errFunc)
		} else if err != nil {
			return err
		}
		indexOpsToUndo[i]++
	}

	if kvp, pkExists, err := te.tea.maybeGet(ctx, keyHash, key); err != nil {
		return err
	} else if pkExists {
		return te.keyErrForKVP(ctx, kvp, true, errFunc)
	}

	delete(te.tea.removedKeys, keyHash)
	te.tea.addedKeys[keyHash] = &doltKVP{k: key, v: val}

	if te.hasAutoInc {
		insertVal, ok := tagToVal[te.autoIncCol.Tag]
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

	te.tea.opCount++
	return nil
}

// InsertRow adds the given row to the table. If the row already exists, use UpdateRow. This converts the given row into
// tuples that are then passed to InsertKeyVal.
func (te *pkTableEditor) InsertRow(ctx context.Context, dRow row.Row, errFunc PKDuplicateErrFunc) error {
	key, err := dRow.NomsMapKey(te.tSch).Value(ctx)
	if err != nil {
		return err
	}
	val, err := dRow.NomsMapValue(te.tSch).Value(ctx)
	if err != nil {
		return err
	}
	tagToVal := make(map[uint64]types.Value)
	_, err = dRow.IterSchema(te.tSch, func(tag uint64, val types.Value) (stop bool, err error) {
		if val == nil {
			tagToVal[tag] = types.NullValue
		} else {
			tagToVal[tag] = val
		}
		return false, nil
	})
	if err != nil {
		return err
	}
	return te.InsertKeyVal(ctx, key.(types.Tuple), val.(types.Tuple), tagToVal, errFunc)
}

// DeleteRow removes the given row from the table.
func (te *pkTableEditor) DeleteRow(ctx context.Context, dRow row.Row) (retErr error) {
	defer te.autoFlush()
	te.flushMutex.RLock()
	defer te.flushMutex.RUnlock()

	key, err := dRow.NomsMapKey(te.tSch).Value(ctx)
	if err != nil {
		return err
	}

	keyHash, err := key.Hash(te.nbf)
	if err != nil {
		return err
	}

	// Regarding the lock's position here, refer to the comment in InsertKeyVal
	te.writeMutex.Lock()
	defer te.writeMutex.Unlock()

	// Index operations should come before all table operations. For the reasoning, refer to the comment in InsertKeyVal
	indexOpsToUndo := make([]int, len(te.indexEds))
	defer func() {
		if retErr != nil {
			for i, opsToUndo := range indexOpsToUndo {
				for undone := 0; undone < opsToUndo; undone++ {
					te.indexEds[i].Undo(ctx)
				}
			}
		}
	}()

	for i, indexEd := range te.indexEds {
		fullKey, partialKey, _, err := dRow.ReduceToIndexKeys(indexEd.Index())
		if err != nil {
			return err
		}
		err = indexEd.DeleteRow(ctx, fullKey, partialKey)
		if err != nil {
			return err
		}
		indexOpsToUndo[i]++
	}

	delete(te.tea.addedKeys, keyHash)
	te.tea.removedKeys[keyHash] = key

	te.tea.opCount++
	return nil
}

// UpdateRow takes the current row and new rows, and updates it accordingly.
func (te *pkTableEditor) UpdateRow(ctx context.Context, dOldRow row.Row, dNewRow row.Row, errFunc PKDuplicateErrFunc) (retErr error) {
	defer te.autoFlush()
	te.flushMutex.RLock()
	defer te.flushMutex.RUnlock()

	dOldKeyVal, err := dOldRow.NomsMapKey(te.tSch).Value(ctx)
	if err != nil {
		return err
	}

	dNewKeyVal, err := dNewRow.NomsMapKey(te.tSch).Value(ctx)
	if err != nil {
		return err
	}
	dNewRowVal, err := dNewRow.NomsMapValue(te.tSch).Value(ctx)
	if err != nil {
		return err
	}

	newHash, err := dNewKeyVal.Hash(dNewRow.Format())
	if err != nil {
		return err
	}
	oldHash, err := dOldKeyVal.Hash(dOldRow.Format())
	if err != nil {
		return err
	}

	// Regarding the lock's position here, refer to the comment in InsertKeyVal
	te.writeMutex.Lock()
	defer te.writeMutex.Unlock()

	// Index operations should come before all table operations. For the reasoning, refer to the comment in InsertKeyVal
	indexOpsToUndo := make([]int, len(te.indexEds))
	defer func() {
		if retErr != nil {
			for i, opsToUndo := range indexOpsToUndo {
				for undone := 0; undone < opsToUndo; undone++ {
					te.indexEds[i].Undo(ctx)
				}
			}
		}
	}()

	for i, indexEd := range te.indexEds {
		oldFullKey, oldPartialKey, _, err := dOldRow.ReduceToIndexKeys(indexEd.Index())
		if err != nil {
			return err
		}
		err = indexEd.DeleteRow(ctx, oldFullKey, oldPartialKey)
		if err != nil {
			return err
		}
		indexOpsToUndo[i]++
		newFullKey, newPartialKey, newVal, err := dNewRow.ReduceToIndexKeys(indexEd.Index())
		if err != nil {
			return err
		}
		err = indexEd.InsertRow(ctx, newFullKey, newPartialKey, newVal)
		if uke, ok := err.(*uniqueKeyErr); ok {
			tableTupleHash, err := uke.TableTuple.Hash(uke.TableTuple.Format())
			if err != nil {
				return err
			}
			kvp, pkExists, err := te.tea.maybeGet(ctx, tableTupleHash, uke.TableTuple)
			if err != nil {
				return err
			}
			if !pkExists {
				keyStr, _ := formatKey(ctx, uke.TableTuple)
				return fmt.Errorf("UNIQUE constraint violation on index '%s', but could not find row with primary key: %s",
					indexEd.Index().Name(), keyStr)
			}
			return te.keyErrForKVP(ctx, kvp, false, errFunc)
		} else if err != nil {
			return err
		}
		indexOpsToUndo[i]++
	}

	delete(te.tea.addedKeys, oldHash)
	te.tea.removedKeys[oldHash] = dOldKeyVal

	if kvp, pkExists, err := te.tea.maybeGet(ctx, newHash, dNewKeyVal); err != nil {
		return err
	} else if pkExists {
		return te.keyErrForKVP(ctx, kvp, true, errFunc)
	}

	delete(te.tea.removedKeys, newHash)
	te.tea.addedKeys[newHash] = &doltKVP{k: dNewKeyVal, v: dNewRowVal}
	te.tea.opCount += 2
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
	if err != nil {
		return nil, err
	}

	te.flushMutex.Lock()
	defer te.flushMutex.Unlock()

	if te.hasAutoInc {
		te.t, err = te.t.SetAutoIncrementValue(te.autoIncVal)
		if err != nil {
			return nil, err
		}
	}

	tbl := te.t
	idxMutex := &sync.Mutex{}
	idxWg := &sync.WaitGroup{}
	idxWg.Add(len(te.indexEds))
	for i := 0; i < len(te.indexEds); i++ {
		go func(i int) {
			defer idxWg.Done()
			indexMap, idxErr := te.indexEds[i].Map(ctx)
			idxMutex.Lock()
			defer idxMutex.Unlock()
			if err != nil {
				return
			}
			if idxErr != nil {
				err = idxErr
				return
			}
			tbl, idxErr = tbl.SetIndexRowData(ctx, te.indexEds[i].Index().Name(), indexMap)
			if idxErr != nil {
				err = idxErr
				return
			}
		}(i)
	}
	idxWg.Wait()
	if err != nil {
		return nil, err
	}
	te.t = tbl

	return te.t, nil
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

// StatementStarted implements TableEditor.
func (te *pkTableEditor) StatementStarted(ctx context.Context) {
	te.flushMutex.Lock()
	defer te.flushMutex.Unlock()
	te.savedTea = te.tea
	te.tea = te.tea.NewFromCurrent()
	te.tea.opCount = te.savedTea.opCount
	for i := 0; i < len(te.indexEds); i++ {
		te.indexEds[i].StatementStarted(ctx)
	}
}

// StatementFinished implements TableEditor.
func (te *pkTableEditor) StatementFinished(ctx context.Context, errored bool) error {
	// If any teas are flushing then we want them to finish first
	err := te.aq.WaitForEmpty()
	if err != nil {
		return err
	}
	te.flushMutex.Lock()
	defer te.flushMutex.Unlock()

	if !errored {
		// We collapse the changes in this tea to the last to reduce the number of map editors that will need to be opened
		if te.tea.prevTea != nil {
			targetTea := te.tea.prevTea

			for keyHash, key := range te.tea.removedKeys {
				delete(targetTea.addedKeys, keyHash)
				targetTea.removedKeys[keyHash] = key
			}
			for keyHash, kvp := range te.tea.addedKeys {
				delete(targetTea.removedKeys, keyHash)
				targetTea.addedKeys[keyHash] = kvp
			}

			targetTea.opCount = te.tea.opCount
			te.tea.prevTea = nil
			te.tea.rowData = types.EmptyMap
			te.tea.addedKeys = nil
			te.tea.removedKeys = nil
			te.tea = targetTea
		}
	} else {
		currentTea := te.tea
		// Loop and remove all newer teas
		for {
			if currentTea == nil || currentTea == te.savedTea {
				break
			}
			nextTea := currentTea.prevTea
			// We're essentially deleting currentTea, so we're closing and removing everything.
			// Some of this is taken from the steps followed when flushing, such as the map nils.
			currentTea.prevTea = nil
			if currentTea.opCount != -1 {
				currentTea.rowData = types.EmptyMap
				currentTea.addedKeys = nil
				currentTea.removedKeys = nil
			}
			currentTea = nextTea
		}
		// If the savedTea was processed due to a large number of ops in the statement triggering an auto flush, then we
		// need to create a new one.
		if te.savedTea.opCount == -1 {
			te.tea = createInitialTableEditAcc(te.savedTea.nbf, te.savedTea.rowData)
		} else {
			te.tea = te.savedTea
		}
	}

	for i := 0; i < len(te.indexEds); i++ {
		iErr := te.indexEds[i].StatementFinished(ctx, errored)
		if iErr != nil && err == nil {
			err = iErr
		}
	}
	if err != nil {
		return err
	}
	te.savedTea = nil
	return nil
}

// Close ensures that all goroutines that may be open are properly disposed of. Attempting to call any other function
// on this editor after calling this function is undefined behavior.
func (te *pkTableEditor) Close() error {
	for _, indexEd := range te.indexEds {
		err := indexEd.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// Flush finalizes all of the changes made so far.
func (te *pkTableEditor) flush() {
	te.flushMutex.Lock()
	defer te.flushMutex.Unlock()

	if te.tea.opCount > 0 {
		newTea := te.tea.NewFromCurrent()
		te.aq.Execute(newTea)
		te.tea = newTea
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

func (te *pkTableEditor) flushEditAccumulator(ctx context.Context, teaInterface interface{}) (err error) {
	// We don't call any locks at the function entrance since this is called from an ActionExecutor with a concurrency of 1
	updatedMap, err := processEditAccumulatorChain(ctx, teaInterface.(*tableEditAccumulator), te.writeMutex)
	if err != nil {
		return err
	}
	newTable, err := te.t.UpdateRows(ctx, updatedMap)
	if err != nil {
		return err
	}
	te.t = newTable
	return nil
}

// processEditAccumulatorChain processes all previous edit accumulators for the one being flushed.
func processEditAccumulatorChain(ctx context.Context, futureTea *tableEditAccumulator, writeMutex *sync.Mutex) (m types.Map, err error) {
	if futureTea.prevTea == nil {
		return futureTea.rowData, nil
	}
	tea := futureTea.prevTea

	ed := types.CreateEditAccForMapEdits(tea.nbf)
	defer ed.Close()
	for _, key := range tea.removedKeys {
		ed.AddEdit(key, nil)
	}
	for _, kvp := range tea.addedKeys {
		ed.AddEdit(kvp.k, kvp.v)
	}

	// If we encounter an error and return, then we need to remove this tea from the chain and update the next's rowData
	encounteredErr := true
	defer func() {
		//TODO: need some way to reset an index editor to a previous point as well
		if encounteredErr {
			// As this is in a defer and we're attempting to capture all errors, that includes panics as well.
			// Naturally a panic doesn't set the err variable, so we have to recover it.
			if recoveredErr := recover(); recoveredErr != nil && err == nil {
				err = recoveredErr.(error)
			}
			// All tea modifications are guarded by writeMutex locks, so we have to acquire it
			writeMutex.Lock()
			futureTea.prevTea = nil
			futureTea.rowData = tea.rowData
			writeMutex.Unlock()
		}
	}()

	if tea.prevTea != nil {
		_, err = processEditAccumulatorChain(ctx, tea, writeMutex)
		if err != nil {
			return types.EmptyMap, err
		}
	}
	accEdits, err := ed.FinishedEditing()
	if err != nil {
		return types.EmptyMap, err
	}
	// We are guaranteed that rowData is valid, as we process teas sequentially.
	updatedMap, _, err := types.ApplyEdits(ctx, accEdits, tea.rowData)
	if err != nil {
		return types.EmptyMap, err
	}
	// No errors were encountered, so we set the bool to false. This should come after ALL calls that may error.
	encounteredErr = false

	// All tea modifications are guarded by writeMutex locks, so we have to acquire it here
	writeMutex.Lock()
	futureTea.prevTea = nil
	futureTea.rowData = updatedMap
	writeMutex.Unlock()
	// An opCount of -1 lets us know that this tea was processed
	tea.opCount = -1
	// not sure where it is, but setting these to nil fixes a memory leak
	tea.addedKeys = nil
	tea.removedKeys = nil
	return updatedMap, nil
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

	return fmt.Sprintf("[%s]", strings.Join(vals, ",")), nil
}
