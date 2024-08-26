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
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/dconfig"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

const tfApproxCapacity = 64

var tupleFactories = &sync.Pool{New: func() interface{} {
	return types.NewTupleFactory(tfApproxCapacity)
}}

var (
	tableEditorMaxOps uint64 = 256 * 1024
)

func init() {
	if maxOpsEnv := os.Getenv(dconfig.EnvEditTableBufferRows); maxOpsEnv != "" {
		if v, err := strconv.ParseUint(maxOpsEnv, 10, 63); err == nil {
			tableEditorMaxOps = v
		}
	}
}

type PKDuplicateCb func(newKeyString, indexName string, existingKey, existingVal types.Tuple, isPk bool) error

type TableEditor interface {
	InsertKeyVal(ctx context.Context, key, val types.Tuple, tagToVal map[uint64]types.Value, cb PKDuplicateCb) error
	DeleteByKey(ctx context.Context, key types.Tuple, tagToVal map[uint64]types.Value) error

	InsertRow(ctx context.Context, r row.Row, cb PKDuplicateCb) error
	UpdateRow(ctx context.Context, old, new row.Row, cb PKDuplicateCb) error
	DeleteRow(ctx context.Context, r row.Row) error

	// GetIndexedRows returns all matching rows for the given key on the index. The key is assumed to be in the format
	// expected of the index, similar to searching on the index map itself.
	GetIndexedRows(ctx *sql.Context, key types.Tuple, indexName string, idxSch schema.Schema) ([]row.Row, error)
	HasEdits() bool
	MarkDirty()

	SetConstraintViolation(ctx context.Context, k types.LesserValuable, v types.Valuable) error

	Table(ctx *sql.Context) (*doltdb.Table, error)
	Schema() schema.Schema
	Name() string
	Format() *types.NomsBinFormat
	ValueReadWriter() types.ValueReadWriter

	StatementStarted(ctx context.Context)
	StatementFinished(ctx context.Context, errored bool) error

	Close(ctx context.Context) error
}

func NewTableEditor(ctx context.Context, t *doltdb.Table, tableSch schema.Schema, name string, opts Options) (TableEditor, error) {
	if schema.IsKeyless(tableSch) {
		return newKeylessTableEditor(ctx, t, tableSch, name, opts)
	}
	return newPkTableEditor(ctx, t, tableSch, name, opts)
}

// Options are properties that define different functionality for the tableEditSession.
type Options struct {
	//TODO: look into this
	ForeignKeyChecksDisabled bool // If true, then ALL foreign key checks AND updates (through CASCADE, etc.) are skipped
	Deaf                     DbEaFactory
	Tempdir                  string
	
	// TargetStaging is true if the table is being edited in the staging root, as opposed to the working root (rare).
	TargetStaging bool
}

// WithDeaf returns a new Options with the given  edit accumulator factory class
func (o Options) WithDeaf(deaf DbEaFactory) Options {
	o.Deaf = deaf
	return o
}

func TestEditorOptions(vrw types.ValueReadWriter) Options {
	return Options{
		ForeignKeyChecksDisabled: false,
		Deaf:                     NewInMemDeaf(vrw),
	}
}

// pkTableEditor supports making multiple row edits (inserts, updates, deletes) to a table. It does error checking for key
// collision etc. in the Close() method, as well as during Insert / Update.
//
// This type is thread-safe, and may be used in a multi-threaded environment.
type pkTableEditor struct {
	t    *doltdb.Table
	tSch schema.Schema
	name string

	opts     Options
	tea      TableEditAccumulator
	nbf      *types.NomsBinFormat
	indexEds []*IndexEditor

	// TupleFactory is not thread-safe.  writeMutex needs to be acquired before using tf
	tf *types.TupleFactory

	// Whenever any write operation occurs on the table editor, this is set to true for the lifetime of the editor.
	dirty uint32

	// This mutex blocks on each operation, so that map reads and updates are serialized. Protects the above data.
	writeMutex *sync.Mutex

	// protects cvEditor
	cvMutex  *sync.Mutex
	cvEditor *types.MapEditor
}

var _ TableEditor = (*pkTableEditor)(nil)

func newPkTableEditor(ctx context.Context, t *doltdb.Table, tableSch schema.Schema, name string, opts Options) (*pkTableEditor, error) {
	tf := tupleFactories.Get().(*types.TupleFactory)
	tf.Reset(t.Format())

	te := &pkTableEditor{
		t:          t,
		tSch:       tableSch,
		name:       name,
		opts:       opts,
		nbf:        t.Format(),
		indexEds:   make([]*IndexEditor, tableSch.Indexes().Count()),
		tf:         tf,
		writeMutex: &sync.Mutex{},
		cvMutex:    &sync.Mutex{},
	}
	var err error
	rowData, err := t.GetNomsRowData(ctx)
	if err != nil {
		return nil, err
	}
	te.tea = opts.Deaf.NewTableEA(ctx, rowData)

	for i, index := range tableSch.Indexes().AllIndexes() {
		indexData, err := t.GetNomsIndexRowData(ctx, index.Name())
		if err != nil {
			return nil, err
		}
		te.indexEds[i] = NewIndexEditor(ctx, index, indexData, tableSch, opts)
	}

	return te, nil
}

func indexKeyToTableKey(nbf *types.NomsBinFormat, indexKey types.Tuple, lookupTags map[uint64]int) (types.Tuple, error) {
	tplItr, err := indexKey.Iterator()

	if err != nil {
		return types.Tuple{}, err
	}

	resVals := make([]types.Value, len(lookupTags)*2)
	for {
		_, tag, err := tplItr.NextUint64()

		if err != nil {
			if err == io.EOF {
				break
			}

			return types.Tuple{}, err
		}

		idx, inKey := lookupTags[tag]

		if inKey {
			_, valVal, err := tplItr.Next()

			if err != nil {
				return types.Tuple{}, err
			}

			resVals[idx*2] = types.Uint(tag)
			resVals[idx*2+1] = valVal
		} else {
			err := tplItr.Skip()

			if err != nil {
				return types.Tuple{}, err
			}
		}
	}

	return types.NewTuple(nbf, resVals...)
}

// GetIndexedRows returns all matching rows for the given key on the index. The key is assumed to be in the format
// expected of the index, similar to searching on the index map itself.
func (te *pkTableEditor) GetIndexedRows(ctx *sql.Context, key types.Tuple, indexName string, idxSch schema.Schema) ([]row.Row, error) {
	for _, indexEd := range te.indexEds {
		if indexEd.idx.Name() == indexName {
			keyHash, err := key.Hash(key.Format())
			if err != nil {
				return nil, err
			}
			idxKeys, err := indexEd.iea.HasPartial(ctx, idxSch, keyHash, key)
			if err != nil {
				return nil, err
			}
			if len(idxKeys) == 0 {
				return nil, nil
			}

			lookupTags := make(map[uint64]int)
			for i, tag := range te.Schema().GetPKCols().Tags {
				lookupTags[tag] = i
			}
			rows := make([]row.Row, len(idxKeys))

			for i, idxKey := range idxKeys {
				pkKey, err := indexKeyToTableKey(te.Format(), idxKey.key, lookupTags)
				if err != nil {
					return nil, err
				}
				pkKeyHash, err := pkKey.Hash(pkKey.Format())
				if err != nil {
					return nil, err
				}
				kvp, ok, err := te.tea.Get(ctx, pkKeyHash, pkKey)
				if err != nil {
					return nil, err
				}
				if !ok {
					keyStr, _ := formatKey(ctx, pkKey)
					return nil, fmt.Errorf("index `%s` has reference to primary key when parent table `%s` does not: %s",
						indexEd.idx.Name(), te.name, keyStr)
				}

				rows[i], err = row.FromNoms(te.Schema(), kvp.k, kvp.v)
				if err != nil {
					return nil, err
				}
			}

			return rows, nil
		}
	}

	// If there are no index entries then we should check if this is actually the primary key. Indexes store all rows in
	// the primary key, so this should only be true if the index is either the primary key or a prefix. The calling
	// function should guarantee that the key is a prefix, so we do not reorder it here.
	if idxSch.GetPKCols().Size() <= te.tSch.GetPKCols().Size() {
		for _, idxCol := range idxSch.GetPKCols().GetColumns() {
			if _, ok := te.tSch.GetPKCols().TagToCol[idxCol.Tag]; !ok {
				keyStr, _ := formatKey(ctx, key)
				return nil, fmt.Errorf("requested the following values from index `%s` on table `%s`: %s",
					indexName, te.name, keyStr)
			}
		}

		keyHash, err := key.Hash(key.Format())
		if err != nil {
			return nil, err
		}

		pkKeys, err := te.tea.HasPartial(ctx, te.tSch, keyHash, key)
		if err != nil {
			return nil, err
		}
		if len(pkKeys) == 0 {
			return nil, nil
		}

		rows := make([]row.Row, len(pkKeys))
		for i, pkKey := range pkKeys {
			pkKeyHash, err := pkKey.key.Hash(pkKey.key.Format())
			if err != nil {
				return nil, err
			}
			kvp, ok, err := te.tea.Get(ctx, pkKeyHash, pkKey.key)
			if err != nil {
				return nil, err
			}
			if !ok {
				return nil, nil
			}
			dRow, err := row.FromNoms(te.Schema(), kvp.k, kvp.v)
			if err != nil {
				return nil, err
			}
			rows[i] = dRow
		}

		return rows, nil
	}

	return nil, fmt.Errorf("an index editor for `%s` could not be found on table `%s`", indexName, te.name)
}

func (te *pkTableEditor) maybeReturnErrForKVP(ctx context.Context, newKeyString, indexName string, kvp *doltKVP, isPk bool, cb PKDuplicateCb) error {
	kVal, err := kvp.k.Value(ctx)
	if err != nil {
		return err
	}

	vVal, err := kvp.v.Value(ctx)
	if err != nil {
		return err
	}

	return cb(newKeyString, indexName, kVal.(types.Tuple), vVal.(types.Tuple), isPk)
}

// InsertKeyVal adds the given tuples to the table.
func (te *pkTableEditor) InsertKeyVal(ctx context.Context, key, val types.Tuple, tagToVal map[uint64]types.Value, cb PKDuplicateCb) (retErr error) {
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

	return te.insertKeyVal(ctx, keyHash, key, val, tagToVal, cb)
}

// InsertKeyVal adds the given tuples to the table.
func (te *pkTableEditor) insertKeyVal(ctx context.Context, keyHash hash.Hash, key, val types.Tuple, tagToVal map[uint64]types.Value, cb PKDuplicateCb) (retErr error) {
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
		fullKey, partialKey, err := row.ReduceToIndexKeysFromTagMap(te.nbf, indexEd.Index(), tagToVal, te.tf)
		if err != nil {
			return err
		}
		err = indexEd.InsertRowWithDupCb(ctx, fullKey, partialKey, types.EmptyTuple(te.nbf), func(ctx context.Context, uke *uniqueKeyErr) error {
			tableTupleHash, err := uke.TableTuple.Hash(uke.TableTuple.Format())
			if err != nil {
				return err
			}
			kvp, pkExists, err := te.tea.Get(ctx, tableTupleHash, uke.TableTuple)
			if err != nil {
				return err
			}
			if !pkExists {
				keyStr, _ := formatKey(ctx, uke.TableTuple)
				return fmt.Errorf("UNIQUE constraint violation on index '%s', but could not find row with primary key: %s",
					indexEd.Index().Name(), keyStr)
			}
			newKeyString, err := formatKey(ctx, uke.IndexTuple)
			if err != nil {
				return err
			}
			err = te.maybeReturnErrForKVP(ctx, newKeyString, indexEd.Index().Name(), kvp, false, cb)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}
		indexOpsToUndo[i]++
	}

	if kvp, pkExists, err := te.tea.Get(ctx, keyHash, key); err != nil {
		return err
	} else if pkExists {
		newKeyString, err := formatKey(ctx, key)
		if err != nil {
			return err
		}
		return te.maybeReturnErrForKVP(ctx, newKeyString, "PRIMARY KEY", kvp, true, cb)
	}

	err := te.tea.Insert(keyHash, key, val)
	if err != nil {
		return err
	}

	te.MarkDirty()
	return nil
}

// InsertRow adds the given row to the table. If the row already exists, use UpdateRow. This converts the given row into
// tuples that are then passed to InsertKeyVal.
func (te *pkTableEditor) InsertRow(ctx context.Context, dRow row.Row, cb PKDuplicateCb) error {
	tagToVal := make(map[uint64]types.Value)
	_, err := dRow.IterSchema(te.tSch, func(tag uint64, val types.Value) (stop bool, err error) {
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

	// Regarding the lock's position here, refer to the comment in InsertKeyVal
	te.writeMutex.Lock()
	defer te.writeMutex.Unlock()

	key, err := dRow.NomsMapKeyTuple(te.tSch, te.tf)
	if err != nil {
		return err
	}
	val, err := dRow.NomsMapValueTuple(te.tSch, te.tf)
	if err != nil {
		return err
	}

	keyHash, err := key.Hash(te.nbf)
	if err != nil {
		return err
	}

	return te.insertKeyVal(ctx, keyHash, key, val, tagToVal, cb)
}

func (te *pkTableEditor) DeleteByKey(ctx context.Context, key types.Tuple, tagToVal map[uint64]types.Value) (retErr error) {
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
		fullKey, partialKey, err := row.ReduceToIndexKeysFromTagMap(te.nbf, indexEd.Index(), tagToVal, te.tf)
		if err != nil {
			return err
		}
		err = indexEd.DeleteRow(ctx, fullKey, partialKey, types.EmptyTuple(te.nbf))
		if err != nil {
			return err
		}
		indexOpsToUndo[i]++
	}

	keyHash, err := key.Hash(te.nbf)
	if err != nil {
		return err
	}

	te.MarkDirty()
	return te.tea.Delete(keyHash, key)
}

// DeleteRow removes the given row from the table. This essentially acts as a convenience function for DeleteKey, while
// ensuring proper thread safety.
func (te *pkTableEditor) DeleteRow(ctx context.Context, dRow row.Row) (retErr error) {
	key, tv, err := row.KeyAndTaggedValuesForRow(dRow, te.tSch)
	if err != nil {
		return err
	}

	return te.DeleteByKey(ctx, key, tv)
}

// UpdateRow takes the current row and new rows, and updates it accordingly.
func (te *pkTableEditor) UpdateRow(ctx context.Context, dOldRow row.Row, dNewRow row.Row, errFunc PKDuplicateCb) (retErr error) {
	te.writeMutex.Lock()
	defer te.writeMutex.Unlock()

	dOldKeyVal, err := dOldRow.NomsMapKeyTuple(te.tSch, te.tf)
	if err != nil {
		return err
	}

	dNewKeyVal, err := dNewRow.NomsMapKeyTuple(te.tSch, te.tf)
	if err != nil {
		return err
	}

	dNewRowVal, err := dNewRow.NomsMapValueTuple(te.tSch, te.tf)
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
		oldFullKey, oldPartialKey, oldVal, err := dOldRow.ReduceToIndexKeys(indexEd.Index(), te.tf)
		if err != nil {
			return err
		}
		err = indexEd.DeleteRow(ctx, oldFullKey, oldPartialKey, oldVal)
		if err != nil {
			return err
		}
		indexOpsToUndo[i]++
		newFullKey, newPartialKey, newVal, err := dNewRow.ReduceToIndexKeys(indexEd.Index(), te.tf)
		if err != nil {
			return err
		}
		err = indexEd.InsertRowWithDupCb(ctx, newFullKey, newPartialKey, newVal, func(ctx context.Context, uke *uniqueKeyErr) error {
			tableTupleHash, err := uke.TableTuple.Hash(uke.TableTuple.Format())
			if err != nil {
				return err
			}
			kvp, pkExists, err := te.tea.Get(ctx, tableTupleHash, uke.TableTuple)
			if err != nil {
				return err
			}
			if !pkExists {
				keyStr, _ := formatKey(ctx, uke.TableTuple)
				return fmt.Errorf("UNIQUE constraint violation on index '%s', but could not find row with primary key: %s",
					indexEd.Index().Name(), keyStr)
			}
			newKeyString, err := formatKey(ctx, uke.IndexTuple)
			if err != nil {
				return err
			}
			return te.maybeReturnErrForKVP(ctx, newKeyString, indexEd.Index().Name(), kvp, false, errFunc)
		})
		if err != nil {
			return err
		}
		indexOpsToUndo[i]++
	}

	err = te.tea.Delete(oldHash, dOldKeyVal)
	if err != nil {
		return err
	}

	te.MarkDirty()

	if kvp, pkExists, err := te.tea.Get(ctx, newHash, dNewKeyVal); err != nil {
		return err
	} else if pkExists {
		str, err := formatKey(ctx, dNewKeyVal)
		if err != nil {
			return err
		}
		return te.maybeReturnErrForKVP(ctx, str, "PRIMARY KEY", kvp, true, errFunc)
	}

	return te.tea.Insert(newHash, dNewKeyVal, dNewRowVal)
}

// Table returns a Table based on the edits given, if any. If Flush() was not called prior, it will be called here.
func (te *pkTableEditor) Table(ctx *sql.Context) (*doltdb.Table, error) {
	if !te.HasEdits() {
		return te.t, nil
	}

	var tbl *doltdb.Table
	err := func() error {
		te.writeMutex.Lock()
		defer te.writeMutex.Unlock()

		updatedMap, err := te.tea.MaterializeEdits(ctx, te.nbf)
		if err != nil {
			return err
		}

		newTable, err := te.t.UpdateNomsRows(ctx, updatedMap)
		if err != nil {
			return err
		}

		te.t = newTable
		tbl = te.t
		return nil
	}()

	if err != nil {
		return nil, err
	}

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
			tbl, idxErr = tbl.SetNomsIndexRows(ctx, te.indexEds[i].Index().Name(), indexMap)
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

	if te.cvEditor != nil {
		cvMap, err := te.cvEditor.Map(ctx)
		if err != nil {
			return nil, err
		}
		te.cvEditor = nil
		tbl, err = tbl.SetConstraintViolations(ctx, cvMap)
		if err != nil {
			return nil, err
		}
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

func (te *pkTableEditor) ValueReadWriter() types.ValueReadWriter {
	return te.t.ValueReadWriter()
}

// StatementStarted implements TableEditor.
func (te *pkTableEditor) StatementStarted(ctx context.Context) {
	te.writeMutex.Lock()
	defer te.writeMutex.Unlock()
	for i := 0; i < len(te.indexEds); i++ {
		te.indexEds[i].StatementStarted(ctx)
	}
}

// StatementFinished implements TableEditor.
func (te *pkTableEditor) StatementFinished(ctx context.Context, errored bool) error {
	te.writeMutex.Lock()
	defer te.writeMutex.Unlock()

	var err error
	if errored {
		err = te.tea.Rollback(ctx)
	} else {
		err = te.tea.Commit(ctx, te.nbf)
	}

	for i := 0; i < len(te.indexEds); i++ {
		iErr := te.indexEds[i].StatementFinished(ctx, errored)
		if err == nil {
			err = iErr
		}
	}

	if err != nil {
		return err
	}

	return nil
}

// SetConstraintViolation implements TableEditor.
func (te *pkTableEditor) SetConstraintViolation(ctx context.Context, k types.LesserValuable, v types.Valuable) error {
	te.cvMutex.Lock()
	defer te.cvMutex.Unlock()

	if te.cvEditor == nil {
		cvMap, err := te.t.GetConstraintViolations(ctx)
		if err != nil {
			return err
		}
		te.cvEditor = cvMap.Edit()
	}
	te.cvEditor.Set(k, v)
	te.MarkDirty()
	return nil
}

// Close ensures that all goroutines that may be open are properly disposed of. Attempting to call any other function
// on this editor after calling this function is undefined behavior.
func (te *pkTableEditor) Close(ctx context.Context) error {
	te.writeMutex.Lock()
	defer te.writeMutex.Unlock()
	defer func() {
		tupleFactories.Put(te.tf)
		te.tf = nil
	}()

	if te.cvEditor != nil {
		te.cvEditor.Close(ctx)
		te.cvEditor = nil
	}

	for _, indexEd := range te.indexEds {
		err := indexEd.Close()
		if err != nil {
			return err
		}
	}

	return nil
}

// MarkDirty implements TableEditor.
func (te *pkTableEditor) MarkDirty() {
	dirty := uint32(1)
	atomic.StoreUint32(&te.dirty, dirty)
}

// HasEdits returns whether the table editor has had any successful write operations. This does not track whether the
// write operations were eventually rolled back (such as through an error on StatementFinished), so it is still possible
// for this to return true when the table editor does not actually contain any new edits. This is preferable to
// potentially returning false when there are edits.
func (te *pkTableEditor) HasEdits() bool {
	return atomic.LoadUint32(&te.dirty) != 0
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
