// Copyright 2020-2021 Dolthub, Inc.
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
	"sync"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

const rebuildIndexFlushInterval = 1 << 25

var _ error = (*uniqueKeyErr)(nil)

// uniqueKeyErr is an error that is returned when a unique constraint has been violated. It contains the index key
// (which is the full row).
type uniqueKeyErr struct {
	TableTuple types.Tuple
	IndexTuple types.Tuple
	IndexName  string
}

// Error implements the error interface.
func (u *uniqueKeyErr) Error() string {
	keyStr, _ := formatKey(context.Background(), u.IndexTuple)
	return fmt.Sprintf("UNIQUE constraint violation on index '%s': %s", u.IndexName, keyStr)
}

// NOTE: Regarding partial keys and full keys. For this example, let's say that our table has a primary key W, with
// non-pk columns X, Y, and Z. You then declare an index over X and Y (in that order). In the table map containing all of
// the rows for the table, each row is composed of two tuples: the first tuple is called the key, the second tuple is
// called the value. The key is the entire primary key, which in this case is Tuple<W> (tags are ignored for this
// example). The value is the remaining three columns: Tuple<X,Y,Z>. Therefore, a row in the table map is
// Row(Tuple<W>,Tuple<X,Y,Z>).
//
// The index map containing all of the rows for the index also follows this format of key and value tuples. However,
// indexes store all of the columns in the key, and have an empty value tuple. An index key contains the indexed columns
// in the order they were defined, along with any primary keys that were not defined in the index. Thus, our example key
// looks like Tuple<X,Y,W>. We refer to this key as the full key in the index context, as with the full key you can
// construct an index row, as it's simply adding an empty tuple to the value, i.e. Row(Tuple<X,Y,W>,Tuple<>). Also with
// a full key, you can find the table row that matches this index row, as the entire primary key (just W) is in the full
// key.
//
// In both the table and index maps, keys are sorted. This means that given X and Y values for the index, we can
// construct a tuple with just those values, Tuple<X,Y>, and find all of the rows in the table with those two values by
// the appended primary key(s). We refer to this prefix of the full key as a partial key. It's easy to think of partial
// keys as just the indexed columns (Tuple<X,Y>), and the full key as the partial key along with the referring primary
// key (Tuple<X,Y> + W = Tuple<X,Y,W>).

// IndexEditor takes in changes to an index map and returns the updated map if changes have been made.
// This type is thread-safe, and may be used in a multi-threaded environment.
type IndexEditor struct {
	nbf *types.NomsBinFormat

	idxSch       schema.Schema
	tblSch       schema.Schema
	idx          schema.Index
	iea          IndexEditAccumulator
	stack        indexOperationStack
	permanentErr error // If this is set then we should always return this error as the IndexEditor is no longer usable

	// This mutex blocks on each operation, so that map reads and updates are serialized
	writeMutex *sync.Mutex
}

// NewIndexEditor creates a new index editor
func NewIndexEditor(ctx context.Context, index schema.Index, indexData types.Map, tableSch schema.Schema, opts Options) *IndexEditor {
	ie := &IndexEditor{
		idxSch:       index.Schema(),
		tblSch:       tableSch,
		idx:          index,
		iea:          opts.Deaf.NewIndexEA(ctx, indexData),
		nbf:          indexData.Format(),
		permanentErr: nil,
		writeMutex:   &sync.Mutex{},
	}
	return ie
}

// InsertRow adds the given row to the index. If the row already exists and the index is unique, then an error is returned.
// Otherwise, it is a no-op.
func (ie *IndexEditor) InsertRow(ctx context.Context, key, partialKey types.Tuple, value types.Tuple) error {
	keyHash, err := key.Hash(key.Format())
	if err != nil {
		return err
	}
	partialKeyHash, err := partialKey.Hash(partialKey.Format())
	if err != nil {
		return err
	}

	ie.writeMutex.Lock()
	defer ie.writeMutex.Unlock()

	if ie.permanentErr != nil {
		return ie.permanentErr
	}

	if ie.idx.IsUnique() {
		if matches, err := ie.iea.HasPartial(ctx, ie.idxSch, partialKeyHash, partialKey); err != nil {
			return err
		} else if len(matches) > 0 {
			tableTuple, err := ie.idx.ToTableTuple(ctx, matches[0].key, ie.nbf)
			if err != nil {
				return err
			}
			// For a UNIQUE key violation, there should only be 1 at max. We still do an "over 0" check for safety though.
			return &uniqueKeyErr{tableTuple, matches[0].key, ie.idx.Name()}
		}
	} else {
		if rowExists, err := ie.iea.Has(ctx, keyHash, key); err != nil {
			return err
		} else if rowExists && value.Empty() {
			ie.stack.Push(true, types.EmptyTuple(key.Format()), types.EmptyTuple(key.Format()), types.EmptyTuple(value.Format()))
			return nil
		}
	}

	err = ie.iea.Insert(ctx, keyHash, partialKeyHash, key, value)
	if err != nil {
		return err
	}

	ie.stack.Push(true, key, partialKey, value)
	return nil
}

// DeleteRow removes the given row from the index.
func (ie *IndexEditor) DeleteRow(ctx context.Context, key, partialKey, value types.Tuple) error {
	keyHash, err := key.Hash(ie.nbf)
	if err != nil {
		return err
	}
	partialKeyHash, err := partialKey.Hash(partialKey.Format())
	if err != nil {
		return err
	}

	ie.writeMutex.Lock()
	defer ie.writeMutex.Unlock()

	if ie.permanentErr != nil {
		return ie.permanentErr
	}

	err = ie.iea.Delete(ctx, keyHash, partialKeyHash, key, value)
	if err != nil {
		return err
	}

	ie.stack.Push(false, key, partialKey, value)
	return nil
}

// HasPartial returns whether the index editor has the given partial key.
func (ie *IndexEditor) HasPartial(ctx context.Context, partialKey types.Tuple) (bool, error) {
	partialKeyHash, err := partialKey.Hash(partialKey.Format())
	if err != nil {
		return false, err
	}

	ie.writeMutex.Lock()
	defer ie.writeMutex.Unlock()

	if ie.permanentErr != nil {
		return false, ie.permanentErr
	}

	tpls, err := ie.iea.HasPartial(ctx, ie.idxSch, partialKeyHash, partialKey)
	if err != nil {
		return false, err
	}
	return len(tpls) > 0, nil
}

// Undo will cause the index editor to undo the last operation at the top of the stack. As Insert and Delete are called,
// they are added onto a limited-size stack, and Undo pops an operation off the top and undoes it. So if there was an
// Insert on a key, it will use Delete on that same key. The stack size is very small, therefore too many consecutive
// calls will cause the stack to empty. This should only be called in the event that an operation was performed that
// has failed for other reasons, such as an INSERT on the parent table failing on a separate index editor. In the event
// that Undo is called and there are no operations to undo OR the reverse operation fails (such as memory capacity
// reached), then we set a permanent error as the index editor is in an invalid state that cannot be corrected.
//
// We don't return an error here as Undo will only be called when there's an error in a different editor. We allow the
// user to handle that initial error, as ALL further calls to this IndexEditor will return the error set here.
func (ie *IndexEditor) Undo(ctx context.Context) {
	if ie.permanentErr != nil {
		return
	}

	indexOp, ok := ie.stack.Pop()
	if !ok {
		panic(fmt.Sprintf("attempted to undo the last operation on index '%s' but failed due to an empty stack", ie.idx.Name()))
	}
	// If an operation succeeds and does not do anything, then an empty tuple is pushed onto the stack.
	if indexOp.fullKey.Empty() {
		return
	}

	if indexOp.isInsert {
		err := ie.DeleteRow(ctx, indexOp.fullKey, indexOp.partialKey, indexOp.value)
		if err != nil {
			ie.permanentErr = fmt.Errorf("index '%s' is in an invalid and unrecoverable state: "+
				"attempted to undo previous insertion but encountered the following error: %v",
				ie.idx.Name(), err)
			return
		}
	} else {
		err := ie.InsertRow(ctx, indexOp.fullKey, indexOp.partialKey, indexOp.value)
		if err != nil {
			ie.permanentErr = fmt.Errorf("index '%s' is in an invalid and unrecoverable state: "+
				"attempted to undo previous deletion but encountered the following error: %v",
				ie.idx.Name(), err)
			return
		}
	}
}

// Map returns a map based on the edits given, if any.
func (ie *IndexEditor) Map(ctx context.Context) (types.Map, error) {
	ie.writeMutex.Lock()
	defer ie.writeMutex.Unlock()

	if ie.permanentErr != nil {
		return types.EmptyMap, ie.permanentErr
	}

	return ie.iea.MaterializeEdits(ctx, ie.nbf)
}

// Index returns this editor's index.
func (ie *IndexEditor) Index() schema.Index {
	return ie.idx
}

// StatementStarted is analogous to the TableEditor implementation, but specific to the IndexEditor.
func (ie *IndexEditor) StatementStarted(ctx context.Context) {
}

// StatementFinished is analogous to the TableEditor implementation, but specific to the IndexEditor.
func (ie *IndexEditor) StatementFinished(ctx context.Context, errored bool) error {
	ie.writeMutex.Lock()
	defer ie.writeMutex.Unlock()

	if ie.permanentErr != nil {
		return ie.permanentErr
	} else if errored {
		return ie.iea.Rollback(ctx)
	}

	return ie.iea.Commit(ctx, ie.nbf)
}

// Close is a no-op for an IndexEditor.
func (ie *IndexEditor) Close() error {
	return ie.permanentErr
}

func RebuildIndex(ctx context.Context, tbl *doltdb.Table, indexName string, opts Options) (types.Map, error) {
	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return types.EmptyMap, err
	}

	tableRowData, err := tbl.GetNomsRowData(ctx)
	if err != nil {
		return types.EmptyMap, err
	}

	index := sch.Indexes().GetByName(indexName)
	if index == nil {
		return types.EmptyMap, fmt.Errorf("index `%s` does not exist", indexName)
	}

	tf := tupleFactories.Get().(*types.TupleFactory)
	tf.Reset(tbl.Format())
	defer tupleFactories.Put(tf)

	opts = opts.WithDeaf(NewBulkImportTEAFactory(tbl.Format(), tbl.ValueReadWriter(), opts.Tempdir))
	rebuiltIndexData, err := rebuildIndexRowData(ctx, tbl.ValueReadWriter(), sch, tableRowData, index, opts, tf)
	if err != nil {
		return types.EmptyMap, err
	}
	return rebuiltIndexData, nil
}

func RebuildAllIndexes(ctx context.Context, t *doltdb.Table, opts Options) (*doltdb.Table, error) {
	sch, err := t.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	if sch.Indexes().Count() == 0 {
		return t, nil
	}

	tableRowData, err := t.GetNomsRowData(ctx)
	if err != nil {
		return nil, err
	}

	indexes, err := t.GetIndexSet(ctx)
	if err != nil {
		return nil, err
	}

	tf := tupleFactories.Get().(*types.TupleFactory)
	tf.Reset(t.Format())
	defer tupleFactories.Put(tf)

	opts = opts.WithDeaf(NewBulkImportTEAFactory(t.Format(), t.ValueReadWriter(), opts.Tempdir))
	for _, index := range sch.Indexes().AllIndexes() {
		rebuiltIndexRowData, err := rebuildIndexRowData(ctx, t.ValueReadWriter(), sch, tableRowData, index, opts, tf)
		if err != nil {
			return nil, err
		}

		indexes, err = indexes.PutNomsIndex(ctx, index.Name(), rebuiltIndexRowData)
		if err != nil {
			return nil, err
		}
	}

	return t.SetIndexSet(ctx, indexes)
}

func rebuildIndexRowData(ctx context.Context, vrw types.ValueReadWriter, sch schema.Schema, tblRowData types.Map, index schema.Index, opts Options, tf *types.TupleFactory) (types.Map, error) {
	emptyIndexMap, err := types.NewMap(ctx, vrw)
	if err != nil {
		return types.EmptyMap, err
	}

	var rowNumber int64
	indexEditor := NewIndexEditor(ctx, index, emptyIndexMap, sch, opts)
	err = tblRowData.IterAll(ctx, func(key, value types.Value) error {
		dRow, err := row.FromNoms(sch, key.(types.Tuple), value.(types.Tuple))
		if err != nil {
			return err
		}

		fullKey, partialKey, keyVal, err := dRow.ReduceToIndexKeys(index, tf)
		if err != nil {
			return err
		}

		err = indexEditor.InsertRow(ctx, fullKey, partialKey, keyVal)
		if err != nil {
			return err
		}

		rowNumber++
		if rowNumber%rebuildIndexFlushInterval == 0 {
			rebuiltIndexMap, err := indexEditor.Map(ctx)
			if err != nil {
				return err
			}

			indexEditor = NewIndexEditor(ctx, index, rebuiltIndexMap, sch, opts)
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
