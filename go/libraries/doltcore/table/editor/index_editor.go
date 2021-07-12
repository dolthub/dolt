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
	"io"
	"sync"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/dolthub/dolt/go/libraries/utils/async"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

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
//
// This type is thread-safe, and may be used in a multi-threaded environment.
type IndexEditor struct {
	idxSch   schema.Schema
	tblSch   schema.Schema
	idx      schema.Index
	iea      *indexEditAccumulator
	savedIea *indexEditAccumulator
	aq       *async.ActionExecutor
	nbf      *types.NomsBinFormat
	idxData  types.Map
	stack    indexOperationStack

	// This mutex blocks on each operation, so that map reads and updates are serialized
	writeMutex *sync.Mutex
	// This mutex ensures that Flush is only called once all current write operations have completed
	flushMutex *sync.RWMutex
}

// uniqueKeyErr is an error that is returned when a unique constraint has been violated. It contains the index key
// (which is the full row).
type uniqueKeyErr struct {
	TableTuple types.Tuple
	IndexTuple types.Tuple
	IndexName  string
}

var _ error = (*uniqueKeyErr)(nil)

// indexEditAccumulator is the index equivalent of the tableEditAccumulator. It tracks all edits done, and allows for
// value checking that uses both existing data and new data.
type indexEditAccumulator struct {
	// This is the indexEditAccumulator that is currently processing on the background thread. Once that thread has
	// finished, it updates rowData and sets this to nil.
	prevIea *indexEditAccumulator

	// This is the map equivalent of the previous indexEditAccumulator, represented by prevIea. While the background
	// thread is processing prevIea, this will be an empty map. Once the thread has finished, it will update this map
	// to be equivalent in content to prevIea, and will set prevIea to nil.
	rowData types.Map

	nbf     *types.NomsBinFormat
	opCount int64

	// addedPartialKeys is a map of partial keys to a map of full keys that match the partial key
	addedPartialKeys map[hash.Hash]map[hash.Hash]types.Tuple
	addedKeys        map[hash.Hash]hashedTuple // These hashes represent the hash of the partial key, with the tuple being the full key
	removedKeys      map[hash.Hash]hashedTuple // These hashes represent the hash of the partial key, with the tuple being the full key
}

// hashedTuple is a tuple accompanied by a hash. The representing value of the hash is dependent on the function
// it is obtained from.
type hashedTuple struct {
	types.Tuple
	hash.Hash
}

// createInitialIndexEditAcc creates the initial indexEditAccumulator. All future ieas should use the method
// NewFromCurrent.
func createInitialIndexEditAcc(indexData types.Map) *indexEditAccumulator {
	return &indexEditAccumulator{
		prevIea:          nil,
		rowData:          indexData,
		nbf:              indexData.Format(),
		addedPartialKeys: make(map[hash.Hash]map[hash.Hash]types.Tuple),
		addedKeys:        make(map[hash.Hash]hashedTuple),
		removedKeys:      make(map[hash.Hash]hashedTuple),
	}
}

// NewFromCurrent returns a new indexEditAccumulator that references the current indexEditAccumulator.
func (iea *indexEditAccumulator) NewFromCurrent() *indexEditAccumulator {
	return &indexEditAccumulator{
		prevIea:          iea,
		rowData:          types.EmptyMap,
		nbf:              iea.nbf,
		addedPartialKeys: make(map[hash.Hash]map[hash.Hash]types.Tuple),
		addedKeys:        make(map[hash.Hash]hashedTuple),
		removedKeys:      make(map[hash.Hash]hashedTuple),
	}
}

// Has returns whether the current indexEditAccumulator contains the given key. This assumes that the given hash is for
// the given key.
func (iea *indexEditAccumulator) Has(ctx context.Context, keyHash hash.Hash, key types.Value) (bool, error) {
	// No locks as all calls and modifications to iea are done from a lock that the caller handles
	if _, ok := iea.addedKeys[keyHash]; ok {
		return true, nil
	}
	if _, ok := iea.removedKeys[keyHash]; !ok {
		// When rowData is updated, prevIea is set to nil. Therefore, if prevIea is non-nil, we use it.
		if iea.prevIea != nil {
			valExists, err := iea.prevIea.Has(ctx, keyHash, key)
			if err != nil {
				return false, err
			}
			return valExists, nil
		} else {
			valExists, err := iea.rowData.Has(ctx, key)
			if err != nil {
				return false, err
			}
			return valExists, nil
		}
	}
	return false, nil
}

// HasPartial returns whether the current indexEditAccumulator contains the given partial key. This assumes that the
// given hash is for the given key. The hashes returned represent the hash of the returned tuple.
func (iea *indexEditAccumulator) HasPartial(
	ctx context.Context,
	idxSch schema.Schema,
	partialKeyHash hash.Hash,
	partialKey types.Tuple,
) ([]hashedTuple, error) {
	if hasNulls, err := partialKey.Contains(types.NullValue); err != nil {
		return nil, err
	} else if hasNulls { // rows with NULL are considered distinct, and therefore we do not match on them
		return nil, nil
	}

	var matches []hashedTuple
	var err error
	if iea.prevIea != nil {
		matches, err = iea.prevIea.HasPartial(ctx, idxSch, partialKeyHash, partialKey)
		if err != nil {
			return nil, err
		}
	} else {
		var mapIter table.TableReadCloser = noms.NewNomsRangeReader(idxSch, iea.rowData,
			[]*noms.ReadRange{{Start: partialKey, Inclusive: true, Reverse: false, Check: func(tuple types.Tuple) (bool, error) {
				return tuple.StartsWith(partialKey), nil
			}}})
		defer mapIter.Close(ctx)
		var r row.Row
		for r, err = mapIter.ReadRow(ctx); err == nil; r, err = mapIter.ReadRow(ctx) {
			tplVal, err := r.NomsMapKey(idxSch).Value(ctx)
			if err != nil {
				return nil, err
			}
			tpl := tplVal.(types.Tuple)
			tplHash, err := tpl.Hash(tpl.Format())
			if err != nil {
				return nil, err
			}
			matches = append(matches, hashedTuple{tpl, tplHash})
		}
		if err != io.EOF {
			return nil, err
		}
	}

	for i := len(matches) - 1; i >= 0; i-- {
		// If we've removed a key that's present here, remove it from the slice
		if _, ok := iea.removedKeys[matches[i].Hash]; ok {
			matches[i] = matches[len(matches)-1]
			matches = matches[:len(matches)-1]
		}
	}
	for addedHash, addedTpl := range iea.addedPartialKeys[partialKeyHash] {
		matches = append(matches, hashedTuple{addedTpl, addedHash})
	}
	return matches, nil
}

// NewIndexEditor returns a new *IndexEditor.
func NewIndexEditor(ctx context.Context, index schema.Index, indexData types.Map, tableSch schema.Schema) *IndexEditor {
	ie := &IndexEditor{
		idxSch:     index.Schema(),
		tblSch:     tableSch,
		idx:        index,
		iea:        createInitialIndexEditAcc(indexData),
		nbf:        indexData.Format(),
		idxData:    indexData,
		writeMutex: &sync.Mutex{},
		flushMutex: &sync.RWMutex{},
	}
	ie.aq = async.NewActionExecutor(ctx, ie.flushEditAccumulator, 1, 1)
	return ie
}

// InsertRow adds the given row to the index. If the row already exists and the index is unique, then an error is returned.
// Otherwise, it is a no-op.
func (ie *IndexEditor) InsertRow(ctx context.Context, key, partialKey types.Tuple) error {
	defer ie.autoFlush()
	ie.flushMutex.RLock()
	defer ie.flushMutex.RUnlock()

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

	if ie.idx.IsUnique() {
		if matches, err := ie.iea.HasPartial(ctx, ie.idxSch, partialKeyHash, partialKey); err != nil {
			return err
		} else if len(matches) > 0 {
			tableTuple, err := ie.idx.ToTableTuple(ctx, matches[0].Tuple, ie.nbf)
			if err != nil {
				return err
			}
			// For a UNIQUE key violation, there should only be 1 at max. We still do an "over 0" check for safety though.
			return &uniqueKeyErr{tableTuple, matches[0].Tuple, ie.idx.Name()}
		}
	} else {
		if rowExists, err := ie.iea.Has(ctx, keyHash, key); err != nil {
			return err
		} else if rowExists {
			ie.stack.Push(true, types.EmptyTuple(key.Format()), types.EmptyTuple(key.Format()))
			return nil
		}
	}

	if _, ok := ie.iea.removedKeys[keyHash]; ok {
		delete(ie.iea.removedKeys, keyHash)
	} else {
		ie.iea.addedKeys[keyHash] = hashedTuple{key, partialKeyHash}
		if matchingMap, ok := ie.iea.addedPartialKeys[partialKeyHash]; ok {
			matchingMap[keyHash] = key
		} else {
			ie.iea.addedPartialKeys[partialKeyHash] = map[hash.Hash]types.Tuple{keyHash: key}
		}
	}

	ie.iea.opCount++
	ie.stack.Push(true, key, partialKey)
	return nil
}

// DeleteRow removes the given row from the index.
func (ie *IndexEditor) DeleteRow(ctx context.Context, key, partialKey types.Tuple) error {
	defer ie.autoFlush()
	ie.flushMutex.RLock()
	defer ie.flushMutex.RUnlock()

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

	if _, ok := ie.iea.addedKeys[keyHash]; ok {
		delete(ie.iea.addedKeys, keyHash)
		delete(ie.iea.addedPartialKeys[partialKeyHash], keyHash)
	} else {
		ie.iea.removedKeys[keyHash] = hashedTuple{key, partialKeyHash}
	}

	ie.iea.opCount++
	ie.stack.Push(false, key, partialKey)
	return nil
}

// HasPartial returns whether the index editor has the given partial key.
func (ie *IndexEditor) HasPartial(ctx context.Context, partialKey types.Tuple) (bool, error) {
	ie.flushMutex.RLock()
	defer ie.flushMutex.RUnlock()

	partialKeyHash, err := partialKey.Hash(partialKey.Format())
	if err != nil {
		return false, err
	}

	ie.writeMutex.Lock()
	defer ie.writeMutex.Unlock()

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
// that Undo is called and there are no operations to undo OR the reverse operation fails (it never should), this panics
// rather than errors, as the index editor is in an invalid state that cannot be corrected.
func (ie *IndexEditor) Undo(ctx context.Context) {
	indexOp, ok := ie.stack.Pop()
	if !ok {
		panic(fmt.Sprintf("attempted to undo the last operation on index '%s' but failed due to an empty stack", ie.idx.Name()))
	}
	// If an operation succeeds and does not do anything, then an empty tuple is pushed onto the stack.
	if indexOp.fullKey.Empty() {
		return
	}
	if indexOp.isInsert {
		err := ie.DeleteRow(ctx, indexOp.fullKey, indexOp.partialKey)
		if err != nil {
			panic(fmt.Sprintf("index '%s' is in an invalid and unrecoverable state: "+
				"attempted to undo previous insertion but encountered the following error: %v",
				ie.idx.Name(), err))
		}
	} else {
		err := ie.InsertRow(ctx, indexOp.fullKey, indexOp.partialKey)
		if err != nil {
			panic(fmt.Sprintf("index '%s' is in an invalid and unrecoverable state: "+
				"attempted to undo previous deletion but encountered the following error: %v",
				ie.idx.Name(), err))
		}
	}
}

// Map returns a map based on the edits given, if any. If Flush() was not called prior, it will be called here.
func (ie *IndexEditor) Map(ctx context.Context) (types.Map, error) {
	ie.flush()
	err := ie.aq.WaitForEmpty()
	if err != nil {
		return types.EmptyMap, err
	}
	return ie.idxData, nil
}

// Index returns this editor's index.
func (ie *IndexEditor) Index() schema.Index {
	return ie.idx
}

// StatementStarted is analogous to the TableEditor implementation, but specific to the IndexEditor.
func (ie *IndexEditor) StatementStarted(ctx context.Context) {
	ie.flushMutex.Lock()
	defer ie.flushMutex.Unlock()
	ie.savedIea = ie.iea
	ie.iea = ie.iea.NewFromCurrent()
	ie.iea.opCount = ie.savedIea.opCount
}

// StatementFinished is analogous to the TableEditor implementation, but specific to the IndexEditor.
func (ie *IndexEditor) StatementFinished(ctx context.Context, errored bool) error {
	// If any ieas are flushing then we want them to finish first
	err := ie.aq.WaitForEmpty()
	if err != nil {
		return err
	}
	ie.flushMutex.Lock()
	defer ie.flushMutex.Unlock()

	if !errored {
		// We collapse the changes in this iea to the last to reduce the number of map editors that will need to be opened
		if ie.iea.prevIea != nil {
			targetIea := ie.iea.prevIea

			for keyHash, hTpl := range ie.iea.removedKeys {
				if _, ok := targetIea.addedKeys[keyHash]; ok {
					delete(targetIea.addedKeys, keyHash)
					delete(targetIea.addedPartialKeys[hTpl.Hash], keyHash)
				} else {
					targetIea.removedKeys[keyHash] = hTpl
				}
			}
			for keyHash, hTpl := range ie.iea.addedKeys {
				if _, ok := targetIea.removedKeys[keyHash]; ok {
					delete(targetIea.removedKeys, keyHash)
				} else {
					targetIea.addedKeys[keyHash] = hTpl
					if matchingMap, ok := targetIea.addedPartialKeys[hTpl.Hash]; ok {
						matchingMap[keyHash] = hTpl.Tuple
					} else {
						targetIea.addedPartialKeys[hTpl.Hash] = map[hash.Hash]types.Tuple{keyHash: hTpl.Tuple}
					}
				}
			}

			targetIea.opCount = ie.iea.opCount
			ie.iea.prevIea = nil
			ie.iea.rowData = types.EmptyMap
			ie.iea.addedKeys = nil
			ie.iea.removedKeys = nil
			ie.iea = targetIea
		}
	} else {
		currentIea := ie.iea
		// Loop and remove all newer ieas
		for {
			if currentIea == nil || currentIea == ie.savedIea {
				break
			}
			nextIea := currentIea.prevIea
			// We're essentially deleting currentIea, so we're closing and removing everything.
			// Some of this is taken from the steps followed when flushing, such as the map nils.
			currentIea.prevIea = nil
			if currentIea.opCount != -1 {
				currentIea.rowData = types.EmptyMap
				currentIea.addedPartialKeys = nil
				currentIea.addedKeys = nil
				currentIea.removedKeys = nil
			}
			currentIea = nextIea
		}
		// If the savedIea was processed due to a large number of ops in the statement triggering an auto flush, then we
		// need to create a new one.
		if ie.savedIea.opCount == -1 {
			ie.iea = createInitialIndexEditAcc(ie.savedIea.rowData)
		} else {
			ie.iea = ie.savedIea
		}
	}
	ie.savedIea = nil
	return nil
}

// Close is a no-op for an IndexEditor.
func (ie *IndexEditor) Close() error {
	return nil
}

// flush finalizes all of the changes made so far.
func (ie *IndexEditor) flush() {
	ie.flushMutex.Lock()
	defer ie.flushMutex.Unlock()

	if ie.iea.opCount > 0 {
		newIea := ie.iea.NewFromCurrent()
		ie.aq.Execute(newIea)
		ie.iea = newIea
	}
}

// autoFlush is called at the end of every write call (after all locks have been released) and checks if we need to
// automatically flush the edits.
func (ie *IndexEditor) autoFlush() {
	ie.flushMutex.RLock()
	ie.writeMutex.Lock()
	runFlush := ie.iea.opCount >= tableEditorMaxOps
	ie.writeMutex.Unlock()
	ie.flushMutex.RUnlock()

	if runFlush {
		ie.flush()
	}
}

func (ie *IndexEditor) flushEditAccumulator(ctx context.Context, ieaInterface interface{}) error {
	// We don't call any locks at the function entrance since this is called from an ActionExecutor with a concurrency of 1
	updatedMap, err := processIndexEditAccumulatorChain(ctx, ieaInterface.(*indexEditAccumulator), ie.writeMutex)
	if err != nil {
		return err
	}
	ie.idxData = updatedMap
	return nil
}

func processIndexEditAccumulatorChain(ctx context.Context, futureIea *indexEditAccumulator, writeMutex *sync.Mutex) (m types.Map, err error) {
	iea := futureIea.prevIea

	ed := types.CreateEditAccForMapEdits(iea.nbf)
	defer ed.Close()
	for _, hTpl := range iea.removedKeys {
		ed.AddEdit(hTpl.Tuple, nil)
	}
	for _, hTpl := range iea.addedKeys {
		ed.AddEdit(hTpl.Tuple, types.EmptyTuple(hTpl.Tuple.Format()))
	}

	// If we encounter an error and return, then we need to remove this iea from the chain and update the next's rowData
	encounteredErr := true
	defer func() {
		if encounteredErr {
			// All iea modifications are guarded by writeMutex locks, so we have to acquire it
			writeMutex.Lock()
			futureIea.prevIea = nil
			futureIea.rowData = iea.rowData
			writeMutex.Unlock()
		}
	}()

	if iea.prevIea != nil {
		_, err = processIndexEditAccumulatorChain(ctx, iea, writeMutex)
		if err != nil {
			return types.EmptyMap, err
		}
	}
	accEdits, err := ed.FinishedEditing()
	if err != nil {
		return types.EmptyMap, err
	}
	// We are guaranteed that rowData is valid, as we process ieas sequentially.
	updatedMap, _, err := types.ApplyEdits(ctx, accEdits, iea.rowData)
	if err != nil {
		return types.EmptyMap, err
	}
	// No errors were encountered, so we set the bool to false. This should come after ALL calls that may error.
	encounteredErr = false

	// All iea modifications are guarded by writeMutex locks, so we have to acquire it here
	writeMutex.Lock()
	futureIea.prevIea = nil
	futureIea.rowData = updatedMap
	writeMutex.Unlock()
	// An opCount of -1 lets us know that this iea was processed
	iea.opCount = -1
	// there used to be a memory leak in tea and this fixed it, so we're doing it to be safe with iea
	iea.addedPartialKeys = nil
	iea.addedKeys = nil
	iea.removedKeys = nil
	return updatedMap, nil
}

// Error implements the error interface.
func (u *uniqueKeyErr) Error() string {
	keyStr, _ := formatKey(context.Background(), u.IndexTuple)
	return fmt.Sprintf("UNIQUE constraint violation on index '%s': %s", u.IndexName, keyStr)
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
	indexEditor := NewIndexEditor(ctx, index, emptyIndexMap, sch)

	err = tblRowData.IterAll(ctx, func(key, value types.Value) error {
		dRow, err := row.FromNoms(sch, key.(types.Tuple), value.(types.Tuple))
		if err != nil {
			return err
		}
		fullKey, partialKey, err := row.ReduceToIndexKeys(index, dRow)
		if err != nil {
			return err
		}
		err = indexEditor.InsertRow(ctx, fullKey, partialKey)
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
