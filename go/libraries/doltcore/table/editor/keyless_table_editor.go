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

	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

// keylessTableEditor accumulates and applies row edits to keyless tables.
type keylessTableEditor struct {
	tbl  *doltdb.Table
	sch  schema.Schema
	name string

	acc      keylessEditAcc
	indexEds []*IndexEditor
	cvEditor *types.MapEditor
	dirty    bool

	eg *errgroup.Group
	mu *sync.Mutex
}

var _ TableEditor = &keylessTableEditor{}

// keylessTableEditors track all changes as row inserts and deletes.
// Changes are accumulated as cardinality deltas rather than totals.
type keylessEditAcc struct {
	deltas map[hash.Hash]rowDelta
	nbf    *types.NomsBinFormat
}

type rowDelta struct {
	key, val types.Tuple
	delta    int64
}

func (acc keylessEditAcc) increment(key, val types.Tuple) error {
	h, err := key.Hash(acc.nbf)
	if err != nil {
		return err
	}

	vd, ok := acc.deltas[h]
	if !ok {
		vd = rowDelta{
			key:   key,
			val:   val,
			delta: 0,
		}
	}

	vd.delta++
	acc.deltas[h] = vd

	return nil
}

func (acc keylessEditAcc) decrement(key, val types.Tuple) error {
	h, err := key.Hash(acc.nbf)
	if err != nil {
		return err
	}

	vd, ok := acc.deltas[h]
	if !ok {
		vd = rowDelta{
			key:   key,
			val:   val,
			delta: 0,
		}
	}

	vd.delta--
	acc.deltas[h] = vd

	return nil
}

func (acc keylessEditAcc) getRowDelta(key types.Tuple) (rowDelta, error) {
	h, err := key.Hash(acc.nbf)
	if err != nil {
		return rowDelta{}, err
	}

	vd, ok := acc.deltas[h]
	if !ok {
		err = fmt.Errorf("did not find delta for key %s", h.String())
		return rowDelta{}, err
	}

	return vd, nil
}

func newKeylessTableEditor(ctx context.Context, tbl *doltdb.Table, sch schema.Schema, name string, opts Options) (TableEditor, error) {
	acc := keylessEditAcc{
		deltas: make(map[hash.Hash]rowDelta),
		nbf:    tbl.Format(),
	}

	eg, _ := errgroup.WithContext(ctx)

	te := &keylessTableEditor{
		tbl:      tbl,
		sch:      sch,
		name:     name,
		acc:      acc,
		indexEds: make([]*IndexEditor, sch.Indexes().Count()),
		dirty:    false,
		eg:       eg,
		mu:       &sync.Mutex{},
	}

	for i, index := range sch.Indexes().AllIndexes() {
		indexData, err := tbl.GetNomsIndexRowData(ctx, index.Name())
		if err != nil {
			return nil, err
		}
		te.indexEds[i] = NewIndexEditor(ctx, index, indexData, sch, opts)
	}
	return te, nil
}

func (kte *keylessTableEditor) InsertKeyVal(ctx context.Context, key, val types.Tuple, tagToVal map[uint64]types.Value, errFunc PKDuplicateErrFunc) error {
	dRow, err := row.FromNoms(kte.sch, key, val)
	if err != nil {
		return err
	}
	return kte.InsertRow(ctx, dRow, errFunc)
}

func (kte *keylessTableEditor) DeleteByKey(ctx context.Context, key types.Tuple, tagToVal map[uint64]types.Value) (err error) {
	kte.mu.Lock()
	defer kte.mu.Unlock()

	defer func() { err = kte.autoFlush(ctx) }()

	nonPkCols := kte.sch.GetNonPKCols()
	tplVals := make([]types.Value, 0, 2*nonPkCols.Size())
	err = nonPkCols.Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		var val types.Value = types.NullValue
		if rowVal, ok := tagToVal[tag]; ok {
			val = rowVal
		}

		tplVals = append(tplVals, types.Uint(tag))
		tplVals = append(tplVals, val)
		return false, nil
	})

	if err != nil {
		return err
	}

	val, err := types.NewTuple(kte.tbl.Format(), tplVals...)
	if err != nil {
		return err
	}

	kte.dirty = true
	return kte.acc.decrement(key, val)
}

// InsertRow implements TableEditor.
func (kte *keylessTableEditor) InsertRow(ctx context.Context, r row.Row, _ PKDuplicateErrFunc) (err error) {
	kte.mu.Lock()
	defer kte.mu.Unlock()

	defer func() { err = kte.autoFlush(ctx) }()

	var key, val types.Tuple
	key, val, err = row.ToNoms(ctx, kte.sch, r)
	if err != nil {
		return err
	}

	kte.dirty = true
	return kte.acc.increment(key, val)
}

// DeleteRow implements TableEditor.
func (kte *keylessTableEditor) DeleteRow(ctx context.Context, r row.Row) (err error) {
	kte.mu.Lock()
	defer kte.mu.Unlock()

	defer func() { err = kte.autoFlush(ctx) }()

	var key, val types.Tuple
	key, val, err = row.ToNoms(ctx, kte.sch, r)
	if err != nil {
		return err
	}

	kte.dirty = true
	return kte.acc.decrement(key, val)
}

// UpdateRow implements TableEditor.
func (kte *keylessTableEditor) UpdateRow(ctx context.Context, old row.Row, new row.Row, _ PKDuplicateErrFunc) (err error) {
	kte.mu.Lock()
	defer kte.mu.Unlock()

	defer func() { err = kte.autoFlush(ctx) }()

	var key, val types.Tuple
	key, val, err = row.ToNoms(ctx, kte.sch, old)
	if err != nil {
		return err
	}

	err = kte.acc.decrement(key, val)
	if err != nil {
		return err
	}

	key, val, err = row.ToNoms(ctx, kte.sch, new)
	if err != nil {
		return err
	}

	kte.dirty = true
	return kte.acc.increment(key, val)
}

func (kte *keylessTableEditor) HasEdits() bool {
	return kte.dirty
}

// GetAutoIncrementValue implements TableEditor, AUTO_INCREMENT is not yet supported for keyless tables.
func (kte *keylessTableEditor) GetAutoIncrementValue() types.Value {
	return types.NullValue
}

// SetAutoIncrementValue implements TableEditor, AUTO_INCREMENT is not yet supported for keyless tables.
func (kte *keylessTableEditor) SetAutoIncrementValue(v types.Value) (err error) {
	kte.dirty = true
	return fmt.Errorf("keyless tables do not support AUTO_INCREMENT")
}

// Table returns a Table based on the edits given, if any. If Flush() was not called prior, it will be called here.
func (kte *keylessTableEditor) Table(ctx context.Context) (*doltdb.Table, error) {
	kte.mu.Lock()
	defer kte.mu.Unlock()

	if !kte.dirty {
		return kte.tbl, nil
	}

	err := kte.flush(ctx)
	if err != nil {
		return nil, err
	}

	err = kte.eg.Wait()
	if err != nil {
		return nil, err
	}

	return kte.tbl, nil
}

// Schema implements TableEditor.
func (kte *keylessTableEditor) Schema() schema.Schema {
	return kte.sch
}

// Name implements TableEditor.
func (kte *keylessTableEditor) Name() string {
	return kte.name
}

// Format implements TableEditor.
func (kte *keylessTableEditor) Format() *types.NomsBinFormat {
	return kte.tbl.Format()
}

// ValueReadWriter implements TableEditor.
func (kte *keylessTableEditor) ValueReadWriter() types.ValueReadWriter {
	return kte.tbl.ValueReadWriter()
}

// StatementStarted implements TableEditor.
func (kte *keylessTableEditor) StatementStarted(ctx context.Context) {}

// StatementFinished implements TableEditor.
func (kte *keylessTableEditor) StatementFinished(ctx context.Context, errored bool) error {
	return nil
}

// SetConstraintViolation implements TableEditor.
func (kte *keylessTableEditor) SetConstraintViolation(ctx context.Context, k types.LesserValuable, v types.Valuable) error {
	kte.mu.Lock()
	defer kte.mu.Unlock()
	if kte.cvEditor == nil {
		cvMap, err := kte.tbl.GetConstraintViolations(ctx)
		if err != nil {
			return err
		}
		kte.cvEditor = cvMap.Edit()
	}
	kte.cvEditor.Set(k, v)
	kte.dirty = true
	return nil
}

// Close implements TableEditor.
func (kte *keylessTableEditor) Close(ctx context.Context) error {
	return nil
}

// autoFlush will call flush() if we have accumulated enough edits.
func (kte *keylessTableEditor) autoFlush(ctx context.Context) error {
	// work is proportional to number of deltas
	// not the number of row operations
	if len(kte.acc.deltas) >= int(tableEditorMaxOps) {
		return kte.flush(ctx)
	}
	return nil
}

func (kte *keylessTableEditor) flush(ctx context.Context) error {
	// should only wait here on a non-autoFlush
	// eg kte.Table()
	err := kte.eg.Wait()
	if err != nil {
		return err
	}

	acc := kte.acc
	tbl := kte.tbl

	// setup fresh accumulator
	kte.acc = keylessEditAcc{
		deltas: make(map[hash.Hash]rowDelta),
		nbf:    acc.nbf,
	}

	kte.eg.Go(func() (err error) {
		kte.tbl, err = applyEdits(ctx, tbl, acc, kte.indexEds, nil)
		return err
	})

	return nil
}

func applyEdits(ctx context.Context, tbl *doltdb.Table, acc keylessEditAcc, indexEds []*IndexEditor, errFunc PKDuplicateErrFunc) (_ *doltdb.Table, retErr error) {
	rowData, err := tbl.GetNomsRowData(ctx)
	if err != nil {
		return nil, err
	}

	idx := 0
	keys := make([]types.Tuple, len(acc.deltas))
	for _, vd := range acc.deltas {
		keys[idx] = vd.key
		idx++
	}

	err = types.SortWithErroringLess(types.TupleSort{Tuples: keys, Nbf: acc.nbf})
	if err != nil {
		return nil, err
	}

	ed := rowData.Edit()
	iter := table.NewMapPointReader(rowData, keys...)

	var ok bool
	for {
		k, v, err := iter.NextTuple(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		delta, err := acc.getRowDelta(k)
		if err != nil {
			return nil, err
		}

		oldv := v
		if v.Empty() {
			// row does not yet exist
			v, ok, err = initializeCardinality(delta.val, delta.delta)

		} else {
			v, ok, err = modifyCardinalityWithDelta(v, delta.delta)

		}
		if err != nil {
			return nil, err
		}

		func(k, v types.Tuple) (*doltdb.Table, error) {
			indexOpsToUndo := make([]int, len(indexEds))
			defer func() {
				if retErr != nil {
					for i, opsToUndo := range indexOpsToUndo {
						for undone := 0; undone < opsToUndo; undone++ {
							indexEds[i].Undo(ctx)
						}
					}
				}
			}()

			for i, indexEd := range indexEds {
				var r row.Row
				if v.Empty() {
					r, _, err = row.KeylessRowsFromTuples(k, oldv)
				} else {
					r, _, err = row.KeylessRowsFromTuples(k, v)
				}
				if err != nil {
					return nil, err
				}
				fullKey, partialKey, value, err := r.ReduceToIndexKeys(indexEd.Index(), nil)
				if err != nil {
					return nil, err
				}

				if delta.delta < 1 {
					err = indexEd.DeleteRow(ctx, fullKey, partialKey, value)
					if err != nil {
						return nil, err
					}
				} else {
					err = indexEd.InsertRow(ctx, fullKey, partialKey, value)
					if err != nil {
						return nil, err
					}
				}
				indexOpsToUndo[i]++
			}
			return nil, nil
		}(k, v)

		if ok {
			ed.Set(k, v)
		} else {
			ed.Remove(k)
		}

	}

	for i := 0; i < len(indexEds); i++ {
		indexMap, idxErr := indexEds[i].Map(ctx)
		if idxErr != nil {
			return nil, err
		}
		tbl, idxErr = tbl.SetNomsIndexRows(ctx, indexEds[i].Index().Name(), indexMap)
		if idxErr != nil {
			return nil, err
		}
	}

	rowData, err = ed.Map(ctx)
	if err != nil {
		return nil, err
	}

	return tbl.UpdateNomsRows(ctx, rowData)
}

// for deletes (cardinality < 1): |ok| is set false
func initializeCardinality(val types.Tuple, card int64) (v types.Tuple, ok bool, err error) {
	if card < 1 {
		return types.Tuple{}, false, nil
	}

	v, err = val.Set(row.KeylessCardinalityValIdx, types.Uint(card))
	if err != nil {
		return v, false, err
	}

	return v, true, nil
}

// for deletes (cardinality < 1): |ok| is set false
func modifyCardinalityWithDelta(val types.Tuple, delta int64) (v types.Tuple, ok bool, err error) {
	c, err := val.Get(row.KeylessCardinalityValIdx)
	if err != nil {
		return v, false, err
	}

	card := int64(c.(types.Uint)) + delta // lossy
	if card < 1 {
		return types.Tuple{}, false, nil
	}

	v, err = val.Set(row.KeylessCardinalityValIdx, types.Uint(card))
	if err != nil {
		return v, false, err
	}

	return v, true, nil
}
