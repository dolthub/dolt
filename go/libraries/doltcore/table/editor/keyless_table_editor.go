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

	acc keylessEditAcc

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

func newKeylessTableEditor(ctx context.Context, tbl *doltdb.Table, sch schema.Schema, name string) (TableEditor, error) {
	acc := keylessEditAcc{
		deltas: make(map[hash.Hash]rowDelta),
		nbf:    tbl.Format(),
	}

	eg, _ := errgroup.WithContext(ctx)

	te := &keylessTableEditor{
		tbl:  tbl,
		sch:  sch,
		name: name,
		acc:  acc,
		eg:   eg,
		mu:   &sync.Mutex{},
	}

	return te, nil
}

func (kte *keylessTableEditor) InsertKeyVal(ctx context.Context, key, val types.Tuple, tagToVal map[uint64]types.Value) error {
	panic("not implemented")
}

// InsertRow implements TableEditor.
func (kte *keylessTableEditor) InsertRow(ctx context.Context, r row.Row) (err error) {
	kte.mu.Lock()
	defer kte.mu.Unlock()

	defer func() { err = kte.autoFlush(ctx) }()

	var key, val types.Tuple
	key, val, err = row.ToNoms(ctx, kte.sch, r)
	if err != nil {
		return err
	}

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

	return kte.acc.decrement(key, val)
}

// UpdateRow implements TableEditor.
func (kte *keylessTableEditor) UpdateRow(ctx context.Context, old row.Row, new row.Row) (err error) {
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

	return kte.acc.increment(key, val)
}

// GetAutoIncrementValue implements TableEditor, AUTO_INCREMENT is not yet supported for keyless tables.
func (kte *keylessTableEditor) GetAutoIncrementValue() types.Value {
	return types.NullValue
}

// SetAutoIncrementValue implements TableEditor, AUTO_INCREMENT is not yet supported for keyless tables.
func (kte *keylessTableEditor) SetAutoIncrementValue(v types.Value) (err error) {
	return fmt.Errorf("keyless tables do not support AUTO_INCREMENT")
}

// Table returns a Table based on the edits given, if any. If Flush() was not called prior, it will be called here.
func (kte *keylessTableEditor) Table(ctx context.Context) (*doltdb.Table, error) {
	kte.mu.Lock()
	defer kte.mu.Unlock()

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

// Close implements TableEditor.
func (kte *keylessTableEditor) Close() error {
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
		kte.tbl, err = applyEdits(ctx, tbl, acc)
		return err
	})

	return nil
}

func applyEdits(ctx context.Context, tbl *doltdb.Table, acc keylessEditAcc) (*doltdb.Table, error) {
	rowData, err := tbl.GetRowData(ctx)
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

		var ok bool
		if v.Empty() {
			// row does not yet exist
			v, ok, err = initializeCardinality(delta.val, delta.delta)
		} else {
			v, ok, err = modifyCardinalityWithDelta(v, delta.delta)
		}
		if err != nil {
			return nil, err
		}

		if ok {
			ed.Set(k, v)
		} else {
			ed.Remove(k)
		}
	}

	rowData, err = ed.Map(ctx)
	if err != nil {
		return nil, err
	}

	return tbl.UpdateRows(ctx, rowData)
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
