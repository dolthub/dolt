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

// InsertRow adds the given row to the table. If the row already exists, use UpdateRow.
func (kte *keylessTableEditor) InsertRow(ctx context.Context, r row.Row) (err error) {
	kte.mu.Lock()
	defer kte.mu.Unlock()

	defer func() { err = kte.autoFlush(ctx) }()

	var key, val types.Tuple
	key, val, err = row.Deconstruct(ctx, kte.sch, r)
	if err != nil {
		return err
	}

	return kte.acc.increment(key, val)
}

// DeleteKey removes the given key from the table.
func (kte *keylessTableEditor) DeleteRow(ctx context.Context, r row.Row) (err error) {
	kte.mu.Lock()
	defer kte.mu.Unlock()

	defer func() { err = kte.autoFlush(ctx) }()

	var key, val types.Tuple
	key, val, err = row.Deconstruct(ctx, kte.sch, r)
	if err != nil {
		return err
	}

	return kte.acc.decrement(key, val)
}

// UpdateRow takes the current row and new rows, and updates it accordingly.
func (kte *keylessTableEditor) UpdateRow(ctx context.Context, old row.Row, new row.Row) (err error) {
	kte.mu.Lock()
	defer kte.mu.Unlock()

	defer func() { err = kte.autoFlush(ctx) }()

	var key, val types.Tuple
	key, val, err = row.Deconstruct(ctx, kte.sch, old)
	if err != nil {
		return err
	}

	err = kte.acc.decrement(key, val)
	if err != nil {
		return err
	}

	key, val, err = row.Deconstruct(ctx, kte.sch, old)
	if err != nil {
		return err
	}

	return kte.acc.increment(key, val)
}

func (kte *keylessTableEditor) GetAutoIncrementValue() types.Value {
	return types.NullValue
}

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

func (kte *keylessTableEditor) Schema() schema.Schema {
	return kte.sch
}

func (kte *keylessTableEditor) Name() string {
	return kte.name
}

func (kte *keylessTableEditor) Format() *types.NomsBinFormat {
	return kte.tbl.Format()
}

// Close ensures that all goroutines that may be open are properly disposed of. Attempting to call any other function
// on this editor after calling this function is undefined behavior.
func (kte *keylessTableEditor) Close() error {
	return nil
}

// autoFlush will call flush() if we have accumulated enough edits.
func (kte *keylessTableEditor) autoFlush(ctx context.Context) error {
	// work is proportional to number of deltas
	// not the number of row operations
	if len(kte.acc.deltas) >= tableEditorMaxOps {
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
	keys := make([]types.Value, len(acc.deltas))
	for _, vd := range acc.deltas {
		keys[idx] = vd.key
		idx++
	}

	err = types.SortWithErroringLess(types.ValueSort{Values: keys, Nbf: acc.nbf})
	if err != nil {
		return nil, err
	}

	ed := rowData.Edit()
	iter := table.NewMapPointReader(rowData, keys...)

	for {
		k, v, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		delta, err := acc.getRowDelta(k.(types.Tuple))
		if err != nil {
			return nil, err
		}

		if v == nil {
			// row does not yet exist
			v, err = setCardinality(delta.val, delta.delta)
		} else {
			v, err = updateCardinality(v.(types.Tuple), delta.delta)
		}
		if err != nil {
			return nil, err
		}

		ed.Set(k, v)
	}

	rowData, err = ed.Map(ctx)
	if err != nil {
		return nil, err
	}

	return tbl.UpdateRows(ctx, rowData)
}

func setCardinality(val types.Tuple, delta int64) (types.Tuple, error) {
	if delta < 1 {
		err := fmt.Errorf("attempted to initialize row with non-positive cardinality")
		return types.Tuple{}, err
	}
	return val.Set(0, types.Uint(delta))
}

func updateCardinality(val types.Tuple, delta int64) (v types.Tuple, err error) {
	c, err := val.Get(0)
	if err != nil {
		return v, err
	}

	card := int64(c.(types.Uint)) + delta // lossy
	if card < 0 {
		card = 0
	}

	return val.Set(0, types.Uint(card))
}
