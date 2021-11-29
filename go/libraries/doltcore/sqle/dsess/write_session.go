// Copyright 2019 Dolthub, Inc.
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

package dsess

import (
	"context"
	"errors"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/types"
)

type WriteSession struct {
	editors map[string]TableWriter
	mu      *sync.Mutex
}

func NewWriteSession() WriteSession {
	return WriteSession{
		editors: make(map[string]TableWriter),
		mu:      &sync.Mutex{},
	}
}

// GetTableWriter returns a TableWriter for the given table. If a schema is provided and it does not match the one
// that is used for currently open editors (if any), then those editors will reload the table from the root.
func (es WriteSession) GetTableWriter(ctx *sql.Context, name string, tbl *doltdb.Table) (TableWriter, error) {
	es.mu.Lock()
	defer es.mu.Unlock()

	edit, ok := es.editors[name]
	if ok {
		return edit, nil
	}

	var err error
	edit, err = newTableEditor(ctx, tbl)
	if err != nil {
		return edit, err
	}
	es.editors[name] = edit

	return edit, nil
}

// Flush applies all pending edits to |root| and returns the result.
func (es WriteSession) Flush(ctx *sql.Context, root *doltdb.RootValue) (*doltdb.RootValue, error) {
	es.mu.Lock()
	defer es.mu.Unlock()

	for name, edit := range es.editors {
		tbl, _, err := root.GetTable(ctx, name)
		if err != nil {
			return nil, err
		}

		tbl, err = edit.Flush(ctx, tbl)
		if err != nil {
			return nil, err
		}

		root, err = root.PutTable(ctx, name, tbl)
		if err != nil {
			return nil, err
		}
	}

	return root, nil
}

// CloseEditors closes all editors in the session.
func (es WriteSession) CloseEditors(ctx *sql.Context) (err error) {
	es.mu.Lock()
	defer es.mu.Unlock()

	for name, edit := range es.editors {
		if cerr := edit.Close(ctx); cerr != nil {
			err = cerr
		}
		delete(es.editors, name)
	}
	return err
}

type TableWriter struct {
	primary indexWriter
	indexes map[string]indexWriter

	thing *autoThing

	// todo(andy): don't love it
	signal *struct{ closed bool }
}

var _ sql.RowReplacer = TableWriter{}
var _ sql.RowUpdater = TableWriter{}
var _ sql.RowInserter = TableWriter{}
var _ sql.RowDeleter = TableWriter{}

func newTableEditor(ctx *sql.Context, tbl *doltdb.Table) (TableWriter, error) {
	primary, indexes, err := indexWriterFromTable(ctx, tbl)
	if err != nil {
		return TableWriter{}, err
	}

	autoCol, err := getAutoIncCol(ctx, tbl)
	if err != nil {
		return TableWriter{}, err
	}

	v, err := tbl.GetAutoIncrementValue(ctx)
	if err != nil {
		panic(err)
	}

	i, err := autoCol.TypeInfo.ConvertNomsValueToValue(v)
	if err != nil {
		panic(err)
	}

	thing := newAutoThing(i)

	signal := struct{ closed bool }{closed: false}

	return TableWriter{
		primary: primary,
		indexes: indexes,
		thing:   thing,
		signal:  &signal,
	}, nil
}

func (ed TableWriter) checkClosed() (err error) {
	if ed.signal.closed {
		err = errors.New("table editor was closed")
	}
	return
}

// StatementBegin implements the interface sql.TableWriter.
func (ed TableWriter) StatementBegin(ctx *sql.Context) {
	for _, dep := range ed.indexes {
		dep.StatementBegin(ctx)
	}
}

// Insert implements the interface sql.TableWriter.
func (ed TableWriter) Insert(ctx *sql.Context, sqlRow sql.Row) (err error) {
	if err = ed.checkClosed(); err != nil {
		return err
	}
	for _, dep := range ed.indexes {
		if err = dep.Insert(ctx, sqlRow); err != nil {
			return err
		}
	}
	if err = ed.primary.Insert(ctx, sqlRow); err != nil {
		return err
	}
	return
}

// Delete implements the interface sql.TableWriter.
func (ed TableWriter) Delete(ctx *sql.Context, sqlRow sql.Row) (err error) {
	if err = ed.checkClosed(); err != nil {
		return err
	}
	for _, dep := range ed.indexes {
		if err = dep.Delete(ctx, sqlRow); err != nil {
			return err
		}
	}
	if err = ed.primary.Delete(ctx, sqlRow); err != nil {
		return err
	}
	return
}

// Update implements the interface sql.TableWriter.
func (ed TableWriter) Update(ctx *sql.Context, oldRow sql.Row, newRow sql.Row) (err error) {
	if err = ed.checkClosed(); err != nil {
		return err
	}
	for _, dep := range ed.indexes {
		if err = dep.Update(ctx, oldRow, newRow); err != nil {
			return err
		}
	}
	if err = ed.primary.Update(ctx, oldRow, newRow); err != nil {
		return err
	}
	return
}

// DiscardChanges implements the interface sql.TableWriter.
func (ed TableWriter) DiscardChanges(ctx *sql.Context, errorEncountered error) (err error) {
	if err = ed.checkClosed(); err != nil {
		return err
	}
	for _, dep := range ed.indexes {
		if err = dep.DiscardChanges(ctx, errorEncountered); err != nil {
			return err
		}
	}
	if err = ed.primary.DiscardChanges(ctx, errorEncountered); err != nil {
		return err
	}
	return
}

// StatementComplete implements the interface sql.TableWriter.
func (ed TableWriter) StatementComplete(ctx *sql.Context) (err error) {
	if err = ed.checkClosed(); err != nil {
		return err
	}
	for _, dep := range ed.indexes {
		if err = dep.StatementComplete(ctx); err != nil {
			return err
		}
	}
	if err = ed.primary.StatementComplete(ctx); err != nil {
		return err
	}
	return
}

func (ed TableWriter) PeekNextAutoIncrementValue(ctx *sql.Context) (interface{}, error) {
	return ed.thing.Peek(), nil
}

// GetNextAutoIncrementValue implements sql.AutoIncrementTable
func (ed TableWriter) GetNextAutoIncrementValue(ctx *sql.Context, potentialVal interface{}) (interface{}, error) {
	return ed.thing.Next(potentialVal), nil
}

// SetAutoIncrementValue implements the interface sql.TableWriter.
func (ed TableWriter) SetAutoIncrementValue(_ *sql.Context, val interface{}) (err error) {
	ed.thing.Set(val)
	return
}

// Flush applies pending edits to |tbl| and returns the result.
func (ed TableWriter) Flush(ctx *sql.Context, tbl *doltdb.Table) (*doltdb.Table, error) {
	p, err := ed.primary.mut.Map(ctx)
	if err != nil {
		return nil, err
	}

	tbl, err = tbl.UpdateRows(ctx, p)
	if err != nil {
		return nil, err
	}

	id, err := tbl.GetIndexData(ctx)
	if err != nil {
		return nil, err
	}
	indexes := id.Edit()

	for name, edit := range ed.indexes {
		idx, err := edit.mut.Map(ctx)
		if err != nil {
			return nil, err
		}
		indexes.Set(
			types.String(name),
			prolly.ValueFromMap(idx),
		)
	}

	id, err = indexes.Map(ctx)
	if err != nil {
		return nil, err
	}

	tbl, err = tbl.SetIndexData(ctx, id)
	if err != nil {
		return nil, err
	}

	tbl, err = tbl.SetAutoIncrementValue(types.Int(ed.thing.Peek()))
	if err != nil {
		return nil, err
	}

	return tbl, nil
}

// Close implements Closer
func (ed TableWriter) Close(ctx *sql.Context) (err error) {
	for _, ie := range ed.indexes {
		if cerr := ie.Close(ctx); cerr != nil {
			err = nil
		}
	}
	ed.signal.closed = true
	return err
}

// blah
func newAutoThing(current interface{}) (at *autoThing) {
	at = &autoThing{
		current: coerceInt64(current),
		mu:      sync.Mutex{},
	}
	return
}

type autoThing struct {
	current int64
	mu      sync.Mutex
}

var _ sql.AutoIncrementSetter = &autoThing{}

func (at *autoThing) SetAutoIncrementValue(_ *sql.Context, value interface{}) (err error) {
	at.Set(value)
	return
}

func (at *autoThing) Close(*sql.Context) (err error) {
	return
}

func (at *autoThing) Next(passed interface{}) int64 {
	at.mu.Lock()
	defer at.mu.Unlock()

	var value int64
	if passed != nil {
		coerceInt64(passed)
	}
	if value > at.current {
		at.current = value
	}

	current := at.current
	at.current++
	return current
}

func (at *autoThing) Peek() int64 {
	at.mu.Lock()
	defer at.mu.Unlock()
	return at.current
}

func (at *autoThing) Set(value interface{}) {
	at.mu.Lock()
	defer at.mu.Unlock()
	at.current = coerceInt64(value)
}

func coerceInt64(value interface{}) int64 {
	switch v := value.(type) {
	case int:
		return int64(v)
	case int8:
		return int64(v)
	case int16:
		return int64(v)
	case int32:
		return int64(v)
	case int64:
		return int64(v)
	default:
		panic(value)
	}
}

func getAutoIncCol(ctx context.Context, tbl *doltdb.Table) (schema.Column, error) {
	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return schema.Column{}, err
	}

	var autoCol schema.Column
	_ = sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		if col.AutoIncrement {
			autoCol = col
			stop = true
		}
		return
	})
	return autoCol, nil
}
