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
	"errors"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/types"
)

type EditSession struct {
	editors map[string]TableEditor
	mu      *sync.Mutex
}

func NewEditSession() EditSession {
	return EditSession{
		editors: make(map[string]TableEditor),
		mu:      &sync.Mutex{},
	}
}

// GetTableEditor returns a TableEditor for the given table. If a schema is provided and it does not match the one
// that is used for currently open editors (if any), then those editors will reload the table from the root.
func (es EditSession) GetTableEditor(ctx *sql.Context, name string, tbl *doltdb.Table) (TableEditor, error) {
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
func (es EditSession) Flush(ctx *sql.Context, root *doltdb.RootValue) (*doltdb.RootValue, error) {
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
func (es EditSession) CloseEditors(ctx *sql.Context) (err error) {
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

type TableEditor struct {
	primary indexEditor
	indexes map[string]indexEditor

	// todo(andy): don't love it
	signal *struct{ closed bool }
}

var _ sql.RowReplacer = TableEditor{}
var _ sql.RowUpdater = TableEditor{}
var _ sql.RowInserter = TableEditor{}
var _ sql.RowDeleter = TableEditor{}

func newTableEditor(ctx *sql.Context, tbl *doltdb.Table) (TableEditor, error) {
	primary, indexes, err := indexEditorsFromTable(ctx, tbl)
	if err != nil {
		return TableEditor{}, err
	}

	signal := struct{ closed bool }{closed: false}

	return TableEditor{
		primary: primary,
		indexes: indexes,
		signal:  &signal,
	}, nil
}

func (ed TableEditor) checkClosed() (err error) {
	if ed.signal.closed {
		err = errors.New("table editor was closed")
	}
	return
}

// StatementBegin implements the interface sql.TableEditor.
func (ed TableEditor) StatementBegin(ctx *sql.Context) {
	for _, dep := range ed.indexes {
		dep.StatementBegin(ctx)
	}
}

// Insert implements the interface sql.TableEditor.
func (ed TableEditor) Insert(ctx *sql.Context, sqlRow sql.Row) (err error) {
	if err = ed.checkClosed(); err != nil {
		return err
	}
	for _, dep := range ed.indexes {
		if err = dep.Insert(ctx, sqlRow); err != nil {
			return err
		}
	}
	return nil
}

// Delete implements the interface sql.TableEditor.
func (ed TableEditor) Delete(ctx *sql.Context, sqlRow sql.Row) (err error) {
	if err = ed.checkClosed(); err != nil {
		return err
	}
	for _, dep := range ed.indexes {
		if err = dep.Delete(ctx, sqlRow); err != nil {
			return err
		}
	}
	return nil
}

// Update implements the interface sql.TableEditor.
func (ed TableEditor) Update(ctx *sql.Context, oldRow sql.Row, newRow sql.Row) (err error) {
	if err = ed.checkClosed(); err != nil {
		return err
	}
	for _, dep := range ed.indexes {
		if err = dep.Update(ctx, oldRow, newRow); err != nil {
			return err
		}
	}
	return nil
}

// DiscardChanges implements the interface sql.TableEditor.
func (ed TableEditor) DiscardChanges(ctx *sql.Context, errorEncountered error) (err error) {
	if err = ed.checkClosed(); err != nil {
		return err
	}
	for _, dep := range ed.indexes {
		if err = dep.DiscardChanges(ctx, errorEncountered); err != nil {
			return err
		}
	}
	return nil
}

// StatementComplete implements the interface sql.TableEditor.
func (ed TableEditor) StatementComplete(ctx *sql.Context) (err error) {
	if err = ed.checkClosed(); err != nil {
		return err
	}
	for _, dep := range ed.indexes {
		if err = dep.StatementComplete(ctx); err != nil {
			return err
		}
	}
	return nil
}

// GetAutoIncrementValue implements the interface sql.TableEditor.
func (ed TableEditor) GetAutoIncrementValue() (interface{}, error) {
	panic("unimplemented")
}

// SetAutoIncrementValue implements the interface sql.TableEditor.
func (ed TableEditor) SetAutoIncrementValue(ctx *sql.Context, val interface{}) error {
	panic("unimplemented")
}

// Flush applies pending edits to |tbl| and returns the result.
func (ed TableEditor) Flush(ctx *sql.Context, tbl *doltdb.Table) (*doltdb.Table, error) {
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

	return tbl.SetIndexData(ctx, id)
}

// Close implements Closer
func (ed TableEditor) Close(ctx *sql.Context) (err error) {
	for _, ie := range ed.indexes {
		if cerr := ie.Close(ctx); cerr != nil {
			err = nil
		}
	}
	ed.signal.closed = true
	return err
}
