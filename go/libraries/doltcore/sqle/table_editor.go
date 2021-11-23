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

package sqle

import (

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/prolly"
)

type tableEditor struct {
	deps      []editDependency
	tempTable bool
}

var _ sql.RowReplacer = (*tableEditor)(nil)
var _ sql.RowUpdater = (*tableEditor)(nil)
var _ sql.RowInserter = (*tableEditor)(nil)
var _ sql.RowDeleter = (*tableEditor)(nil)

type editDependency interface {
	sql.RowReplacer
	sql.RowUpdater
	sql.RowInserter
	sql.RowDeleter
}

func newSqlTableEditor(ctx *sql.Context, t *WritableDoltTable) (*tableEditor, error) {
	ds := dsess.DSessFromSess(ctx.Session)
	ws, err := ds.WorkingSet(ctx, t.db.name)
	if err != nil {
		return nil, err
	}

	tbl, _, err :=  ws.WorkingRoot().GetTable(ctx, t.tableName)
	if err != nil {
		return nil, err
	}

	deps, err := indexDependenciesFromTable(ctx, t.sqlSch, tbl)
	if err != nil {
		return nil, err
	}

	return &tableEditor{
		deps:      deps,
		tempTable: t.IsTemporary(),
	}, nil
}

func indexDependenciesFromTable(ctx *sql.Context, schSch sql.Schema, tbl *doltdb.Table) ([]editDependency, error) {
	rows, err := tbl.GetRowData(ctx)
	if err != nil {
		return nil, err
	}

	return nil, nil
}

// StatementBegin implements the interface sql.TableEditor.
func (ed *tableEditor) StatementBegin(ctx *sql.Context) {
	for _, dep := range ed.deps {
		dep.StatementBegin(ctx)
	}
}

// Insert implements the interface sql.TableEditor.
func (ed *tableEditor) Insert(ctx *sql.Context, sqlRow sql.Row) (err error) {
	for _, dep := range ed.deps {
		if err = dep.Insert(ctx, sqlRow); err != nil {
			return err
		}
	}
	return nil
}

// Delete implements the interface sql.TableEditor.
func (ed *tableEditor) Delete(ctx *sql.Context, sqlRow sql.Row) (err error) {
	for _, dep := range ed.deps {
		if err = dep.Delete(ctx, sqlRow); err != nil {
			return err
		}
	}
	return nil
}

// Update implements the interface sql.TableEditor.
func (ed *tableEditor) Update(ctx *sql.Context, oldRow sql.Row, newRow sql.Row) (err error) {
	for _, dep := range ed.deps {
		if err = dep.Update(ctx, oldRow, newRow); err != nil {
			return err
		}
	}
	return nil
}

// DiscardChanges implements the interface sql.TableEditor.
func (ed *tableEditor) DiscardChanges(ctx *sql.Context, errorEncountered error) (err error) {
	for _, dep := range ed.deps {
		if err = dep.DiscardChanges(ctx, errorEncountered); err != nil {
			return err
		}
	}
	return nil
}

// StatementComplete implements the interface sql.TableEditor.
func (ed *tableEditor) StatementComplete(ctx *sql.Context) (err error) {
	for _, dep := range ed.deps {
		if err = dep.StatementComplete(ctx); err != nil {
			return err
		}
	}
	return nil
}

// GetAutoIncrementValue implements the interface sql.TableEditor.
func (ed *tableEditor) GetAutoIncrementValue() (interface{}, error) {
	panic("unimplemented")
}

// SetAutoIncrementValue implements the interface sql.TableEditor.
func (ed *tableEditor) SetAutoIncrementValue(ctx *sql.Context, val interface{}) error {
	panic("unimplemented")
}

// Close implements Closer
func (ed *tableEditor) Close(ctx *sql.Context) (err error) {
	return
}

func (ed *tableEditor) flush(ctx *sql.Context) (err error) {
	return
}

func (ed *tableEditor) setRoot(ctx *sql.Context, newRoot *doltdb.RootValue) error {
	panic("unimplemented")
}

type indexEditor struct {
	mut  prolly.MutableMap
	sch  schema.Schema
	conv rowConv
}

func newIndexEditor(rowSch sql.Schema, sch schema.Schema, rows prolly.Map) (ed indexEditor) {
	kd, vd := rows.Descriptors()
	conv := newRowConverter(rowSch, sch, kd, vd)

	return indexEditor{
		mut:  rows.Mutate(),
		sch:  sch,
		conv: conv,
	}
}

// StatementBegin implements the interface sql.TableEditor.
func (ed indexEditor) StatementBegin(ctx *sql.Context) {
	return
}

func (ed indexEditor) Insert(ctx *sql.Context, sqlRow sql.Row) (err error) {
	k, v := ed.conv.ConvertRow(sqlRow)
	return ed.mut.Put(ctx, k, v)
}

func (ed indexEditor) Delete(ctx *sql.Context, sqlRow sql.Row) (err error) {
	k, _ := ed.conv.ConvertRow(sqlRow)
	return ed.mut.Put(ctx, k, nil)
}

func (ed indexEditor) Update(ctx *sql.Context, oldRow sql.Row, newRow sql.Row) (err error) {
	k, v := ed.conv.ConvertRow(newRow)
	return ed.mut.Put(ctx, k, v)
}

// DiscardChanges implements the interface sql.TableEditor.
func (ed indexEditor) DiscardChanges(ctx *sql.Context, errorEncountered error) (err error) {
	panic("unimplemented")
}

// StatementComplete implements the interface sql.TableEditor.
func (ed indexEditor) StatementComplete(ctx *sql.Context) (err error) {
	return
}

func (ed indexEditor) GetAutoIncrementValue() (interface{}, error) {
	panic("unimplemented")
}

func (ed indexEditor) SetAutoIncrementValue(ctx *sql.Context, val interface{}) error {
	panic("unimplemented")
}

// Close implements Closer
func (ed indexEditor) Close(ctx *sql.Context) (err error) {
	return
}
