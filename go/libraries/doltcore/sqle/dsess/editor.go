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
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/val"
)

type TableEditor struct {
	deps []indexEditor
}

var _ sql.RowReplacer = TableEditor{}
var _ sql.RowUpdater = TableEditor{}
var _ sql.RowInserter = TableEditor{}
var _ sql.RowDeleter = TableEditor{}

func newSqlTableEditor(ctx *sql.Context, sqlSch sql.Schema, tbl *doltdb.Table) (TableEditor, error) {
	deps, err := indexEditorsFromTable(ctx, sqlSch, tbl)
	if err != nil {
		return TableEditor{}, err
	}

	return TableEditor{
		deps: deps,
	}, nil
}

// StatementBegin implements the interface sql.TableEditor.
func (ed TableEditor) StatementBegin(ctx *sql.Context) {
	for _, dep := range ed.deps {
		dep.StatementBegin(ctx)
	}
}

// Insert implements the interface sql.TableEditor.
func (ed TableEditor) Insert(ctx *sql.Context, sqlRow sql.Row) (err error) {
	for _, dep := range ed.deps {
		if err = dep.Insert(ctx, sqlRow); err != nil {
			return err
		}
	}
	return nil
}

// Delete implements the interface sql.TableEditor.
func (ed TableEditor) Delete(ctx *sql.Context, sqlRow sql.Row) (err error) {
	for _, dep := range ed.deps {
		if err = dep.Delete(ctx, sqlRow); err != nil {
			return err
		}
	}
	return nil
}

// Update implements the interface sql.TableEditor.
func (ed TableEditor) Update(ctx *sql.Context, oldRow sql.Row, newRow sql.Row) (err error) {
	for _, dep := range ed.deps {
		if err = dep.Update(ctx, oldRow, newRow); err != nil {
			return err
		}
	}
	return nil
}

// DiscardChanges implements the interface sql.TableEditor.
func (ed TableEditor) DiscardChanges(ctx *sql.Context, errorEncountered error) (err error) {
	for _, dep := range ed.deps {
		if err = dep.DiscardChanges(ctx, errorEncountered); err != nil {
			return err
		}
	}
	return nil
}

// StatementComplete implements the interface sql.TableEditor.
func (ed TableEditor) StatementComplete(ctx *sql.Context) (err error) {
	for _, dep := range ed.deps {
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

// Close implements Closer
func (ed TableEditor) Close(ctx *sql.Context) (err error) {
	return
}

func (ed TableEditor) flush(ctx *sql.Context) (err error) {
	return
}

func (ed TableEditor) setRoot(ctx *sql.Context, newRoot *doltdb.RootValue) error {
	panic("unimplemented")
}

func indexEditorsFromTable(ctx *sql.Context, schSch sql.Schema, tbl *doltdb.Table) ([]indexEditor, error) {
	return nil, nil
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

var shimPool = pool.NewBuffPool()

func newRowConverter(sqlSch sql.Schema, sch schema.Schema, kd, vd val.TupleDesc) (rc rowConv) {
	rc = rowConv{
		keyMap: nil,
		valMap: nil,
		keyBld: val.TupleBuilder{},
		valBld: val.TupleBuilder{},
	}
	return
}

type rowConv struct {
	keyMap, valMap []int
	keyBld, valBld val.TupleBuilder
}

func (rc rowConv) ConvertRow(row sql.Row) (key, value val.Tuple) {
	for i, j := range rc.keyMap {
		rc.keyBld.PutField(i, row[j])
	}
	key = rc.keyBld.Build(shimPool)

	for i, j := range rc.valMap {
		rc.valBld.PutField(i, row[j])
	}
	value = rc.valBld.Build(shimPool)

	return
}
