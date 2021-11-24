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
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

func indexEditorsFromTable(ctx *sql.Context, tbl *doltdb.Table) (indexEditor, map[string]indexEditor, error) {
	var primary indexEditor
	tblSch, err := tbl.GetSchema(ctx)
	if err != nil {
		return primary, nil, err
	}

	rows, err := tbl.GetRowData(ctx)
	if err != nil {
		return primary, nil, err
	}
	primary = newIndexEditor(tblSch, tblSch, rows)

	indexes, err := tbl.GetIndexData(ctx)
	if err != nil {
		return indexEditor{}, nil, err
	}
	secondary := make(map[string]indexEditor, indexes.Len())

	err = indexes.IterAll(ctx, func(key, value types.Value) error {
		vrw := tbl.ValueReadWriter()
		tv, err := value.(types.Ref).TargetValue(ctx, vrw)
		if err != nil {
			return err
		}

		idxName := string(key.(types.String))
		idxSch := tblSch.Indexes().GetByName(idxName).Schema()
		index := prolly.MapFromValue(tv, idxSch, vrw)

		secondary[idxName] = newIndexEditor(idxSch, tblSch, index)
		return nil
	})
	if err != nil {
		return primary, secondary, err
	}

	return primary, nil, nil
}

type indexEditor struct {
	mut  prolly.MutableMap
	sch  schema.Schema
	conv rowConv
}

func newIndexEditor(sch, idxSch schema.Schema, rows prolly.Map) (ed indexEditor) {
	kd, vd := rows.Descriptors()
	conv := newRowConverter(sch, idxSch, kd, vd)

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

func newRowConverter(sch, idxSch schema.Schema, kd, vd val.TupleDesc) (rc rowConv) {
	panic("unimplemented")
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
