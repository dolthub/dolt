// Copyright 2019 Liquidata, Inc.
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
	"io"

	"github.com/src-d/go-mysql-server/sql"

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
)

// An iterator over the rows of a table.
type doltTableRowIter struct {
	sql.RowIter
	table    *DoltTable
	rowData  types.Map
	ctx      *sql.Context
	nomsIter types.MapIterator
}

// Returns a new row iterator for the table given
func newRowIterator(tbl *DoltTable, ctx *sql.Context) *doltTableRowIter {
	rowData := tbl.table.GetRowData(ctx.Context)
	mapIter := rowData.Iterator(ctx.Context)
	return &doltTableRowIter{table: tbl, rowData: rowData, ctx: ctx, nomsIter: mapIter}
}

// Next returns the next row in this row iterator, or an io.EOF error if there aren't any more.
func (itr *doltTableRowIter) Next() (sql.Row, error) {
	key, val := itr.nomsIter.Next(itr.ctx.Context)
	if key == nil && val == nil {
		return nil, io.EOF
	}

	doltRow := row.FromNoms(itr.table.sch, key.(types.Tuple), val.(types.Tuple))
	return doltRowToSqlRow(doltRow, itr.table.sch), nil
}

// Close required by sql.RowIter interface
func (itr *doltTableRowIter) Close() error {
	return nil
}

// Returns a SQL row representation for the dolt row given.
func doltRowToSqlRow(doltRow row.Row, sch schema.Schema) sql.Row {
	colVals := make(sql.Row, sch.GetAllCols().Size())

	i := 0
	sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool) {
		value, _ := doltRow.GetColVal(tag)
		colVals[i] = doltColValToSqlColVal(value)
		i++
		return false
	})

	return sql.NewRow(colVals...)
}

// Returns a Dolt row representation for SQL row given
func SqlRowToDoltRow(nbf *types.NomsBinFormat, r sql.Row, doltSchema schema.Schema) row.Row {
	taggedVals := make(row.TaggedValues)
	for i, val := range r {
		if val != nil {
			taggedVals[uint64(i)] = SqlValToNomsVal(val)
		}
	}

	return row.New(nbf, doltSchema, taggedVals)
}

// Returns the column value for a SQL column
func doltColValToSqlColVal(val types.Value) interface{} {
	if types.IsNull(val) {
		return nil
	}

	return nomsValToSqlVal(val)
}
