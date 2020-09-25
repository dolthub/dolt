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
	"fmt"
	"io"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

// An iterator over the rows of a table.
type doltTableRowIter struct {
	sql.RowIter
	table    *DoltTable
	rowData  types.Map
	ctx      *sql.Context
	nomsIter types.MapIterator
	end      types.Value
	nbf      *types.NomsBinFormat
}

// Returns a new row iterator for the table given
func newRowIterator(tbl *DoltTable, ctx *sql.Context, partition *doltTablePartition) (*doltTableRowIter, error) {
	rowData, err := tbl.table.GetRowData(ctx)

	if err != nil {
		return nil, err
	}

	var mapIter types.MapIterator
	var end types.Value = nil
	if partition == nil {
		mapIter, err = rowData.BufferedIterator(ctx)
	} else {
		endIter, err := rowData.IteratorAt(ctx, partition.end)

		if err != nil && err != io.EOF {
			return nil, err
		} else if err != io.EOF {
			end, _, err = endIter.Next(ctx)

			if err != nil && err != io.EOF {
				return nil, err
			}
		}

		mapIter, err = rowData.BufferedIteratorAt(ctx, partition.start)
	}

	if err != nil {
		return nil, err
	}

	return &doltTableRowIter{table: tbl, rowData: rowData, ctx: ctx, nomsIter: mapIter, end: end, nbf: rowData.Format()}, nil
}

// Next returns the next row in this row iterator, or an io.EOF error if there aren't any more.
func (itr *doltTableRowIter) Next() (sql.Row, error) {
	key, val, err := itr.nomsIter.Next(itr.ctx)

	if err != nil {
		return nil, err
	}

	if key == nil && val == nil {
		return nil, io.EOF
	}

	if itr.end != nil {
		isLess, err := key.Less(itr.nbf, itr.end)

		if err != nil {
			return nil, err
		}

		if !isLess {
			return nil, io.EOF
		}
	}

	doltRow, err := row.FromNoms(itr.table.sch, key.(types.Tuple), val.(types.Tuple))

	if err != nil {
		return nil, err
	}

	return doltRowToSqlRow(doltRow, itr.table.sch)
}

// Close required by sql.RowIter interface
func (itr *doltTableRowIter) Close() error {
	return nil
}

// Returns a SQL row representation for the dolt row given.
func doltRowToSqlRow(doltRow row.Row, sch schema.Schema) (sql.Row, error) {
	colVals := make(sql.Row, sch.GetAllCols().Size())

	i := 0
	err := sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		var innerErr error
		value, _ := doltRow.GetColVal(tag)
		colVals[i], innerErr = col.TypeInfo.ConvertNomsValueToValue(value)
		if innerErr != nil {
			return true, innerErr
		}
		i++
		return false, nil
	})

	if err != nil {
		return nil, err
	}

	return sql.NewRow(colVals...), nil
}

// Returns a Dolt row representation for SQL row given
func SqlRowToDoltRow(nbf *types.NomsBinFormat, r sql.Row, doltSchema schema.Schema) (row.Row, error) {
	taggedVals := make(row.TaggedValues)
	allCols := doltSchema.GetAllCols()
	for i, val := range r {
		tag := allCols.Tags[i]
		schCol := allCols.TagToCol[tag]
		if val != nil {
			var err error
			taggedVals[tag], err = schCol.TypeInfo.ConvertValueToNomsValue(val)
			if err != nil {
				return nil, err
			}
		} else if !schCol.IsNullable() {
			return nil, fmt.Errorf("column <%v> received nil but is non-nullable", schCol.Name)
		}
	}
	return row.New(nbf, doltSchema, taggedVals)
}
