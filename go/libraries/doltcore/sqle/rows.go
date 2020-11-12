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
	"io"

	"github.com/dolthub/go-mysql-server/sql"

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
	end      types.LesserValuable
	nbf      *types.NomsBinFormat
}

// Returns a new row iterator for the table given
func newRowIterator(tbl *DoltTable, ctx *sql.Context, partition *doltTablePartition) (*doltTableRowIter, error) {
	rowData, err := tbl.table.GetRowData(ctx)

	if err != nil {
		return nil, err
	}

	var mapIter types.MapIterator
	var end types.LesserValuable
	if partition == nil {
		mapIter, err = rowData.BufferedIterator(ctx)
	} else {
		endIter, err := rowData.IteratorAt(ctx, partition.end)

		if err != nil && err != io.EOF {
			return nil, err
		} else if err == nil {
			keyTpl, _, err := endIter.Next(ctx)

			if err != nil && err != io.EOF {
				return nil, err
			}

			if keyTpl != nil {
				end, err = keyTpl.(types.Tuple).AsSlice()

				if err != nil {
					return nil, err
				}
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

	keySl, err := key.(types.Tuple).AsSlice()

	if err != nil {
		return nil, err
	}

	if itr.end != nil {
		isLess, err := keySl.Less(itr.nbf, itr.end)

		if err != nil {
			return nil, err
		}

		if !isLess {
			return nil, io.EOF
		}
	}

	valSl, err := val.(types.Tuple).AsSlice()

	if err != nil {
		return nil, err
	}

	return sqlRowFromNomsTupleValueSlices(keySl, valSl, itr.table.sch)
}

// Close required by sql.RowIter interface
func (itr *doltTableRowIter) Close() error {
	return nil
}

func sqlRowFromNomsTupleValueSlices(keySl, valSl types.TupleValueSlice, sch schema.Schema) (sql.Row, error) {
	allCols := sch.GetAllCols()
	colVals := make(sql.Row, allCols.Size())

	for _, sl := range []types.TupleValueSlice{keySl, valSl} {
		var convErr error
		err := sl.Iter(func(tag uint64, val types.Value) (stop bool, err error) {
			if idx, ok := allCols.TagToIdx[tag]; ok {
				col := allCols.GetByIndex(idx)
				colVals[idx], convErr = col.TypeInfo.ConvertNomsValueToValue(val)

				if convErr != nil {
					return false, err
				}
			}

			return false, nil
		})

		if err != nil {
			return nil, err
		}
	}

	return sql.NewRow(colVals...), nil
}
