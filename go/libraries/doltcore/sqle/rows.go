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
	"context"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/libraries/utils/set"
	"github.com/dolthub/dolt/go/store/types"
)

// An iterator over the rows of a table.
type doltTableRowIter struct {
	sql.RowIter
	ctx    context.Context
	reader table.SqlTableReader
}

// Returns a new row iterator for the table given
func newRowIterator(tbl *DoltTable, ctx *sql.Context, partition *doltTablePartition) (sql.RowIter, error) {
	sch, err := tbl.table.GetSchema(ctx)

	if err != nil {
		return nil, err
	}

	if schema.IsKeyless(sch) {
		return newKeylessRowIterator(ctx, tbl, partition)
	} else {
		return newKeyedRowIter(ctx, tbl, partition)
	}
}

func newKeylessRowIterator(ctx *sql.Context, tbl *DoltTable, partition *doltTablePartition) (*doltTableRowIter, error) {
	var iter table.SqlTableReader
	var err error
	if partition == nil {
		iter, err = table.NewBufferedTableReader(ctx, tbl.table)
	} else {
		iter, err = table.NewBufferedTableReaderForPartition(ctx, tbl.table, partition.start, partition.end)
	}

	if err != nil {
		return nil, err
	}

	return &doltTableRowIter{
		ctx:    ctx,
		reader: iter,
	}, nil
}

func newKeyedRowIter(ctx context.Context, tbl *DoltTable, partition *doltTablePartition) (sql.RowIter, error) {
	sch, err := tbl.table.GetSchema(ctx)

	if err != nil {
		return nil, err
	}

	rowData, err := tbl.table.GetRowData(ctx)

	if err != nil {
		return nil, err
	}

	var mapIter types.MapTupleIterator
	if partition == nil {
		mapIter, err = rowData.RangeIterator(ctx, 0, rowData.Len())
	} else {
		mapIter, err = partition.IteratorForPartition(ctx, rowData)
	}

	if err != nil {
		return nil, err
	}

	cols := sch.GetAllCols().GetColumns()
	tagToSqlColIdx := make(map[uint64]int)

	var projectedCols []string
	if projTbl, ok := interface{}(tbl).(projected); ok {
		projectedCols = projTbl.Project()
	}
	resultColSet := set.NewCaseInsensitiveStrSet(projectedCols)
	for i, col := range cols {
		if len(projectedCols) == 0 || resultColSet.Contains(col.Name) {
			tagToSqlColIdx[col.Tag] = i
		}
	}

	conv := NewKVToSqlRowConverter(tbl.table.Format(), tagToSqlColIdx, cols, len(cols))
	return NewDoltMapIter(ctx, mapIter.Next, nil, conv), nil
}

// Next returns the next row in this row iterator, or an io.EOF error if there aren't any more.
func (itr *doltTableRowIter) Next() (sql.Row, error) {
	return itr.reader.ReadSqlRow(itr.ctx)
}

// Close required by sql.RowIter interface
func (itr *doltTableRowIter) Close() error {
	return nil
}
