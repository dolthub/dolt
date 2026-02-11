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

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/store/types"
)

var _ sql.RowIter = (*keylessRowIter)(nil)

type keylessRowIter struct {
	keyedIter   *index.DoltMapIter
	lastRead    sql.Row
	cardIdx     int
	nonCardCols int
	lastCard    uint64
}

func (k *keylessRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	if k.lastCard == 0 {
		r, err := k.keyedIter.Next(ctx)

		if err != nil {
			return nil, err
		}

		k.lastCard = r[k.cardIdx].(uint64)
		k.lastRead = r[:k.nonCardCols]
	}

	k.lastCard--
	return k.lastRead, nil
}

func (k keylessRowIter) Close(ctx *sql.Context) error {
	return k.keyedIter.Close(ctx)
}

// Returns a new row iterator for the table given
func newRowIterator(ctx context.Context, tbl *doltdb.Table, projCols []uint64, partition doltTablePartition) (sql.RowIter, error) {
	sch, err := tbl.GetSchema(ctx)

	if err != nil {
		return nil, err
	}

	types.AssertFormat_DOLT(tbl.Format())
	return ProllyRowIterFromPartition(ctx, sch, projCols, partition)
}

func ProllyRowIterFromPartition(
		ctx context.Context,
		sch schema.Schema,
		projections []uint64,
		partition doltTablePartition,
) (sql.RowIter, error) {
	rows, err := durable.ProllyMapFromIndex(partition.rowData)
	if err != nil {
		return nil, err
	}

	c, err := rows.Count()
	if err != nil {
		return nil, err
	}
	if partition.end > uint64(c) {
		partition.end = uint64(c)
	}

	iter, err := rows.FetchOrdinalRange(ctx, partition.start, partition.end)
	if err != nil {
		return nil, err
	}

	return index.NewProllyRowIterForMap(sch, rows, iter, projections), nil
}

// SqlTableToRowIter returns a |sql.RowIter| for a full table scan for the given |table|. If
// |columns| is not empty, only columns with names appearing in |columns| will
// have non-|nil| values in the resulting |sql.Row|s. If |columns| is empty,
// values for all columns in the table are populated in each returned Row. The
// returned rows always have the schema of the table, regardless of the value
// of |columns|.  Providing a column name which does not appear in the schema
// is not an error, but no corresponding column will appear in the results.
func SqlTableToRowIter(ctx *sql.Context, table *DoltTable, columns []uint64) (sql.RowIter, error) {
	t, err := table.DoltTable(ctx)
	if err != nil {
		return nil, err
	}

	data, err := t.GetRowData(ctx)
	if err != nil {
		return nil, err
	}

	p := doltTablePartition{
		end:     NoUpperBound,
		rowData: data,
	}

	return newRowIterator(ctx, t, columns, p)
}
