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
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/store/types"
)

var _ sql.RowIter = (*keylessRowIter)(nil)

type keylessRowIter struct {
	keyedIter *index.DoltMapIter

	cardIdx     int
	nonCardCols int

	lastRead sql.Row
	lastCard uint64
}

func (k *keylessRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	if k.lastCard == 0 {
		r, err := k.keyedIter.Next(ctx)

		if err != nil {
			return nil, err
		}

		k.lastCard = r.GetValue(k.cardIdx).(uint64)
		k.lastRead = r.Subslice(0,k.nonCardCols)
	}

	k.lastCard--
	return k.lastRead, nil
}

func (k keylessRowIter) Close(ctx *sql.Context) error {
	return k.keyedIter.Close(ctx)
}

// An iterator over the rows of a table.
type doltTableRowIter struct {
	sql.RowIter
	ctx    context.Context
	reader table.SqlTableReader
}

// Returns a new row iterator for the table given
func newRowIterator(ctx context.Context, tbl *doltdb.Table, projCols []uint64, partition doltTablePartition) (sql.RowIter, error) {
	sch, err := tbl.GetSchema(ctx)

	if err != nil {
		return nil, err
	}

	if types.IsFormat_DOLT(tbl.Format()) {
		return ProllyRowIterFromPartition(ctx, sch, projCols, partition)
	}

	mapIter, err := iterForPartition(ctx, partition)
	if err != nil {
		return nil, err
	}

	if schema.IsKeyless(sch) {
		// would be more optimal to project columns into keyless tables also
		return newKeylessRowIterator(ctx, tbl, projCols, mapIter)
	} else {
		return newKeyedRowIter(ctx, tbl, projCols, mapIter)
	}
}

func newKeylessRowIterator(ctx context.Context, tbl *doltdb.Table, projectedCols []uint64, mapIter types.MapTupleIterator) (sql.RowIter, error) {

	cols, tagToSqlColIdx, err := getTagToResColIdx(ctx, tbl, projectedCols)
	if err != nil {
		return nil, err
	}

	idxOfCardinality := len(cols)
	tagToSqlColIdx[schema.KeylessRowCardinalityTag] = idxOfCardinality

	colsCopy := make([]schema.Column, len(cols), len(cols)+1)
	copy(colsCopy, cols)
	colsCopy = append(colsCopy, schema.NewColumn("__cardinality__", schema.KeylessRowCardinalityTag, types.UintKind, false))

	conv := index.NewKVToSqlRowConverter(tbl.Format(), tagToSqlColIdx, colsCopy, len(colsCopy))
	keyedItr, err := index.NewDoltMapIter(mapIter.NextTuple, nil, conv), nil
	if err != nil {
		return nil, err
	}

	return &keylessRowIter{
		keyedIter:   keyedItr,
		cardIdx:     idxOfCardinality,
		nonCardCols: len(cols),
	}, nil
}

func newKeyedRowIter(ctx context.Context, tbl *doltdb.Table, projectedCols []uint64, mapIter types.MapTupleIterator) (sql.RowIter, error) {

	cols, tagToSqlColIdx, err := getTagToResColIdx(ctx, tbl, projectedCols)
	if err != nil {
		return nil, err
	}

	conv := index.NewKVToSqlRowConverter(tbl.Format(), tagToSqlColIdx, cols, len(cols))
	return index.NewDoltMapIter(mapIter.NextTuple, nil, conv), nil
}

func iterForPartition(ctx context.Context, partition doltTablePartition) (types.MapTupleIterator, error) {
	if partition.end == NoUpperBound {
		c, err := partition.rowData.Count()
		if err != nil {
			return nil, err
		}
		partition.end = c
	}
	return partition.IteratorForPartition(ctx, partition.rowData)
}

func getTagToResColIdx(ctx context.Context, tbl *doltdb.Table, projectedCols []uint64) ([]schema.Column, map[uint64]int, error) {
	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return nil, nil, err
	}

	allCols := sch.GetAllCols().GetColumns()
	tagToSqlColIdx := make(map[uint64]int)

	if projectedCols != nil {
		outCols := make([]schema.Column, len(projectedCols))
		for i := range projectedCols {
			t := projectedCols[i]
			idx := sch.GetAllCols().TagToIdx[t]
			tagToSqlColIdx[t] = i
			outCols[i] = allCols[idx]
		}
		return outCols, tagToSqlColIdx, nil
	}

	for i, col := range allCols {
		tagToSqlColIdx[col.Tag] = i
	}
	return allCols, tagToSqlColIdx, nil
}

// Next returns the next row in this row iterator, or an io.EOF error if there aren't any more.
func (itr *doltTableRowIter) Next() (sql.Row, error) {
	return itr.reader.ReadSqlRow(itr.ctx)
}

// Close required by sql.RowIter interface
func (itr *doltTableRowIter) Close(*sql.Context) error {
	return nil
}

func ProllyRowIterFromPartition(
	ctx context.Context,
	sch schema.Schema,
	projections []uint64,
	partition doltTablePartition,
) (sql.RowIter, error) {
	rows := durable.ProllyMapFromIndex(partition.rowData)
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

// DoltTablePartitionToRowIter returns a sql.RowIter for a partition of the clustered index of |table|.
func DoltTablePartitionToRowIter(ctx *sql.Context, name string, table *doltdb.Table, start uint64, end uint64) (sql.Schema, sql.RowIter, error) {
	sch, err := table.GetSchema(ctx)
	if err != nil {
		return nil, nil, err
	}
	pkSch, err := sqlutil.FromDoltSchema("", name, sch)
	if err != nil {
		return nil, nil, err
	}

	data, err := table.GetRowData(ctx)
	if err != nil {
		return nil, nil, err
	}

	if types.IsFormat_DOLT(data.Format()) {
		idx := durable.ProllyMapFromIndex(data)
		c, err := idx.Count()
		if err != nil {
			return nil, nil, err
		}
		if end > uint64(c) {
			end = uint64(c)
		}
		iter, err := idx.IterOrdinalRange(ctx, start, end)
		if err != nil {
			return nil, nil, err
		}
		rowIter := index.NewProllyRowIterForMap(sch, idx, iter, nil)
		if err != nil {
			return nil, nil, err
		}
		return pkSch.Schema, rowIter, nil
	}

	idx := durable.NomsMapFromIndex(data)
	iterAt, err := idx.IteratorAt(ctx, start)
	if err != nil {
		return nil, nil, err
	}

	iter := types.NewLimitingMapIterator(iterAt, end-start)

	var rowIter sql.RowIter
	if schema.IsKeyless(sch) {
		rowIter, err = newKeylessRowIterator(ctx, table, nil, iter)
		if err != nil {
			return nil, nil, err
		}
	} else {
		rowIter, err = newKeyedRowIter(ctx, table, nil, iter)
		if err != nil {
			return nil, nil, err
		}
	}

	return pkSch.Schema, rowIter, nil
}
