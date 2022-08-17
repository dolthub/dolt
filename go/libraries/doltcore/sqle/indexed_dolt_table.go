// Copyright 2020 Dolthub, Inc.
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
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/go-mysql-server/sql"
)

// IndexedDoltTable is a wrapper for a DoltTable and a doltIndexLookup. It implements the sql.Table interface like
// DoltTable, but its RowIter function returns values that match the indexLookup, instead of all rows. It's returned by
// the DoltTable WithIndexLookup function.
//type IndexedDoltTable struct {
//	table       *DoltTable
//	indexLookup sql.IndexLookup
//}
//
//var _ sql.IndexedTable = (*IndexedDoltTable)(nil)
//
//func (idt *IndexedDoltTable) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
//	return idt.table.GetIndexes(ctx)
//}
//
//func (idt *IndexedDoltTable) LookupPartitions(ctx *sql.Context, lookup sql.IndexLookup) sql.Table {
//	// TODO: this should probably be an error (there should be at most one indexed lookup on a given table)
//	return idt.table.WithIndexLookup(lookup)
//}
//
//func (idt *IndexedDoltTable) Name() string {
//	return idt.table.Name()
//}
//
//func (idt *IndexedDoltTable) String() string {
//	return idt.table.String()
//}
//
//func (idt *IndexedDoltTable) Schema() sql.Schema {
//	return idt.table.Schema()
//}
//
//func (idt *IndexedDoltTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
//	return index.NewRangePartitionIter(ctx, idt.table, idt.indexLookup)
//}
//
//func (idt *IndexedDoltTable) PartitionRows(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
//	return index.RowIterForIndexLookup(ctx, idt.table, idt.indexLookup, idt.table.sqlSch, nil)
//}
//
//func (idt *IndexedDoltTable) PartitionRows2(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
//	return index.RowIterForIndexLookup(ctx, idt.table, idt.indexLookup, idt.table.sqlSch, nil)
//}
//
//func (idt *IndexedDoltTable) IsTemporary() bool {
//	return idt.table.IsTemporary()
//}

var _ sql.IndexedTable = (*WritableIndexedDoltTable)(nil)
var _ sql.UpdatableTable = (*WritableIndexedDoltTable)(nil)
var _ sql.DeletableTable = (*WritableIndexedDoltTable)(nil)
var _ sql.ReplaceableTable = (*WritableIndexedDoltTable)(nil)
var _ sql.StatisticsTable = (*WritableIndexedDoltTable)(nil)
var _ sql.ProjectedTable = (*WritableIndexedDoltTable)(nil)

func NewWritableIndexedDoltTable() *WritableDoltTable {
	//todo backfill doltIndexedLookup info
	// like index and durable state
}

type WritableIndexedDoltTable struct {
	*WritableDoltTable
	idx          index.DoltIndex
	isDoltFormat bool
}

var _ sql.Table2 = (*WritableIndexedDoltTable)(nil)

func (t *WritableIndexedDoltTable) LookupPartitions(ctx *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	if t.idx == nil {
		t.idx = lookup.Index.(index.DoltIndex)
		// make lookup builder
	}
	return index.NewRangePartitionIter(ctx, t.DoltTable, lookup, t.isDoltFormat)

}

func (t *WritableIndexedDoltTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	//return index.NewRangePartitionIter(ctx, t.DoltTable, t.indexLookup)
	panic("called partitions on a lookup indexed table")
}

func (t *WritableIndexedDoltTable) PartitionRows(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	//TODO: we want to make one cursor associated with this indexed table
	// don't call out to indexed package
	// if no current cursor, make one
	// if there is one we will re-use it and scan to
	if t.isDoltFormat {
		// check for point lookup, and reuse cursor
		// index.LookupBuilder
		return t.lb.NewRowIter(part)
		//return index.RowIterForProllyRange(ctx, t.idx, part)
	}
	return index.PartitionIndexedTableRows(ctx, t.idx, part, t.sqlSch, t.projectedCols)
}

func (t *WritableIndexedDoltTable) PartitionRows2(ctx *sql.Context, part sql.Partition) (sql.RowIter2, error) {
	iter, err := index.PartitionIndexedTableRows(ctx, t.idx, part, t.sqlSch, t.projectedCols)
	if err != nil {
		return nil, err
	}

	return iter.(sql.RowIter2), nil
}

// WithProjections implements sql.ProjectedTable
func (t *WritableIndexedDoltTable) WithProjections(colNames []string) sql.Table {
	return &WritableIndexedDoltTable{
		WritableDoltTable: t.WithProjections(colNames).(*WritableDoltTable),
		idx:               t.idx,
	}
}

// Projections implements sql.ProjectedTable
func (t *WritableIndexedDoltTable) Projections() []string {
	names := make([]string, len(t.projectedCols))
	cols := t.sch.GetAllCols()
	for i := range t.projectedCols {
		col := cols.TagToCol[t.projectedCols[i]]
		names[i] = col.Name
	}
	return names
}
