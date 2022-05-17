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
type IndexedDoltTable struct {
	table       *DoltTable
	indexLookup sql.IndexLookup
}

var _ sql.IndexedTable = (*IndexedDoltTable)(nil)

func (idt *IndexedDoltTable) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	return idt.table.GetIndexes(ctx)
}

func (idt *IndexedDoltTable) WithIndexLookup(lookup sql.IndexLookup) sql.Table {
	// TODO: this should probably be an error (there should be at most one indexed lookup on a given table)
	return idt.table.WithIndexLookup(lookup)
}

func (idt *IndexedDoltTable) Name() string {
	return idt.table.Name()
}

func (idt *IndexedDoltTable) String() string {
	return idt.table.String()
}

func (idt *IndexedDoltTable) Schema() sql.Schema {
	return idt.table.Schema()
}

func (idt *IndexedDoltTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	dt, err := idt.table.doltTable(ctx)
	if err != nil {
		return nil, err
	}
	return index.NewRangePartitionIter(ctx, dt, idt.indexLookup)
}

func (idt *IndexedDoltTable) PartitionRows(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	// todo(andy): only used by 'AS OF` queries
	dt, err := idt.table.doltTable(ctx)
	if err != nil {
		return nil, err
	}
	return index.RowIterForIndexLookup(ctx, dt, idt.indexLookup, idt.table.sqlSch, nil)
}

func (idt *IndexedDoltTable) PartitionRows2(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	dt, err := idt.table.doltTable(ctx)
	if err != nil {
		return nil, err
	}
	return index.RowIterForIndexLookup(ctx, dt, idt.indexLookup, idt.table.sqlSch, nil)
}

func (idt *IndexedDoltTable) IsTemporary() bool {
	return idt.table.IsTemporary()
}

var _ sql.IndexedTable = (*WritableIndexedDoltTable)(nil)
var _ sql.UpdatableTable = (*WritableIndexedDoltTable)(nil)
var _ sql.DeletableTable = (*WritableIndexedDoltTable)(nil)
var _ sql.ReplaceableTable = (*WritableIndexedDoltTable)(nil)
var _ sql.StatisticsTable = (*WritableIndexedDoltTable)(nil)
var _ sql.ProjectedTable = (*WritableIndexedDoltTable)(nil)

type WritableIndexedDoltTable struct {
	*WritableDoltTable
	indexLookup sql.IndexLookup
}

var _ sql.Table2 = (*WritableIndexedDoltTable)(nil)

func (t *WritableIndexedDoltTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	dt, err := t.doltTable(ctx)
	if err != nil {
		return nil, err
	}
	return index.NewRangePartitionIter(ctx, dt, t.indexLookup)
}

func (t *WritableIndexedDoltTable) PartitionRows(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	return index.PartitionIndexedTableRows(ctx, t.indexLookup.Index(), part, t.sqlSch, t.projectedCols)
}

func (t *WritableIndexedDoltTable) PartitionRows2(ctx *sql.Context, part sql.Partition) (sql.RowIter2, error) {
	iter, err := index.PartitionIndexedTableRows(ctx, t.indexLookup.Index(), part, t.sqlSch, t.projectedCols)
	if err != nil {
		return nil, err
	}

	return iter.(sql.RowIter2), nil
}

// WithProjections implements sql.ProjectedTable
func (t *WritableIndexedDoltTable) WithProjections(colNames []string) sql.Table {
	return &WritableIndexedDoltTable{
		WritableDoltTable: t.WithProjections(colNames).(*WritableDoltTable),
		indexLookup:       t.indexLookup,
	}
}

// Projections implements sql.ProjectedTable
func (t *WritableIndexedDoltTable) Projections() []string {
	return t.projectedCols
}
