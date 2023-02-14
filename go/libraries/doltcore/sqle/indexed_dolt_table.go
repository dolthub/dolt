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
	"sync"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/store/types"
)

// IndexedDoltTable is a wrapper for a DoltTable. It implements the sql.Table interface like
// DoltTable, but its RowIter function returns values that match a sql.Range, instead of all
// rows. It's returned by the DoltTable.IndexedAccess function.
type IndexedDoltTable struct {
	table        *DoltTable
	idx          index.DoltIndex
	lb           index.LookupBuilder
	isDoltFormat bool
	mu           *sync.Mutex
}

func NewIndexedDoltTable(t *DoltTable, idx index.DoltIndex) *IndexedDoltTable {
	return &IndexedDoltTable{
		table:        t,
		idx:          idx,
		isDoltFormat: types.IsFormat_DOLT(t.Format()),
		mu:           &sync.Mutex{},
	}
}

var _ sql.IndexedTable = (*IndexedDoltTable)(nil)

func (idt *IndexedDoltTable) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	return idt.table.GetIndexes(ctx)
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

func (idt *IndexedDoltTable) Collation() sql.CollationID {
	return sql.CollationID(idt.table.sch.GetCollation())
}

func (idt *IndexedDoltTable) LookupPartitions(ctx *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	return index.NewRangePartitionIter(ctx, idt.table, lookup, idt.isDoltFormat)
}

func (idt *IndexedDoltTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	panic("should call LookupPartitions on an IndexedDoltTable")
}

func (idt *IndexedDoltTable) PartitionRows(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	idt.mu.Lock()
	defer idt.mu.Unlock()
	key, canCache, err := idt.table.DataCacheKey(ctx)
	if err != nil {
		return nil, err
	}

	if idt.lb == nil || !canCache || idt.lb.Key() != key {
		idt.lb, err = index.NewLookupBuilder(ctx, idt.table, idt.idx, key, idt.table.projectedCols, idt.table.sqlSch, idt.isDoltFormat)
		if err != nil {
			return nil, err
		}
	}

	return idt.lb.NewRowIter(ctx, part)
}

func (idt *IndexedDoltTable) PartitionRows2(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	idt.mu.Lock()
	defer idt.mu.Unlock()
	key, canCache, err := idt.table.DataCacheKey(ctx)
	if err != nil {
		return nil, err
	}
	if idt.lb == nil || !canCache || idt.lb.Key() != key {
		idt.lb, err = index.NewLookupBuilder(ctx, idt.table, idt.idx, key, idt.table.projectedCols, idt.table.sqlSch, idt.isDoltFormat)
		if err != nil {
			return nil, err
		}
	}

	return idt.lb.NewRowIter(ctx, part)
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

func NewWritableIndexedDoltTable(t *WritableDoltTable, idx index.DoltIndex) *WritableIndexedDoltTable {
	return &WritableIndexedDoltTable{
		WritableDoltTable: t,
		idx:               idx,
		isDoltFormat:      types.IsFormat_DOLT(idx.Format()),
		mu:                &sync.Mutex{},
	}
}

type WritableIndexedDoltTable struct {
	*WritableDoltTable
	idx          index.DoltIndex
	isDoltFormat bool
	lb           index.LookupBuilder
	mu           *sync.Mutex
}

var _ sql.Table2 = (*WritableIndexedDoltTable)(nil)

func (t *WritableIndexedDoltTable) LookupPartitions(ctx *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	return index.NewRangePartitionIter(ctx, t.DoltTable, lookup, t.isDoltFormat)
}

func (t *WritableIndexedDoltTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	panic("called partitions on a lookup indexed table")
}

func (t *WritableIndexedDoltTable) PartitionRows(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	key, canCache, err := t.DataCacheKey(ctx)
	if err != nil {
		return nil, err
	}
	if t.lb == nil || !canCache || t.lb.Key() != key {
		t.lb, err = index.NewLookupBuilder(ctx, t.DoltTable, t.idx, key, t.projectedCols, t.sqlSch, t.isDoltFormat)
		if err != nil {
			return nil, err
		}
	}

	return t.lb.NewRowIter(ctx, part)
}

func (t *WritableIndexedDoltTable) PartitionRows2(ctx *sql.Context, part sql.Partition) (sql.RowIter2, error) {
	iter, err := t.PartitionRows(ctx, part)
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
	if t.projectedCols == nil {
		return nil
	}

	names := make([]string, len(t.projectedCols))
	cols := t.sch.GetAllCols()
	for i := range t.projectedCols {
		col := cols.TagToCol[t.projectedCols[i]]
		names[i] = col.Name
	}
	return names
}
