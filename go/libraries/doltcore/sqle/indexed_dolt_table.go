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
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/go-mysql-server/sql"
)

var _ sql.IndexedTable = (*WritableIndexedDoltTable)(nil)
var _ sql.UpdatableTable = (*WritableIndexedDoltTable)(nil)
var _ sql.DeletableTable = (*WritableIndexedDoltTable)(nil)
var _ sql.ReplaceableTable = (*WritableIndexedDoltTable)(nil)
var _ sql.StatisticsTable = (*WritableIndexedDoltTable)(nil)
var _ sql.ProjectedTable = (*WritableIndexedDoltTable)(nil)

func NewWritableIndexedDoltTable(t *WritableDoltTable, idx index.DoltIndex) *WritableIndexedDoltTable {
	//todo backfill doltIndexedLookup info
	// like index and durable state
	return &WritableIndexedDoltTable{
		WritableDoltTable: t,
		idx:               idx,
		isDoltFormat:      types.IsFormat_DOLT(idx.Format()),
	}
}

type WritableIndexedDoltTable struct {
	*WritableDoltTable
	idx          index.DoltIndex
	isDoltFormat bool
	lb           index.LookupBuilder
}

var _ sql.Table2 = (*WritableIndexedDoltTable)(nil)

func (t *WritableIndexedDoltTable) LookupPartitions(ctx *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	return index.NewRangePartitionIter(ctx, t.DoltTable, lookup, t.isDoltFormat)
}

func (t *WritableIndexedDoltTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	panic("called partitions on a lookup indexed table")
}

func (t *WritableIndexedDoltTable) PartitionRows(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	if t.lb == nil {
		t.lb = index.NewLookupBuilder(part, t.idx, t.projectedCols, t.sqlSch, t.isDoltFormat)
	}

	return t.lb.NewRowIter(ctx, part)
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
