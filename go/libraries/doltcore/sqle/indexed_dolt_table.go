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
	"encoding/binary"
	"errors"
	"io"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/dolthub/dolt/go/store/types"
)

// IndexedDoltTable is a wrapper for a DoltTable and a doltIndexLookup. It implements the sql.Table interface like
// DoltTable, but its RowIter function returns values that match the indexLookup, instead of all rows. It's returned by
// the DoltTable WithIndexLookup function.
type IndexedDoltTable struct {
	table       *DoltTable
	indexLookup *doltIndexLookup
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
	return sqlutil.NewSinglePartitionIter(idt.indexLookup.IndexRowData()), nil
}

func (idt *IndexedDoltTable) PartitionRows(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	if singlePart, ok := part.(sqlutil.SinglePartition); ok {
		return idt.indexLookup.RowIter(ctx, singlePart.RowData, nil)
	}

	return nil, errors.New("unexpected partition type")
}

func (idt *IndexedDoltTable) IsTemporary() bool {
	return idt.table.IsTemporary()
}

type rangePartition struct {
	partitionRange *noms.ReadRange
	keyBytes       []byte
	rowData        types.Map
}

func (rp rangePartition) Key() []byte {
	return rp.keyBytes
}

type rangePartitionIter struct {
	ranges  []*noms.ReadRange
	curr    int
	mu      *sync.Mutex
	rowData types.Map
}

func NewRangePartitionIter(ranges []*noms.ReadRange, rowData types.Map) *rangePartitionIter {
	return &rangePartitionIter{
		ranges:  ranges,
		curr:    0,
		mu:      &sync.Mutex{},
		rowData: rowData,
	}
}

// Close is required by the sql.PartitionIter interface. Does nothing.
func (itr *rangePartitionIter) Close(*sql.Context) error {
	return nil
}

// Next returns the next partition if there is one, or io.EOF if there isn't.
func (itr *rangePartitionIter) Next(ctx *sql.Context) (sql.Partition, error) {
	itr.mu.Lock()
	defer itr.mu.Unlock()

	if itr.curr >= len(itr.ranges) {
		return nil, io.EOF
	}

	var bytes [4]byte
	binary.BigEndian.PutUint32(bytes[:], uint32(itr.curr))
	part := rangePartition{itr.ranges[itr.curr], bytes[:], itr.rowData}
	itr.curr += 1

	return part, nil
}

var _ sql.IndexedTable = (*WritableIndexedDoltTable)(nil)
var _ sql.UpdatableTable = (*WritableIndexedDoltTable)(nil)
var _ sql.DeletableTable = (*WritableIndexedDoltTable)(nil)
var _ sql.ReplaceableTable = (*WritableIndexedDoltTable)(nil)
var _ sql.StatisticsTable = (*WritableIndexedDoltTable)(nil)

type WritableIndexedDoltTable struct {
	*WritableDoltTable
	indexLookup *doltIndexLookup
}

func (t *WritableIndexedDoltTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	if len(t.indexLookup.ranges) > 1 {
		return NewRangePartitionIter(t.indexLookup.ranges, t.indexLookup.IndexRowData()), nil
	}

	return sqlutil.NewSinglePartitionIter(t.indexLookup.IndexRowData()), nil
}

func (t *WritableIndexedDoltTable) PartitionRows(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	return partitionIndexedTableRows(ctx, t, t.projectedCols, part)
}

func partitionIndexedTableRows(ctx *sql.Context, t *WritableIndexedDoltTable, projectedCols []string, part sql.Partition) (sql.RowIter, error) {
	switch typed := part.(type) {
	case rangePartition:
		return t.indexLookup.RowIterForRanges(ctx, typed.rowData, []*noms.ReadRange{typed.partitionRange}, projectedCols)
	case sqlutil.SinglePartition:
		return t.indexLookup.RowIter(ctx, typed.RowData, projectedCols)
	}

	return nil, errors.New("unknown partition type")
}

func (t *WritableIndexedDoltTable) WithProjection(colNames []string) sql.Table {
	return &WritableIndexedDoltTable{
		WritableDoltTable: t.WithProjection(colNames).(*WritableDoltTable),
		indexLookup:       t.indexLookup,
	}
}
