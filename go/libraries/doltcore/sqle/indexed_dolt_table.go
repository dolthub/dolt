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

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/lookup"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
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
	return sqlutil.NewSinglePartitionIter(), nil
}

func (idt *IndexedDoltTable) PartitionRows(ctx *sql.Context, _ sql.Partition) (sql.RowIter, error) {
	return idt.indexLookup.RowIter(ctx, nil)
}

type rangePartition struct {
	partitionRange lookup.Range
	keyBytes       []byte
}

func (rp rangePartition) Key() []byte {
	return rp.keyBytes
}

type rangePartitionIter struct {
	ranges []lookup.Range
	curr   int
	mu     *sync.Mutex
}

func NewRangePartitionIter(ranges []lookup.Range) *rangePartitionIter {
	return &rangePartitionIter{
		ranges: ranges,
		curr:   0,
		mu:     &sync.Mutex{},
	}
}

// Close is required by the sql.PartitionIter interface. Does nothing.
func (itr *rangePartitionIter) Close() error {
	return nil
}

// Next returns the next partition if there is one, or io.EOF if there isn't.
func (itr *rangePartitionIter) Next() (sql.Partition, error) {
	itr.mu.Lock()
	defer itr.mu.Unlock()

	if itr.curr >= len(itr.ranges) {
		return nil, io.EOF
	}

	var bytes [4]byte
	binary.BigEndian.PutUint32(bytes[:], uint32(itr.curr))
	part := rangePartition{itr.ranges[itr.curr], bytes[:]}
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
		return NewRangePartitionIter(t.indexLookup.ranges), nil
	}
	return sqlutil.NewSinglePartitionIter(), nil
}

func (t *WritableIndexedDoltTable) PartitionRows(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	return partitionIndexedTableRows(ctx, t, t.projectedCols, part)
}

// NumRows returns the unfiltered count of rows contained in the table
func (t *WritableIndexedDoltTable) NumRows(ctx *sql.Context) (uint64, error) {
	m, err := t.table.GetRowData(ctx)

	if err != nil {
		return 0, err
	}

	return m.Len(), nil
}

func partitionIndexedTableRows(ctx *sql.Context, t *WritableIndexedDoltTable, projectedCols []string, part sql.Partition) (sql.RowIter, error) {
	switch typed := part.(type) {
	case rangePartition:
		return t.indexLookup.RowIterForRanges(ctx, []lookup.Range{typed.partitionRange}, projectedCols)
	case sqlutil.SinglePartition:
		return t.indexLookup.RowIter(ctx, projectedCols)
	}

	return nil, errors.New("unknown partition type")
}

func (t *WritableIndexedDoltTable) WithProjection(colNames []string) sql.Table {
	return &WritableIndexedDoltTable{
		WritableDoltTable: t.WithProjection(colNames).(*WritableDoltTable),
		indexLookup:       t.indexLookup,
	}
}
