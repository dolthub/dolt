// Copyright 2020-2021 Dolthub, Inc.
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

package index

import (
	"encoding/binary"
	"io"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/dolthub/dolt/go/store/types"
)

type DoltIndex interface {
	sql.Index
	Schema() schema.Schema
	IndexSchema() schema.Schema
	TableData() types.Map
	IndexRowData() types.Map
}

func NewRangePartitionIter(lookup sql.IndexLookup) sql.PartitionIter {
	dlu := lookup.(*doltIndexLookup)
	return &rangePartitionIter{
		ranges:  dlu.ranges,
		curr:    0,
		mu:      &sync.Mutex{},
		rowData: dlu.IndexRowData(),
	}
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

// Close is required by the sql.PartitionIter interface. Does nothing.
func (itr *rangePartitionIter) Close(*sql.Context) error {
	return nil
}

// Next returns the next partition if there is one, or io.EOF if there isn't.
func (itr *rangePartitionIter) Next(_ *sql.Context) (sql.Partition, error) {
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

func PartitionIndexedTableRows(ctx *sql.Context, idx sql.Index, projectedCols []string, part sql.Partition) (sql.RowIter, error) {
	rp := part.(rangePartition)
	ranges := []*noms.ReadRange{rp.partitionRange}
	return RowIterForRanges(ctx, idx.(DoltIndex), ranges, rp.rowData, projectedCols)
}
