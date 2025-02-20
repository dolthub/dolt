// Copyright 2024 Dolthub, Inc.
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
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

// DoltgresRangeCollection is used by Doltgres as the range collection.
type DoltgresRangeCollection []DoltgresRange

// DoltgresRange represents a range that is used by Doltgres.
type DoltgresRange struct {
	StartExpressions  []sql.Expression // StartExpressions are used to find the starting point for the iterator.
	StopExpressions   []sql.Expression // StopExpressions are used to find the stopping point for the iterator.
	FilterExpressions []sql.Expression // FilterExpressions are used to determine whether a row should be returned.
	PreciseMatch      bool             // PreciseMatch is true when a higher-level filter is unnecessary.
	reverse           bool             // reverse states whether the start and stop points should flip, reversing iteration.
}

// DoltgresPartitionIter is an iterator that returns DoltgresPartition.
type DoltgresPartitionIter struct {
	partitions []DoltgresPartition
	curr       int
}

// DoltgresPartition is analogous to a contiguous iteration over an index. These are used to create the normal range
// iterators.
type DoltgresPartition struct {
	idx  *doltIndex
	rang DoltgresRange
	curr int
}

// DoltgresFilterIter is a special map iterator that is able to perform filter checks without needed to delay the check
// to a higher level, which will bypass reading from the primary table. This mirrors the Postgres behavior.
type DoltgresFilterIter struct {
	sqlCtx  *sql.Context
	inner   prolly.MapIter
	keyDesc val.TupleDesc
	ns      tree.NodeStore
	row     sql.Row
	filters []sql.Expression
}

var _ sql.RangeCollection = DoltgresRangeCollection{}
var _ sql.Range = DoltgresRange{}
var _ sql.PartitionIter = (*DoltgresPartitionIter)(nil)
var _ sql.Partition = DoltgresPartition{}
var _ prolly.MapIter = (*DoltgresFilterIter)(nil)

// Equals implements the sql.RangeCollection interface.
func (ranges DoltgresRangeCollection) Equals(other sql.RangeCollection) (bool, error) {
	otherCollection, ok := other.(DoltgresRangeCollection)
	if !ok {
		return false, nil
	}
	if len(ranges) != len(otherCollection) {
		return false, nil
	}
	for i := range ranges {
		if ok, err := ranges[i].Equals(otherCollection[i]); err != nil || !ok {
			return ok, err
		}
	}
	return true, nil
}

// Len implements the sql.RangeCollection interface.
func (ranges DoltgresRangeCollection) Len() int {
	return len(ranges)
}

// DebugString implements the sql.RangeCollection interface.
func (ranges DoltgresRangeCollection) DebugString() string {
	return ranges.String()
}

// String implements the sql.RangeCollection interface.
func (ranges DoltgresRangeCollection) String() string {
	sb := strings.Builder{}
	sb.WriteByte('[')
	for i, rang := range ranges {
		if i != 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(rang.String())
	}
	sb.WriteByte(']')
	return sb.String()
}

// ToRanges implements the sql.RangeCollection interface.
func (ranges DoltgresRangeCollection) ToRanges() []sql.Range {
	slice := make([]sql.Range, len(ranges))
	for i := range ranges {
		slice[i] = ranges[i]
	}
	return slice
}

// Equals implements the sql.Range interface.
func (d DoltgresRange) Equals(other sql.Range) (bool, error) {
	_, ok := other.(DoltgresRange)
	if !ok {
		return false, nil
	}
	// TODO: this isn't being called for now, so we can just return true and implement it later
	return true, nil
}

// String implements the sql.Range interface.
func (d DoltgresRange) String() string {
	// TODO: implement me
	return "DoltgresRange"
}

// DebugString implements the sql.Range interface.
func (d DoltgresRange) DebugString() string {
	return d.String()
}

// Close implements the sql.PartitionIter interface.
func (iter *DoltgresPartitionIter) Close(*sql.Context) error {
	return nil
}

// Next implements the sql.PartitionIter interface.
func (iter *DoltgresPartitionIter) Next(*sql.Context) (sql.Partition, error) {
	if iter.curr >= len(iter.partitions) {
		return nil, io.EOF
	}
	iter.curr++
	return iter.partitions[iter.curr-1], nil
}

// Key implements the sql.Partition interface.
func (partition DoltgresPartition) Key() []byte {
	var bytes [4]byte
	binary.BigEndian.PutUint32(bytes[:], uint32(partition.curr))
	return bytes[:]
}

// Next implements the prolly.MapIter interface.
func (iter *DoltgresFilterIter) Next(ctx context.Context) (val.Tuple, val.Tuple, error) {
OuterLoop:
	for {
		k, v, err := iter.inner.Next(ctx)
		if err != nil {
			return k, v, err
		}
		if err = doltgresMapSearchKeyToRow(ctx, k, iter.keyDesc, iter.ns, iter.row); err != nil {
			return k, v, err
		}
		for _, filterExpr := range iter.filters {
			result, err := filterExpr.Eval(iter.sqlCtx, iter.row)
			if err != nil {
				return k, v, err
			}
			if !(result.(bool)) {
				continue OuterLoop
			}
		}
		return k, v, err
	}
}

// NewDoltgresPartitionIter creates a new sql.PartitionIter for Doltgres indexing.
func NewDoltgresPartitionIter(ctx *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	idx := lookup.Index.(*doltIndex)
	ranges, ok := lookup.Ranges.(DoltgresRangeCollection)
	if !ok {
		return nil, fmt.Errorf("Doltgres partition iter expected Doltgres ranges")
	}
	partitions := make([]DoltgresPartition, len(ranges))
	for i, rang := range ranges {
		rang.reverse = lookup.IsReverse
		partitions[i] = DoltgresPartition{
			idx:  idx,
			rang: rang,
			curr: i,
		}
	}
	return &DoltgresPartitionIter{
		partitions: partitions,
		curr:       0,
	}, nil
}

// doltgresProllyMapIterator returns a map iterator, which handles the contiguous iteration over the underlying map that
// stores an index's data. This also handles filter expressions, if any are present.
func doltgresProllyMapIterator(ctx *sql.Context, keyDesc val.TupleDesc, ns tree.NodeStore, root tree.Node, rang DoltgresRange) (prolly.MapIter, error) {
	searchRow := make(sql.Row, len(keyDesc.Types))
	var findStartErr error
	findStart := func(_ context.Context, nd tree.Node) int {
		return sort.Search(nd.Count(), func(i int) bool {
			key := val.Tuple(nd.GetKey(i))
			if err := doltgresMapSearchKeyToRow(ctx, key, keyDesc, ns, searchRow); err != nil {
				findStartErr = err
			} else {
				for _, expr := range rang.StartExpressions {
					res, err := expr.Eval(ctx, searchRow)
					if err != nil {
						findStartErr = err
					} else if !(res.(bool)) {
						return false
					}
				}
				return true
			}
			return false
		})
	}
	var findStopErr error
	findStop := func(_ context.Context, nd tree.Node) (idx int) {
		return sort.Search(nd.Count(), func(i int) bool {
			key := val.Tuple(nd.GetKey(i))
			if err := doltgresMapSearchKeyToRow(ctx, key, keyDesc, ns, searchRow); err != nil {
				findStopErr = err
			} else {
				for _, expr := range rang.StopExpressions {
					res, err := expr.Eval(ctx, searchRow)
					if err != nil {
						findStopErr = err
					} else if res.(bool) {
						return true
					}
				}
			}
			return false
		})
	}

	var indexIter prolly.MapIter
	var err error
	if rang.reverse {
		indexIter, err = tree.ReverseOrderedTreeIterFromCursors[val.Tuple, val.Tuple](ctx, root, ns, findStart, findStop)
		if err != nil {
			return nil, err
		}
	} else {
		indexIter, err = tree.OrderedTreeIterFromCursors[val.Tuple, val.Tuple](ctx, root, ns, findStart, findStop)
		if err != nil {
			return nil, err
		}
	}
	if findStartErr != nil {
		return nil, findStartErr
	}
	if findStopErr != nil {
		return nil, findStopErr
	}
	if len(rang.FilterExpressions) == 0 {
		return indexIter, nil
	} else {
		return &DoltgresFilterIter{
			sqlCtx:  ctx,
			inner:   indexIter,
			keyDesc: keyDesc,
			ns:      ns,
			row:     searchRow,
			filters: rang.FilterExpressions,
		}, nil
	}
}

// doltgresMapSearchKeyToRow writes the given key into the given row. As all used functions are expressions, they expect
// a sql.Row, and we must therefore convert the key tuple into the format expected of the expression.
func doltgresMapSearchKeyToRow(ctx context.Context, key val.Tuple, keyDesc val.TupleDesc, ns tree.NodeStore, row sql.Row) (err error) {
	for i := range row {
		row[i], err = tree.GetField(ctx, keyDesc, i, key, ns)
		if err != nil {
			return err
		}
	}
	return
}
