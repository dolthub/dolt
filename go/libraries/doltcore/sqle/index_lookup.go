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
	"context"
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/val"
)

type IndexLookupKeyIterator interface {
	// NextKey returns the next key if it exists, and io.EOF if it does not.
	NextKey(ctx *sql.Context) (row.TaggedValues, error)
}

type doltIndexLookup struct {
	idx DoltIndex
	// The collection of ranges that represent this lookup.
	ranges    []prolly.Range
	sqlRanges sql.RangeCollection
}

var _ sql.IndexLookup = &doltIndexLookup{}

func (il *doltIndexLookup) String() string {
	// TODO: this could be expanded with additional info (like the expression used to create the index lookup)
	return fmt.Sprintf("doltIndexLookup:%s", il.idx.ID())
}

func (il *doltIndexLookup) IndexRowData() prolly.Map {
	return il.idx.IndexRowData()
}

// Index implements the interface sql.IndexLookup
func (il *doltIndexLookup) Index() sql.Index {
	return il.idx
}

func (il *doltIndexLookup) Ranges() sql.RangeCollection {
	panic("implement me")
}

// RowIter returns a row iterator for this index lookup. The iterator will return the single matching row for the index.
func (il *doltIndexLookup) RowIter(ctx *sql.Context, rowData prolly.Map, columns []string) (sql.RowIter, error) {
	return il.RowIterForRanges(ctx, rowData, il.ranges, columns)
}

func (il *doltIndexLookup) indexCoversCols(cols []string) bool {
	if cols == nil {
		return false
	}

	idxCols := il.idx.IndexSchema().GetPKCols()
	covers := true
	for _, colName := range cols {
		if _, ok := idxCols.GetByNameCaseInsensitive(colName); !ok {
			covers = false
			break
		}
	}

	return covers
}

func (il *doltIndexLookup) RowIterForRanges(ctx *sql.Context, rows prolly.Map, ranges []prolly.Range, projs []string) (sql.RowIter, error) {
	if len(ranges) > 1 {
		panic("too many ranges!")
	}

	iter, err := rows.IterRange(ctx, ranges[0])
	if err != nil {
		return nil, err
	}

	sch := il.idx.IndexSchema()

	return rowIterFromMapIter(ctx, sch, rows, iter, projs)
}

type keyIter interface {
	ReadKey(ctx context.Context) (val.Tuple, error)
}

func pruneEmptyRanges(sqlRanges []sql.Range) (pruned []sql.Range, err error) {
	pruned = make([]sql.Range, 0, len(sqlRanges))
	for _, sr := range sqlRanges {
		empty := false
		for _, colExpr := range sr {
			empty, err = colExpr.IsEmpty()
			if err != nil {
				return nil, err
			} else if empty {
				// one of the RangeColumnExprs in |sr|
				// is empty: prune the entire range
				break
			}
		}
		if !empty {
			pruned = append(pruned, sr)
		}
	}
	return pruned, nil
}

func prollyRangeFromSqlRange(sqlRange sql.Range, tb *val.TupleBuilder) (rng prolly.Range, err error) {
	var lower, upper []sql.RangeCut
	for _, expr := range sqlRange {
		lower = append(lower, expr.LowerBound)
		upper = append(upper, expr.UpperBound)
	}

	start := prolly.RangeCut{Inclusive: true}
	startFields := sql.Row{}
	for _, sc := range lower {
		if !isBindingCut(sc) {
			start = prolly.RangeCut{Unbound: true, Inclusive: false}
			break
		}
		start.Inclusive = start.Inclusive && sc.TypeAsLowerBound() == sql.Closed
		startFields = append(startFields, sql.GetRangeCutKey(sc))
	}
	if !start.Unbound {
		start.Key = tupleFromKeys(startFields, tb)
	}

	stop := prolly.RangeCut{Inclusive: true}
	stopFields := sql.Row{}
	for _, sc := range upper {
		if !isBindingCut(sc) {
			stop = prolly.RangeCut{Unbound: true, Inclusive: false}
			break
		}
		stop.Inclusive = stop.Inclusive && sc.TypeAsUpperBound() == sql.Closed
		stopFields = append(stopFields, sql.GetRangeCutKey(sc))
	}
	if !stop.Unbound {
		stop.Key = tupleFromKeys(stopFields, tb)
	}

	return prolly.Range{
		Start:   start,
		Stop:    stop,
		KeyDesc: tb.Desc,
		Reverse: false,
	}, nil
}

func isBindingCut(cut sql.RangeCut) bool {
	return cut != sql.BelowAll{} && cut != sql.AboveAll{}
}

func tupleFromKeys(keys sql.Row, tb *val.TupleBuilder) val.Tuple {
	for i, v := range keys {
		tb.PutField(i, v)
	}
	return tb.Build(shimPool)
}
