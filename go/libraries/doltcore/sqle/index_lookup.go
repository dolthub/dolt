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
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/lookup"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/dolthub/dolt/go/store/types"
)

type IndexLookupKeyIterator interface {
	// NextKey returns the next key if it exists, and io.EOF if it does not.
	NextKey(ctx *sql.Context) (row.TaggedValues, error)
}

type doltIndexLookup struct {
	idx       DoltIndex
	ranges    []lookup.Range // The collection of ranges that represent this lookup.
	sqlRanges sql.RangeCollection
}

var _ sql.IndexLookup = (*doltIndexLookup)(nil)

func (il *doltIndexLookup) String() string {
	// TODO: this could be expanded with additional info (like the expression used to create the index lookup)
	return fmt.Sprintf("doltIndexLookup:%s", il.idx.ID())
}

func (il *doltIndexLookup) IndexRowData() types.Map {
	return il.idx.IndexRowData()
}

// Index implements the interface sql.IndexLookup
func (il *doltIndexLookup) Index() sql.Index {
	return il.idx
}

// Ranges implements the interface sql.IndexLookup
func (il *doltIndexLookup) Ranges() sql.RangeCollection {
	return il.sqlRanges
}

// RowIter returns a row iterator for this index lookup. The iterator will return the single matching row for the index.
func (il *doltIndexLookup) RowIter(ctx *sql.Context, rowData types.Map, columns []string) (sql.RowIter, error) {
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

func (il *doltIndexLookup) RowIterForRanges(ctx *sql.Context, rowData types.Map, ranges []lookup.Range, columns []string) (sql.RowIter, error) {
	readRanges := make([]*noms.ReadRange, len(ranges))
	for i, lookupRange := range ranges {
		readRanges[i] = lookupRange.ToReadRange()
	}

	nrr := noms.NewNomsRangeReader(il.idx.IndexSchema(), rowData, readRanges)

	covers := il.indexCoversCols(columns)
	if covers {
		return NewCoveringIndexRowIterAdapter(ctx, il.idx, nrr, columns), nil
	} else {
		return NewIndexLookupRowIterAdapter(ctx, il.idx, nrr), nil
	}
}

type nomsKeyIter interface {
	ReadKey(ctx context.Context) (types.Tuple, error)
}
