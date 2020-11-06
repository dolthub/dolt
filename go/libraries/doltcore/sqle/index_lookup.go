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
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/lookup"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
)

type IndexLookupKeyIterator interface {
	// NextKey returns the next key if it exists, and io.EOF if it does not.
	NextKey(ctx *sql.Context) (row.TaggedValues, error)
}

type doltIndexLookup struct {
	idx    DoltIndex
	ranges []lookup.Range // The collection of ranges that represent this lookup.
}

var _ sql.MergeableIndexLookup = (*doltIndexLookup)(nil)

func (il *doltIndexLookup) String() string {
	// TODO: this could be expanded with additional info (like the expression used to create the index lookup)
	return fmt.Sprintf("doltIndexLookup:%s", il.idx.ID())
}

// IsMergeable implements sql.MergeableIndexLookup
func (il *doltIndexLookup) IsMergeable(indexLookup sql.IndexLookup) bool {
	if il != nil {
		return false //TODO: remove this when tests are added
	}
	otherIl, ok := indexLookup.(*doltIndexLookup)
	if !ok {
		return false
	}
	return il.idx == otherIl.idx
}

// Intersection implements sql.MergeableIndexLookup
func (il *doltIndexLookup) Intersection(indexLookups ...sql.IndexLookup) (sql.IndexLookup, error) {
	rangeCombinations := make([][]lookup.Range, len(il.ranges))
	for i, ilRange := range il.ranges {
		rangeCombinations[i] = []lookup.Range{ilRange}
	}
	for _, indexLookup := range indexLookups {
		otherIl, ok := indexLookup.(*doltIndexLookup)
		if !ok {
			return nil, fmt.Errorf("failed to intersect sql.IndexLookup with type '%T'", indexLookup)
		}
		var newRangeCombination [][]lookup.Range
		for _, rangeCombination := range rangeCombinations {
			for _, ilRange := range otherIl.ranges {
				rc := make([]lookup.Range, len(rangeCombination)+1)
				copy(rc, rangeCombination)
				rc[len(rangeCombination)] = ilRange
				newRangeCombination = append(newRangeCombination, rc)
			}
		}
	}
	var newRanges []lookup.Range
	var err error
	var ok bool
	for _, rangeCombination := range rangeCombinations {
		intersectedRange := lookup.AllRange()
		for _, rangeToIntersect := range rangeCombination {
			intersectedRange, ok, err = intersectedRange.TryIntersect(rangeToIntersect)
			if err != nil {
				return nil, err
			}
			if !ok {
				break
			}
		}
		if !intersectedRange.IsEmpty() {
			newRanges = append(newRanges, intersectedRange)
		}
	}
	newRanges, err = lookup.SimplifyRanges(newRanges)
	if err != nil {
		return nil, err
	}
	return &doltIndexLookup{
		idx:    il.idx,
		ranges: newRanges,
	}, nil
}

// Union implements sql.MergeableIndexLookup
func (il *doltIndexLookup) Union(indexLookups ...sql.IndexLookup) (sql.IndexLookup, error) {
	var ranges []lookup.Range
	var err error
	if len(il.ranges) == 0 {
		ranges = []lookup.Range{lookup.EmptyRange()}
	} else {
		ranges = make([]lookup.Range, len(il.ranges))
		copy(ranges, il.ranges)
	}
	for _, indexLookup := range indexLookups {
		otherIl, ok := indexLookup.(*doltIndexLookup)
		if !ok {
			return nil, fmt.Errorf("failed to union sql.IndexLookup with type '%T'", indexLookup)
		}
		ranges = append(ranges, otherIl.ranges...)
	}
	ranges, err = lookup.SimplifyRanges(ranges)
	if err != nil {
		return nil, err
	}
	return &doltIndexLookup{
		idx:    il.idx,
		ranges: ranges,
	}, nil
}

// RowIter returns a row iterator for this index lookup. The iterator will return the single matching row for the index.
func (il *doltIndexLookup) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	readRanges := make([]*noms.ReadRange, len(il.ranges))
	for i, lookupRange := range il.ranges {
		readRanges[i] = lookupRange.ToReadRange()
	}
	return &indexLookupRowIterAdapter{
		idx: il.idx,
		keyIter: &doltIndexKeyIter{
			indexMapIter: noms.NewNomsRangeReader(il.idx.IndexSchema(), il.idx.IndexRowData(), readRanges),
		},
		ctx: ctx,
	}, nil
}

type doltIndexKeyIter struct {
	indexMapIter table.TableReadCloser
}

var _ IndexLookupKeyIterator = (*doltIndexKeyIter)(nil)

func (iter *doltIndexKeyIter) NextKey(ctx *sql.Context) (row.TaggedValues, error) {
	indexRow, err := iter.indexMapIter.ReadRow(ctx)
	if err != nil {
		return nil, err
	}
	return row.GetTaggedVals(indexRow)
}
