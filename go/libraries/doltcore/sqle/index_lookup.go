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
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/dolthub/dolt/go/store/types"
)

type IndexLookupKeyIterator interface {
	// NextKey returns the next key if it exists, and io.EOF if it does not.
	NextKey(ctx *sql.Context) (row.TaggedValues, error)
}

type doltIndexLookup struct {
	idx       DoltIndex
	ranges    []*noms.ReadRange
	sqlRanges sql.RangeCollection
}

var _ sql.IndexLookup = (*doltIndexLookup)(nil)

// columnBounds are used to compare a given value in the noms row iterator.
type columnBounds struct {
	lowerbound bound
	upperbound bound
}

type bound struct {
	equals   bool
	infinity bool
	val      types.Value
}

// nomsRangeCheck is used to compare a tuple against a set of comparisons in the noms row iterator.
type nomsRangeCheck []columnBounds

var _ noms.InRangeCheck = nomsRangeCheck{}

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

func (il *doltIndexLookup) RowIterForRanges(ctx *sql.Context, rowData types.Map, ranges []*noms.ReadRange, columns []string) (sql.RowIter, error) {
	nrr := noms.NewNomsRangeReader(il.idx.IndexSchema(), rowData, ranges)

	covers := il.indexCoversCols(columns)
	if covers {
		return NewCoveringIndexRowIterAdapter(ctx, il.idx, nrr, columns), nil
	} else {
		return NewIndexLookupRowIterAdapter(ctx, il.idx, nrr), nil
	}
}

// Between returns whether the given types.Value is between the bounds. In addition, this returns if the value is outside
// the bounds and above the upperbound.
func (cb columnBounds) Between(ctx context.Context, nbf *types.NomsBinFormat, val types.Value) (ok bool, over bool, err error) {
	if !cb.lowerbound.infinity {
		if cb.lowerbound.equals {
			ok, err := val.Less(nbf, cb.lowerbound.val)
			if err != nil || ok {
				return false, false, err
			}
		} else {
			ok, err := cb.lowerbound.val.Less(nbf, val)
			if err != nil || !ok {
				return false, false, err
			}
		}
	}
	if !cb.upperbound.infinity {
		if cb.upperbound.equals {
			ok, err := cb.upperbound.val.Less(nbf, val)
			if err != nil || ok {
				return false, true, err
			}
		} else {
			ok, err := val.Less(nbf, cb.upperbound.val)
			if err != nil || !ok {
				return false, true, err
			}
		}
	}
	return true, false, nil
}

// Equals returns whether the calling columnBounds is equivalent to the given columnBounds.
func (cb columnBounds) Equals(otherBounds columnBounds) bool {
	if cb.lowerbound.infinity != otherBounds.lowerbound.infinity ||
		cb.upperbound.infinity != otherBounds.upperbound.infinity ||
		cb.lowerbound.equals != otherBounds.lowerbound.equals ||
		cb.upperbound.equals != otherBounds.upperbound.equals {
		return false
	}
	if cb.lowerbound.val == nil || otherBounds.lowerbound.val == nil {
		if cb.lowerbound.val != nil || otherBounds.lowerbound.val != nil {
			return false
		}
	} else if !cb.lowerbound.val.Equals(otherBounds.lowerbound.val) {
		return false
	}
	if cb.upperbound.val == nil || otherBounds.upperbound.val == nil {
		if cb.upperbound.val != nil || otherBounds.upperbound.val != nil {
			return false
		}
	} else if !cb.upperbound.val.Equals(otherBounds.upperbound.val) {
		return false
	}
	return true
}

// Check implements the interface noms.InRangeCheck.
func (nrc nomsRangeCheck) Check(ctx context.Context, tuple types.Tuple) (valid bool, skip bool, err error) {
	valid = true
	err = tuple.IterFields(func(tupleIndex uint64, tupleVal types.Value) (stop bool, err error) {
		if tupleIndex%2 == 0 {
			return false, nil
		}
		compIndex := (tupleIndex - 1) / 2
		if compIndex >= uint64(len(nrc)) {
			return true, nil
		}
		ok, over, err := nrc[compIndex].Between(ctx, tuple.Format(), tupleVal)
		if err != nil {
			return true, err
		}
		if !ok {
			skip = true
			if compIndex == 0 && over {
				valid = false
			}
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		return false, false, err
	}
	return valid, skip, nil
}

// Equals returns whether the calling nomsRangeCheck is equivalent to the given nomsRangeCheck.
func (nrc nomsRangeCheck) Equals(otherNrc nomsRangeCheck) bool {
	if len(nrc) != len(otherNrc) {
		return false
	}
	for i := range nrc {
		if !nrc[i].Equals(otherNrc[i]) {
			return false
		}
	}
	return true
}

type nomsKeyIter interface {
	ReadKey(ctx context.Context) (types.Tuple, error)
}
