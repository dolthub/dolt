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

package index

import (
	"context"
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/dolthub/dolt/go/store/types"
)

func RowIterForIndexLookup(ctx *sql.Context, ilu sql.IndexLookup, columns []string) (sql.RowIter, error) {
	lookup := ilu.(*doltIndexLookup)
	idx := lookup.idx

	return RowIterForRanges(ctx, idx, lookup.ranges, lookup.IndexRowData(), columns)
}

func RowIterForRanges(ctx *sql.Context, idx DoltIndex, ranges []*noms.ReadRange, rowData types.Map, columns []string) (sql.RowIter, error) {
	nrr := noms.NewNomsRangeReader(idx.IndexSchema(), rowData, ranges)

	covers := indexCoversCols(idx, columns)
	if covers {
		return NewCoveringIndexRowIterAdapter(ctx, idx, nrr, columns), nil
	} else {
		return NewIndexLookupRowIterAdapter(ctx, idx, nrr), nil
	}
}

func indexCoversCols(idx DoltIndex, cols []string) bool {
	if cols == nil {
		return false
	}

	idxCols := idx.IndexSchema().GetPKCols()
	covers := true
	for _, colName := range cols {
		if _, ok := idxCols.GetByNameCaseInsensitive(colName); !ok {
			covers = false
			break
		}
	}

	return covers
}

type IndexLookupKeyIterator interface {
	// NextKey returns the next key if it exists, and io.EOF if it does not.
	NextKey(ctx *sql.Context) (row.TaggedValues, error)
}

func DoltIndexFromLookup(lookup sql.IndexLookup) DoltIndex {
	return lookup.(*doltIndexLookup).idx
}

type doltIndexLookup struct {
	idx       DoltIndex
	ranges    []*noms.ReadRange
	sqlRanges sql.RangeCollection
}

var _ sql.IndexLookup = (*doltIndexLookup)(nil)

// boundsCase determines the case upon which the bounds are tested.
type boundsCase byte

// For each boundsCase, the first element is the lowerbound and the second element is the upperbound
const (
	boundsCase_infinity_infinity boundsCase = iota
	boundsCase_infinity_lessEquals
	boundsCase_infinity_less
	boundsCase_greaterEquals_infinity
	boundsCase_greaterEquals_lessEquals
	boundsCase_greaterEquals_less
	boundsCase_greater_infinity
	boundsCase_greater_lessEquals
	boundsCase_greater_less
)

// columnBounds are used to compare a given value in the noms row iterator.
type columnBounds struct {
	boundsCase
	lowerbound types.Value
	upperbound types.Value
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

// Between returns whether the given types.Value is between the bounds. In addition, this returns if the value is outside
// the bounds and above the upperbound.
func (cb columnBounds) Between(ctx context.Context, nbf *types.NomsBinFormat, val types.Value) (ok bool, over bool, err error) {
	switch cb.boundsCase {
	case boundsCase_infinity_infinity:
		return true, false, nil
	case boundsCase_infinity_lessEquals:
		ok, err := cb.upperbound.Less(nbf, val)
		if err != nil || ok {
			return false, true, err
		}
	case boundsCase_infinity_less:
		ok, err := val.Less(nbf, cb.upperbound)
		if err != nil || !ok {
			return false, true, err
		}
	case boundsCase_greaterEquals_infinity:
		ok, err := val.Less(nbf, cb.lowerbound)
		if err != nil || ok {
			return false, false, err
		}
	case boundsCase_greaterEquals_lessEquals:
		ok, err := val.Less(nbf, cb.lowerbound)
		if err != nil || ok {
			return false, false, err
		}
		ok, err = cb.upperbound.Less(nbf, val)
		if err != nil || ok {
			return false, true, err
		}
	case boundsCase_greaterEquals_less:
		ok, err := val.Less(nbf, cb.lowerbound)
		if err != nil || ok {
			return false, false, err
		}
		ok, err = val.Less(nbf, cb.upperbound)
		if err != nil || !ok {
			return false, true, err
		}
	case boundsCase_greater_infinity:
		ok, err := cb.lowerbound.Less(nbf, val)
		if err != nil || !ok {
			return false, false, err
		}
	case boundsCase_greater_lessEquals:
		ok, err := cb.lowerbound.Less(nbf, val)
		if err != nil || !ok {
			return false, false, err
		}
		ok, err = cb.upperbound.Less(nbf, val)
		if err != nil || ok {
			return false, true, err
		}
	case boundsCase_greater_less:
		ok, err := cb.lowerbound.Less(nbf, val)
		if err != nil || !ok {
			return false, false, err
		}
		ok, err = val.Less(nbf, cb.upperbound)
		if err != nil || !ok {
			return false, true, err
		}
	default:
		return false, false, fmt.Errorf("unknown bounds")
	}
	return true, false, nil
}

// Equals returns whether the calling columnBounds is equivalent to the given columnBounds.
func (cb columnBounds) Equals(otherBounds columnBounds) bool {
	if cb.boundsCase != otherBounds.boundsCase {
		return false
	}
	if cb.lowerbound == nil || otherBounds.lowerbound == nil {
		if cb.lowerbound != nil || otherBounds.lowerbound != nil {
			return false
		}
	} else if !cb.lowerbound.Equals(otherBounds.lowerbound) {
		return false
	}
	if cb.upperbound == nil || otherBounds.upperbound == nil {
		if cb.upperbound != nil || otherBounds.upperbound != nil {
			return false
		}
	} else if !cb.upperbound.Equals(otherBounds.upperbound) {
		return false
	}
	return true
}

// Check implements the interface noms.InRangeCheck.
func (nrc nomsRangeCheck) Check(ctx context.Context, tuple types.Tuple) (valid bool, skip bool, err error) {
	itr := types.TupleItrPool.Get().(*types.TupleIterator)
	defer types.TupleItrPool.Put(itr)
	err = itr.InitForTuple(tuple)
	if err != nil {
		return false, false, err
	}
	nbf := tuple.Format()

	for i := 0; i < len(nrc) && itr.HasMore(); i++ {
		if err := itr.Skip(); err != nil {
			return false, false, err
		}
		_, val, err := itr.Next()
		if err != nil {
			return false, false, err
		}
		if val == nil {
			break
		}

		ok, over, err := nrc[i].Between(ctx, nbf, val)
		if err != nil {
			return false, false, err
		}
		if !ok {
			return i != 0 || !over, true, nil
		}
	}
	return true, false, nil
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
