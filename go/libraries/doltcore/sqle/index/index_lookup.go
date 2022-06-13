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
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/types"
)

func PartitionIndexedTableRows(ctx *sql.Context, idx sql.Index, part sql.Partition, pkSch sql.PrimaryKeySchema, columns []string) (sql.RowIter, error) {
	rp := part.(rangePartition)
	doltIdx := idx.(DoltIndex)

	if types.IsFormat_DOLT_1(rp.primary.Format()) {
		return RowIterForProllyRange(ctx, doltIdx, rp.prollyRange, pkSch, columns, rp.primary, rp.secondary)
	}

	ranges := []*noms.ReadRange{rp.nomsRange}
	return RowIterForNomsRanges(ctx, doltIdx, ranges, columns, rp.primary, rp.secondary)
}

func RowIterForIndexLookup(ctx *sql.Context, t *doltdb.Table, ilu sql.IndexLookup, pkSch sql.PrimaryKeySchema, columns []string) (sql.RowIter, error) {
	lookup := ilu.(*doltIndexLookup)
	idx := lookup.idx

	primary, secondary, err := idx.GetDurableIndexes(ctx, t)
	if err != nil {
		return nil, err
	}

	if types.IsFormat_DOLT_1(idx.Format()) {
		// todo(andy)
		return RowIterForProllyRange(ctx, idx, lookup.prollyRanges[0], pkSch, columns, primary, secondary)
	} else {
		return RowIterForNomsRanges(ctx, idx, lookup.nomsRanges, columns, primary, secondary)
	}
}

func RowIterForProllyRange(ctx *sql.Context, idx DoltIndex, ranges prolly.Range, pkSch sql.PrimaryKeySchema, columns []string, primary, secondary durable.Index) (sql.RowIter2, error) {
	covers := indexCoversCols(idx, columns)
	if covers {
		return newProllyCoveringIndexIter(ctx, idx, ranges, pkSch, secondary)
	} else {
		return newProllyIndexIter(ctx, idx, ranges, pkSch, primary, secondary)
	}
}

func RowIterForNomsRanges(ctx *sql.Context, idx DoltIndex, ranges []*noms.ReadRange, columns []string, primary, secondary durable.Index) (sql.RowIter, error) {
	m := durable.NomsMapFromIndex(secondary)
	nrr := noms.NewNomsRangeReader(idx.IndexSchema(), m, ranges)

	covers := indexCoversCols(idx, columns)
	if covers || idx.ID() == "PRIMARY" {
		return NewCoveringIndexRowIterAdapter(ctx, idx, nrr, columns), nil
	} else {
		return NewIndexLookupRowIterAdapter(ctx, idx, primary, nrr)
	}
}

func indexCoversCols(idx DoltIndex, cols []string) bool {
	if cols == nil {
		cols = idx.Schema().GetAllCols().GetColumnNames()
	}

	var idxCols *schema.ColCollection
	if types.IsFormat_DOLT_1(idx.Format()) {
		// prolly indexes can cover an index lookup using
		// both the key and value fields of the index,
		// this allows using covering index machinery for
		// primary key index lookups.
		idxCols = idx.IndexSchema().GetAllCols()
	} else {
		// to cover an index lookup, noms indexes must
		// contain all fields in the index's key.
		idxCols = idx.IndexSchema().GetPKCols()
	}

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

func NewRangePartitionIter(ctx *sql.Context, t *doltdb.Table, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	dlu := lookup.(*doltIndexLookup)
	primary, secondary, err := dlu.idx.GetDurableIndexes(ctx, t)
	if err != nil {
		return nil, err
	}
	return &rangePartitionIter{
		nomsRanges:   dlu.nomsRanges,
		prollyRanges: dlu.prollyRanges,
		curr:         0,
		mu:           &sync.Mutex{},
		secondary:    secondary,
		primary:      primary,
	}, nil
}

type rangePartitionIter struct {
	nomsRanges   []*noms.ReadRange
	prollyRanges []prolly.Range
	curr         int
	mu           *sync.Mutex
	// the rows of the table the index references
	primary durable.Index
	// the rows of the index itself
	secondary durable.Index
}

// Close is required by the sql.PartitionIter interface. Does nothing.
func (itr *rangePartitionIter) Close(*sql.Context) error {
	return nil
}

// Next returns the next partition if there is one, or io.EOF if there isn't.
func (itr *rangePartitionIter) Next(_ *sql.Context) (sql.Partition, error) {
	itr.mu.Lock()
	defer itr.mu.Unlock()

	if types.IsFormat_DOLT_1(itr.secondary.Format()) {
		return itr.nextProllyPartition()
	}
	return itr.nextNomsPartition()
}

func (itr *rangePartitionIter) nextProllyPartition() (sql.Partition, error) {
	if itr.curr >= len(itr.prollyRanges) {
		return nil, io.EOF
	}

	var bytes [4]byte
	binary.BigEndian.PutUint32(bytes[:], uint32(itr.curr))
	pr := itr.prollyRanges[itr.curr]
	itr.curr += 1

	return rangePartition{
		prollyRange: pr,
		key:         bytes[:],
		primary:     itr.primary,
		secondary:   itr.secondary,
	}, nil
}

func (itr *rangePartitionIter) nextNomsPartition() (sql.Partition, error) {
	if itr.curr >= len(itr.nomsRanges) {
		return nil, io.EOF
	}

	var bytes [4]byte
	binary.BigEndian.PutUint32(bytes[:], uint32(itr.curr))
	nr := itr.nomsRanges[itr.curr]
	itr.curr += 1

	return rangePartition{
		nomsRange: nr,
		key:       bytes[:],
		primary:   itr.primary,
		secondary: itr.secondary,
	}, nil
}

type rangePartition struct {
	nomsRange   *noms.ReadRange
	prollyRange prolly.Range
	key         []byte
	// the rows of the table the index refers to
	primary durable.Index
	// the index entries
	secondary durable.Index
}

func (rp rangePartition) Key() []byte {
	return rp.key
}

type doltIndexLookup struct {
	idx          DoltIndex
	nomsRanges   []*noms.ReadRange
	prollyRanges []prolly.Range
	sqlRanges    sql.RangeCollection
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
	boundsCase_isNull
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

// Index implements the interface sql.IndexLookup
func (il *doltIndexLookup) Index() sql.Index {
	return il.idx
}

// Ranges implements the interface sql.IndexLookup
func (il *doltIndexLookup) Ranges() sql.RangeCollection {
	return il.sqlRanges
}

// Between returns whether the given types.Value is between the bounds. In addition, this returns if the value is outside
// the bounds and above the upperbound.
func (cb columnBounds) Between(ctx context.Context, nbf *types.NomsBinFormat, val types.Value) (ok bool, over bool, err error) {
	// Only boundCase_isNull matches NULL values,
	// otherwise we terminate the range scan.
	// This is checked early to bypass unpredictable
	// null type comparisons.
	if val.Kind() == types.NullKind {
		isNullCase := cb.boundsCase == boundsCase_isNull
		return isNullCase, !isNullCase, nil
	}

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
	case boundsCase_isNull:
		// an isNull scan skips non-nulls, but does not terminate
		return false, false, nil
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
