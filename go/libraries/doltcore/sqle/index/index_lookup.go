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
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
	"io"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/types"
)

func PartitionIndexedTableRows(ctx *sql.Context, idx sql.Index, part sql.Partition, pkSch sql.PrimaryKeySchema, columns []uint64) (sql.RowIter, error) {
	rp := part.(rangePartition)
	doltIdx := idx.(DoltIndex)

	if types.IsFormat_DOLT(rp.durableState.Primary.Format()) {
		return RowIterForProllyRange(ctx, doltIdx, rp.prollyRange, pkSch, columns, rp.durableState)
	}

	ranges := []*noms.ReadRange{rp.nomsRange}
	return RowIterForNomsRanges(ctx, doltIdx, ranges, columns, rp.durableState)
}

func RowIterForIndexLookup(ctx *sql.Context, t DoltTableable, lookup sql.IndexLookup, pkSch sql.PrimaryKeySchema, columns []uint64) (sql.RowIter, error) {
	idx := lookup.Index.(*doltIndex)
	durableState, err := idx.getDurableState(ctx, t)
	if err != nil {
		return nil, err
	}

	if types.IsFormat_DOLT(idx.Format()) {
		prollyRanges, err := idx.prollyRanges(ctx, idx.ns, lookup.Ranges...)
		if len(prollyRanges) > 1 {
			return nil, fmt.Errorf("expected a single index range")
		}
		if err != nil {
			return nil, err
		}
		return RowIterForProllyRange(ctx, idx, prollyRanges[0], pkSch, columns, durableState)
	} else {
		nomsRanges, err := idx.nomsRanges(ctx, lookup.Ranges...)
		if err != nil {
			return nil, err
		}
		return RowIterForNomsRanges(ctx, idx, nomsRanges, columns, durableState)
	}
}

func RowIterForProllyRange(ctx *sql.Context, idx DoltIndex, r prolly.Range, pkSch sql.PrimaryKeySchema, projections []uint64, durableState *durableIndexState) (sql.RowIter2, error) {
	if len(projections) == 0 {
		projections = idx.Schema().GetAllCols().Tags
	}

	if sql.IsKeyless(pkSch.Schema) {
		// in order to resolve row cardinality, keyless indexes must always perform
		// an indirect lookup through the clustered index.
		return newProllyKeylessIndexIter(ctx, idx, r, pkSch, projections, durableState.Primary, durableState.Secondary)
	}

	covers := idx.coversColumns(durableState, projections)
	if covers {
		return newProllyCoveringIndexIter(ctx, idx, r, pkSch, projections, durableState.Secondary)
	}
	return newProllyIndexIter(ctx, idx, r, pkSch, projections, durableState.Primary, durableState.Secondary)
}

func RowIterForNomsRanges(ctx *sql.Context, idx DoltIndex, ranges []*noms.ReadRange, columns []uint64, durableState *durableIndexState) (sql.RowIter, error) {
	if len(columns) == 0 {
		columns = idx.Schema().GetAllCols().Tags
	}
	m := durable.NomsMapFromIndex(durableState.Secondary)
	nrr := noms.NewNomsRangeReader(idx.IndexSchema(), m, ranges)

	covers := idx.coversColumns(durableState, columns)
	if covers || idx.ID() == "PRIMARY" {
		return NewCoveringIndexRowIterAdapter(ctx, idx, nrr, columns), nil
	} else {
		return NewIndexLookupRowIterAdapter(ctx, idx, durableState, nrr, columns)
	}
}

type IndexLookupKeyIterator interface {
	// NextKey returns the next key if it exists, and io.EOF if it does not.
	NextKey(ctx *sql.Context) (row.TaggedValues, error)
}

func NewRangePartitionIter(ctx *sql.Context, t DoltTableable, lookup sql.IndexLookup, isDoltFmt bool) (sql.PartitionIter, error) {
	idx := lookup.Index.(*doltIndex)
	durableState, err := idx.getDurableState(ctx, t)
	if err != nil {
		return nil, err
	}
	var prollyRanges []prolly.Range
	var nomsRanges []*noms.ReadRange
	if isDoltFmt {
		prollyRanges, err = idx.prollyRanges(ctx, idx.ns, lookup.Ranges...)
	} else {
		nomsRanges, err = idx.nomsRanges(ctx, lookup.Ranges...)
	}
	if err != nil {
		return nil, err
	}
	return &rangePartitionIter{
		nomsRanges:   nomsRanges,
		prollyRanges: prollyRanges,
		curr:         0,
		durableState: durableState,
	}, nil
}

type rangePartitionIter struct {
	nomsRanges   []*noms.ReadRange
	prollyRanges []prolly.Range
	curr         int
	durableState *durableIndexState
}

// Close is required by the sql.PartitionIter interface. Does nothing.
func (itr *rangePartitionIter) Close(*sql.Context) error {
	return nil
}

// Next returns the next partition if there is one, or io.EOF if there isn't.
func (itr *rangePartitionIter) Next(_ *sql.Context) (sql.Partition, error) {
	if types.IsFormat_DOLT(itr.durableState.Secondary.Format()) {
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
		prollyRange:  pr,
		key:          bytes[:],
		durableState: itr.durableState,
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
		nomsRange:    nr,
		key:          bytes[:],
		durableState: itr.durableState,
	}, nil
}

type rangePartition struct {
	nomsRange    *noms.ReadRange
	prollyRange  prolly.Range
	key          []byte
	durableState *durableIndexState
}

func (rp rangePartition) Key() []byte {
	return rp.key
}

type LookupBuilder interface {
	NewRowIter(ctx *sql.Context, part sql.Partition) (sql.RowIter, error)
}

func NewLookupBuilder(part sql.Partition, idx DoltIndex, projections []uint64, pkSch sql.PrimaryKeySchema, isDoltFormat bool) LookupBuilder {
	p := part.(rangePartition)
	if len(projections) == 0 {
		projections = idx.Schema().GetAllCols().Tags
	}

	base := &baseLookupBuilder{
		idx:         idx.(*doltIndex),
		sch:         pkSch,
		projections: projections,
	}
	switch {
	case !isDoltFormat:
		return &nomsLookupBuilder{
			baseLookupBuilder: base,
		}
	case sql.IsKeyless(pkSch.Schema):
		return &keylessLookupBuilder{
			baseLookupBuilder: base,
		}
	case idx.coversColumns(p.durableState, projections) || idx.ID() == "PRIMARY":
		return &coveringLookupBuilder{
			baseLookupBuilder: base,
		}
	default:
		return base
	}
}

type baseLookupBuilder struct {
	idx         *doltIndex
	sch         sql.PrimaryKeySchema
	projections []uint64
}

type coveringLookupBuilder struct {
	*baseLookupBuilder
}

type nomsLookupBuilder struct {
	*baseLookupBuilder
}

type keylessLookupBuilder struct {
	*baseLookupBuilder
}

var _ LookupBuilder = (*baseLookupBuilder)(nil)
var _ LookupBuilder = (*nomsLookupBuilder)(nil)
var _ LookupBuilder = (*coveringLookupBuilder)(nil)
var _ LookupBuilder = (*keylessLookupBuilder)(nil)
var _ LookupBuilder = (*pointLookupBuilder)(nil)

func (lb *baseLookupBuilder) NewRowIter(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	p := part.(rangePartition)
	return newProllyIndexIter(ctx, lb.idx, p.prollyRange, lb.sch, lb.projections, p.durableState.Primary, p.durableState.Secondary)
}

func (lb *nomsLookupBuilder) NewRowIter(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	p := part.(rangePartition)
	ranges := []*noms.ReadRange{p.nomsRange}
	return RowIterForNomsRanges(ctx, lb.idx, ranges, lb.projections, p.durableState)
}

func (lb *coveringLookupBuilder) NewRowIter(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	p := part.(rangePartition)
	return newProllyCoveringIndexIter(ctx, lb.idx, p.prollyRange, lb.sch, lb.projections, p.durableState.Secondary)
}

func (lb *keylessLookupBuilder) NewRowIter(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	p := part.(rangePartition)
	return newProllyKeylessIndexIter(ctx, lb.idx, p.prollyRange, lb.sch, lb.projections, p.durableState.Primary, p.durableState.Secondary)
}

// pointLookupBuilder optimizes constructing repeated secondary point lookups
type pointLookupBuilder struct {
	*baseLookupBuilder

	// primary for default/keyless, secondary for covering
	indexData prolly.Map

	// cur for point lookup reuse
	cur                           *tree.Cursor
	keyDesc                       val.TupleDesc
	pkMap, keyMap, valMap, ordMap val.OrdinalMapping
	pkBld                         *val.TupleBuilder
}

// NewRowIter returns a new index iter for the given partition
func (lb *pointLookupBuilder) NewRowIter(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	// TODO refresh durable state and everything downstream
	// primary, secondary

	p := part.(*rangePartition)
	if !p.prollyRange.IsPointLookup(lb.keyDesc) {
		// fallback to default constructor
		return lb.baseLookupBuilder.NewRowIter(ctx, part)
	}

	rangeIter, err := lb.newPointLookup(p.prollyRange)
	if err != nil {
		return nil, err
	}

	return prollyIndexIter{
		idx:       lb.idx,
		indexIter: rangeIter,
		primary:   lb.indexData,
		pkBld:     lb.pkBld,
		pkMap:     lb.pkMap,
		keyMap:    lb.keyMap,
		valMap:    lb.valMap,
		ordMap:    lb.ordMap,
		sqlSch:    lb.sch.Schema,
	}, nil
}

func (lb *pointLookupBuilder) newPointLookup(rang prolly.Range) (prolly.MapIter, error) {
	//todo move cursor to new point
	key := val.Tuple(lb.cur.CurrentKey())
	value := val.Tuple(lb.cur.CurrentValue())

	if !rang.Matches(key) {
		return prolly.EmptyPointLookup, nil
	}

	return prolly.NewPointLookup(key, value), nil
}

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
