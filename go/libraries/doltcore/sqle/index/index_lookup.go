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

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

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
	if projections == nil {
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
	if lookup.IsPointLookup && isDoltFmt {
		return newPointPartitionIter(ctx, lookup, idx)
	}

	var prollyRanges []prolly.Range
	var nomsRanges []*noms.ReadRange
	var err error
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
		isDoltFmt:    isDoltFmt,
	}, nil
}

func newPointPartitionIter(ctx *sql.Context, lookup sql.IndexLookup, idx *doltIndex) (sql.PartitionIter, error) {
	tb := idx.keyBld
	rng := lookup.Ranges[0]
	ns := idx.ns
	for j, expr := range rng {
		v, err := getRangeCutValue(expr.LowerBound, rng[j].Typ)
		if err != nil {
			return nil, err
		}
		if err = PutField(ctx, ns, tb, j, v); err != nil {
			return nil, err
		}
	}
	tup := tb.BuildPermissive(sharePool)
	return &pointPartition{r: prolly.Range{Tup: tup, Desc: tb.Desc}}, nil
}

var _ sql.PartitionIter = (*pointPartition)(nil)
var _ sql.Partition = (*pointPartition)(nil)

type pointPartition struct {
	r    prolly.Range
	used bool
}

func (p pointPartition) Key() []byte {
	return []byte{0}
}

func (p *pointPartition) Close(c *sql.Context) error {
	return nil
}

func (p *pointPartition) Next(c *sql.Context) (sql.Partition, error) {
	if p.used {
		return nil, io.EOF
	}
	p.used = true
	return *p, nil
}

type rangePartitionIter struct {
	nomsRanges   []*noms.ReadRange
	prollyRanges []prolly.Range
	curr         int
	isDoltFmt    bool
}

// Close is required by the sql.PartitionIter interface. Does nothing.
func (itr *rangePartitionIter) Close(*sql.Context) error {
	return nil
}

// Next returns the next partition if there is one, or io.EOF if there isn't.
func (itr *rangePartitionIter) Next(_ *sql.Context) (sql.Partition, error) {
	if itr.isDoltFmt {
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
	}, nil
}

type rangePartition struct {
	nomsRange   *noms.ReadRange
	prollyRange prolly.Range
	key         []byte
}

func (rp rangePartition) Key() []byte {
	return rp.key
}

// LookupBuilder generates secondary lookups for partitions and
// encapsulates fast path optimizations for certain point lookups.
type LookupBuilder interface {
	// NewRowIter returns a new index iter for the given partition
	NewRowIter(ctx *sql.Context, part sql.Partition) (sql.RowIter, error)
	Key() doltdb.DataCacheKey
}

func NewLookupBuilder(
	ctx *sql.Context,
	tab DoltTableable,
	idx DoltIndex,
	key doltdb.DataCacheKey,
	projections []uint64,
	pkSch sql.PrimaryKeySchema,
	isDoltFormat bool,
) (LookupBuilder, error) {
	if projections == nil {
		projections = idx.Schema().GetAllCols().Tags
	}

	di := idx.(*doltIndex)
	s, err := di.getDurableState(ctx, tab)
	if err != nil {
		return nil, err
	}
	base := &baseLookupBuilder{
		idx:         di,
		key:         key,
		sch:         pkSch,
		projections: projections,
	}

	if isDoltFormat {
		base.sec = durable.ProllyMapFromIndex(s.Secondary)
		base.secKd, base.secVd = base.sec.Descriptors()
		base.ns = base.sec.NodeStore()
	}

	switch {
	case !isDoltFormat:
		return &nomsLookupBuilder{
			baseLookupBuilder: base,
			s:                 s,
		}, nil
	case sql.IsKeyless(pkSch.Schema):
		return &keylessLookupBuilder{
			baseLookupBuilder: base,
			s:                 s,
		}, nil
	case idx.coversColumns(s, projections):
		return newCoveringLookupBuilder(base), nil
	default:
		return newNonCoveringLookupBuilder(s, base), nil
	}
}

func newCoveringLookupBuilder(b *baseLookupBuilder) *coveringLookupBuilder {
	var keyMap, valMap, ordMap val.OrdinalMapping
	if b.idx.IsPrimaryKey() {
		keyMap, valMap, ordMap = primaryIndexMapping(b.idx, b.sch, b.projections)
	} else {
		keyMap, ordMap = coveringIndexMapping(b.idx, b.projections)
	}
	return &coveringLookupBuilder{
		baseLookupBuilder: b,
		keyMap:            keyMap,
		valMap:            valMap,
		ordMap:            ordMap,
	}
}

func newNonCoveringLookupBuilder(s *durableIndexState, b *baseLookupBuilder) *nonCoveringLookupBuilder {
	primary := durable.ProllyMapFromIndex(s.Primary)
	priKd, _ := primary.Descriptors()
	tbBld := val.NewTupleBuilder(priKd)
	pkMap := ordinalMappingFromIndex(b.idx)
	keyProj, valProj, ordProj := projectionMappings(b.idx.Schema(), b.projections)
	return &nonCoveringLookupBuilder{
		baseLookupBuilder: b,
		pri:               primary,
		priKd:             priKd,
		pkBld:             tbBld,
		pkMap:             pkMap,
		keyMap:            keyProj,
		valMap:            valProj,
		ordMap:            ordProj,
	}
}

var _ LookupBuilder = (*baseLookupBuilder)(nil)
var _ LookupBuilder = (*nomsLookupBuilder)(nil)
var _ LookupBuilder = (*coveringLookupBuilder)(nil)
var _ LookupBuilder = (*keylessLookupBuilder)(nil)
var _ LookupBuilder = (*nonCoveringLookupBuilder)(nil)

// baseLookupBuilder is a common lookup builder for prolly covering and
// non covering index lookups.
type baseLookupBuilder struct {
	key doltdb.DataCacheKey

	idx         *doltIndex
	sch         sql.PrimaryKeySchema
	projections []uint64

	sec          prolly.Map
	secKd, secVd val.TupleDesc
	ns           tree.NodeStore

	cur *tree.Cursor
}

func (lb *baseLookupBuilder) Key() doltdb.DataCacheKey {
	return lb.key
}

// NewRowIter implements IndexLookup
func (lb *baseLookupBuilder) NewRowIter(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	panic("cannot call NewRowIter on baseLookupBuilder")
}

// newPointLookup will create a cursor once, and then use the same cursor for
// every subsequent point lookup. Note that equality joins can have a mix of
// point lookups on concrete values, and range lookups for null matches.
func (lb *baseLookupBuilder) newPointLookup(ctx *sql.Context, rang prolly.Range) (prolly.MapIter, error) {
	if lb.cur == nil {
		cur, err := tree.NewCursorAtKey(ctx, lb.sec.NodeStore(), lb.sec.Node(), rang.Tup, lb.secKd)
		if err != nil {
			return nil, err
		}
		if !cur.Valid() {
			// map does not contain |rng|
			return prolly.EmptyPointLookup, nil
		}

		lb.cur = cur
	}

	err := tree.Seek(ctx, lb.cur, rang.Tup, lb.secKd)
	if err != nil {
		return nil, err
	}
	if !lb.cur.Valid() {
		return prolly.EmptyPointLookup, nil
	}

	key := val.Tuple(lb.cur.CurrentKey())
	value := val.Tuple(lb.cur.CurrentValue())

	if !rang.Matches(key) {
		return prolly.EmptyPointLookup, nil
	}

	return prolly.NewPointLookup(key, value), nil
}

func (lb *baseLookupBuilder) rangeIter(ctx *sql.Context, part sql.Partition) (prolly.MapIter, error) {
	switch p := part.(type) {
	case pointPartition:
		return lb.newPointLookup(ctx, p.r)
	case rangePartition:
		return lb.sec.IterRange(ctx, p.prollyRange)
	default:
		panic(fmt.Sprintf("unexpected prolly partition type: %T", part))
	}
}

// coveringLookupBuilder constructs row iters for covering lookups,
// where we only need to cursor seek on a single index to both identify
// target keys and fill all requested projections
type coveringLookupBuilder struct {
	*baseLookupBuilder

	// keyMap transforms secondary index key tuples into SQL tuples.
	// secondary index value tuples are assumed to be empty.
	keyMap, valMap, ordMap val.OrdinalMapping
}

// NewRowIter implements IndexLookup
func (lb *coveringLookupBuilder) NewRowIter(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	rangeIter, err := lb.rangeIter(ctx, part)
	if err != nil {
		return nil, err
	}
	return prollyCoveringIndexIter{
		idx:       lb.idx,
		indexIter: rangeIter,
		keyDesc:   lb.secKd,
		valDesc:   lb.secVd,
		keyMap:    lb.keyMap,
		valMap:    lb.valMap,
		ordMap:    lb.ordMap,
		sqlSch:    lb.sch.Schema,
		ns:        lb.ns,
	}, nil
}

// nonCoveringLookupBuilder constructs row iters for non-covering lookups,
// where we need to seek on the secondary table for key identity, and then
// the primary table to fill all requrested projections.
type nonCoveringLookupBuilder struct {
	*baseLookupBuilder

	pri   prolly.Map
	priKd val.TupleDesc
	pkBld *val.TupleBuilder

	pkMap, keyMap, valMap, ordMap val.OrdinalMapping
}

// NewRowIter implements IndexLookup
func (lb *nonCoveringLookupBuilder) NewRowIter(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	rangeIter, err := lb.rangeIter(ctx, part)
	if err != nil {
		return nil, err
	}
	return prollyIndexIter{
		idx:       lb.idx,
		indexIter: rangeIter,
		primary:   lb.pri,
		pkBld:     lb.pkBld,
		pkMap:     lb.pkMap,
		keyMap:    lb.keyMap,
		valMap:    lb.valMap,
		ordMap:    lb.ordMap,
		sqlSch:    lb.sch.Schema,
	}, nil
}

// TODO keylessLookupBuilder should be similar to the non-covering
// index case, where we will need to reference the primary index,
// but can take advantage of point lookup optimizations
type keylessLookupBuilder struct {
	*baseLookupBuilder
	s *durableIndexState
}

// NewRowIter implements IndexLookup
func (lb *keylessLookupBuilder) NewRowIter(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	var prollyRange prolly.Range
	switch p := part.(type) {
	case rangePartition:
		prollyRange = p.prollyRange
	case pointPartition:
		prollyRange = p.r
	}
	return newProllyKeylessIndexIter(ctx, lb.idx, prollyRange, lb.sch, lb.projections, lb.s.Primary, lb.s.Secondary)
}

type nomsLookupBuilder struct {
	*baseLookupBuilder
	s *durableIndexState
}

// NewRowIter implements IndexLookup
func (lb *nomsLookupBuilder) NewRowIter(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	p := part.(rangePartition)
	ranges := []*noms.ReadRange{p.nomsRange}
	return RowIterForNomsRanges(ctx, lb.idx, ranges, lb.projections, lb.s)
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
