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

func ProllyRangesForIndex(ctx *sql.Context, index sql.Index, ranges sql.RangeCollection) ([]prolly.Range, error) {
	idx := index.(*doltIndex)
	return idx.prollyRanges(ctx, idx.ns, ranges...)
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

func RowIterForProllyRange(ctx *sql.Context, idx DoltIndex, r prolly.Range, pkSch sql.PrimaryKeySchema, projections []uint64, durableState *durableIndexState) (sql.RowIter, error) {
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
	nrr := noms.NewNomsRangeReader(idx.valueReadWriter(), idx.IndexSchema(), m, ranges)

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
		isReverse:    lookup.IsReverse,
	}, nil
}

func newPointPartitionIter(ctx *sql.Context, lookup sql.IndexLookup, idx *doltIndex) (sql.PartitionIter, error) {
	prollyRanges, err := idx.prollyRanges(ctx, idx.ns, lookup.Ranges[0])
	if err != nil {
		return nil, err
	}
	return &pointPartition{
		r: prollyRanges[0],
	}, nil
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
	isReverse    bool
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
		isReverse:   itr.isReverse,
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
		isReverse: itr.isReverse,
	}, nil
}

type rangePartition struct {
	nomsRange   *noms.ReadRange
	prollyRange prolly.Range
	key         []byte
	isReverse   bool
}

func (rp rangePartition) Key() []byte {
	return rp.key
}

// IndexScanBuilder generates secondary lookups for partitions and
// encapsulates fast path optimizations for certain point lookups.
type IndexScanBuilder interface {
	// NewPartitionRowIter returns a sql.RowIter for an index partition.
	NewPartitionRowIter(ctx *sql.Context, part sql.Partition) (sql.RowIter, error)

	// NewRangeMapIter returns a prolly.MapIter for an index partition.
	NewRangeMapIter(ctx context.Context, r prolly.Range, reverse bool) (prolly.MapIter, error)

	// NewSecondaryIter returns an object used to perform secondary lookups
	// for index joins.
	NewSecondaryIter(strict bool, cnt int, nullSafe []bool) SecondaryLookupIterGen

	// Key returns the table root for caching purposes
	Key() doltdb.DataCacheKey
}

func NewIndexReaderBuilder(
	ctx *sql.Context,
	tab DoltTableable,
	idx DoltIndex,
	key doltdb.DataCacheKey,
	projections []uint64,
	pkSch sql.PrimaryKeySchema,
	isDoltFormat bool,
) (IndexScanBuilder, error) {
	if projections == nil {
		projections = idx.Schema().GetAllCols().Tags
	}

	di := idx.(*doltIndex)
	s, err := di.getDurableState(ctx, tab)
	if err != nil {
		return nil, err
	}
	base := &baseIndexImplBuilder{
		idx:         di,
		key:         key,
		sch:         pkSch,
		projections: projections,
	}

	if isDoltFormat {
		base.sec = durable.ProllyMapFromIndex(s.Secondary)
		base.secKd, base.secVd = base.sec.Descriptors()
		base.ns = base.sec.NodeStore()
		base.prefDesc = base.secKd.PrefixDesc(len(di.columns))
	}

	switch {
	case !isDoltFormat:
		return &nomsIndexImplBuilder{
			baseIndexImplBuilder: base,
			s:                    s,
		}, nil
	case sql.IsKeyless(pkSch.Schema):
		return &keylessIndexImplBuilder{
			baseIndexImplBuilder: base,
			s:                    s,
		}, nil
	case idx.coversColumns(s, projections):
		return newCoveringLookupBuilder(base), nil
	case idx.ID() == "PRIMARY":
		// If we are using the primary index, always use a covering lookup builder. In some cases, coversColumns
		// can return false, for example if a column was modified in an older version and has a different tag than
		// the current schema. In those cases, the primary index is still the best we have, so go ahead and use it.
		return newCoveringLookupBuilder(base), nil
	default:
		return newNonCoveringLookupBuilder(s, base)
	}
}

func newCoveringLookupBuilder(b *baseIndexImplBuilder) *coveringIndexImplBuilder {
	var keyMap, valMap, ordMap val.OrdinalMapping
	if b.idx.IsPrimaryKey() {
		keyMap, valMap, ordMap = primaryIndexMapping(b.idx, b.projections)
	} else {
		keyMap, ordMap = coveringIndexMapping(b.idx, b.projections)
	}
	return &coveringIndexImplBuilder{
		baseIndexImplBuilder: b,
		keyMap:               keyMap,
		valMap:               valMap,
		ordMap:               ordMap,
	}
}

// newNonCoveringLookupBuilder returns a IndexScanBuilder that uses the specified index state and
// base lookup builder to create a nonCoveringIndexImplBuilder that uses the secondary index (from
// |b|) to find the PK row identifier, and then uses that PK to look up the complete row from
// the primary index (from |s|). If a baseIndexImplBuilder built on the primary index is passed in,
// this function returns an error.
func newNonCoveringLookupBuilder(s *durableIndexState, b *baseIndexImplBuilder) (*nonCoveringIndexImplBuilder, error) {
	if b.idx.ID() == "PRIMARY" {
		return nil, fmt.Errorf("incompatible index passed to newNonCoveringLookupBuilder: " +
			"primary index passed, but only secondary indexes are supported")
	}

	primary := durable.ProllyMapFromIndex(s.Primary)
	priKd, _ := primary.Descriptors()
	tbBld := val.NewTupleBuilder(priKd)
	pkMap := ordinalMappingFromIndex(b.idx)
	keyProj, valProj, ordProj := projectionMappings(b.idx.Schema(), b.projections)
	return &nonCoveringIndexImplBuilder{
		baseIndexImplBuilder: b,
		pri:                  primary,
		priKd:                priKd,
		pkBld:                tbBld,
		pkMap:                pkMap,
		keyMap:               keyProj,
		valMap:               valProj,
		ordMap:               ordProj,
	}, nil
}

var _ IndexScanBuilder = (*baseIndexImplBuilder)(nil)
var _ IndexScanBuilder = (*nomsIndexImplBuilder)(nil)
var _ IndexScanBuilder = (*coveringIndexImplBuilder)(nil)
var _ IndexScanBuilder = (*keylessIndexImplBuilder)(nil)
var _ IndexScanBuilder = (*nonCoveringIndexImplBuilder)(nil)

// baseIndexImplBuilder is a common lookup builder for prolly covering and
// non covering index lookups.
type baseIndexImplBuilder struct {
	key doltdb.DataCacheKey

	idx         *doltIndex
	sch         sql.PrimaryKeySchema
	projections []uint64

	sec          prolly.Map
	secKd, secVd val.TupleDesc
	prefDesc     val.TupleDesc
	ns           tree.NodeStore
}

func (ib *baseIndexImplBuilder) Key() doltdb.DataCacheKey {
	return ib.key
}

// NewPartitionRowIter implements IndexScanBuilder
func (ib *baseIndexImplBuilder) NewPartitionRowIter(_ *sql.Context, _ sql.Partition) (sql.RowIter, error) {
	panic("cannot call NewRowIter on baseIndexImplBuilder")
}

// NewRangeMapIter implements IndexScanBuilder
func (ib *baseIndexImplBuilder) NewRangeMapIter(_ context.Context, _ prolly.Range, reverse bool) (prolly.MapIter, error) {
	panic("cannot call NewMapIter on baseIndexImplBuilder")
}

func (ib *baseIndexImplBuilder) NewSecondaryIter(strict bool, cnt int, nullSafe []bool) SecondaryLookupIterGen {
	panic("cannot call NewSecondaryIter on baseIndexImplBuilder")
}

// newPointLookup will create a cursor once, and then use the same cursor for
// every subsequent point lookup. Note that equality joins can have a mix of
// point lookups on concrete values, and range lookups for null matches.
func (ib *baseIndexImplBuilder) newPointLookup(ctx *sql.Context, rang prolly.Range) (iter prolly.MapIter, err error) {
	err = ib.sec.GetPrefix(ctx, rang.Tup, ib.prefDesc, func(key val.Tuple, value val.Tuple) (err error) {
		if key != nil && rang.Matches(key) {
			iter = prolly.NewPointLookup(key, value)
		} else {
			iter = prolly.EmptyPointLookup
		}
		return
	})
	return
}

func (ib *baseIndexImplBuilder) rangeIter(ctx *sql.Context, part sql.Partition) (prolly.MapIter, error) {
	switch p := part.(type) {
	case pointPartition:
		return ib.newPointLookup(ctx, p.r)
	case rangePartition:
		if p.isReverse {
			return ib.sec.IterRangeReverse(ctx, p.prollyRange)
		} else {
			return ib.sec.IterRange(ctx, p.prollyRange)
		}
	default:
		panic(fmt.Sprintf("unexpected prolly partition type: %T", part))
	}
}

// coveringIndexImplBuilder constructs row iters for covering lookups,
// where we only need to cursor seek on a single index to both identify
// target keys and fill all requested projections
type coveringIndexImplBuilder struct {
	*baseIndexImplBuilder

	// keyMap transforms secondary index key tuples into SQL tuples.
	// secondary index value tuples are assumed to be empty.
	keyMap, valMap, ordMap val.OrdinalMapping
}

func NewSequenceMapIter(ctx context.Context, ib IndexScanBuilder, ranges []prolly.Range, reverse bool) (prolly.MapIter, error) {
	cur, err := ib.NewRangeMapIter(ctx, ranges[0], reverse)
	if err != nil || len(ranges) < 2 {
		return cur, err
	}
	return &sequenceRangeIter{
		cur:             cur,
		ib:              ib,
		reverse:         reverse,
		remainingRanges: ranges[1:],
	}, nil

}

// sequenceRangeIter iterates a list of ranges into
// an underlying map.
type sequenceRangeIter struct {
	cur             prolly.MapIter
	ib              IndexScanBuilder
	reverse         bool
	remainingRanges []prolly.Range
}

var _ prolly.MapIter = (*sequenceRangeIter)(nil)

// Next implements prolly.MapIter
func (i *sequenceRangeIter) Next(ctx context.Context) (val.Tuple, val.Tuple, error) {
	k, v, err := i.cur.Next(ctx)
	if err == io.EOF {
		if len(i.remainingRanges) == 0 {
			return nil, nil, io.EOF
		}
		i.cur, err = i.ib.NewRangeMapIter(ctx, i.remainingRanges[0], i.reverse)
		if err != nil {
			return nil, nil, err
		}
		i.remainingRanges = i.remainingRanges[1:]
		return i.Next(ctx)
	}
	return k, v, nil
}

// NewRangeMapIter implements IndexScanBuilder
func (ib *coveringIndexImplBuilder) NewRangeMapIter(ctx context.Context, r prolly.Range, reverse bool) (prolly.MapIter, error) {
	if reverse {
		return ib.sec.IterRangeReverse(ctx, r)
	} else {
		return ib.sec.IterRange(ctx, r)
	}
}

// NewPartitionRowIter implements IndexScanBuilder
func (ib *coveringIndexImplBuilder) NewPartitionRowIter(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	rangeIter, err := ib.rangeIter(ctx, part)
	if err != nil {
		return nil, err
	}
	return prollyCoveringIndexIter{
		idx:         ib.idx,
		indexIter:   rangeIter,
		keyDesc:     ib.secKd,
		valDesc:     ib.secVd,
		keyMap:      ib.keyMap,
		valMap:      ib.valMap,
		ordMap:      ib.ordMap,
		sqlSch:      ib.sch.Schema,
		projections: ib.projections,
		ns:          ib.ns,
	}, nil
}

// NewSecondaryIter implements IndexScanBuilder
func (ib *coveringIndexImplBuilder) NewSecondaryIter(strict bool, cnt int, nullSafe []bool) SecondaryLookupIterGen {
	if strict {
		return &covStrictSecondaryLookupGen{m: ib.sec, prefixDesc: ib.secKd.PrefixDesc(cnt), index: ib.idx}
	} else {
		return &covLaxSecondaryLookupGen{m: ib.sec, prefixDesc: ib.secKd.PrefixDesc(cnt), index: ib.idx, nullSafe: nullSafe}
	}
}

// nonCoveringIndexImplBuilder constructs row iters for non-covering lookups,
// where we need to seek on the secondary table for key identity, and then
// the primary table to fill all requested projections.
type nonCoveringIndexImplBuilder struct {
	*baseIndexImplBuilder

	pri   prolly.Map
	priKd val.TupleDesc
	pkBld *val.TupleBuilder

	pkMap, keyMap, valMap, ordMap val.OrdinalMapping
}

type nonCoveringMapIter struct {
	indexIter prolly.MapIter
	primary   prolly.Map
	pkMap     val.OrdinalMapping
	pkBld     *val.TupleBuilder
}

var _ prolly.MapIter = (*nonCoveringMapIter)(nil)

// Next implements prolly.MapIter
func (i *nonCoveringMapIter) Next(ctx context.Context) (val.Tuple, val.Tuple, error) {
	idxKey, _, err := i.indexIter.Next(ctx)
	if err != nil {
		return nil, nil, err
	}
	if idxKey == nil {
		return nil, nil, nil
	}
	for to := range i.pkMap {
		from := i.pkMap.MapOrdinal(to)
		i.pkBld.PutRaw(to, idxKey.GetField(from))
	}
	pk := i.pkBld.Build(sharePool)

	var value val.Tuple
	err = i.primary.Get(ctx, pk, func(_, v val.Tuple) error {
		value = v
		return nil
	})
	return pk, value, nil
}

// NewRangeMapIter implements IndexScanBuilder
func (ib *nonCoveringIndexImplBuilder) NewRangeMapIter(ctx context.Context, r prolly.Range, reverse bool) (prolly.MapIter, error) {
	var secIter prolly.MapIter
	var err error
	if reverse {
		secIter, err = ib.sec.IterRangeReverse(ctx, r)
	} else {
		secIter, err = ib.sec.IterRange(ctx, r)
	}
	if err != nil {
		return nil, err
	}

	return &nonCoveringMapIter{
		indexIter: secIter,
		primary:   ib.pri,
		pkBld:     ib.pkBld,
		pkMap:     ib.pkMap,
	}, nil
}

// NewPartitionRowIter implements IndexScanBuilder
func (ib *nonCoveringIndexImplBuilder) NewPartitionRowIter(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	rangeIter, err := ib.rangeIter(ctx, part)
	if err != nil {
		return nil, err
	}
	return prollyIndexIter{
		idx:         ib.idx,
		indexIter:   rangeIter,
		primary:     ib.pri,
		pkBld:       ib.pkBld,
		pkMap:       ib.pkMap,
		keyMap:      ib.keyMap,
		valMap:      ib.valMap,
		ordMap:      ib.ordMap,
		sqlSch:      ib.sch.Schema,
		projections: ib.projections,
	}, nil
}

func (ib *nonCoveringIndexImplBuilder) NewSecondaryIter(strict bool, cnt int, nullSafe []bool) SecondaryLookupIterGen {
	if strict {
		return &nonCovStrictSecondaryLookupGen{pri: ib.pri, sec: ib.sec, pkMap: ib.pkMap, pkBld: ib.pkBld, sch: ib.idx.tableSch, prefixDesc: ib.secKd.PrefixDesc(cnt)}
	} else {
		return &nonCovLaxSecondaryLookupGen{pri: ib.pri, sec: ib.sec, pkMap: ib.pkMap, pkBld: ib.pkBld, sch: ib.idx.tableSch, prefixDesc: ib.secKd.PrefixDesc(cnt), nullSafe: nullSafe}
	}
}

// TODO keylessIndexImplBuilder should be similar to the non-covering
// index case, where we will need to reference the primary index,
// but can take advantage of point lookup optimizations
type keylessIndexImplBuilder struct {
	*baseIndexImplBuilder
	s *durableIndexState
}

// IndexScanBuilder implements IndexScanBuilder
func (ib *keylessIndexImplBuilder) NewRangeMapIter(ctx context.Context, r prolly.Range, reverse bool) (prolly.MapIter, error) {
	rows := ib.s.Primary
	dsecondary := ib.s.Secondary
	secondary := durable.ProllyMapFromIndex(dsecondary)
	indexIter, err := secondary.IterRange(ctx, r)
	if err != nil {
		return nil, err
	}
	clustered := durable.ProllyMapFromIndex(rows)
	keyDesc := clustered.KeyDesc()
	indexMap := ordinalMappingFromIndex(ib.idx)

	keyBld := val.NewTupleBuilder(keyDesc)
	return &keylessMapIter{
		indexIter:    indexIter,
		clustered:    clustered,
		clusteredMap: indexMap,
		clusteredBld: keyBld,
	}, nil
}

type keylessMapIter struct {
	indexIter prolly.MapIter
	clustered prolly.Map
	// clusteredMap transforms secondary index keys
	// into clustered index keys
	clusteredMap val.OrdinalMapping
	clusteredBld *val.TupleBuilder
}

var _ prolly.MapIter = (*keylessMapIter)(nil)

// Next implements prolly.MapIter
func (i *keylessMapIter) Next(ctx context.Context) (val.Tuple, val.Tuple, error) {
	idxKey, _, err := i.indexIter.Next(ctx)
	if err != nil {
		return nil, nil, err
	}

	for to := range i.clusteredMap {
		from := i.clusteredMap.MapOrdinal(to)
		i.clusteredBld.PutRaw(to, idxKey.GetField(from))
	}
	pk := i.clusteredBld.Build(sharePool)

	var value val.Tuple
	err = i.clustered.Get(ctx, pk, func(k, v val.Tuple) error {
		value = v
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return pk, value, nil
}

// NewPartitionRowIter implements IndexScanBuilder
func (ib *keylessIndexImplBuilder) NewPartitionRowIter(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	var prollyRange prolly.Range
	switch p := part.(type) {
	case rangePartition:
		prollyRange = p.prollyRange
	case pointPartition:
		prollyRange = p.r
	}
	return newProllyKeylessIndexIter(ctx, ib.idx, prollyRange, ib.sch, ib.projections, ib.s.Primary, ib.s.Secondary)
}

func (ib *keylessIndexImplBuilder) NewSecondaryIter(strict bool, cnt int, nullSafe []bool) SecondaryLookupIterGen {
	pri := durable.ProllyMapFromIndex(ib.s.Primary)
	pkDesc, _ := pri.Descriptors()
	pkBld := val.NewTupleBuilder(pkDesc)

	secondary := durable.ProllyMapFromIndex(ib.s.Secondary)

	return &keylessSecondaryLookupGen{
		pri:        pri,
		sec:        secondary,
		sch:        ib.idx.tableSch,
		pkMap:      ordinalMappingFromIndex(ib.idx),
		pkBld:      pkBld,
		prefixDesc: secondary.KeyDesc().PrefixDesc(cnt),
	}
}

type nomsIndexImplBuilder struct {
	*baseIndexImplBuilder
	s *durableIndexState
}

// NewPartitionRowIter implements IndexScanBuilder
func (ib *nomsIndexImplBuilder) NewPartitionRowIter(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	p := part.(rangePartition)
	ranges := []*noms.ReadRange{p.nomsRange}
	return RowIterForNomsRanges(ctx, ib.idx, ranges, ib.projections, ib.s)
}

// NewRangeMapIter implements IndexScanBuilder
func (ib *nomsIndexImplBuilder) NewRangeMapIter(ctx context.Context, r prolly.Range, reverse bool) (prolly.MapIter, error) {
	panic("cannot call NewMapIter on *nomsIndexImplBuilder")
}

func (ib *nomsIndexImplBuilder) NewSecondaryIter(strict bool, cnt int, nullSafe []bool) SecondaryLookupIterGen {
	panic("cannot call NewSecondaryIter on *nomsIndexImplBuilder")
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
func (cb columnBounds) Between(ctx context.Context, vr types.ValueReader, val types.Value) (ok bool, over bool, err error) {
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
		ok, err := cb.upperbound.Less(ctx, vr.Format(), val)
		if err != nil || ok {
			return false, true, err
		}
	case boundsCase_infinity_less:
		ok, err := val.Less(ctx, vr.Format(), cb.upperbound)
		if err != nil || !ok {
			return false, true, err
		}
	case boundsCase_greaterEquals_infinity:
		ok, err := val.Less(ctx, vr.Format(), cb.lowerbound)
		if err != nil || ok {
			return false, false, err
		}
	case boundsCase_greaterEquals_lessEquals:
		ok, err := val.Less(ctx, vr.Format(), cb.lowerbound)
		if err != nil || ok {
			return false, false, err
		}
		ok, err = cb.upperbound.Less(ctx, vr.Format(), val)
		if err != nil || ok {
			return false, true, err
		}
	case boundsCase_greaterEquals_less:
		ok, err := val.Less(ctx, vr.Format(), cb.lowerbound)
		if err != nil || ok {
			return false, false, err
		}
		ok, err = val.Less(ctx, vr.Format(), cb.upperbound)
		if err != nil || !ok {
			return false, true, err
		}
	case boundsCase_greater_infinity:
		ok, err := cb.lowerbound.Less(ctx, vr.Format(), val)
		if err != nil || !ok {
			return false, false, err
		}
	case boundsCase_greater_lessEquals:
		ok, err := cb.lowerbound.Less(ctx, vr.Format(), val)
		if err != nil || !ok {
			return false, false, err
		}
		ok, err = cb.upperbound.Less(ctx, vr.Format(), val)
		if err != nil || ok {
			return false, true, err
		}
	case boundsCase_greater_less:
		ok, err := cb.lowerbound.Less(ctx, vr.Format(), val)
		if err != nil || !ok {
			return false, false, err
		}
		ok, err = val.Less(ctx, vr.Format(), cb.upperbound)
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
func (nrc nomsRangeCheck) Check(ctx context.Context, vr types.ValueReader, tuple types.Tuple) (valid bool, skip bool, err error) {
	itr := types.TupleItrPool.Get().(*types.TupleIterator)
	defer types.TupleItrPool.Put(itr)
	err = itr.InitForTuple(tuple)
	if err != nil {
		return false, false, err
	}

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

		ok, over, err := nrc[i].Between(ctx, vr, val)
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
