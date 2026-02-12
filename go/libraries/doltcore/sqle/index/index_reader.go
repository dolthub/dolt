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
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

func ProllyRangesForIndex(ctx *sql.Context, index sql.Index, ranges sql.RangeCollection) ([]prolly.Range, error) {
	idx := index.(*doltIndex)
	return idx.prollyRanges(ctx, idx.ns, ranges.(sql.MySQLRangeCollection)...)
}

func RowIterForIndexLookup(ctx *sql.Context, t DoltTableable, lookup sql.IndexLookup, pkSch sql.PrimaryKeySchema, columns []uint64) (sql.RowIter, error) {
	mysqlRanges, ok := lookup.Ranges.(sql.MySQLRangeCollection)
	if !ok {
		return nil, fmt.Errorf("expected MySQL ranges while creating row iter")
	}
	idx := lookup.Index.(*doltIndex)
	durableState, err := idx.getDurableState(ctx, t)
	if err != nil {
		return nil, err
	}

	if types.IsFormat_DOLT(idx.Format()) {
		prollyRanges, err := idx.prollyRanges(ctx, idx.ns, mysqlRanges...)
		if len(prollyRanges) > 1 {
			return nil, fmt.Errorf("expected a single index range")
		}
		if err != nil {
			return nil, err
		}
		return RowIterForProllyRange(ctx, idx, prollyRanges[0], pkSch, columns, durableState, lookup.IsReverse)
	}

	panic("Unsupported format for RowIterForIndexLookup")
}

func RowIterForProllyRange(ctx *sql.Context, idx DoltIndex, r prolly.Range, pkSch sql.PrimaryKeySchema, projections []uint64, durableState *durableIndexState, reverse bool) (sql.RowIter, error) {
	if projections == nil {
		projections = idx.Schema().GetAllCols().Tags
	}

	if sql.IsKeyless(pkSch.Schema) {
		// in order to resolve row cardinality, keyless indexes must always perform
		// an indirect lookup through the clustered index.
		return newProllyKeylessIndexIter(ctx, idx, r, pkSch, projections, durableState.Primary, durableState.Secondary, reverse)
	}

	covers := idx.coversColumns(durableState, projections)
	if covers {
		return newProllyCoveringIndexIter(ctx, idx, r, pkSch, projections, durableState.Secondary)
	}
	return newProllyIndexIter(ctx, idx, r, pkSch, projections, durableState.Primary, durableState.Secondary)
}

type IndexLookupKeyIterator interface {
	// NextKey returns the next key if it exists, and io.EOF if it does not.
	NextKey(ctx *sql.Context) (row.TaggedValues, error)
}

func NewRangePartitionIter(ctx *sql.Context, t DoltTableable, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	mysqlRanges := lookup.Ranges.(sql.MySQLRangeCollection)
	idx := lookup.Index.(*doltIndex)
	if lookup.IsPointLookup {
		return newPointPartitionIter(ctx, lookup, idx)
	}

	var prollyRanges []prolly.Range
	var err error
	prollyRanges, err = idx.prollyRanges(ctx, idx.ns, mysqlRanges...)
	if err != nil {
		return nil, err
	}
	return &rangePartitionIter{
		prollyRanges: prollyRanges,
		curr:         0,
		isReverse:    lookup.IsReverse,
	}, nil
}

func newPointPartitionIter(ctx *sql.Context, lookup sql.IndexLookup, idx *doltIndex) (sql.PartitionIter, error) {
	prollyRanges, err := idx.prollyRanges(ctx, idx.ns, lookup.Ranges.(sql.MySQLRangeCollection)[0])
	if err != nil {
		return nil, err
	}
	return &pointPartition{
		r: prollyRanges[0],
	}, nil
}

var _ sql.PartitionIter = (*pointPartition)(nil)
var _ sql.Partition = pointPartition{}

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
	prollyRanges []prolly.Range
	curr         int
	isReverse    bool
}

// Close is required by the sql.PartitionIter interface. Does nothing.
func (itr *rangePartitionIter) Close(*sql.Context) error {
	return nil
}

// Next returns the next partition if there is one, or io.EOF if there isn't.
func (itr *rangePartitionIter) Next(_ *sql.Context) (sql.Partition, error) {
	return itr.nextProllyPartition()
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

type rangePartition struct {
	nomsRange   *noms.ReadRange
	key         []byte
	prollyRange prolly.Range
	isReverse   bool
}

func (rp rangePartition) Key() []byte {
	return rp.key
}

func GetDurableIndex(ctx *sql.Context,
	tab DoltTableable,
	idx DoltIndex) (durable.Index, error) {
	di := idx.(*doltIndex)
	s, err := di.getDurableState(ctx, tab)
	if err != nil {
		return nil, err
	}
	return s.Secondary, nil
}

// vectorPartitionIter is the sql.PartitionIter for vector indexes.
// Because it only ever has one partition, it also implements sql.Partition
// and returns itself in calls to Next.
type vectorPartitionIter struct {
	Column sql.Expression
	sql.OrderAndLimit
	used bool
}

var _ sql.PartitionIter = (*vectorPartitionIter)(nil)
var _ sql.Partition = vectorPartitionIter{}

// Key returns the key used to distinguish partitions. Since it only ever has one partition,
// this value is unused.
func (v vectorPartitionIter) Key() []byte {
	return nil
}

func (v *vectorPartitionIter) Close(_ *sql.Context) error {
	return nil
}

func (v *vectorPartitionIter) Next(_ *sql.Context) (sql.Partition, error) {
	if v.used {
		return nil, io.EOF
	}
	v.used = true
	return *v, nil
}

func NewVectorPartitionIter(lookup sql.IndexLookup) (sql.PartitionIter, error) {
	return &vectorPartitionIter{
		OrderAndLimit: lookup.VectorOrderAndLimit,
	}, nil
}

// IndexScanBuilder generates secondary lookups for partitions and
// encapsulates fast path optimizations for certain point lookups.
type IndexScanBuilder interface {
	IndexRangeIterable

	// NewPartitionRowIter returns a sql.RowIter for an index partition.
	NewPartitionRowIter(ctx *sql.Context, part sql.Partition) (sql.RowIter, error)

	// NewSecondaryIter returns an object used to perform secondary lookups
	// for index joins.
	NewSecondaryIter(strict bool, cnt int, nullSafe []bool) (SecondaryLookupIterGen, error)

	// Key returns the table root for caching purposes
	Key() doltdb.DataCacheKey

	// OutputSchema returns the output KV tuple schema
	OutputSchema() schema.Schema
}

type IndexRangeIterable interface {
	// NewRangeMapIter returns a prolly.MapIter for an index partition.
	NewRangeMapIter(ctx context.Context, r prolly.Range, reverse bool) (prolly.MapIter, error)
}

func NewIndexReaderBuilder(
	ctx *sql.Context,
	tab DoltTableable,
	idx DoltIndex,
	key doltdb.DataCacheKey,
	projections []uint64,
	pkSch sql.PrimaryKeySchema,
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

	secondaryIndex := durable.MapFromIndex(s.Secondary)
	base.ns = secondaryIndex.NodeStore()
	base.secKd, base.secVd = secondaryIndex.Descriptors()
	base.prefDesc = base.secKd.PrefixDesc(len(di.columns))

	switch si := secondaryIndex.(type) {
	case prolly.Map:
		base.sec = si
	case prolly.ProximityMap:
		base.proximitySecondary = si
	default:
		return nil, fmt.Errorf("unknown index type %v", secondaryIndex)
	}

	switch {
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

	primary, err := durable.ProllyMapFromIndex(s.Primary)
	if err != nil {
		return nil, err
	}
	priKd, _ := primary.Descriptors()
	tbBld := val.NewTupleBuilder(priKd, primary.NodeStore())
	pkMap := OrdinalMappingFromIndex(b.idx)
	keyProj, valProj, ordProj := projectionMappings(b.idx.Schema(), b.projections)
	return &nonCoveringIndexImplBuilder{
		baseIndexImplBuilder: b,
		pri:                  primary,
		pkBld:                tbBld,
		pkMap:                pkMap,
		keyMap:               keyProj,
		valMap:               valProj,
		ordMap:               ordProj,
	}, nil
}

var _ IndexScanBuilder = (*baseIndexImplBuilder)(nil)
var _ IndexScanBuilder = (*coveringIndexImplBuilder)(nil)
var _ IndexScanBuilder = (*keylessIndexImplBuilder)(nil)
var _ IndexScanBuilder = (*nonCoveringIndexImplBuilder)(nil)

// baseIndexImplBuilder is a common lookup builder for prolly covering and
// non covering index lookups.
type baseIndexImplBuilder struct {
	ns                 tree.NodeStore
	idx                *doltIndex
	secKd              *val.TupleDesc
	secVd              *val.TupleDesc
	prefDesc           *val.TupleDesc
	sec                prolly.Map
	sch                sql.PrimaryKeySchema
	projections        []uint64
	proximitySecondary prolly.ProximityMap
	key                doltdb.DataCacheKey
	isProximity        bool
}

func (ib *baseIndexImplBuilder) Key() doltdb.DataCacheKey {
	return ib.key
}

func (ib *baseIndexImplBuilder) OutputSchema() schema.Schema {
	return ib.idx.IndexSchema()
}

// NewPartitionRowIter implements IndexScanBuilder
func (ib *baseIndexImplBuilder) NewPartitionRowIter(_ *sql.Context, _ sql.Partition) (sql.RowIter, error) {
	panic("cannot call NewRowIter on baseIndexImplBuilder")
}

// NewRangeMapIter implements IndexScanBuilder
func (ib *baseIndexImplBuilder) NewRangeMapIter(_ context.Context, _ prolly.Range, reverse bool) (prolly.MapIter, error) {
	panic("cannot call NewMapIter on baseIndexImplBuilder")
}

func (ib *baseIndexImplBuilder) NewSecondaryIter(strict bool, cnt int, nullSafe []bool) (SecondaryLookupIterGen, error) {
	panic("cannot call NewSecondaryIter on baseIndexImplBuilder")
}

// newPointLookup will create a cursor once, and then use the same cursor for
// every subsequent point lookup. Note that equality joins can have a mix of
// point lookups on concrete values, and range lookups for null matches.
func (ib *baseIndexImplBuilder) newPointLookup(ctx *sql.Context, rang prolly.Range) (iter prolly.MapIter, err error) {
	if ib.isProximity {
		// TODO: It should be possible to do a point lookup with a proximity index.
		return nil, fmt.Errorf("can't perform point lookup with a proximity index")
	}
	err = ib.sec.GetPrefix(ctx, rang.Tup, ib.prefDesc, func(key val.Tuple, value val.Tuple) (err error) {
		if key != nil && rang.Matches(ctx, key) {
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
		if ib.isProximity {
			return nil, fmt.Errorf("range iter not allowed for vector index")
		}
		if p.isReverse {
			return ib.sec.IterRangeReverse(ctx, p.prollyRange)
		} else {
			return ib.sec.IterRange(ctx, p.prollyRange)
		}
	case vectorPartitionIter:
		return nil, fmt.Errorf("ranger iter not allowed for vector partition")
	default:
		panic(fmt.Sprintf("unexpected prolly partition type: %T", part))
	}
}

func (ib *baseIndexImplBuilder) proximityIter(ctx *sql.Context, part vectorPartitionIter) (prolly.MapIter, error) {
	candidateVector, err := part.Literal.Eval(ctx, nil)
	if err != nil {
		return nil, err
	}
	limit, err := part.Limit.Eval(ctx, nil)
	if err != nil {
		return nil, err
	}
	return ib.proximitySecondary.GetClosest(ctx, candidateVector, int(limit.(int64)))
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

func NewSequenceRangeIter(ctx context.Context, irIter IndexRangeIterable, ranges []prolly.Range, reverse bool) (prolly.MapIter, error) {
	if len(ranges) == 0 {
		return &strictLookupIter{}, nil
	}
	// TODO: probably need to do something with Doltgres ranges here?
	cur, err := irIter.NewRangeMapIter(ctx, ranges[0], reverse)
	if err != nil || len(ranges) < 2 {
		return cur, err
	}
	return &sequenceRangeIter{
		cur:             cur,
		irIter:          irIter,
		reverse:         reverse,
		remainingRanges: ranges[1:],
	}, nil

}

// sequenceRangeIter iterates a list of ranges into
// an underlying map.
type sequenceRangeIter struct {
	cur             prolly.MapIter
	irIter          IndexRangeIterable
	remainingRanges []prolly.Range
	reverse         bool
}

var _ prolly.MapIter = (*sequenceRangeIter)(nil)

// Next implements prolly.MapIter
func (i *sequenceRangeIter) Next(ctx context.Context) (val.Tuple, val.Tuple, error) {
	k, v, err := i.cur.Next(ctx)
	if err == io.EOF {
		if len(i.remainingRanges) == 0 {
			return nil, nil, io.EOF
		}
		i.cur, err = i.irIter.NewRangeMapIter(ctx, i.remainingRanges[0], i.reverse)
		if err != nil {
			return nil, nil, err
		}
		i.remainingRanges = i.remainingRanges[1:]
		return i.Next(ctx)
	} else if err != nil {
		return nil, nil, err
	}
	return k, v, nil
}

type secIterGen struct {
	m prolly.Map
}

func NewSecondaryIterGen(m prolly.Map) IndexRangeIterable {
	return secIterGen{m: m}
}

// NewRangeMapIter implements IndexScanBuilder
func (si secIterGen) NewRangeMapIter(ctx context.Context, r prolly.Range, reverse bool) (prolly.MapIter, error) {
	if reverse {
		return si.m.IterRangeReverse(ctx, r)
	} else {
		return si.m.IterRange(ctx, r)
	}
}

func (ib *coveringIndexImplBuilder) OutputSchema() schema.Schema {
	return ib.idx.IndexSchema()
}

// NewRangeMapIter implements IndexScanBuilder
func (ib *coveringIndexImplBuilder) NewRangeMapIter(ctx context.Context, r prolly.Range, reverse bool) (prolly.MapIter, error) {
	if ib.isProximity {
		return nil, fmt.Errorf("range map iter not allowed for vector index")
	}
	if reverse {
		return ib.sec.IterRangeReverse(ctx, r)
	} else {
		return ib.sec.IterRange(ctx, r)
	}
}

// NewPartitionRowIter implements IndexScanBuilder
func (ib *coveringIndexImplBuilder) NewPartitionRowIter(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	var indexIter prolly.MapIter
	var err error
	if proximityPartition, ok := part.(vectorPartitionIter); ok {
		indexIter, err = ib.proximityIter(ctx, proximityPartition)
	} else {
		indexIter, err = ib.rangeIter(ctx, part)
	}
	if err != nil {
		return nil, err
	}
	return prollyCoveringIndexIter{
		idx:         ib.idx,
		indexIter:   indexIter,
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
func (ib *coveringIndexImplBuilder) NewSecondaryIter(strict bool, cnt int, nullSafe []bool) (SecondaryLookupIterGen, error) {
	if strict {
		return &covStrictSecondaryLookupGen{
			iter:       &strictLookupIter{},
			m:          ib.sec,
			prefixDesc: ib.secKd.PrefixDesc(cnt),
			index:      ib.idx,
		}, nil
	} else {
		return &covLaxSecondaryLookupGen{
			m:          ib.sec,
			prefixDesc: ib.secKd.PrefixDesc(cnt),
			index:      ib.idx,
			nullSafe:   nullSafe,
		}, nil
	}
}

// nonCoveringIndexImplBuilder constructs row iters for non-covering lookups,
// where we need to seek on the secondary table for key identity, and then
// the primary table to fill all requested projections.
type nonCoveringIndexImplBuilder struct {
	*baseIndexImplBuilder

	pri   prolly.Map
	pkBld *val.TupleBuilder

	pkMap, keyMap, valMap, ordMap val.OrdinalMapping
}

type nonCoveringMapIter struct {
	indexIter prolly.MapIter
	pkBld     *val.TupleBuilder
	primary   prolly.Map
	pkMap     val.OrdinalMapping
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
	pk, err := i.pkBld.Build(sharePool)
	if err != nil {
		return nil, nil, err
	}

	var value val.Tuple
	err = i.primary.Get(ctx, pk, func(_, v val.Tuple) error {
		value = v
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return pk, value, nil
}

func (ib *nonCoveringIndexImplBuilder) OutputSchema() schema.Schema {
	// this refs the table schema
	return ib.baseIndexImplBuilder.idx.Schema()
}

// NewRangeMapIter implements IndexScanBuilder
func (ib *nonCoveringIndexImplBuilder) NewRangeMapIter(ctx context.Context, r prolly.Range, reverse bool) (prolly.MapIter, error) {
	if ib.isProximity {
		return nil, fmt.Errorf("range map iter not allowed for vector index")
	}
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
	var indexIter prolly.MapIter
	var err error
	if proximityPartition, ok := part.(vectorPartitionIter); ok {
		indexIter, err = ib.proximityIter(ctx, proximityPartition)
	} else {
		indexIter, err = ib.rangeIter(ctx, part)
	}
	if err != nil {
		return nil, err
	}
	return prollyIndexIter{
		idx:         ib.idx,
		indexIter:   indexIter,
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

func (ib *nonCoveringIndexImplBuilder) NewSecondaryIter(strict bool, cnt int, nullSafe []bool) (SecondaryLookupIterGen, error) {
	if strict {
		return &nonCovStrictSecondaryLookupGen{pri: ib.pri, sec: ib.sec, pkMap: ib.pkMap, pkBld: ib.pkBld, sch: ib.idx.tableSch, prefixDesc: ib.secKd.PrefixDesc(cnt)}, nil
	} else {
		return &nonCovLaxSecondaryLookupGen{pri: ib.pri, sec: ib.sec, pkMap: ib.pkMap, pkBld: ib.pkBld, sch: ib.idx.tableSch, prefixDesc: ib.secKd.PrefixDesc(cnt), nullSafe: nullSafe}, nil
	}
}

func NewKeylessIndexImplBuilder(pri, sec durable.Index, idx DoltIndex) *keylessIndexImplBuilder {
	return &keylessIndexImplBuilder{
		baseIndexImplBuilder: &baseIndexImplBuilder{idx: idx.(*doltIndex)},
		s:                    &durableIndexState{Primary: pri, Secondary: sec},
	}
}

// TODO keylessIndexImplBuilder should be similar to the non-covering
// index case, where we will need to reference the primary index,
// but can take advantage of point lookup optimizations
type keylessIndexImplBuilder struct {
	*baseIndexImplBuilder
	s *durableIndexState
}

func (ib *keylessIndexImplBuilder) OutputSchema() schema.Schema {
	return ib.idx.Schema()
}

// NewRangeMapIter implements IndexScanBuilder
func (ib *keylessIndexImplBuilder) NewRangeMapIter(ctx context.Context, r prolly.Range, reverse bool) (prolly.MapIter, error) {
	rows := ib.s.Primary
	dsecondary := ib.s.Secondary
	secondary, err := durable.ProllyMapFromIndex(dsecondary)
	if err != nil {
		return nil, err
	}
	indexIter, err := secondary.IterRange(ctx, r)
	if err != nil {
		return nil, err
	}
	clustered, err := durable.ProllyMapFromIndex(rows)
	if err != nil {
		return nil, err
	}
	keyDesc := clustered.KeyDesc()
	indexMap := OrdinalMappingFromIndex(ib.idx)

	keyBld := val.NewTupleBuilder(keyDesc, clustered.NodeStore())

	return &keylessLookupIter{pri: clustered, secIter: indexIter, pkMap: indexMap, pkBld: keyBld, prefixDesc: keyDesc}, nil
}

// NewPartitionRowIter implements IndexScanBuilder
func (ib *keylessIndexImplBuilder) NewPartitionRowIter(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	var prollyRange prolly.Range
	var reverse bool
	switch p := part.(type) {
	case rangePartition:
		prollyRange = p.prollyRange
		reverse = p.isReverse
	case pointPartition:
		prollyRange = p.r
	}
	return newProllyKeylessIndexIter(ctx, ib.idx, prollyRange, ib.sch, ib.projections, ib.s.Primary, ib.s.Secondary, reverse)
}

func (ib *keylessIndexImplBuilder) NewSecondaryIter(strict bool, cnt int, nullSafe []bool) (SecondaryLookupIterGen, error) {
	pri, err := durable.ProllyMapFromIndex(ib.s.Primary)
	if err != nil {
		return nil, err
	}
	pkDesc, _ := pri.Descriptors()
	pkBld := val.NewTupleBuilder(pkDesc, pri.NodeStore())

	secondary, err := durable.ProllyMapFromIndex(ib.s.Secondary)
	if err != nil {
		return nil, err
	}

	return &keylessSecondaryLookupGen{
		pri:        pri,
		sec:        secondary,
		sch:        ib.idx.tableSch,
		pkMap:      OrdinalMappingFromIndex(ib.idx),
		pkBld:      pkBld,
		prefixDesc: secondary.KeyDesc().PrefixDesc(cnt),
	}, nil
}
