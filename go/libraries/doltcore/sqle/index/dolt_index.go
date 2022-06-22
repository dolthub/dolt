// Copyright 2020-2021 Dolthub, Inc.
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
	"errors"
	"sync/atomic"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/shim"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

type DoltTableable interface {
	DoltTable(*sql.Context) (*doltdb.Table, error)
	DataCacheKey(*sql.Context) (doltdb.DataCacheKey, bool, error)
}

type DoltIndex interface {
	sql.FilteredIndex
	sql.OrderedIndex
	Schema() schema.Schema
	IndexSchema() schema.Schema
	Format() *types.NomsBinFormat
	IsPrimaryKey() bool

	getDurableState(*sql.Context, DoltTableable) (*durableIndexState, error)
	coversColumns(s *durableIndexState, columns []string) bool
	sqlRowConverter(s *durableIndexState) *KVToSqlRowConverter
	lookupTags(s *durableIndexState) map[uint64]int
}

func DoltDiffIndexesFromTable(ctx context.Context, db, tbl string, t *doltdb.Table) (indexes []sql.Index, err error) {
	sch, err := t.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	// Currently, only support diffs on tables with primary keys, panic?
	if schema.IsKeyless(sch) {
		return nil, nil
	}

	// TODO: do this for other indexes?
	tableRows, err := t.GetRowData(ctx)
	if err != nil {
		return nil, err
	}
	keyBld := maybeGetKeyBuilder(tableRows)

	// TODO: two primary keys???
	cols := sch.GetPKCols().GetColumns()

	// add to_ prefix
	toCols := make([]schema.Column, len(cols))
	for i, col := range cols {
		toCols[i] = col
		toCols[i].Name = "to_" + col.Name
	}

	// to_ columns
	toIndex := doltIndex{
		id:                            "PRIMARY",
		tblName:                       doltdb.DoltDiffTablePrefix + tbl,
		dbName:                        db,
		columns:                       toCols,
		indexSch:                      sch,
		tableSch:                      sch,
		unique:                        true,
		comment:                       "",
		vrw:                           t.ValueReadWriter(),
		keyBld:                        keyBld,
		order:                         sql.IndexOrderAsc,
		constrainedToLookupExpression: true,
	}

	// TODO: need to add from_ columns

	return append(indexes, &toIndex), nil
}

func DoltIndexesFromTable(ctx context.Context, db, tbl string, t *doltdb.Table) (indexes []sql.Index, err error) {
	sch, err := t.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	if !schema.IsKeyless(sch) {
		idx, err := getPrimaryKeyIndex(ctx, db, tbl, t, sch)
		if err != nil {
			return nil, err
		}
		indexes = append(indexes, idx)
	}

	for _, definition := range sch.Indexes().AllIndexes() {
		idx, err := getSecondaryIndex(ctx, db, tbl, t, sch, definition)
		if err != nil {
			return nil, err
		}
		indexes = append(indexes, idx)
	}

	return indexes, nil
}

func TableHasIndex(ctx context.Context, db, tbl string, t *doltdb.Table, i sql.Index) (bool, error) {
	sch, err := t.GetSchema(ctx)
	if err != nil {
		return false, err
	}

	if !schema.IsKeyless(sch) {
		idx, err := getPrimaryKeyIndex(ctx, db, tbl, t, sch)
		if err != nil {
			return false, err
		}
		if indexesMatch(idx, i) {
			return true, nil
		}
	}

	for _, definition := range sch.Indexes().AllIndexes() {
		idx, err := getSecondaryIndex(ctx, db, tbl, t, sch, definition)
		if err != nil {
			return false, err
		}
		if indexesMatch(idx, i) {
			return true, nil
		}
	}

	return false, nil
}

// indexesMatch returns whether the two index objects should be considered the same index for the purpose of a lookup,
// i.e. whether they have the same name and index the same table columns.
func indexesMatch(a sql.Index, b sql.Index) bool {
	dia, dib := a.(*doltIndex), b.(*doltIndex)
	if dia.isPk != dib.isPk || dia.id != dib.id {
		return false
	}

	if len(dia.columns) != len(dib.columns) {
		return false
	}
	for i := range dia.columns {
		if dia.columns[i].Name != dib.columns[i].Name {
			return false
		}
	}

	return true
}

func DoltHistoryIndexesFromTable(ctx context.Context, db, tbl string, t *doltdb.Table) ([]sql.Index, error) {
	indexes, err := DoltIndexesFromTable(ctx, db, tbl, t)
	if err != nil {
		return nil, err
	}

	unorderedIndexes := make([]sql.Index, len(indexes))
	for i := range indexes {
		di := indexes[i].(*doltIndex)
		// History table indexed reads don't come back in order (iterated by commit graph first), and can include rows that
		// weren't asked for (because the index needed may not exist at all revisions)
		di.order = sql.IndexOrderNone
		di.constrainedToLookupExpression = false
		unorderedIndexes[i] = di
	}

	return unorderedIndexes, nil
}

func getPrimaryKeyIndex(ctx context.Context, db, tbl string, t *doltdb.Table, sch schema.Schema) (sql.Index, error) {
	tableRows, err := t.GetRowData(ctx)
	if err != nil {
		return nil, err
	}
	keyBld := maybeGetKeyBuilder(tableRows)

	cols := sch.GetPKCols().GetColumns()

	return &doltIndex{
		id:                            "PRIMARY",
		tblName:                       tbl,
		dbName:                        db,
		columns:                       cols,
		indexSch:                      sch,
		tableSch:                      sch,
		unique:                        true,
		isPk:                          true,
		comment:                       "",
		vrw:                           t.ValueReadWriter(),
		keyBld:                        keyBld,
		order:                         sql.IndexOrderAsc,
		constrainedToLookupExpression: true,
	}, nil
}

func getSecondaryIndex(ctx context.Context, db, tbl string, t *doltdb.Table, sch schema.Schema, idx schema.Index) (sql.Index, error) {
	indexRows, err := t.GetIndexRowData(ctx, idx.Name())
	if err != nil {
		return nil, err
	}
	keyBld := maybeGetKeyBuilder(indexRows)

	cols := make([]schema.Column, idx.Count())
	for i, tag := range idx.IndexedColumnTags() {
		cols[i], _ = idx.GetColumn(tag)
	}

	return &doltIndex{
		id:                            idx.Name(),
		tblName:                       tbl,
		dbName:                        db,
		columns:                       cols,
		indexSch:                      idx.Schema(),
		tableSch:                      sch,
		unique:                        idx.IsUnique(),
		isPk:                          false,
		comment:                       idx.Comment(),
		vrw:                           t.ValueReadWriter(),
		keyBld:                        keyBld,
		order:                         sql.IndexOrderAsc,
		constrainedToLookupExpression: true,
	}, nil
}

type durableIndexState struct {
	key                   doltdb.DataCacheKey
	Primary               durable.Index
	Secondary             durable.Index
	coversAllCols         uint32
	cachedLookupTags      atomic.Value
	cachedSqlRowConverter atomic.Value
}

func (s *durableIndexState) coversAllColumns(i *doltIndex) bool {
	coversI := atomic.LoadUint32(&s.coversAllCols)
	if coversI != 0 {
		return coversI == 1
	}
	cols := i.Schema().GetAllCols()
	var idxCols *schema.ColCollection
	if types.IsFormat_DOLT_1(i.Format()) {
		// prolly indexes can cover an index lookup using
		// both the key and value fields of the index,
		// this allows using covering index machinery for
		// primary key index lookups.
		idxCols = i.IndexSchema().GetAllCols()
	} else {
		// to cover an index lookup, noms indexes must
		// contain all fields in the index's key.
		idxCols = i.IndexSchema().GetPKCols()
	}
	covers := true
	for i := 0; i < cols.Size(); i++ {
		col := cols.GetAtIndex(i)
		if _, ok := idxCols.GetByNameCaseInsensitive(col.Name); !ok {
			covers = false
			break
		}
	}
	if covers {
		atomic.StoreUint32(&s.coversAllCols, 1)
	} else {
		atomic.StoreUint32(&s.coversAllCols, 2)
	}
	return covers
}

func (s *durableIndexState) lookupTags(i *doltIndex) map[uint64]int {
	cached := s.cachedLookupTags.Load()
	if cached == nil {
		tags := i.Schema().GetPKCols().Tags
		sz := len(tags)
		if sz == 0 {
			sz = 1
		}
		tocache := make(map[uint64]int, sz)
		for i, tag := range tags {
			tocache[tag] = i
		}
		if len(tocache) == 0 {
			tocache[schema.KeylessRowIdTag] = 0
		}
		s.cachedLookupTags.Store(tocache)
		cached = tocache
	}
	return cached.(map[uint64]int)
}

func (s *durableIndexState) sqlRowConverter(i *doltIndex) *KVToSqlRowConverter {
	cached := s.cachedSqlRowConverter.Load()
	if cached == nil {
		cached = NewKVToSqlRowConverterForCols(i.Format(), i.Schema())
		s.cachedSqlRowConverter.Store(cached)
	}
	return cached.(*KVToSqlRowConverter)
}

type cachedDurableIndexes struct {
	val atomic.Value
}

func (i *cachedDurableIndexes) load() *durableIndexState {
	l := i.val.Load()
	if l == nil {
		return nil
	}
	return l.(*durableIndexState)
}

func (i *cachedDurableIndexes) store(v *durableIndexState) {
	i.val.Store(v)
}

type doltIndex struct {
	id      string
	tblName string
	dbName  string

	columns []schema.Column

	indexSch schema.Schema
	tableSch schema.Schema
	unique   bool
	isPk     bool
	comment  string
	order    sql.IndexOrder

	constrainedToLookupExpression bool

	vrw    types.ValueReadWriter
	keyBld *val.TupleBuilder

	cache cachedDurableIndexes
}

var _ DoltIndex = (*doltIndex)(nil)

// ColumnExpressionTypes implements the interface sql.Index.
func (di *doltIndex) ColumnExpressionTypes(ctx *sql.Context) []sql.ColumnExpressionType {
	cets := make([]sql.ColumnExpressionType, len(di.columns))
	for i, col := range di.columns {
		cets[i] = sql.ColumnExpressionType{
			Expression: di.tblName + "." + col.Name,
			Type:       col.TypeInfo.ToSqlType(),
		}
	}
	return cets
}

// NewLookup implements the interface sql.Index.
func (di *doltIndex) NewLookup(ctx *sql.Context, ranges ...sql.Range) (sql.IndexLookup, error) {
	if len(ranges) == 0 {
		return nil, nil
	}

	if types.IsFormat_DOLT_1(di.vrw.Format()) {
		return di.newProllyLookup(ctx, tree.NewNodeStore(shim.ChunkStoreFromVRW(di.vrw)), ranges...)
	}

	return di.newNomsLookup(ctx, ranges...)
}

func (di *doltIndex) getDurableState(ctx *sql.Context, ti DoltTableable) (*durableIndexState, error) {
	var newkey doltdb.DataCacheKey
	var cancache bool
	var err error
	newkey, cancache, err = ti.DataCacheKey(ctx)
	if err != nil {
		return nil, err
	}

	var ret *durableIndexState
	if cancache {
		ret = di.cache.load()
		if ret != nil && ret.key == newkey {
			return ret, nil
		}
	}

	ret = new(durableIndexState)

	var t *doltdb.Table
	t, err = ti.DoltTable(ctx)
	if err != nil {
		return nil, err
	}

	var primary, secondary durable.Index

	primary, err = t.GetRowData(ctx)
	if err != nil {
		return nil, err
	}
	if di.ID() == "PRIMARY" {
		secondary = primary
	} else {
		secondary, err = t.GetIndexRowData(ctx, di.ID())
		if err != nil {
			return nil, err
		}
	}

	ret.key = newkey
	ret.Primary = primary
	ret.Secondary = secondary

	if cancache {
		di.cache.store(ret)
	}

	return ret, nil
}

func (di *doltIndex) newProllyLookup(ctx *sql.Context, ns tree.NodeStore, ranges ...sql.Range) (sql.IndexLookup, error) {
	var err error
	sqlRanges, err := pruneEmptyRanges(ranges)
	if err != nil {
		return nil, err
	}

	prs := make([]prolly.Range, len(sqlRanges))
	for i, sr := range sqlRanges {
		prs[i], err = prollyRangeFromSqlRange(ctx, ns, sr, di.keyBld)
		if err != nil {
			return nil, err
		}
	}

	// the sql engine provides ranges that are logically disjoint in value space.
	// however, these ranges may overlap physically within the index. Here we merge
	// physically overlapping ranges to avoid returning duplicate tuples/rows.
	merged := prolly.MergeOverlappingRanges(prs...)

	return &doltIndexLookup{
		idx:          di,
		prollyRanges: merged,
		sqlRanges:    sqlRanges,
	}, nil
}

func (di *doltIndex) newNomsLookup(ctx *sql.Context, ranges ...sql.Range) (sql.IndexLookup, error) {
	// This might remain nil if the given nomsRanges each contain an EmptyRange for one of the columns. This will just
	// cause the lookup to return no rows, which is the desired behavior.
	var readRanges []*noms.ReadRange
RangeLoop:
	for _, rang := range ranges {
		if len(rang) > len(di.columns) {
			return nil, nil
		}

		var lowerKeys []interface{}
		for _, rangeColumnExpr := range rang {
			if rangeColumnExpr.HasLowerBound() {
				lowerKeys = append(lowerKeys, sql.GetRangeCutKey(rangeColumnExpr.LowerBound))
			} else {
				break
			}
		}
		lowerboundTuple, err := di.keysToTuple(ctx, lowerKeys)
		if err != nil {
			return nil, err
		}

		rangeCheck := make(nomsRangeCheck, len(rang))
		for i, rangeColumnExpr := range rang {
			// An empty column expression will mean that no values for this column can be matched, so we can discard the
			// entire range.
			if ok, err := rangeColumnExpr.IsEmpty(); err != nil {
				return nil, err
			} else if ok {
				continue RangeLoop
			}

			cb := columnBounds{}
			// We promote each type as the value has already been validated against the type
			promotedType := di.columns[i].TypeInfo.Promote()
			if rangeColumnExpr.HasLowerBound() {
				key := sql.GetRangeCutKey(rangeColumnExpr.LowerBound)
				val, err := promotedType.ConvertValueToNomsValue(ctx, di.vrw, key)
				if err != nil {
					return nil, err
				}
				if rangeColumnExpr.LowerBound.TypeAsLowerBound() == sql.Closed {
					// For each lowerbound case, we set the upperbound to infinity, as the upperbound can increment to
					// get to the desired overall case while retaining whatever was set for the lowerbound.
					cb.boundsCase = boundsCase_greaterEquals_infinity
				} else {
					cb.boundsCase = boundsCase_greater_infinity
				}
				cb.lowerbound = val
			} else {
				cb.boundsCase = boundsCase_infinity_infinity
			}
			if rangeColumnExpr.HasUpperBound() {
				key := sql.GetRangeCutKey(rangeColumnExpr.UpperBound)
				val, err := promotedType.ConvertValueToNomsValue(ctx, di.vrw, key)
				if err != nil {
					return nil, err
				}
				if rangeColumnExpr.UpperBound.TypeAsUpperBound() == sql.Closed {
					// Bounds cases are enum aliases on bytes, and they're arranged such that we can increment the case
					// that was previously set when evaluating the lowerbound to get the proper overall case.
					cb.boundsCase += 1
				} else {
					cb.boundsCase += 2
				}
				cb.upperbound = val
			}
			if rangeColumnExpr.Type() == sql.RangeType_Null {
				cb.boundsCase = boundsCase_isNull
			}
			rangeCheck[i] = cb
		}

		// If the suffix checks will always succeed (both bounds are infinity) then they can be removed to reduce the
		// number of checks that are called per-row. Always leave one check to skip NULLs.
		for i := len(rangeCheck) - 1; i > 0 && len(rangeCheck) > 1; i-- {
			if rangeCheck[i].boundsCase == boundsCase_infinity_infinity {
				rangeCheck = rangeCheck[:i]
			} else {
				break
			}
		}

		readRanges = append(readRanges, &noms.ReadRange{
			Start:     lowerboundTuple,
			Inclusive: true, // The checks handle whether a value is included or not
			Reverse:   false,
			Check:     rangeCheck,
		})
	}

	return &doltIndexLookup{
		idx:        di,
		nomsRanges: readRanges,
		sqlRanges:  ranges,
	}, nil
}

func (di *doltIndex) sqlRowConverter(s *durableIndexState) *KVToSqlRowConverter {
	return s.sqlRowConverter(di)
}

func (di *doltIndex) lookupTags(s *durableIndexState) map[uint64]int {
	return s.lookupTags(di)
}

func (di *doltIndex) coversColumns(s *durableIndexState, cols []string) bool {
	if cols == nil {
		return s.coversAllColumns(di)
	}

	var idxCols *schema.ColCollection
	if types.IsFormat_DOLT_1(di.Format()) {
		// prolly indexes can cover an index lookup using
		// both the key and value fields of the index,
		// this allows using covering index machinery for
		// primary key index lookups.
		idxCols = di.IndexSchema().GetAllCols()
	} else {
		// to cover an index lookup, noms indexes must
		// contain all fields in the index's key.
		idxCols = di.IndexSchema().GetPKCols()
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

func (di *doltIndex) HandledFilters(filters []sql.Expression) []sql.Expression {
	if types.IsFormat_DOLT_1(di.vrw.Format()) {
		// todo(andy): handle first column filters
		return nil
	} else {
		if di.constrainedToLookupExpression {
			return filters
		}
		return nil
	}
}

func (di *doltIndex) Order() sql.IndexOrder {
	return di.order
}

// Database implement sql.Index
func (di *doltIndex) Database() string {
	return di.dbName
}

// Expressions implements sql.Index
func (di *doltIndex) Expressions() []string {
	strs := make([]string, len(di.columns))
	for i, col := range di.columns {
		strs[i] = di.tblName + "." + col.Name
	}
	return strs
}

// ID implements sql.Index
func (di *doltIndex) ID() string {
	return di.id
}

// IsUnique implements sql.Index
func (di *doltIndex) IsUnique() bool {
	return di.unique
}

// IsPrimaryKey implements DoltIndex.
func (di *doltIndex) IsPrimaryKey() bool {
	return di.isPk
}

// Comment implements sql.Index
func (di *doltIndex) Comment() string {
	return di.comment
}

// IndexType implements sql.Index
func (di *doltIndex) IndexType() string {
	return "BTREE"
}

// IsGenerated implements sql.Index
func (di *doltIndex) IsGenerated() bool {
	return false
}

// Schema returns the dolt Table schema of this index.
func (di *doltIndex) Schema() schema.Schema {
	return di.tableSch
}

// IndexSchema returns the dolt index schema.
func (di *doltIndex) IndexSchema() schema.Schema {
	return di.indexSch
}

// Table implements sql.Index
func (di *doltIndex) Table() string {
	return di.tblName
}

func (di *doltIndex) Format() *types.NomsBinFormat {
	return di.vrw.Format()
}

// keysToTuple returns a tuple that indicates the starting point for an index. The empty tuple will cause the index to
// start at the very beginning.
func (di *doltIndex) keysToTuple(ctx *sql.Context, keys []interface{}) (types.Tuple, error) {
	nbf := di.vrw.Format()
	if len(keys) > len(di.columns) {
		return types.EmptyTuple(nbf), errors.New("too many keys for the column count")
	}

	vals := make([]types.Value, len(keys)*2)
	for i := range keys {
		col := di.columns[i]
		// As an example, if our TypeInfo is Int8, we should not fail to create a tuple if we are returning all keys
		// that have a value of less than 9001, thus we promote the TypeInfo to the widest type.
		val, err := col.TypeInfo.Promote().ConvertValueToNomsValue(ctx, di.vrw, keys[i])
		if err != nil {
			return types.EmptyTuple(nbf), err
		}
		vals[2*i] = types.Uint(col.Tag)
		vals[2*i+1] = val
	}
	return types.NewTuple(nbf, vals...)
}

var sharePool = pool.NewBuffPool()

func maybeGetKeyBuilder(idx durable.Index) *val.TupleBuilder {
	if types.IsFormat_DOLT_1(idx.Format()) {
		kd, _ := durable.ProllyMapFromIndex(idx).Descriptors()
		return val.NewTupleBuilder(kd)
	}
	return nil
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

func prollyRangeFromSqlRange(ctx context.Context, ns tree.NodeStore, rng sql.Range, tb *val.TupleBuilder) (prolly.Range, error) {
	prollyRange := prolly.Range{
		Start: make([]prolly.RangeCut, len(rng)),
		Stop:  make([]prolly.RangeCut, len(rng)),
		Desc:  tb.Desc,
	}

	for i, expr := range rng {
		if !sql.RangeCutIsBinding(expr.LowerBound) {
			continue
		}

		v, err := getRangeCutValue(expr.LowerBound, rng[i].Typ)
		if err != nil {
			return prolly.Range{}, err
		}
		if err = PutField(ctx, ns, tb, i, v); err != nil {
			return prolly.Range{}, err
		}
	}

	// BuildPermissive() allows nulls in non-null fields
	tup := tb.BuildPermissive(sharePool)

	for i, expr := range rng {
		if !sql.RangeCutIsBinding(expr.LowerBound) {
			continue
		}

		bound := expr.LowerBound.TypeAsLowerBound()
		_, null := expr.LowerBound.(sql.NullBound)

		prollyRange.Start[i] = prolly.RangeCut{
			Value:     tup.GetField(i),
			Inclusive: bound == sql.Closed,
			Null:      null,
		}
	}

	for i, expr := range rng {
		if !sql.RangeCutIsBinding(expr.UpperBound) {
			continue
		}

		v, err := getRangeCutValue(expr.UpperBound, rng[i].Typ)
		if err != nil {
			return prolly.Range{}, err
		}
		if err = PutField(ctx, ns, tb, i, v); err != nil {
			return prolly.Range{}, err
		}
	}

	tup = tb.BuildPermissive(sharePool)
	for i, expr := range rng {
		if !sql.RangeCutIsBinding(expr.UpperBound) {
			continue
		}

		bound := expr.UpperBound.TypeAsUpperBound()
		_, null := expr.UpperBound.(sql.NullBound)

		prollyRange.Stop[i] = prolly.RangeCut{
			Value:     tup.GetField(i),
			Inclusive: bound == sql.Closed,
			Null:      null,
		}
	}

	return prollyRange, nil
}

func getRangeCutValue(cut sql.RangeCut, typ sql.Type) (interface{}, error) {
	return typ.Convert(sql.GetRangeCutKey(cut))
}
