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

package sqle

import (
	"errors"
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/dolthub/dolt/go/store/types"
)

type DoltIndex interface {
	sql.Index
	Schema() schema.Schema
	IndexSchema() schema.Schema
	TableData() types.Map
	IndexRowData() types.Map
}

type doltIndex struct {
	cols         []schema.Column
	db           sql.Database
	id           string
	indexRowData types.Map
	indexSch     schema.Schema
	table        *doltdb.Table
	tableData    types.Map
	tableName    string
	tableSch     schema.Schema
	unique       bool
	comment      string
	generated    bool
}

var _ DoltIndex = (*doltIndex)(nil)

// ColumnExpressionTypes implements the interface sql.Index.
func (di *doltIndex) ColumnExpressionTypes(ctx *sql.Context) []sql.ColumnExpressionType {
	cets := make([]sql.ColumnExpressionType, len(di.cols))
	for i, col := range di.cols {
		cets[i] = sql.ColumnExpressionType{
			Expression: di.tableName + "." + col.Name,
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

	// This might remain nil if the given ranges each contain an EmptyRange for one of the columns. This will just
	// cause the lookup to return no rows, which is the desired behavior.
	var readRanges []*noms.ReadRange
RangeLoop:
	for _, rang := range ranges {
		if len(rang) > len(di.cols) {
			return nil, nil
		}

		inclusive := true
		var lowerKeys []interface{}
		for _, rangeColumnExpr := range rang {
			if rangeColumnExpr.HasLowerBound() {
				inclusive = inclusive && rangeColumnExpr.LowerBound.TypeAsLowerBound() == sql.Closed
				lowerKeys = append(lowerKeys, sql.GetRangeCutKey(rangeColumnExpr.LowerBound))
			} else {
				inclusive = false
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
			promotedType := di.cols[i].TypeInfo.Promote()
			if rangeColumnExpr.HasLowerBound() {
				key := sql.GetRangeCutKey(rangeColumnExpr.LowerBound)
				val, err := promotedType.ConvertValueToNomsValue(ctx, di.table.ValueReadWriter(), key)
				if err != nil {
					return nil, err
				}
				cb.lowerbound.equals = rangeColumnExpr.LowerBound.TypeAsLowerBound() == sql.Closed
				cb.lowerbound.infinity = false
				cb.lowerbound.val = val
			} else {
				cb.lowerbound.equals = false
				cb.lowerbound.infinity = true
				cb.lowerbound.val = nil
			}
			if rangeColumnExpr.HasUpperBound() {
				key := sql.GetRangeCutKey(rangeColumnExpr.UpperBound)
				val, err := promotedType.ConvertValueToNomsValue(ctx, di.table.ValueReadWriter(), key)
				if err != nil {
					return nil, err
				}
				cb.upperbound.equals = rangeColumnExpr.UpperBound.TypeAsUpperBound() == sql.Closed
				cb.upperbound.infinity = false
				cb.upperbound.val = val
			} else {
				cb.upperbound.equals = false
				cb.upperbound.infinity = true
				cb.upperbound.val = nil
			}
			rangeCheck[i] = cb
		}

		readRanges = append(readRanges, &noms.ReadRange{
			Start:     lowerboundTuple,
			Inclusive: inclusive,
			Reverse:   false,
			Check:     rangeCheck,
		})
	}

	return &doltIndexLookup{
		idx:       di,
		ranges:    readRanges,
		sqlRanges: ranges,
	}, nil
}

// Database implement sql.Index
func (di *doltIndex) Database() string {
	return di.db.Name()
}

// Expressions implements sql.Index
func (di *doltIndex) Expressions() []string {
	strs := make([]string, len(di.cols))
	for i, col := range di.cols {
		strs[i] = di.tableName + "." + col.Name
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
	return di.generated
}

// Schema returns the dolt table schema of this index.
func (di *doltIndex) Schema() schema.Schema {
	return di.tableSch
}

// IndexSchema returns the dolt index schema.
func (di *doltIndex) IndexSchema() schema.Schema {
	return di.indexSch
}

// Table implements sql.Index
func (di *doltIndex) Table() string {
	return di.tableName
}

// TableData returns the map of table data for this index (the map of the target table, not the index storage table)
func (di *doltIndex) TableData() types.Map {
	return di.tableData
}

// IndexRowData returns the map of index row data.
func (di *doltIndex) IndexRowData() types.Map {
	return di.indexRowData
}

// prefix returns a copy of this index with only the first n columns. If n is >= the number of columns present, then
// the exact index is returned without copying.
func (di *doltIndex) prefix(n int) *doltIndex {
	if n >= len(di.cols) {
		return di
	}
	ndi := *di
	ndi.cols = di.cols[:n]
	ndi.id = fmt.Sprintf("%s_PREFIX_%d", di.id, n)
	ndi.comment = fmt.Sprintf("prefix of %s multi-column index on %d column(s)", di.id, n)
	ndi.generated = true
	return &ndi
}

// keysToTuple returns a tuple that indicates the starting point for an index. The empty tuple will cause the index to
// start at the very beginning.
func (di *doltIndex) keysToTuple(ctx *sql.Context, keys []interface{}) (types.Tuple, error) {
	nbf := di.indexRowData.Format()
	if len(keys) > len(di.cols) {
		return types.EmptyTuple(nbf), errors.New("too many keys for the column count")
	}

	vals := make([]types.Value, len(keys)*2)
	for i := range keys {
		col := di.cols[i]
		// As an example, if our TypeInfo is Int8, we should not fail to create a tuple if we are returning all keys
		// that have a value of less than 9001, thus we promote the TypeInfo to the widest type.
		val, err := col.TypeInfo.Promote().ConvertValueToNomsValue(ctx, di.table.ValueReadWriter(), keys[i])
		if err != nil {
			return types.EmptyTuple(nbf), err
		}
		vals[2*i] = types.Uint(col.Tag)
		vals[2*i+1] = val
	}
	return types.NewTuple(nbf, vals...)
}
