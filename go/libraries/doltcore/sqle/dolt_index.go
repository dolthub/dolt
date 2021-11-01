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
	"context"
	"errors"
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/lookup"
	"github.com/dolthub/dolt/go/store/types"
)

type DoltIndex interface {
	sql.Index
	Schema() schema.Schema
	IndexSchema() schema.Schema
	TableData() types.Map
	IndexRowData() types.Map
	EqualsDoltIndex(index DoltIndex) bool
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
	exprs := di.Expressions()
	if len(ranges[0]) > len(exprs) {
		return nil, nil
	}
	idx := di
	if len(ranges[0]) < len(exprs) {
		idx = idx.prefix(len(ranges[0]))
		exprs = idx.Expressions()
	}

	var lookupRanges []lookup.Range

	for _, rang := range ranges {
		// TODO: support mixing range types and lengths, which will allow us to support indexing over mixed operators
		// For now, grab a range to have a baseline so that we may enforce parity with all other ranges
		var rangeTypes []sql.RangeType
		rangeTypes = make([]sql.RangeType, len(rang[0]))
		for i, r := range rang[0] {
			rangeTypes[i] = r.Type()
		}

		for i, rangeType := range rangeTypes {
			var keys1 []interface{} // used if only one bound is set, or if both bounds are set represents the lowerbound
			var keys2 []interface{} // used only when both bounds are set, thus will always represent the upperbound

			for _, rangeColumn := range rang {
				if len(rangeTypes) != len(rangeColumn) {
					return nil, nil //TODO: support indexes having different range counts
				}
				rangeColumnExpr := rangeColumn[i]
				if rangeColumnExpr.Type() != rangeType {
					return nil, nil //TODO: support mixing range types
				}

				hasLower := rangeColumnExpr.HasLowerBound()
				hasUpper := rangeColumnExpr.HasUpperBound()
				if hasLower && hasUpper {
					keys1 = append(keys1, sql.GetRangeCutKey(rangeColumnExpr.LowerBound))
					keys2 = append(keys2, sql.GetRangeCutKey(rangeColumnExpr.UpperBound))
				} else if hasLower && !hasUpper {
					keys1 = append(keys1, sql.GetRangeCutKey(rangeColumnExpr.LowerBound))
				} else if !hasLower && hasUpper {
					keys1 = append(keys1, sql.GetRangeCutKey(rangeColumnExpr.UpperBound))
				}
			}

			lookupRange, err := idx.sqlRangeToLookupRange(ctx, rangeType, keys1, keys2)
			if err != nil {
				return nil, err
			}
			lookupRanges = append(lookupRanges, lookupRange)
		}
	}

	return &doltIndexLookup{
		idx:       idx,
		ranges:    lookupRanges,
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

// sqlRangeToLookupRange takes a range returned by the sql engine and converts it to the appropriate lookup range used for noms traversal.
func (di *doltIndex) sqlRangeToLookupRange(ctx *sql.Context, rangeType sql.RangeType, keys1, keys2 []interface{}) (lookup.Range, error) {
	switch rangeType {
	case sql.RangeType_Empty:
		return lookup.EmptyRange(), nil
	case sql.RangeType_All:
		return lookup.AllRange(), nil
	case sql.RangeType_GreaterThan:
		tpl, err := di.keysToTuple(keys1)
		if err != nil {
			return lookup.Range{}, err
		}
		return lookup.GreaterThanRange(tpl)
	case sql.RangeType_GreaterOrEqual:
		tpl, err := di.keysToTuple(keys1)
		if err != nil {
			return lookup.Range{}, err
		}
		return lookup.GreaterOrEqualRange(tpl), nil
	case sql.RangeType_LessThan:
		tpl, err := di.keysToTuple(keys1)
		if err != nil {
			return lookup.Range{}, err
		}
		return lookup.LessThanRange(tpl), nil
	case sql.RangeType_LessOrEqual:
		tpl, err := di.keysToTuple(keys1)
		if err != nil {
			return lookup.Range{}, err
		}
		return lookup.LessOrEqualRange(tpl)
	case sql.RangeType_ClosedClosed:
		lowerTpl, err := di.keysToTuple(keys1)
		if err != nil {
			return lookup.Range{}, err
		}
		upperTpl, err := di.keysToTuple(keys2)
		if err != nil {
			return lookup.Range{}, err
		}
		return lookup.ClosedRange(lowerTpl, upperTpl)
	case sql.RangeType_OpenOpen:
		lowerTpl, err := di.keysToTuple(keys1)
		if err != nil {
			return lookup.Range{}, err
		}
		upperTpl, err := di.keysToTuple(keys2)
		if err != nil {
			return lookup.Range{}, err
		}
		return lookup.OpenRange(lowerTpl, upperTpl)
	case sql.RangeType_OpenClosed:
		lowerTpl, err := di.keysToTuple(keys1)
		if err != nil {
			return lookup.Range{}, err
		}
		upperTpl, err := di.keysToTuple(keys2)
		if err != nil {
			return lookup.Range{}, err
		}
		return lookup.CustomRange(lowerTpl, upperTpl, lookup.Open, lookup.Closed)
	case sql.RangeType_ClosedOpen:
		lowerTpl, err := di.keysToTuple(keys1)
		if err != nil {
			return lookup.Range{}, err
		}
		upperTpl, err := di.keysToTuple(keys2)
		if err != nil {
			return lookup.Range{}, err
		}
		return lookup.CustomRange(lowerTpl, upperTpl, lookup.Closed, lookup.Open)
	}
	return lookup.Range{}, sql.ErrInvalidRangeType.New()
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

func (di *doltIndex) keysToTuple(keys []interface{}) (types.Tuple, error) {
	nbf := di.indexRowData.Format()
	if len(di.cols) != len(keys) {
		return types.EmptyTuple(nbf), errors.New("keys must specify all columns for an index")
	}
	var vals []types.Value
	for i, col := range di.cols {
		// As an example, if our TypeInfo is Int8, we should not fail to create a tuple if we are returning all keys
		// that have a value of less than 9001, thus we promote the TypeInfo to the widest type.
		val, err := col.TypeInfo.Promote().ConvertValueToNomsValue(context.Background(), di.table.ValueReadWriter(), keys[i])
		if err != nil {
			return types.EmptyTuple(nbf), err
		}
		vals = append(vals, types.Uint(col.Tag), val)
	}
	return types.NewTuple(nbf, vals...)
}

func (di *doltIndex) EqualsDoltIndex(oIdx DoltIndex) bool {
	if !expressionsAreEquals(di.Expressions(), oIdx.Expressions()) {
		return false
	}

	if di.Database() != oIdx.Database() {
		return false
	}

	if di.Table() != oIdx.Table() {
		return false
	}

	if di.ID() != oIdx.ID() {
		return false
	}

	if di.IsUnique() != oIdx.IsUnique() {
		return false
	}

	if !(schema.SchemasAreEqual(di.IndexSchema(), oIdx.IndexSchema())) {
		return false
	}

	return true
}

func expressionsAreEquals(exprs1, exprs2 []string) bool {
	if exprs1 == nil && exprs2 == nil {
		return true
	} else if exprs1 == nil || exprs2 == nil {
		return false
	}

	if len(exprs1) != len(exprs2) {
		return false
	}

	for i, expr1 := range exprs1 {
		if expr1 != exprs2[i] {
			return false
		}
	}

	return true
}
