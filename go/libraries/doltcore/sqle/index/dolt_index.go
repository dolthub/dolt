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
	"errors"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/dolthub/dolt/go/store/types"
)

type DoltIndexImpl struct {
	Cols          []schema.Column
	Db            sql.Database
	Id            string
	RowIndexData  types.Map
	IndexSch      schema.Schema
	TableRowData  types.Map
	TableName     string
	TableSch      schema.Schema
	Unique        bool
	CommentStr    string
	GeneratedBool bool

	Vrw types.ValueReadWriter
}

var _ DoltIndex = (*DoltIndexImpl)(nil)

// ColumnExpressionTypes implements the interface sql.Index.
func (di *DoltIndexImpl) ColumnExpressionTypes(ctx *sql.Context) []sql.ColumnExpressionType {
	cets := make([]sql.ColumnExpressionType, len(di.Cols))
	for i, col := range di.Cols {
		cets[i] = sql.ColumnExpressionType{
			Expression: di.TableName + "." + col.Name,
			Type:       col.TypeInfo.ToSqlType(),
		}
	}
	return cets
}

// NewLookup implements the interface sql.Index.
func (di *DoltIndexImpl) NewLookup(ctx *sql.Context, ranges ...sql.Range) (sql.IndexLookup, error) {
	if len(ranges) == 0 {
		return nil, nil
	}

	// This might remain nil if the given ranges each contain an EmptyRange for one of the columns. This will just
	// cause the lookup to return no rows, which is the desired behavior.
	var readRanges []*noms.ReadRange
RangeLoop:
	for _, rang := range ranges {
		if len(rang) > len(di.Cols) {
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
			promotedType := di.Cols[i].TypeInfo.Promote()
			if rangeColumnExpr.HasLowerBound() {
				key := sql.GetRangeCutKey(rangeColumnExpr.LowerBound)
				val, err := promotedType.ConvertValueToNomsValue(ctx, di.Vrw, key)
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
				val, err := promotedType.ConvertValueToNomsValue(ctx, di.Vrw, key)
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
			rangeCheck[i] = cb
		}

		// If the suffix checks will always succeed (both bounds are infinity) then they can be removed to reduce the
		// number of checks that are called per-row.
		for i := len(rangeCheck) - 1; i >= 0; i-- {
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
		idx:       di,
		ranges:    readRanges,
		sqlRanges: ranges,
	}, nil
}

// Database implement sql.Index
func (di *DoltIndexImpl) Database() string {
	return di.Db.Name()
}

// Expressions implements sql.Index
func (di *DoltIndexImpl) Expressions() []string {
	strs := make([]string, len(di.Cols))
	for i, col := range di.Cols {
		strs[i] = di.TableName + "." + col.Name
	}
	return strs
}

// ID implements sql.Index
func (di *DoltIndexImpl) ID() string {
	return di.Id
}

// IsUnique implements sql.Index
func (di *DoltIndexImpl) IsUnique() bool {
	return di.Unique
}

// Comment implements sql.Index
func (di *DoltIndexImpl) Comment() string {
	return di.CommentStr
}

// IndexType implements sql.Index
func (di *DoltIndexImpl) IndexType() string {
	return "BTREE"
}

// IsGenerated implements sql.Index
func (di *DoltIndexImpl) IsGenerated() bool {
	return di.GeneratedBool
}

// Schema returns the dolt Table schema of this index.
func (di *DoltIndexImpl) Schema() schema.Schema {
	return di.TableSch
}

// IndexSchema returns the dolt index schema.
func (di *DoltIndexImpl) IndexSchema() schema.Schema {
	return di.IndexSch
}

// Table implements sql.Index
func (di *DoltIndexImpl) Table() string {
	return di.TableName
}

// TableData returns the map of Table data for this index (the map of the target Table, not the index storage Table)
func (di *DoltIndexImpl) TableData() types.Map {
	return di.TableRowData
}

// IndexRowData returns the map of index row data.
func (di *DoltIndexImpl) IndexRowData() types.Map {
	return di.RowIndexData
}

// keysToTuple returns a tuple that indicates the starting point for an index. The empty tuple will cause the index to
// start at the very beginning.
func (di *DoltIndexImpl) keysToTuple(ctx *sql.Context, keys []interface{}) (types.Tuple, error) {
	nbf := di.RowIndexData.Format()
	if len(keys) > len(di.Cols) {
		return types.EmptyTuple(nbf), errors.New("too many keys for the column count")
	}

	vals := make([]types.Value, len(keys)*2)
	for i := range keys {
		col := di.Cols[i]
		// As an example, if our TypeInfo is Int8, we should not fail to create a tuple if we are returning all keys
		// that have a value of less than 9001, thus we promote the TypeInfo to the widest type.
		val, err := col.TypeInfo.Promote().ConvertValueToNomsValue(ctx, di.Vrw, keys[i])
		if err != nil {
			return types.EmptyTuple(nbf), err
		}
		vals[2*i] = types.Uint(col.Tag)
		vals[2*i+1] = val
	}
	return types.NewTuple(nbf, vals...)
}
