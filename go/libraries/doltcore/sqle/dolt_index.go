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
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/val"
)

type DoltIndex interface {
	sql.Index
	IndexSchema() schema.Schema
	IndexRowData() prolly.Map
}

type doltIndex struct {
	id string
	db sql.Database

	indexRowData prolly.Map
	indexSch     schema.Schema
	cols         []schema.Column
	keyBuilder   *val.TupleBuilder

	tableData prolly.Map
	tableName string

	unique    bool
	comment   string
	generated bool
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
func (di *doltIndex) NewLookup(ctx *sql.Context, sqlRanges ...sql.Range) (sql.IndexLookup, error) {
	var err error
	sqlRanges, err = pruneEmptyRanges(sqlRanges)
	if err != nil {
		return nil, err
	}

	ranges := make([]prolly.Range, len(sqlRanges))
	for i, sr := range sqlRanges {
		ranges[i], err = prollyRangeFromSqlRange(sr, di.keyBuilder)
		if err != nil {
			return nil, err
		}
	}

	return &doltIndexLookup{
		idx:       di,
		ranges:    ranges,
		sqlRanges: sqlRanges,
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

// IndexSchema returns the dolt index schema.
func (di *doltIndex) IndexSchema() schema.Schema {
	return di.indexSch
}

// Table implements sql.Index
func (di *doltIndex) Table() string {
	return di.tableName
}

// IndexRowData returns the map of index row data.
func (di *doltIndex) IndexRowData() prolly.Map {
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
func (di *doltIndex) keysToTuple(ctx *sql.Context, keys []interface{}) (val.Tuple, error) {
	panic("unimplemented")
}
