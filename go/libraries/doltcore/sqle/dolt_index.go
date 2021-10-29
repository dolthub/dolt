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

package sqle

import (
	"errors"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/val"
)

type DoltIndex interface {
	sql.Index
	sql.AscendIndex
	sql.DescendIndex
	sql.NegateIndex
	Schema() schema.Schema
	IndexSchema() schema.Schema
	TableData() prolly.Map
	IndexRowData() prolly.Map
	Equals(index DoltIndex) bool
}

type doltIndex struct {
	id        string
	unique    bool
	generated bool
	comment   string
	tableName string
	table     *doltdb.Table
	db        sql.Database

	keyBldr   *val.TupleBuilder
	indexSch  schema.Schema
	indexRows prolly.Map

	tableSch  schema.Schema
	tableRows prolly.Map
}

//TODO: have queries using IS NULL make use of indexes
var _ DoltIndex = (*doltIndex)(nil)

// AscendGreaterOrEqual implements sql.AscendIndex
func (di *doltIndex) AscendGreaterOrEqual(keys ...interface{}) (sql.IndexLookup, error) {
	panic("unimplement")
	//tpl, err := di.tupleFromKeys(keys)
	//if err != nil {
	//	return nil, err
	//}
	//return &doltIndexLookup{
	//	idx: di,
	//	ranges: []lookup.Range{
	//		lookup.GreaterOrEqualRange(tpl),
	//	},
	//}, nil
}

// AscendLessThan implements sql.AscendIndex
func (di *doltIndex) AscendLessThan(keys ...interface{}) (sql.IndexLookup, error) {
	panic("unimplement")
	//tpl, err := di.tupleFromKeys(keys)
	//if err != nil {
	//	return nil, err
	//}
	//return &doltIndexLookup{
	//	idx: di,
	//	ranges: []lookup.Range{
	//		lookup.LessThanRange(tpl),
	//	},
	//}, nil
}

// AscendRange implements sql.AscendIndex
// TODO: rename this from AscendRange to BetweenRange or something
func (di *doltIndex) AscendRange(greaterOrEqual, lessThanOrEqual []interface{}) (sql.IndexLookup, error) {
	panic("unimplement")
	//greaterTpl, err := di.tupleFromKeys(greaterOrEqual)
	//if err != nil {
	//	return nil, err
	//}
	//lessTpl, err := di.tupleFromKeys(lessThanOrEqual)
	//if err != nil {
	//	return nil, err
	//}
	//r, err := lookup.ClosedRange(greaterTpl, lessTpl)
	//if err != nil {
	//	return nil, err
	//}
	//return &doltIndexLookup{
	//	idx: di,
	//	ranges: []lookup.Range{
	//		r,
	//	},
	//}, nil
}

// DescendGreater implements sql.DescendIndex
func (di *doltIndex) DescendGreater(keys ...interface{}) (sql.IndexLookup, error) {
	panic("unimplement")
	//tpl, err := di.tupleFromKeys(keys)
	//if err != nil {
	//	return nil, err
	//}
	//r, err := lookup.GreaterThanRange(tpl)
	//if err != nil {
	//	return nil, err
	//}
	//return &doltIndexLookup{
	//	idx: di,
	//	ranges: []lookup.Range{
	//		r,
	//	},
	//}, nil
}

// DescendLessOrEqual implements sql.DescendIndex
func (di *doltIndex) DescendLessOrEqual(keys ...interface{}) (sql.IndexLookup, error) {
	panic("unimplement")
	//tpl, err := di.tupleFromKeys(keys)
	//if err != nil {
	//	return nil, err
	//}
	//r, err := lookup.LessOrEqualRange(tpl)
	//if err != nil {
	//	return nil, err
	//}
	//return &doltIndexLookup{
	//	idx: di,
	//	ranges: []lookup.Range{
	//		r,
	//	},
	//}, nil
}

// DescendRange implements sql.DescendIndex
// TODO: fix go-mysql-server to remove this duplicate function
func (di *doltIndex) DescendRange(lessOrEqual, greaterOrEqual []interface{}) (sql.IndexLookup, error) {
	return di.AscendRange(greaterOrEqual, lessOrEqual)
}

// Database implement sql.Index
func (di *doltIndex) Database() string {
	return di.db.Name()
}

// Expressions implements sql.Index
func (di *doltIndex) Expressions() []string {
	cols := di.indexSch.GetAllCols().GetColumns()
	strs := make([]string, len(cols))
	for i, col := range cols {
		strs[i] = di.tableName + "." + col.Name
	}
	return strs
}

// Get implements sql.Index
func (di *doltIndex) Get(keys ...interface{}) (sql.IndexLookup, error) {
	tup := tupleFromSqlValues(di.keyBldr, keys...)
	return &doltIndexLookup{
		idx:    di,
		ranges: []prolly.Range{{Point: tup}},
	}, nil
}

// Not implements sql.NegateIndex
func (di *doltIndex) Not(keys ...interface{}) (sql.IndexLookup, error) {
	panic("unimplement")
	//tpl, err := di.tupleFromKeys(keys)
	//if err != nil {
	//	return nil, err
	//}
	//r1 := lookup.LessThanRange(tpl)
	//r2, err := lookup.GreaterThanRange(tpl)
	//if err != nil {
	//	return nil, err
	//}
	//return &doltIndexLookup{
	//	idx: di,
	//	ranges: []lookup.Range{
	//		r1,
	//		r2,
	//	},
	//}, nil
}

// Has implements sql.Index
func (*doltIndex) Has(partition sql.Partition, key ...interface{}) (bool, error) {
	return false, errors.New("unimplemented")
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

// Schema returns the dolt index schema.
func (di *doltIndex) IndexSchema() schema.Schema {
	return di.indexSch
}

// Table implements sql.Index
func (di *doltIndex) Table() string {
	return di.tableName
}

// TableData returns the map of table data for this index (the map of the target table, not the index storage table)
func (di *doltIndex) TableData() prolly.Map {
	return di.tableRows
}

// IndexRowData returns the map of index row data.
func (di *doltIndex) IndexRowData() prolly.Map {
	return di.indexRows
}

func (di *doltIndex) Equals(oIdx DoltIndex) bool {
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
