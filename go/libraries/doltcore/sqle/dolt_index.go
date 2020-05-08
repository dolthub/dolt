// Copyright 2020 Liquidata, Inc.
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

	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/store/types"
)

/*
TODO: implement these interfaces so that >, >=, <, and <= works with indexes

sql.AscendIndex

AscendGreaterOrEqual(keys ...interface{}) (sql.IndexLookup, error)
AscendLessThan(keys ...interface{}) (sql.IndexLookup, error)
AscendRange(greaterOrEqual, lessThan []interface{}) (sql.IndexLookup, error)

sql.DescendIndex

DescendGreater(keys ...interface{}) (sql.IndexLookup, error)
DescendLessOrEqual(keys ...interface{}) (sql.IndexLookup, error)
DescendRange(lessOrEqual, greaterThan []interface{}) (sql.IndexLookup, error)
*/

type DoltIndex interface {
	sql.Index
	DoltDatabase() Database
	Schema() schema.Schema
}

type doltIndex struct {
	cols      []schema.Column
	ctx       *sql.Context
	db        Database
	driver    *DoltIndexDriver
	id        string
	mapSch    schema.Schema
	rowData   types.Map
	table     *doltdb.Table
	tableName string
	tableSch  schema.Schema
}

var _ DoltIndex = (*doltIndex)(nil)

func (di *doltIndex) Database() string {
	return di.db.name
}

func (di *doltIndex) DoltDatabase() Database {
	return di.db
}

func (di *doltIndex) Driver() string {
	return di.driver.ID()
}

func (di *doltIndex) Expressions() []string {
	strs := make([]string, len(di.cols))
	for i, col := range di.cols {
		strs[i] = di.tableName + "." + col.Name
	}
	return strs
}

func (di *doltIndex) Get(key ...interface{}) (sql.IndexLookup, error) {
	if len(di.cols) != len(key) {
		return nil, errors.New("key must specify all columns for index")
	}

	equals := make([]*expression.Equals, len(key))
	for i := 0; i < len(key); i++ {
		equals[i] = expression.NewEquals(
			expression.NewGetField(i, di.cols[i].TypeInfo.ToSqlType(), di.cols[i].Name, di.cols[i].IsNullable()),
			expression.NewLiteral(key[i], di.cols[i].TypeInfo.ToSqlType()),
		)
	}
	var lastExpr sql.Expression = equals[len(equals)-1]
	for i := len(equals) - 2; i >= 0; i-- {
		lastExpr = expression.NewAnd(equals[i], lastExpr)
	}

	crf, err := CreateReaderFuncLimitedByExpressions(di.rowData.Format(), di.mapSch, []sql.Expression{lastExpr})
	if err != nil {
		return nil, err
	}
	mapIter, err := crf(di.ctx, di.rowData)
	if err != nil {
		return nil, err
	}

	return &doltIndexLookup{
		di,
		&doltIndexKeyIter{
			indexMapIter: mapIter,
		},
	}, nil
}

func (*doltIndex) Has(partition sql.Partition, key ...interface{}) (bool, error) {
	// appears to be unused for the moment
	panic("implement me")
}

func (di *doltIndex) ID() string {
	return di.id
}

func (di *doltIndex) Schema() schema.Schema {
	return di.tableSch
}

func (di *doltIndex) Table() string {
	return di.tableName
}
