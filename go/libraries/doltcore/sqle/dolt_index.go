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

type DoltIndex interface {
	sql.Index
	sql.AscendIndex
	sql.DescendIndex
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

type compExpr func(left sql.Expression, right sql.Expression) sql.Expression

var _ DoltIndex = (*doltIndex)(nil)

func (di *doltIndex) AscendGreaterOrEqual(keys ...interface{}) (sql.IndexLookup, error) {
	if len(di.cols) != len(keys) {
		return nil, errors.New("keys must specify all columns for index")
	}
	return di.keysToIter(di.keysToExpr(keys, func(left sql.Expression, right sql.Expression) sql.Expression {
		return expression.NewGreaterThanOrEqual(left, right)
	}))
}

func (di *doltIndex) AscendLessThan(keys ...interface{}) (sql.IndexLookup, error) {
	if len(di.cols) != len(keys) {
		return nil, errors.New("keys must specify all columns for index")
	}
	return di.keysToIter(di.keysToExpr(keys, func(left sql.Expression, right sql.Expression) sql.Expression {
		return expression.NewLessThan(left, right)
	}))
}

// TODO: rename this from AscendRange to BetweenRange or something
func (di *doltIndex) AscendRange(greaterOrEqual, lessThanOrEqual []interface{}) (sql.IndexLookup, error) {
	if len(di.cols) != len(greaterOrEqual) || len(di.cols) != len(lessThanOrEqual) {
		return nil, errors.New("keys must specify all columns for index")
	}
	greaterEqualExprs := di.keysToExpr(greaterOrEqual, func(left sql.Expression, right sql.Expression) sql.Expression {
		return expression.NewGreaterThanOrEqual(left, right)
	})
	lessEqualExprs := di.keysToExpr(lessThanOrEqual, func(left sql.Expression, right sql.Expression) sql.Expression {
		return expression.NewLessThanOrEqual(left, right)
	})
	return di.keysToIter(expression.NewAnd(greaterEqualExprs, lessEqualExprs))
}

func (di *doltIndex) DescendGreater(keys ...interface{}) (sql.IndexLookup, error) {
	if len(di.cols) != len(keys) {
		return nil, errors.New("keys must specify all columns for index")
	}
	return di.keysToIter(di.keysToExpr(keys, func(left sql.Expression, right sql.Expression) sql.Expression {
		return expression.NewGreaterThan(left, right)
	}))
}

func (di *doltIndex) DescendLessOrEqual(keys ...interface{}) (sql.IndexLookup, error) {
	if len(di.cols) != len(keys) {
		return nil, errors.New("keys must specify all columns for index")
	}
	return di.keysToIter(di.keysToExpr(keys, func(left sql.Expression, right sql.Expression) sql.Expression {
		return expression.NewLessThanOrEqual(left, right)
	}))
}

// TODO: fix go-mysql-server to remove this duplicate function
func (di *doltIndex) DescendRange(lessOrEqual, greaterOrEqual []interface{}) (sql.IndexLookup, error) {
	return di.AscendRange(greaterOrEqual, lessOrEqual)
}

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

func (di *doltIndex) Get(keys ...interface{}) (sql.IndexLookup, error) {
	if len(di.cols) != len(keys) {
		return nil, errors.New("keys must specify all columns for index")
	}
	return di.keysToIter(di.keysToExpr(keys, func(left sql.Expression, right sql.Expression) sql.Expression {
		return expression.NewEquals(left, right)
	}))
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

func (di *doltIndex) keysToExpr(keys []interface{}, compFunc compExpr) sql.Expression {
	exprs := make([]sql.Expression, len(keys))
	for i := 0; i < len(keys); i++ {
		exprs[i] = compFunc(
			expression.NewGetField(i, di.cols[i].TypeInfo.ToSqlType(), di.cols[i].Name, di.cols[i].IsNullable()),
			expression.NewLiteral(keys[i], di.cols[i].TypeInfo.ToSqlType()),
		)
	}
	lastExpr := exprs[len(exprs)-1]
	for i := len(exprs) - 2; i >= 0; i-- {
		lastExpr = expression.NewAnd(exprs[i], lastExpr)
	}
	return lastExpr
}

func (di *doltIndex) keysToIter(expr sql.Expression) (sql.IndexLookup, error) {
	crf, err := CreateReaderFuncLimitedByExpressions(di.rowData.Format(), di.mapSch, []sql.Expression{expr})
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
