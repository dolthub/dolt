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
	"fmt"

	"github.com/src-d/go-mysql-server/sql"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
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

type doltIndexPk struct {
	db        Database
	sch       schema.Schema
	tableName string
	table     *doltdb.Table
	driver    *DoltIndexDriver
	cols      []schema.Column
	ctx       *sql.Context
}

var _ DoltIndex = (*doltIndexPk)(nil)

func (pdi *doltIndexPk) Database() string {
	return pdi.db.name
}

func (pdi *doltIndexPk) DoltDatabase() Database {
	return pdi.db
}

func (pdi *doltIndexPk) Driver() string {
	return pdi.driver.ID()
}

func (pdi *doltIndexPk) Expressions() []string {
	strs := make([]string, len(pdi.cols))
	for i, col := range pdi.cols {
		strs[i] = pdi.tableName + "." + col.Name
	}
	return strs
}

func (pdi *doltIndexPk) Get(key ...interface{}) (sql.IndexLookup, error) {
	//TODO: replace all of this when partial keys land
	if len(pdi.cols) != len(key) {
		return nil, errors.New("index does not match the given key length")
	}

	taggedVals := make(row.TaggedValues)
	for i, col := range pdi.cols {
		val, err := col.TypeInfo.ConvertValueToNomsValue(key[i])
		if err != nil {
			return nil, err
		}
		taggedVals[col.Tag] = val
	}

	if pdi.sch.GetPKCols().Size() == 1 {
		return &doltIndexLookup{
			idx: pdi,
			keyIter: &doltIndexSinglePkKeyIter{
				hasReturned: false,
				val:         taggedVals,
			},
		}, nil
	} else {
		rowData, err := pdi.table.GetRowData(pdi.ctx)
		if err != nil {
			return nil, err
		}
		rowDataIter, err := rowData.Iterator(pdi.ctx)
		if err != nil {
			return nil, err
		}
		return &doltIndexLookup{
			idx: pdi,
			keyIter: &doltIndexMultiPkKeyIter{
				tableName:    pdi.tableName,
				tableMapIter: rowDataIter,
				val:          taggedVals,
			},
		}, nil
	}
}

func (*doltIndexPk) Has(partition sql.Partition, key ...interface{}) (bool, error) {
	// appears to be unused for the moment
	panic("not used")
}

func (pdi *doltIndexPk) ID() string {
	return fmt.Sprintf("%s:primaryKey%v", pdi.tableName, len(pdi.cols))
}

func (pdi *doltIndexPk) Schema() schema.Schema {
	return pdi.sch
}

func (pdi *doltIndexPk) Table() string {
	return pdi.tableName
}

type doltIndex struct {
	db        Database
	driver    *DoltIndexDriver
	tableSch  schema.Schema
	tableName string
	table     *doltdb.Table
	index     schema.InnerIndex
	ctx       *sql.Context
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
	tags := di.index.Tags()
	strs := make([]string, len(tags))
	for i, tag := range tags {
		col, _ := di.index.GetColumn(tag)
		strs[i] = di.tableName + "." + col.Name
	}
	return strs
}

func (di *doltIndex) Get(key ...interface{}) (sql.IndexLookup, error) {
	if len(di.index.Tags()) != len(key) {
		return nil, errors.New("key must specify all columns for inner index")
	}

	taggedVals := make(row.TaggedValues)
	for i, tag := range di.index.Tags() {
		if i >= len(key) {
			break
		}
		col, _ := di.index.GetColumn(tag)
		val, err := col.TypeInfo.ConvertValueToNomsValue(key[i])
		if err != nil {
			return nil, err
		}
		taggedVals[tag] = val
	}

	rowData, err := di.table.GetIndexRowData(di.ctx, di.index.OuterIndex().Name())
	if err != nil {
		return nil, err
	}
	rowDataIter, err := rowData.Iterator(di.ctx)
	if err != nil {
		return nil, err
	}
	return &doltIndexLookup{
		di,
		&doltIndexKeyIter{
			index:        di.index,
			indexMapIter: rowDataIter,
			val:          taggedVals,
		},
	}, nil
}

func (*doltIndex) Has(partition sql.Partition, key ...interface{}) (bool, error) {
	// appears to be unused for the moment
	panic("implement me")
}

func (di *doltIndex) ID() string {
	return fmt.Sprintf("%s:%s%v", di.tableName, di.index.Name(), len(di.index.Tags()))
}

func (di *doltIndex) Schema() schema.Schema {
	return di.tableSch
}

func (di *doltIndex) Table() string {
	return di.tableName
}
