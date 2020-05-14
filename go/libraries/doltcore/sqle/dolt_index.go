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

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/liquidata-inc/dolt/go/store/types"
)

type DoltIndex interface {
	sql.Index
	sql.AscendIndex
	sql.DescendIndex
	DoltDatabase() Database
	Schema() schema.Schema
	TableData() types.Map
}

type doltIndex struct {
	cols         []schema.Column
	ctx          *sql.Context
	db           Database
	driver       *DoltIndexDriver
	id           string
	indexRowData types.Map
	indexSch     schema.Schema
	table        *doltdb.Table
	tableData    types.Map
	tableName    string
	tableSch     schema.Schema
}

var _ DoltIndex = (*doltIndex)(nil)

func (di *doltIndex) AscendGreaterOrEqual(keys ...interface{}) (sql.IndexLookup, error) {
	tpl, err := di.keysToTuple(keys, false)
	if err != nil {
		return nil, err
	}
	readRange := &noms.ReadRange{Start: tpl, Inclusive: true, Reverse: false, Check: func(tuple types.Tuple) (bool, error) {
		return true, nil
	}}
	return di.rangeToIter(readRange)
}

func (di *doltIndex) AscendLessThan(keys ...interface{}) (sql.IndexLookup, error) {
	tpl, err := di.keysToTuple(keys, false)
	if err != nil {
		return nil, err
	}
	readRange := &noms.ReadRange{Start: tpl, Inclusive: false, Reverse: true, Check: func(tuple types.Tuple) (bool, error) {
		return true, nil
	}}
	return di.rangeToIter(readRange)
}

// TODO: rename this from AscendRange to BetweenRange or something
func (di *doltIndex) AscendRange(greaterOrEqual, lessThanOrEqual []interface{}) (sql.IndexLookup, error) {
	greaterTpl, err := di.keysToTuple(greaterOrEqual, false)
	if err != nil {
		return nil, err
	}
	lessTpl, err := di.keysToTuple(lessThanOrEqual, true)
	if err != nil {
		return nil, err
	}
	nbf := di.indexRowData.Format()
	readRange := &noms.ReadRange{Start: greaterTpl, Inclusive: true, Reverse: false, Check: func(tuple types.Tuple) (bool, error) {
		return tuple.Less(nbf, lessTpl)
	}}
	return di.rangeToIter(readRange)
}

func (di *doltIndex) DescendGreater(keys ...interface{}) (sql.IndexLookup, error) {
	tpl, err := di.keysToTuple(keys, true)
	if err != nil {
		return nil, err
	}
	readRange := &noms.ReadRange{Start: tpl, Inclusive: true, Reverse: false, Check: func(tuple types.Tuple) (bool, error) {
		return true, nil
	}}
	return di.rangeToIter(readRange)
}

func (di *doltIndex) DescendLessOrEqual(keys ...interface{}) (sql.IndexLookup, error) {
	tpl, err := di.keysToTuple(keys, true)
	if err != nil {
		return nil, err
	}
	readRange := &noms.ReadRange{Start: tpl, Inclusive: true, Reverse: true, Check: func(tuple types.Tuple) (bool, error) {
		return true, nil
	}}
	return di.rangeToIter(readRange)
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
	tpl, err := di.keysToTuple(keys, false)
	if err != nil {
		return nil, err
	}
	nbf := di.indexRowData.Format()
	readRange := &noms.ReadRange{Start: tpl, Inclusive: true, Reverse: false, Check: func(tuple types.Tuple) (bool, error) {
		return tuple.StartsWith(nbf, tpl)
	}}
	return di.rangeToIter(readRange)
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

func (di *doltIndex) TableData() types.Map {
	return di.tableData
}

func (di *doltIndex) keysToTuple(keys []interface{}, appendMaxValue bool) (types.Tuple, error) {
	nbf := di.indexRowData.Format()
	if len(di.cols) != len(keys) {
		return types.EmptyTuple(nbf), errors.New("keys must specify all columns for an index")
	}
	var vals []types.Value
	for i, col := range di.cols {
		val, err := col.TypeInfo.ConvertValueToNomsValue(keys[i])
		if err != nil {
			return types.EmptyTuple(nbf), err
		}
		vals = append(vals, types.Uint(col.Tag), val)
	}
	// In the case of possible partial keys, we may need to match at the beginning or end for matched values, so we
	// append a tag that is beyond the allowed maximum. This will be ignored if it's a full key and not a partial key.
	if appendMaxValue {
		vals = append(vals, types.Uint(uint64(0xffffffffffffffff)))
	}
	return types.NewTuple(nbf, vals...)
}

func (di *doltIndex) rangeToIter(readRange *noms.ReadRange) (sql.IndexLookup, error) {
	var mapIter table.TableReadCloser = noms.NewNomsRangeReader(di.indexSch, di.indexRowData, []*noms.ReadRange{readRange})
	return &doltIndexLookup{
		di,
		&doltIndexKeyIter{
			indexMapIter: mapIter,
		},
	}, nil
}
