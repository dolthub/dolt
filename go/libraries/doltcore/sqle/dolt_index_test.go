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
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dtestutils"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
)

func TestDoltIndexEqual(t *testing.T) {
	indexMap := doltIndexSetup(t)

	tests := []struct {
		indexName    string
		keys         []interface{}
		expectedRows []sql.Row
	}{
		{
			"onepk:primaryKey",
			[]interface{}{1},
			[]sql.Row{{1, 1, 1}},
		},
		{
			"onepk:primaryKey",
			[]interface{}{3},
			[]sql.Row{{3, 3, 3}},
		},
		{
			"onepk:primaryKey",
			[]interface{}{0},
			[]sql.Row{},
		},
		{
			"onepk:primaryKey",
			[]interface{}{5},
			[]sql.Row{},
		},
		{
			"onepk:idx_v1",
			[]interface{}{1},
			[]sql.Row{{1, 1, 1}, {2, 1, 2}},
		},
		{
			"onepk:idx_v1",
			[]interface{}{3},
			[]sql.Row{{3, 3, 3}},
		},
		{
			"twopk:primaryKey",
			[]interface{}{1, 1},
			[]sql.Row{{1, 1, 3, 3}},
		},
		{
			"twopk:primaryKey",
			[]interface{}{2, 0},
			[]sql.Row{},
		},
		{
			"twopk:primaryKey",
			[]interface{}{2, 1},
			[]sql.Row{{2, 1, 4, 4}},
		},
		{
			"twopk:idx_v2v1",
			[]interface{}{3, 4},
			[]sql.Row{{2, 2, 4, 3}},
		},
		{
			"twopk:idx_v2v1",
			[]interface{}{4, 3},
			[]sql.Row{{1, 2, 3, 4}},
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%s|%v", test.indexName, test.keys), func(t *testing.T) {
			index, ok := indexMap[test.indexName]
			require.True(t, ok)
			testDoltIndex(t, test.keys, test.expectedRows, index.Get)
		})
	}
}

func TestDoltIndexGreaterThan(t *testing.T) {
	indexMap := doltIndexSetup(t)

	tests := []struct {
		indexName    string
		keys         []interface{}
		expectedRows []sql.Row
	}{
		{
			"onepk:primaryKey",
			[]interface{}{1},
			[]sql.Row{{2, 1, 2}, {3, 3, 3}, {4, 4, 3}},
		},
		{
			"onepk:primaryKey",
			[]interface{}{3},
			[]sql.Row{{4, 4, 3}},
		},
		{
			"onepk:primaryKey",
			[]interface{}{4},
			[]sql.Row{},
		},
		{
			"onepk:idx_v1",
			[]interface{}{1},
			[]sql.Row{{3, 3, 3}, {4, 4, 3}},
		},
		{
			"onepk:idx_v1",
			[]interface{}{3},
			[]sql.Row{{4, 4, 3}},
		},
		{
			"onepk:idx_v1",
			[]interface{}{4},
			[]sql.Row{},
		},
		{
			"twopk:primaryKey",
			[]interface{}{1, 1},
			[]sql.Row{{1, 2, 3, 4}, {2, 1, 4, 4}, {2, 2, 4, 3}},
		},
		{
			"twopk:primaryKey",
			[]interface{}{2, 1},
			[]sql.Row{{2, 2, 4, 3}},
		},
		{
			"twopk:idx_v2v1",
			[]interface{}{3, 4},
			[]sql.Row{{1, 2, 3, 4}, {2, 1, 4, 4}},
		},
		{
			"twopk:idx_v2v1",
			[]interface{}{4, 3},
			[]sql.Row{{2, 1, 4, 4}},
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%s|%v", test.indexName, test.keys), func(t *testing.T) {
			index, ok := indexMap[test.indexName]
			require.True(t, ok)
			testDoltIndex(t, test.keys, test.expectedRows, index.DescendGreater)
		})
	}
}

func TestDoltIndexGreaterThanOrEqual(t *testing.T) {
	indexMap := doltIndexSetup(t)

	tests := []struct {
		indexName    string
		keys         []interface{}
		expectedRows []sql.Row
	}{
		{
			"onepk:primaryKey",
			[]interface{}{1},
			[]sql.Row{{1, 1, 1}, {2, 1, 2}, {3, 3, 3}, {4, 4, 3}},
		},
		{
			"onepk:primaryKey",
			[]interface{}{3},
			[]sql.Row{{3, 3, 3}, {4, 4, 3}},
		},
		{
			"onepk:primaryKey",
			[]interface{}{4},
			[]sql.Row{{4, 4, 3}},
		},
		{
			"onepk:idx_v1",
			[]interface{}{1},
			[]sql.Row{{1, 1, 1}, {2, 1, 2}, {3, 3, 3}, {4, 4, 3}},
		},
		{
			"onepk:idx_v1",
			[]interface{}{3},
			[]sql.Row{{3, 3, 3}, {4, 4, 3}},
		},
		{
			"onepk:idx_v1",
			[]interface{}{4},
			[]sql.Row{{4, 4, 3}},
		},
		{
			"twopk:primaryKey",
			[]interface{}{1, 1},
			[]sql.Row{{1, 1, 3, 3}, {1, 2, 3, 4}, {2, 1, 4, 4}, {2, 2, 4, 3}},
		},
		{
			"twopk:primaryKey",
			[]interface{}{2, 1},
			[]sql.Row{{2, 1, 4, 4}, {2, 2, 4, 3}},
		},
		{
			"twopk:idx_v2v1",
			[]interface{}{3, 4},
			[]sql.Row{{2, 2, 4, 3}, {1, 2, 3, 4}, {2, 1, 4, 4}},
		},
		{
			"twopk:idx_v2v1",
			[]interface{}{4, 3},
			[]sql.Row{{1, 2, 3, 4}, {2, 1, 4, 4}},
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%s|%v", test.indexName, test.keys), func(t *testing.T) {
			index, ok := indexMap[test.indexName]
			require.True(t, ok)
			testDoltIndex(t, test.keys, test.expectedRows, index.AscendGreaterOrEqual)
		})
	}
}

func TestDoltIndexLessThan(t *testing.T) {
	indexMap := doltIndexSetup(t)

	tests := []struct {
		indexName    string
		keys         []interface{}
		expectedRows []sql.Row
	}{
		{
			"onepk:primaryKey",
			[]interface{}{1},
			[]sql.Row{},
		},
		{
			"onepk:primaryKey",
			[]interface{}{3},
			[]sql.Row{{1, 1, 1}, {2, 1, 2}},
		},
		{
			"onepk:primaryKey",
			[]interface{}{4},
			[]sql.Row{{1, 1, 1}, {2, 1, 2}, {3, 3, 3}},
		},
		{
			"onepk:idx_v1",
			[]interface{}{1},
			[]sql.Row{},
		},
		{
			"onepk:idx_v1",
			[]interface{}{3},
			[]sql.Row{{1, 1, 1}, {2, 1, 2}},
		},
		{
			"onepk:idx_v1",
			[]interface{}{4},
			[]sql.Row{{1, 1, 1}, {2, 1, 2}, {3, 3, 3}},
		},
		{
			"twopk:primaryKey",
			[]interface{}{1, 1},
			[]sql.Row{},
		},
		{
			"twopk:primaryKey",
			[]interface{}{2, 1},
			[]sql.Row{{1, 1, 3, 3}, {1, 2, 3, 4}},
		},
		{
			"twopk:idx_v2v1",
			[]interface{}{3, 4},
			[]sql.Row{{1, 1, 3, 3}},
		},
		{
			"twopk:idx_v2v1",
			[]interface{}{4, 3},
			[]sql.Row{{1, 1, 3, 3}, {2, 2, 4, 3}},
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%s|%v", test.indexName, test.keys), func(t *testing.T) {
			index, ok := indexMap[test.indexName]
			require.True(t, ok)
			testDoltIndex(t, test.keys, test.expectedRows, index.AscendLessThan)
		})
	}
}

func TestDoltIndexLessThanOrEqual(t *testing.T) {
	indexMap := doltIndexSetup(t)

	tests := []struct {
		indexName    string
		keys         []interface{}
		expectedRows []sql.Row
	}{
		{
			"onepk:primaryKey",
			[]interface{}{1},
			[]sql.Row{{1, 1, 1}},
		},
		{
			"onepk:primaryKey",
			[]interface{}{3},
			[]sql.Row{{1, 1, 1}, {2, 1, 2}, {3, 3, 3}},
		},
		{
			"onepk:primaryKey",
			[]interface{}{4},
			[]sql.Row{{1, 1, 1}, {2, 1, 2}, {3, 3, 3}, {4, 4, 3}},
		},
		{
			"onepk:idx_v1",
			[]interface{}{1},
			[]sql.Row{{1, 1, 1}, {2, 1, 2}},
		},
		{
			"onepk:idx_v1",
			[]interface{}{3},
			[]sql.Row{{1, 1, 1}, {2, 1, 2}, {3, 3, 3}},
		},
		{
			"onepk:idx_v1",
			[]interface{}{4},
			[]sql.Row{{1, 1, 1}, {2, 1, 2}, {3, 3, 3}, {4, 4, 3}},
		},
		{
			"twopk:primaryKey",
			[]interface{}{1, 1},
			[]sql.Row{{1, 1, 3, 3}},
		},
		{
			"twopk:primaryKey",
			[]interface{}{2, 1},
			[]sql.Row{{1, 1, 3, 3}, {1, 2, 3, 4}, {2, 1, 4, 4}},
		},
		{
			"twopk:idx_v2v1",
			[]interface{}{3, 4},
			[]sql.Row{{1, 1, 3, 3}, {2, 2, 4, 3}},
		},
		{
			"twopk:idx_v2v1",
			[]interface{}{4, 3},
			[]sql.Row{{1, 1, 3, 3}, {1, 2, 3, 4}, {2, 2, 4, 3}},
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%s|%v", test.indexName, test.keys), func(t *testing.T) {
			index, ok := indexMap[test.indexName]
			require.True(t, ok)
			testDoltIndex(t, test.keys, test.expectedRows, index.DescendLessOrEqual)
		})
	}
}

func TestDoltIndexBetween(t *testing.T) {
	indexMap := doltIndexSetup(t)

	tests := []struct {
		indexName          string
		greaterThanOrEqual []interface{}
		lessThanOrEqual    []interface{}
		expectedRows       []sql.Row
	}{
		{
			"onepk:primaryKey",
			[]interface{}{1},
			[]interface{}{2},
			[]sql.Row{{1, 1, 1}, {2, 1, 2}},
		},
		{
			"onepk:primaryKey",
			[]interface{}{3},
			[]interface{}{3},
			[]sql.Row{{3, 3, 3}},
		},
		{
			"onepk:primaryKey",
			[]interface{}{4},
			[]interface{}{6},
			[]sql.Row{{4, 4, 3}},
		},
		{
			"onepk:primaryKey",
			[]interface{}{0},
			[]interface{}{10},
			[]sql.Row{{1, 1, 1}, {2, 1, 2}, {3, 3, 3}, {4, 4, 3}},
		},
		{
			"onepk:idx_v1",
			[]interface{}{1},
			[]interface{}{2},
			[]sql.Row{{1, 1, 1}, {2, 1, 2}},
		},
		{
			"onepk:idx_v1",
			[]interface{}{2},
			[]interface{}{4},
			[]sql.Row{{3, 3, 3}, {4, 4, 3}},
		},
		{
			"onepk:idx_v1",
			[]interface{}{1},
			[]interface{}{4},
			[]sql.Row{{1, 1, 1}, {2, 1, 2}, {3, 3, 3}, {4, 4, 3}},
		},
		{
			"twopk:primaryKey",
			[]interface{}{1, 1},
			[]interface{}{1, 1},
			[]sql.Row{{1, 1, 3, 3}},
		},
		{
			"twopk:primaryKey",
			[]interface{}{1, 1},
			[]interface{}{2, 1},
			[]sql.Row{{1, 1, 3, 3}, {1, 2, 3, 4}, {2, 1, 4, 4}},
		},
		{
			"twopk:primaryKey",
			[]interface{}{1, 1},
			[]interface{}{2, 5},
			[]sql.Row{{1, 1, 3, 3}, {1, 2, 3, 4}, {2, 1, 4, 4}, {2, 2, 4, 3}},
		},
		{
			"twopk:idx_v2v1",
			[]interface{}{3, 3},
			[]interface{}{3, 4},
			[]sql.Row{{1, 1, 3, 3}, {2, 2, 4, 3}},
		},
		{
			"twopk:idx_v2v1",
			[]interface{}{3, 4},
			[]interface{}{4, 4},
			[]sql.Row{{1, 2, 3, 4}, {2, 1, 4, 4}, {2, 2, 4, 3}},
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%s|%v%v", test.indexName, test.greaterThanOrEqual, test.lessThanOrEqual), func(t *testing.T) {
			index, ok := indexMap[test.indexName]
			require.True(t, ok)

			expectedRows := convertSqlRowToInt64(test.expectedRows)

			indexLookup, err := index.AscendRange(test.greaterThanOrEqual, test.lessThanOrEqual)
			require.NoError(t, err)
			dil, ok := indexLookup.(*doltIndexLookup)
			require.True(t, ok)
			indexIter, err := dil.RowIter(NewTestSQLCtx(context.Background()))
			require.NoError(t, err)

			var readRows []sql.Row
			var nextRow sql.Row
			for nextRow, err = indexIter.Next(); err == nil; nextRow, err = indexIter.Next() {
				readRows = append(readRows, nextRow)
			}
			require.Equal(t, io.EOF, err)

			assert.ElementsMatch(t, expectedRows, readRows)

			indexLookup, err = index.DescendRange(test.lessThanOrEqual, test.greaterThanOrEqual)
			require.NoError(t, err)
			dil, ok = indexLookup.(*doltIndexLookup)
			require.True(t, ok)
			indexIter, err = dil.RowIter(NewTestSQLCtx(context.Background()))
			require.NoError(t, err)

			readRows = nil
			for nextRow, err = indexIter.Next(); err == nil; nextRow, err = indexIter.Next() {
				readRows = append(readRows, nextRow)
			}
			require.Equal(t, io.EOF, err)

			assert.ElementsMatch(t, expectedRows, readRows)
		})
	}
}

func testDoltIndex(t *testing.T, keys []interface{}, expectedRows []sql.Row, compFunc func(keys ...interface{}) (sql.IndexLookup, error)) {
	indexLookup, err := compFunc(keys...)
	require.NoError(t, err)
	dil, ok := indexLookup.(*doltIndexLookup)
	require.True(t, ok)
	indexIter, err := dil.RowIter(NewTestSQLCtx(context.Background()))
	require.NoError(t, err)

	var readRows []sql.Row
	var nextRow sql.Row
	for nextRow, err = indexIter.Next(); err == nil; nextRow, err = indexIter.Next() {
		readRows = append(readRows, nextRow)
	}
	require.Equal(t, io.EOF, err)

	assert.ElementsMatch(t, convertSqlRowToInt64(expectedRows), readRows)
}

func doltIndexSetup(t *testing.T) map[string]DoltIndex {
	ctx := NewTestSQLCtx(context.Background())
	dEnv := dtestutils.CreateTestEnv()
	db := NewDatabase("dolt", dEnv.DoltDB, dEnv.RepoState, dEnv.RepoStateWriter())
	root, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		panic(err)
	}
	root, err = ExecuteSql(dEnv, root, `
CREATE TABLE onepk (
  pk1 BIGINT PRIMARY KEY,
  v1 BIGINT,
  v2 BIGINT
);
CREATE TABLE twopk (
  pk1 BIGINT,
  pk2 BIGINT,
  v1 BIGINT,
  v2 BIGINT,
  PRIMARY KEY (pk1,pk2)
);
CREATE INDEX idx_v1 ON onepk(v1);
CREATE INDEX idx_v2v1 ON twopk(v2, v1);
INSERT INTO onepk VALUES (1, 1, 1), (2, 1, 2), (3, 3, 3), (4, 4, 3);
INSERT INTO twopk VALUES (1, 1, 3, 3), (1, 2, 3, 4), (2, 1, 4, 4), (2, 2, 4, 3);
`)
	require.NoError(t, err)

	onepk, ok, err := root.GetTable(ctx, "onepk")
	require.NoError(t, err)
	require.True(t, ok)
	onepkSch, err := onepk.GetSchema(ctx)
	require.NoError(t, err)
	onepkRowData, err := onepk.GetRowData(ctx)
	require.NoError(t, err)

	twopk, ok, err := root.GetTable(ctx, "twopk")
	require.NoError(t, err)
	require.True(t, ok)
	twopkSch, err := twopk.GetSchema(ctx)
	require.NoError(t, err)
	twopkRowData, err := twopk.GetRowData(ctx)
	require.NoError(t, err)

	idx_v1 := onepkSch.Indexes().Get("idx_v1")
	idx_v1RowData, err := onepk.GetIndexRowData(ctx, idx_v1.Name())
	require.NoError(t, err)
	idx_v1Cols := make([]schema.Column, idx_v1.Count())
	for i, tag := range idx_v1.IndexedColumnTags() {
		idx_v1Cols[i], _ = idx_v1.GetColumn(tag)
	}

	idx_v2v1 := twopkSch.Indexes().Get("idx_v2v1")
	idx_v2v1RowData, err := twopk.GetIndexRowData(ctx, idx_v2v1.Name())
	require.NoError(t, err)
	idx_v2v1Cols := make([]schema.Column, idx_v2v1.Count())
	for i, tag := range idx_v2v1.IndexedColumnTags() {
		idx_v2v1Cols[i], _ = idx_v2v1.GetColumn(tag)
	}

	return map[string]DoltIndex{
		"onepk:primaryKey": &doltIndex{
			cols:         onepkSch.GetPKCols().GetColumns(),
			ctx:          ctx,
			db:           db,
			driver:       nil,
			id:           "onepk:primaryKey",
			indexRowData: onepkRowData,
			indexSch:     onepkSch,
			table:        onepk,
			tableData:    onepkRowData,
			tableName:    "onepk",
			tableSch:     onepkSch,
		},
		"twopk:primaryKey": &doltIndex{
			cols:         twopkSch.GetPKCols().GetColumns(),
			ctx:          ctx,
			db:           db,
			driver:       nil,
			id:           "twopk:primaryKey",
			indexRowData: twopkRowData,
			indexSch:     twopkSch,
			table:        twopk,
			tableData:    twopkRowData,
			tableName:    "twopk",
			tableSch:     twopkSch,
		},
		"onepk:idx_v1": &doltIndex{
			cols:         idx_v1Cols,
			ctx:          ctx,
			db:           db,
			driver:       nil,
			id:           "onepk:idx_v1",
			indexRowData: idx_v1RowData,
			indexSch:     idx_v1.Schema(),
			table:        onepk,
			tableData:    onepkRowData,
			tableName:    "onepk",
			tableSch:     onepkSch,
		},
		"twopk:idx_v2v1": &doltIndex{
			cols:         idx_v2v1Cols,
			ctx:          ctx,
			db:           db,
			driver:       nil,
			id:           "twopk:idx_v2v1",
			indexRowData: idx_v2v1RowData,
			indexSch:     idx_v2v1.Schema(),
			table:        twopk,
			tableData:    twopkRowData,
			tableName:    "twopk",
			tableSch:     twopkSch,
		},
	}
}
