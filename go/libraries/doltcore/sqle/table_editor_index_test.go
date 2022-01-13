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
	"context"
	"strings"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/store/types"
)

func setupEditorIndexTest(t *testing.T) (*env.DoltEnv, *doltdb.RootValue) {
	index_dEnv := dtestutils.CreateTestEnv()
	root, err := index_dEnv.WorkingRoot(context.Background())
	require.NoError(t, err)

	index_initialRoot, err := ExecuteSql(t, index_dEnv, root, `
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
CREATE TABLE oneuni (
  pk1 BIGINT PRIMARY KEY,
  v1 BIGINT,
  v2 BIGINT
);
CREATE TABLE twouni (
  pk1 BIGINT,
  pk2 BIGINT,
  v1 BIGINT,
  v2 BIGINT,
  PRIMARY KEY (pk1,pk2)
);
CREATE INDEX idx_v1 ON onepk(v1);
CREATE INDEX idx_v2v1 ON twopk(v2, v1);
CREATE UNIQUE INDEX idx_v1 ON oneuni(v1);
CREATE UNIQUE INDEX idx_v1v2 ON twouni(v1, v2);
`)

	require.NoError(t, err)

	return index_dEnv, index_initialRoot
}

func TestTableEditorIndexResults(t *testing.T) {
	tests := []struct {
		sqlStatement    string
		expectedIdxv1   []sql.Row
		expectedIdxv2v1 []sql.Row
	}{
		{
			`
INSERT INTO onepk VALUES (1, 2, 3), (4, 5, 6), (7, 8, 9);
INSERT INTO onepk VALUES (3, 2, 1), (6, 5, 4), (9, 8, 7);
`,
			[]sql.Row{{2, 1}, {2, 3}, {5, 4}, {5, 6}, {8, 7}, {8, 9}},
			[]sql.Row{},
		},
		{
			`
INSERT INTO onepk VALUES (1, 11, 111), (2, 22, 222), (3, 33, 333);
UPDATE onepk SET v1 = v1 - 1 WHERE pk1 > 1;
REPLACE INTO onepk VALUES (3, 55, 555), (4, 44, 444);
`,
			[]sql.Row{{11, 1}, {21, 2}, {44, 4}, {55, 3}},
			[]sql.Row{},
		},
		{
			`
INSERT INTO onepk VALUES (1, 11, 111), (2, 22, 222), (3, 33, 333);
INSERT INTO twopk VALUES (4, 44, 444, 4444);
REPLACE INTO twopk VALUES (4, 44, 111, 4444), (5, 55, 222, 5555), (6, 66, 333, 6666);
DELETE FROM onepk WHERE v2 = 222;
`,
			[]sql.Row{{11, 1}, {33, 3}},
			[]sql.Row{{4444, 111, 4, 44}, {5555, 222, 5, 55}, {6666, 333, 6, 66}},
		},
		{
			`
INSERT INTO onepk VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3);
DELETE FROM onepk WHERE pk1 % 2 = 1;
REPLACE INTO onepk VALUES (3, 6, 2), (-1, 4, -3);
UPDATE onepk SET pk1 = v1 + pk1 ORDER BY pk1 DESC;
`,
			[]sql.Row{{2, 4}, {4, 3}, {6, 9}},
			[]sql.Row{},
		},
	}

	for _, test := range tests {
		t.Run(test.sqlStatement, func(t *testing.T) {
			dEnv, initialRoot := setupEditorIndexTest(t)

			root := initialRoot
			for _, sqlStatement := range strings.Split(test.sqlStatement, ";") {
				var err error
				root, err = executeModify(t, context.Background(), dEnv, root, sqlStatement)
				require.NoError(t, err)
			}

			onepk, ok, err := root.GetTable(context.Background(), "onepk")
			require.NoError(t, err)
			require.True(t, ok)
			twopk, ok, err := root.GetTable(context.Background(), "twopk")
			require.NoError(t, err)
			require.True(t, ok)

			onepkSch, err := onepk.GetSchema(context.Background())
			require.NoError(t, err)
			twopkSch, err := twopk.GetSchema(context.Background())
			require.NoError(t, err)

			idx_v1 := onepkSch.Indexes().GetByName("idx_v1")
			require.NotNil(t, idx_v1)
			idx_v2v1 := twopkSch.Indexes().GetByName("idx_v2v1")
			require.NotNil(t, idx_v2v1)

			idx_v1RowData, err := onepk.GetNomsIndexRowData(context.Background(), idx_v1.Name())
			require.NoError(t, err)
			idx_v2v1RowData, err := twopk.GetNomsIndexRowData(context.Background(), idx_v2v1.Name())
			require.NoError(t, err)

			if assert.Equal(t, uint64(len(test.expectedIdxv1)), idx_v1RowData.Len()) && len(test.expectedIdxv1) > 0 {
				var sqlRows []sql.Row
				_ = idx_v1RowData.IterAll(context.Background(), func(key, value types.Value) error {
					r, err := row.FromNoms(idx_v1.Schema(), key.(types.Tuple), value.(types.Tuple))
					assert.NoError(t, err)
					sqlRow, err := sqlutil.DoltRowToSqlRow(r, idx_v1.Schema())
					assert.NoError(t, err)
					sqlRows = append(sqlRows, sqlRow)
					return nil
				})
				assert.ElementsMatch(t, convertSqlRowToInt64(test.expectedIdxv1), sqlRows)
			}
			if assert.Equal(t, uint64(len(test.expectedIdxv2v1)), idx_v2v1RowData.Len()) && len(test.expectedIdxv2v1) > 0 {
				var sqlRows []sql.Row
				_ = idx_v2v1RowData.IterAll(context.Background(), func(key, value types.Value) error {
					r, err := row.FromNoms(idx_v2v1.Schema(), key.(types.Tuple), value.(types.Tuple))
					assert.NoError(t, err)
					sqlRow, err := sqlutil.DoltRowToSqlRow(r, idx_v2v1.Schema())
					assert.NoError(t, err)
					sqlRows = append(sqlRows, sqlRow)
					return nil
				})
				assert.ElementsMatch(t, convertSqlRowToInt64(test.expectedIdxv2v1), sqlRows)
			}
		})
	}
}

func TestTableEditorUniqueIndexResults(t *testing.T) {
	tests := []struct {
		sqlStatement    string
		expectedIdxv1   []sql.Row
		expectedIdxv1v2 []sql.Row
		expectedErr     bool
	}{
		{
			`
INSERT INTO oneuni VALUES (1, 3, 2), (4, 6, 5), (7, 9, 8);
INSERT INTO oneuni VALUES (3, 1, 2), (6, 4, 5), (9, 7, 8);
`,
			[]sql.Row{{1, 3}, {3, 1}, {4, 6}, {6, 4}, {7, 9}, {9, 7}},
			[]sql.Row{},
			false,
		},
		{
			`
INSERT INTO oneuni VALUES (1, 11, 111), (2, 22, 222), (3, 33, 333);
UPDATE oneuni SET v1 = v1 - 1 WHERE pk1 > 1;
REPLACE INTO oneuni VALUES (3, 55, 555), (4, 44, 444);
`,
			[]sql.Row{{11, 1}, {21, 2}, {44, 4}, {55, 3}},
			[]sql.Row{},
			false,
		},
		{
			`
INSERT INTO oneuni VALUES (1, 11, 111), (2, 22, 222), (3, 33, 333);
REPLACE INTO oneuni VALUES (1, 11, 444);
INSERT INTO twouni VALUES (4, 44, 444, 4444);
REPLACE INTO twouni VALUES (4, 44, 111, 4444), (5, 55, 222, 5555), (6, 66, 333, 6666);
DELETE FROM oneuni WHERE v1 = 22;
`,
			[]sql.Row{{11, 1}, {33, 3}},
			[]sql.Row{{111, 4444, 4, 44}, {222, 5555, 5, 55}, {333, 6666, 6, 66}},
			false,
		},
		{
			`
INSERT INTO oneuni VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3);
DELETE FROM oneuni WHERE pk1 % 2 = 1;
REPLACE INTO oneuni VALUES (3, 6, 2), (-1, 4, -3);
UPDATE oneuni SET pk1 = v1 + v2;
`,
			[]sql.Row{{2, 4}, {4, 1}, {6, 8}},
			[]sql.Row{},
			false,
		},
		{
			`
INSERT INTO oneuni VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3);
DELETE FROM oneuni WHERE v1 < 3;
REPLACE INTO oneuni VALUES (4, 2, 2), (5, 3, 3), (3, 1, 1);
`,
			[]sql.Row{{1, 3}, {2, 4}, {3, 5}},
			[]sql.Row{},
			false,
		},
		{
			`
INSERT INTO oneuni VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3);
DELETE FROM oneuni WHERE v1 < 3;
REPLACE INTO oneuni VALUES (4, 2, 2), (5, 2, 3), (3, 1, 1);
`,
			[]sql.Row{{1, 3}, {2, 5}},
			[]sql.Row{},
			false,
		},
		{
			`
INSERT INTO oneuni VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3);
REPLACE INTO oneuni VALUES (1, 1, 1), (2, 2, 2), (3, 2, 3);
`,
			[]sql.Row{{1, 1}, {2, 3}},
			[]sql.Row{},
			false,
		},
		{
			`
INSERT INTO oneuni VALUES (1, 1, 1), (2, 1, 2), (3, 3, 3);
`,
			[]sql.Row{},
			[]sql.Row{},
			true,
		},
		{
			`
INSERT INTO oneuni VALUES (1, 2, 3), (2, 1, 4);
UPDATE oneuni SET v1 = v1 + pk1;
`,
			[]sql.Row{},
			[]sql.Row{},
			true,
		},
	}

	for _, test := range tests {
		t.Run(test.sqlStatement, func(t *testing.T) {
			dEnv, initialRoot := setupEditorIndexTest(t)

			root := initialRoot
			var err error
			for _, sqlStatement := range strings.Split(test.sqlStatement, ";") {
				root, err = executeModify(t, context.Background(), dEnv, root, sqlStatement)
				if err != nil {
					break
				}
			}
			if test.expectedErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			oneuni, ok, err := root.GetTable(context.Background(), "oneuni")
			require.NoError(t, err)
			require.True(t, ok)
			twouni, ok, err := root.GetTable(context.Background(), "twouni")
			require.NoError(t, err)
			require.True(t, ok)

			oneuniSch, err := oneuni.GetSchema(context.Background())
			require.NoError(t, err)
			twouniSch, err := twouni.GetSchema(context.Background())
			require.NoError(t, err)

			idx_v1 := oneuniSch.Indexes().GetByName("idx_v1")
			require.NotNil(t, idx_v1)
			idx_v1v2 := twouniSch.Indexes().GetByName("idx_v1v2")
			require.NotNil(t, idx_v1v2)

			idx_v1RowData, err := oneuni.GetNomsIndexRowData(context.Background(), idx_v1.Name())
			require.NoError(t, err)
			idx_v1v2RowData, err := twouni.GetNomsIndexRowData(context.Background(), idx_v1v2.Name())
			require.NoError(t, err)

			if assert.Equal(t, uint64(len(test.expectedIdxv1)), idx_v1RowData.Len()) && len(test.expectedIdxv1) > 0 {
				var sqlRows []sql.Row
				_ = idx_v1RowData.IterAll(context.Background(), func(key, value types.Value) error {
					r, err := row.FromNoms(idx_v1.Schema(), key.(types.Tuple), value.(types.Tuple))
					assert.NoError(t, err)
					sqlRow, err := sqlutil.DoltRowToSqlRow(r, idx_v1.Schema())
					assert.NoError(t, err)
					sqlRows = append(sqlRows, sqlRow)
					return nil
				})
				assert.ElementsMatch(t, convertSqlRowToInt64(test.expectedIdxv1), sqlRows)
			}
			if assert.Equal(t, uint64(len(test.expectedIdxv1v2)), idx_v1v2RowData.Len()) && len(test.expectedIdxv1v2) > 0 {
				var sqlRows []sql.Row
				_ = idx_v1v2RowData.IterAll(context.Background(), func(key, value types.Value) error {
					r, err := row.FromNoms(idx_v1v2.Schema(), key.(types.Tuple), value.(types.Tuple))
					assert.NoError(t, err)
					sqlRow, err := sqlutil.DoltRowToSqlRow(r, idx_v1v2.Schema())
					assert.NoError(t, err)
					sqlRows = append(sqlRows, sqlRow)
					return nil
				})
				assert.ElementsMatch(t, convertSqlRowToInt64(test.expectedIdxv1v2), sqlRows)
			}
		})
	}
}

func convertSqlRowToInt64(sqlRows []sql.Row) []sql.Row {
	if sqlRows == nil {
		return nil
	}
	newSqlRows := make([]sql.Row, len(sqlRows))
	for i, sqlRow := range sqlRows {
		newSqlRow := make(sql.Row, len(sqlRow))
		for j := range sqlRow {
			switch v := sqlRow[j].(type) {
			case int:
				newSqlRow[j] = int64(v)
			case int8:
				newSqlRow[j] = int64(v)
			case int16:
				newSqlRow[j] = int64(v)
			case int32:
				newSqlRow[j] = int64(v)
			case int64:
				newSqlRow[j] = v
			case nil:
				newSqlRow[j] = nil
			default:
				return sqlRows
			}
		}
		newSqlRows[i] = newSqlRow
	}
	return newSqlRows
}
