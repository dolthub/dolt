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
	"strings"
	"testing"

	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dtestutils"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/store/types"
)

var dEnv *env.DoltEnv
var initialRoot *doltdb.RootValue

func init() {
	dEnv = dtestutils.CreateTestEnv()
	root, err := dEnv.WorkingRoot(context.Background())
	if err != nil {
		panic(err)
	}
	initialRoot, err = ExecuteSql(dEnv, root, `
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
`)
	if err != nil {
		panic(err)
	}
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
UPDATE onepk SET pk1 = v1 + pk1;
`,
			[]sql.Row{{2, 4}, {4, 3}, {6, 9}},
			[]sql.Row{},
		},
	}

	for _, test := range tests {
		t.Run(test.sqlStatement, func(t *testing.T) {
			root := initialRoot
			for _, sqlStatement := range strings.Split(test.sqlStatement, ";") {
				var err error
				root, err = executeModify(context.Background(), dEnv, root, sqlStatement)
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

			idx_v1 := onepkSch.Indexes().Get("idx_v1")
			require.NotNil(t, idx_v1)
			idx_v2v1 := twopkSch.Indexes().Get("idx_v2v1")
			require.NotNil(t, idx_v2v1)

			idx_v1RowData, err := onepk.GetIndexRowData(context.Background(), "idx_v1")
			require.NoError(t, err)
			idx_v2v1RowData, err := twopk.GetIndexRowData(context.Background(), "idx_v2v1")
			require.NoError(t, err)

			if assert.Equal(t, uint64(len(test.expectedIdxv1)), idx_v1RowData.Len()) && len(test.expectedIdxv1) > 0 {
				var sqlRows []sql.Row
				_ = idx_v1RowData.IterAll(context.Background(), func(key, value types.Value) error {
					r, err := row.FromNoms(idx_v1.Schema(), key.(types.Tuple), value.(types.Tuple))
					assert.NoError(t, err)
					sqlRow, err := doltRowToSqlRow(r, idx_v1.Schema())
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
					sqlRow, err := doltRowToSqlRow(r, idx_v2v1.Schema())
					assert.NoError(t, err)
					sqlRows = append(sqlRows, sqlRow)
					return nil
				})
				assert.ElementsMatch(t, convertSqlRowToInt64(test.expectedIdxv2v1), sqlRows)
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
			default:
				return sqlRows
			}
		}
		newSqlRows[i] = newSqlRow
	}
	return newSqlRows
}
