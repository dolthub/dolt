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
	"math"
	"sort"
	"strings"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/store/types"
)

var fk_dEnv *env.DoltEnv
var fk_initialRoot *doltdb.RootValue

func init() {
	fk_dEnv = dtestutils.CreateTestEnv()
	root, err := fk_dEnv.WorkingRoot(context.Background())
	if err != nil {
		panic(err)
	}
	fk_initialRoot, err = ExecuteSql(fk_dEnv, root, `
CREATE TABLE one (
  pk BIGINT PRIMARY KEY,
  v1 BIGINT,
  v2 BIGINT
);
CREATE TABLE two (
  pk BIGINT PRIMARY KEY,
  v1 BIGINT,
  v2 BIGINT
);
CREATE TABLE three (
  pk BIGINT PRIMARY KEY,
  v1 BIGINT,
  v2 BIGINT
);
ALTER TABLE one ADD INDEX secondary (v1);
ALTER TABLE two ADD INDEX secondary (v1,v2);
`)
	if err != nil {
		panic(err)
	}
}

func TestTableEditorForeignKeyCascade(t *testing.T) {
	testRoot, err := ExecuteSql(fk_dEnv, fk_initialRoot, `
ALTER TABLE two ADD FOREIGN KEY (v1) REFERENCES one(v1) ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE three ADD FOREIGN KEY (v1, v2) REFERENCES two(v1, v2) ON DELETE CASCADE ON UPDATE CASCADE;
`)
	require.NoError(t, err)

	tests := []struct {
		name          string
		sqlStatement  string
		expectedOne   []sql.Row
		expectedTwo   []sql.Row
		expectedThree []sql.Row
	}{
		{
			"cascade updates",
			`INSERT INTO one VALUES (1, 1, 4), (2, 2, 5), (3, 3, 6), (4, 4, 5);
			INSERT INTO two VALUES (2, 1, 1), (3, 2, 2), (4, 3, 3), (5, 4, 4);
			INSERT INTO three VALUES (3, 1, 1), (4, 2, 2), (5, 3, 3), (6, 4, 4);
			UPDATE one SET v1 = v1 + v2;
			UPDATE two SET v2 = v1 - 2;`,
			[]sql.Row{{1, 5, 4}, {2, 7, 5}, {3, 9, 6}, {4, 9, 5}},
			[]sql.Row{{2, 5, 3}, {3, 7, 5}, {4, 9, 7}, {5, 9, 7}},
			[]sql.Row{{3, 5, 3}, {4, 7, 5}, {5, 9, 7}, {6, 9, 7}},
		},
		{
			"cascade updates and deletes",
			`INSERT INTO one VALUES (1, 1, 4), (2, 2, 5), (3, 3, 6), (4, 4, 5);
			INSERT INTO two VALUES (2, 1, 1), (3, 2, 2), (4, 3, 3), (5, 4, 4);
			INSERT INTO three VALUES (3, 1, 1), (4, 2, 2), (5, 3, 3), (6, 4, 4);
			UPDATE one SET v1 = v1 + v2;
			DELETE FROM one WHERE pk = 3;
			UPDATE two SET v2 = v1 - 2;`,
			[]sql.Row{{1, 5, 4}, {2, 7, 5}, {4, 9, 5}},
			[]sql.Row{{2, 5, 3}, {3, 7, 5}},
			[]sql.Row{{3, 5, 3}, {4, 7, 5}},
		},
		{
			"cascade insertions",
			`INSERT INTO three VALUES (1, NULL, NULL), (2, NULL, 2), (3, 3, NULL);
			INSERT INTO two VALUES (1, NULL, 1);`,
			[]sql.Row{},
			[]sql.Row{{1, nil, 1}},
			[]sql.Row{{1, nil, nil}, {2, nil, 2}, {3, 3, nil}},
		},
		{
			"cascade updates and deletes after table and column renames",
			`INSERT INTO one VALUES (1, 1, 4), (2, 2, 5), (3, 3, 6), (4, 4, 5);
			INSERT INTO two VALUES (2, 1, 1), (3, 2, 2), (4, 3, 3), (5, 4, 4);
			INSERT INTO three VALUES (3, 1, 1), (4, 2, 2), (5, 3, 3), (6, 4, 4);
			RENAME TABLE one TO new;
			ALTER  TABLE new RENAME COLUMN v1 TO vnew;
			UPDATE new SET vnew = vnew + v2;
			DELETE FROM new WHERE pk = 3;
			UPDATE two SET v2 = v1 - 2;
			RENAME TABLE new TO one;`,
			[]sql.Row{{1, 5, 4}, {2, 7, 5}, {4, 9, 5}},
			[]sql.Row{{2, 5, 3}, {3, 7, 5}},
			[]sql.Row{{3, 5, 3}, {4, 7, 5}},
		},
		{
			"cascade inserts and deletes",
			`INSERT INTO one VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3);
			INSERT INTO two VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3);
			DELETE FROM one;`,
			[]sql.Row{},
			[]sql.Row{},
			[]sql.Row{},
		},
		{
			"cascade inserts and deletes (ep. 2)",
			`INSERT INTO one VALUES (1, NULL, 1);
			INSERT INTO two VALUES (1, NULL, 1), (2, NULL, 2);
			INSERT INTO three VALUES (1, NULL, 1), (2, NULL, 2);
			DELETE FROM one;
			DELETE FROM two WHERE pk = 2`,
			[]sql.Row{},
			[]sql.Row{{1, nil, 1}},
			[]sql.Row{{1, nil, 1}, {2, nil, 2}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := testRoot
			for _, sqlStatement := range strings.Split(test.sqlStatement, ";") {
				var err error
				root, err = executeModify(context.Background(), fk_dEnv, root, sqlStatement)
				require.NoError(t, err)
			}

			assertTableEditorRows(t, fk_dEnv, root, test.expectedOne, "one")
			assertTableEditorRows(t, fk_dEnv, root, test.expectedTwo, "two")
			assertTableEditorRows(t, fk_dEnv, root, test.expectedThree, "three")
		})
	}
}

func TestTableEditorForeignKeySetNull(t *testing.T) {
	testRoot, err := ExecuteSql(fk_dEnv, fk_initialRoot, `
ALTER TABLE two ADD FOREIGN KEY (v1) REFERENCES one(v1) ON DELETE SET NULL ON UPDATE SET NULL;`)
	require.NoError(t, err)

	tests := []struct {
		sqlStatement string
		expectedOne  []sql.Row
		expectedTwo  []sql.Row
	}{
		{
			`INSERT INTO one VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3);
			INSERT INTO two VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3);
			UPDATE one SET v1 = v1 * v2;
			INSERT INTO one VALUES (4, 4, 4);
			INSERT INTO two VALUES (4, 4, 4);
			UPDATE one SET v2 = v1 * v2;`,
			[]sql.Row{{1, 1, 1}, {2, 4, 8}, {3, 9, 27}, {4, 4, 16}},
			[]sql.Row{{1, 1, 1}, {2, nil, 2}, {3, nil, 3}, {4, 4, 4}},
		},
		{
			`INSERT INTO one VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4), (5, 5, 5);
			INSERT INTO two VALUES (1, 1, 1), (2, 2, 2), (4, 4, 4), (5, 5, 5);
			DELETE FROM one WHERE pk BETWEEN 3 AND 4;`,
			[]sql.Row{{1, 1, 1}, {2, 2, 2}, {5, 5, 5}},
			[]sql.Row{{1, 1, 1}, {2, 2, 2}, {4, nil, 4}, {5, 5, 5}},
		},
	}

	for _, test := range tests {
		t.Run(test.sqlStatement, func(t *testing.T) {
			root := testRoot
			for _, sqlStatement := range strings.Split(test.sqlStatement, ";") {
				var err error
				root, err = executeModify(context.Background(), fk_dEnv, root, sqlStatement)
				require.NoError(t, err)
			}

			t.Run("one", func(t *testing.T) {
				assertTableEditorRows(t, nil, root, test.expectedOne, "one")
			})
			t.Run("two", func(t *testing.T) {
				assertTableEditorRows(t, nil, root, test.expectedTwo, "two")
			})
		})
	}
}

func TestTableEditorForeignKeyRestrict(t *testing.T) {
	for _, referenceOption := range []string{
		"ON DELETE RESTRICT ON UPDATE RESTRICT",
		"ON DELETE NO ACTION ON UPDATE NO ACTION",
		"",
	} {
		t.Run(referenceOption, func(t *testing.T) {
			testRoot, err := ExecuteSql(fk_dEnv, fk_initialRoot, fmt.Sprintf(`
			ALTER TABLE two ADD FOREIGN KEY (v1) REFERENCES one(v1) %s;
			INSERT INTO one VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3);
			INSERT INTO two VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3);`, referenceOption))
			require.NoError(t, err)

			tests := []struct {
				setup       string
				trigger     string
				expectedErr bool
			}{
				{
					``,
					`UPDATE one SET v1 = v1;`,
					false,
				},
				{
					``,
					`UPDATE one SET v2 = v1 + v2;`,
					false,
				},
				{
					``,
					`UPDATE one SET v1 = v1 + v2;`,
					true,
				},
				{
					`INSERT INTO one VALUES (4, 4, 4);`,
					`UPDATE one SET v1 = 1 WHERE pk = 4;`,
					false,
				},
				{
					`INSERT INTO one VALUES (4, 4, 4);`,
					`DELETE FROM one WHERE pk > 3;`,
					false,
				},
				{
					`INSERT INTO one VALUES (4, 3, 4);`,
					`DELETE FROM one WHERE pk > 3;`,
					true,
				},
				{
					`INSERT INTO one VALUES (4, 4, 4);
					UPDATE one SET v1 = 2 WHERE pk > 3;`,
					`DELETE FROM one WHERE pk > 3;`,
					true,
				},
				{
					`INSERT INTO one VALUES (4, 4, 4);
					DELETE FROM two WHERE pk = 2;
					UPDATE one SET v1 = 2 WHERE pk > 3;`,
					`DELETE FROM one WHERE pk > 3;`,
					false,
				},
			}

			for _, test := range tests {
				t.Run(test.setup+test.trigger, func(t *testing.T) {
					root := testRoot
					for _, sqlStatement := range strings.Split(test.setup, ";") {
						var err error
						root, err = executeModify(context.Background(), fk_dEnv, root, sqlStatement)
						require.NoError(t, err)
					}
					if test.expectedErr {
						root, err = executeModify(context.Background(), fk_dEnv, root, test.trigger)
						assert.Error(t, err)
					} else {
						root, err = executeModify(context.Background(), fk_dEnv, root, test.trigger)
						assert.NoError(t, err)
					}
				})
			}
		})
	}
}

func TestTableEditorForeignKeyViolations(t *testing.T) {
	testRoot, err := ExecuteSql(fk_dEnv, fk_initialRoot, `
ALTER TABLE two ADD FOREIGN KEY (v1) REFERENCES one(v1) ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE three ADD FOREIGN KEY (v1, v2) REFERENCES two(v1, v2) ON DELETE CASCADE ON UPDATE CASCADE;
`)
	require.NoError(t, err)

	tests := []struct {
		setup   string
		trigger string
	}{
		{
			`INSERT INTO one VALUES (1, 1, 1);
			INSERT INTO two VALUES (1, 1, 1);`,
			`INSERT INTO three VALUES (1, 1, 2);`,
		},
		{
			`INSERT INTO one VALUES (1, 1, 1);
			INSERT INTO two VALUES (1, 1, 1);`,
			`INSERT INTO three VALUES (1, 2, 1);`,
		},
		{
			`INSERT INTO one VALUES (1, 1, 1);
			INSERT INTO two VALUES (1, 1, 1);`,
			`INSERT INTO three VALUES (1, 2, 2);`,
		},
		{
			`INSERT INTO one VALUES (1, 1, 1);
			INSERT INTO two VALUES (1, 1, 1);
			INSERT INTO three VALUES (1, 1, 1);`,
			`UPDATE two SET v1 = 2;`,
		},
		{
			`INSERT INTO one VALUES (1, 1, 1), (2, 2, 2);
			INSERT INTO two VALUES (2, 1, 1);
			INSERT INTO three VALUES (2, 1, 1);
			UPDATE one SET v1 = 2;`,
			`INSERT INTO two VALUES (1, 1, 1);`,
		},
		{
			`INSERT INTO one VALUES (1, 1, 1), (2, 2, 2);
			INSERT INTO two VALUES (2, 1, 1);
			INSERT INTO three VALUES (2, 1, 1);
			DELETE FROM one WHERE pk = 1;`,
			`INSERT INTO two VALUES (1, 1, 1);`,
		},
	}

	for _, test := range tests {
		t.Run(test.setup+test.trigger, func(t *testing.T) {
			root := testRoot
			for _, sqlStatement := range strings.Split(test.setup, ";") {
				var err error
				root, err = executeModify(context.Background(), fk_dEnv, root, sqlStatement)
				require.NoError(t, err)
			}
			root, err = executeModify(context.Background(), fk_dEnv, root, test.trigger)
			assert.Error(t, err)
		})
	}
}

func assertTableEditorRows(t *testing.T, fk_dEnv *env.DoltEnv, root *doltdb.RootValue, expected []sql.Row, tableName string) {
	tbl, ok, err := root.GetTable(context.Background(), tableName)
	require.NoError(t, err)
	require.True(t, ok)

	sch, err := tbl.GetSchema(context.Background())
	require.NoError(t, err)

	rowData, err := tbl.GetRowData(context.Background())
	require.NoError(t, err)

	var sqlRows []sql.Row
	if len(expected) > 0 {
		_ = rowData.IterAll(context.Background(), func(key, value types.Value) error {
			r, err := row.FromNoms(sch, key.(types.Tuple), value.(types.Tuple))
			assert.NoError(t, err)
			sqlRow, err := doltRowToSqlRow(r, sch)
			assert.NoError(t, err)
			sqlRows = append(sqlRows, sqlRow)
			return nil
		})
		assert.Equal(t, convertSqlRowToInt64(expected), sqlRows)
	}

	// we can verify that each index also has the proper contents
	for _, index := range sch.Indexes().AllIndexes() {
		indexRowData, err := tbl.GetIndexRowData(context.Background(), index.Name())
		require.NoError(t, err)
		indexSch := index.Schema()

		colPlacements := make([]int, sch.GetAllCols().Size())
		for i := range colPlacements {
			colPlacements[i] = -1
		}
		for colIndex, colTag := range sch.GetAllCols().Tags {
			for indexIndex, indexTag := range index.AllTags() {
				if colTag == indexTag {
					colPlacements[colIndex] = indexIndex
					continue
				}
			}
		}

		expectedIndexRows := make([]sql.Row, len(expected))
		for rowIndex, expectedRow := range expected {
			expectedIndex := make(sql.Row, len(index.AllTags()))
			for colIndex, val := range expectedRow {
				colPlacement := colPlacements[colIndex]
				if colPlacement != -1 {
					expectedIndex[colPlacement] = val
				}
			}
			expectedIndexRows[rowIndex] = expectedIndex
		}
		expectedIndexRows = convertSqlRowToInt64(expectedIndexRows)
		sort.Slice(expectedIndexRows, func(leftIndex, rightIndex int) bool {
			a := expectedIndexRows[leftIndex]
			b := expectedIndexRows[rightIndex]
			for i := range a {
				aVal, aNotNil := a[i].(int64)
				bVal, bNotNil := b[i].(int64)
				if !aNotNil {
					aVal = math.MaxInt64
				}
				if !bNotNil {
					bVal = math.MaxInt64
				}
				if aVal < bVal {
					return true
				}
			}
			return false
		})

		if len(expectedIndexRows) > 0 {
			var sqlRows []sql.Row
			_ = indexRowData.IterAll(context.Background(), func(key, value types.Value) error {
				r, err := row.FromNoms(indexSch, key.(types.Tuple), value.(types.Tuple))
				assert.NoError(t, err)
				sqlRow, err := doltRowToSqlRow(r, indexSch)
				assert.NoError(t, err)
				sqlRows = append(sqlRows, sqlRow)
				return nil
			})
			assert.Equal(t, expectedIndexRows, sqlRows)
		}
	}
}
