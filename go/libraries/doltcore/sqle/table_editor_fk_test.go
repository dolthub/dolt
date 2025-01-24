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
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
)

func setupEditorFkTest(ctx context.Context, t *testing.T) (*env.DoltEnv, doltdb.RootValue) {
	dEnv := dtestutils.CreateTestEnv()
	root, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		panic(err)
	}
	initialRoot, err := ExecuteSql(ctx, dEnv, root, `
CREATE TABLE one (
  pk BIGINT PRIMARY KEY,
  v1 BIGINT,
  v2 BIGINT,
  INDEX secondary (v1)
);
CREATE TABLE two (
  pk BIGINT PRIMARY KEY,
  v1 BIGINT,
  v2 BIGINT,
  INDEX secondary (v1,v2)
);
CREATE TABLE three (
  pk BIGINT PRIMARY KEY,
  v1 BIGINT,
  v2 BIGINT
);
CREATE TABLE parent (
  id BIGINT PRIMARY KEY,
  v1 BIGINT,
  v2 BIGINT,
  INDEX v1 (v1),
  INDEX v2 (v2)
);
CREATE TABLE child (
  id BIGINT PRIMARY KEY,
  v1 BIGINT,
  v2 BIGINT
);
`)
	require.NoError(t, err)

	return dEnv, initialRoot
}

func TestTableEditorForeignKeyCascade(t *testing.T) {
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
			ctx := context.Background()
			dEnv, initialRoot := setupEditorFkTest(ctx, t)
			defer dEnv.DoltDB(ctx).Close()

			testRoot, err := ExecuteSql(ctx, dEnv, initialRoot, `
ALTER TABLE two ADD FOREIGN KEY (v1) REFERENCES one(v1) ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE three ADD FOREIGN KEY (v1, v2) REFERENCES two(v1, v2) ON DELETE CASCADE ON UPDATE CASCADE;
`)
			require.NoError(t, err)

			root := testRoot
			for _, sqlStatement := range strings.Split(test.sqlStatement, ";") {
				var err error
				root, err = executeModify(t, context.Background(), dEnv, root, sqlStatement)
				require.NoError(t, err)
			}

			assertTableEditorRows(t, root, test.expectedOne, "one")
			assertTableEditorRows(t, root, test.expectedTwo, "two")
			assertTableEditorRows(t, root, test.expectedThree, "three")
		})
	}
}

func TestTableEditorForeignKeySetNull(t *testing.T) {
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
			ctx := context.Background()
			dEnv, initialRoot := setupEditorFkTest(ctx, t)
			defer dEnv.DoltDB(ctx).Close()

			testRoot, err := ExecuteSql(ctx, dEnv, initialRoot, `
ALTER TABLE two ADD FOREIGN KEY (v1) REFERENCES one(v1) ON DELETE SET NULL ON UPDATE SET NULL;`)
			require.NoError(t, err)

			root := testRoot
			for _, sqlStatement := range strings.Split(test.sqlStatement, ";") {
				var err error
				root, err = executeModify(t, context.Background(), dEnv, root, sqlStatement)
				require.NoError(t, err)
			}

			t.Run("one", func(t *testing.T) {
				assertTableEditorRows(t, root, test.expectedOne, "one")
			})
			t.Run("two", func(t *testing.T) {
				assertTableEditorRows(t, root, test.expectedTwo, "two")
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
					ctx := context.Background()
					dEnv, initialRoot := setupEditorFkTest(ctx, t)
					defer dEnv.DoltDB(ctx).Close()

					testRoot, err := ExecuteSql(ctx, dEnv, initialRoot, fmt.Sprintf(`
			ALTER TABLE two ADD FOREIGN KEY (v1) REFERENCES one(v1) %s;
			INSERT INTO one VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3);
			INSERT INTO two VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3);`, referenceOption))
					require.NoError(t, err)

					root := testRoot
					for _, sqlStatement := range strings.Split(test.setup, ";") {
						var err error
						root, err = executeModify(t, context.Background(), dEnv, root, sqlStatement)
						require.NoError(t, err)
					}
					if test.expectedErr {
						root, err = executeModify(t, context.Background(), dEnv, root, test.trigger)
						assert.Error(t, err)
					} else {
						root, err = executeModify(t, context.Background(), dEnv, root, test.trigger)
						assert.NoError(t, err)
					}
				})
			}
		})
	}
}

func TestTableEditorForeignKeyViolations(t *testing.T) {
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
			ctx := context.Background()
			dEnv, initialRoot := setupEditorFkTest(ctx, t)
			defer dEnv.DoltDB(ctx).Close()

			testRoot, err := ExecuteSql(ctx, dEnv, initialRoot, `
ALTER TABLE two ADD FOREIGN KEY (v1) REFERENCES one(v1) ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE three ADD FOREIGN KEY (v1, v2) REFERENCES two(v1, v2) ON DELETE CASCADE ON UPDATE CASCADE;
`)
			require.NoError(t, err)

			root := testRoot
			for _, sqlStatement := range strings.Split(test.setup, ";") {
				var err error
				root, err = executeModify(t, context.Background(), dEnv, root, sqlStatement)
				require.NoError(t, err)
			}
			root, err = executeModify(t, context.Background(), dEnv, root, test.trigger)
			assert.Error(t, err)
		})
	}
}

func TestTableEditorSelfReferentialForeignKeyRestrict(t *testing.T) {
	ctx := context.Background()
	dEnv, initialRoot := setupEditorFkTest(ctx, t)
	defer dEnv.DoltDB(ctx).Close()

	root := initialRoot

	sequentialTests := []struct {
		statement   string
		currentTbl  []sql.Row
		expectedErr bool
	}{
		{
			"ALTER TABLE parent ADD CONSTRAINT fk_named FOREIGN KEY (v2) REFERENCES parent(v1);",
			[]sql.Row{},
			false,
		},
		{
			"INSERT INTO parent VALUES (1,1,1), (2, 2, 1), (3, 3, NULL);",
			[]sql.Row{{1, 1, 1}, {2, 2, 1}, {3, 3, nil}},
			false,
		},
		{
			"UPDATE parent SET v1 = 1 WHERE id = 1;",
			[]sql.Row{{1, 1, 1}, {2, 2, 1}, {3, 3, nil}},
			false,
		},
		{
			"UPDATE parent SET v1 = 4 WHERE id = 3;",
			[]sql.Row{{1, 1, 1}, {2, 2, 1}, {3, 4, nil}},
			false,
		},
		{
			"DELETE FROM parent WHERE id = 3;",
			[]sql.Row{{1, 1, 1}, {2, 2, 1}},
			false,
		},
		{
			"DELETE FROM parent WHERE v1 = 1;",
			[]sql.Row{},
			true,
		},
		{
			"UPDATE parent SET v1 = 2;",
			[]sql.Row{},
			true,
		},
		{
			"REPLACE INTO parent VALUES (1, 1, 1);",
			[]sql.Row{},
			true,
		},
		{
			"UPDATE parent SET v1 = 3, v2 = 3 WHERE id = 2;",
			[]sql.Row{{1, 1, 1}, {2, 3, 3}},
			false,
		},
	}

	for _, test := range sequentialTests {
		newRoot, err := executeModify(t, ctx, dEnv, root, test.statement)
		if test.expectedErr {
			require.Error(t, err)
			continue
		}
		require.NoError(t, err)
		assertTableEditorRows(t, newRoot, test.currentTbl, "parent")
		root = newRoot
	}
}

func TestTableEditorSelfReferentialForeignKeyCascade(t *testing.T) {
	ctx := context.Background()
	dEnv, initialRoot := setupEditorFkTest(ctx, t)
	defer dEnv.DoltDB(ctx).Close()

	root := initialRoot

	sequentialTests := []struct {
		statement   string
		currentTbl  []sql.Row
		expectedErr bool
	}{
		{
			"ALTER TABLE parent ADD CONSTRAINT fk_named FOREIGN KEY (v2) REFERENCES parent(v1) ON UPDATE CASCADE ON DELETE CASCADE;",
			[]sql.Row{},
			false,
		},
		{
			"INSERT INTO parent VALUES (1,1,1), (2, 2, 1), (3, 3, NULL);",
			[]sql.Row{{1, 1, 1}, {2, 2, 1}, {3, 3, nil}},
			false,
		},
		{
			"UPDATE parent SET v1 = 1 WHERE id = 1;",
			[]sql.Row{{1, 1, 1}, {2, 2, 1}, {3, 3, nil}},
			false,
		},
		{
			"UPDATE parent SET v1 = 4 WHERE id = 3;",
			[]sql.Row{{1, 1, 1}, {2, 2, 1}, {3, 4, nil}},
			false,
		},
		{
			"DELETE FROM parent WHERE id = 3;",
			[]sql.Row{{1, 1, 1}, {2, 2, 1}},
			false,
		},
		{
			"UPDATE parent SET v1 = 2;",
			[]sql.Row{},
			true,
		},
		{
			"REPLACE INTO parent VALUES (1, 1, 1), (2, 2, 2);",
			[]sql.Row{{1, 1, 1}, {2, 2, 2}},
			false,
		},
		{ // Repeated UPDATE ensures that it still fails even with changed data
			"UPDATE parent SET v1 = 2;",
			[]sql.Row{},
			true,
		},
		{
			"UPDATE parent SET v1 = 2 WHERE id = 1;",
			[]sql.Row{},
			true,
		},
		{
			"REPLACE INTO parent VALUES (1,1,2), (2,2,1);",
			[]sql.Row{},
			true,
		},
		{
			"UPDATE parent SET v2 = 2 WHERE id = 1;",
			[]sql.Row{{1, 1, 2}, {2, 2, 2}},
			false,
		},
		{
			"UPDATE parent SET v2 = 1 WHERE id = 2;",
			[]sql.Row{{1, 1, 2}, {2, 2, 1}},
			false,
		},
		{ // Repeated UPDATE ensures that it still fails even with changed data
			"UPDATE parent SET v1 = 2;",
			[]sql.Row{},
			true,
		},
		{
			"UPDATE parent SET v1 = 2 WHERE id = 1;",
			[]sql.Row{},
			true,
		},
		{
			"DELETE FROM parent WHERE v1 = 1;",
			[]sql.Row{},
			false,
		},
	}

	for _, test := range sequentialTests {
		newRoot, err := executeModify(t, ctx, dEnv, root, test.statement)
		if test.expectedErr {
			require.Error(t, err)
			continue
		}
		require.NoError(t, err)
		assertTableEditorRows(t, newRoot, test.currentTbl, "parent")
		root = newRoot
	}
}

func TestTableEditorSelfReferentialForeignKeySetNull(t *testing.T) {
	ctx := context.Background()
	dEnv, initialRoot := setupEditorFkTest(ctx, t)
	defer dEnv.DoltDB(ctx).Close()

	root := initialRoot

	sequentialTests := []struct {
		statement   string
		currentTbl  []sql.Row
		expectedErr bool
	}{
		{
			"ALTER TABLE parent ADD CONSTRAINT fk_named FOREIGN KEY (v2) REFERENCES parent(v1) ON UPDATE SET NULL ON DELETE SET NULL;",
			[]sql.Row{},
			false,
		},
		{
			"INSERT INTO parent VALUES (1,1,1), (2, 2, 1), (3, 3, NULL);",
			[]sql.Row{{1, 1, 1}, {2, 2, 1}, {3, 3, nil}},
			false,
		},
		{
			"UPDATE parent SET v1 = 1 WHERE id = 1;",
			[]sql.Row{{1, 1, 1}, {2, 2, 1}, {3, 3, nil}},
			false,
		},
		{
			"UPDATE parent SET v1 = 4 WHERE id = 3;",
			[]sql.Row{{1, 1, 1}, {2, 2, 1}, {3, 4, nil}},
			false,
		},
		{
			"DELETE FROM parent WHERE id = 3;",
			[]sql.Row{{1, 1, 1}, {2, 2, 1}},
			false,
		},
		{
			"UPDATE parent SET v1 = 2;",
			[]sql.Row{},
			true,
		},
		{
			"REPLACE INTO parent VALUES (1, 1, 1), (2, 2, 2);",
			[]sql.Row{{1, 1, 1}, {2, 2, 2}},
			false,
		},
		{ // Repeated UPDATE ensures that it still fails even with changed data
			"UPDATE parent SET v1 = 2;",
			[]sql.Row{},
			true,
		},
		{
			"UPDATE parent SET v1 = 2 WHERE id = 1;",
			[]sql.Row{},
			true,
		},
		{
			"REPLACE INTO parent VALUES (1,1,2), (2,2,1);",
			[]sql.Row{{1, 1, nil}, {2, 2, 1}},
			false,
		},
		{
			"UPDATE parent SET v2 = 2 WHERE id = 1;",
			[]sql.Row{{1, 1, 2}, {2, 2, 1}},
			false,
		},
		{
			"UPDATE parent SET v2 = 1 WHERE id = 2;",
			[]sql.Row{{1, 1, 2}, {2, 2, 1}},
			false,
		},
		{ // Repeated UPDATE ensures that it still fails even with changed data
			"UPDATE parent SET v1 = 2;",
			[]sql.Row{},
			true,
		},
		{
			"UPDATE parent SET v1 = 2 WHERE id = 1;",
			[]sql.Row{},
			true,
		},
		{
			"DELETE FROM parent WHERE v1 = 1;",
			[]sql.Row{{2, 2, nil}},
			false,
		},
	}

	for _, test := range sequentialTests {
		newRoot, err := executeModify(t, ctx, dEnv, root, test.statement)
		if test.expectedErr {
			require.Error(t, err)
			continue
		}
		require.NoError(t, err)
		assertTableEditorRows(t, newRoot, test.currentTbl, "parent")
		root = newRoot
	}
}

func assertTableEditorRows(t *testing.T, root doltdb.RootValue, expected []sql.Row, tableName string) {
	tbl, ok, err := root.GetTable(context.Background(), doltdb.TableName{Name: tableName})
	require.NoError(t, err)
	require.True(t, ok)

	sch, err := tbl.GetSchema(context.Background())
	require.NoError(t, err)

	rows, err := tbl.GetRowData(context.Background())
	require.NoError(t, err)

	var sqlRows []sql.Row
	if len(expected) > 0 {
		sqlRows, err = SqlRowsFromDurableIndex(rows, sch)
		require.NoError(t, err)
		expected = sortInt64Rows(convertSqlRowToInt64(expected))
		sqlRows = sortInt64Rows(convertSqlRowToInt64(sqlRows))
		if !assert.Equal(t, expected, sqlRows) {
			t.Fail()
		}
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

		expectedIndexRows = sortInt64Rows(expectedIndexRows)

		if len(expectedIndexRows) > 0 {
			sqlRows, err = SqlRowsFromDurableIndex(indexRowData, indexSch)
			require.NoError(t, err)
			expected = sortInt64Rows(convertSqlRowToInt64(expected))
			sqlRows = sortInt64Rows(convertSqlRowToInt64(sqlRows))
			if !reflect.DeepEqual(expectedIndexRows, sqlRows) {
				sqlRows = sortInt64Rows(sqlRows)
			}
			assert.Equal(t, expectedIndexRows, sqlRows)
		}
	}
}

func sortInt64Rows(rows []sql.Row) []sql.Row {
	sort.Slice(rows, func(l, r int) bool {
		a, b := rows[l], rows[r]
		for i := range a {
			aa, ok := a[i].(int64)
			if !ok {
				aa = math.MaxInt64
			}
			bb, ok := b[i].(int64)
			if !ok {
				bb = math.MaxInt64
			}
			if aa < bb {
				return true
			}
		}
		return false
	})
	return rows
}

func setupEditorKeylessFkTest(ctx context.Context, t *testing.T) (*env.DoltEnv, doltdb.RootValue) {
	dEnv := dtestutils.CreateTestEnv()
	root, err := dEnv.WorkingRoot(context.Background())
	if err != nil {
		panic(err)
	}
	initialRoot, err := ExecuteSql(ctx, dEnv, root, `
CREATE TABLE one (
  pk BIGINT,
  v1 BIGINT,
  v2 BIGINT,
  INDEX secondary (v1)
);
CREATE TABLE two (
  pk BIGINT,
  v1 BIGINT,
  v2 BIGINT,
  INDEX secondary (v1, v2)
);
CREATE TABLE three (
  pk BIGINT,
  v1 BIGINT,
  v2 BIGINT,
  INDEX v1 (v1),
  INDEX v2 (v2)
);
CREATE TABLE parent (
  id BIGINT,
  v1 BIGINT,
  v2 BIGINT,
  INDEX v1 (v1),
  INDEX v2 (v2)
);
CREATE TABLE child (
  id BIGINT,
  v1 BIGINT,
  v2 BIGINT
);
`)
	require.NoError(t, err)

	return dEnv, initialRoot
}

func TestTableEditorKeylessFKCascade(t *testing.T) {
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
			[]sql.Row{{1, 5, 4}, {4, 9, 5}, {2, 7, 5}, {3, 9, 6}},
			[]sql.Row{{3, 7, 5}, {2, 5, 3}, {5, 9, 7}, {4, 9, 7}},
			[]sql.Row{{5, 9, 7}, {6, 9, 7}, {4, 7, 5}, {3, 5, 3}},
		},
		{
			"cascade updates and deletes",
			`INSERT INTO one VALUES (1, 1, 4), (2, 2, 5), (3, 3, 6), (4, 4, 5);
			INSERT INTO two VALUES (2, 1, 1), (3, 2, 2), (4, 3, 3), (5, 4, 4);
			INSERT INTO three VALUES (3, 1, 1), (4, 2, 2), (5, 3, 3), (6, 4, 4);
			UPDATE one SET v1 = v1 + v2;
			DELETE FROM one WHERE pk = 3;
			UPDATE two SET v2 = v1 - 2;`,
			[]sql.Row{{1, 5, 4}, {4, 9, 5}, {2, 7, 5}},
			[]sql.Row{{3, 7, 5}, {2, 5, 3}},
			[]sql.Row{{4, 7, 5}, {3, 5, 3}},
		},
		{
			"cascade insertions",
			`INSERT INTO three VALUES (1, NULL, NULL), (2, NULL, 2), (3, 3, NULL);
			INSERT INTO two VALUES (1, NULL, 1);`,
			[]sql.Row{},
			[]sql.Row{{1, nil, 1}},
			[]sql.Row{{3, 3, nil}, {2, nil, 2}, {1, nil, nil}},
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
			[]sql.Row{{1, 5, 4}, {4, 9, 5}, {2, 7, 5}},
			[]sql.Row{{3, 7, 5}, {2, 5, 3}},
			[]sql.Row{{4, 7, 5}, {3, 5, 3}},
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
			[]sql.Row{{2, nil, 2}, {1, nil, 1}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			dEnv, initialRoot := setupEditorKeylessFkTest(ctx, t)
			defer dEnv.DoltDB(ctx).Close()

			testRoot, err := ExecuteSql(ctx, dEnv, initialRoot, `
ALTER TABLE two ADD FOREIGN KEY (v1) REFERENCES one(v1) ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE three ADD FOREIGN KEY (v1, v2) REFERENCES two(v1, v2) ON DELETE CASCADE ON UPDATE CASCADE;
`)
			require.NoError(t, err)

			root := testRoot
			for _, sqlStatement := range strings.Split(test.sqlStatement, ";") {
				var err error
				root, err = executeModify(t, context.Background(), dEnv, root, sqlStatement)
				require.NoError(t, err)
			}

			assertTableEditorRows(t, root, test.expectedOne, "one")
			assertTableEditorRows(t, root, test.expectedTwo, "two")
			assertTableEditorRows(t, root, test.expectedThree, "three")
		})
	}
}
