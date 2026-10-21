// Copyright 2026 Dolthub, Inc.
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

package merge_test

import (
	"context"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	dtu "github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor/creation"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

// TestVirtualMergeBaseIndexes checks the secondary indexes of a virtual merge base, which a merge against it never
// reads, against its rows:
//
//	c1 -- x1 -- x2    x
//	  \-- y1 -- y2    y
//	x2 = merge(x1, y1), y2 = merge(y1, x1)
//
// c1 seeds rows 1 to 3 with a = b = 'seed'. x1 and y1 conflict on row 1's a, which the virtual merge base sets back to
// 'seed'; merge row 2 cleanly to a = 'y', b = 'x'; conflict on row 3, which x1 deletes and y1 changes, so the virtual
// merge base restores c1's row; and conflict on row 4, which both add, so the virtual merge base leaves it out.
func TestVirtualMergeBaseIndexes(t *testing.T) {
	ctx := context.Background()
	dEnv := dtu.CreateTestEnv()
	defer dEnv.DoltDB(ctx).Close()
	dbName, eng := engineFromEnvironment(ctx, dEnv)
	sqlCtx, err := eng.NewLocalContext(ctx)
	require.NoError(t, err)
	sqlCtx.SetCurrentDatabase(dbName)

	for _, query := range []string{
		"SET @@autocommit = 0;",
		"CREATE TABLE t (id INT PRIMARY KEY, a VARCHAR(100), b VARCHAR(100), INDEX ia (a), INDEX iab (a, b));",
		"INSERT INTO t VALUES (1, 'seed', 'seed'), (2, 'seed', 'seed'), (3, 'seed', 'seed');",
		"CALL DOLT_COMMIT('-Am', 'c1');",
		"CALL DOLT_BRANCH('x');",
		"CALL DOLT_BRANCH('y');",
		"CALL DOLT_CHECKOUT('x');",
		"UPDATE t SET a = 'x' WHERE id = 1;",
		"UPDATE t SET b = 'x' WHERE id = 2;",
		"DELETE FROM t WHERE id = 3;",
		"INSERT INTO t VALUES (4, 'x', 'x');",
		"CALL DOLT_COMMIT('-am', 'x1');",
		"CALL DOLT_BRANCH('x1');",
		"CALL DOLT_CHECKOUT('y');",
		"UPDATE t SET a = 'y' WHERE id IN (1, 2, 3);",
		"INSERT INTO t VALUES (4, 'y', 'y');",
		"CALL DOLT_COMMIT('-am', 'y1');",
		"CALL DOLT_BRANCH('y1');",
		"CALL DOLT_MERGE('x1', '-m', 'y2');",
		"CALL DOLT_CONFLICTS_RESOLVE('--ours', 't');",
		"CALL DOLT_COMMIT('-am', 'y2');",
		"CALL DOLT_CHECKOUT('x');",
		"CALL DOLT_MERGE('y1', '-m', 'x2');",
		"CALL DOLT_CONFLICTS_RESOLVE('--ours', 't');",
		"CALL DOLT_COMMIT('-am', 'x2');",
	} {
		require.NoError(t, executeQuery(sqlCtx, eng, query), query)
	}

	x, y := resolveBranch(t, sqlCtx, dEnv.DoltDB(ctx), "x"), resolveBranch(t, sqlCtx, dEnv.DoltDB(ctx), "y")
	virtualBase, err := merge.ResolveMergeBase(sqlCtx, x, y)
	require.NoError(t, err)
	root, err := virtualBase.GetRootValue(sqlCtx)
	require.NoError(t, err)
	tbl, ok, err := root.GetTable(sqlCtx, doltdb.TableName{Name: "t"})
	require.NoError(t, err)
	require.True(t, ok)
	sch, err := tbl.GetSchema(sqlCtx)
	require.NoError(t, err)
	rowData, err := tbl.GetRowData(sqlCtx)
	require.NoError(t, err)
	rows, err := durable.ProllyMapFromIndex(rowData)
	require.NoError(t, err)

	kd, vd := rows.Descriptors()
	iter, err := rows.IterAll(sqlCtx)
	require.NoError(t, err)
	var got []sql.Row
	for {
		k, v, err := iter.Next(sqlCtx)
		if err != nil {
			break
		}
		row := sql.Row{}
		for i, field := range []struct {
			desc  *val.TupleDesc
			tuple val.Tuple
			index int
		}{{kd, k, 0}, {vd, v, 0}, {vd, v, 1}} {
			value, err := tree.GetField(sqlCtx, field.desc, field.index, field.tuple, rows.NodeStore())
			require.NoError(t, err, "field %d", i)
			row = append(row, value)
		}
		got = append(got, row)
	}
	assert.Equal(t, []sql.Row{{int32(1), "seed", "seed"}, {int32(2), "y", "x"}, {int32(3), "seed", "seed"}}, got)

	for _, index := range sch.Indexes().AllIndexes() {
		stored, err := tbl.GetIndexRowData(sqlCtx, index.Name())
		require.NoError(t, err)
		built, err := creation.BuildProllyIndexExternal(sqlCtx, tbl.ValueReadWriter(), tbl.NodeStore(), sch, "t", index, rows, nil, nil)
		require.NoError(t, err)
		storedHash, err := stored.HashOf()
		require.NoError(t, err)
		builtHash, err := built.HashOf()
		require.NoError(t, err)
		assert.Equal(t, builtHash, storedHash, "index %s does not match the rows of the virtual merge base", index.Name())
	}
}

// TestVirtualMergeBaseFullTextIndexes checks that searching a virtual merge base's Full-Text index finds its rows:
//
//	c1 -- x1 -- x2    x
//	  \-- y1 -- y2    y
//	x2 = merge(x1, y1), y2 = merge(y1, x1)
//
// c1 seeds rows 1 and 2 with a = b = 'seed'. x1 sets a to 'alpha' in both rows, and y1 sets row 1's a to 'bravo' and
// row 2's b to 'y'. So x1 and y1 conflict on row 1's a, which the virtual merge base sets back to 'seed', and merge
// row 2 to a = 'alpha', b = 'y'.
func TestVirtualMergeBaseFullTextIndexes(t *testing.T) {
	ctx := context.Background()
	dEnv := dtu.CreateTestEnv()
	defer dEnv.DoltDB(ctx).Close()
	dbName, eng := engineFromEnvironment(ctx, dEnv)
	sqlCtx, err := eng.NewLocalContext(ctx)
	require.NoError(t, err)
	sqlCtx.SetCurrentDatabase(dbName)

	for _, query := range []string{
		"SET @@autocommit = 0;",
		"CREATE TABLE t (id INT PRIMARY KEY, a VARCHAR(100), b VARCHAR(100), FULLTEXT INDEX fa (a));",
		"INSERT INTO t VALUES (1, 'seed', 'seed'), (2, 'seed', 'seed');",
		"CALL DOLT_COMMIT('-Am', 'c1');",
		"CALL DOLT_BRANCH('x');",
		"CALL DOLT_BRANCH('y');",
		"CALL DOLT_CHECKOUT('x');",
		"UPDATE t SET a = 'alpha';",
		"CALL DOLT_COMMIT('-am', 'x1');",
		"CALL DOLT_BRANCH('x1');",
		"CALL DOLT_CHECKOUT('y');",
		"UPDATE t SET a = 'bravo' WHERE id = 1;",
		"UPDATE t SET b = 'y' WHERE id = 2;",
		"CALL DOLT_COMMIT('-am', 'y1');",
		"CALL DOLT_BRANCH('y1');",
		"CALL DOLT_MERGE('x1', '-m', 'y2');",
		"CALL DOLT_CONFLICTS_RESOLVE('--ours', 't');",
		"UPDATE t SET a = 'resolved' WHERE id = 1;",
		"CALL DOLT_COMMIT('-am', 'y2');",
		"CALL DOLT_CHECKOUT('x');",
		"CALL DOLT_MERGE('y1', '-m', 'x2');",
		"CALL DOLT_CONFLICTS_RESOLVE('--ours', 't');",
		"UPDATE t SET a = 'resolved' WHERE id = 1;",
		"CALL DOLT_COMMIT('-am', 'x2');",
	} {
		require.NoError(t, executeQuery(sqlCtx, eng, query), query)
	}

	x, y := resolveBranch(t, sqlCtx, dEnv.DoltDB(ctx), "x"), resolveBranch(t, sqlCtx, dEnv.DoltDB(ctx), "y")
	virtualBase, err := merge.ResolveMergeBase(sqlCtx, x, y)
	require.NoError(t, err)
	virtualBaseHash, err := virtualBase.HashOf()
	require.NoError(t, err)
	for _, query := range []string{
		"CALL DOLT_BRANCH('virtual', '" + virtualBaseHash.String() + "');",
		"CALL DOLT_CHECKOUT('virtual');",
	} {
		require.NoError(t, executeQuery(sqlCtx, eng, query), query)
	}

	for word, ids := range map[string][]sql.Row{
		"seed":     {{int32(1)}},
		"alpha":    {{int32(2)}},
		"bravo":    nil,
		"resolved": nil,
	} {
		_, iter, _, err := eng.Query(sqlCtx, "SELECT id FROM t WHERE MATCH(a) AGAINST('"+word+"') ORDER BY id;")
		require.NoError(t, err)
		rows, err := sql.RowIterToRows(sqlCtx, iter)
		require.NoError(t, err)
		assert.Equal(t, ids, rows, "rows matching %s", word)
	}
}

// TestVirtualMergeBaseFullTextIndexesOfKeylessTable is TestVirtualMergeBaseFullTextIndexes for a keyless table. c1
// seeds the row 'seed'; x1 changes it to 'alpha' and y1 to 'bravo', and x2 and y2 resolve the conflict by keeping ours.
// x1 and y1 both delete 'seed', which a keyless merge counts as a conflict, so the virtual merge base keeps c1's copy
// of it, and their added rows.
func TestVirtualMergeBaseFullTextIndexesOfKeylessTable(t *testing.T) {
	ctx := context.Background()
	dEnv := dtu.CreateTestEnv()
	defer dEnv.DoltDB(ctx).Close()
	dbName, eng := engineFromEnvironment(ctx, dEnv)
	sqlCtx, err := eng.NewLocalContext(ctx)
	require.NoError(t, err)
	sqlCtx.SetCurrentDatabase(dbName)

	for _, query := range []string{
		"SET @@autocommit = 0;",
		"CREATE TABLE t (a VARCHAR(100), FULLTEXT INDEX fa (a));",
		"INSERT INTO t VALUES ('seed');",
		"CALL DOLT_COMMIT('-Am', 'c1');",
		"CALL DOLT_BRANCH('x');",
		"CALL DOLT_BRANCH('y');",
		"CALL DOLT_CHECKOUT('x');",
		"UPDATE t SET a = 'alpha';",
		"CALL DOLT_COMMIT('-am', 'x1');",
		"CALL DOLT_BRANCH('x1');",
		"CALL DOLT_CHECKOUT('y');",
		"UPDATE t SET a = 'bravo';",
		"CALL DOLT_COMMIT('-am', 'y1');",
		"CALL DOLT_BRANCH('y1');",
		"CALL DOLT_MERGE('x1', '-m', 'y2');",
		"CALL DOLT_CONFLICTS_RESOLVE('--ours', 't');",
		"CALL DOLT_COMMIT('-am', 'y2');",
		"CALL DOLT_CHECKOUT('x');",
		"CALL DOLT_MERGE('y1', '-m', 'x2');",
		"CALL DOLT_CONFLICTS_RESOLVE('--ours', 't');",
		"CALL DOLT_COMMIT('-am', 'x2');",
	} {
		require.NoError(t, executeQuery(sqlCtx, eng, query), query)
	}

	x, y := resolveBranch(t, sqlCtx, dEnv.DoltDB(ctx), "x"), resolveBranch(t, sqlCtx, dEnv.DoltDB(ctx), "y")
	virtualBase, err := merge.ResolveMergeBase(sqlCtx, x, y)
	require.NoError(t, err)
	virtualBaseHash, err := virtualBase.HashOf()
	require.NoError(t, err)
	for _, query := range []string{
		"CALL DOLT_BRANCH('virtual', '" + virtualBaseHash.String() + "');",
		"CALL DOLT_CHECKOUT('virtual');",
	} {
		require.NoError(t, executeQuery(sqlCtx, eng, query), query)
	}

	for query, expected := range map[string][]sql.Row{
		"SELECT a FROM t ORDER BY a;":                      {{"alpha"}, {"bravo"}, {"seed"}},
		"SELECT a FROM t WHERE MATCH(a) AGAINST('seed');":  {{"seed"}},
		"SELECT a FROM t WHERE MATCH(a) AGAINST('alpha');": {{"alpha"}},
		"SELECT a FROM t WHERE MATCH(a) AGAINST('bravo');": {{"bravo"}},
	} {
		_, iter, _, err := eng.Query(sqlCtx, query)
		require.NoError(t, err)
		rows, err := sql.RowIterToRows(sqlCtx, iter)
		require.NoError(t, err)
		assert.Equal(t, expected, rows, query)
	}
}

// virtualMergeBaseRows builds a history with |queries|, resolves the virtual merge base of branches x and y, and
// returns the rows |query| reads from it.
func virtualMergeBaseRows(t *testing.T, queries []string, query string) []sql.Row {
	_, rows := virtualMergeBase(t, queries, query)
	return rows
}

// virtualMergeBase is virtualMergeBaseRows, and also returns the virtual merge base's root.
func virtualMergeBase(t *testing.T, queries []string, query string) (doltdb.RootValue, []sql.Row) {
	ctx := context.Background()
	dEnv := dtu.CreateTestEnv()
	t.Cleanup(func() { dEnv.DoltDB(ctx).Close() })
	dbName, eng := engineFromEnvironment(ctx, dEnv)
	sqlCtx, err := eng.NewLocalContext(ctx)
	require.NoError(t, err)
	sqlCtx.SetCurrentDatabase(dbName)
	for _, q := range queries {
		require.NoError(t, executeQuery(sqlCtx, eng, q), q)
	}
	x, y := resolveBranch(t, sqlCtx, dEnv.DoltDB(ctx), "x"), resolveBranch(t, sqlCtx, dEnv.DoltDB(ctx), "y")
	virtualBase, err := merge.ResolveMergeBase(sqlCtx, x, y)
	require.NoError(t, err)
	root, err := virtualBase.GetRootValue(sqlCtx)
	require.NoError(t, err)
	assertIndexesMatchRows(t, sqlCtx, root)
	h, err := virtualBase.HashOf()
	require.NoError(t, err)
	for _, q := range []string{"CALL DOLT_BRANCH('virtual', '" + h.String() + "');", "CALL DOLT_CHECKOUT('virtual');"} {
		require.NoError(t, executeQuery(sqlCtx, eng, q), q)
	}
	_, iter, _, err := eng.Query(sqlCtx, query)
	require.NoError(t, err)
	rows, err := sql.RowIterToRows(sqlCtx, iter)
	require.NoError(t, err)
	return root, rows
}

// assertIndexesMatchRows checks that each secondary index, apart from Full-Text and vector indexes, of each table of
// |root| is the index its rows build.
func assertIndexesMatchRows(t *testing.T, ctx *sql.Context, root doltdb.RootValue) {
	names, err := root.GetTableNames(ctx, doltdb.DefaultSchemaName, false)
	require.NoError(t, err)
	for _, name := range names {
		tbl, _, err := root.GetTable(ctx, doltdb.TableName{Name: name})
		require.NoError(t, err)
		sch, err := tbl.GetSchema(ctx)
		require.NoError(t, err)
		rowData, err := tbl.GetRowData(ctx)
		require.NoError(t, err)
		rows, err := durable.ProllyMapFromIndex(rowData)
		require.NoError(t, err)
		for _, index := range sch.Indexes().AllIndexes() {
			if index.IsFullText() || index.IsVector() {
				continue
			}
			stored, err := tbl.GetIndexRowData(ctx, index.Name())
			require.NoError(t, err)
			built, err := creation.BuildProllyIndexExternal(ctx, tbl.ValueReadWriter(), tbl.NodeStore(), sch, name, index, rows, nil, nil)
			require.NoError(t, err)
			storedHash, err := stored.HashOf()
			require.NoError(t, err)
			builtHash, err := built.HashOf()
			require.NoError(t, err)
			assert.Equal(t, builtHash, storedHash, "index %s of %s does not match the rows of the virtual merge base", index.Name(), name)
		}
	}
}

// TestVirtualMergeBaseUnconvertibleValues checks a virtual merge base whose merge bases change column c's type
// differently, a schema conflict, so that it keeps c1's schema, where c is INT:
//
//	c1 -- x1 -- xm -- x2    x
//	  \-- y1 -- ym -- y2    y
//	x2 = merge(xm, y1), y2 = merge(ym, x1)
//
// x1 makes c VARCHAR, sets row 1's c to 'not-an-int' and d to 'x1', changes row 2's d, and adds row 3 with c
// 'not-an-int'. y1 makes c BIGINT and sets row 1's d to 'y1'. xm and ym change c's type to match the other before
// merging. A value that does not convert to INT conflicts: an existing row keeps c1's value, and a row only x1 has
// keeps its other values, with NULL in c.
func TestVirtualMergeBaseUnconvertibleValues(t *testing.T) {
	rows := virtualMergeBaseRows(t, unconvertibleHistory("not-an-int"), "SELECT * FROM t ORDER BY id;")
	assert.Equal(t, []sql.Row{{int32(1), int32(1), "seed"}, {int32(2), int32(2), "x"}, {int32(3), nil, "three"}}, rows)

	// A value that converts takes part in the merge: x1's change to row 1's c wins, and row 3 keeps it.
	rows = virtualMergeBaseRows(t, unconvertibleHistory("42"), "SELECT * FROM t ORDER BY id;")
	assert.Equal(t, []sql.Row{{int32(1), int32(42), "seed"}, {int32(2), int32(2), "x"}, {int32(3), int32(42), "three"}}, rows)
}

// unconvertibleHistory is TestVirtualMergeBaseUnconvertibleValues's history, with x1 setting c to |value|.
func unconvertibleHistory(value string) []string {
	return []string{
		"SET @@autocommit = 0;",
		"CREATE TABLE t (id INT PRIMARY KEY, c INT, d VARCHAR(20));",
		"INSERT INTO t VALUES (1, 1, 'seed'), (2, 2, 'seed');",
		"CALL DOLT_COMMIT('-Am', 'c1');",
		"CALL DOLT_BRANCH('x');",
		"CALL DOLT_BRANCH('y');",
		"CALL DOLT_CHECKOUT('x');",
		"ALTER TABLE t MODIFY c VARCHAR(20);",
		"UPDATE t SET c = '" + value + "', d = 'x1' WHERE id = 1;",
		"UPDATE t SET d = 'x' WHERE id = 2;",
		"INSERT INTO t VALUES (3, '" + value + "', 'three');",
		"CALL DOLT_COMMIT('-am', 'x1');",
		"CALL DOLT_BRANCH('x1');",
		"CALL DOLT_CHECKOUT('y');",
		"ALTER TABLE t MODIFY c BIGINT;",
		"UPDATE t SET d = 'y1' WHERE id = 1;",
		"CALL DOLT_COMMIT('-am', 'y1');",
		"CALL DOLT_BRANCH('y1');",
		"ALTER TABLE t MODIFY c VARCHAR(20);",
		"CALL DOLT_COMMIT('-am', 'ym');",
		"CALL DOLT_MERGE('x1', '-m', 'y2');",
		"CALL DOLT_CONFLICTS_RESOLVE('--ours', 't');",
		"CALL DOLT_COMMIT('-am', 'y2');",
		"CALL DOLT_CHECKOUT('x');",
		"UPDATE t SET c = '5';",
		"ALTER TABLE t MODIFY c BIGINT;",
		"CALL DOLT_COMMIT('-am', 'xm');",
		"CALL DOLT_MERGE('y1', '-m', 'x2');",
		"CALL DOLT_CONFLICTS_RESOLVE('--ours', 't');",
		"CALL DOLT_COMMIT('-am', 'x2');",
	}
}

// TestVirtualMergeBaseUnconvertibleKeylessValues is TestVirtualMergeBaseUnconvertibleValues for a keyless table. x1
// adds the row ('not-an-int', 'k'), whose c does not convert to INT, and y1 adds (2, 'y'). The row that does not
// convert conflicts, and keeps the copies c1 has of it, none; the other row merges.
func TestVirtualMergeBaseUnconvertibleKeylessValues(t *testing.T) {
	rows := virtualMergeBaseRows(t, []string{
		"SET @@autocommit = 0;",
		"CREATE TABLE t (c INT, d VARCHAR(20));",
		"INSERT INTO t VALUES (1, 'a');",
		"CALL DOLT_COMMIT('-Am', 'c1');",
		"CALL DOLT_BRANCH('x');",
		"CALL DOLT_BRANCH('y');",
		"CALL DOLT_CHECKOUT('x');",
		"ALTER TABLE t MODIFY c VARCHAR(20);",
		"INSERT INTO t VALUES ('not-an-int', 'k');",
		"CALL DOLT_COMMIT('-am', 'x1');",
		"CALL DOLT_BRANCH('x1');",
		"CALL DOLT_CHECKOUT('y');",
		"ALTER TABLE t MODIFY c BIGINT;",
		"INSERT INTO t VALUES (2, 'y');",
		"CALL DOLT_COMMIT('-am', 'y1');",
		"CALL DOLT_BRANCH('y1');",
		"ALTER TABLE t MODIFY c VARCHAR(20);",
		"CALL DOLT_COMMIT('-am', 'ym');",
		"SET @@dolt_allow_commit_conflicts = 1;",
		"CALL DOLT_MERGE('x1', '-m', 'y2');",
		"CALL DOLT_CONFLICTS_RESOLVE('--ours', 't');",
		"CALL DOLT_COMMIT('-am', 'y2');",
		"CALL DOLT_CHECKOUT('x');",
		"DELETE FROM t WHERE c = 'not-an-int';",
		"ALTER TABLE t MODIFY c BIGINT;",
		"CALL DOLT_COMMIT('-am', 'xm');",
		"CALL DOLT_MERGE('y1', '-m', 'x2');",
		"CALL DOLT_CONFLICTS_RESOLVE('--ours', 't');",
		"CALL DOLT_COMMIT('-am', 'x2');",
	}, "SELECT * FROM t ORDER BY c;")
	assert.Equal(t, []sql.Row{{int32(1), "a"}, {int32(2), "y"}}, rows)
}

// TestVirtualMergeBaseKeylessRowsCollapse checks a virtual merge base of a keyless table whose merge bases have
// different schemas, so that it matches their rows by their values under c1's schema, (a):
//
//	c1 -- x1 -- x2    x
//	  \-- y1 -- y2    y
//	x2 = merge(x1, y1), y2 = merge(y1, x1)
//
// c1 has one copy each of rows 7, 8 and 9. x1 adds column c, and has two copies of (7, 1) and three of (7, 2), which
// are all row 7 under c1's schema. y1 deletes row 8. So the virtual merge base has five copies of row 7, none of row
// 8, and one of row 9.
func TestVirtualMergeBaseKeylessRowsCollapse(t *testing.T) {
	rows := virtualMergeBaseRows(t, []string{
		"SET @@autocommit = 0;",
		"SET @@dolt_allow_commit_conflicts = 1;",
		"CREATE TABLE t (a INT);",
		"INSERT INTO t VALUES (7), (8), (9);",
		"CALL DOLT_COMMIT('-Am', 'c1');",
		"CALL DOLT_BRANCH('x');",
		"CALL DOLT_BRANCH('y');",
		"CALL DOLT_CHECKOUT('x');",
		"ALTER TABLE t ADD COLUMN c INT;",
		"UPDATE t SET c = 1 WHERE a = 7;",
		"INSERT INTO t VALUES (7, 1), (7, 2), (7, 2), (7, 2);",
		"CALL DOLT_COMMIT('-am', 'x1');",
		"CALL DOLT_BRANCH('x1');",
		"CALL DOLT_CHECKOUT('y');",
		"DELETE FROM t WHERE a = 8;",
		"CALL DOLT_COMMIT('-am', 'y1');",
		"CALL DOLT_BRANCH('y1');",
		"CALL DOLT_MERGE('x1', '-m', 'y2');",
		"CALL DOLT_CONFLICTS_RESOLVE('--ours', 't');",
		"CALL DOLT_COMMIT('-am', 'y2');",
		"CALL DOLT_CHECKOUT('x');",
		"CALL DOLT_MERGE('y1', '-m', 'x2');",
		"CALL DOLT_CONFLICTS_RESOLVE('--ours', 't');",
		"CALL DOLT_COMMIT('-am', 'x2');",
	}, "SELECT a, count(*) FROM t GROUP BY a ORDER BY a;")
	assert.Equal(t, []sql.Row{{int32(7), int64(5)}, {int32(9), int64(1)}}, rows)
}

// TestVirtualMergeBaseFullTextIndexesOfCleanRows is TestVirtualMergeBaseFullTextIndexes for merge bases that change
// different rows, x1 row 1's a to 'alpha' and y1 row 2's to 'bravo', which merge cleanly. Merging them still has to
// rebuild the Full-Text index, which the virtual merge base does after its rows are merged.
func TestVirtualMergeBaseFullTextIndexesOfCleanRows(t *testing.T) {
	rows := virtualMergeBaseRows(t, []string{
		"SET @@autocommit = 0;",
		"CREATE TABLE t (id INT PRIMARY KEY, a VARCHAR(100), FULLTEXT INDEX fa (a));",
		"INSERT INTO t VALUES (1, 'seed'), (2, 'seed'), (3, 'seed');",
		"CALL DOLT_COMMIT('-Am', 'c1');",
		"CALL DOLT_BRANCH('x');",
		"CALL DOLT_BRANCH('y');",
		"CALL DOLT_CHECKOUT('x');",
		"UPDATE t SET a = 'alpha' WHERE id = 1;",
		"CALL DOLT_COMMIT('-am', 'x1');",
		"CALL DOLT_BRANCH('x1');",
		"CALL DOLT_CHECKOUT('y');",
		"UPDATE t SET a = 'bravo' WHERE id = 2;",
		"CALL DOLT_COMMIT('-am', 'y1');",
		"CALL DOLT_BRANCH('y1');",
		"CALL DOLT_MERGE('x1', '-m', 'y2');",
		"CALL DOLT_CHECKOUT('x');",
		"CALL DOLT_MERGE('y1', '-m', 'x2');",
	}, `SELECT 'alpha', id FROM t WHERE MATCH(a) AGAINST('alpha')
		UNION ALL SELECT 'bravo', id FROM t WHERE MATCH(a) AGAINST('bravo')
		UNION ALL SELECT 'seed', id FROM t WHERE MATCH(a) AGAINST('seed')
		ORDER BY 1, 2;`)
	assert.Equal(t, []sql.Row{{"alpha", int32(1)}, {"bravo", int32(2)}, {"seed", int32(3)}}, rows)
}

// TestVirtualMergeBaseSpecialIndexes checks a virtual merge base's indexes against its rows, for a composite string key,
// a prefix index, a nullable unique index and a composite index:
//
//	c1 -- x1 -- x2    x
//	  \-- y1 -- y2    y
//	x2 = merge(x1, y1), y2 = merge(y1, x1)
//
// x1 and y1 conflict on row 1's a, which they change out of its prefix, on row 2's a, which they change within it, on
// row 3's b, which they change from NULL, and on row 6's b, which x1 changes to NULL, and both add row 5, so the
// virtual merge base takes c1's values back into those indexes and leaves row 5 out. Row 4 is untouched.
func TestVirtualMergeBaseSpecialIndexes(t *testing.T) {
	_, rows := virtualMergeBase(t, []string{
		"SET @@autocommit = 0;",
		"CREATE TABLE t (k1 VARCHAR(10), k2 INT, a VARCHAR(100), b VARCHAR(100), PRIMARY KEY (k1, k2), INDEX ip (a(3)), UNIQUE INDEX ub (b), INDEX iab (a, b));",
		"INSERT INTO t VALUES ('r', 1, 'seedling', 'u1'), ('r', 2, 'seedless', 'u2'), ('r', 3, 'same', NULL), ('r', 4, 'keep', 'k'), ('r', 6, 'six', 'b6');",
		"CALL DOLT_COMMIT('-Am', 'c1');",
		"CALL DOLT_BRANCH('x');",
		"CALL DOLT_BRANCH('y');",
		"CALL DOLT_CHECKOUT('x');",
		"UPDATE t SET a = 'xavier' WHERE k2 = 1;",
		"UPDATE t SET a = 'seedxx' WHERE k2 = 2;",
		"UPDATE t SET b = 'x3' WHERE k2 = 3;",
		"UPDATE t SET b = NULL WHERE k2 = 6;",
		"INSERT INTO t VALUES ('r', 5, 'xx', 'x5');",
		"CALL DOLT_COMMIT('-am', 'x1');",
		"CALL DOLT_BRANCH('x1');",
		"CALL DOLT_CHECKOUT('y');",
		"UPDATE t SET a = 'yvonne' WHERE k2 = 1;",
		"UPDATE t SET a = 'seedyy' WHERE k2 = 2;",
		"UPDATE t SET b = 'y3' WHERE k2 = 3;",
		"UPDATE t SET b = 'y6' WHERE k2 = 6;",
		"INSERT INTO t VALUES ('r', 5, 'yy', 'y5');",
		"CALL DOLT_COMMIT('-am', 'y1');",
		"CALL DOLT_BRANCH('y1');",
		"CALL DOLT_MERGE('x1', '-m', 'y2');",
		"CALL DOLT_CONFLICTS_RESOLVE('--ours', 't');",
		"CALL DOLT_COMMIT('-am', 'y2');",
		"CALL DOLT_CHECKOUT('x');",
		"CALL DOLT_MERGE('y1', '-m', 'x2');",
		"CALL DOLT_CONFLICTS_RESOLVE('--ours', 't');",
		"CALL DOLT_COMMIT('-am', 'x2');",
	}, `SELECT 'table', k2, a, b FROM t
		UNION ALL SELECT 'ip', k2, a, b FROM t FORCE INDEX (ip) WHERE a LIKE 'see%'
		UNION ALL SELECT 'ub', k2, a, b FROM t FORCE INDEX (ub) WHERE b IS NULL
		UNION ALL SELECT 'ub', k2, a, b FROM t FORCE INDEX (ub) WHERE b = 'b6'
		UNION ALL SELECT 'iab', k2, a, b FROM t FORCE INDEX (iab) WHERE a = 'seedling'
		ORDER BY 1, 2;`)
	assert.Equal(t, []sql.Row{
		{"iab", int32(1), "seedling", "u1"},
		{"ip", int32(1), "seedling", "u1"},
		{"ip", int32(2), "seedless", "u2"},
		{"table", int32(1), "seedling", "u1"},
		{"table", int32(2), "seedless", "u2"},
		{"table", int32(3), "same", nil},
		{"table", int32(4), "keep", "k"},
		{"table", int32(6), "six", "b6"},
		{"ub", int32(3), "same", nil},
		{"ub", int32(6), "six", "b6"},
	}, rows)
}

// TestVirtualMergeBaseVectorIndex checks nearest-neighbor searches of a virtual merge base's vector index:
//
//	c1 -- x1 -- x2    x
//	  \-- y1 -- y2    y
//	x2 = merge(x1, y1), y2 = merge(y1, x1)
//
// c1 has rows 1 at [50,60] and 2 at [42,52]. x1 moves row 1 to [0,0] and adds row 3 at [10,10]; y1 moves row 1 to
// [99,99] and adds row 3 at [90,90]. So the virtual merge base has row 1 back at [50,60], row 2 untouched, and no row
// 3. Each probe is nearest to one of its rows, and nearer still to a vector it must not have.
func TestVirtualMergeBaseVectorIndex(t *testing.T) {
	probe := func(v string) string {
		return "(SELECT '" + v + "', id FROM t ORDER BY VEC_DISTANCE(e, STRING_TO_VECTOR('" + v + "')) LIMIT 1)"
	}
	rows := virtualMergeBaseRows(t, []string{
		"SET @@autocommit = 0;",
		"CREATE TABLE t (id INT PRIMARY KEY, e VECTOR(2) NOT NULL, VECTOR INDEX vidx (e));",
		"INSERT INTO t VALUES (1, STRING_TO_VECTOR('[50,60]')), (2, STRING_TO_VECTOR('[42,52]'));",
		"CALL DOLT_COMMIT('-Am', 'c1');",
		"CALL DOLT_BRANCH('x');",
		"CALL DOLT_BRANCH('y');",
		"CALL DOLT_CHECKOUT('x');",
		"UPDATE t SET e = STRING_TO_VECTOR('[0,0]') WHERE id = 1;",
		"INSERT INTO t VALUES (3, STRING_TO_VECTOR('[10,10]'));",
		"CALL DOLT_COMMIT('-am', 'x1');",
		"CALL DOLT_BRANCH('x1');",
		"CALL DOLT_CHECKOUT('y');",
		"UPDATE t SET e = STRING_TO_VECTOR('[99,99]') WHERE id = 1;",
		"INSERT INTO t VALUES (3, STRING_TO_VECTOR('[90,90]'));",
		"CALL DOLT_COMMIT('-am', 'y1');",
		"CALL DOLT_BRANCH('y1');",
		"CALL DOLT_MERGE('x1', '-m', 'y2');",
		"CALL DOLT_CONFLICTS_RESOLVE('--ours', 't');",
		"CALL DOLT_COMMIT('-am', 'y2');",
		"CALL DOLT_CHECKOUT('x');",
		"CALL DOLT_MERGE('y1', '-m', 'x2');",
		"CALL DOLT_CONFLICTS_RESOLVE('--ours', 't');",
		"CALL DOLT_COMMIT('-am', 'x2');",
	}, probe("[0,0]")+" UNION ALL "+probe("[10,10]")+" UNION ALL "+probe("[99,99]")+" UNION ALL "+probe("[90,90]")+
		" UNION ALL "+probe("[50,60]")+" UNION ALL (SELECT 'rows', count(*) FROM t);")
	assert.Equal(t, []sql.Row{
		{"[0,0]", int64(2)},
		{"[10,10]", int64(2)},
		{"[99,99]", int64(1)},
		{"[90,90]", int64(1)},
		{"[50,60]", int64(1)},
		{"rows", int64(2)},
	}, rows)
}

func resolveBranch(t *testing.T, ctx *sql.Context, ddb *doltdb.DoltDB, branch string) *doltdb.Commit {
	spec, err := doltdb.NewCommitSpec(branch)
	require.NoError(t, err)
	optCmt, err := ddb.Resolve(ctx, spec, nil)
	require.NoError(t, err)
	commit, ok := optCmt.ToCommit()
	require.True(t, ok)
	return commit
}
