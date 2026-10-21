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
	h, err := virtualBase.HashOf()
	require.NoError(t, err)
	for _, q := range []string{"CALL DOLT_BRANCH('virtual', '" + h.String() + "');", "CALL DOLT_CHECKOUT('virtual');"} {
		require.NoError(t, executeQuery(sqlCtx, eng, q), q)
	}
	_, iter, _, err := eng.Query(sqlCtx, query)
	require.NoError(t, err)
	rows, err := sql.RowIterToRows(sqlCtx, iter)
	require.NoError(t, err)
	return rows
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

func resolveBranch(t *testing.T, ctx *sql.Context, ddb *doltdb.DoltDB, branch string) *doltdb.Commit {
	spec, err := doltdb.NewCommitSpec(branch)
	require.NoError(t, err)
	optCmt, err := ddb.Resolve(ctx, spec, nil)
	require.NoError(t, err)
	commit, ok := optCmt.ToCommit()
	require.True(t, ok)
	return commit
}
