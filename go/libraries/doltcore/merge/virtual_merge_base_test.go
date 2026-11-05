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
	dtu "github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
)

// The tests here build a criss-cross history with SQL, resolve the virtual merge base of branches x and y, and read it
// through a branch pointed at it:
//
//	c1 -- x1 -- x2    x
//	  \-- y1 -- y2    y
//	x2 = merge(x1, y1), y2 = merge(y1, x1)
//
// Every test also checks that the virtual merge base's secondary indexes are the ones its rows build, since a merge
// against it never reads them.

var ours = []string{"CALL DOLT_CONFLICTS_RESOLVE('--ours', 't');"}

// TestVirtualMergeBaseIndexes: c1 seeds rows 1 to 3 with a = b = 'seed'. x1 and y1 conflict on row 1's a, which the
// virtual merge base sets back to 'seed'; merge row 2 cleanly to a = 'y', b = 'x'; conflict on row 3, which x1 deletes
// and y1 changes, so the virtual merge base restores c1's row; and conflict on row 4, which both add, so the virtual
// merge base leaves it out.
func TestVirtualMergeBaseIndexes(t *testing.T) {
	rows := virtualMergeBaseRows(t, dtu.CrissCross{
		Seed: []string{
			"CREATE TABLE t (id INT PRIMARY KEY, a VARCHAR(100), b VARCHAR(100), INDEX ia (a), INDEX iab (a, b));",
			"INSERT INTO t VALUES (1, 'seed', 'seed'), (2, 'seed', 'seed'), (3, 'seed', 'seed');",
		},
		X1: []string{
			"UPDATE t SET a = 'x' WHERE id = 1;",
			"UPDATE t SET b = 'x' WHERE id = 2;",
			"DELETE FROM t WHERE id = 3;",
			"INSERT INTO t VALUES (4, 'x', 'x');",
		},
		Y1: []string{"UPDATE t SET a = 'y' WHERE id IN (1, 2, 3);", "INSERT INTO t VALUES (4, 'y', 'y');"},
		Y2: ours,
		X2: ours,
	}, "SELECT * FROM t ORDER BY id;")
	assert.Equal(t, []sql.Row{{int32(1), "seed", "seed"}, {int32(2), "y", "x"}, {int32(3), "seed", "seed"}}, rows)
}

// TestVirtualMergeBaseSpecialIndexes is TestVirtualMergeBaseIndexes for a composite string key, a prefix index, a
// nullable unique index and a composite index. x1 and y1 conflict on row 1's a, which they change out of its prefix,
// on row 2's a, which they change within it, on row 3's b, which they change from NULL, and on row 6's b, which x1
// changes to NULL, and both add row 5, so the virtual merge base takes c1's values back into those indexes and leaves
// row 5 out. Row 4 is untouched.
func TestVirtualMergeBaseSpecialIndexes(t *testing.T) {
	rows := virtualMergeBaseRows(t, dtu.CrissCross{
		Seed: []string{
			"CREATE TABLE t (k1 VARCHAR(10), k2 INT, a VARCHAR(100), b VARCHAR(100), PRIMARY KEY (k1, k2), INDEX ip (a(3)), UNIQUE INDEX ub (b), INDEX iab (a, b));",
			"INSERT INTO t VALUES ('r', 1, 'seedling', 'u1'), ('r', 2, 'seedless', 'u2'), ('r', 3, 'same', NULL), ('r', 4, 'keep', 'k'), ('r', 6, 'six', 'b6');",
		},
		X1: []string{
			"UPDATE t SET a = 'xavier' WHERE k2 = 1;",
			"UPDATE t SET a = 'seedxx' WHERE k2 = 2;",
			"UPDATE t SET b = 'x3' WHERE k2 = 3;",
			"UPDATE t SET b = NULL WHERE k2 = 6;",
			"INSERT INTO t VALUES ('r', 5, 'xx', 'x5');",
		},
		Y1: []string{
			"UPDATE t SET a = 'yvonne' WHERE k2 = 1;",
			"UPDATE t SET a = 'seedyy' WHERE k2 = 2;",
			"UPDATE t SET b = 'y3' WHERE k2 = 3;",
			"UPDATE t SET b = 'y6' WHERE k2 = 6;",
			"INSERT INTO t VALUES ('r', 5, 'yy', 'y5');",
		},
		Y2: ours,
		X2: ours,
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

// fullTextSearches searches column a of table t for 'alpha', 'bravo', 'resolved' and 'seed'.
const fullTextSearches = `SELECT 'alpha', a FROM t WHERE MATCH(a) AGAINST('alpha')
	UNION ALL SELECT 'bravo', a FROM t WHERE MATCH(a) AGAINST('bravo')
	UNION ALL SELECT 'resolved', a FROM t WHERE MATCH(a) AGAINST('resolved')
	UNION ALL SELECT 'seed', a FROM t WHERE MATCH(a) AGAINST('seed')
	ORDER BY 1, 2;`

// TestVirtualMergeBaseFullTextIndexes checks that searching a virtual merge base's Full-Text index finds its rows. c1
// seeds rows 1 and 2 with a = b = 'seed'. x1 sets a to 'alpha' in both rows, and y1 sets row 1's a to 'bravo' and
// row 2's b to 'y'. So x1 and y1 conflict on row 1's a, which the virtual merge base sets back to 'seed', and merge
// row 2 to a = 'alpha', b = 'y'.
func TestVirtualMergeBaseFullTextIndexes(t *testing.T) {
	resolve := append(ours, "UPDATE t SET a = 'resolved' WHERE id = 1;")
	rows := virtualMergeBaseRows(t, dtu.CrissCross{
		Seed: []string{
			"CREATE TABLE t (id INT PRIMARY KEY, a VARCHAR(100), b VARCHAR(100), FULLTEXT INDEX fa (a));",
			"INSERT INTO t VALUES (1, 'seed', 'seed'), (2, 'seed', 'seed');",
		},
		X1: []string{"UPDATE t SET a = 'alpha';"},
		Y1: []string{"UPDATE t SET a = 'bravo' WHERE id = 1;", "UPDATE t SET b = 'y' WHERE id = 2;"},
		Y2: resolve,
		X2: resolve,
	}, fullTextSearches)
	assert.Equal(t, []sql.Row{{"alpha", "alpha"}, {"seed", "seed"}}, rows)
}

// TestVirtualMergeBaseFullTextIndexesOfKeylessTable is TestVirtualMergeBaseFullTextIndexes for a keyless table. c1
// seeds the row 'seed'; x1 changes it to 'alpha' and y1 to 'bravo', and x2 and y2 resolve the conflict by keeping ours.
// x1 and y1 both delete 'seed', which a keyless merge counts as a conflict, so the virtual merge base keeps c1's copy
// of it, and their added rows.
func TestVirtualMergeBaseFullTextIndexesOfKeylessTable(t *testing.T) {
	rows := virtualMergeBaseRows(t, dtu.CrissCross{
		Seed: []string{"CREATE TABLE t (a VARCHAR(100), FULLTEXT INDEX fa (a));", "INSERT INTO t VALUES ('seed');"},
		X1:   []string{"UPDATE t SET a = 'alpha';"},
		Y1:   []string{"UPDATE t SET a = 'bravo';"},
		Y2:   ours,
		X2:   ours,
	}, fullTextSearches)
	assert.Equal(t, []sql.Row{{"alpha", "alpha"}, {"bravo", "bravo"}, {"seed", "seed"}}, rows)
}

// TestVirtualMergeBaseFullTextIndexesOfCleanRows is TestVirtualMergeBaseFullTextIndexes for merge bases that change
// different rows, x1 row 1's a to 'alpha' and y1 row 2's to 'bravo', which merge cleanly. Merging them still has to
// rebuild the Full-Text index, which the virtual merge base does after its rows are merged.
func TestVirtualMergeBaseFullTextIndexesOfCleanRows(t *testing.T) {
	rows := virtualMergeBaseRows(t, dtu.CrissCross{
		Seed: []string{
			"CREATE TABLE t (id INT PRIMARY KEY, a VARCHAR(100), FULLTEXT INDEX fa (a));",
			"INSERT INTO t VALUES (1, 'seed'), (2, 'seed'), (3, 'seed');",
		},
		X1: []string{"UPDATE t SET a = 'alpha' WHERE id = 1;"},
		Y1: []string{"UPDATE t SET a = 'bravo' WHERE id = 2;"},
	}, fullTextSearches)
	assert.Equal(t, []sql.Row{{"alpha", "alpha"}, {"bravo", "bravo"}, {"seed", "seed"}}, rows)
}

// TestVirtualMergeBaseVectorIndex checks nearest-neighbor searches of a virtual merge base's vector index. c1 has rows
// 1 at [50,60] and 2 at [42,52]. x1 moves row 1 to [0,0] and adds row 3 at [10,10]; y1 moves row 1 to [99,99] and adds
// row 3 at [90,90]. So the virtual merge base has row 1 back at [50,60], row 2 untouched, and no row 3. Each probe is
// nearest to one of its rows, and nearer still to a vector it must not have.
func TestVirtualMergeBaseVectorIndex(t *testing.T) {
	probe := func(v string) string {
		return "(SELECT '" + v + "', id FROM t ORDER BY VEC_DISTANCE(e, STRING_TO_VECTOR('" + v + "')) LIMIT 1)"
	}
	rows := virtualMergeBaseRows(t, dtu.CrissCross{
		Seed: []string{
			"CREATE TABLE t (id INT PRIMARY KEY, e VECTOR(2) NOT NULL, VECTOR INDEX vidx (e));",
			"INSERT INTO t VALUES (1, STRING_TO_VECTOR('[50,60]')), (2, STRING_TO_VECTOR('[42,52]'));",
		},
		X1: []string{"UPDATE t SET e = STRING_TO_VECTOR('[0,0]') WHERE id = 1;", "INSERT INTO t VALUES (3, STRING_TO_VECTOR('[10,10]'));"},
		Y1: []string{"UPDATE t SET e = STRING_TO_VECTOR('[99,99]') WHERE id = 1;", "INSERT INTO t VALUES (3, STRING_TO_VECTOR('[90,90]'));"},
		Y2: ours,
		X2: ours,
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

// TestVirtualMergeBaseUnconvertibleValues checks a virtual merge base whose merge bases change column c's type
// differently, a schema conflict, so that it keeps c1's schema, where c is INT:
//
//	c1 -- x1 -- xm -- x2    x
//	  \-- y1 -- ym -- y2    y
//	x2 = merge(xm, y1), y2 = merge(ym, x1)
//
// x1 makes c VARCHAR, sets row 1's c to |value| and d to 'x1', changes row 2's d, and adds row 3 with c |value|. y1
// makes c BIGINT and sets row 1's d to 'y1'. xm and ym change c's type to match the other before merging. A value that
// does not convert to INT conflicts: an existing row keeps c1's value, and a row only x1 has keeps its other values,
// with NULL in c. A value that converts takes part in the merge.
func TestVirtualMergeBaseUnconvertibleValues(t *testing.T) {
	history := func(value string) dtu.CrissCross {
		return dtu.CrissCross{
			Seed: []string{"CREATE TABLE t (id INT PRIMARY KEY, c INT, d VARCHAR(20));", "INSERT INTO t VALUES (1, 1, 'seed'), (2, 2, 'seed');"},
			X1: []string{
				"ALTER TABLE t MODIFY c VARCHAR(20);",
				"UPDATE t SET c = '" + value + "', d = 'x1' WHERE id = 1;",
				"UPDATE t SET d = 'x' WHERE id = 2;",
				"INSERT INTO t VALUES (3, '" + value + "', 'three');",
			},
			Y1: []string{"ALTER TABLE t MODIFY c BIGINT;", "UPDATE t SET d = 'y1' WHERE id = 1;"},
			YM: []string{"ALTER TABLE t MODIFY c VARCHAR(20);"},
			Y2: ours,
			XM: []string{"UPDATE t SET c = '5';", "ALTER TABLE t MODIFY c BIGINT;"},
			X2: ours,
		}
	}
	rows := virtualMergeBaseRows(t, history("not-an-int"), "SELECT * FROM t ORDER BY id;")
	assert.Equal(t, []sql.Row{{int32(1), int32(1), "seed"}, {int32(2), int32(2), "x"}, {int32(3), nil, "three"}}, rows)
	rows = virtualMergeBaseRows(t, history("42"), "SELECT * FROM t ORDER BY id;")
	assert.Equal(t, []sql.Row{{int32(1), int32(42), "seed"}, {int32(2), int32(2), "x"}, {int32(3), int32(42), "three"}}, rows)
}

// TestVirtualMergeBaseUnconvertibleKeylessValues is TestVirtualMergeBaseUnconvertibleValues for a keyless table. x1
// adds the row ('not-an-int', 'k'), whose c does not convert to INT, and y1 adds (2, 'y'). The row that does not
// convert conflicts, and keeps the copies c1 has of it, none; the other row merges.
func TestVirtualMergeBaseUnconvertibleKeylessValues(t *testing.T) {
	rows := virtualMergeBaseRows(t, dtu.CrissCross{
		Seed: []string{"SET @@dolt_allow_commit_conflicts = 1;", "CREATE TABLE t (c INT, d VARCHAR(20));", "INSERT INTO t VALUES (1, 'a');"},
		X1:   []string{"ALTER TABLE t MODIFY c VARCHAR(20);", "INSERT INTO t VALUES ('not-an-int', 'k');"},
		Y1:   []string{"ALTER TABLE t MODIFY c BIGINT;", "INSERT INTO t VALUES (2, 'y');"},
		YM:   []string{"ALTER TABLE t MODIFY c VARCHAR(20);"},
		Y2:   ours,
		XM:   []string{"DELETE FROM t WHERE c = 'not-an-int';", "ALTER TABLE t MODIFY c BIGINT;"},
		X2:   ours,
	}, "SELECT * FROM t ORDER BY c;")
	assert.Equal(t, []sql.Row{{int32(1), "a"}, {int32(2), "y"}}, rows)
}

// TestVirtualMergeBaseKeylessRowsCollapse checks a virtual merge base of a keyless table whose merge bases have
// different schemas, so that it matches their rows by their values under c1's schema, (a). c1 has one copy each of
// rows 7, 8 and 9. x1 adds column c, and has two copies of (7, 1) and three of (7, 2), which are all row 7 under c1's
// schema. y1 deletes row 8. So the virtual merge base has five copies of row 7, none of row 8, and one of row 9.
func TestVirtualMergeBaseKeylessRowsCollapse(t *testing.T) {
	rows := virtualMergeBaseRows(t, dtu.CrissCross{
		Seed: []string{"SET @@dolt_allow_commit_conflicts = 1;", "CREATE TABLE t (a INT);", "INSERT INTO t VALUES (7), (8), (9);"},
		X1: []string{
			"ALTER TABLE t ADD COLUMN c INT;",
			"UPDATE t SET c = 1 WHERE a = 7;",
			"INSERT INTO t VALUES (7, 1), (7, 2), (7, 2), (7, 2);",
		},
		Y1: []string{"DELETE FROM t WHERE a = 8;"},
		Y2: ours,
		X2: ours,
	}, "SELECT a, count(*) FROM t GROUP BY a ORDER BY a;")
	assert.Equal(t, []sql.Row{{int32(7), int64(5)}, {int32(9), int64(1)}}, rows)
}

// virtualMergeBaseRows builds |history|, resolves the virtual merge base of branches x and y, checks its secondary
// indexes, and returns the rows |query| reads from it.
func virtualMergeBaseRows(t *testing.T, history dtu.CrissCross, query string) []sql.Row {
	ctx := context.Background()
	dEnv := dtu.CreateTestEnv()
	t.Cleanup(func() { dEnv.DoltDB(ctx).Close() })
	dbName, eng := engineFromEnvironment(ctx, dEnv)
	sqlCtx, err := eng.NewLocalContext(ctx)
	require.NoError(t, err)
	sqlCtx.SetCurrentDatabase(dbName)
	for _, q := range history.Script() {
		require.NoError(t, executeQuery(sqlCtx, eng, q), q)
	}
	x, y := resolveBranch(t, sqlCtx, dEnv.DoltDB(ctx), "x"), resolveBranch(t, sqlCtx, dEnv.DoltDB(ctx), "y")
	virtualBase, err := merge.ResolveMergeBase(sqlCtx, x, y)
	require.NoError(t, err)
	root, err := virtualBase.GetRootValue(sqlCtx)
	require.NoError(t, err)
	stale, err := dtu.StaleIndexes(sqlCtx, root)
	require.NoError(t, err)
	assert.Empty(t, stale, "indexes of the virtual merge base that do not match its rows")
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

func resolveBranch(t *testing.T, ctx *sql.Context, ddb *doltdb.DoltDB, branch string) *doltdb.Commit {
	spec, err := doltdb.NewCommitSpec(branch)
	require.NoError(t, err)
	optCmt, err := ddb.Resolve(ctx, spec, nil)
	require.NoError(t, err)
	commit, ok := optCmt.ToCommit()
	require.True(t, ok)
	return commit
}
