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

package enginetest

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// schemaBranchSetup builds the history from https://github.com/dolthub/dolt/issues/12050. A schema branch is merged
// into main and feature, and never receives their changes, so main and feature have two merge bases, the fork point
// and s2:
//
//	c1 -- c2 -- c3 -- m    main
//	 |     \-- f1 -- f    feature
//	 \-- s1 -- s2          schema
//	m = merge(c3, s2), f = merge(f1, s2)
//
// c2 seeds rows 1 and 2. c3 edits row 1 and deletes row 2. f1 adds row 50. s1 and s2 add columns n and m.
//
// |extraSeedCommits| adds c2-3, c2-4, ... after c2, each seeding one more row, and feature forks from the last of
// them, which makes the fork point as tall as s2 (1) or taller (2). With |schemaFirst|, s1 and s2 are committed
// before c2, so the fork point is newer than s2.
func schemaBranchSetup(extraSeedCommits int, schemaFirst bool) []string {
	schema := []string{
		"CALL DOLT_CHECKOUT('schema');",
		"ALTER TABLE t ADD COLUMN n VARCHAR(100);",
		"CALL DOLT_COMMIT('-am', 's1');",
		"ALTER TABLE t ADD COLUMN m VARCHAR(100);",
		"CALL DOLT_COMMIT('-am', 's2');",
		"CALL DOLT_CHECKOUT('main');",
	}
	script := []string{
		"CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(100));",
		"CALL DOLT_COMMIT('-Am', 'c1');",
		"CALL DOLT_BRANCH('schema');",
	}
	if schemaFirst {
		script = append(script, schema...)
	}
	script = append(script,
		"INSERT INTO t VALUES (1, 'seed'), (2, 'seed');",
		"CALL DOLT_COMMIT('-am', 'c2');")
	for i := 0; i < extraSeedCommits; i++ {
		script = append(script,
			fmt.Sprintf("INSERT INTO t VALUES (%d, 'seed');", 3+i),
			fmt.Sprintf("CALL DOLT_COMMIT('-am', 'c2-%d');", 3+i))
	}
	script = append(script,
		"CALL DOLT_BRANCH('feature');",
		"UPDATE t SET v = 'edited on main' WHERE id = 1;",
		"DELETE FROM t WHERE id = 2;",
		"CALL DOLT_COMMIT('-am', 'c3');",
		"CALL DOLT_CHECKOUT('feature');",
		"INSERT INTO t VALUES (50, 'feature');",
		"CALL DOLT_COMMIT('-am', 'f1');")
	if !schemaFirst {
		script = append(script, schema...)
	}
	return append(script,
		"CALL DOLT_CHECKOUT('main');",
		"CALL DOLT_MERGE('schema', '-m', 'm');",
		"CALL DOLT_CHECKOUT('feature');",
		"CALL DOLT_MERGE('schema', '-m', 'f');")
}

// longRunningSchemaBranchSetup builds six cycles of the schema branch pattern, with feature merging main once after
// cycle 3:
//
//	c0 -- c1 -- main1 -- m1 -- ... -- main6 -- m6                 main
//	 |     \-- feat1 -- f1 -- ... -- sync -- ... -- feat6 -- f6    feature
//	 \-- s1 -- s2 -- s3 -- s4 -- s5 -- s6                         schema
//	m<i> = merge(main<i>, s<i>), f<i> = merge(feat<i>, s<i>), sync = merge(f3, m3)
//
// c1 seeds rows 1, 2 and 3. In cycle i, s<i> adds column x<i>, main<i> inserts row 100+i and sets row 1 to
// "main <i>", and feat<i> inserts row 200+i. main5 also deletes row 2. With |conflict|, feat5 also sets row 1 to
// "feature 5", and row 3 is edited in different columns: v on main5 and w on feat5.
func longRunningSchemaBranchSetup(conflict bool) []string {
	script := []string{
		"CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(100), w VARCHAR(100));",
		"CALL DOLT_COMMIT('-Am', 'c0');",
		"CALL DOLT_BRANCH('schema');",
		"INSERT INTO t VALUES (1, 'seed', 'seed'), (2, 'seed', 'seed'), (3, 'seed', 'seed');",
		"CALL DOLT_COMMIT('-am', 'c1');",
		"CALL DOLT_BRANCH('feature');",
	}
	for i := 1; i <= 6; i++ {
		script = append(script,
			"CALL DOLT_CHECKOUT('schema');",
			fmt.Sprintf("ALTER TABLE t ADD COLUMN x%d VARCHAR(100);", i),
			fmt.Sprintf("CALL DOLT_COMMIT('-am', 's%d');", i),
			"CALL DOLT_CHECKOUT('main');",
			fmt.Sprintf("INSERT INTO t (id, v) VALUES (%d, 'main');", 100+i),
			fmt.Sprintf("UPDATE t SET v = 'main %d' WHERE id = 1;", i))
		if i == 5 {
			script = append(script, "DELETE FROM t WHERE id = 2;")
			if conflict {
				script = append(script, "UPDATE t SET v = 'main' WHERE id = 3;")
			}
		}
		script = append(script,
			fmt.Sprintf("CALL DOLT_COMMIT('-am', 'main%d');", i),
			fmt.Sprintf("CALL DOLT_MERGE('schema', '-m', 'm%d');", i),
			"CALL DOLT_CHECKOUT('feature');",
			fmt.Sprintf("INSERT INTO t (id, v) VALUES (%d, 'feature');", 200+i))
		if conflict && i == 5 {
			script = append(script,
				"UPDATE t SET v = 'feature 5' WHERE id = 1;",
				"UPDATE t SET w = 'feature' WHERE id = 3;")
		}
		script = append(script,
			fmt.Sprintf("CALL DOLT_COMMIT('-am', 'feat%d');", i),
			fmt.Sprintf("CALL DOLT_MERGE('schema', '-m', 'f%d');", i))
		if i == 3 {
			script = append(script, "CALL DOLT_MERGE('main', '-m', 'sync');")
		}
	}
	return script
}

// longRunningRows are the rows of t, other than rows 1 and 3, after main and feature merge in either direction.
var longRunningRows = []sql.Row{
	{101, "main", nil}, {102, "main", nil}, {103, "main", nil}, {104, "main", nil}, {105, "main", nil}, {106, "main", nil},
	{201, "feature", nil}, {202, "feature", nil}, {203, "feature", nil}, {204, "feature", nil}, {205, "feature", nil},
	{206, "feature", nil},
}

// conflictingMergeBasesSetup builds a criss-cross whose merge bases, x1 and y1, conflict with each other:
//
//	c1 -- x1 -- x2 [-- x3]    x
//	  \-- y1 -- y2            y
//	x2 = merge(x1, y1), y2 = merge(y1, x1)
//
// c1 seeds rows 1 and 2 with a = b = 'seed'. x1 sets a to 'x' in both rows, and y1 sets row 1's a to 'y' and row 2's
// b to 'y'. So x1 and y1 conflict on row 1's a, and row 2 merges cleanly to a = 'x', b = 'y'. x2 and y2 resolve
// the conflict to |xResolution| and |yResolution|. With |xLater|, x3 then sets row 2's a to 'x later'.
func conflictingMergeBasesSetup(xResolution, yResolution string, xLater bool) []string {
	return conflictingMergeBasesSetupWithTable("CREATE TABLE t (id INT PRIMARY KEY, a VARCHAR(100), b VARCHAR(100));",
		xResolution, yResolution, xLater)
}

func conflictingMergeBasesSetupWithTable(createTable, xResolution, yResolution string, xLater bool) []string {
	script := []string{
		"SET @@autocommit = 0;",
		createTable,
		"INSERT INTO t (id, a, b) VALUES (1, 'seed', 'seed'), (2, 'seed', 'seed');",
		"CALL DOLT_COMMIT('-Am', 'c1');",
		"CALL DOLT_BRANCH('x');",
		"CALL DOLT_BRANCH('y');",
		"CALL DOLT_CHECKOUT('x');",
		"UPDATE t SET a = 'x';",
		"CALL DOLT_COMMIT('-am', 'x1');",
		"CALL DOLT_CHECKOUT('y');",
		"UPDATE t SET a = 'y' WHERE id = 1;",
		"UPDATE t SET b = 'y' WHERE id = 2;",
		"CALL DOLT_COMMIT('-am', 'y1');",
		"CALL DOLT_BRANCH('y1');",
		"CALL DOLT_MERGE('x', '-m', 'y2');",
		"CALL DOLT_CONFLICTS_RESOLVE('--ours', 't');",
		fmt.Sprintf("UPDATE t SET a = '%s' WHERE id = 1;", yResolution),
		"CALL DOLT_COMMIT('-am', 'y2');",
		"CALL DOLT_CHECKOUT('x');",
		"CALL DOLT_MERGE('y1', '-m', 'x2');",
		"CALL DOLT_CONFLICTS_RESOLVE('--ours', 't');",
		fmt.Sprintf("UPDATE t SET a = '%s' WHERE id = 1;", xResolution),
		"CALL DOLT_COMMIT('-am', 'x2');",
	}
	if xLater {
		script = append(script,
			"UPDATE t SET a = 'x later' WHERE id = 2;",
			"CALL DOLT_COMMIT('-am', 'x3');")
	}
	return script
}

// threeMergeBasesSetup builds two branches with three merge bases, p1, p2 and p3, two of which conflict:
//
//	c1 -- m1start -- merge p1 -- merge p2 -- merge p3 -- edit row 2    m1
//	 |\-- m2start -- merge p3 -- merge p2 -- merge p1 -- edit row 3    m2
//	 |\-- p1    sets rows 1 and 2's a to 'p1'
//	 |\-- p2    sets rows 1 and 3's a to 'p2'
//	 \-- p3     sets row 4's b to 'p3'
//
// c1 seeds rows 1 to 4 with a = b = 'seed'. p1 and p2 conflict on row 1's a, and m1 and m2 both resolve it to
// 'resolved'. m1start and m2start add rows 10 and 20.
func threeMergeBasesSetup() []string {
	return []string{
		"SET @@autocommit = 0;",
		"CREATE TABLE t (id INT PRIMARY KEY, a VARCHAR(100), b VARCHAR(100));",
		"INSERT INTO t VALUES (1, 'seed', 'seed'), (2, 'seed', 'seed'), (3, 'seed', 'seed'), (4, 'seed', 'seed');",
		"CALL DOLT_COMMIT('-Am', 'c1');",
		"CALL DOLT_BRANCH('p1');",
		"CALL DOLT_BRANCH('p2');",
		"CALL DOLT_BRANCH('p3');",
		"CALL DOLT_BRANCH('m1');",
		"CALL DOLT_BRANCH('m2');",
		"CALL DOLT_CHECKOUT('p1');",
		"UPDATE t SET a = 'p1' WHERE id IN (1, 2);",
		"CALL DOLT_COMMIT('-am', 'p1');",
		"CALL DOLT_CHECKOUT('p2');",
		"UPDATE t SET a = 'p2' WHERE id IN (1, 3);",
		"CALL DOLT_COMMIT('-am', 'p2');",
		"CALL DOLT_CHECKOUT('p3');",
		"UPDATE t SET b = 'p3' WHERE id = 4;",
		"CALL DOLT_COMMIT('-am', 'p3');",
		"CALL DOLT_CHECKOUT('m1');",
		"INSERT INTO t VALUES (10, 'm1', NULL);",
		"CALL DOLT_COMMIT('-am', 'm1start');",
		"CALL DOLT_MERGE('p1', '-m', 'm1 merges p1');",
		"CALL DOLT_MERGE('p2', '-m', 'm1 merges p2');",
		"CALL DOLT_CONFLICTS_RESOLVE('--ours', 't');",
		"UPDATE t SET a = 'resolved' WHERE id = 1;",
		"CALL DOLT_COMMIT('-am', 'm1 merges p2');",
		"CALL DOLT_MERGE('p3', '-m', 'm1 merges p3');",
		"UPDATE t SET b = 'm1' WHERE id = 2;",
		"CALL DOLT_COMMIT('-am', 'm1 edits row 2');",
		"CALL DOLT_CHECKOUT('m2');",
		"INSERT INTO t VALUES (20, 'm2', NULL);",
		"CALL DOLT_COMMIT('-am', 'm2start');",
		"CALL DOLT_MERGE('p3', '-m', 'm2 merges p3');",
		"CALL DOLT_MERGE('p2', '-m', 'm2 merges p2');",
		"CALL DOLT_MERGE('p1', '-m', 'm2 merges p1');",
		"CALL DOLT_CONFLICTS_RESOLVE('--ours', 't');",
		"UPDATE t SET a = 'resolved' WHERE id = 1;",
		"CALL DOLT_COMMIT('-am', 'm2 merges p1');",
		"UPDATE t SET b = 'm2' WHERE id = 3;",
		"CALL DOLT_COMMIT('-am', 'm2 edits row 3');",
		"CALL DOLT_CHECKOUT('m1');",
	}
}

// deleteAgainstEditSetup builds a criss-cross whose merge bases, x1 and y1, conflict on whether row 1 exists:
//
//	c1 -- x1 -- x2    x
//	  \-- y1 -- y2    y
//	x2 = merge(x1, y1), y2 = merge(y1, x1)
//
// c1 seeds rows 1 and 2 with a = b = 'seed'. x1 deletes row 1 and sets row 2's a to 'x'; y1 sets row 1's a to 'y'
// and row 2's b to 'y'. x2 and y2 resolve row 1 by deleting it, or by keeping y1's row, as |xKeeps| and |yKeeps| say.
func deleteAgainstEditSetup(xKeeps, yKeeps bool) []string {
	resolve := func(keep bool) string {
		if keep {
			return "REPLACE INTO t VALUES (1, 'y', 'seed');"
		}
		return "DELETE FROM t WHERE id = 1;"
	}
	return []string{
		"SET @@autocommit = 0;",
		"CREATE TABLE t (id INT PRIMARY KEY, a VARCHAR(100), b VARCHAR(100));",
		"INSERT INTO t VALUES (1, 'seed', 'seed'), (2, 'seed', 'seed');",
		"CALL DOLT_COMMIT('-Am', 'c1');",
		"CALL DOLT_BRANCH('x');",
		"CALL DOLT_BRANCH('y');",
		"CALL DOLT_CHECKOUT('x');",
		"DELETE FROM t WHERE id = 1;",
		"UPDATE t SET a = 'x' WHERE id = 2;",
		"CALL DOLT_COMMIT('-am', 'x1');",
		"CALL DOLT_CHECKOUT('y');",
		"UPDATE t SET a = 'y' WHERE id = 1;",
		"UPDATE t SET b = 'y' WHERE id = 2;",
		"CALL DOLT_COMMIT('-am', 'y1');",
		"CALL DOLT_BRANCH('y1');",
		"CALL DOLT_MERGE('x', '-m', 'y2');",
		"CALL DOLT_CONFLICTS_RESOLVE('--ours', 't');",
		resolve(yKeeps),
		"CALL DOLT_COMMIT('-am', 'y2');",
		"CALL DOLT_CHECKOUT('x');",
		"CALL DOLT_MERGE('y1', '-m', 'x2');",
		"CALL DOLT_CONFLICTS_RESOLVE('--ours', 't');",
		resolve(xKeeps),
		"CALL DOLT_COMMIT('-am', 'x2');",
	}
}

// nestedCrissCrossSetup builds two criss-crosses, one on top of the other, so that the merge bases of x3 and y3 are
// x2 and y2, whose own merge bases are x1 and y1:
//
//	c1 -- x1 -- x2 -- x3    x
//	  \-- y1 -- y2 -- y3    y
//	x2 = merge(x1, y1), y2 = merge(y1, x1)
//	x3 = merge(x2, y2), y3 = merge(y2, x2)
//
// The first criss-cross is conflictingMergeBasesSetup's, with row 1's a resolved to 'seed' in x2 and to 'y' in y2.
// x2 and y2 conflict on it again, because it conflicted between x1 and y1. x3 and y3 resolve it to 'y' and 'q'.
func nestedCrissCrossSetup() []string {
	return append(conflictingMergeBasesSetup("seed", "y", false),
		"CALL DOLT_BRANCH('x2');",
		"CALL DOLT_CHECKOUT('y');",
		"CALL DOLT_BRANCH('y2');",
		"CALL DOLT_MERGE('x2', '-m', 'y3');",
		"CALL DOLT_CONFLICTS_RESOLVE('--ours', 't');",
		"UPDATE t SET a = 'q' WHERE id = 1;",
		"CALL DOLT_COMMIT('-am', 'y3');",
		"CALL DOLT_CHECKOUT('x');",
		"CALL DOLT_MERGE('y2', '-m', 'x3');",
		"CALL DOLT_CONFLICTS_RESOLVE('--ours', 't');",
		"UPDATE t SET a = 'y' WHERE id = 1;",
		"CALL DOLT_COMMIT('-am', 'x3');",
	)
}

// schemaChangingMergeBasesSetup builds a criss-cross whose merge bases, x1 and y1, conflict and have different
// schemas:
//
//	c1 -- x1 -- x2    x
//	  \-- y1 -- y2    y
//	x2 = merge(x1, y1), y2 = merge(y1, x1)
//
// c1 seeds rows 1 and 2 with a = b = 'seed'. x1 adds column c, sets row 1's a to 'x', and adds row 3. y1 sets row 1's
// a to 'y'. So x1 and y1 conflict on row 1's a, and row 3 merges cleanly. x2 resolves the conflict to 'x wins'; y2
// resolves it back to 'seed' and deletes row 3.
func schemaChangingMergeBasesSetup() []string {
	return []string{
		"SET @@autocommit = 0;",
		"CREATE TABLE t (id INT PRIMARY KEY, a VARCHAR(100), b VARCHAR(100));",
		"INSERT INTO t VALUES (1, 'seed', 'seed'), (2, 'seed', 'seed');",
		"CALL DOLT_COMMIT('-Am', 'c1');",
		"CALL DOLT_BRANCH('x');",
		"CALL DOLT_BRANCH('y');",
		"CALL DOLT_CHECKOUT('x');",
		"ALTER TABLE t ADD COLUMN c VARCHAR(100);",
		"UPDATE t SET a = 'x' WHERE id = 1;",
		"INSERT INTO t VALUES (3, 'x', 'x', 'x');",
		"CALL DOLT_COMMIT('-am', 'x1');",
		"CALL DOLT_BRANCH('x1');",
		"CALL DOLT_CHECKOUT('y');",
		"UPDATE t SET a = 'y' WHERE id = 1;",
		"CALL DOLT_COMMIT('-am', 'y1');",
		"CALL DOLT_BRANCH('y1');",
		"CALL DOLT_MERGE('x1', '-m', 'y2');",
		"CALL DOLT_CONFLICTS_RESOLVE('--ours', 't');",
		"UPDATE t SET a = 'seed' WHERE id = 1;",
		"DELETE FROM t WHERE id = 3;",
		"CALL DOLT_COMMIT('-am', 'y2');",
		"CALL DOLT_CHECKOUT('x');",
		"CALL DOLT_MERGE('y1', '-m', 'x2');",
		"CALL DOLT_CONFLICTS_RESOLVE('--ours', 't');",
		"UPDATE t SET a = 'x wins' WHERE id = 1;",
		"CALL DOLT_COMMIT('-am', 'x2');",
	}
}

// jsonMergeBasesSetup builds a criss-cross whose merge bases, x1 and y1, add different keys to a JSON document:
//
//	c1 -- x1 -- x2 -- x3    x
//	  \-- y1 -- y2          y
//	x2 = merge(x1, y1), y2 = merge(y1, x1)
//
// c1 seeds row 1 with j = {}. x1 adds key x and y1 adds key y, which x2 and y2 merge to {"x": 1, "y": 1}. x3 sets x
// to 2.
func jsonMergeBasesSetup() []string {
	return []string{
		"SET @@autocommit = 0;",
		"CREATE TABLE t (id INT PRIMARY KEY, j JSON);",
		"INSERT INTO t VALUES (1, '{}');",
		"CALL DOLT_COMMIT('-Am', 'c1');",
		"CALL DOLT_BRANCH('x');",
		"CALL DOLT_BRANCH('y');",
		"CALL DOLT_CHECKOUT('x');",
		`UPDATE t SET j = '{"x": 1}';`,
		"CALL DOLT_COMMIT('-am', 'x1');",
		"CALL DOLT_BRANCH('x1');",
		"CALL DOLT_CHECKOUT('y');",
		`UPDATE t SET j = '{"y": 1}';`,
		"CALL DOLT_COMMIT('-am', 'y1');",
		"CALL DOLT_BRANCH('y1');",
		"CALL DOLT_MERGE('x1', '-m', 'y2');",
		"CALL DOLT_CHECKOUT('x');",
		"CALL DOLT_MERGE('y1', '-m', 'x2');",
		`UPDATE t SET j = '{"x": 2, "y": 1}';`,
		"CALL DOLT_COMMIT('-am', 'x3');",
	}
}

// crissCrossSetup builds a criss-cross from c1, made by |seed|:
//
//	c1 -- x1 -- x2    x
//	  \-- y1 -- y2    y
//	x2 = merge(x1, y1), y2 = merge(y1, x1)
//
// |x1| and |y1| make x1 and y1. |y2| and |x2| resolve the merges that make y2 and x2 before they are committed.
func crissCrossSetup(seed, x1, y1, y2, x2 []string) []string {
	script := append([]string{"SET @@autocommit = 0;"}, seed...)
	script = append(script, "CALL DOLT_COMMIT('-Am', 'c1');", "CALL DOLT_BRANCH('x');", "CALL DOLT_BRANCH('y');",
		"CALL DOLT_CHECKOUT('x');")
	script = append(script, x1...)
	script = append(script, "CALL DOLT_COMMIT('-am', 'x1');", "CALL DOLT_BRANCH('x1');", "CALL DOLT_CHECKOUT('y');")
	script = append(script, y1...)
	script = append(script, "CALL DOLT_COMMIT('-am', 'y1');", "CALL DOLT_BRANCH('y1');", "CALL DOLT_MERGE('x1', '-m', 'y2');")
	script = append(script, y2...)
	script = append(script, "CALL DOLT_COMMIT('-am', 'y2');", "CALL DOLT_CHECKOUT('x');", "CALL DOLT_MERGE('y1', '-m', 'x2');")
	script = append(script, x2...)
	return append(script, "CALL DOLT_COMMIT('-am', 'x2');")
}

// conflictedRowWithCleanCellsSetup builds a criss-cross whose merge bases, x1 and y1, conflict on two cells of a row
// and change its other cells cleanly:
//
//	c1 -- x1 -- x2 [-- x3]    x
//	  \-- y1 -- y2            y
//	x2 = merge(x1, y1), y2 = merge(y1, x1)
//
// c1 seeds row 1 with a = b = c = 'seed' and d NULL. x1 sets a, b and d to 'x'; y1 sets a, c and d to 'y'. So x1 and
// y1 conflict on a, whose base is 'seed', and on d, whose base is NULL, and merge b to 'x' and c to 'y'. x2 and y2
// resolve the row to a = 'resolved', b = 'x', c = 'y', and d = |xD| and |yD|, SQL literals. With |xLater|, x3 then
// sets b to 'x later'.
func conflictedRowWithCleanCellsSetup(xD, yD string, xLater bool) []string {
	resolve := func(d string) string {
		return fmt.Sprintf("UPDATE t SET a = 'resolved', b = 'x', c = 'y', d = %s WHERE id = 1;", d)
	}
	script := crissCrossSetup(
		[]string{
			"CREATE TABLE t (id INT PRIMARY KEY, a VARCHAR(100), b VARCHAR(100), c VARCHAR(100), d VARCHAR(100));",
			"INSERT INTO t VALUES (1, 'seed', 'seed', 'seed', NULL);",
		},
		[]string{"UPDATE t SET a = 'x', b = 'x', d = 'x' WHERE id = 1;"},
		[]string{"UPDATE t SET a = 'y', c = 'y', d = 'y' WHERE id = 1;"},
		[]string{"CALL DOLT_CONFLICTS_RESOLVE('--ours', 't');", resolve(yD)},
		[]string{"CALL DOLT_CONFLICTS_RESOLVE('--ours', 't');", resolve(xD)},
	)
	if xLater {
		script = append(script, "UPDATE t SET b = 'x later' WHERE id = 1;", "CALL DOLT_COMMIT('-am', 'x3');")
	}
	return script
}

// addAddSetup builds a criss-cross whose merge bases, x1 and y1, both add row 3, with different values:
//
//	c1 -- x1 -- x2    x
//	  \-- y1 -- y2    y
//	x2 = merge(x1, y1), y2 = merge(y1, x1)
//
// c1 seeds row 1. x1 adds row 3 with a = 'x', and y1 with a = 'y'. Their merge base has no row 3, so the virtual merge
// base has none either. x2 and y2 resolve row 3 with |xResolution| and |yResolution|.
func addAddSetup(xResolution, yResolution string) []string {
	return crissCrossSetup(
		[]string{
			"CREATE TABLE t (id INT PRIMARY KEY, a VARCHAR(100));",
			"INSERT INTO t VALUES (1, 'seed');",
		},
		[]string{"INSERT INTO t VALUES (3, 'x');"},
		[]string{"INSERT INTO t VALUES (3, 'y');"},
		[]string{"CALL DOLT_CONFLICTS_RESOLVE('--ours', 't');", yResolution},
		[]string{"CALL DOLT_CONFLICTS_RESOLVE('--ours', 't');", xResolution},
	)
}

// foldedConflictsSetup builds two branches whose three merge bases, p1, p2 and p3, conflict in different pairs, so
// that folding them into one virtual merge base finds conflicts in more than one fold:
//
//	c1 -- m1start -- merge p1 -- merge p2 -- merge p3 -- m1    m1
//	 |\-- m2start -- merge p3 -- merge p2 -- merge p1 -- m2    m2
//	 |\-- p1    sets row 1's a to 'p1' and deletes row 2
//	 |\-- p2    sets row 1's a and b to 'p2', and row 3's b to 'p2'
//	 \-- p3     sets row 1's b to 'p3', and row 2's a to 'p3'
//
// c1 seeds rows 1 to 3 with a = b = 'seed'. p1 and p2 conflict on row 1's a, p2 and p3 on row 1's b, and p1 and p3 on
// row 2, which one deletes and the other changes. Whatever order the merge bases fold in, the virtual merge base has
// c1's row 1, with a and b conflicted, c1's row 2, which git's merge-ort also takes unmarked for a delete against a
// change, and p2's row 3. m1 and m2 resolve every conflict with ours, and finally set row 1 to |m1Row1| and |m2Row1|,
// the values of a and b. m1 deletes row 2 and m2 keeps c1's.
func foldedConflictsSetup(m1Row1, m2Row1 string) []string {
	return []string{
		"SET @@autocommit = 0;",
		"CREATE TABLE t (id INT PRIMARY KEY, a VARCHAR(100), b VARCHAR(100));",
		"INSERT INTO t VALUES (1, 'seed', 'seed'), (2, 'seed', 'seed'), (3, 'seed', 'seed');",
		"CALL DOLT_COMMIT('-Am', 'c1');",
		"CALL DOLT_BRANCH('p1');",
		"CALL DOLT_BRANCH('p2');",
		"CALL DOLT_BRANCH('p3');",
		"CALL DOLT_BRANCH('m1');",
		"CALL DOLT_BRANCH('m2');",
		"CALL DOLT_CHECKOUT('p1');",
		"UPDATE t SET a = 'p1' WHERE id = 1;",
		"DELETE FROM t WHERE id = 2;",
		"CALL DOLT_COMMIT('-am', 'p1');",
		"CALL DOLT_CHECKOUT('p2');",
		"UPDATE t SET a = 'p2', b = 'p2' WHERE id = 1;",
		"UPDATE t SET b = 'p2' WHERE id = 3;",
		"CALL DOLT_COMMIT('-am', 'p2');",
		"CALL DOLT_CHECKOUT('p3');",
		"UPDATE t SET b = 'p3' WHERE id = 1;",
		"UPDATE t SET a = 'p3' WHERE id = 2;",
		"CALL DOLT_COMMIT('-am', 'p3');",
		"CALL DOLT_CHECKOUT('m1');",
		"INSERT INTO t VALUES (10, 'm1', NULL);",
		"CALL DOLT_COMMIT('-am', 'm1start');",
		"CALL DOLT_MERGE('p1', '-m', 'm1 merges p1');",
		"CALL DOLT_MERGE('p2', '-m', 'm1 merges p2');",
		"CALL DOLT_CONFLICTS_RESOLVE('--ours', 't');",
		"CALL DOLT_COMMIT('-am', 'm1 merges p2');",
		"CALL DOLT_MERGE('p3', '-m', 'm1 merges p3');",
		"CALL DOLT_CONFLICTS_RESOLVE('--ours', 't');",
		"CALL DOLT_COMMIT('-am', 'm1 merges p3');",
		fmt.Sprintf("REPLACE INTO t VALUES (1, %s);", m1Row1),
		"DELETE FROM t WHERE id = 2;",
		"CALL DOLT_COMMIT('-am', 'm1');",
		"CALL DOLT_CHECKOUT('m2');",
		"INSERT INTO t VALUES (20, 'm2', NULL);",
		"CALL DOLT_COMMIT('-am', 'm2start');",
		"CALL DOLT_MERGE('p3', '-m', 'm2 merges p3');",
		"CALL DOLT_MERGE('p2', '-m', 'm2 merges p2');",
		"CALL DOLT_CONFLICTS_RESOLVE('--ours', 't');",
		"CALL DOLT_COMMIT('-am', 'm2 merges p2');",
		"CALL DOLT_MERGE('p1', '-m', 'm2 merges p1');",
		"CALL DOLT_CONFLICTS_RESOLVE('--ours', 't');",
		"CALL DOLT_COMMIT('-am', 'm2 merges p1');",
		fmt.Sprintf("REPLACE INTO t VALUES (1, %s);", m2Row1),
		"REPLACE INTO t VALUES (2, 'seed', 'seed');",
		"CALL DOLT_COMMIT('-am', 'm2');",
		"CALL DOLT_CHECKOUT('m1');",
	}
}

// previewParityScript previews merging |source| into the checked-out |target| after |setup|, checks that the preview
// changes nothing, then merges and checks that the merge finds what the preview did. |summary| is the expected
// dolt_preview_merge_conflicts_summary, and |conflicts| the rows of |columns| from table |table|'s conflicts. With no
// summary rows, the merge must be clean.
func previewParityScript(name string, setup []string, target, source, table, columns string, summary, conflicts []sql.Row) queries.ScriptTest {
	merge := queries.ScriptTestAssertion{
		Query:    fmt.Sprintf("CALL DOLT_MERGE('%s', '-m', 'merge %s into %s');", source, source, target),
		Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
	}
	if len(summary) == 0 {
		merge.Expected = []sql.Row{{doltCommit, 0, 0, "merge successful"}}
	}
	assertions := []queries.ScriptTestAssertion{
		{Query: "SET @head = hashof('HEAD');", SkipResultsCheck: true},
		{
			Query:    fmt.Sprintf("SELECT * FROM dolt_preview_merge_conflicts_summary('%s', '%s');", target, source),
			Expected: summary,
		},
	}
	if conflicts != nil {
		assertions = append(assertions, queries.ScriptTestAssertion{
			Query:    fmt.Sprintf("SELECT %s FROM dolt_preview_merge_conflicts('%s', '%s', '%s');", columns, target, source, table),
			Expected: conflicts,
		})
	}
	assertions = append(assertions,
		queries.ScriptTestAssertion{Query: "SELECT hashof('HEAD') = @head;", Expected: []sql.Row{{true}}},
		queries.ScriptTestAssertion{Query: "SELECT count(*) FROM dolt_status;", Expected: []sql.Row{{0}}},
		queries.ScriptTestAssertion{Query: "SELECT is_merging FROM dolt_merge_status;", Expected: []sql.Row{{false}}},
		merge,
	)
	if conflicts != nil {
		assertions = append(assertions, queries.ScriptTestAssertion{
			Query:    fmt.Sprintf("SELECT %s FROM dolt_conflicts_%s;", columns, table),
			Expected: conflicts,
		})
	}
	return queries.ScriptTest{Name: name, SetUpScript: setup, Assertions: assertions}
}

// storedGeneratedMergeBasesSetup builds a criss-cross whose merge bases, x1 and y1, conflict on a, a source of the
// stored generated column g = a + b:
//
//	c1 -- x1 -- x2 [-- x3]    x
//	  \-- y1 -- y2            y
//	x2 = merge(x1, y1), y2 = merge(y1, x1)
//
// c1 seeds row 1 with a = b = 0. x1 sets a and b to 1, and y1 sets a to 3. So x1 and y1 conflict on a, merge b to 1,
// and have different values of g. x2 and y2 resolve a to |xA| and |yA|, and keep b = 1. With |xLater|, x3 sets b to 2.
func storedGeneratedMergeBasesSetup(xA, yA int, xLater bool) []string {
	script := crissCrossSetup(
		[]string{
			"CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT, g INT AS (a + b) STORED, INDEX ib (b), INDEX ig (g));",
			"INSERT INTO t (id, a, b) VALUES (1, 0, 0);",
		},
		[]string{"UPDATE t SET a = 1, b = 1 WHERE id = 1;"},
		[]string{"UPDATE t SET a = 3 WHERE id = 1;"},
		[]string{"CALL DOLT_CONFLICTS_RESOLVE('--ours', 't');", fmt.Sprintf("UPDATE t SET a = %d, b = 1 WHERE id = 1;", yA)},
		[]string{"CALL DOLT_CONFLICTS_RESOLVE('--ours', 't');", fmt.Sprintf("UPDATE t SET a = %d, b = 1 WHERE id = 1;", xA)},
	)
	if xLater {
		script = append(script, "UPDATE t SET b = 2 WHERE id = 1;", "CALL DOLT_COMMIT('-am', 'x3');")
	}
	return script
}

// resolvedMergeScript merges |source| after |setup|, expecting conflicts, runs |resolve| to resolve them, commits, and
// checks that |query| returns |expected|. |expected| is git's result for the same history and resolution.
func resolvedMergeScript(name string, setup []string, source string, resolve []string, query string, expected []sql.Row) queries.ScriptTest {
	assertions := []queries.ScriptTestAssertion{
		{
			Query:    fmt.Sprintf("CALL DOLT_MERGE('%s', '-m', 'merge %s');", source, source),
			Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
		},
	}
	for _, statement := range resolve {
		assertions = append(assertions, queries.ScriptTestAssertion{Query: statement, SkipResultsCheck: true})
	}
	assertions = append(assertions,
		queries.ScriptTestAssertion{Query: "SELECT count(*) FROM dolt_conflicts;", Expected: []sql.Row{{0}}},
		queries.ScriptTestAssertion{Query: fmt.Sprintf("CALL DOLT_COMMIT('-am', 'resolve merge of %s');", source), SkipResultsCheck: true},
		queries.ScriptTestAssertion{Query: query, Expected: expected},
		queries.ScriptTestAssertion{Query: "SELECT count(*) FROM dolt_status;", Expected: []sql.Row{{0}}},
	)
	return queries.ScriptTest{Name: name, SetUpScript: setup, Assertions: assertions}
}

// longRunningConflictSetup is longRunningSchemaBranchSetup with a conflict, on feature, without autocommit.
func longRunningConflictSetup() []string {
	return append(longRunningSchemaBranchSetup(true), "CALL DOLT_CHECKOUT('feature');", "SET @@autocommit = 0;")
}

// AllMergeMultipleBasesScripts returns the merges with several merge bases and the resolutions of their conflicts.
func AllMergeMultipleBasesScripts() []queries.ScriptTest {
	var scripts []queries.ScriptTest
	for _, list := range [][]queries.ScriptTest{MergeMultipleBasesScripts, MergeMultipleBasesResolveOursScripts,
		MergeMultipleBasesResolveTheirsScripts, MergeMultipleBasesResolveByEditScripts, MergeMultipleBasesAbortScripts,
		MergeMultipleBasesPreviewScripts} {
		scripts = append(scripts, list...)
	}
	return scripts
}

// MergeMultipleBasesResolveOursScripts resolve each conflicting merge against a virtual merge base by keeping ours,
// as `git checkout --ours` does for the same history.
var MergeMultipleBasesResolveOursScripts = []queries.ScriptTest{
	resolvedMergeScript("long-running schema branch with a conflict: resolve with ours", longRunningConflictSetup(), "main",
		[]string{"CALL DOLT_CONFLICTS_RESOLVE('--ours', 't');"},
		"SELECT id, v, w FROM t WHERE id IN (1, 3) ORDER BY id;", []sql.Row{{1, "feature 5", "seed"}, {3, "main", "feature"}}),
	resolvedMergeScript("conflicting merge bases, resolved differently: resolve with ours", conflictingMergeBasesSetup("x wins", "y wins", false), "y",
		[]string{"CALL DOLT_CONFLICTS_RESOLVE('--ours', 't');"},
		"SELECT * FROM t ORDER BY id;", []sql.Row{{1, "x wins", "seed"}, {2, "x", "y"}}),
	resolvedMergeScript("conflicting merge bases, one side resolved to the old value: resolve with ours", conflictingMergeBasesSetup("seed", "y", false), "y",
		[]string{"CALL DOLT_CONFLICTS_RESOLVE('--ours', 't');"},
		"SELECT * FROM t ORDER BY id;", []sql.Row{{1, "seed", "seed"}, {2, "x", "y"}}),
	resolvedMergeScript("merge bases that conflict on a deleted row, resolved differently: resolve with ours", deleteAgainstEditSetup(false, true), "y",
		[]string{"CALL DOLT_CONFLICTS_RESOLVE('--ours', 't');"},
		"SELECT * FROM t ORDER BY id;", []sql.Row{{2, "x", "y"}}),
}

// MergeMultipleBasesResolveTheirsScripts resolve each conflicting merge against a virtual merge base by taking theirs,
// as `git checkout --theirs` does for the same history. Where ours deleted the row, theirs restores it.
var MergeMultipleBasesResolveTheirsScripts = []queries.ScriptTest{
	resolvedMergeScript("long-running schema branch with a conflict: resolve with theirs", longRunningConflictSetup(), "main",
		[]string{"CALL DOLT_CONFLICTS_RESOLVE('--theirs', 't');"},
		"SELECT id, v, w FROM t WHERE id IN (1, 3) ORDER BY id;", []sql.Row{{1, "main 6", "seed"}, {3, "main", "feature"}}),
	resolvedMergeScript("conflicting merge bases, resolved differently: resolve with theirs", conflictingMergeBasesSetup("x wins", "y wins", false), "y",
		[]string{"CALL DOLT_CONFLICTS_RESOLVE('--theirs', 't');"},
		"SELECT * FROM t ORDER BY id;", []sql.Row{{1, "y wins", "seed"}, {2, "x", "y"}}),
	resolvedMergeScript("conflicting merge bases, one side resolved to the old value: resolve with theirs", conflictingMergeBasesSetup("seed", "y", false), "y",
		[]string{"CALL DOLT_CONFLICTS_RESOLVE('--theirs', 't');"},
		"SELECT * FROM t ORDER BY id;", []sql.Row{{1, "y", "seed"}, {2, "x", "y"}}),
	resolvedMergeScript("merge bases that conflict on a deleted row, resolved differently: resolve with theirs", deleteAgainstEditSetup(false, true), "y",
		[]string{"CALL DOLT_CONFLICTS_RESOLVE('--theirs', 't');"},
		"SELECT * FROM t ORDER BY id;", []sql.Row{{1, "y", "seed"}, {2, "x", "y"}}),
}

// MergeMultipleBasesResolveByEditScripts resolve each conflicting merge against a virtual merge base by writing the
// row, here every column 'edited', and then deleting the conflict, as editing the file does in git.
var MergeMultipleBasesResolveByEditScripts = []queries.ScriptTest{
	resolvedMergeScript("long-running schema branch with a conflict: resolve by editing the row", longRunningConflictSetup(), "main",
		[]string{
			"REPLACE INTO t (id, v, w, x1, x2, x3, x4, x5, x6) VALUES (1, 'edited', 'edited', 'edited', 'edited', 'edited', 'edited', 'edited', 'edited');",
			"DELETE FROM dolt_conflicts_t;",
		},
		"SELECT id, v, w, x6 FROM t WHERE id IN (1, 3) ORDER BY id;", []sql.Row{{1, "edited", "edited", "edited"}, {3, "main", "feature", nil}}),
	resolvedMergeScript("conflicting merge bases, resolved differently: resolve by editing the row", conflictingMergeBasesSetup("x wins", "y wins", false), "y",
		[]string{"REPLACE INTO t VALUES (1, 'edited', 'edited');", "DELETE FROM dolt_conflicts_t;"},
		"SELECT * FROM t ORDER BY id;", []sql.Row{{1, "edited", "edited"}, {2, "x", "y"}}),
	resolvedMergeScript("conflicting merge bases, one side resolved to the old value: resolve by editing the row", conflictingMergeBasesSetup("seed", "y", false), "y",
		[]string{"REPLACE INTO t VALUES (1, 'edited', 'edited');", "DELETE FROM dolt_conflicts_t;"},
		"SELECT * FROM t ORDER BY id;", []sql.Row{{1, "edited", "edited"}, {2, "x", "y"}}),
	resolvedMergeScript("merge bases that conflict on a deleted row, resolved differently: resolve by editing the row", deleteAgainstEditSetup(false, true), "y",
		[]string{"REPLACE INTO t VALUES (1, 'edited', 'edited');", "DELETE FROM dolt_conflicts_t;"},
		"SELECT * FROM t ORDER BY id;", []sql.Row{{1, "edited", "edited"}, {2, "x", "y"}}),
}

// abortedMergeScript merges |source| after |setup|, expecting conflicts, aborts the merge, and checks that |query|
// returns |expected|, the rows before the merge, and that no merge or conflict is left, as `git merge --abort` does.
func abortedMergeScript(name string, setup []string, source string, query string, expected []sql.Row) queries.ScriptTest {
	return queries.ScriptTest{
		Name:        name,
		SetUpScript: setup,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    fmt.Sprintf("CALL DOLT_MERGE('%s', '-m', 'merge %s');", source, source),
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "CALL DOLT_MERGE('--abort');",
				Expected: []sql.Row{{"", 0, 0, "merge aborted"}},
			},
			{Query: query, Expected: expected},
			{Query: "SELECT count(*) FROM dolt_conflicts;", Expected: []sql.Row{{0}}},
			{Query: "SELECT is_merging FROM dolt_merge_status;", Expected: []sql.Row{{false}}},
			{Query: "SELECT count(*) FROM dolt_status;", Expected: []sql.Row{{0}}},
		},
	}
}

// MergeMultipleBasesAbortScripts abort each conflicting merge against a virtual merge base.
var MergeMultipleBasesAbortScripts = []queries.ScriptTest{
	abortedMergeScript("long-running schema branch with a conflict: abort", longRunningConflictSetup(), "main",
		"SELECT id, v, w FROM t WHERE id IN (1, 2, 3) OR id > 103 AND id < 200 ORDER BY id;",
		[]sql.Row{{1, "feature 5", "seed"}, {2, "seed", "seed"}, {3, "seed", "feature"}}),
	abortedMergeScript("conflicting merge bases, resolved differently: abort", conflictingMergeBasesSetup("x wins", "y wins", false), "y",
		"SELECT * FROM t ORDER BY id;", []sql.Row{{1, "x wins", "seed"}, {2, "x", "y"}}),
	abortedMergeScript("conflicting merge bases, one side resolved to the old value: abort", conflictingMergeBasesSetup("seed", "y", false), "y",
		"SELECT * FROM t ORDER BY id;", []sql.Row{{1, "seed", "seed"}, {2, "x", "y"}}),
	abortedMergeScript("merge bases that conflict on a deleted row, resolved differently: abort", deleteAgainstEditSetup(false, true), "y",
		"SELECT * FROM t ORDER BY id;", []sql.Row{{2, "x", "y"}}),
}

// MergeMultipleBasesPreviewScripts preview merges against a virtual merge base, and check that the merges find the
// same conflicts.
var MergeMultipleBasesPreviewScripts = []queries.ScriptTest{
	previewParityScript("preview: conflicting merge bases, one side resolved to the old value: merge x into y",
		append(conflictingMergeBasesSetup("seed", "y", false), "CALL DOLT_CHECKOUT('y');"), "y", "x", "t", "base_a, our_a, their_a",
		[]sql.Row{{"t", uint64(1), uint64(0)}}, []sql.Row{{"seed", "y", "seed"}}),
	previewParityScript("preview: conflicting merge bases with a virtual generated column first: merge y into x",
		conflictingMergeBasesSetupWithTable(
			"CREATE TABLE t (id INT PRIMARY KEY, g INT AS (id * 2) VIRTUAL, a VARCHAR(100), b VARCHAR(100));",
			"x wins", "y wins", false),
		"x", "y", "t", "base_a, base_b, our_a, their_a", []sql.Row{{"t", uint64(1), uint64(0)}},
		[]sql.Row{{"seed", "seed", "x wins", "y wins"}}),
	previewParityScript("preview: conflicting merge bases with a stored generated column, resolved differently: merge y into x",
		storedGeneratedMergeBasesSetup(10, 20, false), "x", "y", "t", "base_a, base_b, base_g, our_a, their_a",
		[]sql.Row{{"t", uint64(1), uint64(0)}}, []sql.Row{{0, 1, 1, 10, 20}}),
	previewParityScript("preview: conflicting merge bases, resolved the same way: merge y into x",
		conflictingMergeBasesSetup("resolved", "resolved", false), "x", "y", "t", "", nil, nil),
	previewParityScript("preview: conflicting merge bases, resolved the same way, then a type change: merge y into x",
		append(conflictingMergeBasesSetup("resolved", "resolved", false),
			"CALL DOLT_CHECKOUT('y');", "ALTER TABLE t MODIFY COLUMN a TEXT;", "CALL DOLT_COMMIT('-am', 'y3');",
			"CALL DOLT_CHECKOUT('x');"),
		"x", "y", "t", "", nil, nil),
	previewParityScript("preview: merge bases that add a row with different values, one side deleting it: merge y into x",
		addAddSetup("DELETE FROM t WHERE id = 3;", "REPLACE INTO t VALUES (3, 'y');"), "x", "y", "t",
		"base_id, our_id, their_id, their_a", []sql.Row{{"t", uint64(1), uint64(0)}}, []sql.Row{{nil, nil, 3, "y"}}),
	previewParityScript("preview: merge bases that add a row with different values, one side deleting it: merge x into y",
		append(addAddSetup("DELETE FROM t WHERE id = 3;", "REPLACE INTO t VALUES (3, 'y');"), "CALL DOLT_CHECKOUT('y');"),
		"y", "x", "t", "base_id, our_id, our_a, their_id", []sql.Row{{"t", uint64(1), uint64(0)}}, []sql.Row{{nil, 3, "y", nil}}),
	previewParityScript("preview: conflicting merge bases with different schemas: merge y into x",
		schemaChangingMergeBasesSetup(), "x", "y", "t", "base_id, base_a, our_a, their_a",
		[]sql.Row{{"t", uint64(1), uint64(0)}}, []sql.Row{{1, "seed", "x wins", "seed"}}),
	previewParityScript("preview: nested criss-cross: merge y into x",
		nestedCrissCrossSetup(), "x", "y", "t", "base_a, our_a, their_a",
		[]sql.Row{{"t", uint64(1), uint64(0)}}, []sql.Row{{"seed", "y", "q"}}),
	previewParityScript("preview: nested criss-cross: merge x into y",
		append(nestedCrissCrossSetup(), "CALL DOLT_CHECKOUT('y');"), "y", "x", "t", "base_a, our_a, their_a",
		[]sql.Row{{"t", uint64(1), uint64(0)}}, []sql.Row{{"seed", "q", "y"}}),
	previewParityScript("preview: three merge bases that conflict in different folds: merge m2 into m1",
		foldedConflictsSetup("'m1', 'both'", "'seed', 'both'"), "m1", "m2", "t", "base_id, base_a, our_a, their_a",
		[]sql.Row{{"t", uint64(1), uint64(0)}}, []sql.Row{{1, "seed", "m1", "seed"}}),
	previewParityScript("preview: conflicting merge bases, one side dropping the table: merge y into x",
		append(conflictingMergeBasesSetup("x wins", "seed", false), "DROP TABLE t;", "CALL DOLT_COMMIT('-am', 'x3');"),
		"x", "y", "t", "", []sql.Row{{"t", nil, uint64(1)}}, nil),
}

// MergeMultipleBasesScripts merge branches that have more than one merge base. The expected results are git's: each
// history was also built in git, with a directory per table, a file per row and a line per column value, and `git
// merge` (git 2.39) produced the same rows and conflicts.
var MergeMultipleBasesScripts = []queries.ScriptTest{
	{
		Name:        "schema branch: merge main into feature",
		SetUpScript: append(schemaBranchSetup(0, false), "CALL DOLT_CHECKOUT('feature');"),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('main', '-m', 'merge main into feature');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "SELECT * FROM t ORDER BY id;",
				Expected: []sql.Row{{1, "edited on main", nil, nil}, {50, "feature", nil, nil}},
			},
		},
	},
	{
		Name:        "schema branch: merge feature into main",
		SetUpScript: append(schemaBranchSetup(0, false), "CALL DOLT_CHECKOUT('main');"),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('feature', '-m', 'merge feature into main');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "SELECT * FROM t ORDER BY id;",
				Expected: []sql.Row{{1, "edited on main", nil, nil}, {50, "feature", nil, nil}},
			},
		},
	},
	{
		// The fork point c2-3 is as tall as s2.
		Name:        "schema branch, fork point as tall as the schema tip: merge main into feature",
		SetUpScript: append(schemaBranchSetup(1, false), "CALL DOLT_CHECKOUT('feature');"),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('main', '-m', 'merge main into feature');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "SELECT * FROM t ORDER BY id;",
				Expected: []sql.Row{{1, "edited on main", nil, nil}, {3, "seed", nil, nil}, {50, "feature", nil, nil}},
			},
		},
	},
	{
		// The fork point c2-4 is taller than s2.
		Name:        "schema branch, fork point taller than the schema tip: merge feature into main",
		SetUpScript: append(schemaBranchSetup(2, false), "CALL DOLT_CHECKOUT('main');"),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('feature', '-m', 'merge feature into main');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query: "SELECT * FROM t ORDER BY id;",
				Expected: []sql.Row{{1, "edited on main", nil, nil}, {3, "seed", nil, nil}, {4, "seed", nil, nil},
					{50, "feature", nil, nil}},
			},
		},
	},
	{
		// The fork point c2-4 is taller than s2 and newer, which changes the order the merge bases are merged in.
		Name:        "schema branch, schema tip older than the fork point: merge main into feature",
		SetUpScript: append(schemaBranchSetup(2, true), "CALL DOLT_CHECKOUT('feature');"),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('main', '-m', 'merge main into feature');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query: "SELECT * FROM t ORDER BY id;",
				Expected: []sql.Row{{1, "edited on main", nil, nil}, {3, "seed", nil, nil}, {4, "seed", nil, nil},
					{50, "feature", nil, nil}},
			},
		},
	},
	{
		Name:        "long-running schema branch: merge main into feature",
		SetUpScript: append(longRunningSchemaBranchSetup(false), "CALL DOLT_CHECKOUT('feature');"),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('main', '-m', 'merge main into feature');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "SELECT id, v, w FROM t ORDER BY id;",
				Expected: append([]sql.Row{{1, "main 6", "seed"}, {3, "seed", "seed"}}, longRunningRows...),
			},
			{
				Query:    "SELECT count(*) FROM information_schema.columns WHERE table_schema = database() AND table_name = 't';",
				Expected: []sql.Row{{9}},
			},
		},
	},
	{
		Name:        "long-running schema branch: merge feature into main",
		SetUpScript: append(longRunningSchemaBranchSetup(false), "CALL DOLT_CHECKOUT('main');"),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('feature', '-m', 'merge feature into main');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "SELECT id, v, w FROM t ORDER BY id;",
				Expected: append([]sql.Row{{1, "main 6", "seed"}, {3, "seed", "seed"}}, longRunningRows...),
			},
		},
	},
	{
		// Row 1 conflicts against its value when feature last merged main. Row 3 merges, column by column.
		Name:        "long-running schema branch with a conflict: merge main into feature",
		SetUpScript: append(longRunningSchemaBranchSetup(true), "CALL DOLT_CHECKOUT('feature');", "SET @@autocommit = 0;"),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('main', '-m', 'merge main into feature');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "SELECT base_id, base_v, base_w, our_v, our_w, their_v, their_w FROM dolt_conflicts_t;",
				Expected: []sql.Row{{1, "main 3", "seed", "feature 5", "seed", "main 6", "seed"}},
			},
			{
				Query:    "SELECT id, v, w FROM t WHERE id <> 1 ORDER BY id;",
				Expected: append([]sql.Row{{3, "main", "feature"}}, longRunningRows...),
			},
		},
	},
	{
		Name:        "long-running schema branch with a conflict: merge feature into main",
		SetUpScript: append(longRunningSchemaBranchSetup(true), "CALL DOLT_CHECKOUT('main');", "SET @@autocommit = 0;"),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('feature', '-m', 'merge feature into main');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "SELECT base_id, base_v, base_w, our_v, our_w, their_v, their_w FROM dolt_conflicts_t;",
				Expected: []sql.Row{{1, "main 3", "seed", "main 6", "seed", "feature 5", "seed"}},
			},
			{
				Query:    "SELECT id, v, w FROM t WHERE id <> 1 ORDER BY id;",
				Expected: append([]sql.Row{{3, "main", "feature"}}, longRunningRows...),
			},
		},
	},
	{
		// x2 and y2 resolve the conflict between the merge bases the same way.
		Name:        "conflicting merge bases, resolved the same way: merge y into x",
		SetUpScript: conflictingMergeBasesSetup("resolved", "resolved", false),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('y', '-m', 'merge y into x');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "SELECT * FROM t ORDER BY id;",
				Expected: []sql.Row{{1, "resolved", "seed"}, {2, "x", "y"}},
			},
		},
	},
	{
		// x2 and y2 resolve it differently, so row 1 conflicts again. Its base value is c1's, the base of x1 and y1.
		Name:        "conflicting merge bases, resolved differently: merge y into x",
		SetUpScript: conflictingMergeBasesSetup("x wins", "y wins", false),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('y', '-m', 'merge y into x');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "SELECT base_id, base_a, base_b, our_a, our_b, their_a, their_b FROM dolt_conflicts_t;",
				Expected: []sql.Row{{1, "seed", "seed", "x wins", "seed", "y wins", "seed"}},
			},
			{
				Query:    "SELECT * FROM t WHERE id = 2;",
				Expected: []sql.Row{{2, "x", "y"}},
			},
		},
	},
	{
		// Row 2 merged cleanly between the merge bases, so x3's later edit to it does not conflict.
		Name:        "conflicting merge bases, then an edit to a row they merged cleanly: merge y into x",
		SetUpScript: conflictingMergeBasesSetup("resolved", "resolved", true),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('y', '-m', 'merge y into x');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "SELECT * FROM t ORDER BY id;",
				Expected: []sql.Row{{1, "resolved", "seed"}, {2, "x later", "y"}},
			},
		},
	},
	{
		// x2 resolves the conflict back to c1's value. That is still a change from the conflicted merge bases, as in
		// git, so row 1 conflicts instead of taking y2's value.
		Name:        "conflicting merge bases, one side resolved to the old value: merge y into x",
		SetUpScript: conflictingMergeBasesSetup("seed", "y", false),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('y', '-m', 'merge y into x');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "SELECT base_a, our_a, their_a FROM dolt_conflicts_t;",
				Expected: []sql.Row{{"seed", "seed", "y"}},
			},
		},
	},
	{
		// The previews report the conflict that the merge finds.
		Name:        "conflicting merge bases, one side resolved to the old value: preview merging y into x",
		SetUpScript: conflictingMergeBasesSetup("seed", "y", false),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * FROM dolt_preview_merge_conflicts_summary('x', 'y');",
				Expected: []sql.Row{{"t", uint64(1), uint64(0)}},
			},
			{
				Query:    "SELECT base_a, our_a, their_a FROM dolt_preview_merge_conflicts('x', 'y', 't');",
				Expected: []sql.Row{{"seed", "seed", "y"}},
			},
		},
	},
	{
		// The merge bases p1, p2 and p3 are merged together, oldest first, including p1 and p2's conflict.
		Name:        "three merge bases: merge m2 into m1",
		SetUpScript: threeMergeBasesSetup(),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('m2', '-m', 'merge m2 into m1');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query: "SELECT * FROM t ORDER BY id;",
				Expected: []sql.Row{{1, "resolved", "seed"}, {2, "p1", "m1"}, {3, "p2", "m2"}, {4, "seed", "p3"},
					{10, "m1", nil}, {20, "m2", nil}},
			},
		},
	},
	{
		Name:        "merge bases that conflict on a deleted row, both deleting it: merge y into x",
		SetUpScript: deleteAgainstEditSetup(false, false),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('y', '-m', 'merge y into x');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "SELECT * FROM t ORDER BY id;",
				Expected: []sql.Row{{2, "x", "y"}},
			},
		},
	},
	{
		Name:        "merge bases that conflict on a deleted row, both keeping it: merge y into x",
		SetUpScript: deleteAgainstEditSetup(true, true),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('y', '-m', 'merge y into x');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "SELECT * FROM t ORDER BY id;",
				Expected: []sql.Row{{1, "y", "seed"}, {2, "x", "y"}},
			},
		},
	},
	{
		// x2 deleted row 1 and y2 kept it, so row 1 conflicts, as in git.
		Name:        "merge bases that conflict on a deleted row, resolved differently: merge y into x",
		SetUpScript: deleteAgainstEditSetup(false, true),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('y', '-m', 'merge y into x');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "SELECT base_id, base_a, our_id, their_id, their_a FROM dolt_conflicts_t;",
				Expected: []sql.Row{{1, "seed", nil, 1, "y"}},
			},
		},
	},
	{
		// As "then an edit to a row they merged cleanly", with a secondary index on b that must match the rows.
		Name: "conflicting merge bases with a secondary index: merge y into x",
		SetUpScript: conflictingMergeBasesSetupWithTable(
			"CREATE TABLE t (id INT PRIMARY KEY, a VARCHAR(100), b VARCHAR(100), INDEX idx_b (b));", "resolved", "resolved", true),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('y', '-m', 'merge y into x');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "SELECT * FROM t ORDER BY id;",
				Expected: []sql.Row{{1, "resolved", "seed"}, {2, "x later", "y"}},
			},
			{
				Query:    "SELECT id FROM t FORCE INDEX (idx_b) WHERE b = 'y';",
				Expected: []sql.Row{{2}},
			},
			{
				Query:    "SELECT id FROM t FORCE INDEX (idx_b) WHERE b = 'seed';",
				Expected: []sql.Row{{1}},
			},
		},
	},
	{
		// x3 drops t, and y2 resolved the conflict between the merge bases back to c1's value, so y2's t is the
		// virtual merge base's. y2 still changed row 1's a from the conflicted merge bases, as in git, so dropping t
		// conflicts. git conflicts on row 1's file; Dolt conflicts on the whole table.
		Name: "conflicting merge bases, one side dropping the table: merge y into x",
		SetUpScript: append(conflictingMergeBasesSetup("x wins", "seed", false),
			"DROP TABLE t;",
			"CALL DOLT_COMMIT('-am', 'x3');"),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('y', '-m', 'merge y into x');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "SELECT table_name, our_schema, description FROM dolt_schema_conflicts;",
				Expected: []sql.Row{{"t", "<deleted>", "cannot merge a table deletion with data modification"}},
			},
			{
				Query:    "SELECT table_name, status FROM dolt_status;",
				Expected: []sql.Row{{"t", "schema conflict"}},
			},
		},
	},
	{
		// The merge bases x1 and y1 have different schemas. They still merge row by row: row 3 is in the virtual merge
		// base, so y2's delete of it wins, and row 1's a conflicted between them, so x2's and y2's resolutions
		// conflict.
		Name:        "conflicting merge bases with different schemas: merge y into x",
		SetUpScript: schemaChangingMergeBasesSetup(),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('y', '-m', 'merge y into x');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "SELECT base_id, base_a, our_a, their_a FROM dolt_conflicts_t;",
				Expected: []sql.Row{{1, "seed", "x wins", "seed"}},
			},
			{
				Query:    "SELECT * FROM t ORDER BY id;",
				Expected: []sql.Row{{1, "x wins", "seed", nil}, {2, "seed", "seed", nil}},
			},
		},
	},
	{
		// Row 1's a conflicted between x1 and y1, so it conflicts between x2 and y2 even though x2 holds c1's value.
		// It is marked in their virtual merge base too, so x3's and y3's resolutions conflict.
		Name:        "nested criss-cross: merge y into x",
		SetUpScript: nestedCrissCrossSetup(),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('y', '-m', 'merge y into x');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "SELECT base_a, our_a, their_a FROM dolt_conflicts_t;",
				Expected: []sql.Row{{"seed", "y", "q"}},
			},
		},
	},
	{
		// x2 and y2 resolve the conflict between the merge bases to the same value, and y3 changes the column's type
		// without changing the value. The values are equal in the merged type, so they do not conflict.
		Name: "conflicting merge bases, resolved the same way, then a type change: merge y into x",
		SetUpScript: append(conflictingMergeBasesSetup("resolved", "resolved", false),
			"CALL DOLT_CHECKOUT('y');",
			"ALTER TABLE t MODIFY COLUMN a TEXT;",
			"CALL DOLT_COMMIT('-am', 'y3');",
			"CALL DOLT_CHECKOUT('x');"),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('y', '-m', 'merge y into x');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "SELECT * FROM t ORDER BY id;",
				Expected: []sql.Row{{1, "resolved", "seed"}, {2, "x", "y"}},
			},
		},
	},
	{
		// The virtual merge base merges x1's and y1's JSON documents to {"x": 1, "y": 1} whatever
		// @@dolt_dont_merge_json is, so that it can be rebuilt the same way in any session. Only x3 changed the
		// document from it.
		Name:        "merge bases with merged JSON documents, with @@dolt_dont_merge_json: merge y into x",
		SetUpScript: append(jsonMergeBasesSetup(), "SET @@dolt_dont_merge_json = 1;"),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('y', '-m', 'merge y into x');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "SELECT j FROM t;",
				Expected: []sql.Row{{types.MustJSON(`{"x": 2, "y": 1}`)}},
			},
		},
	},
	{
		// A virtual generated column comes before a, so a's stored position differs from its position among all
		// columns.
		Name: "conflicting merge bases with a virtual generated column first, resolved differently: merge y into x",
		SetUpScript: conflictingMergeBasesSetupWithTable(
			"CREATE TABLE t (id INT PRIMARY KEY, g INT AS (id * 2) VIRTUAL, a VARCHAR(100), b VARCHAR(100));",
			"x wins", "y wins", false),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('y', '-m', 'merge y into x');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "SELECT base_a, base_b, our_a, our_b, their_a, their_b FROM dolt_conflicts_t;",
				Expected: []sql.Row{{"seed", "seed", "x wins", "seed", "y wins", "seed"}},
			},
			{
				Query:    "SELECT * FROM t WHERE id = 2;",
				Expected: []sql.Row{{2, 4, "x", "y"}},
			},
		},
	},
	{
		Name: "conflicting merge bases with a virtual generated column first, resolved the same way: merge y into x",
		SetUpScript: conflictingMergeBasesSetupWithTable(
			"CREATE TABLE t (id INT PRIMARY KEY, g INT AS (id * 2) VIRTUAL, a VARCHAR(100), b VARCHAR(100));",
			"resolved", "resolved", true),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('y', '-m', 'merge y into x');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "SELECT * FROM t ORDER BY id;",
				Expected: []sql.Row{{1, 2, "resolved", "seed"}, {2, 4, "x later", "y"}},
			},
		},
	},
	{
		// s is generated from a and b. It changes with a in x1 and y1, but a generated value is computed, not merged,
		// so x3's later edit to b does not conflict with y2.
		Name: "conflicting merge bases with a stored generated column, then an edit to its source: merge y into x",
		SetUpScript: append(conflictingMergeBasesSetupWithTable(
			"CREATE TABLE t (id INT PRIMARY KEY, a VARCHAR(100), b VARCHAR(100), s VARCHAR(201) AS (CONCAT(a, '-', b)) STORED);",
			"resolved", "resolved", false),
			"UPDATE t SET b = 'x later' WHERE id = 1;",
			"CALL DOLT_COMMIT('-am', 'x3');"),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('y', '-m', 'merge y into x');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "SELECT * FROM t ORDER BY id;",
				Expected: []sql.Row{{1, "resolved", "x later", "resolved-x later"}, {2, "x", "y", "x-y"}},
			},
		},
	},
	{
		// y3 moves a after b, so a is at a different position in each side's schema.
		Name: "conflicting merge bases, resolved the same way, then a column move: merge y into x",
		SetUpScript: append(conflictingMergeBasesSetup("resolved", "resolved", false),
			"CALL DOLT_CHECKOUT('y');",
			"ALTER TABLE t MODIFY COLUMN a VARCHAR(100) AFTER b;",
			"CALL DOLT_COMMIT('-am', 'y3');",
			"CALL DOLT_CHECKOUT('x');"),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('y', '-m', 'merge y into x');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "SELECT id, a, b FROM t ORDER BY id;",
				Expected: []sql.Row{{1, "resolved", "seed"}, {2, "x", "y"}},
			},
		},
	},
	{
		// As "one side resolved to the old value", with a type change on y. x2 holds c1's value, but it still
		// conflicts with y2's.
		Name: "conflicting merge bases, one side resolved to the old value, then a type change: merge y into x",
		SetUpScript: append(conflictingMergeBasesSetup("seed", "y", false),
			"CALL DOLT_CHECKOUT('y');",
			"ALTER TABLE t MODIFY COLUMN a TEXT;",
			"CALL DOLT_COMMIT('-am', 'y3');",
			"CALL DOLT_CHECKOUT('x');"),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('y', '-m', 'merge y into x');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "SELECT base_a, our_a, their_a FROM dolt_conflicts_t;",
				Expected: []sql.Row{{"seed", "seed", "y"}},
			},
		},
	},
	{
		// y3 drops a, whose value conflicted between the merge bases, after x2 and y2 resolved it differently. As with
		// a single merge base, dropping a column that the other side changed conflicts on the row.
		Name: "conflicting merge bases, resolved differently, then a drop of the conflicted column: merge y into x",
		SetUpScript: append(conflictingMergeBasesSetup("x wins", "y wins", false),
			"CALL DOLT_CHECKOUT('y');",
			"ALTER TABLE t DROP COLUMN a;",
			"CALL DOLT_COMMIT('-am', 'y3');",
			"CALL DOLT_CHECKOUT('x');"),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('y', '-m', 'merge y into x');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "SELECT table_name, description FROM dolt_schema_conflicts;",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT base_a, base_b, our_b, our_diff_type, their_b, their_diff_type FROM dolt_conflicts_t;",
				Expected: []sql.Row{{"seed", "seed", "seed", "modified", "seed", "modified"}},
			},
		},
	},
	{
		// x1 and y1 each give a different row the unique value 'u', so merging them violates the unique key on a. The
		// virtual merge base keeps both rows with 'u', without the violation, and its index on a must still build.
		// x2 and y2 each change a different row away from 'u', so the merge is clean and valid.
		Name: "merge bases that violate a unique key together: merge y into x",
		SetUpScript: crissCrossSetup(
			[]string{
				"CREATE TABLE t (id INT PRIMARY KEY, a VARCHAR(100), UNIQUE KEY ua (a));",
				"INSERT INTO t VALUES (1, 'seed 1'), (2, 'seed 2');",
			},
			[]string{"UPDATE t SET a = 'u' WHERE id = 1;"},
			[]string{"UPDATE t SET a = 'u' WHERE id = 2;"},
			[]string{"UPDATE t SET a = 'y2' WHERE id = 2;", "DELETE FROM dolt_constraint_violations_t;"},
			[]string{"UPDATE t SET a = 'x2' WHERE id = 1;", "DELETE FROM dolt_constraint_violations_t;"},
		),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('y', '-m', 'merge y into x');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "SELECT * FROM t ORDER BY id;",
				Expected: []sql.Row{{1, "x2"}, {2, "y2"}},
			},
			{
				Query:    "SELECT id FROM t FORCE INDEX (ua) WHERE a = 'y2';",
				Expected: []sql.Row{{2}},
			},
		},
	},
	{
		// As above, but x2 and y2 each move the other row away from 'u'. Each takes the other's change, so the merged
		// rows hold neither 'u'.
		Name: "merge bases that violate a unique key together, each side moving the other row: merge y into x",
		SetUpScript: crissCrossSetup(
			[]string{
				"CREATE TABLE t (id INT PRIMARY KEY, a VARCHAR(100), UNIQUE KEY ua (a));",
				"INSERT INTO t VALUES (1, 'seed 1'), (2, 'seed 2');",
			},
			[]string{"UPDATE t SET a = 'u' WHERE id = 1;"},
			[]string{"UPDATE t SET a = 'u' WHERE id = 2;"},
			[]string{"UPDATE t SET a = 'y2' WHERE id = 1;", "DELETE FROM dolt_constraint_violations_t;"},
			[]string{"UPDATE t SET a = 'x2' WHERE id = 2;", "DELETE FROM dolt_constraint_violations_t;"},
		),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('y', '-m', 'merge y into x');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "SELECT * FROM t ORDER BY id;",
				Expected: []sql.Row{{1, "y2"}, {2, "x2"}},
			},
		},
	},
	{
		// Merging x1 and y1 violates the check on row 1. The virtual merge base keeps the merged row; x2 and y2 each
		// fix it, the same way, so the merge is clean.
		Name: "merge bases that violate a check constraint together: merge y into x",
		SetUpScript: crissCrossSetup(
			[]string{
				"CREATE TABLE t (id INT PRIMARY KEY, a VARCHAR(100), b VARCHAR(100), CHECK (a <> b));",
				"INSERT INTO t VALUES (1, 'seed a', 'seed b');",
			},
			[]string{"UPDATE t SET a = 'z' WHERE id = 1;"},
			[]string{"UPDATE t SET b = 'z' WHERE id = 1;"},
			[]string{"UPDATE t SET b = 'fixed' WHERE id = 1;", "DELETE FROM dolt_constraint_violations_t;"},
			[]string{"UPDATE t SET b = 'fixed' WHERE id = 1;", "DELETE FROM dolt_constraint_violations_t;"},
		),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('y', '-m', 'merge y into x');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "SELECT * FROM t ORDER BY id;",
				Expected: []sql.Row{{1, "z", "fixed"}},
			},
		},
	},
	{
		// x1 deletes parent 2 and y1 adds a child of it, so merging them violates the foreign key. y2 drops the child
		// and x2 restores the parent. Against the virtual merge base, which has the child but not the parent, y2
		// deleted the child and x2 added the parent, so the merge is clean and valid.
		Name: "merge bases that violate a foreign key together: merge y into x",
		SetUpScript: crissCrossSetup(
			[]string{
				"CREATE TABLE parent (id INT PRIMARY KEY);",
				"CREATE TABLE child (id INT PRIMARY KEY, pid INT, FOREIGN KEY (pid) REFERENCES parent (id));",
				"INSERT INTO parent VALUES (1), (2);",
			},
			[]string{"DELETE FROM parent WHERE id = 2;"},
			[]string{"INSERT INTO child VALUES (10, 2);"},
			[]string{"DELETE FROM dolt_constraint_violations_child;", "DELETE FROM child WHERE id = 10;"},
			[]string{"DELETE FROM dolt_constraint_violations_child;", "INSERT INTO parent VALUES (2);"},
		),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('y', '-m', 'merge y into x');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "SELECT * FROM parent ORDER BY id;",
				Expected: []sql.Row{{1}, {2}},
			},
			{
				Query:    "SELECT * FROM child;",
				Expected: []sql.Row{},
			},
		},
	},
	{
		// x1 and y1 conflict on row 1's b, and also violate the unique key on a together. The virtual merge base keeps
		// row 1's clean a, 'u', next to row 2's 'u', and c1's b. x2 and y2 keep their own row 1, so b conflicts again,
		// and y2's row 1 has a = 'seed 1', a change from the virtual merge base. Both move row 2 to 'fixed'.
		Name: "merge bases with a conflict and a unique key violation: merge y into x",
		SetUpScript: crissCrossSetup(
			[]string{
				"CREATE TABLE t (id INT PRIMARY KEY, a VARCHAR(100), b VARCHAR(100), UNIQUE KEY ua (a));",
				"INSERT INTO t VALUES (1, 'seed 1', 'seed'), (2, 'seed 2', 'seed');",
			},
			[]string{"UPDATE t SET a = 'u', b = 'x' WHERE id = 1;"},
			[]string{"UPDATE t SET a = 'u' WHERE id = 2;", "UPDATE t SET b = 'y' WHERE id = 1;"},
			[]string{"CALL DOLT_CONFLICTS_RESOLVE('--ours', 't');", "UPDATE t SET a = 'fixed' WHERE id = 2;",
				"DELETE FROM dolt_constraint_violations_t;"},
			[]string{"CALL DOLT_CONFLICTS_RESOLVE('--ours', 't');", "UPDATE t SET a = 'fixed' WHERE id = 2;",
				"DELETE FROM dolt_constraint_violations_t;"},
		),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('y', '-m', 'merge y into x');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "SELECT base_id, base_a, base_b, our_a, our_b, their_a, their_b FROM dolt_conflicts_t;",
				Expected: []sql.Row{{1, "u", "seed", "u", "x", "seed 1", "y"}},
			},
			{
				Query:    "SELECT * FROM t WHERE id = 2;",
				Expected: []sql.Row{{2, "fixed", "seed"}},
			},
			{
				Query:    "SELECT count(*) FROM dolt_constraint_violations_t;",
				Expected: []sql.Row{{0}},
			},
		},
	},
	{
		// b and c merged cleanly between the merge bases, so the virtual merge base keeps them, and x3's later edit
		// to b does not conflict, although a and d in the same row conflicted.
		Name:        "merge bases that conflict on two cells of a row, then an edit to a clean cell: merge y into x",
		SetUpScript: conflictedRowWithCleanCellsSetup("NULL", "NULL", true),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('y', '-m', 'merge y into x');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "SELECT * FROM t;",
				Expected: []sql.Row{{1, "resolved", "x later", "y", nil}},
			},
		},
	},
	{
		Name: "merge bases that conflict on two cells of a row, then an edit to a clean cell: merge x into y",
		SetUpScript: append(conflictedRowWithCleanCellsSetup("NULL", "NULL", true),
			"CALL DOLT_CHECKOUT('y');"),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('x', '-m', 'merge x into y');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "SELECT * FROM t;",
				Expected: []sql.Row{{1, "resolved", "x later", "y", nil}},
			},
		},
	},
	{
		// x2 and y2 agree on a but not on d, the second conflicted cell, whose base is NULL. y2 keeps NULL, which is
		// still a change from the conflicted merge bases, so d conflicts.
		Name:        "merge bases that conflict on two cells of a row, resolved differently in the second: merge y into x",
		SetUpScript: conflictedRowWithCleanCellsSetup("'x wins'", "NULL", false),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('y', '-m', 'merge y into x');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "SELECT base_a, base_b, base_c, base_d, our_a, our_b, our_c, our_d, their_a, their_b, their_c, their_d FROM dolt_conflicts_t;",
				Expected: []sql.Row{{"seed", "x", "y", nil, "resolved", "x", "y", "x wins", "resolved", "x", "y", nil}},
			},
		},
	},
	{
		Name: "merge bases that conflict on two cells of a row, resolved differently in the second: merge x into y",
		SetUpScript: append(conflictedRowWithCleanCellsSetup("'x wins'", "NULL", false),
			"CALL DOLT_CHECKOUT('y');"),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('x', '-m', 'merge x into y');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "SELECT base_a, base_b, base_c, base_d, our_a, our_b, our_c, our_d, their_a, their_b, their_c, their_d FROM dolt_conflicts_t;",
				Expected: []sql.Row{{"seed", "x", "y", nil, "resolved", "x", "y", nil, "resolved", "x", "y", "x wins"}},
			},
		},
	},
	{
		// x2 and y2 agree on row 3, so the merge takes it.
		Name:        "merge bases that add a row with different values, resolved the same way: merge y into x",
		SetUpScript: addAddSetup("REPLACE INTO t VALUES (3, 'same');", "REPLACE INTO t VALUES (3, 'same');"),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('y', '-m', 'merge y into x');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "SELECT * FROM t ORDER BY id;",
				Expected: []sql.Row{{1, "seed"}, {3, "same"}},
			},
		},
	},
	{
		// Row 3 has no base: base_id is NULL, unlike a base row with NULL values.
		Name:        "merge bases that add a row with different values, resolved differently: merge y into x",
		SetUpScript: addAddSetup("REPLACE INTO t VALUES (3, 'x');", "REPLACE INTO t VALUES (3, 'y');"),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('y', '-m', 'merge y into x');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "SELECT base_id, base_a, our_id, our_a, their_id, their_a FROM dolt_conflicts_t;",
				Expected: []sql.Row{{nil, nil, 3, "x", 3, "y"}},
			},
		},
	},
	{
		Name:        "merge bases that add a row with different values, resolved differently: merge x into y",
		SetUpScript: append(addAddSetup("REPLACE INTO t VALUES (3, 'x');", "REPLACE INTO t VALUES (3, 'y');"), "CALL DOLT_CHECKOUT('y');"),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('x', '-m', 'merge x into y');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "SELECT base_id, base_a, our_id, our_a, their_id, their_a FROM dolt_conflicts_t;",
				Expected: []sql.Row{{nil, nil, 3, "y", 3, "x"}},
			},
		},
	},
	{
		// x2 deletes row 3 and y2 keeps it. The virtual merge base has no row 3, but y2's row is still a change from the\n\t\t// conflicted merge bases, as in git, so it conflicts with the delete instead of being added.
		Name:        "merge bases that add a row with different values, one side deleting it: merge y into x",
		SetUpScript: addAddSetup("DELETE FROM t WHERE id = 3;", "REPLACE INTO t VALUES (3, 'y');"),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('y', '-m', 'merge y into x');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "SELECT base_id, base_a, our_id, our_a, their_id, their_a FROM dolt_conflicts_t;",
				Expected: []sql.Row{{nil, nil, nil, nil, 3, "y"}},
			},
		},
	},
	{
		Name:        "merge bases that add a row with different values, one side deleting it: merge x into y",
		SetUpScript: append(addAddSetup("DELETE FROM t WHERE id = 3;", "REPLACE INTO t VALUES (3, 'y');"), "CALL DOLT_CHECKOUT('y');"),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('x', '-m', 'merge x into y');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "SELECT base_id, base_a, our_id, our_a, their_id, their_a FROM dolt_conflicts_t;",
				Expected: []sql.Row{{nil, nil, 3, "y", nil, nil}},
			},
		},
	},
	{
		Name:        "merge bases that add a row with different values, both deleting it: merge y into x",
		SetUpScript: addAddSetup("DELETE FROM t WHERE id = 3;", "DELETE FROM t WHERE id = 3;"),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('y', '-m', 'merge y into x');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "SELECT * FROM t ORDER BY id;",
				Expected: []sql.Row{{1, "seed"}},
			},
		},
	},
	{
		// x1 and y1 each create table u, which c1 lacks, with different values in row 1 and the same row 2. The virtual
		// merge base has u with row 2 but without row 1. x2 and y2 resolve row 1 differently, and y2 deletes row 2,
		// which stays deleted.
		Name: "merge bases that create a table with different rows, resolved differently: merge y into x",
		SetUpScript: crissCrossSetup(
			[]string{"CREATE TABLE t (id INT PRIMARY KEY);"},
			[]string{"CREATE TABLE u (id INT PRIMARY KEY, a VARCHAR(100));", "INSERT INTO u VALUES (1, 'x'), (2, 'same');",
				"CALL DOLT_ADD('u');"},
			[]string{"CREATE TABLE u (id INT PRIMARY KEY, a VARCHAR(100));", "INSERT INTO u VALUES (1, 'y'), (2, 'same');",
				"CALL DOLT_ADD('u');"},
			[]string{"CALL DOLT_CONFLICTS_RESOLVE('--ours', 'u');", "UPDATE u SET a = 'y2' WHERE id = 1;",
				"DELETE FROM u WHERE id = 2;"},
			[]string{"CALL DOLT_CONFLICTS_RESOLVE('--ours', 'u');", "UPDATE u SET a = 'x2' WHERE id = 1;"},
		),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('y', '-m', 'merge y into x');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "SELECT base_id, base_a, our_id, our_a, their_id, their_a FROM dolt_conflicts_u;",
				Expected: []sql.Row{{nil, nil, 1, "x2", 1, "y2"}},
			},
			{
				Query:    "SELECT * FROM u ORDER BY id;",
				Expected: []sql.Row{{1, "x2"}},
			},
		},
	},
	{
		// m1 and m2 disagree on row 1's a, which conflicted between p1 and p2, and agree on its b. m2 holds c1's a, the
		// virtual merge base's value, which is still a change from the conflicted merge bases. m2 also holds c1's row 2,
		// which is no change from the virtual merge base, so m1's delete wins, as in git. Row 3, which the merge bases
		// merged cleanly, and the rows m1start and m2start added, merge cleanly.
		Name:        "three merge bases that conflict in different folds, tips disagreeing on the first conflicted cell: merge m2 into m1",
		SetUpScript: foldedConflictsSetup("'m1', 'both'", "'seed', 'both'"),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('m2', '-m', 'merge m2 into m1');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "SELECT base_id, base_a, base_b, our_id, our_a, our_b, their_id, their_a, their_b FROM dolt_conflicts_t ORDER BY base_id;",
				Expected: []sql.Row{{1, "seed", "seed", 1, "m1", "both", 1, "seed", "both"}},
			},
			{
				Query:    "SELECT * FROM t WHERE id <> 1 ORDER BY id;",
				Expected: []sql.Row{{3, "seed", "p2"}, {10, "m1", nil}, {20, "m2", nil}},
			},
		},
	},
	{
		// As above, with m1 and m2 disagreeing on row 1's b, which conflicted between p2 and p3, instead of its a.
		Name:        "three merge bases that conflict in different folds, tips disagreeing on the second conflicted cell: merge m2 into m1",
		SetUpScript: foldedConflictsSetup("'both', 'seed'", "'both', 'm2'"),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('m2', '-m', 'merge m2 into m1');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "SELECT base_id, base_a, base_b, our_id, our_a, our_b, their_id, their_a, their_b FROM dolt_conflicts_t ORDER BY base_id;",
				Expected: []sql.Row{{1, "seed", "seed", 1, "both", "seed", 1, "both", "m2"}},
			},
		},
	},
	{
		// g differs between x1 and y1 because a does, but a generated value is computed, not merged, so only a
		// conflicts. x2 and y2 resolve a the same way, and x3's change to b, and so to g, merges cleanly.
		Name:        "conflicting merge bases with a stored generated column, then a change to another source: merge y into x",
		SetUpScript: storedGeneratedMergeBasesSetup(10, 10, true),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('y', '-m', 'merge y into x');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "SELECT * FROM t;",
				Expected: []sql.Row{{1, 10, 2, 12}},
			},
			{
				Query:    "SELECT id FROM t FORCE INDEX (ig) WHERE g = 12;",
				Expected: []sql.Row{{1}},
			},
		},
	},
	{
		Name: "conflicting merge bases with a stored generated column, then a change to another source: merge x into y",
		SetUpScript: append(storedGeneratedMergeBasesSetup(10, 10, true),
			"CALL DOLT_CHECKOUT('y');"),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('x', '-m', 'merge x into y');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "SELECT * FROM t;",
				Expected: []sql.Row{{1, 10, 2, 12}},
			},
		},
	},
	{
		// x2 and y2 resolve a differently, so a conflicts. The virtual merge base has c1's a, x1's b, and g computed from
		// them.
		Name:        "conflicting merge bases with a stored generated column, resolved differently: merge y into x",
		SetUpScript: storedGeneratedMergeBasesSetup(10, 20, false),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('y', '-m', 'merge y into x');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "SELECT base_a, base_b, base_g, our_a, our_g, their_a, their_g FROM dolt_conflicts_t;",
				Expected: []sql.Row{{0, 1, 1, 10, 11, 20, 21}},
			},
		},
	},
}
