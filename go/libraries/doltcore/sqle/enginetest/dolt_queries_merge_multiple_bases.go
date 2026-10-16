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
		"INSERT INTO t VALUES (1, 'seed', 'seed'), (2, 'seed', 'seed');",
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
}
