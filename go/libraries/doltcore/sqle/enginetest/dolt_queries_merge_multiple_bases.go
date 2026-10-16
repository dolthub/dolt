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
}
