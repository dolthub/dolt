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

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dtablefunctions"
)

// schemaBranchCrissCrossSetup builds the history from https://github.com/dolthub/dolt/issues/12050: a data-free
// |schema| branch is merged into both |main| and |feature|, giving them two best common ancestors: the feature's fork
// point and the |schema| tip. |extraSeedCommits| adds commits to main before the fork, which controls whether the fork
// point is shorter than (0), as tall as (1), or taller than (2) the |schema| tip.
func schemaBranchCrissCrossSetup(extraSeedCommits int) []string {
	script := []string{
		"CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(20));",
		"CALL DOLT_COMMIT('-Am', 'c1: create table');",
		"CALL DOLT_BRANCH('schema');",
		"INSERT INTO t VALUES (1, 'seed'), (2, 'seed');",
		"CALL DOLT_COMMIT('-am', 'c2: seed rows on main');",
	}
	for i := 0; i < extraSeedCommits; i++ {
		script = append(script,
			fmt.Sprintf("INSERT INTO t VALUES (%d, 'seed');", i+3),
			fmt.Sprintf("CALL DOLT_COMMIT('-am', 'c2-%d: more seed rows');", i+3))
	}
	return append(script,
		"CALL DOLT_BRANCH('feature');",
		"UPDATE t SET v = 'edited on main' WHERE id = 1;",
		"DELETE FROM t WHERE id = 2;",
		"CALL DOLT_COMMIT('-am', 'c3: main edits row 1 and deletes row 2');",
		"CALL DOLT_CHECKOUT('feature');",
		"INSERT INTO t VALUES (50, 'feature');",
		"CALL DOLT_COMMIT('-am', 'f1: feature adds row 50');",
		"CALL DOLT_CHECKOUT('schema');",
		"ALTER TABLE t ADD COLUMN n INT NULL;",
		"CALL DOLT_COMMIT('-am', 's1: add column n');",
		"ALTER TABLE t ADD COLUMN m INT NULL;",
		"CALL DOLT_COMMIT('-am', 's2: add column m');",
		"CALL DOLT_CHECKOUT('main');",
		"CALL DOLT_MERGE('schema', '-m', 'merge schema into main');",
		"CALL DOLT_CHECKOUT('feature');",
		"CALL DOLT_MERGE('schema', '-m', 'merge schema into feature');",
		"CALL DOLT_CHECKOUT('main');",
	)
}

// schemaBranchMigrationCycle adds a column on |schema|, merges it into |main| and |feature|, and changes data on both.
func schemaBranchMigrationCycle(cycle int) []string {
	return []string{
		"CALL DOLT_CHECKOUT('schema');",
		fmt.Sprintf("ALTER TABLE t ADD COLUMN x%d INT NULL;", cycle),
		fmt.Sprintf("CALL DOLT_COMMIT('-am', 's: add column x%d');", cycle),
		"CALL DOLT_CHECKOUT('main');",
		fmt.Sprintf("INSERT INTO t (id, v) VALUES (%d, 'main');", 100+cycle),
		fmt.Sprintf("UPDATE t SET v = 'edited on main %d' WHERE id = 1;", cycle),
		fmt.Sprintf("CALL DOLT_COMMIT('-am', 'main data %d');", cycle),
		"CALL DOLT_MERGE('schema', '-m', 'merge schema into main');",
		"CALL DOLT_CHECKOUT('feature');",
		fmt.Sprintf("INSERT INTO t (id, v) VALUES (%d, 'feature');", 200+cycle),
		fmt.Sprintf("CALL DOLT_COMMIT('-am', 'feature data %d');", cycle),
		"CALL DOLT_MERGE('schema', '-m', 'merge schema into feature');",
	}
}

func longRunningFeatureSetup() []string {
	script := schemaBranchCrissCrossSetup(0)
	for cycle := 1; cycle <= 3; cycle++ {
		script = append(script, schemaBranchMigrationCycle(cycle)...)
	}
	script = append(script, "CALL DOLT_CHECKOUT('feature');", "CALL DOLT_MERGE('main', '-m', 'first sync with main');")
	for cycle := 4; cycle <= 6; cycle++ {
		script = append(script, schemaBranchMigrationCycle(cycle)...)
	}
	return append(script, "CALL DOLT_CHECKOUT('main');")
}

// conflictingMergeBasesSetup builds a criss-cross whose two merge bases, x1 and y1, both changed row 1. x2 merges y1
// into x1 keeping x1's value; y2 merges x1 into y1 keeping |y2Resolution|'s value.
func conflictingMergeBasesSetup(y2Resolution string) []string {
	return []string{
		"SET @@autocommit = 0;",
		"CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(20));",
		"INSERT INTO t VALUES (1, 'orig'), (2, 'orig');",
		"CALL DOLT_COMMIT('-Am', 'c1');",
		"CALL DOLT_BRANCH('x');",
		"CALL DOLT_BRANCH('y');",
		"CALL DOLT_CHECKOUT('x');",
		"UPDATE t SET v = 'x' WHERE id = 1;",
		"CALL DOLT_COMMIT('-am', 'x1');",
		"CALL DOLT_CHECKOUT('y');",
		"UPDATE t SET v = 'y' WHERE id = 1;",
		"CALL DOLT_COMMIT('-am', 'y1');",
		"CALL DOLT_CHECKOUT('x');",
		"CALL DOLT_MERGE('y');",
		"CALL DOLT_CONFLICTS_RESOLVE('--ours', 't');",
		"UPDATE t SET v = 'x2' WHERE id = 2;",
		"CALL DOLT_COMMIT('-am', 'x2');",
		"CALL DOLT_CHECKOUT('y');",
		"CALL DOLT_MERGE('x~1');",
		"CALL DOLT_CONFLICTS_RESOLVE('" + y2Resolution + "', 't');",
		"CALL DOLT_COMMIT('-am', 'y2');",
		"CALL DOLT_CHECKOUT('x');",
	}
}

var mergeMainIntoFeatureIsClean = []queries.ScriptTestAssertion{
	{
		Query:    "CALL DOLT_CHECKOUT('feature');",
		Expected: []sql.Row{{0, "Switched to branch 'feature'"}},
	},
	{
		Query:    "CALL DOLT_MERGE('main', '-m', 'merge main into feature');",
		Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
	},
}

// CrissCrossMergeScripts expect the results of git's default merge strategy, which merges several best common
// ancestors into a virtual merge base. Expected rows match git 2.39 run on an equivalent history.
var CrissCrossMergeScripts = []queries.ScriptTest{
	{
		Name:        "criss-cross merge: schema tip taller than the fork point",
		SetUpScript: schemaBranchCrissCrossSetup(0),
		Assertions: append(mergeMainIntoFeatureIsClean,
			queries.ScriptTestAssertion{
				Query:    "SELECT id, v, n, m FROM t ORDER BY id;",
				Expected: []sql.Row{{1, "edited on main", nil, nil}, {50, "feature", nil, nil}},
			},
		),
	},
	{
		Name:        "criss-cross merge: schema tip taller than the fork point, feature into main",
		SetUpScript: schemaBranchCrissCrossSetup(0),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('feature', '-m', 'merge feature into main');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "SELECT id, v FROM t ORDER BY id;",
				Expected: []sql.Row{{1, "edited on main"}, {50, "feature"}},
			},
		},
	},
	{
		Name:        "criss-cross merge: fork point as tall as the schema tip",
		SetUpScript: schemaBranchCrissCrossSetup(1),
		Assertions: append(mergeMainIntoFeatureIsClean,
			queries.ScriptTestAssertion{
				Query:    "SELECT id, v FROM t ORDER BY id;",
				Expected: []sql.Row{{1, "edited on main"}, {3, "seed"}, {50, "feature"}},
			},
		),
	},
	{
		Name:        "criss-cross merge: fork point taller than the schema tip",
		SetUpScript: schemaBranchCrissCrossSetup(2),
		Assertions: append(mergeMainIntoFeatureIsClean,
			queries.ScriptTestAssertion{
				Query:    "SELECT id, v FROM t ORDER BY id;",
				Expected: []sql.Row{{1, "edited on main"}, {3, "seed"}, {4, "seed"}, {50, "feature"}},
			},
		),
	},
	{
		Name:        "criss-cross merge: long-running feature across many schema migrations",
		SetUpScript: longRunningFeatureSetup(),
		Assertions: append(mergeMainIntoFeatureIsClean,
			queries.ScriptTestAssertion{
				Query: "SELECT id, v FROM t ORDER BY id;",
				Expected: []sql.Row{
					{1, "edited on main 6"}, {50, "feature"},
					{101, "main"}, {102, "main"}, {103, "main"}, {104, "main"}, {105, "main"}, {106, "main"},
					{201, "feature"}, {202, "feature"}, {203, "feature"}, {204, "feature"}, {205, "feature"}, {206, "feature"},
				},
			},
			queries.ScriptTestAssertion{
				Query:    "SELECT count(*) FROM information_schema.columns WHERE table_name = 't';",
				Expected: []sql.Row{{10}},
			},
		),
	},
}

var ConflictingMergeBasesScripts = []queries.ScriptTest{
	{
		// git reports this conflict too. A single merge base would silently take one side's value.
		Name:        "criss-cross merge: merge bases conflict and the sides resolved them differently",
		SetUpScript: conflictingMergeBasesSetup("--ours"),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT count(*) FROM dolt_merge_bases('x', 'y');",
				Expected: []sql.Row{{2}},
			},
			{
				Query:    "CALL DOLT_MERGE('y');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "SELECT base_id, base_v, our_v, their_v FROM dolt_conflicts_t;",
				Expected: []sql.Row{{1, "orig", "x", "y"}},
			},
			{
				Query:    "SELECT id, v FROM t ORDER BY id;",
				Expected: []sql.Row{{1, "x"}, {2, "x2"}},
			},
		},
	},
	{
		Name:        "criss-cross merge: merge bases conflict and the sides resolved them the same way",
		SetUpScript: conflictingMergeBasesSetup("--theirs"),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('y');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:    "SELECT id, v FROM t ORDER BY id;",
				Expected: []sql.Row{{1, "x"}, {2, "x2"}},
			},
		},
	},
}

// CrissCrossMergeReaderScripts check that functions and tables describing a merge use the same virtual merge base as
// DOLT_MERGE.
var CrissCrossMergeReaderScripts = []queries.ScriptTest{
	{
		Name:        "criss-cross merge preview: schema tip taller than the fork point",
		SetUpScript: schemaBranchCrissCrossSetup(0),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * FROM dolt_preview_merge_conflicts_summary('feature', 'main');",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT count(*) FROM dolt_preview_merge_conflicts('feature', 'main', 't');",
				Expected: []sql.Row{{0}},
			},
		},
	},
	{
		Name:        "criss-cross merge preview: merge bases conflict and the sides resolved them differently",
		SetUpScript: conflictingMergeBasesSetup("--ours"),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * FROM dolt_preview_merge_conflicts_summary('x', 'y');",
				Expected: []sql.Row{{"t", uint64(1), uint64(0)}},
			},
			{
				Query:    "SELECT base_id, base_v, our_v, their_v FROM dolt_preview_merge_conflicts('x', 'y', 't');",
				Expected: []sql.Row{{1, "orig", "x", "y"}},
			},
		},
	},
	{
		// The merge bases x1 and y1 each add a column, so only the virtual merge base has both a and b.
		Name: "criss-cross merge: schema conflicts report the virtual merge base schema",
		SetUpScript: []string{
			"SET @@autocommit = 0;",
			"CREATE TABLE t (id INT PRIMARY KEY, v INT);",
			"CALL DOLT_COMMIT('-Am', 'c1');",
			"CALL DOLT_BRANCH('x');",
			"CALL DOLT_BRANCH('y');",
			"CALL DOLT_CHECKOUT('x');",
			"ALTER TABLE t ADD COLUMN a INT;",
			"CALL DOLT_COMMIT('-am', 'x1');",
			"CALL DOLT_CHECKOUT('y');",
			"ALTER TABLE t ADD COLUMN b INT;",
			"CALL DOLT_COMMIT('-am', 'y1');",
			"CALL DOLT_CHECKOUT('x');",
			"CALL DOLT_MERGE('y', '-m', 'x2');",
			"CALL DOLT_CHECKOUT('y');",
			"CALL DOLT_MERGE('x~1', '-m', 'y2');",
			"ALTER TABLE t MODIFY COLUMN v BIGINT;",
			"CALL DOLT_COMMIT('-am', 'y3');",
			"CALL DOLT_CHECKOUT('x');",
			"ALTER TABLE t MODIFY COLUMN v VARCHAR(20);",
			"CALL DOLT_COMMIT('-am', 'x3');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT count(*) FROM dolt_merge_bases('x', 'y');",
				Expected: []sql.Row{{2}},
			},
			{
				Query:    "CALL DOLT_MERGE('y');",
				Expected: []sql.Row{{"", 0, 1, "conflicts found"}},
			},
			{
				Query:    "SELECT table_name, base_schema LIKE '%`a` int%' AND base_schema LIKE '%`b` int%' FROM dolt_schema_conflicts;",
				Expected: []sql.Row{{"t", true}},
			},
		},
	},
}

// MergeBaseSelectionScripts run with a commit clock that advances on every commit. With several merge bases, the single
// merge base is the newest one, as in git, even when another is taller.
var MergeBaseSelectionScripts = []queries.ScriptTest{
	{
		Name:        "merge base selection: the newest merge base wins over a taller one",
		SetUpScript: schemaBranchCrissCrossSetup(2),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT message FROM dolt_log('--all') WHERE commit_hash = dolt_merge_base('main', 'feature');",
				Expected: []sql.Row{{"s2: add column m"}},
			},
			{
				Query:    "SELECT message FROM dolt_log('--all') WHERE commit_hash = dolt_merge_base('feature', 'main');",
				Expected: []sql.Row{{"s2: add column m"}},
			},
			{
				Query:    "SELECT (SELECT merge_base FROM dolt_merge_bases('main', 'feature') LIMIT 1) = dolt_merge_base('main', 'feature');",
				Expected: []sql.Row{{true}},
			},
			{
				// git diff main...feature also diffs against the schema tip and reports the inherited rows as added.
				Query:    "SELECT to_id FROM dolt_diff('main...feature', 't') WHERE diff_type = 'added' ORDER BY to_id;",
				Expected: []sql.Row{{1}, {2}, {3}, {4}, {50}},
			},
		},
	},
}

// ThreeDotScripts run with a commit clock that advances on every commit. Like git diff A...B, three-dot diffs use the
// newest merge base and warn that there are several. Like git log A...B, three-dot logs exclude every merge base.
var ThreeDotScripts = []queries.ScriptTest{
	{
		Name:        "three-dot ranges with several merge bases",
		SetUpScript: schemaBranchCrissCrossSetup(2),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:                           "SELECT count(*) FROM dolt_diff('main...feature', 't');",
				Expected:                        []sql.Row{{5}},
				ExpectedWarning:                 dtablefunctions.MultipleMergeBasesWarningCode,
				ExpectedWarningsCount:           1,
				ExpectedWarningMessageSubstring: "main...feature: multiple merge bases, using ",
			},
			{
				Query:                           "SELECT table_name, rows_added FROM dolt_diff_stat('main...feature');",
				Expected:                        []sql.Row{{"t", int64(5)}},
				ExpectedWarning:                 dtablefunctions.MultipleMergeBasesWarningCode,
				ExpectedWarningsCount:           1,
				ExpectedWarningMessageSubstring: "main...feature: multiple merge bases, using ",
			},
			{
				Query:                           "SELECT to_table_name, data_change FROM dolt_diff_summary('main...feature');",
				Expected:                        []sql.Row{{"t", true}},
				ExpectedWarning:                 dtablefunctions.MultipleMergeBasesWarningCode,
				ExpectedWarningsCount:           1,
				ExpectedWarningMessageSubstring: "main...feature: multiple merge bases, using ",
			},
			{
				Query:                           "SELECT count(*) > 0 FROM dolt_patch('main...feature');",
				Expected:                        []sql.Row{{true}},
				ExpectedWarning:                 dtablefunctions.MultipleMergeBasesWarningCode,
				ExpectedWarningsCount:           1,
				ExpectedWarningMessageSubstring: "main...feature: multiple merge bases, using ",
			},
			{
				Query:    "SELECT count(*) FROM dolt_diff('main...schema', 't');",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "SHOW WARNINGS;",
				Expected: []sql.Row{},
			},
			{
				Query: "SELECT message FROM dolt_log('main...feature') ORDER BY message;",
				Expected: []sql.Row{
					{"c3: main edits row 1 and deletes row 2"},
					{"f1: feature adds row 50"},
					{"merge schema into feature"},
					{"merge schema into main"},
				},
			},
			{
				Query:    "SHOW WARNINGS;",
				Expected: []sql.Row{},
			},
		},
	},
}

// CountCommitsScripts expect the counts of git rev-list --left-right --count from...to on an equivalent history.
var CountCommitsScripts = []queries.ScriptTest{
	{
		Name: "dolt_count_commits: single fork",
		SetUpScript: []string{
			"CREATE TABLE t (id INT PRIMARY KEY);",
			"CALL DOLT_COMMIT('-Am', 'c1');",
			"CALL DOLT_BRANCH('other');",
			"INSERT INTO t VALUES (1);",
			"CALL DOLT_COMMIT('-am', 'main 1');",
			"INSERT INTO t VALUES (2);",
			"CALL DOLT_COMMIT('-am', 'main 2');",
			"CALL DOLT_CHECKOUT('other');",
			"INSERT INTO t VALUES (3);",
			"CALL DOLT_COMMIT('-am', 'other 1');",
			"CALL DOLT_CHECKOUT('main');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_COUNT_COMMITS('--from', 'main', '--to', 'other');",
				Expected: []sql.Row{{uint64(2), uint64(1)}},
			},
			{
				Query:    "CALL DOLT_COUNT_COMMITS('--from', 'main', '--to', 'main');",
				Expected: []sql.Row{{uint64(0), uint64(0)}},
			},
			{
				Query:    "CALL DOLT_MERGE('other', '-m', 'merge other');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				// main 1, main 2 and the merge commit; git rev-list --left-right --count main...other gives 3 0.
				Query:    "CALL DOLT_COUNT_COMMITS('--from', 'main', '--to', 'other');",
				Expected: []sql.Row{{uint64(3), uint64(0)}},
			},
		},
	},
	{
		Name:        "dolt_count_commits: several merge bases",
		SetUpScript: schemaBranchCrissCrossSetup(2),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_COUNT_COMMITS('--from', 'main', '--to', 'feature');",
				Expected: []sql.Row{{uint64(2), uint64(2)}},
			},
			{
				Query:    "CALL DOLT_COUNT_COMMITS('--from', 'feature', '--to', 'main');",
				Expected: []sql.Row{{uint64(2), uint64(2)}},
			},
			{
				Query:    "CALL DOLT_COUNT_COMMITS('--from', 'main', '--to', 'schema');",
				Expected: []sql.Row{{uint64(5), uint64(0)}},
			},
		},
	},
}

var MergeBasesTableFunctionScripts = []queries.ScriptTest{
	{
		Name:        "dolt_merge_bases: lists every merge base of a criss-cross history",
		SetUpScript: schemaBranchCrissCrossSetup(0),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT l.message FROM dolt_merge_bases('main', 'feature') b " +
					"JOIN dolt_log('--all') l ON l.commit_hash = b.merge_base ORDER BY l.message;",
				Expected: []sql.Row{{"c2: seed rows on main"}, {"s2: add column m"}},
			},
			{
				Query:    "SELECT count(*) FROM dolt_merge_bases(hashof('feature'), 'main');",
				Expected: []sql.Row{{2}},
			},
			{
				Query:    "SELECT dolt_merge_base('main', 'feature') IN (SELECT merge_base FROM dolt_merge_bases('main', 'feature'));",
				Expected: []sql.Row{{true}},
			},
			{
				Query: "SELECT l.message FROM dolt_merge_bases('main', 'schema') b " +
					"JOIN dolt_log('--all') l ON l.commit_hash = b.merge_base;",
				Expected: []sql.Row{{"s2: add column m"}},
			},
			{
				Query:    "SELECT count(*) FROM dolt_merge_bases('main', NULL);",
				Expected: []sql.Row{{0}},
			},
			{
				Query:          "SELECT * FROM dolt_merge_bases('main');",
				ExpectedErrStr: "function 'dolt_merge_bases' expected 2 arguments, 1 received",
			},
			{
				Query:          "SELECT * FROM dolt_merge_bases('main', 'feature', 'schema');",
				ExpectedErrStr: "function 'dolt_merge_bases' expected 2 arguments, 3 received",
			},
			{
				Query:          "SELECT * FROM dolt_merge_bases('main', 'nonexistent');",
				ExpectedErrStr: "branch not found: nonexistent",
			},
		},
	},
}
