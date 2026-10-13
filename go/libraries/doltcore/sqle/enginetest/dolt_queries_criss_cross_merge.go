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

var MergeBasesTableFunctionScripts = []queries.ScriptTest{
	{
		Name:        "dolt_merge_bases: lists every merge base of a criss-cross history",
		SetUpScript: schemaBranchCrissCrossSetup(0),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT l.message FROM dolt_merge_bases('main', 'feature') b " +
					"JOIN dolt_log('main', 'feature', 'schema') l ON l.commit_hash = b.merge_base ORDER BY l.message;",
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
					"JOIN dolt_log('main', 'feature', 'schema') l ON l.commit_hash = b.merge_base;",
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

// MergeBasesOrderScripts run with a commit clock that advances on every commit. dolt_merge_bases lists the newest merge
// base first, as `git merge-base --all` does, even when another merge base is taller.
var MergeBasesOrderScripts = []queries.ScriptTest{
	{
		Name:        "dolt_merge_bases: newest merge base first",
		SetUpScript: schemaBranchCrissCrossSetup(2),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT l.message FROM (SELECT merge_base FROM dolt_merge_bases('main', 'feature') LIMIT 1 OFFSET 0) b " +
					"JOIN dolt_log('main', 'feature', 'schema') l ON l.commit_hash = b.merge_base;",
				Expected: []sql.Row{{"s2: add column m"}},
			},
			{
				Query: "SELECT l.message FROM (SELECT merge_base FROM dolt_merge_bases('main', 'feature') LIMIT 1 OFFSET 1) b " +
					"JOIN dolt_log('main', 'feature', 'schema') l ON l.commit_hash = b.merge_base;",
				Expected: []sql.Row{{"c2-4: more seed rows"}},
			},
			{
				Query: "SELECT l.message FROM (SELECT merge_base FROM dolt_merge_bases('feature', 'main') LIMIT 1 OFFSET 0) b " +
					"JOIN dolt_log('main', 'feature', 'schema') l ON l.commit_hash = b.merge_base;",
				Expected: []sql.Row{{"s2: add column m"}},
			},
		},
	},
}

// fanInSetup creates branches p1..p|n| from main with one commit each, and merges them into m1 in order and into m2 in
// reverse order, so m1 and m2 have |n| merge bases.
func fanInSetup(n int) []string {
	script := []string{
		"CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(20));",
		"CALL DOLT_COMMIT('-Am', 'create table');",
		"CALL DOLT_BRANCH('m1');",
		"CALL DOLT_BRANCH('m2');",
	}
	for i := 1; i <= n; i++ {
		script = append(script,
			fmt.Sprintf("CALL DOLT_BRANCH('p%d', 'main');", i),
			fmt.Sprintf("CALL DOLT_CHECKOUT('p%d');", i),
			fmt.Sprintf("INSERT INTO t VALUES (%d, 'p%d');", i, i),
			fmt.Sprintf("CALL DOLT_COMMIT('-am', 'p%d');", i))
	}
	script = append(script,
		"CALL DOLT_CHECKOUT('m1');",
		"INSERT INTO t VALUES (1001, 'm1');",
		"CALL DOLT_COMMIT('-am', 'm1');")
	for i := 1; i <= n; i++ {
		script = append(script, fmt.Sprintf("CALL DOLT_MERGE('p%d', '-m', 'm1 merges p%d');", i, i))
	}
	script = append(script,
		"CALL DOLT_CHECKOUT('m2');",
		"INSERT INTO t VALUES (1002, 'm2');",
		"CALL DOLT_COMMIT('-am', 'm2');")
	for i := n; i >= 1; i-- {
		script = append(script, fmt.Sprintf("CALL DOLT_MERGE('p%d', '-m', 'm2 merges p%d');", i, i))
	}
	return append(script, "CALL DOLT_CHECKOUT('main');")
}

// crossMergeRound adds a commit to branches |x| and |y|, tags them <x><round> and <y><round>, and merges each tag into
// the other branch, so the two branches criss-cross every round.
func crossMergeRound(x, y string, round int) []string {
	return []string{
		fmt.Sprintf("CALL DOLT_CHECKOUT('%s');", x),
		fmt.Sprintf("INSERT INTO t VALUES (%d, '%s');", 100*round+1, x),
		fmt.Sprintf("CALL DOLT_COMMIT('-am', '%s%d');", x, round),
		fmt.Sprintf("CALL DOLT_TAG('%s%d', '%s');", x, round, x),
		fmt.Sprintf("CALL DOLT_CHECKOUT('%s');", y),
		fmt.Sprintf("INSERT INTO t VALUES (%d, '%s');", 100*round+2, y),
		fmt.Sprintf("CALL DOLT_COMMIT('-am', '%s%d');", y, round),
		fmt.Sprintf("CALL DOLT_TAG('%s%d', '%s');", y, round, y),
		fmt.Sprintf("CALL DOLT_MERGE('%s%d', '-m', '%s merges %s%d');", x, round, y, x, round),
		fmt.Sprintf("CALL DOLT_CHECKOUT('%s');", x),
		fmt.Sprintf("CALL DOLT_MERGE('%s%d', '-m', '%s merges %s%d');", y, round, x, y, round),
	}
}

// fourBranchesSetup creates branches a, b, c and d. c and d cross-merge every round for 4 rounds. After round 2, a
// and b each commit and then merge c and d respectively. Then a gets 20 more commits and b gets 1.
func fourBranchesSetup() []string {
	script := []string{
		"CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(20));",
		"CALL DOLT_COMMIT('-Am', 'create table');",
		"CALL DOLT_BRANCH('a');",
		"CALL DOLT_BRANCH('b');",
		"CALL DOLT_BRANCH('c');",
		"CALL DOLT_BRANCH('d');",
	}
	for round := 1; round <= 4; round++ {
		script = append(script, crossMergeRound("c", "d", round)...)
		if round == 2 {
			script = append(script,
				"CALL DOLT_CHECKOUT('a');",
				"INSERT INTO t VALUES (1000, 'a');",
				"CALL DOLT_COMMIT('-am', 'a 0');",
				"CALL DOLT_MERGE('c', '-m', 'a merges c');",
				"CALL DOLT_CHECKOUT('b');",
				"INSERT INTO t VALUES (2000, 'b');",
				"CALL DOLT_COMMIT('-am', 'b 0');",
				"CALL DOLT_MERGE('d', '-m', 'b merges d');")
		}
	}
	script = append(script, "CALL DOLT_CHECKOUT('a');")
	for i := 1; i <= 20; i++ {
		script = append(script,
			fmt.Sprintf("INSERT INTO t VALUES (%d, 'a');", 1000+i),
			fmt.Sprintf("CALL DOLT_COMMIT('-am', 'a %d');", i))
	}
	return append(script,
		"CALL DOLT_CHECKOUT('b');",
		"INSERT INTO t VALUES (2001, 'b');",
		"CALL DOLT_COMMIT('-am', 'b 1');",
		"CALL DOLT_CHECKOUT('main');")
}

// orderedMergeBaseBranches lists the branches pointing at the merge bases of |left| and |right|, in dolt_merge_bases
// order.
func orderedMergeBaseBranches(left, right string) string {
	return fmt.Sprintf("SELECT br.name FROM (SELECT merge_base, ROW_NUMBER() OVER () AS rn FROM dolt_merge_bases('%s', '%s')) b "+
		"JOIN dolt_branches br ON br.hash = b.merge_base ORDER BY b.rn;", left, right)
}

// orderedMergeBaseTags lists the tags pointing at the merge bases of |left| and |right|, in dolt_merge_bases order.
func orderedMergeBaseTags(left, right string) string {
	return fmt.Sprintf("SELECT tg.tag_name FROM (SELECT merge_base, ROW_NUMBER() OVER () AS rn FROM dolt_merge_bases('%s', '%s')) b "+
		"JOIN dolt_tags tg ON tg.tag_hash = b.merge_base ORDER BY b.rn;", left, right)
}

// MergeBasesManyScripts run with a commit clock that advances on every commit, so the order of merge bases is fixed.
var MergeBasesManyScripts = []queries.ScriptTest{
	{
		Name:        "dolt_merge_bases: 3 merge bases",
		SetUpScript: fanInSetup(3),
		Assertions: []queries.ScriptTestAssertion{
			{Query: orderedMergeBaseBranches("m1", "m2"), Expected: []sql.Row{{"p3"}, {"p2"}, {"p1"}}},
			{Query: orderedMergeBaseBranches("m2", "m1"), Expected: []sql.Row{{"p3"}, {"p2"}, {"p1"}}},
			{
				Query:    "SELECT dolt_merge_base('m1', 'm2') IN (SELECT merge_base FROM dolt_merge_bases('m1', 'm2'));",
				Expected: []sql.Row{{true}},
			},
			{Query: orderedMergeBaseBranches("m1", "p2"), Expected: []sql.Row{{"p2"}}},
			{Query: orderedMergeBaseBranches("p1", "p2"), Expected: []sql.Row{{"main"}}},
		},
	},
	{
		Name:        "dolt_merge_bases: 4 merge bases",
		SetUpScript: fanInSetup(4),
		Assertions: []queries.ScriptTestAssertion{
			{Query: orderedMergeBaseBranches("m1", "m2"), Expected: []sql.Row{{"p4"}, {"p3"}, {"p2"}, {"p1"}}},
		},
	},
	{
		Name:        "dolt_merge_bases: 5 merge bases",
		SetUpScript: fanInSetup(5),
		Assertions: []queries.ScriptTestAssertion{
			{Query: orderedMergeBaseBranches("m1", "m2"), Expected: []sql.Row{{"p5"}, {"p4"}, {"p3"}, {"p2"}, {"p1"}}},
			{Query: "SELECT count(*) FROM dolt_merge_bases('m2', 'm1');", Expected: []sql.Row{{5}}},
		},
	},
	{
		Name: "dolt_merge_bases: repeated criss-cross merges keep only the last round",
		SetUpScript: func() []string {
			script := []string{
				"CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(20));",
				"CALL DOLT_COMMIT('-Am', 'create table');",
				"CALL DOLT_BRANCH('x');",
				"CALL DOLT_BRANCH('y');",
			}
			for round := 1; round <= 5; round++ {
				script = append(script, crossMergeRound("x", "y", round)...)
			}
			return script
		}(),
		Assertions: []queries.ScriptTestAssertion{
			{Query: orderedMergeBaseTags("x", "y"), Expected: []sql.Row{{"y5"}, {"x5"}}},
			{Query: orderedMergeBaseTags("y", "x"), Expected: []sql.Row{{"y5"}, {"x5"}}},
			{Query: orderedMergeBaseTags("x", "y3"), Expected: []sql.Row{{"y3"}}},
		},
	},
	{
		Name:        "dolt_merge_bases: four branches, two of which cross-merge every round",
		SetUpScript: fourBranchesSetup(),
		Assertions: []queries.ScriptTestAssertion{
			{Query: orderedMergeBaseTags("a", "b"), Expected: []sql.Row{{"d2"}, {"c2"}}},
			{Query: orderedMergeBaseTags("c", "d"), Expected: []sql.Row{{"d4"}, {"c4"}}},
			{
				Query: "SELECT l.message FROM dolt_merge_bases('a', 'd') b " +
					"JOIN dolt_log('a', 'b', 'c', 'd') l ON l.commit_hash = b.merge_base;",
				Expected: []sql.Row{{"c merges d2"}},
			},
			{
				Query: "SELECT l.message FROM dolt_merge_bases('b', 'c') b " +
					"JOIN dolt_log('a', 'b', 'c', 'd') l ON l.commit_hash = b.merge_base;",
				Expected: []sql.Row{{"d merges c2"}},
			},
		},
	},
}
