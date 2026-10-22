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
	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dtablefunctions"
)

// taggedCrissCrossSetup builds a criss-cross, the smallest one with two merge bases, tagged baseA and baseB:
//
//	main -- baseA -- mergeA    branchA
//	    \-- baseB -- mergeB    branchB
//	mergeA = merge(baseA, baseB), mergeB = merge(baseB, baseA)
//
// baseB is committed after baseA, so it is listed first, as in `git merge-base --all`. The scripts using it run with a
// commit clock that advances on every commit, so later commits have later committer dates.
func taggedCrissCrossSetup() []string {
	return []string{
		"CREATE TABLE t (id INT PRIMARY KEY);",
		"CALL DOLT_COMMIT('-Am', 'main');",
		"CALL DOLT_BRANCH('branchA');",
		"CALL DOLT_BRANCH('branchB');",
		"CALL DOLT_CHECKOUT('branchA');",
		"INSERT INTO t VALUES (1);",
		"CALL DOLT_COMMIT('-am', 'baseA');",
		"CALL DOLT_TAG('baseA');",
		"CALL DOLT_CHECKOUT('branchB');",
		"INSERT INTO t VALUES (2);",
		"CALL DOLT_COMMIT('-am', 'baseB');",
		"CALL DOLT_TAG('baseB');",
		"CALL DOLT_MERGE('baseA', '-m', 'mergeB');",
		"CALL DOLT_CHECKOUT('branchA');",
		"CALL DOLT_MERGE('baseB', '-m', 'mergeA');",
		"CALL DOLT_CHECKOUT('main');",
	}
}

// MergeBasesTableFunctionScripts test what the dolt_merge_bases table function adds on top of the merge base walk,
// which the commitgraph and merge tests cover: how arguments are evaluated and resolved, and how the function behaves
// in a query.
var MergeBasesTableFunctionScripts = []queries.ScriptTest{
	{
		Name:        "dolt_merge_bases",
		SetUpScript: append(taggedCrissCrossSetup(), "SET @left = 'branchA';"),
		Assertions: []queries.ScriptTestAssertion{
			{
				// ROW_NUMBER() keeps the order of the table function's rows.
				Query: "SELECT tg.tag_name FROM (SELECT merge_base, ROW_NUMBER() OVER () AS rn FROM dolt_merge_bases('branchA', 'branchB')) b " +
					"JOIN dolt_tags tg ON tg.tag_hash = b.merge_base ORDER BY b.rn;",
				Expected: []sql.Row{{"baseB"}, {"baseA"}},
			},
			{
				Query: "SELECT tg.tag_name FROM (SELECT merge_base, ROW_NUMBER() OVER () AS rn FROM dolt_merge_bases('branchB', 'branchA')) b " +
					"JOIN dolt_tags tg ON tg.tag_hash = b.merge_base ORDER BY b.rn;",
				Expected: []sql.Row{{"baseB"}, {"baseA"}},
			},
			{
				Query:    "SELECT merge_base = hashof('main') FROM dolt_merge_bases('main', 'branchA');",
				Expected: []sql.Row{{true}},
			},

			// Arguments are expressions.
			{
				Query:    "SELECT count(*) FROM dolt_merge_bases(CONCAT('branch', 'A'), CONCAT('branch', UPPER('b')));",
				Expected: []sql.Row{{2}},
			},
			{
				Query:    "SELECT count(*) FROM dolt_merge_bases(@left, 'branchB');",
				Expected: []sql.Row{{2}},
			},
			{
				Query:    "SELECT count(*) FROM dolt_merge_bases(hashof('branchA'), hashof('branchB'));",
				Expected: []sql.Row{{2}},
			},
			{
				Query:    "SELECT count(*) FROM dolt_merge_bases(NULL, 'branchB');",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "SELECT count(*) FROM dolt_merge_bases('branchA', NULL);",
				Expected: []sql.Row{{0}},
			},
			{
				Query:          "SELECT * FROM dolt_merge_bases(1, 'branchB');",
				ExpectedErrStr: "received '1' when expecting string",
			},
			{
				Query:          "SELECT * FROM dolt_merge_bases('branchA');",
				ExpectedErrStr: "function 'dolt_merge_bases' expected 2 arguments, 1 received",
			},
			{
				Query:          "SELECT * FROM dolt_merge_bases('branchA', 'branchB', 'main');",
				ExpectedErrStr: "function 'dolt_merge_bases' expected 2 arguments, 3 received",
			},

			// Arguments resolve like other commit specs.
			{
				Query:    "SELECT merge_base = hashof('baseA') FROM dolt_merge_bases('branchA~1', 'branchB');",
				Expected: []sql.Row{{true}},
			},
			{
				Query:    "SELECT merge_base = hashof('baseB') FROM dolt_merge_bases('baseB', 'branchA');",
				Expected: []sql.Row{{true}},
			},
			{
				Query:          "SELECT * FROM dolt_merge_bases('branchA', 'nonexistent');",
				ExpectedErrStr: "branch not found: nonexistent",
			},

			// The function is a table in a query.
			{
				Query: "SELECT tg.tag_name FROM dolt_merge_bases('branchA', 'branchB') b " +
					"JOIN dolt_tags tg ON tg.tag_hash = b.merge_base ORDER BY tg.tag_name;",
				Expected: []sql.Row{{"baseA"}, {"baseB"}},
			},
			{
				Query:    "SELECT count(*) FROM dolt_log('branchA') WHERE commit_hash IN (SELECT merge_base FROM dolt_merge_bases('branchA', 'branchB'));",
				Expected: []sql.Row{{2}},
			},
			{
				Query: "SELECT br.name, count(*) FROM dolt_branches br " +
					"JOIN LATERAL (SELECT merge_base FROM dolt_merge_bases(br.name, 'branchB')) b GROUP BY br.name ORDER BY br.name;",
				Expected: []sql.Row{{"branchA", 2}, {"branchB", 1}, {"main", 1}},
			},

			// HEAD is the head of the current database's branch.
			{
				Query:    "USE `mydb/branchA`;",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT count(*) FROM dolt_merge_bases('HEAD', 'branchB');",
				Expected: []sql.Row{{2}},
			},
			{
				Query:    "SELECT merge_base = hashof('main') FROM dolt_merge_bases('HEAD~2', 'branchB');",
				Expected: []sql.Row{{true}},
			},
		},
	},
}

// ThreeDotScripts test three-dot ranges of commits with several merge bases. Like git diff A...B, three-dot diffs use
// the merge base with the newest committer date, baseB, and warn that there are several. Like git log A...B, three-dot
// logs exclude every merge base.
var ThreeDotScripts = []queries.ScriptTest{
	{
		Name:        "three-dot ranges with several merge bases",
		SetUpScript: taggedCrissCrossSetup(),
		Assertions: []queries.ScriptTestAssertion{
			{
				// From baseB, which has row 2, to branchB, which has rows 1 and 2.
				Query:                           "SELECT diff_type, to_id FROM dolt_diff('branchA...branchB', 't');",
				Expected:                        []sql.Row{{"added", int32(1)}},
				ExpectedWarning:                 dtablefunctions.MultipleMergeBasesWarningCode,
				ExpectedWarningsCount:           1,
				ExpectedWarningMessageSubstring: "branchA...branchB: multiple merge bases, using ",
			},
			{
				Query:                           "SELECT diff_type, to_id FROM dolt_diff('branchB...branchA', 't');",
				Expected:                        []sql.Row{{"added", int32(1)}},
				ExpectedWarning:                 dtablefunctions.MultipleMergeBasesWarningCode,
				ExpectedWarningsCount:           1,
				ExpectedWarningMessageSubstring: "branchB...branchA: multiple merge bases, using ",
			},
			{
				Query:                           "SELECT table_name, rows_added FROM dolt_diff_stat('branchA...branchB');",
				Expected:                        []sql.Row{{"t", int64(1)}},
				ExpectedWarning:                 dtablefunctions.MultipleMergeBasesWarningCode,
				ExpectedWarningsCount:           1,
				ExpectedWarningMessageSubstring: "branchA...branchB: multiple merge bases, using ",
			},
			{
				Query:                           "SELECT to_table_name, data_change FROM dolt_diff_summary('branchA...branchB');",
				Expected:                        []sql.Row{{"t", true}},
				ExpectedWarning:                 dtablefunctions.MultipleMergeBasesWarningCode,
				ExpectedWarningsCount:           1,
				ExpectedWarningMessageSubstring: "branchA...branchB: multiple merge bases, using ",
			},
			{
				Query:                           "SELECT statement FROM dolt_patch('branchA...branchB');",
				Expected:                        []sql.Row{{"INSERT INTO `t` (`id`) VALUES (1);"}},
				ExpectedWarning:                 dtablefunctions.MultipleMergeBasesWarningCode,
				ExpectedWarningsCount:           1,
				ExpectedWarningMessageSubstring: "branchA...branchB: multiple merge bases, using ",
			},

			// One merge base, or a two-dot range, does not warn.
			{
				Query:    "SELECT diff_type, to_id FROM dolt_diff('main...branchA', 't');",
				Expected: []sql.Row{{"added", int32(1)}, {"added", int32(2)}},
			},
			{
				Query:    "SHOW WARNINGS;",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT count(*) FROM dolt_diff('branchA..branchB', 't');",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "SHOW WARNINGS;",
				Expected: []sql.Row{},
			},

			// Neither baseA nor baseB is in branchA...branchB.
			{
				Query:    "SELECT message FROM dolt_log('branchA...branchB') ORDER BY message;",
				Expected: []sql.Row{{"mergeA"}, {"mergeB"}},
			},
			{
				Query:    "SHOW WARNINGS;",
				Expected: []sql.Row{},
			},
		},
	},
}
