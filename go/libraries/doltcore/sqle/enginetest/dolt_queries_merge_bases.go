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
)

// MergeBasesTableFunctionScripts test what the dolt_merge_bases table function adds on top of the merge base walk,
// which the commitgraph and merge tests cover: how arguments are evaluated and resolved, and how the function behaves
// in a query. They run with a commit clock that advances on every commit, so later commits have later committer dates.
//
// The history is a criss-cross, the smallest one with two merge bases, tagged x0 and y0:
//
//	main -- x0 -- x1    branch x
//	   \-- y0 -- y1    branch y
//	x1 = merge(x0, y0), y1 = merge(y0, x0)
//
// y0 is committed after x0, so it is listed first, as in `git merge-base --all`.
var MergeBasesTableFunctionScripts = []queries.ScriptTest{
	{
		Name: "dolt_merge_bases",
		SetUpScript: []string{
			"CREATE TABLE t (id INT PRIMARY KEY);",
			"CALL DOLT_COMMIT('-Am', 'main');",
			"CALL DOLT_BRANCH('x');",
			"CALL DOLT_BRANCH('y');",
			"CALL DOLT_CHECKOUT('x');",
			"INSERT INTO t VALUES (1);",
			"CALL DOLT_COMMIT('-am', 'x0');",
			"CALL DOLT_TAG('x0');",
			"CALL DOLT_CHECKOUT('y');",
			"INSERT INTO t VALUES (2);",
			"CALL DOLT_COMMIT('-am', 'y0');",
			"CALL DOLT_TAG('y0');",
			"CALL DOLT_MERGE('x0', '-m', 'y1');",
			"CALL DOLT_CHECKOUT('x');",
			"CALL DOLT_MERGE('y0', '-m', 'x1');",
			"CALL DOLT_CHECKOUT('main');",
			"SET @left = 'x';",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				// ROW_NUMBER() keeps the order of the table function's rows.
				Query: "SELECT tg.tag_name FROM (SELECT merge_base, ROW_NUMBER() OVER () AS rn FROM dolt_merge_bases('x', 'y')) b " +
					"JOIN dolt_tags tg ON tg.tag_hash = b.merge_base ORDER BY b.rn;",
				Expected: []sql.Row{{"y0"}, {"x0"}},
			},
			{
				Query: "SELECT tg.tag_name FROM (SELECT merge_base, ROW_NUMBER() OVER () AS rn FROM dolt_merge_bases('y', 'x')) b " +
					"JOIN dolt_tags tg ON tg.tag_hash = b.merge_base ORDER BY b.rn;",
				Expected: []sql.Row{{"y0"}, {"x0"}},
			},
			{
				Query:    "SELECT merge_base = hashof('main') FROM dolt_merge_bases('main', 'x');",
				Expected: []sql.Row{{true}},
			},

			// Arguments are expressions.
			{
				Query:    "SELECT count(*) FROM dolt_merge_bases(CONCAT('', 'x'), LOWER('Y'));",
				Expected: []sql.Row{{2}},
			},
			{
				Query:    "SELECT count(*) FROM dolt_merge_bases(@left, 'y');",
				Expected: []sql.Row{{2}},
			},
			{
				Query:    "SELECT count(*) FROM dolt_merge_bases(hashof('x'), hashof('y'));",
				Expected: []sql.Row{{2}},
			},
			{
				Query:    "SELECT count(*) FROM dolt_merge_bases(NULL, 'y');",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "SELECT count(*) FROM dolt_merge_bases('x', NULL);",
				Expected: []sql.Row{{0}},
			},
			{
				Query:          "SELECT * FROM dolt_merge_bases(1, 'y');",
				ExpectedErrStr: "received '1' when expecting string",
			},
			{
				Query:          "SELECT * FROM dolt_merge_bases('x');",
				ExpectedErrStr: "function 'dolt_merge_bases' expected 2 arguments, 1 received",
			},
			{
				Query:          "SELECT * FROM dolt_merge_bases('x', 'y', 'main');",
				ExpectedErrStr: "function 'dolt_merge_bases' expected 2 arguments, 3 received",
			},

			// Arguments resolve like other commit specs.
			{
				Query:    "SELECT merge_base = hashof('x0') FROM dolt_merge_bases('x~1', 'y');",
				Expected: []sql.Row{{true}},
			},
			{
				Query:    "SELECT merge_base = hashof('y0') FROM dolt_merge_bases('y0', 'x');",
				Expected: []sql.Row{{true}},
			},
			{
				Query:          "SELECT * FROM dolt_merge_bases('x', 'nonexistent');",
				ExpectedErrStr: "branch not found: nonexistent",
			},

			// The function is a table in a query.
			{
				Query: "SELECT tg.tag_name FROM dolt_merge_bases('x', 'y') b " +
					"JOIN dolt_tags tg ON tg.tag_hash = b.merge_base ORDER BY tg.tag_name;",
				Expected: []sql.Row{{"x0"}, {"y0"}},
			},
			{
				Query:    "SELECT count(*) FROM dolt_log('x') WHERE commit_hash IN (SELECT merge_base FROM dolt_merge_bases('x', 'y'));",
				Expected: []sql.Row{{2}},
			},
			{
				Query: "SELECT br.name, count(*) FROM dolt_branches br " +
					"JOIN LATERAL (SELECT merge_base FROM dolt_merge_bases(br.name, 'y')) b GROUP BY br.name ORDER BY br.name;",
				Expected: []sql.Row{{"main", 1}, {"x", 2}, {"y", 1}},
			},

			// HEAD is the head of the current database's branch.
			{
				Query:    "USE `mydb/x`;",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT count(*) FROM dolt_merge_bases('HEAD', 'y');",
				Expected: []sql.Row{{2}},
			},
			{
				Query:    "SELECT merge_base = hashof('main') FROM dolt_merge_bases('HEAD~2', 'y');",
				Expected: []sql.Row{{true}},
			},
		},
	},
}
