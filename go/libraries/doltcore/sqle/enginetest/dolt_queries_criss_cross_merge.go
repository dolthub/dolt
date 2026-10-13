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
