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

// schemaBranchSetup builds the history from https://github.com/dolthub/dolt/issues/12050. A schema branch is merged
// into main and feature, and never receives their changes, so main and feature have two merge bases, c2 and s2:
//
//	c1 -- c2 -- c3 -- m    main
//	 |     \-- f1 -- f    feature
//	 \-- s1 -- s2          schema
//	m = merge(c3, s2), f = merge(f1, s2)
//
// c2 seeds rows 1 and 2. c3 edits row 1 and deletes row 2. f1 adds row 50. s1 and s2 add columns n and m.
var schemaBranchSetup = []string{
	"CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(100));",
	"CALL DOLT_COMMIT('-Am', 'c1');",
	"CALL DOLT_BRANCH('schema');",
	"INSERT INTO t VALUES (1, 'seed'), (2, 'seed');",
	"CALL DOLT_COMMIT('-am', 'c2');",
	"CALL DOLT_BRANCH('feature');",
	"UPDATE t SET v = 'edited on main' WHERE id = 1;",
	"DELETE FROM t WHERE id = 2;",
	"CALL DOLT_COMMIT('-am', 'c3');",
	"CALL DOLT_CHECKOUT('feature');",
	"INSERT INTO t VALUES (50, 'feature');",
	"CALL DOLT_COMMIT('-am', 'f1');",
	"CALL DOLT_CHECKOUT('schema');",
	"ALTER TABLE t ADD COLUMN n VARCHAR(100);",
	"CALL DOLT_COMMIT('-am', 's1');",
	"ALTER TABLE t ADD COLUMN m VARCHAR(100);",
	"CALL DOLT_COMMIT('-am', 's2');",
	"CALL DOLT_CHECKOUT('main');",
	"CALL DOLT_MERGE('schema', '-m', 'm');",
	"CALL DOLT_CHECKOUT('feature');",
	"CALL DOLT_MERGE('schema', '-m', 'f');",
}

// MergeMultipleBasesScripts merge branches that have more than one merge base. The expected results are git's: each
// history was also built in git, with a directory per table, a file per row and a line per column value, and `git
// merge` (git 2.39) produced the same rows and conflicts.
var MergeMultipleBasesScripts = []queries.ScriptTest{
	{
		Name:        "schema branch: merge main into feature",
		SetUpScript: append(append([]string{}, schemaBranchSetup...), "CALL DOLT_CHECKOUT('feature');"),
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
		SetUpScript: append(append([]string{}, schemaBranchSetup...), "CALL DOLT_CHECKOUT('main');"),
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
}
