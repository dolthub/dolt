// Copyright 2025 Dolthub, Inc.
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

var BranchActivityTests = []queries.ScriptTest{
	{
		Name: "branch activity table shows all branches",
		SetUpScript: []string{
			"CALL dolt_branch('feature1')",
			"CALL dolt_branch('feature2')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM dolt_branch_activity",
				Expected: []sql.Row{{3}},
			},
			{
				Query: "SELECT branch FROM dolt_branch_activity ORDER BY branch",
				Expected: []sql.Row{
					{"feature1"},
					{"feature2"},
					{"main"},
				},
			},
		},
	},
	{
		// This may be a little counterintuitive, but we can create a branch without resolving it first so we end
		// up with a last_write but no last_read.
		Name: "branch creation does sets last write but not last read",
		SetUpScript: []string{
			"CALL dolt_branch('new_branch')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT branch, last_read , last_write IS NULL FROM dolt_branch_activity WHERE branch = 'new_branch'",
				Expected: []sql.Row{
					{"new_branch", nil, false},
				},
			},
		},
	},
	{
		Name: "AS OF updates the read time for a branch",
		SetUpScript: []string{
			"CREATE TABLE t (id INT PRIMARY KEY)",
			"INSERT INTO t VALUES (1), (2), (3)",
			"CALL dolt_commit('-Am', 'initial commit')",
			"CALL dolt_branch('new_branch')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				// Creating the branch results in a write time, but no read time.
				Query: "SELECT branch, last_read IS NULL, last_write IS NULL FROM dolt_branch_activity WHERE branch = 'new_branch'",
				Expected: []sql.Row{
					{"new_branch", true, false},
				},
			},
			{
				Query:    "SELECT * FROM t AS OF 'HEAD'",
				Expected: []sql.Row{{1}, {2}, {3}},
			},
			{
				Query:    "SELECT * FROM t AS OF 'new_branch'",
				Expected: []sql.Row{{1}, {2}, {3}},
			},
			{
				Query:            "SELECT SLEEP(1)", // activity update is async, give it a moment
				SkipResultsCheck: true,
			},
			{
				Query: "SELECT branch, last_read IS NULL, last_write IS NULL FROM dolt_branch_activity WHERE branch = 'new_branch'",
				Expected: []sql.Row{
					{"new_branch", false, false},
				},
			},
			{
				Query:    "SELECT branch, last_read IS NULL, last_write IS NULL FROM dolt_branch_activity WHERE branch = 'HEAD'",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "branch checkout counts as read activity",
		SetUpScript: []string{
			"CALL dolt_branch('test_branch')",
			"SELECT SLEEP(4)", // ensure time difference between branch creation and checkout
			"CALL dolt_checkout('test_branch')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT branch, last_read IS NOT NULL, last_write IS NOT NULL FROM dolt_branch_activity WHERE branch = 'test_branch'",
				Expected: []sql.Row{
					{"test_branch", true, true},
				},
			},
			{
				Query: "SELECT branch, TIMESTAMPDIFF(SECOND,last_write,last_read) > 3, TIMESTAMPDIFF(SECOND,last_write,last_read) <=5 FROM dolt_branch_activity WHERE branch = 'test_branch'",
				Expected: []sql.Row{
					{"test_branch", true, true},
				},
			},
			{
				Query:    "SELECT COUNT(*) FROM dolt_branch_activity WHERE system_start_time IS NOT NULL",
				Expected: []sql.Row{{2}},
			},
		},
	},
	{
		Name: "branch activity filtered for delete branches",
		SetUpScript: []string{
			"CALL dolt_branch('temp_branch')",
			"CALL dolt_checkout('temp_branch')",
			"CALL dolt_checkout('main')",
			"CALL dolt_branch('-d', 'temp_branch')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM dolt_branch_activity WHERE branch = 'temp_branch'",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "SELECT branch FROM dolt_branch_activity",
				Expected: []sql.Row{{"main"}},
			},
		},
	},
	{
		Name: "branch activity when writing to another branch",
		SetUpScript: []string{
			"CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(20))",
			"INSERT INTO t VALUES (1,'foo')",
			"CALL dolt_commit('-Am', 'initial commit')",
			"CALL dolt_branch('other_branch')",
			"SELECT SLEEP(1)", // branch_activity update is async, give it a moment.
			"SELECT last_write INTO @lw FROM dolt_branch_activity WHERE branch = 'other_branch'",
			"SELECT SLEEP(2)", // Ensure time stamp difference is noticeable
			"UPDATE `mydb/other_branch`.t SET v='baz' WHERE id=1",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT last_write <> @lw FROM dolt_branch_activity WHERE branch = 'other_branch'",
				Expected: []sql.Row{{true}},
			},
		},
	},
}
