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
	"github.com/dolthub/go-mysql-server/sql/types"
)

var DoltStashTests = []queries.ScriptTest{
	{
		Name: "DOLT_STASH() subcommands error on empty space.",
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "CALL DOLT_STASH('push', 'myStash');",
				ExpectedErrStr: "No local changes to save",
			},
			{
				Query:    "CREATE TABLE test (i int)",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:          "CALL DOLT_STASH('push', 'myStash');",
				ExpectedErrStr: "No local changes to save",
			},
			{
				Query:          "CALL DOLT_STASH('pop', 'myStash');",
				ExpectedErrStr: "No stash entries found.",
			},
			{
				Query:          "CALL DOLT_STASH('drop', 'myStash');",
				ExpectedErrStr: "No stash entries found.",
			},
			{
				Query:    "CALL DOLT_STASH('clear','myStash');",
				Expected: []sql.Row{{0}},
			},
		},
	},
	{
		Name: "Simple push and pop with DOLT_STASH()",
		SetUpScript: []string{
			"CREATE TABLE test(pk BIGINT PRIMARY KEY, v varchar(10))",
			"CALL DOLT_COMMIT('-A', '-m', 'Created table')",
			"INSERT INTO test VALUES (1, 'a')",
			"CALL DOLT_STASH('push', 'myStash');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * FROM DOLT_STASHES;",
				Expected: []sql.Row{{"myStash", "stash@{0}", "main", doltCommit, "Created table"}},
			},
			{
				Query:            "CALL DOLT_STASH('pop', 'myStash');",
				SkipResultsCheck: true,
			},
			{
				Query:    "SELECT * FROM DOLT_STASHES",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "Clearing stash removes all entries in stash list",
		SetUpScript: []string{
			"CREATE TABLE test(pk BIGINT PRIMARY KEY, v varchar(10))",
			"CALL DOLT_COMMIT('-A','-m', 'Created table')",
			"INSERT INTO test VALUES (1, 'a')",
			"CALL DOLT_STASH('push', 'stash1')",
			"INSERT INTO test VALUES (2, 'b')",
			"CALL DOLT_STASH('push', 'stash2')",
			" INSERT INTO test VALUES (3, 'c')",
			"CALL DOLT_STASH('push', 'stash2')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT * FROM dolt_stashes;",
				Expected: []sql.Row{
					{"stash1", "stash@{0}", "main", doltCommit, "Created table"},
					{"stash2", "stash@{0}", "main", doltCommit, "Created table"},
					{"stash2", "stash@{1}", "main", doltCommit, "Created table"},
				},
			},
			{
				Query:            "CALL DOLT_STASH('clear', 'stash2');",
				SkipResultsCheck: true,
			},
			{
				Query: "SELECT * FROM dolt_stashes;",
				Expected: []sql.Row{
					{"stash1", "stash@{0}", "main", doltCommit, "Created table"},
				},
			},
		},
	},
	{
		Name: "Clearing and stashing again",
		SetUpScript: []string{
			"CREATE TABLE test(pk BIGINT PRIMARY KEY, v varchar(10))",
			"CALL DOLT_COMMIT('-A', '-m', 'Created table')",
			"INSERT INTO test VALUES (1, 'a')",
			"CALL DOLT_STASH('push', 'myStash')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT * FROM DOLT_STASHES;",
				Expected: []sql.Row{
					{"myStash", "stash@{0}", "main", doltCommit, "Created table"},
				},
			},
			{
				Query:            "CALL DOLT_STASH('clear', 'myStash');",
				SkipResultsCheck: true,
			},
			{
				Query:    "INSERT INTO test VALUES (2, 'b');",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:            "CALL DOLT_STASH('push', 'myStash');",
				SkipResultsCheck: true,
			},
			{
				Query: "SELECT * FROM DOLT_STASHES;",
				Expected: []sql.Row{
					{"myStash", "stash@{0}", "main", doltCommit, "Created table"},
				},
			},
		},
	},
	{
		Name: "Popping specific stashes",
		SetUpScript: []string{
			"CREATE TABLE test(pk BIGINT PRIMARY KEY, v varchar(10))",
			"CALL DOLT_COMMIT('-A','-m', 'Created table')",
			"INSERT INTO test VALUES (1, 'a')",
			"CALL DOLT_STASH('push', 'myStash')",
			"INSERT INTO test VALUES (2, 'b')",
			"CALL DOLT_STASH('push', 'myStash')",
			"INSERT INTO test VALUES (3, 'c')",
			"CALL DOLT_STASH('push', 'myStash')",
			"INSERT INTO test VALUES (4, 'd')",
			"CALL DOLT_STASH('push', 'myStash')",
			"CALL DOLT_STASH('pop', 'myStash', 'stash@{3}')",
			"CALL DOLT_STASH('pop', 'myStash', 'stash@{1}')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT * FROM test",
				Expected: []sql.Row{
					{1, "a"},
					{3, "c"},
				},
			},
		},
	},
	{
		Name: "Stashing on different branches",
		SetUpScript: []string{
			"CREATE TABLE test(pk BIGINT PRIMARY KEY, v varchar(10))",
			"CALL DOLT_COMMIT('-A', '-m', 'Created table')",
			"INSERT INTO test VALUES (1, 'a')",
			"CALL DOLT_STASH('push', 'myStash')",
			"CALL DOLT_CHECKOUT('-b', 'br1')",
			"INSERT INTO test VALUES (2, 'b')",
			"CALL DOLT_STASH('push', 'myStash')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT * FROM DOLT_STASHES;",
				Expected: []sql.Row{
					{"myStash", "stash@{0}", "br1", doltCommit, "Created table"},
					{"myStash", "stash@{1}", "main", doltCommit, "Created table"},
				},
			},
		},
	},
	{
		Name: "Popping stash onto different branch",
		SetUpScript: []string{
			"CREATE TABLE test(pk BIGINT PRIMARY KEY, v varchar(10))",
			"CALL DOLT_COMMIT('-A', '-m', 'Created table')",
			"CALL DOLT_BRANCH('br1')",
			"INSERT INTO test VALUES (1, 'a')",
			"CALL DOLT_COMMIT('-A', '-m', 'Added a row')",
			"INSERT INTO test VALUES (2, 'b')",
			"CALL DOLT_STASH('push', 'myStash')",
			"CALL DOLT_CHECKOUT('br1')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT * FROM DOLT_STASHES;",
				Expected: []sql.Row{
					{"myStash", "stash@{0}", "main", doltCommit, "Added a row"},
				},
			},
			{
				Query:            "CALL DOLT_STASH('pop', 'myStash');",
				SkipResultsCheck: true,
			},
			{
				Query: "SELECT * FROM TEST;",
				Expected: []sql.Row{
					{2, "b"},
				},
			},
		},
	},
	{
		Name: "Can drop specific stash",
		SetUpScript: []string{
			"CREATE TABLE test(pk BIGINT PRIMARY KEY, v varchar(10))",
			"CALL DOLT_COMMIT('-A', '-m', 'Created table')",
			"INSERT INTO test VALUES (1, 'a')",
			"CALL DOLT_STASH('push', 'myStash')",
			"INSERT INTO test VALUES (2, 'b')",
			"CALL DOLT_COMMIT('-a', '-m', 'Added 2 b')",
			"INSERT INTO test VALUES (3, 'c')",
			"CALL DOLT_STASH('push', 'myStash')",
			"INSERT INTO test VALUES (4, 'd')",
			"CALL DOLT_COMMIT('-a','-m', 'Added 4 d')",
			"INSERT INTO test VALUES (5, 'c')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT * FROM DOLT_STASHES;",
				Expected: []sql.Row{
					{"myStash", "stash@{0}", "main", doltCommit, "Added 2 b"},
					{"myStash", "stash@{1}", "main", doltCommit, "Created table"},
				},
			},
			{
				Query:            "CALL DOLT_STASH('push', 'myStash');",
				SkipResultsCheck: true,
			},
			{
				Query:            "CALL DOLT_STASH('drop', 'myStash', 'stash@{1}');",
				SkipResultsCheck: true,
			},
			{
				Query: "SELECT * FROM DOLT_STASHES;",
				Expected: []sql.Row{
					{"myStash", "stash@{0}", "main", doltCommit, "Added 4 d"},
					{"myStash", "stash@{1}", "main", doltCommit, "Created table"},
				},
			},
		},
	},
	{
		Name: "Can pop into dirty working set without conflict",
		SetUpScript: []string{
			"CREATE TABLE test(pk BIGINT PRIMARY KEY, v varchar(10))",
			"CALL DOLT_COMMIT('-A', '-m', 'Created table')",
			"INSERT INTO test VALUES (1, 'a')",
			"CALL DOLT_STASH('push', 'myStash')",
			"INSERT INTO test VALUES (2, 'b')",
			"CALL DOLT_STASH('pop', 'myStash')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT * FROM test",
				Expected: []sql.Row{
					{1, "a"},
					{2, "b"},
				},
			},
		},
	},
	{
		Name: "Can't pop into dirty working set with conflict",
		SetUpScript: []string{
			"CREATE TABLE test(pk BIGINT PRIMARY KEY, v varchar(10))",
			"CALL DOLT_COMMIT('-A','-m', 'Created table')",
			"INSERT INTO test VALUES (1, 'a')",
			"CALL DOLT_STASH('push', 'myStash')",
			"INSERT INTO test VALUES (1, 'b')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "CALL DOLT_STASH('pop', 'myStash');",
				ExpectedErrStr: "error: Your local changes to the following tables would be overwritten by applying stash 0:\n\t{'test'}\n" +
					"Please commit your changes or stash them before you merge.\nAborting\nThe stash entry is kept in case you need it again.\n",
			},
			{
				Query: "SELECT * FROM DOLT_STASHES;",
				Expected: []sql.Row{
					{"myStash", "stash@{0}", "main", doltCommit, "Created table"},
				},
			},
			{
				Query: "SELECT * FROM test;",
				Expected: []sql.Row{
					{1, "b"},
				},
			},
		},
	},
	{
		Name: "Can stash modified staged and working set of changes",
		SetUpScript: []string{
			"CREATE TABLE test(pk BIGINT PRIMARY KEY, v varchar(10))",
			"CALL DOLT_COMMIT('-A', '-m', 'Created table')",
			"INSERT INTO test VALUES (1, 'a')",
			"CALL DOLT_ADD('.')",
			"INSERT INTO test VALUES (2, 'b')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT * FROM DOLT_STATUS",
				Expected: []sql.Row{
					{"test", true, "modified"},
					{"test", false, "modified"},
				},
			},
			{
				Query:            "CALL DOLT_STASH('push', 'myStash');",
				SkipResultsCheck: true,
			},
			{
				Query:    "SELECT * FROM test;",
				Expected: []sql.Row{},
			},
			{
				Query:            "CALL DOLT_STASH('pop', 'myStash');",
				SkipResultsCheck: true,
			},
			{
				Query: "SELECT * FROM dolt_status;",
				Expected: []sql.Row{
					{"test", false, "modified"},
				},
			},
		},
	},
	{
		Name: "Can use --include-untracked on push",
		SetUpScript: []string{
			"CREATE TABLE test(pk BIGINT PRIMARY KEY, v varchar(10))",
			"CALL DOLT_ADD('.')",
			"CREATE TABLE new(id int primary key)",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT * FROM DOLT_STATUS",
				Expected: []sql.Row{
					{"test", true, "new table"},
					{"new", false, "new table"},
				},
			},
			{
				Query:            "CALL DOLT_STASH('push', 'myStash', '--include-untracked');",
				SkipResultsCheck: true,
			},
			{
				Query:    "SELECT * FROM DOLT_STATUS",
				Expected: []sql.Row{},
			},
			{
				Query:            "CALL DOLT_STASH('pop', 'myStash');",
				SkipResultsCheck: true,
			},
			{
				Query: "SELECT * FROM dolt_status;",
				Expected: []sql.Row{
					{"test", true, "new table"},
					{"new", false, "new table"},
				},
			},
		},
	},
	{
		Name: "Stash with tracked and untracked tables",
		SetUpScript: []string{
			"CREATE TABLE new(i INT PRIMARY KEY)",
			"CALL DOLT_ADD('.')",
			"INSERT INTO new VALUES (1),(2)",
			"CREATE TABLE test(id INT)",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT * FROM DOLT_STATUS",
				Expected: []sql.Row{
					{"new", true, "new table"},
					{"test", false, "new table"},
					{"new", false, "modified"},
				},
			},
			{
				Query:            "CALL DOLT_STASH('push', 'myStash')",
				SkipResultsCheck: true,
			},
			{
				Query: "SELECT * FROM DOLT_STATUS",
				Expected: []sql.Row{
					{"test", false, "new table"},
				},
			},
			{
				Query:            "CALL DOLT_STASH('pop', 'myStash');",
				SkipResultsCheck: true,
			},
			{
				Query: "SELECT * FROM DOLT_STATUS",
				Expected: []sql.Row{
					{"new", true, "new table"},
					{"test", false, "new table"},
				},
			},
		},
	},
	{
		Name: "stashing working set with deleted table and popping it",
		SetUpScript: []string{
			"CREATE TABLE new_tab(id INT PRIMARY KEY)",
			"CALL DOLT_COMMIT('-A', '-m', 'Created table')",
			"DROP TABLE new_tab",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT * FROM DOLT_STATUS;",
				Expected: []sql.Row{
					{"new_tab", false, "deleted"},
				},
			},
			{
				Query:            "CALL DOLT_STASH('push', 'myStash');",
				SkipResultsCheck: true,
			},
			{
				Query: "SHOW TABLES;",
				Expected: []sql.Row{
					{"new_tab"},
				},
			},
			{
				Query:            "CALL DOLT_STASH('pop', 'myStash');",
				SkipResultsCheck: true,
			},
			{
				Query:    "SHOW TABLES;",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "popping stash with deleted table that is deleted already on current head",
		SetUpScript: []string{
			"CREATE TABLE test(pk BIGINT PRIMARY KEY, v varchar(10))",
			"CALL DOLT_COMMIT('-A', '-m', 'Created table')",
			"CALL DOLT_BRANCH('branch1');",
			"CALL DOLT_CHECKOUT('-b', 'branch2');",
			"DROP TABLE test;",
			"CALL DOLT_COMMIT('-A','-m','Dropped test');",
			"CALL DOLT_CHECKOUT('branch1');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SHOW TABLES;",
				Expected: []sql.Row{
					{"test"},
				},
			},
			{
				Query:    "DROP TABLE test;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:            "CALL DOLT_STASH('push', 'myStash');",
				SkipResultsCheck: true,
			},
			{
				Query:    "CALL DOLT_CHECKOUT('branch2');",
				Expected: []sql.Row{{0, "Switched to branch 'branch2'"}},
			},
			{
				Query:            "CALL DOLT_STASH('pop', 'myStash');",
				SkipResultsCheck: true,
			},
			{
				Query:    "SELECT * FROM DOLT_STATUS",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "popping stash with deleted table that the same table exists on current head",
		SetUpScript: []string{
			"CREATE TABLE test(pk BIGINT PRIMARY KEY, v varchar(10))",
			"CALL DOLT_COMMIT('-A', '-m', 'Created table')",
			"CALL DOLT_BRANCH('branch1');",
			"CALL DOLT_BRANCH('branch2');",
			"CALL DOLT_CHECKOUT('branch1');",
			"DROP TABLE test;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "CALL DOLT_STASH('push', 'myStash');",
				SkipResultsCheck: true,
			},
			{
				Query:    "CALL DOLT_CHECKOUT('branch2');",
				Expected: []sql.Row{{0, "Switched to branch 'branch2'"}},
			},
			{
				Query:            "CALL DOLT_STASH('pop', 'myStash');",
				SkipResultsCheck: true,
			},
			{
				Query: "SELECT * FROM DOLT_STATUS",
				Expected: []sql.Row{
					{"test", false, "deleted"},
				},
			},
		},
	},
	{
		Name: "popping stash with deleted table that different table with same name on current head gives conflict",
		SetUpScript: []string{
			"CREATE TABLE test(pk BIGINT PRIMARY KEY, v varchar(10))",
			"CALL DOLT_COMMIT('-A', '-m', 'Created table')",
			"CALL DOLT_BRANCH('branch1')",
			"CALL DOLT_BRANCH('branch2')",
			"CALL DOLT_CHECKOUT('branch1')",
			"DROP TABLE test",
			"CALL DOLT_STASH('push', 'myStash')",
			"CALL DOLT_CHECKOUT('branch2')",
			"DROP TABLE test",
			"CREATE TABLE test (id BIGINT PRIMARY KEY)",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "CALL DOLT_STASH('pop', 'myStash');",
				ExpectedErrStr: "merge aborted: schema conflict found for table test \n " +
					"please resolve schema conflicts before merging: \n" +
					"\ttable was modified in one branch and deleted in the other",
			},
		},
	},
	{
		Name: "popping stash with added table with PK on current head with the exact same table is added already",
		SetUpScript: []string{
			"CREATE TABLE test(pk BIGINT PRIMARY KEY, v varchar(10))",
			"CALL DOLT_COMMIT('-A', '-m', 'Created table')",
			"CALL DOLT_BRANCH('branch1')",
			"CALL DOLT_CHECKOUT('-b',  'branch2')",
			"CREATE TABLE new_test(id INT PRIMARY KEY)",
			"INSERT INTO new_test VALUES (1)",
			"CALL DOLT_COMMIT('-A', '-m', 'Created new_test')",
			"CALL DOLT_CHECKOUT('branch1')",
			"CREATE TABLE new_test(id INT PRIMARY KEY)",
			"INSERT INTO new_test VALUES (1)",
			"CALL DOLT_ADD('.')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "CALL DOLT_STASH('push', 'myStash');",
				SkipResultsCheck: true,
			},
			{
				Query:    "CALL DOLT_CHECKOUT('branch2');",
				Expected: []sql.Row{{0, "Switched to branch 'branch2'"}},
			},
			{
				Query:            "CALL DOLT_STASH('pop', 'myStash');",
				SkipResultsCheck: true,
			},
			{
				Query:    "SELECT * FROM DOLT_STATUS",
				Expected: []sql.Row{},
			},
		},
	},
}
