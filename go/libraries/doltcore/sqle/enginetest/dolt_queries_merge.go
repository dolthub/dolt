// Copyright 2022 Dolthub, Inc.
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
	"github.com/dolthub/go-mysql-server/sql/plan"

	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dfunctions"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

// ===== MERGE =====

var MergeScripts = []queries.ScriptTest{
	{
		Name: "CALL DOLT_MERGE ff correctly works with autocommit off",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key)",
			"call DOLT_ADD('.')",
			"INSERT INTO test VALUES (0),(1),(2);",
			"SET autocommit = 0",
			"SELECT DOLT_COMMIT('-a', '-m', 'Step 1');",
			"SELECT DOLT_CHECKOUT('-b', 'feature-branch')",
			"INSERT INTO test VALUES (3);",
			"UPDATE test SET pk=1000 WHERE pk=0;",
			"CALL DOLT_ADD('.');",
			"SELECT DOLT_COMMIT('-a', '-m', 'this is a ff');",
			"SELECT DOLT_CHECKOUT('main');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				// FF-Merge
				Query:    "CALL DOLT_MERGE('feature-branch')",
				Expected: []sql.Row{{1, 0}},
			},
			{
				Query:    "SELECT is_merging, source, target, unmerged_tables FROM DOLT_MERGE_STATUS;",
				Expected: []sql.Row{{false, nil, nil, nil}},
			},
			{
				Query:    "SELECT * from dolt_status",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT DOLT_CHECKOUT('-b', 'new-branch')",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "INSERT INTO test VALUES (4)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
		},
	},
	{
		Name: "CALL DOLT_MERGE no-ff correctly works with autocommit off",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key)",
			"call DOLT_ADD('.')",
			"INSERT INTO test VALUES (0),(1),(2);",
			"SET autocommit = 0",
			"SELECT DOLT_COMMIT('-a', '-m', 'Step 1', '--date', '2022-08-06T12:00:00');",
			"SELECT DOLT_CHECKOUT('-b', 'feature-branch')",
			"INSERT INTO test VALUES (3);",
			"UPDATE test SET pk=1000 WHERE pk=0;",
			"SELECT DOLT_COMMIT('-a', '-m', 'this is a ff', '--date', '2022-08-06T12:00:01');",
			"SELECT DOLT_CHECKOUT('main');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				// No-FF-Merge
				Query:    "CALL DOLT_MERGE('feature-branch', '-no-ff', '-m', 'this is a no-ff')",
				Expected: []sql.Row{{1, 0}},
			},
			{
				Query:    "SELECT is_merging, source, target, unmerged_tables FROM DOLT_MERGE_STATUS;",
				Expected: []sql.Row{{false, nil, nil, nil}},
			},
			{
				Query:    "SELECT * from dolt_status",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT COUNT(*) FROM dolt_log",
				Expected: []sql.Row{{5}}, // includes the merge commit created by no-ff and setup commits
			},
			{
				Query:    "select message from dolt_log order by date DESC LIMIT 1;",
				Expected: []sql.Row{{"this is a no-ff"}}, // includes the merge commit created by no-ff
			},
			{
				Query:    "SELECT DOLT_CHECKOUT('-b', 'other-branch')",
				Expected: []sql.Row{{0}},
			},
		},
	},
	{
		Name: "CALL DOLT_MERGE without conflicts correctly works with autocommit off with commit flag",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key)",
			"CALL DOLT_ADD('.')",
			"INSERT INTO test VALUES (0),(1),(2);",
			"SET autocommit = 0",
			"SELECT DOLT_COMMIT('-a', '-m', 'Step 1', '--date', '2022-08-06T12:00:01');",
			"SELECT DOLT_CHECKOUT('-b', 'feature-branch')",
			"INSERT INTO test VALUES (3);",
			"UPDATE test SET pk=1000 WHERE pk=0;",
			"SELECT DOLT_COMMIT('-a', '-m', 'this is a normal commit', '--date', '2022-08-06T12:00:02');",
			"SELECT DOLT_CHECKOUT('main');",
			"INSERT INTO test VALUES (5),(6),(7);",
			"SELECT DOLT_COMMIT('-a', '-m', 'add some more values', '--date', '2022-08-06T12:00:03');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('feature-branch', '-m', 'this is a merge', '--commit')",
				Expected: []sql.Row{{0, 0}},
			},
			{
				Query:    "SELECT is_merging, source, target, unmerged_tables FROM DOLT_MERGE_STATUS;",
				Expected: []sql.Row{{false, nil, nil, nil}},
			},
			{
				Query:    "SELECT COUNT(*) from dolt_status",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "SELECT COUNT(*) FROM dolt_log",
				Expected: []sql.Row{{6}},
			},
			{
				Query:    "select message from dolt_log where date > '2022-08-08' order by date DESC LIMIT 1;",
				Expected: []sql.Row{{"this is a merge"}},
			},
		},
	},
	{
		Name: "CALL DOLT_MERGE without conflicts correctly works with autocommit off and no commit flag",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key)",
			"CALL DOLT_ADD('.')",
			"INSERT INTO test VALUES (0),(1),(2);",
			"SET autocommit = 0",
			"SELECT DOLT_COMMIT('-a', '-m', 'Step 1', '--date', '2022-08-06T12:00:01');",
			"SELECT DOLT_CHECKOUT('-b', 'feature-branch')",
			"INSERT INTO test VALUES (3);",
			"UPDATE test SET pk=1000 WHERE pk=0;",
			"SELECT DOLT_COMMIT('-a', '-m', 'this is a normal commit', '--date', '2022-08-06T12:00:02');",
			"SELECT DOLT_CHECKOUT('main');",
			"INSERT INTO test VALUES (5),(6),(7);",
			"SELECT DOLT_COMMIT('-a', '-m', 'add some more values', '--date', '2022-08-06T12:00:03');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('feature-branch', '-m', 'this is a merge', '--no-commit')",
				Expected: []sql.Row{{0, 0}},
			},
			{
				Query:    "SELECT is_merging, source, target, unmerged_tables FROM DOLT_MERGE_STATUS;",
				Expected: []sql.Row{{true, "feature-branch", "refs/heads/main", ""}},
			},
			{
				Query:    "SELECT COUNT(*) from dolt_status",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT COUNT(*) FROM dolt_log",
				Expected: []sql.Row{{4}},
			},
			{
				// careful to filter out the initial commit, which will be later than the ones above
				Query:    "select message from dolt_log where date < '2022-08-08' order by date DESC LIMIT 1;",
				Expected: []sql.Row{{"add some more values"}},
			},
			{
				Query:       "SELECT DOLT_CHECKOUT('-b', 'other-branch')",
				ExpectedErr: dsess.ErrWorkingSetChanges,
			},
		},
	},
	{
		Name: "CALL DOLT_MERGE with conflicts can be correctly resolved when autocommit is off",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key, val int)",
			"call DOLT_ADD('.')",
			"INSERT INTO test VALUES (0, 0)",
			"SET autocommit = 0",
			"SELECT DOLT_COMMIT('-a', '-m', 'Step 1', '--date', '2022-08-06T12:00:01');",
			"SELECT DOLT_CHECKOUT('-b', 'feature-branch')",
			"INSERT INTO test VALUES (1, 1);",
			"UPDATE test SET val=1000 WHERE pk=0;",
			"SELECT DOLT_COMMIT('-a', '-m', 'this is a normal commit', '--date', '2022-08-06T12:00:02');",
			"SELECT DOLT_CHECKOUT('main');",
			"UPDATE test SET val=1001 WHERE pk=0;",
			"SELECT DOLT_COMMIT('-a', '-m', 'update a value', '--date', '2022-08-06T12:00:03');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('feature-branch', '-m', 'this is a merge')",
				Expected: []sql.Row{{0, 1}},
			},
			{
				Query:    "SELECT is_merging, source, target, unmerged_tables FROM DOLT_MERGE_STATUS;",
				Expected: []sql.Row{{true, "feature-branch", "refs/heads/main", "test"}},
			},
			{
				Query:    "SELECT * from dolt_status",
				Expected: []sql.Row{{"test", true, "modified"}, {"test", false, "conflict"}},
			},
			{
				Query:    "SELECT COUNT(*) FROM dolt_log",
				Expected: []sql.Row{{4}},
			},
			{
				Query:    "select message from dolt_log where date < '2022-08-08' order by date DESC LIMIT 1;",
				Expected: []sql.Row{{"update a value"}},
			},
			{
				Query:       "SELECT DOLT_CHECKOUT('-b', 'other-branch')",
				ExpectedErr: dsess.ErrWorkingSetChanges,
			},
			{
				Query:    "SELECT COUNT(*) FROM dolt_conflicts",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "DELETE FROM dolt_conflicts_test",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * from test ORDER BY pk",
				Expected: []sql.Row{{0, 1001}, {1, 1}},
			},
		},
	},
	{
		Name: "CALL DOLT_MERGE ff & squash correctly works with autocommit off",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key)",
			"call DOLT_ADD('.')",
			"INSERT INTO test VALUES (0),(1),(2);",
			"SET autocommit = 0",
			"SELECT DOLT_COMMIT('-a', '-m', 'Step 1');",
			"SELECT DOLT_CHECKOUT('-b', 'feature-branch')",
			"INSERT INTO test VALUES (3);",
			"UPDATE test SET pk=1000 WHERE pk=0;",
			"SELECT DOLT_COMMIT('-a', '-m', 'this is a ff');",
			"SELECT DOLT_CHECKOUT('main');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('feature-branch', '--squash')",
				Expected: []sql.Row{{1, 0}},
			},
			{
				Query:    "SELECT count(*) from dolt_status",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT COUNT(*) FROM dolt_log",
				Expected: []sql.Row{{3}},
			},
			{
				Query:    "SELECT * FROM test order by pk",
				Expected: []sql.Row{{1}, {2}, {3}, {1000}},
			},
		},
	},
	{
		Name: "CALL DOLT_MERGE ff & squash with a checkout in between",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key)",
			"call DOLT_ADD('.')",
			"INSERT INTO test VALUES (0),(1),(2);",
			"SET autocommit = 0",
			"SELECT DOLT_COMMIT('-a', '-m', 'Step 1');",
			"SELECT DOLT_CHECKOUT('-b', 'feature-branch')",
			"INSERT INTO test VALUES (3);",
			"UPDATE test SET pk=1000 WHERE pk=0;",
			"SELECT DOLT_COMMIT('-a', '-m', 'this is a ff');",
			"SELECT DOLT_CHECKOUT('main');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('feature-branch', '--squash')",
				Expected: []sql.Row{{1, 0}},
			},
			{
				Query:       "SELECT DOLT_CHECKOUT('-b', 'other')",
				ExpectedErr: dsess.ErrWorkingSetChanges,
			},
			{
				Query:    "SELECT * FROM test order by pk",
				Expected: []sql.Row{{1}, {2}, {3}, {1000}},
			},
		},
	},
	{
		Name: "CALL DOLT_MERGE ff",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key)",
			"CALL DOLT_ADD('.')",
			"INSERT INTO test VALUES (0),(1),(2);",
			"SELECT DOLT_COMMIT('-a', '-m', 'Step 1');",
			"SELECT DOLT_CHECKOUT('-b', 'feature-branch')",
			"INSERT INTO test VALUES (3);",
			"UPDATE test SET pk=1000 WHERE pk=0;",
			"SELECT DOLT_COMMIT('-a', '-m', 'this is a ff');",
			"SELECT DOLT_CHECKOUT('main');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				// FF-Merge
				Query:    "CALL DOLT_MERGE('feature-branch')",
				Expected: []sql.Row{{1, 0}},
			},
			{
				Query:    "SELECT is_merging, source, target, unmerged_tables FROM DOLT_MERGE_STATUS;",
				Expected: []sql.Row{{false, nil, nil, nil}},
			},
			{
				Query:    "SELECT * from dolt_status",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT DOLT_CHECKOUT('-b', 'new-branch')",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "INSERT INTO test VALUES (4)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
		},
	},
	{
		Name: "CALL DOLT_MERGE no-ff",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key)",
			"CALL DOLT_ADD('.')",
			"INSERT INTO test VALUES (0),(1),(2);",
			"SELECT DOLT_COMMIT('-a', '-m', 'Step 1');",
			"SELECT DOLT_CHECKOUT('-b', 'feature-branch')",
			"INSERT INTO test VALUES (3);",
			"UPDATE test SET pk=1000 WHERE pk=0;",
			"SELECT DOLT_COMMIT('-a', '-m', 'this is a ff');",
			"SELECT DOLT_CHECKOUT('main');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				// No-FF-Merge
				Query:    "CALL DOLT_MERGE('feature-branch', '-no-ff', '-m', 'this is a no-ff')",
				Expected: []sql.Row{{1, 0}},
			},
			{
				Query:    "SELECT is_merging, source, target, unmerged_tables FROM DOLT_MERGE_STATUS;",
				Expected: []sql.Row{{false, nil, nil, nil}},
			},
			{
				Query:    "SELECT * from dolt_status",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT COUNT(*) FROM dolt_log",
				Expected: []sql.Row{{5}}, // includes the merge commit created by no-ff and setup commits
			},
			{
				Query:    "select message from dolt_log order by date DESC LIMIT 1;",
				Expected: []sql.Row{{"this is a no-ff"}}, // includes the merge commit created by no-ff
			},
			{
				Query:    "SELECT DOLT_CHECKOUT('-b', 'other-branch')",
				Expected: []sql.Row{{0}},
			},
		},
	},
	{
		Name: "CALL DOLT_MERGE with no conflicts works",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key)",
			"CALL DOLT_ADD('.')",
			"INSERT INTO test VALUES (0),(1),(2);",
			"SELECT DOLT_COMMIT('-a', '-m', 'Step 1', '--date', '2022-08-06T12:00:00');",
			"SELECT DOLT_CHECKOUT('-b', 'feature-branch')",
			"INSERT INTO test VALUES (3);",
			"UPDATE test SET pk=1000 WHERE pk=0;",
			"SELECT DOLT_COMMIT('-a', '-m', 'this is a normal commit', '--date', '2022-08-06T12:00:01');",
			"SELECT DOLT_CHECKOUT('main');",
			"INSERT INTO test VALUES (5),(6),(7);",
			"SELECT DOLT_COMMIT('-a', '-m', 'add some more values', '--date', '2022-08-06T12:00:02');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "CALL DOLT_MERGE('feature-branch', '--no-commit', '--commit')",
				ExpectedErrStr: "cannot define both 'commit' and 'no-commit' flags at the same time",
			},
			{
				Query:    "CALL DOLT_MERGE('feature-branch', '-m', 'this is a merge')",
				Expected: []sql.Row{{0, 0}},
			},
			{
				Query:    "SELECT COUNT(*) from dolt_status",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "SELECT COUNT(*) FROM dolt_log",
				Expected: []sql.Row{{6}}, // includes the merge commit and a new commit created by successful merge
			},
			{
				Query:    "select message from dolt_log where date > '2022-08-08' order by date DESC LIMIT 1;",
				Expected: []sql.Row{{"this is a merge"}},
			},
		},
	},
	{
		Name: "CALL DOLT_MERGE with no conflicts works with no-commit flag",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key)",
			"CALL DOLT_ADD('.')",
			"INSERT INTO test VALUES (0),(1),(2);",
			"SELECT DOLT_COMMIT('-a', '-m', 'Step 1', '--date', '2022-08-06T12:00:00');",
			"SELECT DOLT_CHECKOUT('-b', 'feature-branch')",
			"INSERT INTO test VALUES (3);",
			"UPDATE test SET pk=1000 WHERE pk=0;",
			"SELECT DOLT_COMMIT('-a', '-m', 'this is a normal commit', '--date', '2022-08-06T12:00:01');",
			"SELECT DOLT_CHECKOUT('main');",
			"INSERT INTO test VALUES (5),(6),(7);",
			"SELECT DOLT_COMMIT('-a', '-m', 'add some more values', '--date', '2022-08-06T12:00:02');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('feature-branch', '-m', 'this is a merge', '--no-commit')",
				Expected: []sql.Row{{0, 0}},
			},
			{
				Query:    "SELECT COUNT(*) from dolt_status",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT COUNT(*) FROM dolt_log",
				Expected: []sql.Row{{4}},
			},
			{
				Query:    "select message from dolt_log where date < '2022-08-08' order by date DESC LIMIT 1;",
				Expected: []sql.Row{{"add some more values"}},
			},
			{
				Query:    "SELECT DOLT_CHECKOUT('-b', 'other-branch')",
				Expected: []sql.Row{{0}},
			},
		},
	},
	{
		Name: "CALL DOLT_MERGE with conflict is queryable and committable with dolt_allow_commit_conflicts on",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key, val int)",
			"CALL DOLT_ADD('.')",
			"INSERT INTO test VALUES (0, 0)",
			"SELECT DOLT_COMMIT('-a', '-m', 'Step 1');",
			"SELECT DOLT_CHECKOUT('-b', 'feature-branch')",
			"INSERT INTO test VALUES (1, 1);",
			"UPDATE test SET val=1000 WHERE pk=0;",
			"SELECT DOLT_COMMIT('-a', '-m', 'this is a normal commit');",
			"SELECT DOLT_CHECKOUT('main');",
			"UPDATE test SET val=1001 WHERE pk=0;",
			"SELECT DOLT_COMMIT('-a', '-m', 'update a value');",
			"set dolt_allow_commit_conflicts = on",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('feature-branch')",
				Expected: []sql.Row{{0, 1}},
			},
			{
				Query:    "SELECT count(*) from dolt_conflicts_test",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT DOLT_MERGE('--abort')",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "SELECT is_merging, source, target, unmerged_tables FROM DOLT_MERGE_STATUS;",
				Expected: []sql.Row{{false, nil, nil, nil}},
			},
			{
				Query:    "SELECT * FROM test",
				Expected: []sql.Row{{0, 1001}},
			},
			{
				Query:    "SELECT count(*) from dolt_conflicts_test",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "SELECT count(*) from dolt_status",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "SET dolt_allow_commit_conflicts = 0",
				Expected: []sql.Row{{}},
			},
			{
				Query:          "SELECT DOLT_MERGE('feature-branch')",
				ExpectedErrStr: dsess.ErrUnresolvedConflictsCommit.Error(),
			},
			{
				Query:    "SELECT count(*) from dolt_conflicts_test", // transaction has been rolled back, 0 results
				Expected: []sql.Row{{0}},
			},
		},
	},
	{
		Name: "CALL DOLT_MERGE with conflicts can be aborted when autocommit is off",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key, val int)",
			"CALL DOLT_ADD('.')",
			"INSERT INTO test VALUES (0, 0)",
			"SET autocommit = 0",
			"SELECT DOLT_COMMIT('-a', '-m', 'Step 1');",
			"SELECT DOLT_CHECKOUT('-b', 'feature-branch')",
			"INSERT INTO test VALUES (1, 1);",
			"UPDATE test SET val=1000 WHERE pk=0;",
			"SELECT DOLT_COMMIT('-a', '-m', 'this is a normal commit');",
			"SELECT DOLT_CHECKOUT('main');",
			"UPDATE test SET val=1001 WHERE pk=0;",
			"SELECT DOLT_COMMIT('-a', '-m', 'update a value');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('feature-branch', '-m', 'this is a merge')",
				Expected: []sql.Row{{0, 1}},
			},
			{
				Query:    "SELECT * from dolt_status",
				Expected: []sql.Row{{"test", true, "modified"}, {"test", false, "conflict"}},
			},
			{
				Query:    "SELECT COUNT(*) FROM dolt_conflicts",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT DOLT_MERGE('--abort')",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "SELECT is_merging, source, target, unmerged_tables FROM DOLT_MERGE_STATUS;",
				Expected: []sql.Row{{false, nil, nil, nil}},
			},
			{
				Query:    "SELECT * from dolt_status",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT COUNT(*) FROM dolt_log",
				Expected: []sql.Row{{4}},
			},
			{
				Query:    "SELECT * FROM test ORDER BY pk",
				Expected: []sql.Row{{0, 1001}},
			},
			{
				Query:    "SELECT DOLT_CHECKOUT('-b', 'other-branch')",
				Expected: []sql.Row{{0}},
			},
		},
	},
	{
		Name: "CALL DOLT_MERGE complains when a merge overrides local changes",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key, val int)",
			"CALL DOLT_ADD('.')",
			"INSERT INTO test VALUES (0, 0)",
			"SET autocommit = 0",
			"SELECT DOLT_COMMIT('-a', '-m', 'Step 1');",
			"SELECT DOLT_CHECKOUT('-b', 'feature-branch')",
			"INSERT INTO test VALUES (1, 1);",
			"UPDATE test SET val=1000 WHERE pk=0;",
			"SELECT DOLT_COMMIT('-a', '-m', 'this is a normal commit');",
			"SELECT DOLT_CHECKOUT('main');",
			"UPDATE test SET val=1001 WHERE pk=0;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:       "CALL DOLT_MERGE('feature-branch', '-m', 'this is a merge')",
				ExpectedErr: dfunctions.ErrUncommittedChanges,
			},
			{
				Query:    "SELECT is_merging, source, target, unmerged_tables FROM DOLT_MERGE_STATUS;",
				Expected: []sql.Row{{false, nil, nil, nil}},
			},
		},
	},
	{
		Name: "Drop and add primary key on two branches converges to same schema",
		SetUpScript: []string{
			"create table t1 (i int);",
			"call dolt_add('.');",
			"call dolt_commit('-am', 't1 table')",
			"call dolt_checkout('-b', 'b1')",
			"alter table t1 add primary key(i)",
			"alter table t1 drop primary key",
			"alter table t1 add primary key(i)",
			"alter table t1 drop primary key",
			"alter table t1 add primary key(i)",
			"call dolt_commit('-am', 'b1 primary key changes')",
			"call dolt_checkout('main')",
			"alter table t1 add primary key(i)",
			"call dolt_commit('-am', 'main primary key change')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_merge('b1')",
				Expected: []sql.Row{{0, 0}},
			},
			{
				Query:    "select count(*) from dolt_conflicts",
				Expected: []sql.Row{{0}},
			},
		},
	},
	{
		Name: "Constraint violations are persisted",
		SetUpScript: []string{
			"set dolt_force_transaction_commit = on;",
			"CREATE table parent (pk int PRIMARY KEY, col1 int);",
			"CREATE table child (pk int PRIMARY KEY, parent_fk int, FOREIGN KEY (parent_fk) REFERENCES parent(pk));",
			"CREATE table other (pk int);",
			"CALL DOLT_ADD('.')",
			"INSERT INTO parent VALUES (1, 1), (2, 2);",
			"CALL DOLT_COMMIT('-am', 'setup');",
			"CALL DOLT_BRANCH('branch1');",
			"CALL DOLT_BRANCH('branch2');",
			"DELETE FROM parent where pk = 1;",
			"CALL DOLT_COMMIT('-am', 'delete parent 1');",
			"CALL DOLT_CHECKOUT('branch1');",
			"INSERT INTO CHILD VALUES (1, 1);",
			"CALL DOLT_COMMIT('-am', 'insert child of parent 1');",
			"CALL DOLT_CHECKOUT('main');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('branch1');",
				Expected: []sql.Row{{0, 1}},
			},
			{
				Query:    "SELECT violation_type, pk, parent_fk from dolt_constraint_violations_child;",
				Expected: []sql.Row{{uint64(merge.CvType_ForeignKey), 1, 1}},
			},
		},
	},
	{
		// from constraint-violations.bats
		Name: "ancestor contains fk, main parent remove with backup, other child add, restrict",
		SetUpScript: []string{
			"CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));",
			"CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1));",
			"CALL DOLT_ADD('.')",
			"INSERT INTO parent VALUES (10, 1), (20, 2), (30, 2);",
			"INSERT INTO child VALUES (1, 1);",
			"CALL DOLT_COMMIT('-am', 'MC1');",
			"CALL DOLT_BRANCH('other');",
			"DELETE from parent WHERE pk = 20;",
			"CALL DOLT_COMMIT('-am', 'MC2');",

			"CALL DOLT_CHECKOUT('other');",
			"INSERT INTO child VALUES (2, 2);",
			"CALL DOLT_COMMIT('-am', 'OC1');",
			"CALL DOLT_CHECKOUT('main');",
			"set DOLT_FORCE_TRANSACTION_COMMIT = on;",
			"CALL DOLT_MERGE('other');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * from dolt_constraint_violations",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * from dolt_constraint_violations_parent",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * from dolt_constraint_violations_child",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * from parent;",
				Expected: []sql.Row{{10, 1}, {30, 2}},
			},
			{
				Query:    "SELECT * from child;",
				Expected: []sql.Row{{1, 1}, {2, 2}},
			},
		},
	},
	// unique indexes
	{
		Name: "unique keys, insert violation",
		SetUpScript: []string{
			"SET dolt_force_transaction_commit = on;",
			"CREATE TABLE t (pk int PRIMARY KEY, col1 int UNIQUE);",
			"CALL dolt_add('.')",
			"CALL DOLT_COMMIT('-am', 'create table');",

			"CALL DOLT_CHECKOUT('-b', 'right');",
			"INSERT INTO t VALUES (2, 1), (3, 3);",
			"CALL DOLT_COMMIT('-am', 'right insert');",

			"CALL DOLT_CHECKOUT('main');",
			"INSERT INTO t values (1, 1), (4, 4);",
			"CALL DOLT_COMMIT('-am', 'left insert');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('right');",
				Expected: []sql.Row{{0, 1}},
			},
			{
				Query:    "SELECT * from t;",
				Expected: []sql.Row{{1, 1}, {2, 1}, {3, 3}, {4, 4}},
			},
			{
				Query:    "SELECT violation_type, pk, col1 from dolt_constraint_violations_t;",
				Expected: []sql.Row{{uint64(merge.CvType_UniqueIndex), 1, 1}, {uint64(merge.CvType_UniqueIndex), 2, 1}},
			},
			{
				Query:    "SELECT is_merging, source, target, unmerged_tables FROM DOLT_MERGE_STATUS;",
				Expected: []sql.Row{{true, "right", "refs/heads/main", "t"}},
			},
		},
	},
	{
		Name: "unique keys, update violation from left",
		SetUpScript: []string{
			"SET dolt_force_transaction_commit = on;",
			"CREATE TABLE t (pk int PRIMARY KEY, col1 int UNIQUE);",
			"CALL DOLT_ADD('.')",
			"INSERT INTO t VALUES (1, 1), (2, 2);",
			"CALL DOLT_COMMIT('-am', 'create table');",

			"CALL DOLT_CHECKOUT('-b', 'right');",
			"INSERT INTO t values (3, 3);",
			"CALL DOLT_COMMIT('-am', 'right insert');",

			"CALL DOLT_CHECKOUT('main');",
			"UPDATE t SET col1 = 3 where pk = 2;",
			"CALL DOLT_COMMIT('-am', 'left insert');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('right');",
				Expected: []sql.Row{{0, 1}},
			},
			{
				Query:    "SELECT * from t;",
				Expected: []sql.Row{{1, 1}, {2, 3}, {3, 3}},
			},
			{
				Query:    "SELECT violation_type, pk, col1 from dolt_constraint_violations_t;",
				Expected: []sql.Row{{uint64(merge.CvType_UniqueIndex), 2, 3}, {uint64(merge.CvType_UniqueIndex), 3, 3}},
			},
		},
	},
	{
		Name: "unique keys, update violation from right",
		SetUpScript: []string{
			"SET dolt_force_transaction_commit = on;",
			"CREATE TABLE t (pk int PRIMARY KEY, col1 int UNIQUE);",
			"CALL DOLT_ADD('.')",
			"INSERT INTO t VALUES (1, 1), (2, 2);",
			"CALL DOLT_COMMIT('-am', 'create table');",

			"CALL DOLT_CHECKOUT('-b', 'right');",
			"UPDATE t SET col1 = 3 where pk = 2;",
			"CALL DOLT_COMMIT('-am', 'right insert');",

			"CALL DOLT_CHECKOUT('main');",
			"INSERT INTO t values (3, 3);",
			"CALL DOLT_COMMIT('-am', 'left insert');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('right');",
				Expected: []sql.Row{{0, 1}},
			},
			{
				Query:    "SELECT * from t;",
				Expected: []sql.Row{{1, 1}, {2, 3}, {3, 3}},
			},
			{
				Query:    "SELECT violation_type, pk, col1 from dolt_constraint_violations_t;",
				Expected: []sql.Row{{uint64(merge.CvType_UniqueIndex), 2, 3}, {uint64(merge.CvType_UniqueIndex), 3, 3}},
			},
		},
	},
	{
		Name: "cell-wise merges can result in a unique key violation",
		SetUpScript: []string{
			"SET dolt_force_transaction_commit = on;",
			"CREATE TABLE t (pk int PRIMARY KEY, col1 int, col2 int, UNIQUE col1_col2_u (col1, col2));",
			"CALL DOLT_ADD('.')",
			"INSERT INTO T VALUES (1, 1, 1), (2, NULL, NULL);",
			"CALL DOLT_COMMIT('-am', 'setup');",

			"CALL DOLT_CHECKOUT('-b', 'right');",
			"UPDATE t SET col2 = 1 where pk = 2;",
			"CALL DOLT_COMMIT('-am', 'right edit');",

			"CALL DOLT_CHECKOUT('main');",
			"UPDATE t SET col1 = 1 where pk = 2;",
			"CALL DOLT_COMMIT('-am', 'left edit');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('right');",
				Expected: []sql.Row{{0, 1}},
			},
			{
				Query:    "SELECT * from t;",
				Expected: []sql.Row{{1, 1, 1}, {2, 1, 1}},
			},
			{
				Query:    "SELECT violation_type, pk, col1, col2 from dolt_constraint_violations_t;",
				Expected: []sql.Row{{uint64(merge.CvType_UniqueIndex), 1, 1, 1}, {uint64(merge.CvType_UniqueIndex), 2, 1, 1}},
			},
		},
	},
	// Behavior between new and old format diverges in the case where right adds
	// a unique key constraint and resolves existing violations.
	// In the old format, because the violations exist on the left the merge is aborted.
	// In the new format, the merge can be completed successfully without error.
	// See MergeArtifactScripts and OldFormatMergeConflictsAndCVsScripts
	{
		Name: "left adds a unique key constraint and resolves existing violations",
		SetUpScript: []string{
			"SET dolt_force_transaction_commit = on;",
			"CREATE TABLE t (pk int PRIMARY KEY, col1 int);",
			"CALL DOLT_ADD('.')",
			"INSERT INTO t VALUES (1, 1), (2, 1);",
			"CALL DOLT_COMMIT('-am', 'table and data');",

			"CALL DOLT_CHECKOUT('-b', 'right');",
			"INSERT INTO t VALUES (3, 3);",
			"CALL DOLT_COMMIT('-am', 'right edit');",

			"CALL DOLT_CHECKOUT('main');",
			"UPDATE t SET col1 = 2 where pk = 2;",
			"ALTER TABLE t ADD UNIQUE col1_uniq (col1);",
			"CALL DOLT_COMMIT('-am', 'left adds a unique index');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('right');",
				Expected: []sql.Row{{0, 0}},
			},
			{
				Query:    "SELECT * from t;",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}},
			},
		},
	},
	{
		Name: "insert two tables with the same name and different schema",
		SetUpScript: []string{
			"SET dolt_allow_commit_conflicts = on;",
			"CALL DOLT_CHECKOUT('-b', 'other');",
			"CREATE TABLE t (pk int PRIMARY key, col1 int, extracol int);",
			"CALL DOLT_ADD('.')",
			"INSERT into t VALUES (1, 1, 1);",
			"CALL DOLT_COMMIT('-am', 'right');",

			"CALL DOLT_CHECKOUT('main');",
			"CREATE TABLE t (pk int PRIMARY key, col1 int);",
			"CALL DOLT_ADD('.')",
			"INSERT into t VALUES (2, 2);",
			"CALL DOLT_COMMIT('-am', 'left');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "CALL DOLT_MERGE('other');",
				ExpectedErrStr: "table with same name added in 2 commits can't be merged",
			},
		},
	},
	{
		Name: "insert two tables with the same name and schema that don't conflict",
		SetUpScript: []string{
			"SET dolt_allow_commit_conflicts = on;",
			"CALL DOLT_CHECKOUT('-b', 'other');",
			"CREATE TABLE t (pk int PRIMARY key, col1 int);",
			"CALL DOLT_ADD('.')",
			"INSERT into t VALUES (1, 1);",
			"CALL DOLT_COMMIT('-am', 'right');",

			"CALL DOLT_CHECKOUT('main');",
			"CREATE TABLE t (pk int PRIMARY key, col1 int);",
			"CALL DOLT_ADD('.')",
			"INSERT into t VALUES (2, 2);",
			"CALL DOLT_COMMIT('-am', 'left');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('other');",
				Expected: []sql.Row{{0, 0}},
			},
			{
				Query:    "SELECT * from t;",
				Expected: []sql.Row{{1, 1}, {2, 2}},
			},
		},
	},
	{
		Name: "insert two tables with the same name and schema that conflict",
		SetUpScript: []string{
			"SET dolt_allow_commit_conflicts = on;",
			"CALL DOLT_CHECKOUT('-b', 'other');",
			"CREATE TABLE t (pk int PRIMARY key, col1 int);",
			"CALL DOLT_ADD('.')",
			"INSERT into t VALUES (1, -1);",
			"CALL DOLT_COMMIT('-am', 'right');",

			"CALL DOLT_CHECKOUT('main');",
			"CREATE TABLE t (pk int PRIMARY key, col1 int);",
			"CALL DOLT_ADD('.')",
			"INSERT into t VALUES (1, 1);",
			"CALL DOLT_COMMIT('-am', 'left');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('other');",
				Expected: []sql.Row{{0, 1}},
			},
			{
				Query:    "SELECT base_pk, base_col1, our_pk, our_col1, their_pk, their_col1 from dolt_conflicts_t;",
				Expected: []sql.Row{{nil, nil, 1, 1, 1, -1}},
			},
			{
				Query:    "SELECT * from t;",
				Expected: []sql.Row{{1, 1}},
			},
		},
	},
	{
		Name: "merge with new triggers defined",
		SetUpScript: []string{
			"SET dolt_allow_commit_conflicts = on;",
			// create table and trigger1 (main & other)
			"CREATE TABLE x(a BIGINT PRIMARY KEY)",
			"CREATE TRIGGER trigger1 BEFORE INSERT ON x FOR EACH ROW SET new.a = new.a + 1",
			"CALL dolt_add('-A')",
			"CALL dolt_commit('-m', 'added table with trigger')",
			"CALL dolt_branch('-c', 'main', 'other')",
			// create trigger2 on main
			"CREATE TRIGGER trigger2 BEFORE INSERT ON x FOR EACH ROW SET new.a = (new.a * 2) + 10",
			"CALL dolt_commit('-am', 'created trigger2 on main')",
			// create trigger3 & trigger4 on other
			"CALL dolt_checkout('other')",
			"CREATE TRIGGER trigger3 BEFORE INSERT ON x FOR EACH ROW SET new.a = (new.a * 2) + 100",
			"CREATE TRIGGER trigger4 BEFORE INSERT ON x FOR EACH ROW SET new.a = (new.a * 2) + 1000",
			"UPDATE dolt_schemas SET id = id + 1 WHERE name = 'trigger4'",
			"CALL dolt_commit('-am', 'created triggers 3 & 4 on other');",
			"CALL dolt_checkout('main');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('other');",
				Expected: []sql.Row{{0, 1}},
			},
			// todo: merge triggers correctly
			//{
			//	Query:    "select count(*) from dolt_schemas where type = 'trigger';",
			//	Expected: []sql.Row{{4}},
			//},
		},
	},
	{
		Name: "dolt_merge() works with no auto increment overlap",
		SetUpScript: []string{
			"CREATE TABLE t (pk int PRIMARY KEY AUTO_INCREMENT, c0 int);",
			"CALL DOLT_ADD('.')",
			"INSERT INTO t (c0) VALUES (1), (2);",
			"CALL dolt_commit('-a', '-m', 'cm1');",
			"CALL dolt_checkout('-b', 'test');",
			"INSERT INTO t (c0) VALUES (3), (4);",
			"CALL dolt_commit('-a', '-m', 'cm2');",
			"CALL dolt_checkout('main');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL dolt_merge('test');",
				Expected: []sql.Row{{1, 0}},
			},
			{
				Query:    "INSERT INTO t VALUES (NULL,5),(6,6),(NULL,7);",
				Expected: []sql.Row{{sql.OkResult{RowsAffected: 3, InsertID: 5}}},
			},
			{
				Query: "SELECT * FROM t ORDER BY pk;",
				Expected: []sql.Row{
					{1, 1},
					{2, 2},
					{3, 3},
					{4, 4},
					{5, 5},
					{6, 6},
					{7, 7},
				},
			},
		},
	},
	{
		Name: "dolt_merge() (3way) works with no auto increment overlap",
		SetUpScript: []string{
			"CREATE TABLE t (pk int PRIMARY KEY AUTO_INCREMENT, c0 int);",
			"CALL DOLT_ADD('.')",
			"INSERT INTO t (c0) VALUES (1);",
			"CALL dolt_commit('-a', '-m', 'cm1');",
			"CALL dolt_checkout('-b', 'test');",
			"INSERT INTO t (pk,c0) VALUES (3,3), (4,4);",
			"CALL dolt_commit('-a', '-m', 'cm2');",
			"CALL dolt_checkout('main');",
			"INSERT INTO t (c0) VALUES (5);",
			"CALL dolt_commit('-a', '-m', 'cm3');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL dolt_merge('test');",
				Expected: []sql.Row{{0, 0}},
			},
			{
				Query:    "INSERT INTO t VALUES (NULL,6),(7,7),(NULL,8);",
				Expected: []sql.Row{{sql.OkResult{RowsAffected: 3, InsertID: 6}}},
			},
			{
				Query: "SELECT * FROM t ORDER BY pk;",
				Expected: []sql.Row{
					{1, 1},
					{3, 3},
					{4, 4},
					{5, 5},
					{6, 6},
					{7, 7},
					{8, 8},
				},
			},
		},
	},
	{
		Name: "dolt_merge() with a gap in an auto increment key",
		SetUpScript: []string{
			"CREATE TABLE t (pk int PRIMARY KEY AUTO_INCREMENT, c0 int);",
			"INSERT INTO t (c0) VALUES (1), (2);",
			"CALL dolt_add('-A');",
			"CALL dolt_commit('-am', 'cm1');",
			"CALL dolt_checkout('-b', 'test');",
			"INSERT INTO t VALUES (4,4), (5,5);",
			"CALL dolt_commit('-am', 'cm2');",
			"CALL dolt_checkout('main');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL dolt_merge('test');",
				Expected: []sql.Row{{1, 0}},
			},
			{
				Query:    "INSERT INTO t VALUES (3,3),(NULL,6);",
				Expected: []sql.Row{{sql.OkResult{RowsAffected: 2, InsertID: 3}}},
			},
			{
				Query: "SELECT * FROM t ORDER BY pk;",
				Expected: []sql.Row{
					{1, 1},
					{2, 2},
					{3, 3},
					{4, 4},
					{5, 5},
					{6, 6},
				},
			},
		},
	},
	{
		Name: "dolt_merge() (3way) with a gap in an auto increment key",
		SetUpScript: []string{
			"CREATE TABLE t (pk int PRIMARY KEY AUTO_INCREMENT, c0 int);",
			"INSERT INTO t (c0) VALUES (1);",
			"CALL dolt_add('-A');",
			"CALL dolt_commit('-am', 'cm1');",
			"CALL dolt_checkout('-b', 'test');",
			"INSERT INTO t VALUES (4,4), (5,5);",
			"CALL dolt_commit('-am', 'cm2');",
			"CALL dolt_checkout('main');",
			"INSERT INTO t (c0) VALUES (6);",
			"CALL dolt_commit('-am', 'cm3');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL dolt_merge('test');",
				Expected: []sql.Row{{0, 0}},
			},
			{
				Query:    "INSERT INTO t VALUES (3,3),(NULL,7);",
				Expected: []sql.Row{{sql.OkResult{RowsAffected: 2, InsertID: 3}}},
			},
			{
				Query: "SELECT * FROM t ORDER BY pk;",
				Expected: []sql.Row{
					{1, 1},
					{3, 3},
					{4, 4},
					{5, 5},
					{6, 6},
					{7, 7},
				},
			},
		},
	},
	{
		Name: "add multiple columns, then set and unset a value. No conflicts expected.",
		SetUpScript: []string{
			"CREATE table t (pk int primary key);",
			"Insert into t values (1), (2);",
			"alter table t add column col1 int;",
			"alter table t add column col2 int;",
			"CALL DOLT_ADD('.');",
			"CALL DOLT_COMMIT('-am', 'setup');",
			"CALL DOLT_CHECKOUT('-b', 'right');",
			"update t set col1 = 1 where pk = 1;",
			"update t set col1 = null where pk = 1;",
			"CALL DOLT_COMMIT('--allow-empty', '-am', 'right cm');",
			"CALL DOLT_CHECKOUT('main');",
			"DELETE from t where pk = 1;",
			"CALL DOLT_COMMIT('-am', 'left cm');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('right');",
				Expected: []sql.Row{{0, 0}},
			},
			{
				Query:    "SELECT * FROM t;",
				Expected: []sql.Row{{2, nil, nil}},
			},
		},
	},
}

var Dolt1MergeScripts = []queries.ScriptTest{
	{
		Name: "Merge errors if the primary key types have changed (even if the new type has the same NomsKind)",
		SetUpScript: []string{
			"CREATE TABLE t (pk1 bigint, pk2 bigint, PRIMARY KEY (pk1, pk2));",
			"CALL DOLT_ADD('.')",
			"CALL DOLT_COMMIT('-am', 'setup');",

			"CALL DOLT_CHECKOUT('-b', 'right');",
			"ALTER TABLE t MODIFY COLUMN pk2 tinyint",
			"INSERT INTO t VALUES (2, 2);",
			"CALL DOLT_COMMIT('-am', 'right commit');",

			"CALL DOLT_CHECKOUT('main');",
			"INSERT INTO t VALUES (1, 1);",
			"CALL DOLT_COMMIT('-am', 'left commit');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "CALL DOLT_MERGE('right');",
				ExpectedErrStr: "error: cannot merge two tables with different primary key sets",
			},
		},
	},
	{
		Name:        "`Delete from table` should keep artifacts - conflicts",
		SetUpScript: createConflictsSetupScript,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select base_pk, base_col1, our_pk, our_col1, their_pk, their_col1 from dolt_conflicts_t;",
				Expected: []sql.Row{
					{nil, nil, 1, -100, 1, 100},
					{nil, nil, 2, -200, 2, 200},
				},
			},
			{
				Query:    "delete from t;",
				Expected: []sql.Row{{sql.NewOkResult(2)}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{},
			},
			{
				Query: "select base_pk, base_col1, our_pk, our_col1, their_pk, their_col1 from dolt_conflicts_t;",
				Expected: []sql.Row{
					{nil, nil, nil, nil, 1, 100},
					{nil, nil, nil, nil, 2, 200},
				},
			},
		},
	},
	{
		Name:        "`Truncate table` should keep artifacts - conflicts",
		SetUpScript: createConflictsSetupScript,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select base_pk, base_col1, our_pk, our_col1, their_pk, their_col1 from dolt_conflicts_t;",
				Expected: []sql.Row{
					{nil, nil, 1, -100, 1, 100},
					{nil, nil, 2, -200, 2, 200},
				},
			},
			{
				Query:    "truncate t;",
				Expected: []sql.Row{{sql.NewOkResult(2)}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{},
			},
			{
				Query: "select base_pk, base_col1, our_pk, our_col1, their_pk, their_col1 from dolt_conflicts_t;",
				Expected: []sql.Row{
					{nil, nil, nil, nil, 1, 100},
					{nil, nil, nil, nil, 2, 200},
				},
			},
		},
	},
	{
		Name:        "`Delete from table` should keep artifacts - violations",
		SetUpScript: createViolationsSetupScript,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select pk, col1 from dolt_constraint_violations_t;",
				Expected: []sql.Row{{1, 1}, {2, 1}},
			},
			{
				Query:    "delete from t;",
				Expected: []sql.Row{{sql.NewOkResult(4)}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{},
			},
			{
				Query:    "select pk, col1 from dolt_constraint_violations_t;",
				Expected: []sql.Row{{1, 1}, {2, 1}},
			},
		},
	},
	{
		Name:        "`Truncate table` should keep artifacts - violations",
		SetUpScript: createViolationsSetupScript,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select pk, col1 from dolt_constraint_violations_t;",
				Expected: []sql.Row{{1, 1}, {2, 1}},
			},
			{
				Query:    "truncate t;",
				Expected: []sql.Row{{sql.NewOkResult(4)}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{},
			},
			{
				Query:    "select pk, col1 from dolt_constraint_violations_t;",
				Expected: []sql.Row{{1, 1}, {2, 1}},
			},
		},
	},
}

var KeylessMergeCVsAndConflictsScripts = []queries.ScriptTest{
	{
		Name: "Keyless merge with unique indexes documents violations",
		SetUpScript: []string{
			"SET dolt_force_transaction_commit = on;",
			"CREATE table t (col1 int, col2 int UNIQUE);",
			"CALL DOLT_ADD('.')",
			"CALL DOLT_COMMIT('-am', 'setup');",

			"CALL DOLT_CHECKOUT('-b', 'right');",
			"INSERT INTO t VALUES (2, 1);",
			"CALL DOLT_COMMIT('-am', 'right');",

			"CALL DOLT_CHECKOUT('main');",
			"INSERT INTO t values (1, 1);",
			"CALL DOLT_COMMIT('-am', 'left');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('right');",
				Expected: []sql.Row{{0, 1}},
			},
			{
				Query:    "SELECT violation_type, col1, col2 from dolt_constraint_violations_t ORDER BY col1 ASC;",
				Expected: []sql.Row{{uint64(merge.CvType_UniqueIndex), 1, 1}, {uint64(merge.CvType_UniqueIndex), 2, 1}},
			},
			{
				Query:    "SELECT * from t ORDER BY col1 ASC;",
				Expected: []sql.Row{{1, 1}, {2, 1}},
			},
		},
	},
	{
		Name: "Keyless merge with foreign keys documents violations",
		SetUpScript: []string{
			"SET dolt_force_transaction_commit = on;",
			"CREATE table parent (pk int PRIMARY KEY);",
			"CREATE table child (parent_fk int, FOREIGN KEY (parent_fk) REFERENCES parent (pk));",
			"CALL DOLT_ADD('.')",
			"INSERT INTO parent VALUES (1);",
			"CALL DOLT_COMMIT('-am', 'setup');",

			"CALL DOLT_CHECKOUT('-b', 'right');",
			"INSERT INTO child VALUES (1);",
			"CALL DOLT_COMMIT('-am', 'right');",

			"CALL DOLT_CHECKOUT('main');",
			"DELETE from parent where pk = 1;",
			"CALL DOLT_COMMIT('-am', 'left');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('right');",
				Expected: []sql.Row{{0, 1}},
			},
			{
				Query:    "SELECT violation_type, parent_fk from dolt_constraint_violations_child;",
				Expected: []sql.Row{{uint64(merge.CvType_ForeignKey), 1}},
			},
			{
				Query:    "SELECT * from parent;",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * from child;",
				Expected: []sql.Row{{1}},
			},
		},
	},
	{
		Name: "Keyless merge documents conflicts",
		SetUpScript: []string{
			"SET dolt_allow_commit_conflicts = on;",
			"CREATE table t (col1 int, col2 int);",
			"CALL DOLT_ADD('.')",
			"CALL DOLT_COMMIT('-am', 'setup');",

			"CALL DOLT_CHECKOUT('-b', 'right');",
			"INSERT INTO t VALUES (1, 1);",
			"CALL DOLT_COMMIT('-am', 'right');",

			"CALL DOLT_CHECKOUT('main');",
			"INSERT INTO t VALUES (1, 1);",
			"CALL DOLT_COMMIT('-am', 'left');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('right');",
				Expected: []sql.Row{{0, 1}},
			},
			{
				Query:    "SELECT base_col1, base_col2, our_col1, our_col2, their_col1, their_col2 from dolt_conflicts_t;",
				Expected: []sql.Row{{nil, nil, 1, 1, 1, 1}},
			},
		},
	},
}

var DoltConflictTableNameTableTests = []queries.ScriptTest{
	{
		Name: "conflict diff types",
		SetUpScript: []string{
			"SET dolt_allow_commit_conflicts = on;",
			"CREATE table t (pk int PRIMARY KEY, col1 int);",
			"CALL DOLT_ADD('.')",
			"INSERT INTO t VALUES (1, 1);",
			"INSERT INTO t VALUES (2, 2);",
			"INSERT INTO t VALUES (3, 3);",
			"CALL DOLT_COMMIT('-am', 'create table with row');",

			"CALL DOLT_CHECKOUT('-b', 'other');",
			"UPDATE t set col1 = 3 where pk = 1;",
			"UPDATE t set col1 = 0 where pk = 2;",
			"DELETE FROM t where pk = 3;",
			"INSERT INTO t VALUES (4, -4);",
			"CALL DOLT_COMMIT('-am', 'right edit');",

			"CALL DOLT_CHECKOUT('main');",
			"UPDATE t set col1 = 2 where pk = 1;",
			"DELETE FROM t where pk = 2;",
			"UPDATE t set col1 = 0 where pk = 3;",
			"INSERT INTO t VALUES (4, 4);",
			"CALL DOLT_COMMIT('-am', 'left edit');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('other');",
				Expected: []sql.Row{{0, 1}},
			},
			{
				Query: "SELECT base_pk, base_col1, our_pk, our_col1, our_diff_type, their_pk, their_col1, their_diff_type" +
					" from dolt_conflicts_t ORDER BY COALESCE(base_pk, our_pk, their_pk) ASC;",
				Expected: []sql.Row{
					{1, 1, 1, 2, "modified", 1, 3, "modified"},
					{2, 2, nil, nil, "removed", 2, 0, "modified"},
					{3, 3, 3, 0, "modified", nil, nil, "removed"},
					{nil, nil, 4, 4, "added", 4, -4, "added"},
				},
			},
		},
	},
	{
		Name: "keyless cardinality columns",
		SetUpScript: []string{
			"SET dolt_allow_commit_conflicts = on;",
			"CREATE table t (col1 int);",
			"CALL DOLT_ADD('.')",
			"INSERT INTO t VALUES (1), (2), (3), (4), (6);",
			"CALL DOLT_COMMIT('-am', 'init');",

			"CALL DOLT_CHECKOUT('-b', 'right');",
			"INSERT INTO t VALUES (1);",
			"DELETE FROM t where col1 = 2;",
			"INSERT INTO t VALUES (3);",
			"INSERT INTO t VALUES (4), (4);",
			"INSERT INTO t VALUES (5);",
			"DELETE from t where col1 = 6;",
			"CALL DOLT_COMMIT('-am', 'right');",

			"CALL DOLT_CHECKOUT('main');",
			"DELETE FROM t WHERE col1 = 1;",
			"INSERT INTO t VALUES (2);",
			"INSERT INTO t VALUES (3);",
			"INSERT INTO t VALUES (4);",
			"INSERT INTO t VALUES (5);",
			"DELETE from t where col1 = 6;",
			"CALL DOLT_COMMIT('-am', 'left');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('right');",
				Expected: []sql.Row{{0, 1}},
			},
			{
				Query: "SELECT base_col1, our_col1, their_col1, our_diff_type, their_diff_type, base_cardinality, our_cardinality, their_cardinality from dolt_conflicts_t ORDER BY COALESCE(base_col1, our_col1, their_col1) ASC;",
				Expected: []sql.Row{
					{1, nil, 1, "removed", "modified", uint64(1), uint64(0), uint64(2)},
					{2, 2, nil, "modified", "removed", uint64(1), uint64(2), uint64(0)},
					{3, 3, 3, "modified", "modified", uint64(1), uint64(2), uint64(2)},
					{4, 4, 4, "modified", "modified", uint64(1), uint64(2), uint64(3)},
					{nil, 5, 5, "added", "added", uint64(0), uint64(1), uint64(1)},
					{6, nil, nil, "removed", "removed", uint64(1), uint64(0), uint64(0)},
				},
			},
		},
	},
}

var createConflictsSetupScript = []string{
	"create table t (pk int primary key, col1 int);",
	"call dolt_commit('-Am', 'create table');",
	"call dolt_checkout('-b', 'other');",

	"insert into t values (1, 100);",
	"insert into t values (2, 200);",
	"call dolt_commit('-Am', 'other commit');",

	"call dolt_checkout('main');",
	"insert into t values (1, -100);",
	"insert into t values (2, -200);",
	"call dolt_commit('-Am', 'main commit');",

	"set dolt_allow_commit_conflicts = on;",
	"call dolt_merge('other');",
}

var createViolationsSetupScript = []string{
	"CREATE TABLE t (pk int PRIMARY KEY, col1 int UNIQUE);",
	"CALL DOLT_COMMIT('-Am', 'create table');",

	"CALL DOLT_CHECKOUT('-b', 'other');",
	"INSERT INTO t VALUES (2, 1), (3, 3);",
	"CALL DOLT_COMMIT('-am', 'other insert');",

	"CALL DOLT_CHECKOUT('main');",
	"INSERT INTO t values (1, 1), (4, 4);",
	"CALL DOLT_COMMIT('-am', 'main insert');",

	"SET dolt_force_transaction_commit = on;",
	"call dolt_merge('other');",
}

var Dolt1ConflictTableNameTableTests = []queries.ScriptTest{
	{
		Name:        "Provides a dolt_conflicts_id",
		SetUpScript: createConflictsSetupScript,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "set @hash1 = (select dolt_conflict_id from dolt_conflicts_t where our_pk = 1);",
			},
			{
				Query: "set @hash2 = (select dolt_conflict_id from dolt_conflicts_t where our_pk = 2);",
			},
			{
				Query: "select base_pk, base_col1, our_pk, our_col1, their_pk, their_col1 from dolt_conflicts_t where dolt_conflict_id = @hash1;",
				Expected: []sql.Row{
					{nil, nil, 1, -100, 1, 100},
				},
			},
			{
				Query: "select base_pk, base_col1, our_pk, our_col1, their_pk, their_col1 from dolt_conflicts_t where dolt_conflict_id = @hash2;",
				Expected: []sql.Row{
					{nil, nil, 2, -200, 2, 200},
				},
			},
			// Make sure that we can update using it
			{
				Query:    "update dolt_conflicts_t SET our_col1 = their_col1 where dolt_conflict_id = @hash1;",
				Expected: []sql.Row{{sql.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			{
				Query: "select base_pk, base_col1, our_pk, our_col1, their_pk, their_col1 from dolt_conflicts_t;",
				Expected: []sql.Row{
					{nil, nil, 1, 100, 1, 100},
					{nil, nil, 2, -200, 2, 200},
				},
			},
			// And delete
			{
				Query:    "delete from dolt_conflicts_t where dolt_conflict_id = @hash1;",
				Expected: []sql.Row{{sql.OkResult{RowsAffected: 1}}},
			},
			{
				Query: "select base_pk, base_col1, our_pk, our_col1, their_pk, their_col1 from dolt_conflicts_t;",
				Expected: []sql.Row{
					{nil, nil, 2, -200, 2, 200},
				},
			},
		},
	},
	{
		Name: "dolt_conflicts_id is unique across merges",
		SetUpScript: append(createConflictsSetupScript, []string{
			"CALL DOLT_COMMIT('-afm', 'commit conflicts');",

			"CALL DOLT_CHECKOUT('-b', 'other2');",
			"UPDATE t SET col1 = 9999 where pk = 1;",
			"CALL DOLT_COMMIT('-afm', 'commit on other2');",

			"CALL DOLT_CHECKOUT('main');",
			"UPDATE t SET col1 = 8888 where pk = 1;",
			"CALL DOLT_COMMIT('-afm', 'commit on main');",

			"CALL DOLT_MERGE('other2');",

			"set @hash1 = (select dolt_conflict_id from dolt_conflicts_t where our_pk = 1 and their_col1 = 100);",
			"set @hash2 = (select dolt_conflict_id from dolt_conflicts_t where our_pk = 1 and their_col1 = 9999);",
			"set @hash3 = (select dolt_conflict_id from dolt_conflicts_t where our_pk = 2);",
		}...),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select @hash1 != @hash2 AND @hash2 != @hash3;",
				Expected: []sql.Row{{true}},
			},
		},
	},
	{
		Name:        "Updates on our columns get applied to the source table - smoke",
		SetUpScript: createConflictsSetupScript,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select base_pk, base_col1, our_pk, our_col1, their_pk, their_col1 from dolt_conflicts_t;",
				Expected: []sql.Row{
					{nil, nil, 1, -100, 1, 100},
					{nil, nil, 2, -200, 2, 200},
				},
			},
			{
				Query:    "update dolt_conflicts_t set our_col1 = 1000 where our_pk = 1;",
				Expected: []sql.Row{{sql.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			{
				Query: "select base_pk, base_col1, our_pk, our_col1, their_pk, their_col1 from dolt_conflicts_t;",
				Expected: []sql.Row{
					{nil, nil, 1, 1000, 1, 100},
					{nil, nil, 2, -200, 2, 200},
				},
			},
			{
				Query: "select * from t;",
				Expected: []sql.Row{
					{1, 1000},
					{2, -200},
				},
			},
			{
				Query:    "update dolt_conflicts_t set our_col1 = their_col1;",
				Expected: []sql.Row{{sql.OkResult{RowsAffected: 2, Info: plan.UpdateInfo{Matched: 2, Updated: 2}}}},
			},
			{
				Query: "select base_pk, base_col1, our_pk, our_col1, their_pk, their_col1 from dolt_conflicts_t;",
				Expected: []sql.Row{
					{nil, nil, 1, 100, 1, 100},
					{nil, nil, 2, 200, 2, 200},
				},
			},
			{
				Query: "select * from t;",
				Expected: []sql.Row{
					{1, 100},
					{2, 200},
				},
			},
		},
	},
	{
		Name: "Updates on our columns get applied to the source table - compound / inverted pks",
		SetUpScript: []string{
			"create table t (pk2 int, pk1 int, col1 int, primary key (pk1, pk2));",
			"call dolt_commit('-Am', 'create table');",

			"call dolt_checkout('-b', 'other');",
			"insert into t values (1, 1, 100), (2, 1, 200);",
			"call dolt_commit('-Am', 'other commit');",

			"call dolt_checkout('main');",
			"insert into t values (1, 1, -100), (2, 1, -200);",
			"call dolt_commit('-Am', 'main commit');",

			"set dolt_allow_commit_conflicts = on;",
			"call dolt_merge('other');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select base_pk1, base_pk2, base_col1, our_pk1, our_pk2, our_col1, their_pk1, their_pk2, their_col1 from dolt_conflicts_t;",
				Expected: []sql.Row{
					{nil, nil, nil, 1, 1, -100, 1, 1, 100},
					{nil, nil, nil, 1, 2, -200, 1, 2, 200},
				},
			},
			{
				Query:    "Update dolt_conflicts_t set our_col1 = 1000;",
				Expected: []sql.Row{{sql.OkResult{RowsAffected: 2, Info: plan.UpdateInfo{Matched: 2, Updated: 2}}}},
			},
			{
				Query: "select base_pk1, base_pk2, base_col1, our_pk1, our_pk2, our_col1, their_pk1, their_pk2, their_col1 from dolt_conflicts_t;",
				Expected: []sql.Row{
					{nil, nil, nil, 1, 1, 1000, 1, 1, 100},
					{nil, nil, nil, 1, 2, 1000, 1, 2, 200},
				},
			},
			{
				Query: "select * from t;",
				Expected: []sql.Row{
					{1, 1, 1000},
					{2, 1, 1000},
				},
			},
		},
	},
	{
		Name: "Updates on our columns get applied to the source table - keyless",
		SetUpScript: []string{
			"create table t (name varchar(100), price int);",
			"call dolt_commit('-Am', 'create table');",

			"call dolt_checkout('-b', 'other');",
			"insert into t values ('apple', 1);",
			"call dolt_commit('-Am', 'other commit');",

			"call dolt_checkout('main');",
			"insert into t values ('apple', 1), ('apple', 1);",
			"call dolt_commit('-Am', 'main commit');",

			"set dolt_allow_commit_conflicts = on;",
			"call dolt_merge('other');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select base_name, base_price, base_cardinality, our_name, our_price, our_cardinality, their_name, their_price, their_cardinality from dolt_conflicts_t;",
				Expected: []sql.Row{
					{nil, nil, uint64(0), "apple", 1, uint64(2), "apple", 1, uint64(1)},
				},
			},
			// Arguably this behavior is weird. If you ran this same query
			// against the original table, it would update two rows. Since this
			// was run against the conflicts table, only one row is updated.
			{
				Query: "update dolt_conflicts_t set our_name = 'orange' where our_name = 'apple'",
				Expected: []sql.Row{
					{sql.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Updated: 1, Matched: 1}}},
				},
			},
			{
				Query: "select base_name, base_price, base_cardinality, our_name, our_price, our_cardinality, their_name, their_price, their_cardinality from dolt_conflicts_t;",
				Expected: []sql.Row{
					{nil, nil, uint64(0), "apple", 1, uint64(1), "apple", 1, uint64(1)},
				},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{"apple", 1}, {"orange", 1}},
			},
			// Updating cardinality should be no-op.
			{
				Query: "update dolt_conflicts_t set our_cardinality = 10, their_cardinality = 10, base_cardinality = 10;",
			},
			{
				Query: "select base_name, base_price, base_cardinality, our_name, our_price, our_cardinality, their_name, their_price, their_cardinality from dolt_conflicts_t;",
				Expected: []sql.Row{
					{nil, nil, uint64(0), "apple", 1, uint64(1), "apple", 1, uint64(1)},
				},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{"apple", 1}, {"orange", 1}},
			},
		},
	},
	{
		Name: "Updating our cols when the row is missing inserts the row",
		SetUpScript: []string{
			"create table t (pk int primary key, col1 int);",
			"insert into t values (1, null);",
			"insert into t values (2, null);",
			"insert into t values (3, null);",
			"call dolt_commit('-Am', 'create table');",
			"call dolt_checkout('-b', 'other');",

			"update t set col1 = 100 where pk = 1;",
			"delete from t where pk = 2;",
			"update t set col1 = 300 where pk = 3;",
			"insert into t values (4, 400);",
			"call dolt_commit('-Am', 'other commit');",

			"call dolt_checkout('main');",
			"update t set col1 = -100 where pk = 1;",
			"update t set col1 = -200 where pk = 2;",
			"delete from t where pk = 3;",
			"insert into t values (4, -400);",
			"call dolt_commit('-Am', 'main commit');",

			"set dolt_allow_commit_conflicts = on;",
			"call dolt_merge('other');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select base_pk, base_col1, our_pk, our_col1, their_pk, their_col1 from dolt_conflicts_t;",
				Expected: []sql.Row{
					{1, nil, 1, -100, 1, 100},
					{2, nil, 2, -200, nil, nil},
					{3, nil, nil, nil, 3, 300},
					{nil, nil, 4, -400, 4, 400},
				},
			},
			{
				Query:    "delete from t;",
				Expected: []sql.Row{{sql.NewOkResult(3)}},
			},
			{
				Query: "select base_pk, base_col1, our_pk, our_col1, their_pk, their_col1 from dolt_conflicts_t;",
				Expected: []sql.Row{
					{1, nil, nil, nil, 1, 100},
					{2, nil, nil, nil, nil, nil},
					{3, nil, nil, nil, 3, 300},
					{nil, nil, nil, nil, 4, 400},
				},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{},
			},
			// The new rows PKs must be fully specified
			{
				Query:          "update dolt_conflicts_t set our_col1 = their_col1;",
				ExpectedErrStr: "column name 'our_pk' is non-nullable but attempted to set a value of null",
			},
			// Take theirs
			{
				Query:    "update dolt_conflicts_t set our_pk = their_pk, our_col1 = their_col1;",
				Expected: []sql.Row{{sql.OkResult{RowsAffected: 3, Info: plan.UpdateInfo{Matched: 4, Updated: 3}}}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{1, 100}, {3, 300}, {4, 400}},
			},
		},
	},
	{
		Name:        "Updating our cols when our, their, and base schemas are not the equal errors",
		SetUpScript: append(createConflictsSetupScript, "ALTER TABLE t add column col2 int FIRST;"),
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "update dolt_conflicts_t set base_col1 = 9999, their_col1 = 9999;",
				ExpectedErrStr: "the source table cannot be automatically updated through the conflict table since the base, our, and their schemas are not equal",
			},
		},
	},
	{
		Name:        "Updates on their or base columns do nothing",
		SetUpScript: createConflictsSetupScript,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select base_pk, base_col1, our_pk, our_col1, their_pk, their_col1 from dolt_conflicts_t;",
				Expected: []sql.Row{
					{nil, nil, 1, -100, 1, 100},
					{nil, nil, 2, -200, 2, 200},
				},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{1, -100}, {2, -200}},
			},
			{
				Query:    "update dolt_conflicts_t set base_col1 = 9999, their_col1 = 9999;",
				Expected: []sql.Row{{sql.OkResult{RowsAffected: 2, Info: plan.UpdateInfo{Matched: 2, Updated: 2}}}},
			},
			{
				Query: "select base_pk, base_col1, our_pk, our_col1, their_pk, their_col1 from dolt_conflicts_t;",
				Expected: []sql.Row{
					{nil, nil, 1, -100, 1, 100},
					{nil, nil, 2, -200, 2, 200},
				},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{1, -100}, {2, -200}},
			},
		},
	},
}

// MergeArtifactsScripts tests new format merge behavior where
// existing violations and conflicts are merged together.
var MergeArtifactsScripts = []queries.ScriptTest{
	{
		Name: "conflicts on different branches can be merged",
		SetUpScript: []string{
			"SET dolt_allow_commit_conflicts = on",
			"CALL DOLT_CHECKOUT('-b', 'conflicts1');",
			"CREATE table t (pk int PRIMARY KEY, col1 int);",
			"CALL DOLT_ADD('.')",
			"CALL DOLT_COMMIT('-am', 'create table');",
			"CALL DOLT_BRANCH('conflicts2');",

			// branches conflicts1 and conflicts2 both have a table t with no rows

			// create a conflict for pk 1 in conflicts1
			"INSERT INTO t VALUES (1, 1);",
			"CALL DOLT_COMMIT('-am', 'insert pk 1');",
			"CALL DOLT_BRANCH('other');",
			"UPDATE t set col1 = 100 where pk = 1;",
			"CALL DOLT_COMMIT('-am', 'left edit');",
			"CALL DOLT_CHECKOUT('other');",
			"UPDATE T set col1 = -100 where pk = 1;",
			"CALL DOLT_COMMIT('-am', 'right edit');",
			"CALL DOLT_CHECKOUT('conflicts1');",
			"CALL DOLT_MERGE('other');",
			"CALL DOLT_COMMIT('-afm', 'commit conflicts on conflicts1');",

			// create a conflict for pk 2 in conflicts2
			"CALL DOLT_CHECKOUT('conflicts2');",
			"INSERT INTO t VALUES (2, 2);",
			"CALL DOLT_COMMIT('-am', 'insert pk 2');",
			"CALL DOLT_BRANCH('other2');",
			"UPDATE t set col1 = 100 where pk = 2;",
			"CALL DOLT_COMMIT('-am', 'left edit');",
			"CALL DOLT_CHECKOUT('other2');",
			"UPDATE T set col1 = -100 where pk = 2;",
			"CALL DOLT_COMMIT('-am', 'right edit');",
			"CALL DOLT_CHECKOUT('conflicts2');",
			"CALL DOLT_MERGE('other2');",
			"CALL DOLT_COMMIT('-afm', 'commit conflicts on conflicts2');",

			"CALL DOLT_CHECKOUT('conflicts1');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT base_pk, base_col1, our_pk, our_col1, their_pk, their_col1 from dolt_conflicts_t;",
				Expected: []sql.Row{{1, 1, 1, 100, 1, -100}},
			},
			{
				Query:    "SELECT pk, col1 from t;",
				Expected: []sql.Row{{1, 100}},
			},
			{
				Query:    "CALL DOLT_CHECKOUT('conflicts2');",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "SELECT base_pk, base_col1, our_pk, our_col1, their_pk, their_col1 from dolt_conflicts_t;",
				Expected: []sql.Row{{2, 2, 2, 100, 2, -100}},
			},
			{
				Query:    "SELECT pk, col1 from t;",
				Expected: []sql.Row{{2, 100}},
			},
			{
				Query:    "CALL DOLT_MERGE('conflicts1');",
				Expected: []sql.Row{{0, 1}},
			},
			{
				Query: "SELECT base_pk, base_col1, our_pk, our_col1, their_pk, their_col1 from dolt_conflicts_t;",
				Expected: []sql.Row{
					{1, 1, 1, 100, 1, -100},
					{2, 2, 2, 100, 2, -100},
				},
			},
			{
				Query: "SELECT pk, col1 from t;",
				Expected: []sql.Row{
					{1, 100},
					{2, 100},
				},
			},
			{
				Query: "UPDATE t SET col1 = 300;",
				Expected: []sql.Row{{sql.OkResult{
					RowsAffected: 2,
					Info: plan.UpdateInfo{
						Matched: 2,
						Updated: 2,
					},
				}}},
			},
			{
				Query: "SELECT base_pk, base_col1, our_pk, our_col1, their_pk, their_col1 from dolt_conflicts_t;",
				Expected: []sql.Row{
					{1, 1, 1, 300, 1, -100},
					{2, 2, 2, 300, 2, -100},
				},
			},
		},
	},
	{
		Name: "conflicts of different schemas can't coexist",
		SetUpScript: []string{
			"SET dolt_allow_commit_conflicts = on",
			"CREATE table t (pk int PRIMARY KEY, col1 int);",
			"CALL DOLT_ADD('.')",
			"CALL DOLT_COMMIT('-am', 'create table');",
			"INSERT INTO t VALUES (1, 1);",
			"CALL DOLT_COMMIT('-am', 'insert pk 1');",

			"CALL DOLT_BRANCH('other');",
			"UPDATE t set col1 = 100 where pk = 1;",
			"CALL DOLT_COMMIT('-am', 'left edit');",
			"CALL DOLT_CHECKOUT('other');",

			"UPDATE T set col1 = -100 where pk = 1;",
			"CALL DOLT_COMMIT('-am', 'right edit');",
			"CALL DOLT_CHECKOUT('main');",
			"CALL DOLT_MERGE('other');",
			"CALL DOLT_COMMIT('-afm', 'commit conflicts on main');",
			"ALTER TABLE t ADD COLUMN col2 int;",
			"CALL DOLT_COMMIT('-afm', 'alter schema');",
			"CALL DOLT_CHECKOUT('-b', 'other2');",
			"UPDATE t set col2 = -1000 where pk = 1;",
			"CALL DOLT_COMMIT('-afm', 'update pk 1 to -1000');",
			"CALL DOLT_CHECKOUT('main');",
			"UPDATE t set col2 = 1000 where pk = 1;",
			"CALL DOLT_COMMIT('-afm', 'update pk 1 to 1000');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "CALL DOLT_MERGE('other2');",
				ExpectedErrStr: "the existing conflicts are of a different schema than the conflicts generated by this merge. Please resolve them and try again",
			},
			{
				Query:    "SELECT base_pk, base_col1, our_pk, our_col1, their_pk, their_col1 from dolt_conflicts_t;",
				Expected: []sql.Row{{1, 1, 1, 100, 1, -100}},
			},
			{
				Query:    "SELECT pk, col1, col2 from t;",
				Expected: []sql.Row{{1, 100, 1000}},
			},
		},
	},
	{
		Name: "violations with an older commit hash are overwritten if the value is the same",
		SetUpScript: []string{
			"set dolt_force_transaction_commit = on;",

			"CALL DOLT_CHECKOUT('-b', 'viol1');",
			"CREATE TABLE parent (pk int PRIMARY KEY);",
			"CREATE TABLE child (pk int PRIMARY KEY, fk int, FOREIGN KEY (fk) REFERENCES parent (pk));",
			"CALL DOLT_ADD('.')",
			"CALL DOLT_COMMIT('-am', 'setup table');",
			"CALL DOLT_BRANCH('viol2');",
			"CALL DOLT_BRANCH('other3');",
			"INSERT INTO parent VALUES (1);",
			"CALL DOLT_COMMIT('-am', 'viol1 setup');",

			"CALL DOLT_CHECKOUT('-b', 'other');",
			"INSERT INTO child VALUES (1, 1);",
			"CALL DOLT_COMMIT('-am', 'insert child of 1');",

			"CALL DOLT_CHECKOUT('viol1');",
			"DELETE FROM parent where pk = 1;",
			"CALL DOLT_COMMIT('-am', 'delete 1');",
			"CALL DOLT_MERGE('other');",
			"CALL DOLT_COMMIT('-afm', 'commit violations 1');",

			"CALL DOLT_CHECKOUT('viol2');",
			"INSERT INTO parent values (2);",
			"CALL DOLT_COMMIT('-am', 'viol2 setup');",

			"CALL DOLT_CHECKOUT('-b', 'other2');",
			"INSERT into child values (2, 2);",
			"CALL DOLT_COMMIT('-am', 'insert child of 2');",

			"CALL DOLT_CHECKOUT('viol2');",
			"DELETE FROM parent where pk = 2;",
			"CALL DOLT_COMMIT('-am', 'delete 2');",
			"CALL DOLT_MERGE('other2');",
			"CALL DOLT_COMMIT('-afm', 'commit violations 2');",

			"CALL DOLT_CHECKOUT('other3');",
			"INSERT INTO PARENT VALUES (3);",
			"CALL DOLT_COMMIT('-am', 'edit needed to trigger three-way merge');",

			"CALL DOLT_CHECKOUT('viol1');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT violation_type, pk, fk from dolt_constraint_violations_child;",
				Expected: []sql.Row{{uint64(merge.CvType_ForeignKey), 1, 1}},
			},
			{
				Query:    "SELECT pk, fk from child;",
				Expected: []sql.Row{{1, 1}},
			},
			{
				Query:    "SELECT * from parent;",
				Expected: []sql.Row{},
			},
			{
				Query:    "CALL DOLT_CHECKOUT('viol2');",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "SELECT violation_type, pk, fk from dolt_constraint_violations_child;",
				Expected: []sql.Row{{uint64(merge.CvType_ForeignKey), 2, 2}},
			},
			{
				Query:    "SELECT pk, fk from child;",
				Expected: []sql.Row{{2, 2}},
			},
			{
				Query:    "SELECT * from parent;",
				Expected: []sql.Row{},
			},
			{
				Query:    "CALL DOLT_MERGE('viol1');",
				Expected: []sql.Row{{0, 1}},
			},
			// the commit hashes for the above two violations change in this merge
			{
				Query:    "SELECT violation_type, fk, pk from dolt_constraint_violations_child;",
				Expected: []sql.Row{{uint64(merge.CvType_ForeignKey), 1, 1}, {uint64(merge.CvType_ForeignKey), 2, 2}},
			},
			{
				Query:    "SELECT pk, fk from child;",
				Expected: []sql.Row{{1, 1}, {2, 2}},
			},
			{
				Query:    "SELECT * from parent;",
				Expected: []sql.Row{},
			},
			{
				Query:            "CALL DOLT_COMMIT('-afm', 'commit active merge');",
				SkipResultsCheck: true,
			},
			{
				Query:    "SET FOREIGN_KEY_CHECKS=0;",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "UPDATE child set fk = 4;",
				Expected: []sql.Row{{sql.OkResult{RowsAffected: 2, InsertID: 0, Info: plan.UpdateInfo{Matched: 2, Updated: 2}}}},
			},
			{
				Query:            "CALL DOLT_COMMIT('-afm', 'update children to new value');",
				SkipResultsCheck: true,
			},
			{
				Query:    "CALL DOLT_MERGE('other3');",
				Expected: []sql.Row{{0, 1}},
			},
			{
				Query: "SELECT violation_type, pk, fk from dolt_constraint_violations_child;",
				Expected: []sql.Row{
					{uint64(merge.CvType_ForeignKey), 1, 1},
					{uint64(merge.CvType_ForeignKey), 1, 4},
					{uint64(merge.CvType_ForeignKey), 2, 2},
					{uint64(merge.CvType_ForeignKey), 2, 4}},
			},
		},
	},
	{
		Name: "merging unique key violations in left and right",
		SetUpScript: []string{
			"SET dolt_force_transaction_commit = on;",
			"CREATE TABLE t (pk int PRIMARY KEY, col1 int UNIQUE);",
			"CALL DOLT_ADD('.')",
			"CALL DOLT_COMMIT('-am', 'create table t');",
			"CALL DOLT_BRANCH('right');",
			"CALL DOLT_BRANCH('left2');",

			"CALL DOLT_CHECKOUT('-b', 'right2');",
			"INSERT INTO T VALUES (4, 1);",
			"CALL DOLT_COMMIT('-am', 'right2 insert');",

			"CALL DOLT_CHECKOUT('right');",
			"INSERT INTO T VALUES (3, 1);",
			"CALL DOLT_COMMIT('-am', 'right insert');",

			"CALL DOLT_CHECKOUT('left2');",
			"INSERT INTO T VALUES (2, 1);",
			"CALL DOLT_COMMIT('-am', 'left2 insert');",

			"CALL DOLT_CHECKOUT('main');",
			"INSERT INTO T VALUES (1, 1);",
			"CALL DOLT_COMMIT('-am', 'left insert');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('left2');",
				Expected: []sql.Row{{0, 1}},
			},
			{
				Query:    "SELECT * from t;",
				Expected: []sql.Row{{1, 1}, {2, 1}},
			},
			{
				Query:    "SELECT violation_type, pk, col1 from dolt_constraint_violations_t;",
				Expected: []sql.Row{{uint64(merge.CvType_UniqueIndex), 1, 1}, {uint64(merge.CvType_UniqueIndex), 2, 1}},
			},
			{
				Query:            "CALL DOLT_COMMIT('-afm', 'commit unique key viol');",
				SkipResultsCheck: true,
			},
			{
				Query:    "CALL DOLT_CHECKOUT('right');",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "CALL DOLT_MERGE('right2');",
				Expected: []sql.Row{{0, 1}},
			},
			{
				Query:    "SELECT * from t;",
				Expected: []sql.Row{{3, 1}, {4, 1}},
			},
			{
				Query:    "SELECT violation_type, pk, col1 from dolt_constraint_violations_t;",
				Expected: []sql.Row{{uint64(merge.CvType_UniqueIndex), 3, 1}, {uint64(merge.CvType_UniqueIndex), 4, 1}},
			},
			{
				Query:            "CALL DOLT_COMMIT('-afm', 'commit unique key viol');",
				SkipResultsCheck: true,
			},
			{
				Query:    "CALL DOLT_CHECKOUT('main');",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "CALL DOLT_MERGE('right');",
				Expected: []sql.Row{{0, 1}},
			},
			{
				Query:    "SELECT * from t;",
				Expected: []sql.Row{{1, 1}, {2, 1}, {3, 1}, {4, 1}},
			},
			{
				Query: "SELECT violation_type, pk, col1 from dolt_constraint_violations_t;",
				Expected: []sql.Row{
					{uint64(merge.CvType_UniqueIndex), 1, 1},
					{uint64(merge.CvType_UniqueIndex), 2, 1},
					{uint64(merge.CvType_UniqueIndex), 3, 1},
					{uint64(merge.CvType_UniqueIndex), 4, 1}},
			},
		},
	},
	{
		Name: "right adds a unique key constraint and resolves existing violations.",
		SetUpScript: []string{
			"SET dolt_force_transaction_commit = on;",
			"CREATE TABLE t (pk int PRIMARY KEY, col1 int);",
			"CALL DOLT_ADD('.')",
			"INSERT INTO t VALUES (1, 1), (2, 1);",
			"CALL DOLT_COMMIT('-am', 'table and data');",

			"CALL DOLT_CHECKOUT('-b', 'right');",
			"UPDATE t SET col1 = 2 where pk = 2;",
			"ALTER TABLE t ADD UNIQUE col1_uniq (col1);",
			"CALL DOLT_COMMIT('-am', 'right adds a unique index');",

			"CALL DOLT_CHECKOUT('main');",
			"INSERT INTO t VALUES (3, 3);",
			"CALL DOLT_COMMIT('-am', 'left edit');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('right');",
				Expected: []sql.Row{{0, 0}},
			},
			{
				Query:    "SELECT * from t;",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}},
			},
		},
	},
	{
		Name: "Multiple foreign key violations for a given row not supported",
		SetUpScript: []string{
			"SET dolt_force_transaction_commit = on;",
			`
			CREATE TABLE parent(
			  pk int PRIMARY KEY, 
			  col1 int, 
			  col2 int, 
			  INDEX par_col1_idx (col1), 
			  INDEX par_col2_idx (col2)
			);`,
			`
			CREATE TABLE child(
			  pk int PRIMARY KEY,
			  col1 int,
			  col2 int,
			  FOREIGN KEY (col1) REFERENCES parent(col1),
			  FOREIGN KEY (col2) REFERENCES parent(col2)
			);`,
			"CALL DOLT_ADD('.')",
			"INSERT INTO parent VALUES (1, 1, 1);",
			"CALL DOLT_COMMIT('-am', 'initial');",

			"CALL DOLT_CHECKOUT('-b', 'right');",
			"INSERT INTO CHILD VALUES (1, 1, 1);",
			"CALL DOLT_COMMIT('-am', 'insert child');",

			"CALL DOLT_CHECKOUT('main');",
			"DELETE from parent where pk = 1;",
			"CALL DOLT_COMMIT('-am', 'delete parent');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "CALL DOLT_MERGE('right');",
				ExpectedErrStr: "multiple violations for row not supported: pk ( 1 ) of table 'child' violates foreign keys 'parent (col1)' and 'parent (col2)'",
			},
			{
				Query:    "SELECT * from parent;",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * from child;",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "Multiple unique key violations for a given row not supported",
		SetUpScript: []string{
			"SET dolt_force_transaction_commit = on;",
			"CREATE table t (pk int PRIMARY KEY, col1 int UNIQUE, col2 int UNIQUE);",
			"CALL DOLT_ADD('.')",
			"CALL DOLT_COMMIT('-am', 'setup');",

			"CALL DOLT_CHECKOUT('-b', 'right');",
			"INSERT into t VALUES (2, 1, 1);",
			"CALL DOLT_COMMIT('-am', 'right insert');",

			"CALL DOLT_CHECKOUT('main');",
			"INSERT INTO t VALUES (1, 1, 1);",
			"CALL DOLT_COMMIT('-am', 'left insert');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "CALL DOLT_MERGE('right');",
				ExpectedErrStr: "multiple violations for row not supported: pk ( 1 ) of table 't' violates unique keys 'col1' and 'col2'",
			},
			{
				Query:    "SELECT * from t;",
				Expected: []sql.Row{{1, 1, 1}},
			},
		},
	},
}

// OldFormatMergeConflictsAndCVsScripts tests old format merge behavior
// where violations are appended and merges are aborted if there are existing
// violations and/or conflicts.
var OldFormatMergeConflictsAndCVsScripts = []queries.ScriptTest{
	{
		Name: "merging branches into a constraint violated head. Any new violations are appended",
		SetUpScript: []string{
			"CREATE table parent (pk int PRIMARY KEY, col1 int);",
			"CREATE table child (pk int PRIMARY KEY, parent_fk int, FOREIGN KEY (parent_fk) REFERENCES parent(pk));",
			"CREATE table other (pk int);",
			"CALL DOLT_ADD('.')",
			"INSERT INTO parent VALUES (1, 1), (2, 2);",
			"CALL DOLT_COMMIT('-am', 'setup');",
			"CALL DOLT_BRANCH('branch1');",
			"CALL DOLT_BRANCH('branch2');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				// we need dolt_force_transaction_commit because we want to
				// transaction commit constraint violations that occur as a
				// result of a merge.
				Query:    "set autocommit = off, dolt_force_transaction_commit = on",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "DELETE FROM parent where pk = 1;",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:            "CALL DOLT_COMMIT('-am', 'delete parent 1');",
				SkipResultsCheck: true,
			},
			{
				Query:    "CALL DOLT_CHECKOUT('branch1');",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "INSERT INTO CHILD VALUES (1, 1);",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:            "CALL DOLT_COMMIT('-am', 'insert child of parent 1');",
				SkipResultsCheck: true,
			},
			{
				Query:    "CALL DOLT_CHECKOUT('main');",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "CALL DOLT_MERGE('branch1');",
				Expected: []sql.Row{{0, 1}},
			},
			{
				Query:    "SELECT violation_type, pk, parent_fk from dolt_constraint_violations_child;",
				Expected: []sql.Row{{uint16(1), 1, 1}},
			},
			{
				Query:    "COMMIT;",
				Expected: []sql.Row{},
			},
			{
				Query:          "CALL DOLT_COMMIT('-am', 'commit constraint violations');",
				ExpectedErrStr: "error: the table(s) child have constraint violations",
			},
			{
				Query:            "CALL DOLT_COMMIT('-afm', 'commit constraint violations');",
				SkipResultsCheck: true,
			},
			{
				Query:    "CALL DOLT_BRANCH('branch3');",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "DELETE FROM parent where pk = 2;",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:            "CALL DOLT_COMMIT('-afm', 'remove parent 2');",
				SkipResultsCheck: true,
			},
			{
				Query:    "CALL DOLT_CHECKOUT('branch2');",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "INSERT INTO OTHER VALUES (1);",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:            "CALL DOLT_COMMIT('-am', 'non-fk insert');",
				SkipResultsCheck: true,
			},
			{
				Query:    "CALL DOLT_CHECKOUT('main');",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "CALL DOLT_MERGE('branch2');",
				Expected: []sql.Row{{0, 1}},
			},
			{
				Query:    "SELECT violation_type, pk, parent_fk from dolt_constraint_violations_child;",
				Expected: []sql.Row{{uint16(1), 1, 1}},
			},
			{
				Query:    "COMMIT;",
				Expected: []sql.Row{},
			},
			{
				Query:          "CALL DOLT_COMMIT('-am', 'commit non-conflicting merge');",
				ExpectedErrStr: "error: the table(s) child have constraint violations",
			},
			{
				Query:            "CALL DOLT_COMMIT('-afm', 'commit non-conflicting merge');",
				SkipResultsCheck: true,
			},
			{
				Query:    "CALL DOLT_CHECKOUT('branch3');",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "INSERT INTO CHILD VALUES (2, 2);",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:            "CALL DOLT_COMMIT('-afm', 'add child of parent 2');",
				SkipResultsCheck: true,
			},
			{
				Query:    "CALL DOLT_CHECKOUT('main');",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "CALL DOLT_MERGE('branch3');",
				Expected: []sql.Row{{0, 1}},
			},
			{
				Query:    "SELECT violation_type, pk, parent_fk from dolt_constraint_violations_child;",
				Expected: []sql.Row{{uint16(1), 1, 1}, {uint16(1), 2, 2}},
			},
		},
	},
	{
		Name: "conflicting merge aborts when conflicts and violations already exist",
		SetUpScript: []string{
			"CREATE table parent (pk int PRIMARY KEY, col1 int);",
			"CREATE table child (pk int PRIMARY KEY, parent_fk int, FOREIGN KEY (parent_fk) REFERENCES parent(pk));",
			"CALL DOLT_ADD('.')",
			"INSERT INTO parent VALUES (1, 1), (2, 1);",
			"CALL DOLT_COMMIT('-am', 'create table with data');",
			"CALL DOLT_BRANCH('other');",
			"CALL DOLT_BRANCH('other2');",
			"UPDATE parent SET col1 = 2 where pk = 1;",
			"DELETE FROM parent where pk = 2;",
			"CALL DOLT_COMMIT('-am', 'updating col1 to 2 and remove pk = 2');",
			"CALL DOLT_CHECKOUT('other');",
			"UPDATE parent SET col1 = 3 where pk = 1;",
			"INSERT into child VALUEs (1, 2);",
			"CALL DOLT_COMMIT('-am', 'updating col1 to 3 and adding child of pk 2');",
			"CALL DOLT_CHECKOUT('other2')",
			"UPDATE parent SET col1 = 4 where pk = 1",
			"CALL DOLT_COMMIT('-am', 'updating col1 to 4');",
			"CALL DOLT_CHECKOUT('main');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SET dolt_force_transaction_commit = 1",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "CALL DOLT_MERGE('other');",
				Expected: []sql.Row{{0, 1}},
			},
			{
				Query:    "SELECT * from parent;",
				Expected: []sql.Row{{1, 2}},
			},
			{
				Query:    "SELECT * from child;",
				Expected: []sql.Row{{1, 2}},
			},
			{
				Query:    "SELECT base_col1, base_pk, our_col1, our_pk, their_col1, their_pk from dolt_conflicts_parent;",
				Expected: []sql.Row{{1, 1, 2, 1, 3, 1}},
			},
			{
				Query:    "SELECT violation_type, pk, parent_fk from dolt_constraint_violations_child;",
				Expected: []sql.Row{{uint16(1), 1, 2}},
			},
			// commit so we can merge again
			{
				Query:            "CALL DOLT_COMMIT('-afm', 'committing merge conflicts');",
				SkipResultsCheck: true,
			},
			{
				Query:          "CALL DOLT_MERGE('other2');",
				ExpectedErrStr: "existing unresolved conflicts would be overridden by new conflicts produced by merge. Please resolve them and try again",
			},
			{
				Query:    "SELECT * from parent;",
				Expected: []sql.Row{{1, 2}},
			},
			{
				Query:    "SELECT * from child;",
				Expected: []sql.Row{{1, 2}},
			},
			{
				Query:    "SELECT base_col1, base_pk, our_col1, our_pk, their_col1, their_pk from dolt_conflicts_parent;",
				Expected: []sql.Row{{1, 1, 2, 1, 3, 1}},
			},
			{
				Query:    "SELECT violation_type, pk, parent_fk from dolt_constraint_violations_child;",
				Expected: []sql.Row{{uint16(1), 1, 2}},
			},
		},
	},
	{
		Name: "non-conflicting / non-violating merge succeeds when conflicts and violations already exist",
		SetUpScript: []string{
			"CREATE table parent (pk int PRIMARY KEY, col1 int);",
			"CREATE table child (pk int PRIMARY KEY, parent_fk int, FOREIGN KEY (parent_fk) REFERENCES parent(pk));",
			"CALL DOLT_ADD('.')",
			"INSERT INTO parent VALUES (1, 1), (2, 1);",
			"CALL DOLT_COMMIT('-am', 'create table with data');",
			"CALL DOLT_BRANCH('other');",
			"CALL DOLT_BRANCH('other2');",
			"UPDATE parent SET col1 = 2 where pk = 1;",
			"DELETE FROM parent where pk = 2;",
			"CALL DOLT_COMMIT('-am', 'updating col1 to 2 and remove pk = 2');",
			"CALL DOLT_CHECKOUT('other');",
			"UPDATE parent SET col1 = 3 where pk = 1;",
			"INSERT into child VALUES (1, 2);",
			"CALL DOLT_COMMIT('-am', 'updating col1 to 3 and adding child of pk 2');",
			"CALL DOLT_CHECKOUT('other2')",
			"INSERT INTO parent values (3, 1);",
			"CALL DOLT_COMMIT('-am', 'insert parent with pk 3');",
			"CALL DOLT_CHECKOUT('main');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SET dolt_force_transaction_commit = 1;",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "CALL DOLT_MERGE('other');",
				Expected: []sql.Row{{0, 1}},
			},
			{
				Query:    "SELECT * from parent;",
				Expected: []sql.Row{{1, 2}},
			},
			{
				Query:    "SELECT * from child;",
				Expected: []sql.Row{{1, 2}},
			},
			{
				Query:    "SELECT base_col1, base_pk, our_col1, our_pk, their_col1, their_pk from dolt_conflicts_parent;",
				Expected: []sql.Row{{1, 1, 2, 1, 3, 1}},
			},
			{
				Query:    "SELECT violation_type, pk, parent_fk from dolt_constraint_violations_child;",
				Expected: []sql.Row{{uint16(1), 1, 2}},
			},
			// commit so we can merge again
			{
				Query:            "CALL DOLT_COMMIT('-afm', 'committing merge conflicts');",
				SkipResultsCheck: true,
			},
			{
				Query:    "CALL DOLT_MERGE('other2');",
				Expected: []sql.Row{{0, 1}},
			},
			{
				Query:    "SELECT * from parent;",
				Expected: []sql.Row{{1, 2}, {3, 1}},
			},
			{
				Query:    "SELECT * from child;",
				Expected: []sql.Row{{1, 2}},
			},
			{
				Query:    "SELECT base_col1, base_pk, our_col1, our_pk, their_col1, their_pk from dolt_conflicts_parent;",
				Expected: []sql.Row{{1, 1, 2, 1, 3, 1}},
			},
			{
				Query:    "SELECT violation_type, pk, parent_fk from dolt_constraint_violations_child;",
				Expected: []sql.Row{{uint16(1), 1, 2}},
			},
		},
	},
	// Unique key violations
	{
		Name: "unique key violations that already exist in the left abort the merge with an error",
		SetUpScript: []string{
			"SET dolt_force_transaction_commit = on;",
			"CREATE TABLE t (pk int PRIMARY KEY, col1 int);",
			"CALL DOLT_ADD('.')",
			"CALL DOLT_COMMIT('-am', 'table');",
			"CALL DOLT_BRANCH('right');",
			"INSERT INTO t VALUES (1, 1), (2, 1);",
			"CALL DOLT_COMMIT('-am', 'data');",

			"CALL DOLT_CHECKOUT('right');",
			"ALTER TABLE t ADD UNIQUE col1_uniq (col1);",
			"CALL DOLT_COMMIT('-am', 'unqiue constraint');",

			"CALL DOLT_CHECKOUT('main');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "CALL DOLT_MERGE('right');",
				ExpectedErrStr: "duplicate unique key given: [1]",
			},
			{
				Query:    "SELECT * from t",
				Expected: []sql.Row{{1, 1}, {2, 1}},
			},
			{
				Query: "show create table t",
				Expected: []sql.Row{{"t",
					"CREATE TABLE `t` (\n" +
						"  `pk` int NOT NULL,\n" +
						"  `col1` int,\n" +
						"  PRIMARY KEY (`pk`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	// not a base case, but helpful to understand...
	{
		Name: "right adds a unique key constraint and fixes existing violations. On merge, because left still has the violation, merge is aborted.",
		SetUpScript: []string{
			"SET dolt_force_transaction_commit = on;",
			"CREATE TABLE t (pk int PRIMARY KEY, col1 int);",
			"CALL DOLT_ADD('.');",
			"INSERT INTO t VALUES (1, 1), (2, 1);",
			"CALL DOLT_COMMIT('-am', 'table and data');",

			"CALL DOLT_CHECKOUT('-b', 'right');",
			"UPDATE t SET col1 = 2 where pk = 2;",
			"ALTER TABLE t ADD UNIQUE col1_uniq (col1);",
			"CALL DOLT_COMMIT('-am', 'right adds a unique index');",

			"CALL DOLT_CHECKOUT('main');",
			"INSERT INTO t VALUES (3, 3);",
			"CALL DOLT_COMMIT('-am', 'left edit');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "CALL DOLT_MERGE('right');",
				ExpectedErrStr: "duplicate unique key given: [1]",
			},
		},
	},
}

var verifyConstraintsSetupScript = []string{
	"CREATE TABLE parent3 (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX (v1));",
	"CREATE TABLE child3 (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name1 FOREIGN KEY (v1) REFERENCES parent3 (v1));",
	"CREATE TABLE parent4 (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX (v1));",
	"CREATE TABLE child4 (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name2 FOREIGN KEY (v1) REFERENCES parent4 (v1));",
	"CALL DOLT_ADD('.')",
	"INSERT INTO parent3 VALUES (1, 1);",
	"INSERT INTO parent4 VALUES (2, 2);",
	"SET foreign_key_checks=0;",
	"INSERT INTO child3 VALUES (1, 1), (2, 2);",
	"INSERT INTO child4 VALUES (1, 1), (2, 2);",
	"SET foreign_key_checks=1;",
	"CALL DOLT_COMMIT('-afm', 'has fk violations');",
	`
	CREATE TABLE parent1 (
  		pk BIGINT PRIMARY KEY,
  		v1 BIGINT,
  		INDEX (v1)
	);`,
	`
	CREATE TABLE parent2 (
	  pk BIGINT PRIMARY KEY,
	  v1 BIGINT,
	  INDEX (v1)
	);`,
	`
	CREATE TABLE child1 (
	  pk BIGINT PRIMARY KEY,
	  parent1_v1 BIGINT,
	  parent2_v1 BIGINT,
	  CONSTRAINT child1_parent1 FOREIGN KEY (parent1_v1) REFERENCES parent1 (v1),
	  CONSTRAINT child1_parent2 FOREIGN KEY (parent2_v1) REFERENCES parent2 (v1)
	);`,
	`
	CREATE TABLE child2 (
	  pk BIGINT PRIMARY KEY,
	  parent2_v1 BIGINT,
	  CONSTRAINT child2_parent2 FOREIGN KEY (parent2_v1) REFERENCES parent2 (v1)
	);`,
	"INSERT INTO parent1 VALUES (1,1), (2,2), (3,3);",
	"INSERT INTO parent2 VALUES (1,1), (2,2), (3,3);",
	"INSERT INTO child1 VALUES (1,1,1), (2,2,2);",
	"INSERT INTO child2 VALUES (2,2), (3,3);",
	"SET foreign_key_checks=0;",
	"INSERT INTO child3 VALUES (3, 3);",
	"INSERT INTO child4 VALUES (3, 3);",
	"SET foreign_key_checks=1;",
}

var DoltVerifyConstraintsTestScripts = []queries.ScriptTest{
	{
		Name:        "verify-constraints: SQL no violations",
		SetUpScript: verifyConstraintsSetupScript,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT CONSTRAINTS_VERIFY('child1')",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "SELECT * from dolt_constraint_violations",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT CONSTRAINTS_VERIFY('--all', 'child1');",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "SELECT * from dolt_constraint_violations",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name:        "verify-constraints: Stored Procedure no violations",
		SetUpScript: verifyConstraintsSetupScript,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_VERIFY_CONSTRAINTS('child1')",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "SELECT * from dolt_constraint_violations",
				Expected: []sql.Row{},
			},
			{
				Query:    "CALL DOLT_VERIFY_CONSTRAINTS('--all', 'child1');",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "SELECT * from dolt_constraint_violations",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name:        "verify-constraints: SQL no named tables",
		SetUpScript: verifyConstraintsSetupScript,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "SET DOLT_FORCE_TRANSACTION_COMMIT = 1;",
				SkipResultsCheck: true,
			},
			{
				Query:    "SELECT CONSTRAINTS_VERIFY();",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT * from dolt_constraint_violations;",
				Expected: []sql.Row{{"child3", uint64(1)}, {"child4", uint64(1)}},
			},
		},
	},
	{
		Name:        "verify-constraints: Stored Procedure no named tables",
		SetUpScript: verifyConstraintsSetupScript,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "SET DOLT_FORCE_TRANSACTION_COMMIT = 1;",
				SkipResultsCheck: true,
			},
			{
				Query:    "CALL DOLT_VERIFY_CONSTRAINTS();",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT * from dolt_constraint_violations;",
				Expected: []sql.Row{{"child3", uint64(1)}, {"child4", uint64(1)}},
			},
		},
	},
	{
		Name:        "verify-constraints: SQL named table",
		SetUpScript: verifyConstraintsSetupScript,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "SET DOLT_FORCE_TRANSACTION_COMMIT = 1;",
				SkipResultsCheck: true,
			},
			{
				Query:    "SELECT CONSTRAINTS_VERIFY('child3');",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT * from dolt_constraint_violations;",
				Expected: []sql.Row{{"child3", uint64(1)}},
			},
		},
	},
	{
		Name:        "verify-constraints: Stored Procedure named table",
		SetUpScript: verifyConstraintsSetupScript,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "SET DOLT_FORCE_TRANSACTION_COMMIT = 1;",
				SkipResultsCheck: true,
			},
			{
				Query:    "CALL DOLT_VERIFY_CONSTRAINTS('child3');",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT * from dolt_constraint_violations;",
				Expected: []sql.Row{{"child3", uint64(1)}},
			},
		},
	},
	{
		Name:        "verify-constraints: SQL named tables",
		SetUpScript: verifyConstraintsSetupScript,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "SET DOLT_FORCE_TRANSACTION_COMMIT = 1;",
				SkipResultsCheck: true,
			},
			{
				Query:    "SELECT CONSTRAINTS_VERIFY('child3', 'child4');",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT * from dolt_constraint_violations;",
				Expected: []sql.Row{{"child3", uint64(1)}, {"child4", uint64(1)}},
			},
		},
	},
	{
		Name:        "verify-constraints: Stored Procedure named tables",
		SetUpScript: verifyConstraintsSetupScript,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "SET DOLT_FORCE_TRANSACTION_COMMIT = 1;",
				SkipResultsCheck: true,
			},
			{
				Query:    "SELECT CONSTRAINTS_VERIFY('child3', 'child4');",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT * from dolt_constraint_violations;",
				Expected: []sql.Row{{"child3", uint64(1)}, {"child4", uint64(1)}},
			},
		},
	},
	{
		Name:        "verify-constraints: SQL --all no named tables",
		SetUpScript: verifyConstraintsSetupScript,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "SET DOLT_FORCE_TRANSACTION_COMMIT = 1;",
				SkipResultsCheck: true,
			},
			{
				Query:    "SELECT CONSTRAINTS_VERIFY('--all');",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT * from dolt_constraint_violations;",
				Expected: []sql.Row{{"child3", uint64(2)}, {"child4", uint64(2)}},
			},
		},
	},
	{
		Name:        "verify-constraints: Stored Procedure --all no named tables",
		SetUpScript: verifyConstraintsSetupScript,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "SET DOLT_FORCE_TRANSACTION_COMMIT = 1;",
				SkipResultsCheck: true,
			},
			{
				Query:    "CALL DOLT_VERIFY_CONSTRAINTS('--all');",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT * from dolt_constraint_violations;",
				Expected: []sql.Row{{"child3", uint64(2)}, {"child4", uint64(2)}},
			},
		},
	},
	{
		Name:        "verify-constraints: SQL --all named table",
		SetUpScript: verifyConstraintsSetupScript,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "SET DOLT_FORCE_TRANSACTION_COMMIT = 1;",
				SkipResultsCheck: true,
			},
			{
				Query:    "SELECT CONSTRAINTS_VERIFY('--all', 'child3');",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT * from dolt_constraint_violations;",
				Expected: []sql.Row{{"child3", uint64(2)}},
			},
		},
	},
	{
		Name:        "verify-constraints: Stored Procedure --all named table",
		SetUpScript: verifyConstraintsSetupScript,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "SET DOLT_FORCE_TRANSACTION_COMMIT = 1;",
				SkipResultsCheck: true,
			},
			{
				Query:    "CALL DOLT_VERIFY_CONSTRAINTS('--all', 'child3');",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT * from dolt_constraint_violations;",
				Expected: []sql.Row{{"child3", uint64(2)}},
			},
		},
	},
	{
		Name:        "verify-constraints: SQL --all named tables",
		SetUpScript: verifyConstraintsSetupScript,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "SET DOLT_FORCE_TRANSACTION_COMMIT = 1;",
				SkipResultsCheck: true,
			},
			{
				Query:    "SELECT CONSTRAINTS_VERIFY('--all', 'child3', 'child4');",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT * from dolt_constraint_violations;",
				Expected: []sql.Row{{"child3", uint64(2)}, {"child4", uint64(2)}},
			},
		},
	},
	{
		Name:        "verify-constraints: Stored Procedure --all named tables",
		SetUpScript: verifyConstraintsSetupScript,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "SET DOLT_FORCE_TRANSACTION_COMMIT = 1;",
				SkipResultsCheck: true,
			},
			{
				Query:    "CALL DOLT_VERIFY_CONSTRAINTS('--all', 'child3', 'child4');",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT * from dolt_constraint_violations;",
				Expected: []sql.Row{{"child3", uint64(2)}, {"child4", uint64(2)}},
			},
		},
	},
	{
		Name:        "verify-constraints: SQL --output-only",
		SetUpScript: verifyConstraintsSetupScript,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT CONSTRAINTS_VERIFY('--output-only', 'child3', 'child4');",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT * from dolt_constraint_violations;",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name:        "verify-constraints: Stored Procedures --output-only",
		SetUpScript: verifyConstraintsSetupScript,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_VERIFY_CONSTRAINTS('--output-only', 'child3', 'child4');",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT * from dolt_constraint_violations;",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name:        "verify-constraints: SQL --all --output-only",
		SetUpScript: verifyConstraintsSetupScript,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT CONSTRAINTS_VERIFY('--all', '--output-only', 'child3', 'child4');",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT * from dolt_constraint_violations;",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name:        "verify-constraints: Stored Procedures --all --output-only",
		SetUpScript: verifyConstraintsSetupScript,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_VERIFY_CONSTRAINTS('--all', '--output-only', 'child3', 'child4');",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT * from dolt_constraint_violations;",
				Expected: []sql.Row{},
			},
		},
	},
}
