// Copyright 2021 Dolthub, Inc.
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
	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dfunctions"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

// DoltScripts are script tests specific to Dolt (not the engine in general), e.g. by involving Dolt functions. Break
// this slice into others with good names as it grows.
var DoltScripts = []enginetest.ScriptTest{
	{
		Name: "test as of indexed join (https://github.com/dolthub/dolt/issues/2189)",
		SetUpScript: []string{
			"create table a (pk int primary key, c1 int)",
			"insert into a values (1,1), (2,2), (3,3)",
			"select DOLT_COMMIT('-a', '-m', 'first commit')",
			"insert into a values (4,4), (5,5), (6,6)",
			"select DOLT_COMMIT('-a', '-m', 'second commit')",
			"set @second_commit = (select commit_hash from dolt_log order by date desc limit 1)",
			"set @first_commit = (select commit_hash from dolt_log order by date desc limit 1,1)",
		},
		Assertions: []enginetest.ScriptTestAssertion{
			{
				Query: "select a1.* from a as of @second_commit a1 " +
					"left join a as of @first_commit a2 on a1.pk = a2.pk where a2.pk is null order by 1",
				Expected: []sql.Row{
					{4, 4},
					{5, 5},
					{6, 6},
				},
			},
			{
				Query: "select a1.* from a as of @second_commit a1 " +
					"left join a as of @second_commit a2 on a1.pk = a2.pk where a2.pk is null order by 1",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "Show create table with various keys and constraints",
		SetUpScript: []string{
			"create table t1(a int primary key, b varchar(10) not null default 'abc')",
			"alter table t1 add constraint ck1 check (b like '%abc%')",
			"create index t1b on t1(b)",
			"create table t2(c int primary key, d varchar(10))",
			"alter table t2 add constraint fk1 foreign key (d) references t1 (b)",
			"alter table t2 add constraint t2du unique (d)",
		},
		Assertions: []enginetest.ScriptTestAssertion{
			{
				Query: "show create table t1",
				Expected: []sql.Row{
					{"t1", "CREATE TABLE `t1` (\n" +
						"  `a` int NOT NULL,\n" +
						"  `b` varchar(10) NOT NULL DEFAULT \"abc\",\n" +
						"  PRIMARY KEY (`a`),\n" +
						"  KEY `t1b` (`b`),\n" +
						"  CONSTRAINT `ck1` CHECK (`b` LIKE \"%abc%\")\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"},
				},
			},
			{
				Query: "show create table t2",
				Expected: []sql.Row{
					{"t2", "CREATE TABLE `t2` (\n" +
						"  `c` int NOT NULL,\n" +
						"  `d` varchar(10),\n" +
						"  PRIMARY KEY (`c`),\n" +
						"  UNIQUE KEY `d_1` (`d`),\n" +
						"  CONSTRAINT `fk1` FOREIGN KEY (`d`) REFERENCES `t1` (`b`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"},
				},
			},
		},
	},
}

var ScopedDoltHistoryScriptTests = []enginetest.ScriptTest{
	{
		Name: "scoped-dolt-history-system-table: filtering results on non-pk tables",
		SetUpScript: []string{
			"create table foo1 (n int, abcd text);",
			"insert into foo1 values (1, 'Eins'), (2, 'Zwei'), (3, 'Drei');",
			"select dolt_commit('-am', 'inserting into foo1');",
		},
		Assertions: []enginetest.ScriptTestAssertion{
			{
				Query:    "select n, abcd FROM DOLT_HISTORY_foo1 where n=1;",
				Expected: []sql.Row{{1, "Eins"}},
			},
		},
	},
}

var DoltMerge = []enginetest.ScriptTest{
	{
		Name: "DOLT_MERGE ff correctly works with autocommit off",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key)",
			"INSERT INTO test VALUES (0),(1),(2);",
			"SET autocommit = 0",
			"SELECT DOLT_COMMIT('-a', '-m', 'Step 1');",
			"SELECT DOLT_CHECKOUT('-b', 'feature-branch')",
			"INSERT INTO test VALUES (3);",
			"UPDATE test SET pk=1000 WHERE pk=0;",
			"SELECT DOLT_COMMIT('-a', '-m', 'this is a ff');",
			"SELECT DOLT_CHECKOUT('main');",
		},
		Assertions: []enginetest.ScriptTestAssertion{
			{
				// FF-Merge
				Query:    "SELECT DOLT_MERGE('feature-branch')",
				Expected: []sql.Row{{1}},
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
		Name: "DOLT_MERGE no-ff correctly works with autocommit off",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key)",
			"INSERT INTO test VALUES (0),(1),(2);",
			"SET autocommit = 0",
			"SELECT DOLT_COMMIT('-a', '-m', 'Step 1');",
			"SELECT DOLT_CHECKOUT('-b', 'feature-branch')",
			"INSERT INTO test VALUES (3);",
			"UPDATE test SET pk=1000 WHERE pk=0;",
			"SELECT DOLT_COMMIT('-a', '-m', 'this is a ff');",
			"SELECT DOLT_CHECKOUT('main');",
		},
		Assertions: []enginetest.ScriptTestAssertion{
			{
				// No-FF-Merge
				Query:    "SELECT DOLT_MERGE('feature-branch', '-no-ff', '-m', 'this is a no-ff')",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT * from dolt_status",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT COUNT(*) FROM dolt_log",
				Expected: []sql.Row{{4}}, // includes the merge commit created by no-ff
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
		Name: "DOLT_MERGE without conflicts correctly works with autocommit off",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key)",
			"INSERT INTO test VALUES (0),(1),(2);",
			"SET autocommit = 0",
			"SELECT DOLT_COMMIT('-a', '-m', 'Step 1');",
			"SELECT DOLT_CHECKOUT('-b', 'feature-branch')",
			"INSERT INTO test VALUES (3);",
			"UPDATE test SET pk=1000 WHERE pk=0;",
			"SELECT DOLT_COMMIT('-a', '-m', 'this is a normal commit');",
			"SELECT DOLT_CHECKOUT('main');",
			"INSERT INTO test VALUES (5),(6),(7);",
			"SELECT DOLT_COMMIT('-a', '-m', 'add some more values');",
		},
		Assertions: []enginetest.ScriptTestAssertion{
			{
				Query:    "SELECT DOLT_MERGE('feature-branch', '-m', 'this is a merge')",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT COUNT(*) from dolt_status",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT COUNT(*) FROM dolt_log",
				Expected: []sql.Row{{3}},
			},
			{
				Query:    "select message from dolt_log order by date DESC LIMIT 1;",
				Expected: []sql.Row{{"add some more values"}},
			},
			{
				Query:       "SELECT DOLT_CHECKOUT('-b', 'other-branch')",
				ExpectedErr: dsess.ErrWorkingSetChanges,
			},
		},
	},
	{
		Name: "DOLT_MERGE with conflicts can be correctly resolved when autocommit is off",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key, val int)",
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
		Assertions: []enginetest.ScriptTestAssertion{
			{
				Query:    "SELECT DOLT_MERGE('feature-branch', '-m', 'this is a merge')",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "SELECT * from dolt_status",
				Expected: []sql.Row{{"test", true, "modified"}, {"test", false, "conflict"}},
			},
			{
				Query:    "SELECT COUNT(*) FROM dolt_log",
				Expected: []sql.Row{{3}},
			},
			{
				Query:    "select message from dolt_log order by date DESC LIMIT 1;",
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
		Name: "DOLT_MERGE ff & squash correctly works with autocommit off",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key)",
			"INSERT INTO test VALUES (0),(1),(2);",
			"SET autocommit = 0",
			"SELECT DOLT_COMMIT('-a', '-m', 'Step 1');",
			"SELECT DOLT_CHECKOUT('-b', 'feature-branch')",
			"INSERT INTO test VALUES (3);",
			"UPDATE test SET pk=1000 WHERE pk=0;",
			"SELECT DOLT_COMMIT('-a', '-m', 'this is a ff');",
			"SELECT DOLT_CHECKOUT('main');",
		},
		Assertions: []enginetest.ScriptTestAssertion{
			{
				Query:    "SELECT DOLT_MERGE('feature-branch', '--squash')",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT count(*) from dolt_status",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT COUNT(*) FROM dolt_log",
				Expected: []sql.Row{{2}},
			},
			{
				Query:    "SELECT * FROM test order by pk",
				Expected: []sql.Row{{1}, {2}, {3}, {1000}},
			},
		},
	},
	{
		Name: "DOLT_MERGE ff & squash with a checkout in between",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key)",
			"INSERT INTO test VALUES (0),(1),(2);",
			"SET autocommit = 0",
			"SELECT DOLT_COMMIT('-a', '-m', 'Step 1');",
			"SELECT DOLT_CHECKOUT('-b', 'feature-branch')",
			"INSERT INTO test VALUES (3);",
			"UPDATE test SET pk=1000 WHERE pk=0;",
			"SELECT DOLT_COMMIT('-a', '-m', 'this is a ff');",
			"SELECT DOLT_CHECKOUT('main');",
		},
		Assertions: []enginetest.ScriptTestAssertion{
			{
				Query:    "SELECT DOLT_MERGE('feature-branch', '--squash')",
				Expected: []sql.Row{{1}},
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
		Name: "DOLT_MERGE ff",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key)",
			"INSERT INTO test VALUES (0),(1),(2);",
			"SELECT DOLT_COMMIT('-a', '-m', 'Step 1');",
			"SELECT DOLT_CHECKOUT('-b', 'feature-branch')",
			"INSERT INTO test VALUES (3);",
			"UPDATE test SET pk=1000 WHERE pk=0;",
			"SELECT DOLT_COMMIT('-a', '-m', 'this is a ff');",
			"SELECT DOLT_CHECKOUT('main');",
		},
		Assertions: []enginetest.ScriptTestAssertion{
			{
				// FF-Merge
				Query:    "SELECT DOLT_MERGE('feature-branch')",
				Expected: []sql.Row{{1}},
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
		Name: "DOLT_MERGE no-ff",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key)",
			"INSERT INTO test VALUES (0),(1),(2);",
			"SELECT DOLT_COMMIT('-a', '-m', 'Step 1');",
			"SELECT DOLT_CHECKOUT('-b', 'feature-branch')",
			"INSERT INTO test VALUES (3);",
			"UPDATE test SET pk=1000 WHERE pk=0;",
			"SELECT DOLT_COMMIT('-a', '-m', 'this is a ff');",
			"SELECT DOLT_CHECKOUT('main');",
		},
		Assertions: []enginetest.ScriptTestAssertion{
			{
				// No-FF-Merge
				Query:    "SELECT DOLT_MERGE('feature-branch', '-no-ff', '-m', 'this is a no-ff')",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT * from dolt_status",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT COUNT(*) FROM dolt_log",
				Expected: []sql.Row{{4}}, // includes the merge commit created by no-ff
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
		Name: "DOLT_MERGE with no conflicts works",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key)",
			"INSERT INTO test VALUES (0),(1),(2);",
			"SELECT DOLT_COMMIT('-a', '-m', 'Step 1');",
			"SELECT DOLT_CHECKOUT('-b', 'feature-branch')",
			"INSERT INTO test VALUES (3);",
			"UPDATE test SET pk=1000 WHERE pk=0;",
			"SELECT DOLT_COMMIT('-a', '-m', 'this is a normal commit');",
			"SELECT DOLT_CHECKOUT('main');",
			"INSERT INTO test VALUES (5),(6),(7);",
			"SELECT DOLT_COMMIT('-a', '-m', 'add some more values');",
		},
		Assertions: []enginetest.ScriptTestAssertion{
			{
				Query:    "SELECT DOLT_MERGE('feature-branch', '-m', 'this is a merge')",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT COUNT(*) from dolt_status",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT COUNT(*) FROM dolt_log",
				Expected: []sql.Row{{3}},
			},
			{
				Query:    "select message from dolt_log order by date DESC LIMIT 1;",
				Expected: []sql.Row{{"add some more values"}},
			},
			{
				Query:    "SELECT DOLT_CHECKOUT('-b', 'other-branch')",
				Expected: []sql.Row{{0}},
			},
		},
	},
	{
		Name: "DOLT_MERGE with conflict is queryable and commitable until dolt_allow_commit_conflicts is turned off",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key, val int)",
			"INSERT INTO test VALUES (0, 0)",
			"SELECT DOLT_COMMIT('-a', '-m', 'Step 1');",
			"SELECT DOLT_CHECKOUT('-b', 'feature-branch')",
			"INSERT INTO test VALUES (1, 1);",
			"UPDATE test SET val=1000 WHERE pk=0;",
			"SELECT DOLT_COMMIT('-a', '-m', 'this is a normal commit');",
			"SELECT DOLT_CHECKOUT('main');",
			"UPDATE test SET val=1001 WHERE pk=0;",
			"SELECT DOLT_COMMIT('-a', '-m', 'update a value');",
		},
		Assertions: []enginetest.ScriptTestAssertion{
			{
				Query:    "SELECT DOLT_MERGE('feature-branch')",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "SELECT count(*) from dolt_conflicts_test",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT DOLT_MERGE('--abort')",
				Expected: []sql.Row{{1}},
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
				ExpectedErrStr: doltdb.ErrUnresolvedConflicts.Error(),
			},
			{
				Query:          "SELECT count(*) from dolt_conflicts_test", // Commit allows queries when flags are set.
				ExpectedErrStr: doltdb.ErrUnresolvedConflicts.Error(),
			},
		},
	},
	{
		Name: "DOLT_MERGE with conflicts can be aborted when autocommit is off",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key, val int)",
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
		Assertions: []enginetest.ScriptTestAssertion{
			{
				Query:    "SELECT DOLT_MERGE('feature-branch', '-m', 'this is a merge')",
				Expected: []sql.Row{{0}},
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
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT * from dolt_status",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT COUNT(*) FROM dolt_log",
				Expected: []sql.Row{{3}},
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
		Name: "DOLT_MERGE complains when a merge overrides local changes",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key, val int)",
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
		Assertions: []enginetest.ScriptTestAssertion{
			{
				Query:       "SELECT DOLT_MERGE('feature-branch', '-m', 'this is a merge')",
				ExpectedErr: dfunctions.ErrUncommittedChanges,
			},
		},
	},
}

var DiffTableTests = []enginetest.ScriptTest{
	{
		Name: "base case: added rows",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 int, c2 int);",
			"insert into t values (1, 2, 3), (4, 5, 6);",
			"set @Commit1 = (select DOLT_COMMIT('-am', 'creating table t'));",
		},
		Assertions: []enginetest.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM DOLT_DIFF_t;",
				Expected: []sql.Row{{2}},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit1 ORDER BY to_pk;",
				Expected: []sql.Row{
					{1, 2, 3, nil, nil, nil, "added"},
					{4, 5, 6, nil, nil, nil, "added"},
				},
			},
		},
	},
	{
		Name: "base case: modified rows",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 int, c2 int);",
			"insert into t values (1, 2, 3), (4, 5, 6);",
			"set @Commit1 = (select DOLT_COMMIT('-am', 'creating table t'));",

			"update t set c2=0 where pk=1",
			"set @Commit2 = (select DOLT_COMMIT('-am', 'modifying row'));",
		},
		Assertions: []enginetest.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM DOLT_DIFF_t;",
				Expected: []sql.Row{{3}},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit2 ORDER BY to_pk;",
				Expected: []sql.Row{
					{1, 2, 0, 1, 2, 3, "modified"},
				},
			},
		},
	},
	{
		Name: "base case: deleted row",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 int, c2 int);",
			"insert into t values (1, 2, 3), (4, 5, 6);",
			"set @Commit1 = (select DOLT_COMMIT('-am', 'creating table t'));",

			"delete from t where pk=1",
			"set @Commit2 = (select DOLT_COMMIT('-am', 'modifying row'));",
		},
		Assertions: []enginetest.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM DOLT_DIFF_t;",
				Expected: []sql.Row{{3}},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit2 ORDER BY to_pk;",
				Expected: []sql.Row{
					{nil, nil, nil, 1, 2, 3, "removed"},
				},
			},
		},
	},
	{
		// In this case, we do not expect to see the old/dropped table included in the dolt_diff_table output
		Name: "table drop and recreate with overlapping schema",
		SetUpScript: []string{
			"create table t (pk int primary key, c int);",
			"insert into t values (1, 2), (3, 4);",
			"set @Commit1 = (select DOLT_COMMIT('-am', 'creating table t'));",

			"drop table t;",
			"set @Commit2 = (select DOLT_COMMIT('-am', 'dropping table t'));",

			"create table t (pk int primary key, c int);",
			"insert into t values (100, 200), (300, 400);",
			"set @Commit3 = (select DOLT_COMMIT('-am', 'recreating table t'));",
		},
		Assertions: []enginetest.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM DOLT_DIFF_t",
				Expected: []sql.Row{{2}},
			},
			{
				Query: "SELECT to_pk, to_c, from_pk, from_c, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit3 ORDER BY to_pk;",
				Expected: []sql.Row{
					{100, 200, nil, nil, "added"},
					{300, 400, nil, nil, "added"},
				},
			},
		},
	},
	{
		// When a column is dropped we should see the column's value set to null in that commit
		Name: "column drop",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 int, c2 int);",
			"insert into t values (1, 2, 3), (4, 5, 6);",
			"set @Commit1 = (select DOLT_COMMIT('-am', 'creating table t'));",

			"alter table t drop column c1;",
			"set @Commit2 = (select DOLT_COMMIT('-am', 'dropping column c'));",
		},
		Assertions: []enginetest.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM DOLT_DIFF_t;",
				Expected: []sql.Row{{4}},
			},
			{
				Query: "SELECT to_pk, to_c2, from_pk, from_c2 FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit1 ORDER BY to_pk;",
				Expected: []sql.Row{
					{1, 3, nil, nil},
					{4, 6, nil, nil},
				},
			},
			{
				Query: "SELECT to_pk, to_c2, from_pk, from_c2 FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit2 ORDER BY to_pk;",
				Expected: []sql.Row{
					{1, 3, 1, 3},
					{4, 6, 4, 6},
				},
			},
		},
	},
	{
		// When a column is dropped and recreated with the same type, we expect it to be included in dolt_diff output
		Name: "column drop and recreate with same type",
		SetUpScript: []string{
			"create table t (pk int primary key, c int);",
			"insert into t values (1, 2), (3, 4);",
			"set @Commit1 = (select DOLT_COMMIT('-am', 'creating table t'));",

			"alter table t drop column c;",
			"set @Commit2 = (select DOLT_COMMIT('-am', 'dropping column c'));",

			"alter table t add column c int;",
			"insert into t values (100, 101);",
			"set @Commit3 = (select DOLT_COMMIT('-am', 'inserting into t'));",
		},
		Assertions: []enginetest.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM DOLT_DIFF_t;",
				Expected: []sql.Row{{5}},
			},
			{
				Query: "SELECT to_pk, to_c, from_pk, from_c, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit1 ORDER BY to_pk;",
				Expected: []sql.Row{
					{1, 2, nil, nil, "added"},
					{3, 4, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c, from_pk, from_c, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit2 ORDER BY to_pk;",
				Expected: []sql.Row{
					{1, nil, 1, 2, "modified"},
					{3, nil, 3, 4, "modified"},
				},
			},
			{
				Query: "SELECT to_pk, to_c, from_pk, from_c, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit3 ORDER BY to_pk;",
				Expected: []sql.Row{
					{100, 101, nil, nil, "added"},
				},
			},
		},
	},
	{
		// When a column is dropped and then another column with the same type is renamed to that name, we expect it to be included in dolt_diff output
		Name: "column drop, then rename column with same type to same name",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 int, c2 int);",
			"insert into t values (1, 2, 3), (4, 5, 6);",
			"set @Commit1 = (select DOLT_COMMIT('-am', 'creating table t'));",

			"alter table t drop column c1;",
			"set @Commit2 = (select DOLT_COMMIT('-am', 'dropping column c1'));",

			"alter table t rename column c2 to c1;",
			"insert into t values (100, 101);",
			"set @Commit3 = (select DOLT_COMMIT('-am', 'inserting into t'));",
		},
		Assertions: []enginetest.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM DOLT_DIFF_t;",
				Expected: []sql.Row{{5}},
			},
			{
				Query: "SELECT to_pk, to_c1, from_pk, from_c1, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit1 ORDER BY to_pk;",
				Expected: []sql.Row{
					{1, 3, nil, nil, "added"},
					{4, 6, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, from_pk, from_c1, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit2 ORDER BY to_pk;",
				Expected: []sql.Row{
					{1, 3, 1, 3, "modified"},
					{4, 6, 4, 6, "modified"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, from_pk, from_c1, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit3 ORDER BY to_pk;",
				Expected: []sql.Row{
					{100, 101, nil, nil, "added"},
				},
			},
		},
	},
	{
		// When a column is dropped and recreated with a different type, we expect only the new column
		// to be included in dolt_diff output, with previous values coerced (with any warnings reported) to the new type
		Name: "column drop and recreate with different type that can be coerced (int -> string)",
		SetUpScript: []string{
			"create table t (pk int primary key, c int);",
			"insert into t values (1, 2), (3, 4);",
			"set @Commit1 = (select DOLT_COMMIT('-am', 'creating table t'));",

			"alter table t drop column c;",
			"set @Commit2 = (select DOLT_COMMIT('-am', 'dropping column c'));",

			"alter table t add column c text;",
			"insert into t values (100, '101');",
			"set @Commit3 = (select DOLT_COMMIT('-am', 're-adding column c'));",
		},
		Assertions: []enginetest.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM DOLT_DIFF_t;",
				Expected: []sql.Row{{5}},
			},
			{
				Query: "SELECT to_pk, to_c, from_pk, from_c, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit1 ORDER BY to_pk;",
				Expected: []sql.Row{
					{1, "2", nil, nil, "added"},
					{3, "4", nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c, from_pk, from_c, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit2 ORDER BY to_pk;",
				Expected: []sql.Row{
					{1, nil, 1, "2", "modified"},
					{3, nil, 3, "4", "modified"},
				},
			},
			{
				Query: "SELECT to_pk, to_c, from_pk, from_c, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit3 ORDER BY to_pk;",
				Expected: []sql.Row{
					{100, "101", nil, nil, "added"},
				},
			},
		},
	},
	{
		Name: "column drop and recreate with different type that can NOT be coerced (string -> int)",
		SetUpScript: []string{
			"create table t (pk int primary key, c text);",
			"insert into t values (1, 'two'), (3, 'four');",
			"set @Commit1 = (select DOLT_COMMIT('-am', 'creating table t'));",

			"alter table t drop column c;",
			"set @Commit2 = (select DOLT_COMMIT('-am', 'dropping column c'));",

			"alter table t add column c int;",
			"insert into t values (100, 101);",
			"set @Commit3 = (select DOLT_COMMIT('-am', 're-adding column c'));",
		},
		Assertions: []enginetest.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM DOLT_DIFF_t;",
				Expected: []sql.Row{{5}},
			},
			{
				Query: "SELECT to_pk, to_c, from_pk, from_c, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit1 ORDER BY to_pk;",
				Expected: []sql.Row{
					{1, nil, nil, nil, "added"},
					{3, nil, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c, from_pk, from_c, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit2 ORDER BY to_pk;",
				Expected: []sql.Row{
					{1, nil, 1, nil, "modified"},
					{3, nil, 3, nil, "modified"},
				},
			},
			{
				Query: "SELECT to_pk, to_c, from_pk, from_c, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit3 ORDER BY to_pk;",
				Expected: []sql.Row{
					{100, 101, nil, nil, "added"},
				},
			},
			{
				Query:                           "select * from dolt_diff_t;",
				ExpectedWarning:                 1105,
				ExpectedWarningsCount:           4,
				ExpectedWarningMessageSubstring: "unable to coerce value from field",
				SkipResultsCheck:                true,
			},
		},
	},
	{
		Name: "multiple column renames",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 int);",
			"insert into t values (1, 2);",
			"set @Commit1 = (select DOLT_COMMIT('-am', 'creating table t'));",

			"alter table t rename column c1 to c2;",
			"insert into t values (3, 4);",
			"set @Commit2 = (select DOLT_COMMIT('-am', 'renaming c1 to c2'));",

			"alter table t drop column c2;",
			"set @Commit3 = (select DOLT_COMMIT('-am', 'dropping column c2'));",

			"alter table t add column c2 int;",
			"insert into t values (100, '101');",
			"set @Commit4 = (select DOLT_COMMIT('-am', 'recreating column c2'));",
		},
		Assertions: []enginetest.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM DOLT_DIFF_t;",
				Expected: []sql.Row{{5}},
			},
			{
				Query: "SELECT to_pk, to_c2, from_pk, from_c2, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit1 ORDER BY to_pk;",
				Expected: []sql.Row{
					{1, nil, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c2, from_pk, from_c2, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit2 ORDER BY to_pk;",
				Expected: []sql.Row{
					{3, 4, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c2, from_pk, from_c2, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit3 ORDER BY to_pk;",
				Expected: []sql.Row{
					{1, nil, 1, 2, "modified"},
					{3, nil, 3, 4, "modified"},
				},
			},
			{
				Query: "SELECT to_pk, to_c2, from_pk, from_c2, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit4 ORDER BY to_pk;",
				Expected: []sql.Row{
					{100, 101, nil, nil, "added"},
				},
			},
		},
	},
	{
		Name: "primary key change",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 int);",
			"insert into t values (1, 2), (3, 4);",
			"set @Commit1 = (select DOLT_COMMIT('-am', 'creating table t'));",

			"alter table t drop primary key;",
			"insert into t values (5, 6);",
			"set @Commit2 = (select DOLT_COMMIT('-am', 'dropping primary key'));",

			"alter table t add primary key (c1);",
			"set @Commit3 = (select DOLT_COMMIT('-am', 'adding primary key'));",

			"insert into t values (7, 8);",
			"set @Commit4 = (select DOLT_COMMIT('-am', 'adding more data'));",
		},
		Assertions: []enginetest.ScriptTestAssertion{
			{
				Query:                           "select * from dolt_diff_t;",
				ExpectedWarning:                 1105,
				ExpectedWarningsCount:           1,
				ExpectedWarningMessageSubstring: "cannot render full diff between commits",
				SkipResultsCheck:                true,
			},
			{
				Query:    "SELECT COUNT(*) FROM DOLT_DIFF_t;;",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT to_pk, to_c1, from_pk, from_c1, diff_type FROM DOLT_DIFF_t where to_commit=@Commit4;",
				Expected: []sql.Row{{7, 8, nil, nil, "added"}},
			},
		},
	},
}

var UnscopedDiffTableTests = []enginetest.ScriptTest{
	// There's a bug in queries with where clauses that compare column equality with a
	// variable. These UnscopedDiffTableTests use "commit_hash in (@Commit1)" to work around that bug.
	// https://github.com/dolthub/go-mysql-server/issues/790
	{
		Name: "basic case with three tables",
		SetUpScript: []string{
			"create table x (a int primary key, b int, c int)",
			"create table y (a int primary key, b int, c int)",
			"insert into x values (1, 2, 3), (2, 3, 4)",
			"set @Commit1 = (select DOLT_COMMIT('-am', 'Creating tables x and y'))",

			"create table z (a int primary key, b int, c int)",
			"insert into z values (100, 101, 102)",
			"set @Commit2 = (select DOLT_COMMIT('-am', 'Creating tables z'))",

			"insert into y values (-1, -2, -3), (-2, -3, -4)",
			"insert into z values (101, 102, 103)",
			"set @Commit3 = (select DOLT_COMMIT('-am', 'Inserting into tables y and z'))",
		},
		Assertions: []enginetest.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM DOLT_DIFF",
				Expected: []sql.Row{{5}},
			},
			{
				Query:    "select table_name from DOLT_DIFF where commit_hash in (@Commit1)",
				Expected: []sql.Row{{"x"}, {"y"}},
			},
			{
				Query:    "select table_name from DOLT_DIFF where commit_hash in (@Commit2)",
				Expected: []sql.Row{{"z"}},
			},
			{
				Query:    "select table_name from DOLT_DIFF where commit_hash in (@Commit3)",
				Expected: []sql.Row{{"y"}, {"z"}},
			},
		},
	},
	{
		Name: "renamed table",
		SetUpScript: []string{
			"create table x (a int primary key, b int, c int)",
			"create table y (a int primary key, b int, c int)",
			"insert into x values (1, 2, 3), (2, 3, 4)",
			"set @Commit1 = (select DOLT_COMMIT('-am', 'Creating tables x and y'))",

			"create table z (a int primary key, b int, c int)",
			"insert into z values (100, 101, 102)",
			"set @Commit2 = (select DOLT_COMMIT('-am', 'Creating tables z'))",

			"rename table x to x1",
			"set @Commit3 = (select DOLT_COMMIT('-am', 'Renaming table x to x1'))",
		},
		Assertions: []enginetest.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM DOLT_DIFF",
				Expected: []sql.Row{{4}},
			},
			{
				Query:    "select table_name from DOLT_DIFF where commit_hash in (@Commit1)",
				Expected: []sql.Row{{"x"}, {"y"}},
			},
			{
				Query:    "select table_name from DOLT_DIFF where commit_hash in (@Commit2)",
				Expected: []sql.Row{{"z"}},
			},
			{
				Query:    "select table_name from DOLT_DIFF where commit_hash in (@Commit3)",
				Expected: []sql.Row{{"x1"}},
			},
		},
	},
	{
		Name: "dropped table",
		SetUpScript: []string{
			"create table x (a int primary key, b int, c int)",
			"create table y (a int primary key, b int, c int)",
			"insert into x values (1, 2, 3), (2, 3, 4)",
			"set @Commit1 = (select DOLT_COMMIT('-am', 'Creating tables x and y'))",

			"drop table x",
			"set @Commit2 = (select DOLT_COMMIT('-am', 'Dropping table x'))",
		},
		Assertions: []enginetest.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM DOLT_DIFF",
				Expected: []sql.Row{{3}},
			},
			{
				Query:    "select table_name from DOLT_DIFF where commit_hash in (@Commit1)",
				Expected: []sql.Row{{"x"}, {"y"}},
			},
			{
				Query:    "select table_name from DOLT_DIFF where commit_hash in (@Commit2)",
				Expected: []sql.Row{{"x"}},
			},
		},
	},
	{
		Name: "empty commit handling",
		SetUpScript: []string{
			"create table x (a int primary key, b int, c int)",
			"create table y (a int primary key, b int, c int)",
			"insert into x values (1, 2, 3), (2, 3, 4)",
			"set @Commit1 = (select DOLT_COMMIT('-am', 'Creating tables x and y'))",

			"set @Commit2 = (select DOLT_COMMIT('--allow-empty', '-m', 'Empty!'))",

			"insert into y values (-1, -2, -3), (-2, -3, -4)",
			"set @Commit3 = (select DOLT_COMMIT('-am', 'Inserting into tables y and z'))",
		},
		Assertions: []enginetest.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM DOLT_DIFF",
				Expected: []sql.Row{{3}},
			},
			{
				Query:    "select table_name from DOLT_DIFF where commit_hash in (@Commit1)",
				Expected: []sql.Row{{"x"}, {"y"}},
			},
			{
				Query:    "select table_name from DOLT_DIFF where commit_hash in (@Commit2)",
				Expected: []sql.Row{},
			},
			{
				Query:    "select table_name from DOLT_DIFF where commit_hash in (@Commit3)",
				Expected: []sql.Row{{"y"}},
			},
		},
	},
	{
		Name: "includes commits from all branches",
		SetUpScript: []string{
			"select dolt_checkout('-b', 'branch1')",
			"create table x (a int primary key, b int, c int)",
			"create table y (a int primary key, b int, c int)",
			"insert into x values (1, 2, 3), (2, 3, 4)",
			"set @Commit1 = (select DOLT_COMMIT('-am', 'Creating tables x and y'))",

			"select dolt_checkout('-b', 'branch2')",
			"create table z (a int primary key, b int, c int)",
			"insert into z values (100, 101, 102)",
			"set @Commit2 = (select DOLT_COMMIT('-am', 'Creating tables z'))",

			"insert into y values (-1, -2, -3), (-2, -3, -4)",
			"insert into z values (101, 102, 103)",
			"set @Commit3 = (select DOLT_COMMIT('-am', 'Inserting into tables y and z'))",
		},
		Assertions: []enginetest.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM DOLT_DIFF",
				Expected: []sql.Row{{5}},
			},
			{
				Query:    "select table_name from DOLT_DIFF where commit_hash in (@Commit1)",
				Expected: []sql.Row{{"x"}, {"y"}},
			},
			{
				Query:    "select table_name from DOLT_DIFF where commit_hash in (@Commit2)",
				Expected: []sql.Row{{"z"}},
			},
			{
				Query:    "select table_name from DOLT_DIFF where commit_hash in (@Commit3)",
				Expected: []sql.Row{{"y"}, {"z"}},
			},
		},
	},
	// The DOLT_DIFF system table doesn't currently show any diff data for a merge commit.
	// When processing a merge commit, diff.GetTableDeltas isn't aware of branch context, so it
	// doesn't detect that any tables have changed.
	{
		Name: "merge history handling",
		SetUpScript: []string{
			"select dolt_checkout('-b', 'branch1')",
			"create table x (a int primary key, b int, c int)",
			"create table y (a int primary key, b int, c int)",
			"insert into x values (1, 2, 3), (2, 3, 4)",
			"set @Commit1 = (select DOLT_COMMIT('-am', 'Creating tables x and y'))",

			"select dolt_checkout('-b', 'branch2')",
			"create table z (a int primary key, b int, c int)",
			"insert into z values (100, 101, 102)",
			"set @Commit2 = (select DOLT_COMMIT('-am', 'Creating tables z'))",

			"select DOLT_MERGE('branch1')",
			"set @Commit3 = (select DOLT_COMMIT('-am', 'Merging branch1 into branch2'))",
		},
		Assertions: []enginetest.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM DOLT_DIFF",
				Expected: []sql.Row{{3}},
			},
			{
				Query:    "select table_name from DOLT_DIFF where commit_hash in (@Commit1)",
				Expected: []sql.Row{{"x"}, {"y"}},
			},
			{
				Query:    "select table_name from DOLT_DIFF where commit_hash in (@Commit2)",
				Expected: []sql.Row{{"z"}},
			},
			{
				Query:    "select table_name from DOLT_DIFF where commit_hash in (@Commit3)",
				Expected: []sql.Row{},
			},
		},
	},
}
