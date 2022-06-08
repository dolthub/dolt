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
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dfunctions"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

var ShowCreateTableAsOfScriptTest = queries.ScriptTest{
	Name: "Show create table as of",
	SetUpScript: []string{
		"set @Commit0 = hashof('main');",
		"create table a (pk int primary key, c1 int);",
		"set @Commit1 = dolt_commit('-am', 'creating table a');",
		"alter table a add column c2 text;",
		"set @Commit2 = dolt_commit('-am', 'adding column c2');",
		"alter table a drop column c1;",
		"alter table a add constraint unique_c2 unique(c2);",
		"set @Commit3 = dolt_commit('-am', 'dropping column c1');",
	},
	Assertions: []queries.ScriptTestAssertion{
		{
			Query:       "show create table a as of @Commit0;",
			ExpectedErr: sql.ErrTableNotFound,
		},
		{
			Query: "show create table a as of @Commit1;",
			Expected: []sql.Row{
				{"a", "CREATE TABLE `a` (\n" +
					"  `pk` int NOT NULL,\n" +
					"  `c1` int,\n" +
					"  PRIMARY KEY (`pk`)\n" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin",
				},
			},
		},
		{
			Query: "show create table a as of @Commit2;",
			Expected: []sql.Row{
				{"a", "CREATE TABLE `a` (\n" +
					"  `pk` int NOT NULL,\n" +
					"  `c1` int,\n" +
					"  `c2` text,\n" +
					"  PRIMARY KEY (`pk`)\n" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin",
				},
			},
		},
		{
			Query: "show create table a as of @Commit3;",
			Expected: []sql.Row{
				{"a", "CREATE TABLE `a` (\n" +
					"  `pk` int NOT NULL,\n" +
					"  `c2` text,\n" +
					"  PRIMARY KEY (`pk`),\n" +
					"  UNIQUE KEY `c2` (`c2`)\n" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin",
				},
			},
		},
	},
}

var DescribeTableAsOfScriptTest = queries.ScriptTest{
	Name: "Describe table as of",
	SetUpScript: []string{
		"set @Commit0 = dolt_commit('--allow-empty', '-m', 'before creating table a');",
		"create table a (pk int primary key, c1 int);",
		"set @Commit1 = dolt_commit('-am', 'creating table a');",
		"alter table a add column c2 text;",
		"set @Commit2 = dolt_commit('-am', 'adding column c2');",
		"alter table a drop column c1;",
		"set @Commit3 = dolt_commit('-am', 'dropping column c1');",
	},
	Assertions: []queries.ScriptTestAssertion{
		{
			Query:       "describe a as of @Commit0;",
			ExpectedErr: sql.ErrTableNotFound,
		},
		{
			Query: "describe a as of @Commit1;",
			Expected: []sql.Row{
				{"pk", "int", "NO", "PRI", "", ""},
				{"c1", "int", "YES", "", "", ""},
			},
		},
		{
			Query: "describe a as of @Commit2;",
			Expected: []sql.Row{
				{"pk", "int", "NO", "PRI", "", ""},
				{"c1", "int", "YES", "", "", ""},
				{"c2", "text", "YES", "", "", ""},
			},
		},
		{
			Query: "describe a as of @Commit3;",
			Expected: []sql.Row{
				{"pk", "int", "NO", "PRI", "", ""},
				{"c2", "text", "YES", "", "", ""},
			},
		},
	},
}

// DoltScripts are script tests specific to Dolt (not the engine in general), e.g. by involving Dolt functions. Break
// this slice into others with good names as it grows.
var DoltScripts = []queries.ScriptTest{
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
		Assertions: []queries.ScriptTestAssertion{
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
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "show create table t1",
				Expected: []sql.Row{
					{"t1", "CREATE TABLE `t1` (\n" +
						"  `a` int NOT NULL,\n" +
						"  `b` varchar(10) NOT NULL DEFAULT \"abc\",\n" +
						"  PRIMARY KEY (`a`),\n" +
						"  KEY `t1b` (`b`),\n" +
						"  CONSTRAINT `ck1` CHECK (`b` LIKE \"%abc%\")\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "show create table t2",
				Expected: []sql.Row{
					{"t2", "CREATE TABLE `t2` (\n" +
						"  `c` int NOT NULL,\n" +
						"  `d` varchar(10),\n" +
						"  PRIMARY KEY (`c`),\n" +
						"  UNIQUE KEY `d_0` (`d`),\n" +
						"  CONSTRAINT `fk1` FOREIGN KEY (`d`) REFERENCES `t1` (`b`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
		},
	},
	{
		Name: "Query table with 10K rows ",
		SetUpScript: []string{
			"create table bigTable (pk int primary key, c0 int);",
			makeLargeInsert(10_000),
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select count(*) from bigTable;",
				Expected: []sql.Row{
					{int32(10_000)},
				},
			},
			{
				Query: "select * from bigTable order by pk limit 5 offset 9990;",
				Expected: []sql.Row{
					{int64(9990), int64(9990)},
					{int64(9991), int64(9991)},
					{int64(9992), int64(9992)},
					{int64(9993), int64(9993)},
					{int64(9994), int64(9994)},
				},
			},
		},
	},
	{
		Name: "SHOW CREATE PROCEDURE works with Dolt external procedures",
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SHOW CREATE PROCEDURE dolt_checkout;",
				Expected: []sql.Row{
					{
						"dolt_checkout",
						"",
						"CREATE PROCEDURE dolt_checkout() SELECT 'External stored procedure defined by mydb';",
						"utf8mb4",
						"utf8mb4_0900_bin",
						"utf8mb4_0900_bin",
					},
				},
			},
		},
	},
}

func makeLargeInsert(sz int) string {
	var sb strings.Builder
	sb.WriteString("insert into bigTable values (0,0)")
	for i := 1; i < sz; i++ {
		sb.WriteString(fmt.Sprintf(",(%d,%d)", i, i))
	}
	sb.WriteString(";")
	return sb.String()
}

// DoltUserPrivTests are tests for Dolt-specific functionality that includes privilege checking logic.
var DoltUserPrivTests = []queries.UserPrivilegeTest{
	{
		Name: "dolt_diff table function privilege checking",
		SetUpScript: []string{
			"CREATE TABLE mydb.test (pk BIGINT PRIMARY KEY);",
			"CREATE TABLE mydb.test2 (pk BIGINT PRIMARY KEY);",
			"SELECT DOLT_COMMIT('-am', 'creating tables test and test2');",
			"INSERT INTO mydb.test VALUES (1);",
			"SELECT DOLT_COMMIT('-am', 'inserting into test');",
			"CREATE USER tester@localhost;",
		},
		Assertions: []queries.UserPrivilegeTestAssertion{
			{
				// Without access to the database, dolt_diff should fail with a database access error
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM dolt_diff('test', 'main~', 'main');",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				// Grant single-table access to the underlying user table
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT SELECT ON mydb.test TO tester@localhost;",
				Expected: []sql.Row{{sql.NewOkResult(0)}},
			},
			{
				// After granting access to mydb.test, dolt_diff should work
				User:     "tester",
				Host:     "localhost",
				Query:    "SELECT COUNT(*) FROM dolt_diff('test', 'main~', 'main');",
				Expected: []sql.Row{{1}},
			},
			{
				// With access to the db, but not the table, dolt_diff should fail
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM dolt_diff('test2', 'main~', 'main');",
				ExpectedErr: sql.ErrPrivilegeCheckFailed,
			},
			{
				// Revoke select on mydb.test
				User:     "root",
				Host:     "localhost",
				Query:    "REVOKE SELECT ON mydb.test from tester@localhost;",
				Expected: []sql.Row{{sql.NewOkResult(0)}},
			},
			{
				// After revoking access, dolt_diff should fail
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM dolt_diff('test', 'main~', 'main');",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				// Grant multi-table access for all of mydb
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT SELECT ON mydb.* to tester@localhost;",
				Expected: []sql.Row{{sql.NewOkResult(0)}},
			},
			{
				// After granting access to the entire db, dolt_diff should work
				User:     "tester",
				Host:     "localhost",
				Query:    "SELECT COUNT(*) FROM dolt_diff('test', 'main~', 'main');",
				Expected: []sql.Row{{1}},
			},
			{
				// Revoke multi-table access
				User:     "root",
				Host:     "localhost",
				Query:    "REVOKE SELECT ON mydb.* from tester@localhost;",
				Expected: []sql.Row{{sql.NewOkResult(0)}},
			},
			{
				// After revoking access, dolt_diff should fail
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM dolt_diff('test', 'main~', 'main');",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				// Grant global access to *.*
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT SELECT ON *.* to tester@localhost;",
				Expected: []sql.Row{{sql.NewOkResult(0)}},
			},
			{
				// After granting global access to *.*, dolt_diff should work
				User:     "tester",
				Host:     "localhost",
				Query:    "SELECT COUNT(*) FROM dolt_diff('test', 'main~', 'main');",
				Expected: []sql.Row{{1}},
			},
			{
				// Revoke global access
				User:     "root",
				Host:     "localhost",
				Query:    "REVOKE ALL ON *.* from tester@localhost;",
				Expected: []sql.Row{{sql.NewOkResult(0)}},
			},
			{
				// After revoking global access, dolt_diff should fail
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM dolt_diff('test', 'main~', 'main');",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
		},
	},
}

var HistorySystemTableScriptTests = []queries.ScriptTest{
	{
		Name: "empty table",
		SetUpScript: []string{
			"create table t (n int, c text);",
			"set @Commit1 = dolt_commit('-am', 'creating table t');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select count(*) from DOLT_HISTORY_t;",
				Expected: []sql.Row{{0}},
			},
		},
	},
	{
		Name: "keyless table",
		SetUpScript: []string{
			"create table foo1 (n int, de text);",
			"insert into foo1 values (1, 'Ein'), (2, 'Zwei'), (3, 'Drei');",
			"set @Commit1 = dolt_commit('-am', 'inserting into foo1');",

			"update foo1 set de='Eins' where n=1;",
			"set @Commit2 = dolt_commit('-am', 'updating data in foo1');",

			"insert into foo1 values (4, 'Vier');",
			"set @Commit3 = dolt_commit('-am', 'inserting data in foo1');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select count(*) from DOLT_HISTORY_foO1;",
				Expected: []sql.Row{{10}},
			},
			{
				Query:    "select n, de from dolt_history_foo1 where commit_hash=@Commit1;",
				Expected: []sql.Row{{1, "Ein"}, {2, "Zwei"}, {3, "Drei"}},
			},
			{
				Query:    "select n, de from dolt_history_Foo1 where commit_hash=@Commit2;",
				Expected: []sql.Row{{1, "Eins"}, {2, "Zwei"}, {3, "Drei"}},
			},
			{
				Query:    "select n, de from dolt_history_foo1 where commit_hash=@Commit3;",
				Expected: []sql.Row{{1, "Eins"}, {2, "Zwei"}, {3, "Drei"}, {4, "Vier"}},
			},
		},
	},
	{
		Name: "primary key table: basic cases",
		SetUpScript: []string{
			"create table foo1 (n int primary key, de text);",
			"insert into foo1 values (1, 'Eins'), (2, 'Zwei'), (3, 'Drei');",
			"set @Commit1 = dolt_commit('-am', 'inserting into foo1');",

			"alter table foo1 add column fr text;",
			"insert into foo1 values (4, 'Vier', 'Quatre');",
			"set @Commit2 = dolt_commit('-am', 'adding column and inserting data in foo1');",

			"update foo1 set fr='Un' where n=1;",
			"set @Commit3 = dolt_commit('-am', 'updating data in foo1');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select count(*) from Dolt_History_Foo1;",
				Expected: []sql.Row{{11}},
			},
			{
				Query:    "select n, de, fr from dolt_history_FOO1 where commit_hash = @Commit1;",
				Expected: []sql.Row{{1, "Eins", nil}, {2, "Zwei", nil}, {3, "Drei", nil}},
			},
			{
				Query:    "select n, de, fr from dolt_history_foo1 where commit_hash = @Commit2;",
				Expected: []sql.Row{{1, "Eins", nil}, {2, "Zwei", nil}, {3, "Drei", nil}, {4, "Vier", "Quatre"}},
			},
			{
				Query:    "select n, de, fr from dolt_history_foo1 where commit_hash = @Commit3;",
				Expected: []sql.Row{{1, "Eins", "Un"}, {2, "Zwei", nil}, {3, "Drei", nil}, {4, "Vier", "Quatre"}},
			},
		},
	},
	{
		Name: "primary key table: non-pk schema changes",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 int, c2 text);",
			"insert into t values (1, 2, '3'), (4, 5, '6');",
			"set @Commit1 = DOLT_COMMIT('-am', 'creating table t');",

			"alter table t drop column c2;",
			"set @Commit2 = DOLT_COMMIT('-am', 'dropping column c2');",

			"alter table t rename column c1 to c2;",
			"set @Commit3 = DOLT_COMMIT('-am', 'renaming c1 to c2');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select count(*) from dolt_history_t;",
				Expected: []sql.Row{{6}},
			},
			{
				// TODO: Instead of just spot checking the non-existence of c1, it would be useful to be able to
				//       assert the full schema of the result set. ScriptTestAssertion doesn't support that currently,
				//       but the code from QueryTest could be ported over to ScriptTestAssertion.
				Query:       "select c1 from dolt_history_t;",
				ExpectedErr: sql.ErrColumnNotFound,
			},
			{
				Query:    "select pk, c2 from dolt_history_t where commit_hash=@Commit1;",
				Expected: []sql.Row{{1, 2}, {4, 5}},
			},
			{
				Query:    "select pk, c2 from dolt_history_t where commit_hash=@Commit2;",
				Expected: []sql.Row{{1, 2}, {4, 5}},
			},
			{
				Query:    "select pk, c2 from dolt_history_t where commit_hash=@Commit3;",
				Expected: []sql.Row{{1, 2}, {4, 5}},
			},
		},
	},
	{
		Name: "primary key table: rename table",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 int, c2 text);",
			"insert into t values (1, 2, '3'), (4, 5, '6');",
			"set @Commit1 = DOLT_COMMIT('-am', 'creating table t');",

			"alter table t rename to t2;",
			"set @Commit2 = DOLT_COMMIT('-am', 'renaming table to t2');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:       "select count(*) from dolt_history_t;",
				ExpectedErr: sql.ErrTableNotFound,
			},
			{
				Query:    "select count(*) from dolt_history_T2;",
				Expected: []sql.Row{{2}},
			},
			{
				Query:    "select pk, c1, c2 from dolt_history_t2 where commit_hash != @Commit1;",
				Expected: []sql.Row{{1, 2, "3"}, {4, 5, "6"}},
			},
		},
	},
	{
		Name: "primary key table: delete and recreate table",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 int, c2 text);",
			"insert into t values (1, 2, '3'), (4, 5, '6');",
			"set @Commit1 = DOLT_COMMIT('-am', 'creating table t');",

			"drop table t;",
			"set @Commit2 = DOLT_COMMIT('-am', 'dropping table t');",

			"create table t (pk int primary key, c1 int);",
			"set @Commit3 = DOLT_COMMIT('-am', 'recreating table t');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				// TODO: The history system table processes history in parallel and pulls the rows for the
				//       user table at all commits. This means we can't currently detect when a table was dropped
				//       and if a different table with the same name exists at earlier commits, those results will
				//       be included in the history table. It may make more sense to have history scoped only
				//       to the current instance of the table, which would require changing the history system table
				//       to use something like an iterator approach where it goes back sequentially until it detects
				//       the current table doesn't exist any more and then stop.
				Query:    "select count(*) from dolt_history_t;",
				Expected: []sql.Row{{2}},
			},
		},
	},
}

var MergeScripts = []queries.ScriptTest{
	{
		Name: "CALL DOLT_MERGE ff correctly works with autocommit off",
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
		Assertions: []queries.ScriptTestAssertion{
			{
				// FF-Merge
				Query:    "CALL DOLT_MERGE('feature-branch')",
				Expected: []sql.Row{{1, 0}},
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
				// No-FF-Merge
				Query:    "CALL DOLT_MERGE('feature-branch', '-no-ff', '-m', 'this is a no-ff')",
				Expected: []sql.Row{{1, 0}},
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
		Name: "CALL DOLT_MERGE without conflicts correctly works with autocommit off",
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
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('feature-branch', '-m', 'this is a merge')",
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
		Name: "CALL DOLT_MERGE with conflicts can be correctly resolved when autocommit is off",
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
				Query:    "SELECT COUNT(*) FROM dolt_log",
				Expected: []sql.Row{{4}},
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
		Name: "CALL DOLT_MERGE ff & squash correctly works with autocommit off",
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
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_MERGE('feature-branch', '-m', 'this is a merge')",
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
		Name: "CALL DOLT_MERGE with conflict is queryable and committable with dolt_allow_commit_conflicts on",
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
		},
	},
	{
		Name: "Drop and add primary key on two branches converges to same schema",
		SetUpScript: []string{
			"create table t1 (i int);",
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
		Name: "merging branches into a constraint violated head. Any new violations are appended",
		SetUpScript: []string{
			"CREATE table parent (pk int PRIMARY KEY, col1 int);",
			"CREATE table child (pk int PRIMARY KEY, parent_fk int, FOREIGN KEY (parent_fk) REFERENCES parent(pk));",
			"CREATE table other (pk int);",
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
				Expected: []sql.Row{{"foreign key", 1, 1}},
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
				Expected: []sql.Row{{"foreign key", 1, 1}},
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
				Expected: []sql.Row{{"foreign key", 1, 1}, {"foreign key", 2, 2}},
			},
		},
	},
	{
		Name: "conflicting merge aborts when conflicts and violations already exist",
		SetUpScript: []string{
			"CREATE table parent (pk int PRIMARY KEY, col1 int);",
			"CREATE table child (pk int PRIMARY KEY, parent_fk int, FOREIGN KEY (parent_fk) REFERENCES parent(pk));",
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
				Expected: []sql.Row{{"foreign key", 1, 2}},
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
				Expected: []sql.Row{{"foreign key", 1, 2}},
			},
		},
	},
	{
		Name: "non-conflicting / non-violating merge succeeds when conflicts and violations already exist",
		SetUpScript: []string{
			"CREATE table parent (pk int PRIMARY KEY, col1 int);",
			"CREATE table child (pk int PRIMARY KEY, parent_fk int, FOREIGN KEY (parent_fk) REFERENCES parent(pk));",
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
				Expected: []sql.Row{{"foreign key", 1, 2}},
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
				Expected: []sql.Row{{"foreign key", 1, 2}},
			},
		},
	},
}

var DoltBranchScripts = []queries.ScriptTest{
	{
		Name: "Create branches from HEAD with dolt_branch procedure",
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_BRANCH('myNewBranch1')",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "SELECT COUNT(*) FROM DOLT_BRANCHES WHERE NAME='myNewBranch1';",
				Expected: []sql.Row{{1}},
			},
			{
				// Trying to recreate that branch fails without the force flag
				Query:          "CALL DOLT_BRANCH('myNewBranch1')",
				ExpectedErrStr: "fatal: A branch named 'myNewBranch1' already exists.",
			},
			{
				Query:    "CALL DOLT_BRANCH('-f', 'myNewBranch1')",
				Expected: []sql.Row{{0}},
			},
		},
	},
	{
		Name: "Rename branches with dolt_branch procedure",
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_BRANCH('myNewBranch1')",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "CALL DOLT_BRANCH('myNewBranch2')",
				Expected: []sql.Row{{0}},
			},
			{
				// Renaming to an existing name fails without the force flag
				Query:          "CALL DOLT_BRANCH('-m', 'myNewBranch1', 'myNewBranch2')",
				ExpectedErrStr: "already exists",
			},
			{
				Query:    "CALL DOLT_BRANCH('-mf', 'myNewBranch1', 'myNewBranch2')",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "CALL DOLT_BRANCH('-m', 'myNewBranch2', 'myNewBranch3')",
				Expected: []sql.Row{{0}},
			},
		},
	},
	{
		Name: "Copy branches from other branches using dolt_branch procedure",
		SetUpScript: []string{
			"CALL DOLT_BRANCH('myNewBranch1')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "CALL DOLT_BRANCH('-c')",
				ExpectedErrStr: "error: invalid usage",
			},
			{
				Query:          "CALL DOLT_BRANCH('-c', 'myNewBranch1')",
				ExpectedErrStr: "error: invalid usage",
			},
			{
				Query:          "CALL DOLT_BRANCH('-c', 'myNewBranch2')",
				ExpectedErrStr: "error: invalid usage",
			},
			{
				Query:          "CALL DOLT_BRANCH('-c', '', '')",
				ExpectedErrStr: "error: cannot branch empty string",
			},
			{
				Query:    "CALL DOLT_BRANCH('-c', 'myNewBranch1', 'myNewBranch2')",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "SELECT COUNT(*) FROM DOLT_BRANCHES WHERE NAME='myNewBranch2';",
				Expected: []sql.Row{{1}},
			},
			{
				Query:          "CALL DOLT_BRANCH('-c', 'myNewBranch1', 'myNewBranch2')",
				ExpectedErrStr: "fatal: A branch named 'myNewBranch2' already exists.",
			},
			{
				Query:    "CALL DOLT_BRANCH('-cf', 'myNewBranch1', 'myNewBranch2')",
				Expected: []sql.Row{{0}},
			},
		},
	},
	{
		Name: "Delete branches with dolt_branch procedure",
		SetUpScript: []string{
			"CALL DOLT_BRANCH('myNewBranch1')",
			"CALL DOLT_BRANCH('myNewBranch2')",
			"CALL DOLT_BRANCH('myNewBranch3')",
			"CALL DOLT_BRANCH('myNewBranchWithCommit')",
			"CALL DOLT_CHECKOUT('myNewBranchWithCommit')",
			"CALL DOLT_COMMIT('--allow-empty', '-am', 'empty commit')",
			"CALL DOLT_CHECKOUT('main')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "CALL DOLT_BRANCH('-d')",
				ExpectedErrStr: "error: invalid usage",
			},
			{
				Query:          "CALL DOLT_BRANCH('-d', '')",
				ExpectedErrStr: "error: cannot branch empty string",
			},
			{
				Query:          "CALL DOLT_BRANCH('-d', 'branchDoesNotExist')",
				ExpectedErrStr: "branch not found",
			},
			{
				Query:    "CALL DOLT_BRANCH('-d', 'myNewBranch1')",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "SELECT COUNT(*) FROM DOLT_BRANCHES WHERE NAME='myNewBranch1'",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "CALL DOLT_BRANCH('-d', 'myNewBranch2', 'myNewBranch3')",
				Expected: []sql.Row{{0}},
			},
			{
				// Trying to delete a branch with unpushed changes fails without force option
				Query:          "CALL DOLT_BRANCH('-d', 'myNewBranchWithCommit')",
				ExpectedErrStr: "attempted to delete a branch that is not fully merged into its parent; use `-f` to force",
			},
			{
				Query:    "CALL DOLT_BRANCH('-df', 'myNewBranchWithCommit')",
				Expected: []sql.Row{{0}},
			},
		},
	},
	{
		Name: "Create branch from startpoint",
		SetUpScript: []string{
			"create table a (x int)",
			"set @commit1 = (select DOLT_COMMIT('-am', 'add table a'));",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "show tables",
				Expected: []sql.Row{{"a"}, {"myview"}},
			},
			{
				Query:    "CALL DOLT_CHECKOUT('-b', 'newBranch', 'head~1')",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "show tables",
				Expected: []sql.Row{{"myview"}},
			},
			{
				Query:    "CALL DOLT_CHECKOUT('-b', 'newBranch2', @commit1)",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "show tables",
				Expected: []sql.Row{{"a"}, {"myview"}},
			},
			{
				Query:          "CALL DOLT_CHECKOUT('-b', 'otherBranch', 'unknownCommit')",
				ExpectedErrStr: "fatal: 'unknownCommit' is not a commit and a branch 'otherBranch' cannot be created from it",
			},
		},
	},
}

var DoltReset = []queries.ScriptTest{
	{
		Name: "CALL DOLT_RESET('--hard') should reset the merge state after uncommitted merge",
		SetUpScript: []string{
			"CREATE TABLE test1 (pk int NOT NULL, c1 int, c2 int, PRIMARY KEY (pk));",
			"INSERT INTO test1 values (0,1,1);",
			"CALL DOLT_COMMIT('-am', 'added table')",

			"CALL DOLT_CHECKOUT('-b', 'merge_branch');",
			"UPDATE test1 set c1 = 2;",
			"CALL DOLT_COMMIT('-am', 'update pk 0 = 2,1 to test1');",

			"CALL DOLT_CHECKOUT('main');",
			"UPDATE test1 set c2 = 2;",
			"CALL DOLT_COMMIT('-am', 'update pk 0 = 1,2 to test1');",

			"CALL DOLT_MERGE('merge_branch');",

			"CALL DOLT_RESET('--hard');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "CALL DOLT_MERGE('--abort')",
				ExpectedErrStr: "fatal: There is no merge to abort",
			},
		},
	},
	{
		Name: "CALL DOLT_RESET('--hard') should reset the merge state after conflicting merge",
		SetUpScript: []string{
			"SET dolt_allow_commit_conflicts = on",
			"CREATE TABLE test1 (pk int NOT NULL, c1 int, c2 int, PRIMARY KEY (pk));",
			"INSERT INTO test1 values (0,1,1);",
			"CALL DOLT_COMMIT('-am', 'added table')",

			"CALL DOLT_CHECKOUT('-b', 'merge_branch');",
			"UPDATE test1 set c1 = 2, c2 = 2;",
			"CALL DOLT_COMMIT('-am', 'update pk 0 = 2,2 to test1');",

			"CALL DOLT_CHECKOUT('main');",
			"UPDATE test1 set c1 = 3, c2 = 3;",
			"CALL DOLT_COMMIT('-am', 'update pk 0 = 3,3 to test1');",

			"CALL DOLT_MERGE('merge_branch');",
			"CALL DOLT_RESET('--hard');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "CALL DOLT_MERGE('--abort')",
				ExpectedErrStr: "fatal: There is no merge to abort",
			},
		},
	},
}

var DiffSystemTableScriptTests = []queries.ScriptTest{
	{
		Name: "base case: added rows",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 int, c2 int);",
			"insert into t values (1, 2, 3), (4, 5, 6);",
			"set @Commit1 = (select DOLT_COMMIT('-am', 'creating table t'));",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM DOLT_DIFF_t;",
				Expected: []sql.Row{{2}},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit1 ORDER BY to_pk, to_c2, to_c2, from_pk, from_c1, from_c2, diff_type;",
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
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM DOLT_DIFF_t;",
				Expected: []sql.Row{{3}},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit2 ORDER BY to_pk, to_c2, to_c2, from_pk, from_c1, from_c2, diff_type;",
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
		Assertions: []queries.ScriptTestAssertion{
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
		Assertions: []queries.ScriptTestAssertion{
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
		Assertions: []queries.ScriptTestAssertion{
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
		Assertions: []queries.ScriptTestAssertion{
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
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM DOLT_DIFF_t;",
				Expected: []sql.Row{{5}},
			},
			{
				Query: "SELECT to_pk, to_c1, from_pk, from_c1, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit1 ORDER BY to_pk;",
				Expected: []sql.Row{
					{1, 2, nil, nil, "added"},
					{4, 5, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, from_pk, from_c1, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit2 ORDER BY to_pk;",
				Expected: []sql.Row{
					{1, nil, 1, 2, "modified"},
					{4, nil, 4, 5, "modified"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, from_pk, from_c1, diff_type FROM DOLT_DIFF_t WHERE TO_COMMIT=@Commit3 ORDER BY to_pk;",
				Expected: []sql.Row{
					{100, 101, nil, nil, "added"},
					// TODO: It's more correct to also return the following rows.
					//{1, 3, 1, nil, "modified"},
					//{4, 6, 4, nil, "modified"}

					// To explain why, let's inspect table t at each of the commits:
					//
					//     @Commit1          @Commit2         @Commit3
					// +----+----+----+     +----+----+     +-----+-----+
					// | pk | c1 | c2 |     | pk | c2 |     | pk  | c1  |
					// +----+----+----+     +----+----+     +-----+-----+
					// | 1  | 2  | 3  |     | 1  | 3  |     | 1   | 3   |
					// | 4  | 5  | 6  |     | 4  | 6  |     | 4   | 6   |
					// +----+----+----+     +----+----+     | 100 | 101 |
					//                                      +-----+-----+
					//
					// If you were to interpret each table using the schema at
					// @Commit3, (pk, c1), you would see the following:
					//
					//   @Commit1            @Commit2         @Commit3
					// +----+----+         +----+------+     +-----+-----+
					// | pk | c1 |         | pk | c1   |     | pk  | c1  |
					// +----+----+         +----+------+     +-----+-----+
					// | 1  | 2  |         | 1  | NULL |     | 1   | 3   |
					// | 4  | 5  |         | 4  | NULL |     | 4   | 6   |
					// +----+----+         +----+------+     | 100 | 101 |
					//                                       +-----+-----+
					//
					// The corresponding diffs for the interpreted tables:
					//
					// Diff between init and @Commit1:
					// + (1, 2)
					// + (4, 5)
					//
					// Diff between @Commit1 and @Commit2:
					// ~ (1, NULL)
					// ~ (4, NULL)
					//
					// Diff between @Commit2 and @Commit3:
					// ~ (1, 3) <- currently not outputted
					// ~ (4, 6) <- currently not outputted
					// + (100, 101)
					//
					// The missing rows are not produced by diff since the
					// underlying value of the prolly trees are not modified during a column rename.
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
		Assertions: []queries.ScriptTestAssertion{
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
		Assertions: []queries.ScriptTestAssertion{
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
		Assertions: []queries.ScriptTestAssertion{
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
		Assertions: []queries.ScriptTestAssertion{
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
	{
		Name: "table with commit column should maintain its data in diff",
		SetUpScript: []string{
			"CREATE TABLE t (pk int PRIMARY KEY, commit text);",
			"CALL dolt_commit('-am', 'creating table t');",
			"INSERT INTO t VALUES (1, 'hi');",
			"CALL dolt_commit('-am', 'insert data');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT to_pk, to_commit, from_pk, from_commit, diff_type from dolt_diff_t;",
				Expected: []sql.Row{{1, "hi", nil, nil, "added"}},
			},
		},
	},
	{
		Name: "selecting to_pk columns",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 int, c2 int);",
			"insert into t values (1, 2, 3), (4, 5, 6);",
			"set @Commit1 = (select DOLT_COMMIT('-am', 'first commit'));",
			"insert into t values (7, 8, 9);",
			"set @Commit2 = (select DOLT_COMMIT('-am', 'second commit'));",
			"update t set c1 = 0 where pk > 5;",
			"set @Commit3 = (select DOLT_COMMIT('-am', 'third commit'));",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM DOLT_DIFF_t;",
				Expected: []sql.Row{{4}},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type FROM DOLT_DIFF_t WHERE to_pk = 1 ORDER BY to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type;",
				Expected: []sql.Row{
					{1, 2, 3, nil, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type FROM DOLT_DIFF_t WHERE to_pk > 1 ORDER BY to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type;",
				Expected: []sql.Row{
					{4, 5, 6, nil, nil, nil, "added"},
					{7, 0, 9, 7, 8, 9, "modified"},
					{7, 8, 9, nil, nil, nil, "added"},
				},
			},
		},
	},
	{
		Name: "selecting to_pk1 and to_pk2 columns",
		SetUpScript: []string{
			"create table t (pk1 int, pk2 int, c1 int, primary key (pk1, pk2));",
			"insert into t values (1, 2, 3), (4, 5, 6);",
			"set @Commit1 = (select DOLT_COMMIT('-am', 'first commit'));",
			"insert into t values (7, 8, 9);",
			"set @Commit2 = (select DOLT_COMMIT('-am', 'second commit'));",
			"update t set c1 = 0 where pk1 > 5;",
			"set @Commit3 = (select DOLT_COMMIT('-am', 'third commit'));",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM DOLT_DIFF_t;",
				Expected: []sql.Row{{4}},
			},
			{
				Query: "SELECT to_pk1, to_pk2, to_c1, from_pk1, from_pk2, from_c1, diff_type FROM DOLT_DIFF_t WHERE to_pk1 = 1 ORDER BY to_pk1, to_pk2, to_c1, from_pk1, from_pk2, from_c1, diff_type;",
				Expected: []sql.Row{
					{1, 2, 3, nil, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk1, to_pk2, to_c1, from_pk1, from_pk2, from_c1, diff_type FROM DOLT_DIFF_t WHERE to_pk1 = 1 and to_pk2 = 2 ORDER BY to_pk1, to_pk2, to_c1, from_pk1, from_pk2, from_c1, diff_type;",
				Expected: []sql.Row{
					{1, 2, 3, nil, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk1, to_pk2, to_c1, from_pk1, from_pk2, from_c1, diff_type FROM DOLT_DIFF_t WHERE to_pk1 > 1 and to_pk2 < 10 ORDER BY to_pk1, to_pk2, to_c1, from_pk1, from_pk2, from_c1, diff_type;",
				Expected: []sql.Row{
					{4, 5, 6, nil, nil, nil, "added"},
					{7, 8, 0, 7, 8, 9, "modified"},
					{7, 8, 9, nil, nil, nil, "added"},
				},
			},
		},
	},
}

var DiffTableFunctionScriptTests = []queries.ScriptTest{
	{
		Name: "invalid arguments",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 text, c2 text);",
			"set @Commit1 = dolt_commit('-am', 'creating table t');",

			"insert into t values(1, 'one', 'two'), (2, 'two', 'three');",
			"set @Commit2 = dolt_commit('-am', 'inserting into t');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:       "SELECT * from dolt_diff('t');",
				ExpectedErr: sql.ErrInvalidArgumentNumber,
			},
			{
				Query:       "SELECT * from dolt_diff('t', @Commit1);",
				ExpectedErr: sql.ErrInvalidArgumentNumber,
			},
			{
				Query:       "SELECT * from dolt_diff('t', @Commit1, @Commit2, 'extra');",
				ExpectedErr: sql.ErrInvalidArgumentNumber,
			},
			{
				Query:       "SELECT * from dolt_diff(null, null, null);",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:       "SELECT * from dolt_diff(123, @Commit1, @Commit2);",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:       "SELECT * from dolt_diff('t', 123, @Commit2);",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:       "SELECT * from dolt_diff('t', @Commit1, 123);",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:       "SELECT * from dolt_diff('doesnotexist', @Commit1, @Commit2);",
				ExpectedErr: sql.ErrTableNotFound,
			},
			{
				Query:          "SELECT * from dolt_diff('t', 'fakefakefakefakefakefakefakefake', @Commit2);",
				ExpectedErrStr: "target commit not found",
			},
			{
				Query:          "SELECT * from dolt_diff('t', @Commit1, 'fake-branch');",
				ExpectedErrStr: "branch not found: fake-branch",
			},
			{
				Query:       "SELECT * from dolt_diff('t', @Commit1, concat('fake', '-', 'branch'));",
				ExpectedErr: sqle.ErrInvalidNonLiteralArgument,
			},
			{
				Query:       "SELECT * from dolt_diff('t', hashof('main'), @Commit2);",
				ExpectedErr: sqle.ErrInvalidNonLiteralArgument,
			},
			{
				Query:       "SELECT * from dolt_diff(LOWER('T'), hashof('main'), @Commit2);",
				ExpectedErr: sqle.ErrInvalidNonLiteralArgument,
			},
		},
	},
	{
		Name: "basic case",
		SetUpScript: []string{
			"set @Commit0 = HashOf('HEAD');",

			"create table t (pk int primary key, c1 text, c2 text);",
			"set @Commit1 = dolt_commit('-am', 'creating table t');",

			"insert into t values(1, 'one', 'two');",
			"set @Commit2 = dolt_commit('-am', 'inserting into table t');",

			"create table t2 (pk int primary key, c1 text, c2 text);",
			"insert into t2 values(100, 'hundred', 'hundert');",
			"set @Commit3 = dolt_commit('-am', 'inserting into table t2');",

			"insert into t values(2, 'two', 'three'), (3, 'three', 'four');",
			"update t set c1='uno', c2='dos' where pk=1;",
			"set @Commit4 = dolt_commit('-am', 'inserting into table t');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type from dolt_diff('t', @Commit1, @Commit2);",
				Expected: []sql.Row{{1, "one", "two", nil, nil, nil, "added"}},
			},
			{
				Query:    "SELECT COUNT(*) from dolt_diff('t', @Commit2, @Commit3);",
				Expected: []sql.Row{{0}},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type from dolt_diff('t', @Commit3, @Commit4);",
				Expected: []sql.Row{
					{1, "uno", "dos", 1, "one", "two", "modified"},
					{2, "two", "three", nil, nil, nil, "added"},
					{3, "three", "four", nil, nil, nil, "added"},
				},
			},
			{
				// Table t2 had no changes between Commit3 and Commit4, so results should be empty
				Query:    "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type  from dolt_diff('T2', @Commit3, @Commit4);",
				Expected: []sql.Row{},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type  from dolt_diff('t', @Commit1, @Commit4);",
				Expected: []sql.Row{
					{1, "uno", "dos", nil, nil, nil, "added"},
					{2, "two", "three", nil, nil, nil, "added"},
					{3, "three", "four", nil, nil, nil, "added"},
				},
			},
			{
				// Reverse the to/from commits to see the diff from the other direction
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type  from dolt_diff('T', @Commit4, @Commit1);",
				Expected: []sql.Row{
					{nil, nil, nil, 1, "uno", "dos", "removed"},
					{nil, nil, nil, 2, "two", "three", "removed"},
					{nil, nil, nil, 3, "three", "four", "removed"},
				},
			},
		},
	},
	{
		Name: "diff with branch refs",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 text, c2 text);",
			"set @Commit1 = dolt_commit('-am', 'creating table t');",

			"insert into t values(1, 'one', 'two');",
			"set @Commit2 = dolt_commit('-am', 'inserting row 1 into t in main');",

			"select dolt_checkout('-b', 'branch1');",
			"alter table t drop column c2;",
			"set @Commit3 = dolt_commit('-am', 'dropping column c2 in branch1');",

			"delete from t where pk=1;",
			"set @Commit4 = dolt_commit('-am', 'deleting row 1 in branch1');",

			"insert into t values (2, 'two');",
			"set @Commit5 = dolt_commit('-am', 'inserting row 2 in branch1');",

			"select dolt_checkout('main');",
			"insert into t values (2, 'two', 'three');",
			"set @Commit6 = dolt_commit('-am', 'inserting row 2 in main');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT to_pk, to_c1, from_pk, from_c1, from_c2, diff_type from dolt_diff('t', 'main', 'branch1');",
				Expected: []sql.Row{
					{nil, nil, 1, "one", "two", "removed"},
					{2, "two", 2, "two", "three", "modified"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, diff_type from dolt_diff('t', 'branch1', 'main');",
				Expected: []sql.Row{
					{1, "one", "two", nil, nil, "added"},
					{2, "two", "three", 2, "two", "modified"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, from_pk, from_c1, from_c2, diff_type from dolt_diff('t', 'main~', 'branch1');",
				Expected: []sql.Row{
					{nil, nil, 1, "one", "two", "removed"},
					{2, "two", nil, nil, nil, "added"},
				},
			},
		},
	},
	{
		Name: "schema modification: drop and recreate column with same type",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 text, c2 text);",
			"set @Commit1 = dolt_commit('-am', 'creating table t');",

			"insert into t values(1, 'one', 'two'), (2, 'two', 'three');",
			"set @Commit2 = dolt_commit('-am', 'inserting into t');",

			"alter table t drop column c2;",
			"set @Commit3 = dolt_commit('-am', 'dropping column c2');",

			"alter table t add column c2 text;",
			"insert into t values (3, 'three', 'four');",
			"update t set c2='foo' where pk=1;",
			"set @Commit4 = dolt_commit('-am', 'adding column c2, inserting, and updating data');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type from dolt_diff('t', @Commit1, @Commit2);",
				Expected: []sql.Row{
					{1, "one", "two", nil, nil, nil, "added"},
					{2, "two", "three", nil, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, from_pk, from_c1, from_c2, diff_type from dolt_diff('t', @Commit2, @Commit3);",
				Expected: []sql.Row{
					{1, "one", 1, "one", "two", "modified"},
					{2, "two", 2, "two", "three", "modified"},
				},
			},
			{
				Query:       "SELECT to_c2 from dolt_diff('t', @Commit2, @Commit3);",
				ExpectedErr: sql.ErrColumnNotFound,
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, diff_type from dolt_diff('t', @Commit3, @Commit4);",
				Expected: []sql.Row{
					{1, "one", "foo", 1, "one", "modified"},
					// This row doesn't show up as changed because adding a column doesn't touch the row data.
					//{2, "two", nil, 2, "two", "modified"},
					{3, "three", "four", nil, nil, "added"},
				},
			},
			{
				Query:       "SELECT from_c2 from dolt_diff('t', @Commit3, @Commit4);",
				ExpectedErr: sql.ErrColumnNotFound,
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type from dolt_diff('t', @Commit1, @Commit4);",
				Expected: []sql.Row{
					{1, "one", "foo", nil, nil, nil, "added"},
					{2, "two", nil, nil, nil, nil, "added"},
					{3, "three", "four", nil, nil, nil, "added"},
				},
			},
		},
	},
	{
		Name: "schema modification: rename columns",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 text, c2 int);",
			"set @Commit1 = dolt_commit('-am', 'creating table t');",

			"insert into t values(1, 'one', -1), (2, 'two', -2);",
			"set @Commit2 = dolt_commit('-am', 'inserting into t');",

			"alter table t rename column c2 to c3;",
			"set @Commit3 = dolt_commit('-am', 'renaming column c2 to c3');",

			"insert into t values (3, 'three', -3);",
			"update t set c3=1 where pk=1;",
			"set @Commit4 = dolt_commit('-am', 'inserting and updating data');",

			"alter table t rename column c3 to c2;",
			"insert into t values (4, 'four', -4);",
			"set @Commit5 = dolt_commit('-am', 'renaming column c3 to c2, and inserting data');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type from dolt_diff('t', @Commit1, @Commit2);",
				Expected: []sql.Row{
					{1, "one", -1, nil, nil, nil, "added"},
					{2, "two", -2, nil, nil, nil, "added"},
				},
			},
			{
				Query:       "SELECT to_c2 from dolt_diff('t', @Commit2, @Commit3);",
				ExpectedErr: sql.ErrColumnNotFound,
			},
			{
				Query:    "SELECT to_pk, to_c1, to_c3, from_pk, from_c1, from_c2, diff_type from dolt_diff('t', @Commit2, @Commit3);",
				Expected: []sql.Row{},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c3, from_pk, from_c1, from_c3, diff_type from dolt_diff('t', @Commit3, @Commit4);",
				Expected: []sql.Row{
					{3, "three", -3, nil, nil, nil, "added"},
					{1, "one", 1, 1, "one", -1, "modified"},
				},
			},
			{
				Query:       "SELECT from_c2 from dolt_diff('t', @Commit4, @Commit5);",
				ExpectedErr: sql.ErrColumnNotFound,
			},
			{
				Query:       "SELECT to_c3 from dolt_diff('t', @Commit4, @Commit5);",
				ExpectedErr: sql.ErrColumnNotFound,
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c3, diff_type from dolt_diff('t', @Commit4, @Commit5);",
				Expected: []sql.Row{
					{4, "four", -4, nil, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type from dolt_diff('t', @Commit1, @Commit5);",
				Expected: []sql.Row{
					{1, "one", 1, nil, nil, nil, "added"},
					{2, "two", -2, nil, nil, nil, "added"},
					{3, "three", -3, nil, nil, nil, "added"},
					{4, "four", -4, nil, nil, nil, "added"},
				},
			},
		},
	},
	{
		Name: "schema modification: drop and rename columns with different types",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 text, c2 text);",
			"set @Commit1 = dolt_commit('-am', 'creating table t');",

			"insert into t values(1, 'one', 'asdf'), (2, 'two', '2');",
			"set @Commit2 = dolt_commit('-am', 'inserting into t');",

			"alter table t drop column c2;",
			"set @Commit3 = dolt_commit('-am', 'dropping column c2');",

			"insert into t values (3, 'three');",
			"update t set c1='fdsa' where pk=1;",
			"set @Commit4 = dolt_commit('-am', 'inserting and updating data');",

			"alter table t add column c2 int;",
			"insert into t values (4, 'four', -4);",
			"set @Commit5 = dolt_commit('-am', 'adding column c2, and inserting data');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type from dolt_diff('t', @Commit1, @Commit2);",
				Expected: []sql.Row{
					{1, "one", "asdf", nil, nil, nil, "added"},
					{2, "two", "2", nil, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, from_pk, from_c1, from_c2, diff_type from dolt_diff('t', @Commit2, @Commit3);",
				Expected: []sql.Row{
					{1, "one", 1, "one", "asdf", "modified"},
					{2, "two", 2, "two", "2", "modified"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, from_pk, from_c1, diff_type from dolt_diff('t', @Commit3, @Commit4);",
				Expected: []sql.Row{
					{3, "three", nil, nil, "added"},
					{1, "fdsa", 1, "one", "modified"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, diff_type from dolt_diff('t', @Commit4, @Commit5);",
				Expected: []sql.Row{
					{4, "four", -4, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type from dolt_diff('t', @Commit1, @Commit5);",
				Expected: []sql.Row{
					{1, "fdsa", nil, nil, nil, nil, "added"},
					{2, "two", nil, nil, nil, nil, "added"},
					{3, "three", nil, nil, nil, nil, "added"},
					{4, "four", -4, nil, nil, nil, "added"},
				},
			},
		},
	},
	{
		Name: "table with commit column should maintain its data in diff",
		SetUpScript: []string{
			"CREATE TABLE t (pk int PRIMARY KEY, commit text);",
			"set @Commit1 = dolt_commit('-am', 'creating table t');",
			"INSERT INTO t VALUES (1, 'hi');",
			"set @Commit2 = dolt_commit('-am', 'insert data');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT to_pk, to_commit, from_pk, from_commit, diff_type from dolt_diff('t', @Commit1, @Commit2);",
				Expected: []sql.Row{{1, "hi", nil, nil, "added"}},
			},
		},
	},
}

var UnscopedDiffSystemTableScriptTests = []queries.ScriptTest{
	{
		Name: "basic case with three tables",
		SetUpScript: []string{
			"create table x (a int primary key, b int, c int);",
			"create table y (a int primary key, b int, c int);",
			"insert into x values (1, 2, 3), (2, 3, 4);",
			"set @Commit1 = (select DOLT_COMMIT('-am', 'Creating tables x and y'));",

			"create table z (a int primary key, b int, c int);",
			"insert into z values (100, 101, 102);",
			"set @Commit2 = (select DOLT_COMMIT('-am', 'Creating tables z'));",

			"insert into y values (-1, -2, -3), (-2, -3, -4);",
			"insert into z values (101, 102, 103);",
			"set @Commit3 = (select DOLT_COMMIT('-am', 'Inserting into tables y and z'));",

			"alter table y add column d int;",
			"set @Commit4 = (select DOLT_COMMIT('-am', 'Modify schema of table y'));",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM DOLT_DIFF",
				Expected: []sql.Row{{6}},
			},
			{
				Query:    "select table_name, schema_change, data_change from DOLT_DIFF where commit_hash in (@Commit1)",
				Expected: []sql.Row{{"x", true, true}, {"y", true, false}},
			},
			{
				Query:    "select table_name, schema_change, data_change from DOLT_DIFF where commit_hash in (@Commit2)",
				Expected: []sql.Row{{"z", true, true}},
			},
			{
				Query:    "select table_name, schema_change, data_change from DOLT_DIFF where commit_hash in (@Commit3)",
				Expected: []sql.Row{{"y", false, true}, {"z", false, true}},
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
			"insert into x1 values (1000, 1001, 1002);",
			"set @Commit3 = (select DOLT_COMMIT('-am', 'Renaming table x to x1 and inserting data'))",

			"rename table x1 to x2",
			"set @Commit4 = (select DOLT_COMMIT('-am', 'Renaming table x1 to x2'))",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM DOLT_DIFF",
				Expected: []sql.Row{{5}},
			},
			{
				Query:    "select table_name, schema_change, data_change from DOLT_DIFF where commit_hash in (@Commit1)",
				Expected: []sql.Row{{"x", true, true}, {"y", true, false}},
			},
			{
				Query:    "select table_name, schema_change, data_change from DOLT_DIFF where commit_hash in (@Commit2)",
				Expected: []sql.Row{{"z", true, true}},
			},
			{
				Query:    "select table_name, schema_change, data_change from DOLT_DIFF where commit_hash in (@Commit3)",
				Expected: []sql.Row{{"x1", true, true}},
			},
			{
				Query:    "select table_name, schema_change, data_change from DOLT_DIFF where commit_hash in (@Commit4)",
				Expected: []sql.Row{{"x2", true, false}},
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
			"set @Commit2 = (select DOLT_COMMIT('-am', 'Dropping non-empty table x'))",

			"drop table y",
			"set @Commit3 = (select DOLT_COMMIT('-am', 'Dropping empty table y'))",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM DOLT_DIFF",
				Expected: []sql.Row{{4}},
			},
			{
				Query:    "select table_name, schema_change, data_change from DOLT_DIFF where commit_hash in (@Commit1)",
				Expected: []sql.Row{{"x", true, true}, {"y", true, false}},
			},
			{
				Query:    "select table_name, schema_change, data_change from DOLT_DIFF where commit_hash in (@Commit2)",
				Expected: []sql.Row{{"x", true, true}},
			},
			{
				Query:    "select table_name, schema_change, data_change from DOLT_DIFF where commit_hash in (@Commit3)",
				Expected: []sql.Row{{"y", true, false}},
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
			"set @Commit3 = (select DOLT_COMMIT('-am', 'Inserting into table y'))",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM DOLT_DIFF",
				Expected: []sql.Row{{3}},
			},
			{
				Query:    "select table_name, schema_change, data_change from DOLT_DIFF where commit_hash in (@Commit1)",
				Expected: []sql.Row{{"x", true, true}, {"y", true, false}},
			},
			{
				Query:    "select table_name, schema_change, data_change from DOLT_DIFF where commit_hash in (@Commit2)",
				Expected: []sql.Row{},
			},
			{
				Query:    "select table_name, schema_change, data_change from DOLT_DIFF where commit_hash in (@Commit3)",
				Expected: []sql.Row{{"y", false, true}},
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
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM DOLT_DIFF",
				Expected: []sql.Row{{5}},
			},
			{
				Query:    "select table_name, schema_change, data_change from DOLT_DIFF where commit_hash in (@Commit1)",
				Expected: []sql.Row{{"x", true, true}, {"y", true, false}},
			},
			{
				Query:    "select table_name, schema_change, data_change from DOLT_DIFF where commit_hash in (@Commit2)",
				Expected: []sql.Row{{"z", true, true}},
			},
			{
				Query:    "select table_name, schema_change, data_change from DOLT_DIFF where commit_hash in (@Commit3)",
				Expected: []sql.Row{{"y", false, true}, {"z", false, true}},
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
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM DOLT_DIFF",
				Expected: []sql.Row{{3}},
			},
			{
				Query:    "select table_name, schema_change, data_change from DOLT_DIFF where commit_hash in (@Commit1)",
				Expected: []sql.Row{{"x", true, true}, {"y", true, false}},
			},
			{
				Query:    "select table_name, schema_change, data_change from DOLT_DIFF where commit_hash in (@Commit2)",
				Expected: []sql.Row{{"z", true, true}},
			},
			{
				Query:    "select table_name, schema_change, data_change from DOLT_DIFF where commit_hash in (@Commit3)",
				Expected: []sql.Row{},
			},
		},
	},
}

var CommitDiffSystemTableScriptTests = []queries.ScriptTest{
	{
		Name: "error handling",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 int, c2 int);",
			"insert into t values (1, 2, 3), (4, 5, 6);",
			"set @Commit1 = (select DOLT_COMMIT('-am', 'creating table t'));",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "SELECT * FROM DOLT_COMMIT_DIFF_t;",
				ExpectedErrStr: "error querying table dolt_commit_diff_t: dolt_commit_diff_* tables must be filtered to a single 'to_commit'",
			},
			{
				Query:          "SELECT * FROM DOLT_COMMIT_DIFF_t where to_commit=@Commit1;",
				ExpectedErrStr: "error querying table dolt_commit_diff_t: dolt_commit_diff_* tables must be filtered to a single 'from_commit'",
			},
			{
				Query:          "SELECT * FROM DOLT_COMMIT_DIFF_t where from_commit=@Commit1;",
				ExpectedErrStr: "error querying table dolt_commit_diff_t: dolt_commit_diff_* tables must be filtered to a single 'to_commit'",
			},
		},
	},
	{
		Name: "base case: insert, update, delete",
		SetUpScript: []string{
			"set @Commit0 = HASHOF('HEAD');",
			"create table t (pk int primary key, c1 int, c2 int);",
			"insert into t values (1, 2, 3), (4, 5, 6);",
			"set @Commit1 = (select DOLT_COMMIT('-am', 'creating table t'));",

			"update t set c2=0 where pk=1",
			"set @Commit2 = (select DOLT_COMMIT('-am', 'modifying row'));",

			"update t set c2=-1 where pk=1",
			"set @Commit3 = (select DOLT_COMMIT('-am', 'modifying row'));",

			"update t set c2=-2 where pk=1",
			"set @Commit4 = (select DOLT_COMMIT('-am', 'modifying row'));",

			"delete from t where pk=1",
			"set @Commit5 = (select DOLT_COMMIT('-am', 'modifying row'));",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type FROM DOLT_COMMIT_DIFF_t WHERE TO_COMMIT=@Commit1 and FROM_COMMIT=@Commit0;",
				Expected: []sql.Row{
					{1, 2, 3, nil, nil, nil, "added"},
					{4, 5, 6, nil, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type FROM DOLT_COMMIT_DIFF_t WHERE TO_COMMIT=@Commit2 and FROM_COMMIT=@Commit1 ORDER BY to_pk;",
				Expected: []sql.Row{
					{1, 2, 0, 1, 2, 3, "modified"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type FROM DOLT_COMMIT_DIFF_T WHERE TO_COMMIT=@Commit4 and FROM_COMMIT=@Commit1 ORDER BY to_pk;",
				Expected: []sql.Row{
					{1, 2, -2, 1, 2, 3, "modified"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type FROM DOLT_commit_DIFF_t WHERE TO_COMMIT=@Commit5 and FROM_COMMIT=@Commit4 ORDER BY to_pk;",
				Expected: []sql.Row{
					{nil, nil, nil, 1, 2, -2, "removed"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type FROM DOLT_COMMIT_DIFF_t WHERE TO_COMMIT=@Commit5 and FROM_COMMIT=@Commit0 ORDER BY to_pk;",
				Expected: []sql.Row{
					{4, 5, 6, nil, nil, nil, "added"},
				},
			},
		},
	},
	{
		// When a column is dropped we should see the column's value set to null in that commit
		Name: "schema modification: column drop",
		SetUpScript: []string{
			"set @Commit0 = HASHOF('HEAD');",
			"create table t (pk int primary key, c1 int, c2 int);",
			"insert into t values (1, 2, 3), (4, 5, 6);",
			"set @Commit1 = (select DOLT_COMMIT('-am', 'creating table t'));",

			"alter table t drop column c1;",
			"set @Commit2 = (select DOLT_COMMIT('-am', 'dropping column c'));",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT to_pk, to_c2, from_pk, from_c2 FROM DOLT_COMMIT_DIFF_t WHERE TO_COMMIT=@Commit1 and FROM_COMMIT=@Commit0 ORDER BY to_pk;",
				Expected: []sql.Row{
					{1, 3, nil, nil},
					{4, 6, nil, nil},
				},
			},
			{
				Query: "SELECT to_pk, to_c2, from_pk, from_c2 FROM DOLT_COMMIT_DIFF_t WHERE TO_COMMIT=@Commit2 and FROM_COMMIT=@Commit1 ORDER BY to_pk;",
				Expected: []sql.Row{
					{1, 3, 1, 3},
					{4, 6, 4, 6},
				},
			},
		},
	},
	{
		// When a column is dropped and recreated with the same type, we expect it to be included in dolt_diff output
		Name: "schema modification: column drop, recreate with same type",
		SetUpScript: []string{
			"set @Commit0 = HASHOF('HEAD');",
			"create table t (pk int primary key, c int);",
			"insert into t values (1, 2), (3, 4);",
			"set @Commit1 = (select DOLT_COMMIT('-am', 'creating table t'));",

			"alter table t drop column c;",
			"set @Commit2 = (select DOLT_COMMIT('-am', 'dropping column c'));",

			"alter table t add column c int;",
			"insert into t values (100, 101);",
			"set @Commit3 = (select DOLT_COMMIT('-am', 'inserting into t'));",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT to_pk, to_c, from_pk, from_c, diff_type FROM DOLT_COMMIT_DIFF_t WHERE TO_COMMIT=@Commit1 and FROM_COMMIT=@Commit0 ORDER BY to_pk;",
				Expected: []sql.Row{
					{1, 2, nil, nil, "added"},
					{3, 4, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, from_pk, from_c, diff_type FROM DOLT_COMMIT_DIFF_t WHERE TO_COMMIT=@Commit2 and FROM_COMMIT=@Commit1 ORDER BY to_pk;",
				Expected: []sql.Row{
					{1, 1, 2, "modified"},
					{3, 3, 4, "modified"},
				},
			},
			{
				Query: "SELECT to_pk, to_c, from_pk, from_c, diff_type FROM DOLT_COMMIT_DIFF_t WHERE TO_COMMIT=@Commit3 and FROM_COMMIT=@Commit2 ORDER BY to_pk;",
				Expected: []sql.Row{
					{100, 101, nil, nil, "added"},
				},
			},
		},
	},
	{
		// When a column is dropped and another column with the same type is renamed to that name, we expect it to be included in dolt_diff output
		Name: "schema modification: column drop, rename column with same type to same name",
		SetUpScript: []string{
			"set @Commit0 = HASHOF('HEAD');",
			"create table t (pk int primary key, c1 int, c2 int);",
			"insert into t values (1, 2, 3), (4, 5, 6);",
			"set @Commit1 = DOLT_COMMIT('-am', 'creating table t');",

			"alter table t drop column c1;",
			"set @Commit2 = DOLT_COMMIT('-am', 'dropping column c1');",

			"alter table t rename column c2 to c1;",
			"insert into t values (100, 101);",
			"set @Commit3 = DOLT_COMMIT('-am', 'inserting into t');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT to_pk, to_c1, from_pk, from_c1, diff_type FROM DOLT_COMMIT_DIFF_t WHERE TO_COMMIT=@Commit1 and FROM_COMMIT=@Commit0 ORDER BY to_pk;",
				Expected: []sql.Row{
					{1, 2, nil, nil, "added"},
					{4, 5, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, from_pk, from_c1, diff_type FROM DOLT_COMMIT_DIFF_t WHERE TO_COMMIT=@Commit2 and FROM_COMMIT=@Commit1 ORDER BY to_pk;",
				Expected: []sql.Row{
					{1, nil, 1, 2, "modified"},
					{4, nil, 4, 5, "modified"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, from_pk, from_c1, diff_type FROM DOLT_COMMIT_DIFF_t WHERE TO_COMMIT=@Commit3 and FROM_COMMIT=@Commit2 ORDER BY to_pk;",
				Expected: []sql.Row{
					// TODO: Missing rows here see TestDiffSystemTable tests
					{100, 101, nil, nil, "added"},
				},
			},
		},
	},

	{
		// When a column is dropped and recreated with a different type, we expect only the new column
		// to be included in dolt_commit_diff output, with previous values coerced (with any warnings reported) to the new type
		Name: "schema modification: column drop, recreate with different type that can be coerced (int -> string)",
		SetUpScript: []string{
			"set @Commit0 = HASHOF('HEAD');",
			"create table t (pk int primary key, c int);",
			"insert into t values (1, 2), (3, 4);",
			"set @Commit1 = DOLT_COMMIT('-am', 'creating table t');",

			"alter table t drop column c;",
			"set @Commit2 = DOLT_COMMIT('-am', 'dropping column c');",

			"alter table t add column c text;",
			"insert into t values (100, '101');",
			"set @Commit3 = DOLT_COMMIT('-am', 're-adding column c');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT to_pk, to_c, from_pk, from_c, diff_type FROM DOLT_COMMIT_DIFF_t WHERE TO_COMMIT=@Commit1 and FROM_COMMIT=@Commit0 ORDER BY to_pk;",
				Expected: []sql.Row{
					{1, "2", nil, nil, "added"},
					{3, "4", nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c, from_pk, from_c, diff_type FROM DOLT_COMMIT_DIFF_t WHERE TO_COMMIT=@Commit2 and FROM_COMMIT=@Commit1 ORDER BY to_pk;",
				Expected: []sql.Row{
					{1, nil, 1, "2", "modified"},
					{3, nil, 3, "4", "modified"},
				},
			},
			{
				Query: "SELECT to_pk, to_c, from_pk, from_c, diff_type FROM DOLT_COMMIT_DIFF_t WHERE TO_COMMIT=@Commit3 and FROM_COMMIT=@Commit2 ORDER BY to_pk;",
				Expected: []sql.Row{
					{100, "101", nil, nil, "added"},
				},
			},
		},
	},
	{
		Name: "schema modification: column drop, recreate with different type that can't be coerced (string -> int)",
		SetUpScript: []string{
			"set @Commit0 = HASHOF('HEAD');",
			"create table t (pk int primary key, c text);",
			"insert into t values (1, 'two'), (3, 'four');",
			"set @Commit1 = (select DOLT_COMMIT('-am', 'creating table t'));",

			"alter table t drop column c;",
			"set @Commit2 = (select DOLT_COMMIT('-am', 'dropping column c'));",

			"alter table t add column c int;",
			"insert into t values (100, 101);",
			"set @Commit3 = (select DOLT_COMMIT('-am', 're-adding column c'));",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT to_pk, to_c, from_pk, from_c, diff_type FROM DOLT_COMMIT_DIFF_t WHERE TO_COMMIT=@Commit1 and FROM_COMMIT=@Commit0 ORDER BY to_pk;",
				Expected: []sql.Row{
					{1, nil, nil, nil, "added"},
					{3, nil, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c, from_pk, from_c, diff_type FROM DOLT_COMMIT_DIFF_t WHERE TO_COMMIT=@Commit2 and FROM_COMMIT=@Commit1 ORDER BY to_pk;",
				Expected: []sql.Row{
					{1, nil, 1, nil, "modified"},
					{3, nil, 3, nil, "modified"},
				},
			},
			{
				Query: "SELECT to_pk, to_c, from_pk, from_c, diff_type FROM DOLT_COMMIT_DIFF_t WHERE TO_COMMIT=@Commit3 and FROM_COMMIT=@Commit2 ORDER BY to_pk;",
				Expected: []sql.Row{
					{100, 101, nil, nil, "added"},
				},
			},
			{
				Query:                           "select * from dolt_commit_diff_t where to_commit=@Commit3 and from_commit=@Commit1;",
				ExpectedWarning:                 1105,
				ExpectedWarningsCount:           2,
				ExpectedWarningMessageSubstring: "unable to coerce value from field",
				SkipResultsCheck:                true,
			},
		},
	},
	{
		Name: "schema modification: primary key change",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 int);",
			"insert into t values (1, 2), (3, 4);",
			"set @Commit1 = DOLT_COMMIT('-am', 'creating table t');",

			"alter table t drop primary key;",
			"insert into t values (5, 6);",
			"set @Commit2 = DOLT_COMMIT('-am', 'dropping primary key');",

			"alter table t add primary key (c1);",
			"set @Commit3 = DOLT_COMMIT('-am', 'adding primary key');",

			"insert into t values (7, 8);",
			"set @Commit4 = DOLT_COMMIT('-am', 'adding more data');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:                           "select * from dolt_commit_diff_t where from_commit=@Commit1 and to_commit=@Commit4;",
				ExpectedWarning:                 1105,
				ExpectedWarningsCount:           1,
				ExpectedWarningMessageSubstring: "cannot render full diff between commits",
				SkipResultsCheck:                true,
			},
			{
				Query:    "SELECT to_pk, to_c1, from_pk, from_c1, diff_type FROM DOLT_commit_DIFF_t where from_commit=@Commit3 and to_commit=@Commit4;",
				Expected: []sql.Row{{7, 8, nil, nil, "added"}},
			},
		},
	},
}

// DoltDiffPlanTests are tests that check our query plans for various operations on the dolt diff system tables
var DoltDiffPlanTests = []queries.QueryPlanTest{
	{
		Query: `select * from dolt_diff_one_pk where to_pk=1`,
		ExpectedPlan: "Exchange(parallelism=2)\n" +
			" └─ IndexedTableAccess(dolt_diff_one_pk on [dolt_diff_one_pk.to_pk] with ranges: [{[1, 1]}])\n" +
			"",
	},
	{
		Query: `select * from dolt_diff_one_pk where to_pk>=10 and to_pk<=100`,
		ExpectedPlan: "Exchange(parallelism=2)\n" +
			" └─ IndexedTableAccess(dolt_diff_one_pk on [dolt_diff_one_pk.to_pk] with ranges: [{[10, 100]}])\n" +
			"",
	},
	{
		Query: `select * from dolt_diff_two_pk where to_pk1=1`,
		ExpectedPlan: "Exchange(parallelism=2)\n" +
			" └─ IndexedTableAccess(dolt_diff_two_pk on [dolt_diff_two_pk.to_pk1,dolt_diff_two_pk.to_pk2] with ranges: [{[1, 1], (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `select * from dolt_diff_two_pk where to_pk1=1 and to_pk2=2`,
		ExpectedPlan: "Exchange(parallelism=2)\n" +
			" └─ IndexedTableAccess(dolt_diff_two_pk on [dolt_diff_two_pk.to_pk1,dolt_diff_two_pk.to_pk2] with ranges: [{[1, 1], [2, 2]}])\n" +
			"",
	},
	{
		Query: `select * from dolt_diff_two_pk where to_pk1 < 1 and to_pk2 > 10`,
		ExpectedPlan: "Exchange(parallelism=2)\n" +
			" └─ IndexedTableAccess(dolt_diff_two_pk on [dolt_diff_two_pk.to_pk1,dolt_diff_two_pk.to_pk2] with ranges: [{(-∞, 1), (10, ∞)}])\n" +
			"",
	},
}
