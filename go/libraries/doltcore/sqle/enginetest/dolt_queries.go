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
			"create table t1 (n int primary key, de text);",
			"insert into t1 values (1, 'Eins'), (2, 'Zwei'), (3, 'Drei');",
			"set @Commit1 = dolt_commit('-am', 'inserting into t1');",

			"alter table t1 add column fr text;",
			"insert into t1 values (4, 'Vier', 'Quatre');",
			"set @Commit2 = dolt_commit('-am', 'adding column and inserting data in t1');",

			"update t1 set fr='Un' where n=1;",
			"update t1 set fr='Deux' where n=2;",
			"set @Commit3 = dolt_commit('-am', 'updating data in t1');",

			"update t1 set de=concat(de, ', meine herren') where n>1;",
			"set @Commit4 = dolt_commit('-am', 'be polite when you address a gentleman');",

			"delete from t1 where n=2;",
			"set @Commit5 = dolt_commit('-am', 'we don''t need the number 2');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select count(*) from Dolt_History_t1;",
				Expected: []sql.Row{{18}},
			},
			{
				Query:    "select n, de, fr from dolt_history_T1 where commit_hash = @Commit1;",
				Expected: []sql.Row{{1, "Eins", nil}, {2, "Zwei", nil}, {3, "Drei", nil}},
			},
			{
				Query:    "select n, de, fr from dolt_history_T1 where commit_hash = @Commit2;",
				Expected: []sql.Row{{1, "Eins", nil}, {2, "Zwei", nil}, {3, "Drei", nil}, {4, "Vier", "Quatre"}},
			},
			{
				Query:    "select n, de, fr from dolt_history_T1 where commit_hash = @Commit3;",
				Expected: []sql.Row{{1, "Eins", "Un"}, {2, "Zwei", "Deux"}, {3, "Drei", nil}, {4, "Vier", "Quatre"}},
			},
			{
				Query: "select n, de, fr from dolt_history_T1 where commit_hash = @Commit4;",
				Expected: []sql.Row{
					{1, "Eins", "Un"},
					{2, "Zwei, meine herren", "Deux"},
					{3, "Drei, meine herren", nil},
					{4, "Vier, meine herren", "Quatre"},
				},
			},
			{
				Query: "select n, de, fr from dolt_history_T1 where commit_hash = @Commit5;",
				Expected: []sql.Row{
					{1, "Eins", "Un"},
					{3, "Drei, meine herren", nil},
					{4, "Vier, meine herren", "Quatre"},
				},
			},
			{
				Query: "select de, fr, commit_hash=@commit1, commit_hash=@commit2, commit_hash=@commit3, commit_hash=@commit4" +
					" from dolt_history_T1 where n=2 order by commit_date",
				Expected: []sql.Row{
					{"Zwei", nil, true, false, false, false},
					{"Zwei", nil, false, true, false, false},
					{"Zwei", "Deux", false, false, true, false},
					{"Zwei, meine herren", "Deux", false, false, false, true},
				},
			},
		},
	},
	{
		Name: "index by primary key",
		SetUpScript: []string{
			"create table t1 (pk int primary key, c int);",
			"insert into t1 values (1,2), (3,4)",
			"set @Commit1 = dolt_commit('-am', 'initial table');",
			"insert into t1 values (5,6), (7,8)",
			"set @Commit2 = dolt_commit('-am', 'two more rows');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select pk, c, commit_hash = @Commit1, commit_hash = @Commit2 from dolt_history_t1",
				Expected: []sql.Row{
					{1, 2, false, true},
					{3, 4, false, true},
					{5, 6, false, true},
					{7, 8, false, true},
					{1, 2, true, false},
					{3, 4, true, false},
				},
			},
			{
				Query: "select pk, c from dolt_history_t1 order by pk",
				Expected: []sql.Row{
					{1, 2},
					{1, 2},
					{3, 4},
					{3, 4},
					{5, 6},
					{7, 8},
				},
			},
			{
				Query: "select pk, c from dolt_history_t1 order by pk, c",
				Expected: []sql.Row{
					{1, 2},
					{1, 2},
					{3, 4},
					{3, 4},
					{5, 6},
					{7, 8},
				},
			},
			{
				Query: "select pk, c from dolt_history_t1 where pk = 3",
				Expected: []sql.Row{
					{3, 4},
					{3, 4},
				},
			},
			{
				Query: "select pk, c from dolt_history_t1 where pk = 3 and commit_hash = @Commit2",
				Expected: []sql.Row{
					{3, 4},
				},
			},
			{
				Query: "explain select pk, c from dolt_history_t1 where pk = 3",
				Expected: []sql.Row{
					{"Project(dolt_history_t1.pk, dolt_history_t1.c)"},
					{" └─ Filter(dolt_history_t1.pk = 3)"},
					{"     └─ Projected table access on [pk c]"},
					{"         └─ Exchange"},
					{"             └─ IndexedTableAccess(dolt_history_t1 on [dolt_history_t1.pk] with ranges: [{[3, 3]}])"},
				},
			},
			{
				Query: "explain select pk, c from dolt_history_t1 where pk = 3 and committer = 'someguy'",
				Expected: []sql.Row{
					{"Project(dolt_history_t1.pk, dolt_history_t1.c)"},
					{" └─ Filter((dolt_history_t1.pk = 3) AND (dolt_history_t1.committer = \"someguy\"))"},
					{"     └─ Projected table access on [pk c committer]"},
					{"         └─ Exchange"},
					{"             └─ IndexedTableAccess(dolt_history_t1 on [dolt_history_t1.pk] with ranges: [{[3, 3]}])"},
				},
			},
		},
	},
	{
		Name: "adding an index",
		SetUpScript: []string{
			"create table t1 (pk int primary key, c int);",
			"insert into t1 values (1,2), (3,4)",
			"set @Commit1 = dolt_commit('-am', 'initial table');",
			"insert into t1 values (5,6), (7,8)",
			"set @Commit2 = dolt_commit('-am', 'two more rows');",
			"insert into t1 values (9,10), (11,12)",
			"create index t1_c on t1(c)",
			"set @Commit2 = dolt_commit('-am', 'two more rows and an index');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select pk, c from dolt_history_t1 order by pk",
				Expected: []sql.Row{
					{1, 2},
					{1, 2},
					{1, 2},
					{3, 4},
					{3, 4},
					{3, 4},
					{5, 6},
					{5, 6},
					{7, 8},
					{7, 8},
					{9, 10},
					{11, 12},
				},
			},
			{
				Query: "select pk, c from dolt_history_t1 where c = 4 order by pk",
				Expected: []sql.Row{
					{3, 4},
					{3, 4},
					{3, 4},
				},
			},
			{
				Query: "select pk, c from dolt_history_t1 where c = 10 order by pk",
				Expected: []sql.Row{
					{9, 10},
				},
			},
			{
				Query: "explain select pk, c from dolt_history_t1 where c = 4",
				Expected: []sql.Row{
					{"Project(dolt_history_t1.pk, dolt_history_t1.c)"},
					{" └─ Filter(dolt_history_t1.c = 4)"},
					{"     └─ Projected table access on [pk c]"},
					{"         └─ Exchange"},
					{"             └─ IndexedTableAccess(dolt_history_t1 on [dolt_history_t1.c] with ranges: [{[4, 4]}])"},
				},
			},
			{
				Query: "explain select pk, c from dolt_history_t1 where c = 10 and committer = 'someguy'",
				Expected: []sql.Row{
					{"Project(dolt_history_t1.pk, dolt_history_t1.c)"},
					{" └─ Filter((dolt_history_t1.c = 10) AND (dolt_history_t1.committer = \"someguy\"))"},
					{"     └─ Projected table access on [pk c committer]"},
					{"         └─ Exchange"},
					{"             └─ IndexedTableAccess(dolt_history_t1 on [dolt_history_t1.c] with ranges: [{[10, 10]}])"}},
			},
		},
	},
	{
		Name: "primary key table: non-pk column drops and adds",
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
				Query:    "select pk, c2 from dolt_history_t where commit_hash=@Commit1 order by pk;",
				Expected: []sql.Row{{1, nil}, {4, nil}},
			},
			{
				Query:    "select pk, c2 from dolt_history_t where commit_hash=@Commit2 order by pk;",
				Expected: []sql.Row{{1, nil}, {4, nil}},
			},
			{
				Query:    "select pk, c2 from dolt_history_t where commit_hash=@Commit3 order by pk;",
				Expected: []sql.Row{{1, 2}, {4, 5}},
			},
		},
	},
	{
		Name: "primary key table: non-pk column type changes",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 int, c2 text);",
			"insert into t values (1, 2, '3'), (4, 5, '6');",
			"set @Commit1 = DOLT_COMMIT('-am', 'creating table t');",
			"alter table t modify column c2 int;",
			"set @Commit2 = DOLT_COMMIT('-am', 'changed type of c2');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select count(*) from dolt_history_t;",
				Expected: []sql.Row{{4}},
			},
			// Can't represent the old schema in the current one, so it gets nil valued
			{
				Query:    "select pk, c2 from dolt_history_t where commit_hash=@Commit1 order by pk;",
				Expected: []sql.Row{{1, nil}, {4, nil}},
			},
			{
				Query:    "select pk, c2 from dolt_history_t where commit_hash=@Commit2 order by pk;",
				Expected: []sql.Row{{1, 3}, {4, 6}},
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
		Name: "working set changes",
		SetUpScript: []string{
			"create table regularTable (a int primary key, b int, c int);",
			"create table droppedTable (a int primary key, b int, c int);",
			"create table renamedEmptyTable (a int primary key, b int, c int);",
			"insert into droppedTable values (1, 2, 3), (2, 3, 4);",
			"set @Commit1 = (select DOLT_COMMIT('-am', 'Creating tables x and y'));",

			// data change: false; schema change: true
			"create table addedTable (a int primary key, b int, c int);",
			"call DOLT_ADD('addedTable');",
			// data change: true; schema change: true
			"drop table droppedTable;",
			"call DOLT_ADD('droppedTable');",
			// data change: false; schema change: true
			"rename table renamedEmptyTable to newRenamedEmptyTable",
			// data change: true; schema change: false
			"insert into regularTable values (1, 2, 3);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM DOLT_DIFF;",
				Expected: []sql.Row{{7}},
			},
			{
				Query: "SELECT * FROM DOLT_DIFF WHERE COMMIT_HASH='WORKING' ORDER BY table_name;",
				Expected: []sql.Row{
					{"WORKING", "addedTable", "NULL", "NULL", "NULL", "NULL", false, true},
					{"WORKING", "droppedTable", "NULL", "NULL", "NULL", "NULL", true, true},
					{"WORKING", "newRenamedEmptyTable", "NULL", "NULL", "NULL", "NULL", false, true},
					{"WORKING", "regularTable", "NULL", "NULL", "NULL", "NULL", true, false},
				},
			},
		},
	},
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
		ExpectedPlan: "Exchange\n" +
			" └─ IndexedTableAccess(dolt_diff_one_pk on [dolt_diff_one_pk.to_pk] with ranges: [{[1, 1]}])\n" +
			"",
	},
	{
		Query: `select * from dolt_diff_one_pk where to_pk>=10 and to_pk<=100`,
		ExpectedPlan: "Exchange\n" +
			" └─ IndexedTableAccess(dolt_diff_one_pk on [dolt_diff_one_pk.to_pk] with ranges: [{[10, 100]}])\n" +
			"",
	},
	{
		Query: `select * from dolt_diff_two_pk where to_pk1=1`,
		ExpectedPlan: "Exchange\n" +
			" └─ IndexedTableAccess(dolt_diff_two_pk on [dolt_diff_two_pk.to_pk1,dolt_diff_two_pk.to_pk2] with ranges: [{[1, 1], (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `select * from dolt_diff_two_pk where to_pk1=1 and to_pk2=2`,
		ExpectedPlan: "Exchange\n" +
			" └─ IndexedTableAccess(dolt_diff_two_pk on [dolt_diff_two_pk.to_pk1,dolt_diff_two_pk.to_pk2] with ranges: [{[1, 1], [2, 2]}])\n" +
			"",
	},
	{
		Query: `select * from dolt_diff_two_pk where to_pk1 < 1 and to_pk2 > 10`,
		ExpectedPlan: "Exchange\n" +
			" └─ IndexedTableAccess(dolt_diff_two_pk on [dolt_diff_two_pk.to_pk1,dolt_diff_two_pk.to_pk2] with ranges: [{(-∞, 1), (10, ∞)}])\n" +
			"",
	},
}

var NewFormatQueryPlanTests = []queries.QueryPlanTest{
	{
		Query: `SELECT * FROM one_pk ORDER BY pk`,
		ExpectedPlan: "Projected table access on [pk c1 c2 c3 c4 c5]\n" +
			" └─ IndexedTableAccess(one_pk on [one_pk.pk] with ranges: [{(-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM two_pk ORDER BY pk1, pk2`,
		ExpectedPlan: "Projected table access on [pk1 pk2 c1 c2 c3 c4 c5]\n" +
			" └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2] with ranges: [{(-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM two_pk ORDER BY pk1`,
		ExpectedPlan: "Projected table access on [pk1 pk2 c1 c2 c3 c4 c5]\n" +
			" └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2] with ranges: [{(-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT pk1 AS one, pk2 AS two FROM two_pk ORDER BY pk1, pk2`,
		ExpectedPlan: "Project(two_pk.pk1 as one, two_pk.pk2 as two)\n" +
			" └─ Projected table access on [pk1 pk2]\n" +
			"     └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2] with ranges: [{(-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT pk1 AS one, pk2 AS two FROM two_pk ORDER BY one, two`,
		ExpectedPlan: "Project(two_pk.pk1 as one, two_pk.pk2 as two)\n" +
			" └─ Projected table access on [pk1 pk2]\n" +
			"     └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2] with ranges: [{(-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT t1.i FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 1`,
		ExpectedPlan: "Project(t1.i)\n" +
			" └─ IndexedJoin(t1.i = (t2.i + 1))\n" +
			"     ├─ Filter(t2.i = 1)\n" +
			"     │   └─ TableAlias(t2)\n" +
			"     │       └─ IndexedTableAccess(mytable on [mytable.i] with ranges: [{[1, 1]}])\n" +
			"     └─ Filter(t1.i = 2)\n" +
			"         └─ TableAlias(t1)\n" +
			"             └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `select row_number() over (order by i desc), mytable.i as i2 
				from mytable join othertable on i = i2 order by 1`,
		ExpectedPlan: "Sort(row_number() over (order by i desc) ASC)\n" +
			" └─ Project(row_number() over ( order by [mytable.i, idx=0, type=BIGINT, nullable=false] DESC) as row_number() over (order by i desc), i2)\n" +
			"     └─ Window(row_number() over ( order by [mytable.i, idx=0, type=BIGINT, nullable=false] DESC), mytable.i as i2)\n" +
			"         └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"             ├─ Table(mytable)\n" +
			"             └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM one_pk_two_idx WHERE v1 < 2 AND v2 IS NOT NULL`,
		ExpectedPlan: "Filter((one_pk_two_idx.v1 < 2) AND (NOT(one_pk_two_idx.v2 IS NULL)))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(one_pk_two_idx on [one_pk_two_idx.v1,one_pk_two_idx.v2] with ranges: [{(-∞, 2), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM one_pk_two_idx WHERE v1 IN (1, 2) AND v2 <= 2`,
		ExpectedPlan: "Filter((one_pk_two_idx.v1 HASH IN (1, 2)) AND (one_pk_two_idx.v2 <= 2))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(one_pk_two_idx on [one_pk_two_idx.v1,one_pk_two_idx.v2] with ranges: [{[2, 2], (-∞, 2]}, {[1, 1], (-∞, 2]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM one_pk_three_idx WHERE v1 > 2 AND v2 = 3`,
		ExpectedPlan: "Filter((one_pk_three_idx.v1 > 2) AND (one_pk_three_idx.v2 = 3))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(one_pk_three_idx on [one_pk_three_idx.v1,one_pk_three_idx.v2,one_pk_three_idx.v3] with ranges: [{(2, ∞), [3, 3], (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM one_pk_three_idx WHERE v1 > 2 AND v3 = 3`,
		ExpectedPlan: "Filter((one_pk_three_idx.v1 > 2) AND (one_pk_three_idx.v3 = 3))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(one_pk_three_idx on [one_pk_three_idx.v1,one_pk_three_idx.v2,one_pk_three_idx.v3] with ranges: [{(2, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `select row_number() over (order by i desc), mytable.i as i2 
				from mytable join othertable on i = i2
				where mytable.i = 2
				order by 1`,
		ExpectedPlan: "Sort(row_number() over (order by i desc) ASC)\n" +
			" └─ Project(row_number() over ( order by [mytable.i, idx=0, type=BIGINT, nullable=false] DESC) as row_number() over (order by i desc), i2)\n" +
			"     └─ Window(row_number() over ( order by [mytable.i, idx=0, type=BIGINT, nullable=false] DESC), mytable.i as i2)\n" +
			"         └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"             ├─ Filter(mytable.i = 2)\n" +
			"             │   └─ IndexedTableAccess(mytable on [mytable.i] with ranges: [{[2, 2]}])\n" +
			"             └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"",
	},
	{
		Query: `INSERT INTO mytable(i,s) SELECT t1.i, 'hello' FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 1`,
		ExpectedPlan: "Insert(i, s)\n" +
			" ├─ Table(mytable)\n" +
			" └─ Project(i, s)\n" +
			"     └─ Project(t1.i, \"hello\")\n" +
			"         └─ IndexedJoin(t1.i = (t2.i + 1))\n" +
			"             ├─ Filter(t2.i = 1)\n" +
			"             │   └─ TableAlias(t2)\n" +
			"             │       └─ IndexedTableAccess(mytable on [mytable.i] with ranges: [{[1, 1]}])\n" +
			"             └─ Filter(t1.i = 2)\n" +
			"                 └─ TableAlias(t1)\n" +
			"                     └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `SELECT /*+ JOIN_ORDER(t1, t2) */ t1.i FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 1`,
		ExpectedPlan: "Project(t1.i)\n" +
			" └─ InnerJoin(t1.i = (t2.i + 1))\n" +
			"     ├─ Filter(t1.i = 2)\n" +
			"     │   └─ Projected table access on [i]\n" +
			"     │       └─ TableAlias(t1)\n" +
			"     │           └─ IndexedTableAccess(mytable on [mytable.i] with ranges: [{[2, 2]}])\n" +
			"     └─ Filter(t2.i = 1)\n" +
			"         └─ Projected table access on [i]\n" +
			"             └─ TableAlias(t2)\n" +
			"                 └─ IndexedTableAccess(mytable on [mytable.i] with ranges: [{[1, 1]}])\n" +
			"",
	},
	{
		Query: `SELECT /*+ JOIN_ORDER(t1, mytable) */ t1.i FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 1`,
		ExpectedPlan: "Project(t1.i)\n" +
			" └─ IndexedJoin(t1.i = (t2.i + 1))\n" +
			"     ├─ Filter(t2.i = 1)\n" +
			"     │   └─ TableAlias(t2)\n" +
			"     │       └─ IndexedTableAccess(mytable on [mytable.i] with ranges: [{[1, 1]}])\n" +
			"     └─ Filter(t1.i = 2)\n" +
			"         └─ TableAlias(t1)\n" +
			"             └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `SELECT /*+ JOIN_ORDER(t1, t2, t3) */ t1.i FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 1`,
		ExpectedPlan: "Project(t1.i)\n" +
			" └─ IndexedJoin(t1.i = (t2.i + 1))\n" +
			"     ├─ Filter(t2.i = 1)\n" +
			"     │   └─ TableAlias(t2)\n" +
			"     │       └─ IndexedTableAccess(mytable on [mytable.i] with ranges: [{[1, 1]}])\n" +
			"     └─ Filter(t1.i = 2)\n" +
			"         └─ TableAlias(t1)\n" +
			"             └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `SELECT t1.i FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 1`,
		ExpectedPlan: "Project(t1.i)\n" +
			" └─ IndexedJoin(t1.i = (t2.i + 1))\n" +
			"     ├─ Filter(t2.i = 1)\n" +
			"     │   └─ TableAlias(t2)\n" +
			"     │       └─ IndexedTableAccess(mytable on [mytable.i] with ranges: [{[1, 1]}])\n" +
			"     └─ Filter(t1.i = 2)\n" +
			"         └─ TableAlias(t1)\n" +
			"             └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2`,
		ExpectedPlan: "Project(mytable.i, othertable.i2, othertable.s2)\n" +
			" └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"     ├─ Table(mytable)\n" +
			"     └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"",
	},
	{
		Query: `SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2 OR s = s2`,
		ExpectedPlan: "Project(mytable.i, othertable.i2, othertable.s2)\n" +
			" └─ IndexedJoin((mytable.i = othertable.i2) OR (mytable.s = othertable.s2))\n" +
			"     ├─ Table(mytable)\n" +
			"     └─ Concat\n" +
			"         ├─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"         └─ IndexedTableAccess(othertable on [othertable.s2])\n" +
			"",
	},
	{
		Query: `SELECT i, i2, s2 FROM mytable INNER JOIN othertable ot ON i = i2 OR s = s2`,
		ExpectedPlan: "Project(mytable.i, ot.i2, ot.s2)\n" +
			" └─ IndexedJoin((mytable.i = ot.i2) OR (mytable.s = ot.s2))\n" +
			"     ├─ Table(mytable)\n" +
			"     └─ TableAlias(ot)\n" +
			"         └─ Concat\n" +
			"             ├─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"             └─ IndexedTableAccess(othertable on [othertable.s2])\n" +
			"",
	},
	{
		Query: `SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2 OR SUBSTRING_INDEX(s, ' ', 1) = s2`,
		ExpectedPlan: "Project(mytable.i, othertable.i2, othertable.s2)\n" +
			" └─ IndexedJoin((mytable.i = othertable.i2) OR (SUBSTRING_INDEX(mytable.s, \" \", 1) = othertable.s2))\n" +
			"     ├─ Table(mytable)\n" +
			"     └─ Concat\n" +
			"         ├─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"         └─ IndexedTableAccess(othertable on [othertable.s2])\n" +
			"",
	},
	{
		Query: `SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2 OR SUBSTRING_INDEX(s, ' ', 1) = s2 OR SUBSTRING_INDEX(s, ' ', 2) = s2`,
		ExpectedPlan: "Project(mytable.i, othertable.i2, othertable.s2)\n" +
			" └─ IndexedJoin(((mytable.i = othertable.i2) OR (SUBSTRING_INDEX(mytable.s, \" \", 1) = othertable.s2)) OR (SUBSTRING_INDEX(mytable.s, \" \", 2) = othertable.s2))\n" +
			"     ├─ Table(mytable)\n" +
			"     └─ Concat\n" +
			"         ├─ Concat\n" +
			"         │   ├─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"         │   └─ IndexedTableAccess(othertable on [othertable.s2])\n" +
			"         └─ IndexedTableAccess(othertable on [othertable.s2])\n" +
			"",
	},
	{
		Query: `SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2 UNION SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2`,
		ExpectedPlan: "Distinct\n" +
			" └─ Union\n" +
			"     ├─ Project(mytable.i, othertable.i2, othertable.s2)\n" +
			"     │   └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"     │       ├─ Table(mytable)\n" +
			"     │       └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"     └─ Project(mytable.i, othertable.i2, othertable.s2)\n" +
			"         └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"             ├─ Table(mytable)\n" +
			"             └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"",
	},
	{
		Query: `SELECT sub.i, sub.i2, sub.s2, ot.i2, ot.s2 FROM (SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2) sub INNER JOIN othertable ot ON sub.i = ot.i2`,
		ExpectedPlan: "Project(sub.i, sub.i2, sub.s2, ot.i2, ot.s2)\n" +
			" └─ IndexedJoin(sub.i = ot.i2)\n" +
			"     ├─ SubqueryAlias(sub)\n" +
			"     │   └─ Project(mytable.i, othertable.i2, othertable.s2)\n" +
			"     │       └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"     │           ├─ Table(mytable)\n" +
			"     │           └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"     └─ TableAlias(ot)\n" +
			"         └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"",
	},
	{
		Query: `SELECT sub.i, sub.i2, sub.s2, ot.i2, ot.s2 FROM othertable ot INNER JOIN (SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2) sub ON sub.i = ot.i2`,
		ExpectedPlan: "Project(sub.i, sub.i2, sub.s2, ot.i2, ot.s2)\n" +
			" └─ IndexedJoin(sub.i = ot.i2)\n" +
			"     ├─ SubqueryAlias(sub)\n" +
			"     │   └─ Project(mytable.i, othertable.i2, othertable.s2)\n" +
			"     │       └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"     │           ├─ Table(mytable)\n" +
			"     │           └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"     └─ TableAlias(ot)\n" +
			"         └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"",
	},
	{
		Query: `SELECT sub.i, sub.i2, sub.s2, ot.i2, ot.s2 FROM othertable ot LEFT JOIN (SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2 WHERE CONVERT(s2, signed) <> 0) sub ON sub.i = ot.i2 WHERE ot.i2 > 0`,
		ExpectedPlan: "Project(sub.i, sub.i2, sub.s2, ot.i2, ot.s2)\n" +
			" └─ LeftJoin(sub.i = ot.i2)\n" +
			"     ├─ Filter(ot.i2 > 0)\n" +
			"     │   └─ TableAlias(ot)\n" +
			"     │       └─ IndexedTableAccess(othertable on [othertable.i2] with ranges: [{(0, ∞)}])\n" +
			"     └─ HashLookup(child: (sub.i), lookup: (ot.i2))\n" +
			"         └─ CachedResults\n" +
			"             └─ SubqueryAlias(sub)\n" +
			"                 └─ Project(mytable.i, othertable.i2, othertable.s2)\n" +
			"                     └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"                         ├─ Table(mytable)\n" +
			"                         └─ Filter(NOT((convert(othertable.s2, signed) = 0)))\n" +
			"                             └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"",
	},
	{
		Query: `select /*+ JOIN_ORDER( i, k, j ) */  * from one_pk i join one_pk k on i.pk = k.pk join (select pk, rand() r from one_pk) j on i.pk = j.pk`,
		ExpectedPlan: "IndexedJoin(i.pk = j.pk)\n" +
			" ├─ IndexedJoin(i.pk = k.pk)\n" +
			" │   ├─ TableAlias(i)\n" +
			" │   │   └─ Table(one_pk)\n" +
			" │   └─ TableAlias(k)\n" +
			" │       └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			" └─ HashLookup(child: (j.pk), lookup: (i.pk))\n" +
			"     └─ CachedResults\n" +
			"         └─ SubqueryAlias(j)\n" +
			"             └─ Project(one_pk.pk, RAND() as r)\n" +
			"                 └─ Projected table access on [pk]\n" +
			"                     └─ Table(one_pk)\n" +
			"",
	},
	{
		Query: `INSERT INTO mytable SELECT sub.i + 10, ot.s2 FROM othertable ot INNER JOIN (SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2) sub ON sub.i = ot.i2`,
		ExpectedPlan: "Insert()\n" +
			" ├─ Table(mytable)\n" +
			" └─ Project(i, s)\n" +
			"     └─ Project((sub.i + 10), ot.s2)\n" +
			"         └─ IndexedJoin(sub.i = ot.i2)\n" +
			"             ├─ SubqueryAlias(sub)\n" +
			"             │   └─ Project(mytable.i)\n" +
			"             │       └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"             │           ├─ Table(mytable)\n" +
			"             │           └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"             └─ TableAlias(ot)\n" +
			"                 └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"",
	},
	{
		Query: `SELECT mytable.i, selfjoin.i FROM mytable INNER JOIN mytable selfjoin ON mytable.i = selfjoin.i WHERE selfjoin.i IN (SELECT 1 FROM DUAL)`,
		ExpectedPlan: "Project(mytable.i, selfjoin.i)\n" +
			" └─ Filter(selfjoin.i IN (Project(1)\n" +
			"     └─ Table(dual)\n" +
			"    ))\n" +
			"     └─ IndexedJoin(mytable.i = selfjoin.i)\n" +
			"         ├─ Table(mytable)\n" +
			"         └─ TableAlias(selfjoin)\n" +
			"             └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `SELECT s2, i2, i FROM mytable INNER JOIN othertable ON i = i2`,
		ExpectedPlan: "Project(othertable.s2, othertable.i2, mytable.i)\n" +
			" └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"     ├─ Table(mytable)\n" +
			"     └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"",
	},
	{
		Query: `SELECT i, i2, s2 FROM othertable JOIN mytable ON i = i2`,
		ExpectedPlan: "Project(mytable.i, othertable.i2, othertable.s2)\n" +
			" └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"     ├─ Table(othertable)\n" +
			"     └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `SELECT s2, i2, i FROM othertable JOIN mytable ON i = i2`,
		ExpectedPlan: "Project(othertable.s2, othertable.i2, mytable.i)\n" +
			" └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"     ├─ Table(othertable)\n" +
			"     └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `SELECT s2, i2, i FROM othertable JOIN mytable ON i = i2`,
		ExpectedPlan: "Project(othertable.s2, othertable.i2, mytable.i)\n" +
			" └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"     ├─ Table(othertable)\n" +
			"     └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `SELECT s2, i2, i FROM othertable JOIN mytable ON i = i2 LIMIT 1`,
		ExpectedPlan: "Limit(1)\n" +
			" └─ Project(othertable.s2, othertable.i2, mytable.i)\n" +
			"     └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"         ├─ Table(othertable)\n" +
			"         └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i2 = i`,
		ExpectedPlan: "Project(mytable.i, othertable.i2, othertable.s2)\n" +
			" └─ IndexedJoin(othertable.i2 = mytable.i)\n" +
			"     ├─ Table(mytable)\n" +
			"     └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"",
	},
	{
		Query: `SELECT s2, i2, i FROM mytable INNER JOIN othertable ON i2 = i`,
		ExpectedPlan: "Project(othertable.s2, othertable.i2, mytable.i)\n" +
			" └─ IndexedJoin(othertable.i2 = mytable.i)\n" +
			"     ├─ Table(mytable)\n" +
			"     └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM MYTABLE JOIN OTHERTABLE ON i = i2 AND NOT (s2 <=> s)`,
		ExpectedPlan: "IndexedJoin((mytable.i = othertable.i2) AND (NOT((othertable.s2 <=> mytable.s))))\n" +
			" ├─ Table(mytable)\n" +
			" └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM MYTABLE JOIN OTHERTABLE ON i = i2 AND NOT (s2 = s)`,
		ExpectedPlan: "IndexedJoin((mytable.i = othertable.i2) AND (NOT((othertable.s2 = mytable.s))))\n" +
			" ├─ Table(mytable)\n" +
			" └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM MYTABLE JOIN OTHERTABLE ON i = i2 AND CONCAT(s, s2) IS NOT NULL`,
		ExpectedPlan: "IndexedJoin((mytable.i = othertable.i2) AND (NOT(concat(mytable.s, othertable.s2) IS NULL)))\n" +
			" ├─ Table(mytable)\n" +
			" └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM MYTABLE JOIN OTHERTABLE ON i = i2 AND s > s2`,
		ExpectedPlan: "InnerJoin((mytable.i = othertable.i2) AND (mytable.s > othertable.s2))\n" +
			" ├─ Projected table access on [i s]\n" +
			" │   └─ Table(mytable)\n" +
			" └─ Projected table access on [s2 i2]\n" +
			"     └─ Table(othertable)\n" +
			"",
	},
	{
		Query: `SELECT * FROM MYTABLE JOIN OTHERTABLE ON i = i2 AND NOT(s > s2)`,
		ExpectedPlan: "InnerJoin((mytable.i = othertable.i2) AND (NOT((mytable.s > othertable.s2))))\n" +
			" ├─ Projected table access on [i s]\n" +
			" │   └─ Table(mytable)\n" +
			" └─ Projected table access on [s2 i2]\n" +
			"     └─ Table(othertable)\n" +
			"",
	},
	{
		Query: `SELECT /*+ JOIN_ORDER(mytable, othertable) */ s2, i2, i FROM mytable INNER JOIN (SELECT * FROM othertable) othertable ON i2 = i`,
		ExpectedPlan: "Project(othertable.s2, othertable.i2, mytable.i)\n" +
			" └─ InnerJoin(othertable.i2 = mytable.i)\n" +
			"     ├─ Table(mytable)\n" +
			"     └─ HashLookup(child: (othertable.i2), lookup: (mytable.i))\n" +
			"         └─ CachedResults\n" +
			"             └─ SubqueryAlias(othertable)\n" +
			"                 └─ Projected table access on [s2 i2]\n" +
			"                     └─ Table(othertable)\n" +
			"",
	},
	{
		Query: `SELECT s2, i2, i FROM mytable LEFT JOIN (SELECT * FROM othertable) othertable ON i2 = i`,
		ExpectedPlan: "Project(othertable.s2, othertable.i2, mytable.i)\n" +
			" └─ LeftJoin(othertable.i2 = mytable.i)\n" +
			"     ├─ Table(mytable)\n" +
			"     └─ HashLookup(child: (othertable.i2), lookup: (mytable.i))\n" +
			"         └─ CachedResults\n" +
			"             └─ SubqueryAlias(othertable)\n" +
			"                 └─ Projected table access on [s2 i2]\n" +
			"                     └─ Table(othertable)\n" +
			"",
	},
	{
		Query: `SELECT s2, i2, i FROM (SELECT * FROM mytable) mytable RIGHT JOIN (SELECT * FROM othertable) othertable ON i2 = i`,
		ExpectedPlan: "Project(othertable.s2, othertable.i2, mytable.i)\n" +
			" └─ RightJoin(othertable.i2 = mytable.i)\n" +
			"     ├─ HashLookup(child: (mytable.i), lookup: (othertable.i2))\n" +
			"     │   └─ CachedResults\n" +
			"     │       └─ SubqueryAlias(mytable)\n" +
			"     │           └─ Projected table access on [i s]\n" +
			"     │               └─ Table(mytable)\n" +
			"     └─ SubqueryAlias(othertable)\n" +
			"         └─ Projected table access on [s2 i2]\n" +
			"             └─ Table(othertable)\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a WHERE a.s is not null`,
		ExpectedPlan: "Filter(NOT(a.s IS NULL))\n" +
			" └─ Projected table access on [i s]\n" +
			"     └─ TableAlias(a)\n" +
			"         └─ IndexedTableAccess(mytable on [mytable.s] with ranges: [{(<nil>, ∞)}, {(-∞, <nil>)}])\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a inner join mytable b on (a.i = b.s) WHERE a.s is not null`,
		ExpectedPlan: "Project(a.i, a.s)\n" +
			" └─ IndexedJoin(a.i = b.s)\n" +
			"     ├─ Filter(NOT(a.s IS NULL))\n" +
			"     │   └─ TableAlias(a)\n" +
			"     │       └─ IndexedTableAccess(mytable on [mytable.s] with ranges: [{(<nil>, ∞)}, {(-∞, <nil>)}])\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ IndexedTableAccess(mytable on [mytable.s])\n" +
			"",
	},
	{
		Query: `SELECT /*+ JOIN_ORDER(b, a) */ a.* FROM mytable a inner join mytable b on (a.i = b.s) WHERE a.s is not null`,
		ExpectedPlan: "Project(a.i, a.s)\n" +
			" └─ IndexedJoin(a.i = b.s)\n" +
			"     ├─ TableAlias(b)\n" +
			"     │   └─ Table(mytable)\n" +
			"     └─ Filter(NOT(a.s IS NULL))\n" +
			"         └─ TableAlias(a)\n" +
			"             └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a inner join mytable b on (a.i = b.s) WHERE a.s not in ('1', '2', '3', '4')`,
		ExpectedPlan: "Project(a.i, a.s)\n" +
			" └─ IndexedJoin(a.i = b.s)\n" +
			"     ├─ Filter(NOT((a.s HASH IN (\"1\", \"2\", \"3\", \"4\"))))\n" +
			"     │   └─ TableAlias(a)\n" +
			"     │       └─ IndexedTableAccess(mytable on [mytable.s] with ranges: [{(1, 2)}, {(2, 3)}, {(3, 4)}, {(4, ∞)}, {(-∞, 1)}])\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ IndexedTableAccess(mytable on [mytable.s])\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a inner join mytable b on (a.i = b.s) WHERE a.i in (1, 2, 3, 4)`,
		ExpectedPlan: "Project(a.i, a.s)\n" +
			" └─ IndexedJoin(a.i = b.s)\n" +
			"     ├─ Filter(a.i HASH IN (1, 2, 3, 4))\n" +
			"     │   └─ TableAlias(a)\n" +
			"     │       └─ IndexedTableAccess(mytable on [mytable.i] with ranges: [{[2, 2]}, {[3, 3]}, {[4, 4]}, {[1, 1]}])\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ IndexedTableAccess(mytable on [mytable.s])\n" +
			"",
	},
	{
		Query: `SELECT * FROM mytable WHERE i in (1, 2, 3, 4)`,
		ExpectedPlan: "Filter(mytable.i HASH IN (1, 2, 3, 4))\n" +
			" └─ Projected table access on [i s]\n" +
			"     └─ IndexedTableAccess(mytable on [mytable.i] with ranges: [{[2, 2]}, {[3, 3]}, {[4, 4]}, {[1, 1]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM mytable WHERE i in (CAST(NULL AS SIGNED), 2, 3, 4)`,
		ExpectedPlan: "Filter(mytable.i HASH IN (NULL, 2, 3, 4))\n" +
			" └─ Projected table access on [i s]\n" +
			"     └─ IndexedTableAccess(mytable on [mytable.i] with ranges: [{[2, 2]}, {[3, 3]}, {[4, 4]}, {[<nil>, <nil>]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM mytable WHERE i in (1+2)`,
		ExpectedPlan: "Filter(mytable.i HASH IN (3))\n" +
			" └─ Projected table access on [i s]\n" +
			"     └─ IndexedTableAccess(mytable on [mytable.i] with ranges: [{[3, 3]}])\n" +
			"",
	},
	{
		Query: `SELECT * from mytable where upper(s) IN ('FIRST ROW', 'SECOND ROW')`,
		ExpectedPlan: "Filter(UPPER(mytable.s) HASH IN (\"FIRST ROW\", \"SECOND ROW\"))\n" +
			" └─ Projected table access on [i s]\n" +
			"     └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT * from mytable where cast(i as CHAR) IN ('a', 'b')`,
		ExpectedPlan: "Filter(convert(mytable.i, char) HASH IN (\"a\", \"b\"))\n" +
			" └─ Projected table access on [i s]\n" +
			"     └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT * from mytable where cast(i as CHAR) IN ('1', '2')`,
		ExpectedPlan: "Filter(convert(mytable.i, char) HASH IN (\"1\", \"2\"))\n" +
			" └─ Projected table access on [i s]\n" +
			"     └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT * from mytable where (i > 2) IN (true)`,
		ExpectedPlan: "Filter((mytable.i > 2) HASH IN (true))\n" +
			" └─ Projected table access on [i s]\n" +
			"     └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT * from mytable where (i + 6) IN (7, 8)`,
		ExpectedPlan: "Filter((mytable.i + 6) HASH IN (7, 8))\n" +
			" └─ Projected table access on [i s]\n" +
			"     └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT * from mytable where (i + 40) IN (7, 8)`,
		ExpectedPlan: "Filter((mytable.i + 40) HASH IN (7, 8))\n" +
			" └─ Projected table access on [i s]\n" +
			"     └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT * from mytable where (i = 1 | false) IN (true)`,
		ExpectedPlan: "Filter((mytable.i = 1) HASH IN (true))\n" +
			" └─ Projected table access on [i s]\n" +
			"     └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT * from mytable where (i = 1 & false) IN (true)`,
		ExpectedPlan: "Filter((mytable.i = 0) HASH IN (true))\n" +
			" └─ Projected table access on [i s]\n" +
			"     └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT * FROM mytable WHERE i in (2*i)`,
		ExpectedPlan: "Filter(mytable.i IN ((2 * mytable.i)))\n" +
			" └─ Projected table access on [i s]\n" +
			"     └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT * FROM mytable WHERE i in (i)`,
		ExpectedPlan: "Filter(mytable.i IN (mytable.i))\n" +
			" └─ Projected table access on [i s]\n" +
			"     └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT * from mytable WHERE 4 IN (i + 2)`,
		ExpectedPlan: "Filter(4 IN ((mytable.i + 2)))\n" +
			" └─ Projected table access on [i s]\n" +
			"     └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT * from mytable WHERE s IN (cast('first row' AS CHAR))`,
		ExpectedPlan: "Filter(mytable.s HASH IN (\"first row\"))\n" +
			" └─ Projected table access on [i s]\n" +
			"     └─ IndexedTableAccess(mytable on [mytable.s] with ranges: [{[first row, first row]}])\n" +
			"",
	},
	{
		Query: `SELECT * from mytable WHERE s IN (lower('SECOND ROW'), 'FIRST ROW')`,
		ExpectedPlan: "Filter(mytable.s HASH IN (\"second row\", \"FIRST ROW\"))\n" +
			" └─ Projected table access on [i s]\n" +
			"     └─ IndexedTableAccess(mytable on [mytable.s] with ranges: [{[FIRST ROW, FIRST ROW]}, {[second row, second row]}])\n" +
			"",
	},
	{
		Query: `SELECT * from mytable where true IN (i > 3)`,
		ExpectedPlan: "Filter(true IN ((mytable.i > 3)))\n" +
			" └─ Projected table access on [i s]\n" +
			"     └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a, mytable b where a.i = b.i`,
		ExpectedPlan: "Project(a.i, a.s)\n" +
			" └─ IndexedJoin(a.i = b.i)\n" +
			"     ├─ TableAlias(a)\n" +
			"     │   └─ Table(mytable)\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a, mytable b where a.s = b.i OR a.i = 1`,
		ExpectedPlan: "Project(a.i, a.s)\n" +
			" └─ InnerJoin((a.s = b.i) OR (a.i = 1))\n" +
			"     ├─ Projected table access on [i s]\n" +
			"     │   └─ TableAlias(a)\n" +
			"     │       └─ Table(mytable)\n" +
			"     └─ Projected table access on [i]\n" +
			"         └─ TableAlias(b)\n" +
			"             └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a, mytable b where NOT(a.i = b.s OR a.s = b.i)`,
		ExpectedPlan: "Project(a.i, a.s)\n" +
			" └─ InnerJoin(NOT(((a.i = b.s) OR (a.s = b.i))))\n" +
			"     ├─ Projected table access on [i s]\n" +
			"     │   └─ TableAlias(a)\n" +
			"     │       └─ Table(mytable)\n" +
			"     └─ Projected table access on [s i]\n" +
			"         └─ TableAlias(b)\n" +
			"             └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a, mytable b where a.i = b.s OR a.s = b.i IS FALSE`,
		ExpectedPlan: "Project(a.i, a.s)\n" +
			" └─ InnerJoin((a.i = b.s) OR (a.s = b.i) IS FALSE)\n" +
			"     ├─ Projected table access on [i s]\n" +
			"     │   └─ TableAlias(a)\n" +
			"     │       └─ Table(mytable)\n" +
			"     └─ Projected table access on [s i]\n" +
			"         └─ TableAlias(b)\n" +
			"             └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a, mytable b where a.i >= b.i`,
		ExpectedPlan: "Project(a.i, a.s)\n" +
			" └─ InnerJoin(a.i >= b.i)\n" +
			"     ├─ Projected table access on [i s]\n" +
			"     │   └─ TableAlias(a)\n" +
			"     │       └─ Table(mytable)\n" +
			"     └─ Projected table access on [i]\n" +
			"         └─ TableAlias(b)\n" +
			"             └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a, mytable b where a.i = a.s`,
		ExpectedPlan: "Project(a.i, a.s)\n" +
			" └─ CrossJoin\n" +
			"     ├─ Filter(a.i = a.s)\n" +
			"     │   └─ Projected table access on [i s]\n" +
			"     │       └─ TableAlias(a)\n" +
			"     │           └─ Table(mytable)\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a, mytable b where a.i in (2, 432, 7)`,
		ExpectedPlan: "Project(a.i, a.s)\n" +
			" └─ CrossJoin\n" +
			"     ├─ Filter(a.i HASH IN (2, 432, 7))\n" +
			"     │   └─ Projected table access on [i s]\n" +
			"     │       └─ TableAlias(a)\n" +
			"     │           └─ IndexedTableAccess(mytable on [mytable.i] with ranges: [{[432, 432]}, {[7, 7]}, {[2, 2]}])\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a, mytable b, mytable c, mytable d where a.i = b.i AND b.i = c.i AND c.i = d.i AND c.i = 2`,
		ExpectedPlan: "Project(a.i, a.s)\n" +
			" └─ IndexedJoin(a.i = b.i)\n" +
			"     ├─ TableAlias(a)\n" +
			"     │   └─ Table(mytable)\n" +
			"     └─ IndexedJoin(b.i = c.i)\n" +
			"         ├─ TableAlias(b)\n" +
			"         │   └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"         └─ IndexedJoin(c.i = d.i)\n" +
			"             ├─ Filter(c.i = 2)\n" +
			"             │   └─ TableAlias(c)\n" +
			"             │       └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"             └─ TableAlias(d)\n" +
			"                 └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a, mytable b, mytable c, mytable d where a.i = b.i AND b.i = c.i AND (c.i = d.s OR c.i = 2)`,
		ExpectedPlan: "Project(a.i, a.s)\n" +
			" └─ InnerJoin((c.i = d.s) OR (c.i = 2))\n" +
			"     ├─ InnerJoin(b.i = c.i)\n" +
			"     │   ├─ InnerJoin(a.i = b.i)\n" +
			"     │   │   ├─ Projected table access on [i s]\n" +
			"     │   │   │   └─ TableAlias(a)\n" +
			"     │   │   │       └─ Table(mytable)\n" +
			"     │   │   └─ Projected table access on [i]\n" +
			"     │   │       └─ TableAlias(b)\n" +
			"     │   │           └─ Table(mytable)\n" +
			"     │   └─ Projected table access on [i]\n" +
			"     │       └─ TableAlias(c)\n" +
			"     │           └─ Table(mytable)\n" +
			"     └─ Projected table access on [s]\n" +
			"         └─ TableAlias(d)\n" +
			"             └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a, mytable b, mytable c, mytable d where a.i = b.i AND b.i = c.i`,
		ExpectedPlan: "Project(a.i, a.s)\n" +
			" └─ CrossJoin\n" +
			"     ├─ InnerJoin(b.i = c.i)\n" +
			"     │   ├─ InnerJoin(a.i = b.i)\n" +
			"     │   │   ├─ Projected table access on [i s]\n" +
			"     │   │   │   └─ TableAlias(a)\n" +
			"     │   │   │       └─ Table(mytable)\n" +
			"     │   │   └─ Projected table access on [i]\n" +
			"     │   │       └─ TableAlias(b)\n" +
			"     │   │           └─ Table(mytable)\n" +
			"     │   └─ Projected table access on [i]\n" +
			"     │       └─ TableAlias(c)\n" +
			"     │           └─ Table(mytable)\n" +
			"     └─ TableAlias(d)\n" +
			"         └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a CROSS JOIN mytable b where a.i = b.i`,
		ExpectedPlan: "Project(a.i, a.s)\n" +
			" └─ IndexedJoin(a.i = b.i)\n" +
			"     ├─ TableAlias(a)\n" +
			"     │   └─ Table(mytable)\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a CROSS JOIN mytable b where a.i = b.i OR a.i = b.s`,
		ExpectedPlan: "Project(a.i, a.s)\n" +
			" └─ IndexedJoin((a.i = b.i) OR (a.i = b.s))\n" +
			"     ├─ TableAlias(a)\n" +
			"     │   └─ Table(mytable)\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ Concat\n" +
			"             ├─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"             └─ IndexedTableAccess(mytable on [mytable.s])\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a CROSS JOIN mytable b where NOT(a.i = b.s OR a.s = b.i)`,
		ExpectedPlan: "Project(a.i, a.s)\n" +
			" └─ InnerJoin(NOT(((a.i = b.s) OR (a.s = b.i))))\n" +
			"     ├─ Projected table access on [i s]\n" +
			"     │   └─ TableAlias(a)\n" +
			"     │       └─ Table(mytable)\n" +
			"     └─ Projected table access on [s i]\n" +
			"         └─ TableAlias(b)\n" +
			"             └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a CROSS JOIN mytable b where a.i = b.s OR a.s = b.i IS FALSE`,
		ExpectedPlan: "Project(a.i, a.s)\n" +
			" └─ InnerJoin((a.i = b.s) OR (a.s = b.i) IS FALSE)\n" +
			"     ├─ Projected table access on [i s]\n" +
			"     │   └─ TableAlias(a)\n" +
			"     │       └─ Table(mytable)\n" +
			"     └─ Projected table access on [s i]\n" +
			"         └─ TableAlias(b)\n" +
			"             └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a CROSS JOIN mytable b where a.i >= b.i`,
		ExpectedPlan: "Project(a.i, a.s)\n" +
			" └─ InnerJoin(a.i >= b.i)\n" +
			"     ├─ Projected table access on [i s]\n" +
			"     │   └─ TableAlias(a)\n" +
			"     │       └─ Table(mytable)\n" +
			"     └─ Projected table access on [i]\n" +
			"         └─ TableAlias(b)\n" +
			"             └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a CROSS JOIN mytable b where a.i = a.i`,
		ExpectedPlan: "Project(a.i, a.s)\n" +
			" └─ CrossJoin\n" +
			"     ├─ Filter(a.i = a.i)\n" +
			"     │   └─ Projected table access on [i s]\n" +
			"     │       └─ TableAlias(a)\n" +
			"     │           └─ Table(mytable)\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a CROSS JOIN mytable b CROSS JOIN mytable c CROSS JOIN mytable d where a.i = b.i AND b.i = c.i AND c.i = d.i AND c.i = 2`,
		ExpectedPlan: "Project(a.i, a.s)\n" +
			" └─ IndexedJoin(a.i = b.i)\n" +
			"     ├─ TableAlias(a)\n" +
			"     │   └─ Table(mytable)\n" +
			"     └─ IndexedJoin(b.i = c.i)\n" +
			"         ├─ TableAlias(b)\n" +
			"         │   └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"         └─ IndexedJoin(c.i = d.i)\n" +
			"             ├─ Filter(c.i = 2)\n" +
			"             │   └─ TableAlias(c)\n" +
			"             │       └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"             └─ TableAlias(d)\n" +
			"                 └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a CROSS JOIN mytable b CROSS JOIN mytable c CROSS JOIN mytable d where a.i = b.i AND b.i = c.i AND (c.i = d.s OR c.i = 2)`,
		ExpectedPlan: "Project(a.i, a.s)\n" +
			" └─ InnerJoin((c.i = d.s) OR (c.i = 2))\n" +
			"     ├─ InnerJoin(b.i = c.i)\n" +
			"     │   ├─ InnerJoin(a.i = b.i)\n" +
			"     │   │   ├─ Projected table access on [i s]\n" +
			"     │   │   │   └─ TableAlias(a)\n" +
			"     │   │   │       └─ Table(mytable)\n" +
			"     │   │   └─ Projected table access on [i]\n" +
			"     │   │       └─ TableAlias(b)\n" +
			"     │   │           └─ Table(mytable)\n" +
			"     │   └─ Projected table access on [i]\n" +
			"     │       └─ TableAlias(c)\n" +
			"     │           └─ Table(mytable)\n" +
			"     └─ Projected table access on [s]\n" +
			"         └─ TableAlias(d)\n" +
			"             └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a CROSS JOIN mytable b CROSS JOIN mytable c CROSS JOIN mytable d where a.i = b.i AND b.s = c.s`,
		ExpectedPlan: "Project(a.i, a.s)\n" +
			" └─ CrossJoin\n" +
			"     ├─ InnerJoin(b.s = c.s)\n" +
			"     │   ├─ InnerJoin(a.i = b.i)\n" +
			"     │   │   ├─ Projected table access on [i s]\n" +
			"     │   │   │   └─ TableAlias(a)\n" +
			"     │   │   │       └─ Table(mytable)\n" +
			"     │   │   └─ Projected table access on [s i]\n" +
			"     │   │       └─ TableAlias(b)\n" +
			"     │   │           └─ Table(mytable)\n" +
			"     │   └─ Projected table access on [s]\n" +
			"     │       └─ TableAlias(c)\n" +
			"     │           └─ Table(mytable)\n" +
			"     └─ TableAlias(d)\n" +
			"         └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM mytable a inner join mytable b on (a.i = b.s) WHERE a.i BETWEEN 10 AND 20`,
		ExpectedPlan: "Project(a.i, a.s)\n" +
			" └─ IndexedJoin(a.i = b.s)\n" +
			"     ├─ Filter(a.i BETWEEN 10 AND 20)\n" +
			"     │   └─ TableAlias(a)\n" +
			"     │       └─ IndexedTableAccess(mytable on [mytable.i] with ranges: [{[10, 20]}])\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ IndexedTableAccess(mytable on [mytable.s])\n" +
			"",
	},
	{
		Query: `SELECT lefttable.i, righttable.s
			FROM (SELECT * FROM mytable) lefttable
			JOIN (SELECT * FROM mytable) righttable
			ON lefttable.i = righttable.i AND righttable.s = lefttable.s
			ORDER BY lefttable.i ASC`,
		ExpectedPlan: "Sort(lefttable.i ASC)\n" +
			" └─ Project(lefttable.i, righttable.s)\n" +
			"     └─ InnerJoin((lefttable.i = righttable.i) AND (righttable.s = lefttable.s))\n" +
			"         ├─ SubqueryAlias(lefttable)\n" +
			"         │   └─ Projected table access on [i s]\n" +
			"         │       └─ Table(mytable)\n" +
			"         └─ HashLookup(child: (righttable.i, righttable.s), lookup: (lefttable.i, lefttable.s))\n" +
			"             └─ CachedResults\n" +
			"                 └─ SubqueryAlias(righttable)\n" +
			"                     └─ Projected table access on [i s]\n" +
			"                         └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT s2, i2, i FROM mytable RIGHT JOIN (SELECT * FROM othertable) othertable ON i2 = i`,
		ExpectedPlan: "Project(othertable.s2, othertable.i2, mytable.i)\n" +
			" └─ RightIndexedJoin(othertable.i2 = mytable.i)\n" +
			"     ├─ SubqueryAlias(othertable)\n" +
			"     │   └─ Projected table access on [s2 i2]\n" +
			"     │       └─ Table(othertable)\n" +
			"     └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `SELECT s2, i2, i FROM mytable INNER JOIN (SELECT * FROM othertable) othertable ON i2 = i`,
		ExpectedPlan: "Project(othertable.s2, othertable.i2, mytable.i)\n" +
			" └─ IndexedJoin(othertable.i2 = mytable.i)\n" +
			"     ├─ SubqueryAlias(othertable)\n" +
			"     │   └─ Projected table access on [s2 i2]\n" +
			"     │       └─ Table(othertable)\n" +
			"     └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `SELECT * FROM (SELECT * FROM othertable) othertable_alias WHERE s2 = 'a'`,
		ExpectedPlan: "SubqueryAlias(othertable_alias)\n" +
			" └─ Filter(othertable.s2 = \"a\")\n" +
			"     └─ Projected table access on [s2 i2]\n" +
			"         └─ IndexedTableAccess(othertable on [othertable.s2] with ranges: [{[a, a]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM othertable) othertable_one) othertable_two) othertable_three WHERE s2 = 'a'`,
		ExpectedPlan: "SubqueryAlias(othertable_three)\n" +
			" └─ SubqueryAlias(othertable_two)\n" +
			"     └─ SubqueryAlias(othertable_one)\n" +
			"         └─ Filter(othertable.s2 = \"a\")\n" +
			"             └─ Projected table access on [s2 i2]\n" +
			"                 └─ IndexedTableAccess(othertable on [othertable.s2] with ranges: [{[a, a]}])\n" +
			"",
	},
	{
		Query: `SELECT othertable.s2, othertable.i2, mytable.i FROM mytable INNER JOIN (SELECT * FROM othertable) othertable ON othertable.i2 = mytable.i WHERE othertable.s2 > 'a'`,
		ExpectedPlan: "Project(othertable.s2, othertable.i2, mytable.i)\n" +
			" └─ IndexedJoin(othertable.i2 = mytable.i)\n" +
			"     ├─ SubqueryAlias(othertable)\n" +
			"     │   └─ Filter(othertable.s2 > \"a\")\n" +
			"     │       └─ Projected table access on [s2 i2]\n" +
			"     │           └─ IndexedTableAccess(othertable on [othertable.s2] with ranges: [{(a, ∞)}])\n" +
			"     └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `SELECT mytable.i, mytable.s FROM mytable WHERE mytable.i = (SELECT i2 FROM othertable LIMIT 1)`,
		ExpectedPlan: "IndexedInSubqueryFilter(mytable.i IN ((Limit(1)\n" +
			" └─ Project(othertable.i2)\n" +
			"     └─ Projected table access on [i2]\n" +
			"         └─ Table(othertable)\n" +
			")))\n" +
			" └─ Projected table access on [i s]\n" +
			"     └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `SELECT mytable.i, mytable.s FROM mytable WHERE mytable.i IN (SELECT i2 FROM othertable)`,
		ExpectedPlan: "IndexedInSubqueryFilter(mytable.i IN ((Project(othertable.i2)\n" +
			" └─ Projected table access on [i2]\n" +
			"     └─ Table(othertable)\n" +
			")))\n" +
			" └─ Projected table access on [i s]\n" +
			"     └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `SELECT mytable.i, mytable.s FROM mytable WHERE mytable.i IN (SELECT i2 FROM othertable WHERE mytable.i = othertable.i2)`,
		ExpectedPlan: "Filter(mytable.i IN (Project(othertable.i2)\n" +
			" └─ Filter(mytable.i = othertable.i2)\n" +
			"     └─ Projected table access on [i2]\n" +
			"         └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"))\n" +
			" └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT * FROM mytable mt INNER JOIN othertable ot ON mt.i = ot.i2 AND mt.i > 2`,
		ExpectedPlan: "IndexedJoin(mt.i = ot.i2)\n" +
			" ├─ Filter(mt.i > 2)\n" +
			" │   └─ TableAlias(mt)\n" +
			" │       └─ IndexedTableAccess(mytable on [mytable.i] with ranges: [{(2, ∞)}])\n" +
			" └─ TableAlias(ot)\n" +
			"     └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"",
	},
	{
		Query: `SELECT /*+ JOIN_ORDER(mt, o) */ * FROM mytable mt INNER JOIN one_pk o ON mt.i = o.pk AND mt.s = o.c2`,
		ExpectedPlan: "IndexedJoin((mt.i = o.pk) AND (mt.s = o.c2))\n" +
			" ├─ TableAlias(mt)\n" +
			" │   └─ Table(mytable)\n" +
			" └─ TableAlias(o)\n" +
			"     └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"",
	},
	{
		Query: `SELECT i, i2, s2 FROM mytable RIGHT JOIN othertable ON i = i2 - 1`,
		ExpectedPlan: "Project(mytable.i, othertable.i2, othertable.s2)\n" +
			" └─ RightIndexedJoin(mytable.i = (othertable.i2 - 1))\n" +
			"     ├─ Table(othertable)\n" +
			"     └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"",
	},
	{
		Query: `SELECT * FROM tabletest, mytable mt INNER JOIN othertable ot ON mt.i = ot.i2`,
		ExpectedPlan: "CrossJoin\n" +
			" ├─ Table(tabletest)\n" +
			" └─ IndexedJoin(mt.i = ot.i2)\n" +
			"     ├─ TableAlias(mt)\n" +
			"     │   └─ Table(mytable)\n" +
			"     └─ TableAlias(ot)\n" +
			"         └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"",
	},
	{
		Query: `SELECT t1.timestamp FROM reservedWordsTable t1 JOIN reservedWordsTable t2 ON t1.TIMESTAMP = t2.tImEstamp`,
		ExpectedPlan: "Project(t1.Timestamp)\n" +
			" └─ IndexedJoin(t1.Timestamp = t2.Timestamp)\n" +
			"     ├─ TableAlias(t1)\n" +
			"     │   └─ Table(reservedWordsTable)\n" +
			"     └─ TableAlias(t2)\n" +
			"         └─ IndexedTableAccess(reservedWordsTable on [reservedWordsTable.Timestamp])\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2`,
		ExpectedPlan: "Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			" └─ IndexedJoin((one_pk.pk = two_pk.pk1) AND (one_pk.pk = two_pk.pk2))\n" +
			"     ├─ Table(one_pk)\n" +
			"     └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2 OR one_pk.c2 = two_pk.c3`,
		ExpectedPlan: "Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			" └─ InnerJoin(((one_pk.pk = two_pk.pk1) AND (one_pk.pk = two_pk.pk2)) OR (one_pk.c2 = two_pk.c3))\n" +
			"     ├─ Projected table access on [pk c2]\n" +
			"     │   └─ Table(one_pk)\n" +
			"     └─ Projected table access on [pk1 pk2 c3]\n" +
			"         └─ Table(two_pk)\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON opk.pk=tpk.pk1 AND opk.pk=tpk.pk2`,
		ExpectedPlan: "Project(opk.pk, tpk.pk1, tpk.pk2)\n" +
			" └─ IndexedJoin((opk.pk = tpk.pk1) AND (opk.pk = tpk.pk2))\n" +
			"     ├─ TableAlias(opk)\n" +
			"     │   └─ Table(one_pk)\n" +
			"     └─ TableAlias(tpk)\n" +
			"         └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2`,
		ExpectedPlan: "Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			" └─ IndexedJoin((one_pk.pk = two_pk.pk1) AND (one_pk.pk = two_pk.pk2))\n" +
			"     ├─ Table(one_pk)\n" +
			"     └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON one_pk.pk <=> two_pk.pk1 AND one_pk.pk = two_pk.pk2`,
		ExpectedPlan: "Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			" └─ LeftIndexedJoin((one_pk.pk <=> two_pk.pk1) AND (one_pk.pk = two_pk.pk2))\n" +
			"     ├─ Table(one_pk)\n" +
			"     └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON one_pk.pk = two_pk.pk1 AND one_pk.pk <=> two_pk.pk2`,
		ExpectedPlan: "Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			" └─ LeftIndexedJoin((one_pk.pk = two_pk.pk1) AND (one_pk.pk <=> two_pk.pk2))\n" +
			"     ├─ Table(one_pk)\n" +
			"     └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON one_pk.pk <=> two_pk.pk1 AND one_pk.pk <=> two_pk.pk2`,
		ExpectedPlan: "Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			" └─ LeftIndexedJoin((one_pk.pk <=> two_pk.pk1) AND (one_pk.pk <=> two_pk.pk2))\n" +
			"     ├─ Table(one_pk)\n" +
			"     └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk RIGHT JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2`,
		ExpectedPlan: "Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			" └─ RightIndexedJoin((one_pk.pk = two_pk.pk1) AND (one_pk.pk = two_pk.pk2))\n" +
			"     ├─ Table(two_pk)\n" +
			"     └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"",
	},
	{
		Query: `SELECT * FROM (SELECT * FROM othertable) othertable_alias WHERE othertable_alias.i2 = 1`,
		ExpectedPlan: "SubqueryAlias(othertable_alias)\n" +
			" └─ Filter(othertable.i2 = 1)\n" +
			"     └─ Projected table access on [s2 i2]\n" +
			"         └─ IndexedTableAccess(othertable on [othertable.i2] with ranges: [{[1, 1]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM (SELECT * FROM othertable WHERE i2 = 1) othertable_alias WHERE othertable_alias.i2 = 1`,
		ExpectedPlan: "SubqueryAlias(othertable_alias)\n" +
			" └─ Filter(othertable.i2 = 1)\n" +
			"     └─ Filter(othertable.i2 = 1)\n" +
			"         └─ Projected table access on [i2 s2]\n" +
			"             └─ IndexedTableAccess(othertable on [othertable.i2] with ranges: [{[1, 1]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table ORDER BY date_col ASC`,
		ExpectedPlan: "Sort(datetime_table.date_col ASC)\n" +
			" └─ Projected table access on [i date_col datetime_col timestamp_col time_col]\n" +
			"     └─ Table(datetime_table)\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table ORDER BY date_col ASC LIMIT 100`,
		ExpectedPlan: "Limit(100)\n" +
			" └─ TopN(Limit: [100]; datetime_table.date_col ASC)\n" +
			"     └─ Projected table access on [i date_col datetime_col timestamp_col time_col]\n" +
			"         └─ Table(datetime_table)\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table ORDER BY date_col ASC LIMIT 100 OFFSET 100`,
		ExpectedPlan: "Limit(100)\n" +
			" └─ Offset(100)\n" +
			"     └─ TopN(Limit: [(100 + 100)]; datetime_table.date_col ASC)\n" +
			"         └─ Projected table access on [i date_col datetime_col timestamp_col time_col]\n" +
			"             └─ Table(datetime_table)\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table where date_col = '2020-01-01'`,
		ExpectedPlan: "Filter(datetime_table.date_col = \"2020-01-01\")\n" +
			" └─ Projected table access on [i date_col datetime_col timestamp_col time_col]\n" +
			"     └─ IndexedTableAccess(datetime_table on [datetime_table.date_col] with ranges: [{[2020-01-01, 2020-01-01]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table where date_col > '2020-01-01'`,
		ExpectedPlan: "Filter(datetime_table.date_col > \"2020-01-01\")\n" +
			" └─ Projected table access on [i date_col datetime_col timestamp_col time_col]\n" +
			"     └─ IndexedTableAccess(datetime_table on [datetime_table.date_col] with ranges: [{(2020-01-01, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table where datetime_col = '2020-01-01'`,
		ExpectedPlan: "Filter(datetime_table.datetime_col = \"2020-01-01\")\n" +
			" └─ Projected table access on [i date_col datetime_col timestamp_col time_col]\n" +
			"     └─ IndexedTableAccess(datetime_table on [datetime_table.datetime_col] with ranges: [{[2020-01-01, 2020-01-01]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table where datetime_col > '2020-01-01'`,
		ExpectedPlan: "Filter(datetime_table.datetime_col > \"2020-01-01\")\n" +
			" └─ Projected table access on [i date_col datetime_col timestamp_col time_col]\n" +
			"     └─ IndexedTableAccess(datetime_table on [datetime_table.datetime_col] with ranges: [{(2020-01-01, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table where timestamp_col = '2020-01-01'`,
		ExpectedPlan: "Filter(datetime_table.timestamp_col = \"2020-01-01\")\n" +
			" └─ Projected table access on [i date_col datetime_col timestamp_col time_col]\n" +
			"     └─ IndexedTableAccess(datetime_table on [datetime_table.timestamp_col] with ranges: [{[2020-01-01, 2020-01-01]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table where timestamp_col > '2020-01-01'`,
		ExpectedPlan: "Filter(datetime_table.timestamp_col > \"2020-01-01\")\n" +
			" └─ Projected table access on [i date_col datetime_col timestamp_col time_col]\n" +
			"     └─ IndexedTableAccess(datetime_table on [datetime_table.timestamp_col] with ranges: [{(2020-01-01, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table dt1 join datetime_table dt2 on dt1.timestamp_col = dt2.timestamp_col`,
		ExpectedPlan: "IndexedJoin(dt1.timestamp_col = dt2.timestamp_col)\n" +
			" ├─ TableAlias(dt1)\n" +
			" │   └─ Table(datetime_table)\n" +
			" └─ TableAlias(dt2)\n" +
			"     └─ IndexedTableAccess(datetime_table on [datetime_table.timestamp_col])\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table dt1 join datetime_table dt2 on dt1.date_col = dt2.timestamp_col`,
		ExpectedPlan: "IndexedJoin(dt1.date_col = dt2.timestamp_col)\n" +
			" ├─ TableAlias(dt1)\n" +
			" │   └─ Table(datetime_table)\n" +
			" └─ TableAlias(dt2)\n" +
			"     └─ IndexedTableAccess(datetime_table on [datetime_table.timestamp_col])\n" +
			"",
	},
	{
		Query: `SELECT * FROM datetime_table dt1 join datetime_table dt2 on dt1.datetime_col = dt2.timestamp_col`,
		ExpectedPlan: "IndexedJoin(dt1.datetime_col = dt2.timestamp_col)\n" +
			" ├─ TableAlias(dt1)\n" +
			" │   └─ Table(datetime_table)\n" +
			" └─ TableAlias(dt2)\n" +
			"     └─ IndexedTableAccess(datetime_table on [datetime_table.timestamp_col])\n" +
			"",
	},
	{
		Query: `SELECT dt1.i FROM datetime_table dt1
			join datetime_table dt2 on dt1.date_col = date(date_sub(dt2.timestamp_col, interval 2 day))
			order by 1`,
		ExpectedPlan: "Sort(dt1.i ASC)\n" +
			" └─ Project(dt1.i)\n" +
			"     └─ IndexedJoin(dt1.date_col = DATE(DATE_SUB(dt2.timestamp_col, INTERVAL 2 DAY)))\n" +
			"         ├─ TableAlias(dt2)\n" +
			"         │   └─ Table(datetime_table)\n" +
			"         └─ TableAlias(dt1)\n" +
			"             └─ IndexedTableAccess(datetime_table on [datetime_table.date_col])\n" +
			"",
	},
	{
		Query: `SELECT dt1.i FROM datetime_table dt1
			join datetime_table dt2 on dt1.date_col = date(date_sub(dt2.timestamp_col, interval 2 day))
			order by 1 limit 3 offset 0`,
		ExpectedPlan: "Limit(3)\n" +
			" └─ Offset(0)\n" +
			"     └─ TopN(Limit: [(3 + 0)]; dt1.i ASC)\n" +
			"         └─ Project(dt1.i)\n" +
			"             └─ IndexedJoin(dt1.date_col = DATE(DATE_SUB(dt2.timestamp_col, INTERVAL 2 DAY)))\n" +
			"                 ├─ TableAlias(dt2)\n" +
			"                 │   └─ Table(datetime_table)\n" +
			"                 └─ TableAlias(dt1)\n" +
			"                     └─ IndexedTableAccess(datetime_table on [datetime_table.date_col])\n" +
			"",
	},
	{
		Query: `SELECT dt1.i FROM datetime_table dt1
			join datetime_table dt2 on dt1.date_col = date(date_sub(dt2.timestamp_col, interval 2 day))
			order by 1 limit 3`,
		ExpectedPlan: "Limit(3)\n" +
			" └─ TopN(Limit: [3]; dt1.i ASC)\n" +
			"     └─ Project(dt1.i)\n" +
			"         └─ IndexedJoin(dt1.date_col = DATE(DATE_SUB(dt2.timestamp_col, INTERVAL 2 DAY)))\n" +
			"             ├─ TableAlias(dt2)\n" +
			"             │   └─ Table(datetime_table)\n" +
			"             └─ TableAlias(dt1)\n" +
			"                 └─ IndexedTableAccess(datetime_table on [datetime_table.date_col])\n" +
			"",
	},
	{
		Query: `SELECT pk FROM one_pk
						JOIN two_pk tpk ON one_pk.pk=tpk.pk1 AND one_pk.pk=tpk.pk2
						JOIN two_pk tpk2 ON tpk2.pk1=TPK.pk2 AND TPK2.pk2=tpk.pk1`,
		ExpectedPlan: "Project(one_pk.pk)\n" +
			" └─ IndexedJoin((one_pk.pk = tpk.pk1) AND (one_pk.pk = tpk.pk2))\n" +
			"     ├─ Table(one_pk)\n" +
			"     └─ IndexedJoin((tpk2.pk1 = tpk.pk2) AND (tpk2.pk2 = tpk.pk1))\n" +
			"         ├─ TableAlias(tpk)\n" +
			"         │   └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"         └─ TableAlias(tpk2)\n" +
			"             └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT /* JOIN_ORDER(tpk, one_pk, tpk2) */
						pk FROM one_pk
						JOIN two_pk tpk ON one_pk.pk=tpk.pk1 AND one_pk.pk=tpk.pk2
						JOIN two_pk tpk2 ON tpk2.pk1=TPK.pk2 AND TPK2.pk2=tpk.pk1`,
		ExpectedPlan: "Project(one_pk.pk)\n" +
			" └─ IndexedJoin((tpk2.pk1 = tpk.pk2) AND (tpk2.pk2 = tpk.pk1))\n" +
			"     ├─ IndexedJoin((one_pk.pk = tpk.pk1) AND (one_pk.pk = tpk.pk2))\n" +
			"     │   ├─ TableAlias(tpk)\n" +
			"     │   │   └─ Table(two_pk)\n" +
			"     │   └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"     └─ TableAlias(tpk2)\n" +
			"         └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT /* JOIN_ORDER(tpk, one_pk, tpk2) */
						pk FROM one_pk
						JOIN two_pk tpk ON one_pk.pk=tpk.pk1 AND one_pk.pk=tpk.pk2
						LEFT JOIN two_pk tpk2 ON tpk2.pk1=TPK.pk2 AND TPK2.pk2=tpk.pk1`,
		ExpectedPlan: "Project(one_pk.pk)\n" +
			" └─ LeftIndexedJoin((tpk2.pk1 = tpk.pk2) AND (tpk2.pk2 = tpk.pk1))\n" +
			"     ├─ IndexedJoin((one_pk.pk = tpk.pk1) AND (one_pk.pk = tpk.pk2))\n" +
			"     │   ├─ TableAlias(tpk)\n" +
			"     │   │   └─ Table(two_pk)\n" +
			"     │   └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"     └─ TableAlias(tpk2)\n" +
			"         └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT pk,tpk.pk1,tpk2.pk1,tpk.pk2,tpk2.pk2 FROM one_pk 
						JOIN two_pk tpk ON pk=tpk.pk1 AND pk-1=tpk.pk2 
						JOIN two_pk tpk2 ON pk-1=TPK2.pk1 AND pk=tpk2.pk2
						ORDER BY 1`,
		ExpectedPlan: "Sort(one_pk.pk ASC)\n" +
			" └─ Project(one_pk.pk, tpk.pk1, tpk2.pk1, tpk.pk2, tpk2.pk2)\n" +
			"     └─ IndexedJoin(((one_pk.pk - 1) = tpk2.pk1) AND (one_pk.pk = tpk2.pk2))\n" +
			"         ├─ IndexedJoin((one_pk.pk = tpk.pk1) AND ((one_pk.pk - 1) = tpk.pk2))\n" +
			"         │   ├─ Table(one_pk)\n" +
			"         │   └─ TableAlias(tpk)\n" +
			"         │       └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"         └─ TableAlias(tpk2)\n" +
			"             └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT pk FROM one_pk
						LEFT JOIN two_pk tpk ON one_pk.pk=tpk.pk1 AND one_pk.pk=tpk.pk2
						LEFT JOIN two_pk tpk2 ON tpk2.pk1=TPK.pk2 AND TPK2.pk2=tpk.pk1`,
		ExpectedPlan: "Project(one_pk.pk)\n" +
			" └─ LeftIndexedJoin((tpk2.pk1 = tpk.pk2) AND (tpk2.pk2 = tpk.pk1))\n" +
			"     ├─ LeftIndexedJoin((one_pk.pk = tpk.pk1) AND (one_pk.pk = tpk.pk2))\n" +
			"     │   ├─ Table(one_pk)\n" +
			"     │   └─ TableAlias(tpk)\n" +
			"     │       └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"     └─ TableAlias(tpk2)\n" +
			"         └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT pk FROM one_pk
						LEFT JOIN two_pk tpk ON one_pk.pk=tpk.pk1 AND one_pk.pk=tpk.pk2
						JOIN two_pk tpk2 ON tpk2.pk1=TPK.pk2 AND TPK2.pk2=tpk.pk1`,
		ExpectedPlan: "Project(one_pk.pk)\n" +
			" └─ IndexedJoin((tpk2.pk1 = tpk.pk2) AND (tpk2.pk2 = tpk.pk1))\n" +
			"     ├─ LeftIndexedJoin((one_pk.pk = tpk.pk1) AND (one_pk.pk = tpk.pk2))\n" +
			"     │   ├─ Table(one_pk)\n" +
			"     │   └─ TableAlias(tpk)\n" +
			"     │       └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"     └─ TableAlias(tpk2)\n" +
			"         └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT pk FROM one_pk
						JOIN two_pk tpk ON one_pk.pk=tpk.pk1 AND one_pk.pk=tpk.pk2
						LEFT JOIN two_pk tpk2 ON tpk2.pk1=TPK.pk2 AND TPK2.pk2=tpk.pk1`,
		ExpectedPlan: "Project(one_pk.pk)\n" +
			" └─ LeftIndexedJoin((tpk2.pk1 = tpk.pk2) AND (tpk2.pk2 = tpk.pk1))\n" +
			"     ├─ IndexedJoin((one_pk.pk = tpk.pk1) AND (one_pk.pk = tpk.pk2))\n" +
			"     │   ├─ Table(one_pk)\n" +
			"     │   └─ TableAlias(tpk)\n" +
			"     │       └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"     └─ TableAlias(tpk2)\n" +
			"         └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT pk FROM one_pk 
						RIGHT JOIN two_pk tpk ON one_pk.pk=tpk.pk1 AND one_pk.pk=tpk.pk2
						RIGHT JOIN two_pk tpk2 ON tpk.pk1=TPk2.pk2 AND tpk.pk2=TPK2.pk1`,
		ExpectedPlan: "Project(one_pk.pk)\n" +
			" └─ RightIndexedJoin((tpk.pk1 = tpk2.pk2) AND (tpk.pk2 = tpk2.pk1))\n" +
			"     ├─ TableAlias(tpk2)\n" +
			"     │   └─ Table(two_pk)\n" +
			"     └─ RightIndexedJoin((one_pk.pk = tpk.pk1) AND (one_pk.pk = tpk.pk2))\n" +
			"         ├─ TableAlias(tpk)\n" +
			"         │   └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"         └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"",
	},
	{
		Query: `SELECT i,pk1,pk2 FROM mytable JOIN two_pk ON i-1=pk1 AND i-2=pk2`,
		ExpectedPlan: "Project(mytable.i, two_pk.pk1, two_pk.pk2)\n" +
			" └─ IndexedJoin(((mytable.i - 1) = two_pk.pk1) AND ((mytable.i - 2) = two_pk.pk2))\n" +
			"     ├─ Table(mytable)\n" +
			"     └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON pk=pk1`,
		ExpectedPlan: "Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			" └─ LeftIndexedJoin(one_pk.pk = two_pk.pk1)\n" +
			"     ├─ Table(one_pk)\n" +
			"     └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i`,
		ExpectedPlan: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ LeftIndexedJoin(one_pk.pk = niltable.i)\n" +
			"     ├─ Table(one_pk)\n" +
			"     └─ IndexedTableAccess(niltable on [niltable.i])\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i`,
		ExpectedPlan: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ RightIndexedJoin(one_pk.pk = niltable.i)\n" +
			"     ├─ Table(niltable)\n" +
			"     └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"",
	},
	{
		Query: `SELECT pk,nt.i,nt2.i FROM one_pk 
						RIGHT JOIN niltable nt ON pk=nt.i
						RIGHT JOIN niltable nt2 ON pk=nt2.i + 1`,
		ExpectedPlan: "Project(one_pk.pk, nt.i, nt2.i)\n" +
			" └─ RightIndexedJoin(one_pk.pk = (nt2.i + 1))\n" +
			"     ├─ TableAlias(nt2)\n" +
			"     │   └─ Table(niltable)\n" +
			"     └─ RightIndexedJoin(one_pk.pk = nt.i)\n" +
			"         ├─ TableAlias(nt)\n" +
			"         │   └─ Table(niltable)\n" +
			"         └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i AND f IS NOT NULL`,
		ExpectedPlan: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ LeftIndexedJoin((one_pk.pk = niltable.i) AND (NOT(niltable.f IS NULL)))\n" +
			"     ├─ Table(one_pk)\n" +
			"     └─ IndexedTableAccess(niltable on [niltable.i])\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i and pk > 0`,
		ExpectedPlan: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ RightJoin((one_pk.pk = niltable.i) AND (one_pk.pk > 0))\n" +
			"     ├─ Projected table access on [pk]\n" +
			"     │   └─ Table(one_pk)\n" +
			"     └─ Projected table access on [i f]\n" +
			"         └─ Table(niltable)\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE f IS NOT NULL`,
		ExpectedPlan: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ Filter(NOT(niltable.f IS NULL))\n" +
			"     └─ LeftIndexedJoin(one_pk.pk = niltable.i)\n" +
			"         ├─ Table(one_pk)\n" +
			"         └─ IndexedTableAccess(niltable on [niltable.i])\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE i2 > 1`,
		ExpectedPlan: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ Filter(niltable.i2 > 1)\n" +
			"     └─ LeftIndexedJoin(one_pk.pk = niltable.i)\n" +
			"         ├─ Table(one_pk)\n" +
			"         └─ IndexedTableAccess(niltable on [niltable.i])\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE i > 1`,
		ExpectedPlan: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ Filter(niltable.i > 1)\n" +
			"     └─ LeftIndexedJoin(one_pk.pk = niltable.i)\n" +
			"         ├─ Table(one_pk)\n" +
			"         └─ IndexedTableAccess(niltable on [niltable.i])\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE c1 > 10`,
		ExpectedPlan: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ LeftIndexedJoin(one_pk.pk = niltable.i)\n" +
			"     ├─ Filter(one_pk.c1 > 10)\n" +
			"     │   └─ Table(one_pk)\n" +
			"     └─ IndexedTableAccess(niltable on [niltable.i])\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i WHERE f IS NOT NULL`,
		ExpectedPlan: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ RightIndexedJoin(one_pk.pk = niltable.i)\n" +
			"     ├─ Filter(NOT(niltable.f IS NULL))\n" +
			"     │   └─ Table(niltable)\n" +
			"     └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE pk > 1`,
		ExpectedPlan: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ LeftIndexedJoin(one_pk.pk = niltable.i)\n" +
			"     ├─ Filter(one_pk.pk > 1)\n" +
			"     │   └─ IndexedTableAccess(one_pk on [one_pk.pk] with ranges: [{(1, ∞)}])\n" +
			"     └─ IndexedTableAccess(niltable on [niltable.i])\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i WHERE pk > 0`,
		ExpectedPlan: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ Filter(one_pk.pk > 0)\n" +
			"     └─ RightIndexedJoin(one_pk.pk = niltable.i)\n" +
			"         ├─ Table(niltable)\n" +
			"         └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON pk=pk1`,
		ExpectedPlan: "Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			" └─ IndexedJoin(one_pk.pk = two_pk.pk1)\n" +
			"     ├─ Table(one_pk)\n" +
			"     └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT /*+ JOIN_ORDER(two_pk, one_pk) */ pk,pk1,pk2 FROM one_pk JOIN two_pk ON pk=pk1`,
		ExpectedPlan: "Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			" └─ IndexedJoin(one_pk.pk = two_pk.pk1)\n" +
			"     ├─ Table(two_pk)\n" +
			"     └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"",
	},
	{
		Query: `SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a JOIN two_pk b ON a.pk1=b.pk1 AND a.pk2=b.pk2 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(a.pk1 ASC, a.pk2 ASC, b.pk1 ASC)\n" +
			" └─ Project(a.pk1, a.pk2, b.pk1, b.pk2)\n" +
			"     └─ IndexedJoin((a.pk1 = b.pk1) AND (a.pk2 = b.pk2))\n" +
			"         ├─ TableAlias(a)\n" +
			"         │   └─ Table(two_pk)\n" +
			"         └─ TableAlias(b)\n" +
			"             └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a JOIN two_pk b ON a.pk1=b.pk2 AND a.pk2=b.pk1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(a.pk1 ASC, a.pk2 ASC, b.pk1 ASC)\n" +
			" └─ Project(a.pk1, a.pk2, b.pk1, b.pk2)\n" +
			"     └─ IndexedJoin((a.pk1 = b.pk2) AND (a.pk2 = b.pk1))\n" +
			"         ├─ TableAlias(a)\n" +
			"         │   └─ Table(two_pk)\n" +
			"         └─ TableAlias(b)\n" +
			"             └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a JOIN two_pk b ON b.pk1=a.pk1 AND a.pk2=b.pk2 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(a.pk1 ASC, a.pk2 ASC, b.pk1 ASC)\n" +
			" └─ Project(a.pk1, a.pk2, b.pk1, b.pk2)\n" +
			"     └─ IndexedJoin((b.pk1 = a.pk1) AND (a.pk2 = b.pk2))\n" +
			"         ├─ TableAlias(a)\n" +
			"         │   └─ Table(two_pk)\n" +
			"         └─ TableAlias(b)\n" +
			"             └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a JOIN two_pk b ON a.pk1+1=b.pk1 AND a.pk2+1=b.pk2 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(a.pk1 ASC, a.pk2 ASC, b.pk1 ASC)\n" +
			" └─ Project(a.pk1, a.pk2, b.pk1, b.pk2)\n" +
			"     └─ IndexedJoin(((a.pk1 + 1) = b.pk1) AND ((a.pk2 + 1) = b.pk2))\n" +
			"         ├─ TableAlias(a)\n" +
			"         │   └─ Table(two_pk)\n" +
			"         └─ TableAlias(b)\n" +
			"             └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a, two_pk b WHERE a.pk1=b.pk1 AND a.pk2=b.pk2 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(a.pk1 ASC, a.pk2 ASC, b.pk1 ASC)\n" +
			" └─ Project(a.pk1, a.pk2, b.pk1, b.pk2)\n" +
			"     └─ IndexedJoin((a.pk1 = b.pk1) AND (a.pk2 = b.pk2))\n" +
			"         ├─ TableAlias(a)\n" +
			"         │   └─ Table(two_pk)\n" +
			"         └─ TableAlias(b)\n" +
			"             └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a, two_pk b WHERE a.pk1=b.pk2 AND a.pk2=b.pk1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(a.pk1 ASC, a.pk2 ASC, b.pk1 ASC)\n" +
			" └─ Project(a.pk1, a.pk2, b.pk1, b.pk2)\n" +
			"     └─ IndexedJoin((a.pk1 = b.pk2) AND (a.pk2 = b.pk1))\n" +
			"         ├─ TableAlias(a)\n" +
			"         │   └─ Table(two_pk)\n" +
			"         └─ TableAlias(b)\n" +
			"             └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT one_pk.c5,pk1,pk2 FROM one_pk JOIN two_pk ON pk=pk1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(one_pk.c5 ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
			" └─ Project(one_pk.c5, two_pk.pk1, two_pk.pk2)\n" +
			"     └─ IndexedJoin(one_pk.pk = two_pk.pk1)\n" +
			"         ├─ Table(one_pk)\n" +
			"         └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT opk.c5,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON opk.pk=tpk.pk1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(opk.c5 ASC, tpk.pk1 ASC, tpk.pk2 ASC)\n" +
			" └─ Project(opk.c5, tpk.pk1, tpk.pk2)\n" +
			"     └─ IndexedJoin(opk.pk = tpk.pk1)\n" +
			"         ├─ TableAlias(opk)\n" +
			"         │   └─ Table(one_pk)\n" +
			"         └─ TableAlias(tpk)\n" +
			"             └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT opk.c5,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON pk=pk1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(opk.c5 ASC, tpk.pk1 ASC, tpk.pk2 ASC)\n" +
			" └─ Project(opk.c5, tpk.pk1, tpk.pk2)\n" +
			"     └─ IndexedJoin(opk.pk = tpk.pk1)\n" +
			"         ├─ TableAlias(opk)\n" +
			"         │   └─ Table(one_pk)\n" +
			"         └─ TableAlias(tpk)\n" +
			"             └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT opk.c5,pk1,pk2 FROM one_pk opk, two_pk tpk WHERE pk=pk1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(opk.c5 ASC, tpk.pk1 ASC, tpk.pk2 ASC)\n" +
			" └─ Project(opk.c5, tpk.pk1, tpk.pk2)\n" +
			"     └─ IndexedJoin(opk.pk = tpk.pk1)\n" +
			"         ├─ TableAlias(opk)\n" +
			"         │   └─ Table(one_pk)\n" +
			"         └─ TableAlias(tpk)\n" +
			"             └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT one_pk.c5,pk1,pk2 FROM one_pk,two_pk WHERE pk=pk1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(one_pk.c5 ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
			" └─ Project(one_pk.c5, two_pk.pk1, two_pk.pk2)\n" +
			"     └─ IndexedJoin(one_pk.pk = two_pk.pk1)\n" +
			"         ├─ Table(one_pk)\n" +
			"         └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i ORDER BY 1`,
		ExpectedPlan: "Sort(one_pk.pk ASC)\n" +
			" └─ Project(one_pk.pk, niltable.i, niltable.f)\n" +
			"     └─ LeftIndexedJoin(one_pk.pk = niltable.i)\n" +
			"         ├─ Table(one_pk)\n" +
			"         └─ IndexedTableAccess(niltable on [niltable.i])\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE f IS NOT NULL ORDER BY 1`,
		ExpectedPlan: "Sort(one_pk.pk ASC)\n" +
			" └─ Project(one_pk.pk, niltable.i, niltable.f)\n" +
			"     └─ Filter(NOT(niltable.f IS NULL))\n" +
			"         └─ LeftIndexedJoin(one_pk.pk = niltable.i)\n" +
			"             ├─ Table(one_pk)\n" +
			"             └─ IndexedTableAccess(niltable on [niltable.i])\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE pk > 1 ORDER BY 1`,
		ExpectedPlan: "Sort(one_pk.pk ASC)\n" +
			" └─ Project(one_pk.pk, niltable.i, niltable.f)\n" +
			"     └─ LeftIndexedJoin(one_pk.pk = niltable.i)\n" +
			"         ├─ Filter(one_pk.pk > 1)\n" +
			"         │   └─ IndexedTableAccess(one_pk on [one_pk.pk] with ranges: [{(1, ∞)}])\n" +
			"         └─ IndexedTableAccess(niltable on [niltable.i])\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i ORDER BY 2,3`,
		ExpectedPlan: "Sort(niltable.i ASC, niltable.f ASC)\n" +
			" └─ Project(one_pk.pk, niltable.i, niltable.f)\n" +
			"     └─ RightIndexedJoin(one_pk.pk = niltable.i)\n" +
			"         ├─ Table(niltable)\n" +
			"         └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i WHERE f IS NOT NULL ORDER BY 2,3`,
		ExpectedPlan: "Sort(niltable.i ASC, niltable.f ASC)\n" +
			" └─ Project(one_pk.pk, niltable.i, niltable.f)\n" +
			"     └─ RightIndexedJoin(one_pk.pk = niltable.i)\n" +
			"         ├─ Filter(NOT(niltable.f IS NULL))\n" +
			"         │   └─ Table(niltable)\n" +
			"         └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i WHERE pk > 0 ORDER BY 2,3`,
		ExpectedPlan: "Sort(niltable.i ASC, niltable.f ASC)\n" +
			" └─ Project(one_pk.pk, niltable.i, niltable.f)\n" +
			"     └─ Filter(one_pk.pk > 0)\n" +
			"         └─ RightIndexedJoin(one_pk.pk = niltable.i)\n" +
			"             ├─ Table(niltable)\n" +
			"             └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"",
	},
	{
		Query: `SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i and pk > 0 ORDER BY 2,3`,
		ExpectedPlan: "Sort(niltable.i ASC, niltable.f ASC)\n" +
			" └─ Project(one_pk.pk, niltable.i, niltable.f)\n" +
			"     └─ RightJoin((one_pk.pk = niltable.i) AND (one_pk.pk > 0))\n" +
			"         ├─ Projected table access on [pk]\n" +
			"         │   └─ Table(one_pk)\n" +
			"         └─ Projected table access on [i f]\n" +
			"             └─ Table(niltable)\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(one_pk.pk ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
			" └─ Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			"     └─ IndexedJoin((one_pk.pk = two_pk.pk1) AND (one_pk.pk = two_pk.pk2))\n" +
			"         ├─ Table(one_pk)\n" +
			"         └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON pk1-pk>0 AND pk2<1`,
		ExpectedPlan: "Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			" └─ InnerJoin((two_pk.pk1 - one_pk.pk) > 0)\n" +
			"     ├─ Projected table access on [pk]\n" +
			"     │   └─ Table(one_pk)\n" +
			"     └─ Filter(two_pk.pk2 < 1)\n" +
			"         └─ Projected table access on [pk1 pk2]\n" +
			"             └─ Table(two_pk)\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(one_pk.pk ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
			" └─ Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			"     └─ CrossJoin\n" +
			"         ├─ Projected table access on [pk]\n" +
			"         │   └─ Table(one_pk)\n" +
			"         └─ Projected table access on [pk1 pk2]\n" +
			"             └─ Table(two_pk)\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(one_pk.pk ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
			" └─ Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			"     └─ LeftIndexedJoin((one_pk.pk = two_pk.pk1) AND (one_pk.pk = two_pk.pk2))\n" +
			"         ├─ Table(one_pk)\n" +
			"         └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON pk=pk1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(one_pk.pk ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
			" └─ Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			"     └─ LeftIndexedJoin(one_pk.pk = two_pk.pk1)\n" +
			"         ├─ Table(one_pk)\n" +
			"         └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk RIGHT JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(one_pk.pk ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
			" └─ Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			"     └─ RightIndexedJoin((one_pk.pk = two_pk.pk1) AND (one_pk.pk = two_pk.pk2))\n" +
			"         ├─ Table(two_pk)\n" +
			"         └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON opk.pk=tpk.pk1 AND opk.pk=tpk.pk2 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(opk.pk ASC, tpk.pk1 ASC, tpk.pk2 ASC)\n" +
			" └─ Project(opk.pk, tpk.pk1, tpk.pk2)\n" +
			"     └─ IndexedJoin((opk.pk = tpk.pk1) AND (opk.pk = tpk.pk2))\n" +
			"         ├─ TableAlias(opk)\n" +
			"         │   └─ Table(one_pk)\n" +
			"         └─ TableAlias(tpk)\n" +
			"             └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON pk=tpk.pk1 AND pk=tpk.pk2 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(opk.pk ASC, tpk.pk1 ASC, tpk.pk2 ASC)\n" +
			" └─ Project(opk.pk, tpk.pk1, tpk.pk2)\n" +
			"     └─ IndexedJoin((opk.pk = tpk.pk1) AND (opk.pk = tpk.pk2))\n" +
			"         ├─ TableAlias(opk)\n" +
			"         │   └─ Table(one_pk)\n" +
			"         └─ TableAlias(tpk)\n" +
			"             └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2])\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk,two_pk WHERE one_pk.c1=two_pk.c1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(one_pk.pk ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
			" └─ Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			"     └─ InnerJoin(one_pk.c1 = two_pk.c1)\n" +
			"         ├─ Projected table access on [pk c1]\n" +
			"         │   └─ Table(one_pk)\n" +
			"         └─ Projected table access on [pk1 pk2 c1]\n" +
			"             └─ Table(two_pk)\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2,one_pk.c1 AS foo, two_pk.c1 AS bar FROM one_pk JOIN two_pk ON one_pk.c1=two_pk.c1 ORDER BY 1,2,3`,
		ExpectedPlan: "Sort(one_pk.pk ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
			" └─ Project(one_pk.pk, two_pk.pk1, two_pk.pk2, one_pk.c1 as foo, two_pk.c1 as bar)\n" +
			"     └─ InnerJoin(one_pk.c1 = two_pk.c1)\n" +
			"         ├─ Projected table access on [pk c1]\n" +
			"         │   └─ Table(one_pk)\n" +
			"         └─ Projected table access on [pk1 pk2 c1]\n" +
			"             └─ Table(two_pk)\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2,one_pk.c1 AS foo,two_pk.c1 AS bar FROM one_pk JOIN two_pk ON one_pk.c1=two_pk.c1 WHERE one_pk.c1=10`,
		ExpectedPlan: "Project(one_pk.pk, two_pk.pk1, two_pk.pk2, one_pk.c1 as foo, two_pk.c1 as bar)\n" +
			" └─ InnerJoin(one_pk.c1 = two_pk.c1)\n" +
			"     ├─ Filter(one_pk.c1 = 10)\n" +
			"     │   └─ Projected table access on [pk c1]\n" +
			"     │       └─ Table(one_pk)\n" +
			"     └─ Projected table access on [pk1 pk2 c1]\n" +
			"         └─ Table(two_pk)\n" +
			"",
	},
	{
		Query: `SELECT pk,pk2 FROM one_pk t1, two_pk t2 WHERE pk=1 AND pk2=1 ORDER BY 1,2`,
		ExpectedPlan: "Sort(t1.pk ASC, t2.pk2 ASC)\n" +
			" └─ Project(t1.pk, t2.pk2)\n" +
			"     └─ CrossJoin\n" +
			"         ├─ Filter(t1.pk = 1)\n" +
			"         │   └─ Projected table access on [pk]\n" +
			"         │       └─ TableAlias(t1)\n" +
			"         │           └─ IndexedTableAccess(one_pk on [one_pk.pk] with ranges: [{[1, 1]}])\n" +
			"         └─ Filter(t2.pk2 = 1)\n" +
			"             └─ Projected table access on [pk2]\n" +
			"                 └─ TableAlias(t2)\n" +
			"                     └─ Table(two_pk)\n" +
			"",
	},
	{
		Query: `SELECT pk,pk1,pk2 FROM one_pk t1, two_pk t2 WHERE pk=1 AND pk2=1 AND pk1=1 ORDER BY 1,2`,
		ExpectedPlan: "Sort(t1.pk ASC, t2.pk1 ASC)\n" +
			" └─ Project(t1.pk, t2.pk1, t2.pk2)\n" +
			"     └─ CrossJoin\n" +
			"         ├─ Filter(t1.pk = 1)\n" +
			"         │   └─ Projected table access on [pk]\n" +
			"         │       └─ TableAlias(t1)\n" +
			"         │           └─ IndexedTableAccess(one_pk on [one_pk.pk] with ranges: [{[1, 1]}])\n" +
			"         └─ Filter((t2.pk2 = 1) AND (t2.pk1 = 1))\n" +
			"             └─ Projected table access on [pk1 pk2]\n" +
			"                 └─ TableAlias(t2)\n" +
			"                     └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2] with ranges: [{[1, 1], (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT i FROM mytable mt
		WHERE (SELECT i FROM mytable where i = mt.i and i > 2) IS NOT NULL
		AND (SELECT i2 FROM othertable where i2 = i) IS NOT NULL`,
		ExpectedPlan: "Project(mt.i)\n" +
			" └─ Filter((NOT((Project(mytable.i)\n" +
			"     └─ Filter(mytable.i = mt.i)\n" +
			"         └─ Projected table access on [i]\n" +
			"             └─ Filter(mytable.i > 2)\n" +
			"                 └─ IndexedTableAccess(mytable on [mytable.i] with ranges: [{(2, ∞)}])\n" +
			"    ) IS NULL)) AND (NOT((Project(othertable.i2)\n" +
			"     └─ Filter(othertable.i2 = mt.i)\n" +
			"         └─ Projected table access on [i2]\n" +
			"             └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"    ) IS NULL)))\n" +
			"     └─ TableAlias(mt)\n" +
			"         └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT i FROM mytable mt
		WHERE (SELECT i FROM mytable where i = mt.i) IS NOT NULL
		AND (SELECT i2 FROM othertable where i2 = i and i > 2) IS NOT NULL`,
		ExpectedPlan: "Project(mt.i)\n" +
			" └─ Filter((NOT((Project(mytable.i)\n" +
			"     └─ Filter(mytable.i = mt.i)\n" +
			"         └─ Projected table access on [i]\n" +
			"             └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"    ) IS NULL)) AND (NOT((Project(othertable.i2)\n" +
			"     └─ Filter((othertable.i2 = mt.i) AND (mt.i > 2))\n" +
			"         └─ Projected table access on [i2]\n" +
			"             └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"    ) IS NULL)))\n" +
			"     └─ TableAlias(mt)\n" +
			"         └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `SELECT pk,pk2, (SELECT pk from one_pk where pk = 1 limit 1) FROM one_pk t1, two_pk t2 WHERE pk=1 AND pk2=1 ORDER BY 1,2`,
		ExpectedPlan: "Sort(t1.pk ASC, t2.pk2 ASC)\n" +
			" └─ Project(t1.pk, t2.pk2, (Limit(1)\n" +
			"     └─ Project(one_pk.pk)\n" +
			"         └─ Projected table access on [pk]\n" +
			"             └─ Filter(one_pk.pk = 1)\n" +
			"                 └─ IndexedTableAccess(one_pk on [one_pk.pk] with ranges: [{[1, 1]}])\n" +
			"    ) as (SELECT pk from one_pk where pk = 1 limit 1))\n" +
			"     └─ CrossJoin\n" +
			"         ├─ Filter(t1.pk = 1)\n" +
			"         │   └─ TableAlias(t1)\n" +
			"         │       └─ IndexedTableAccess(one_pk on [one_pk.pk] with ranges: [{[1, 1]}])\n" +
			"         └─ Filter(t2.pk2 = 1)\n" +
			"             └─ TableAlias(t2)\n" +
			"                 └─ Table(two_pk)\n" +
			"",
	},
	{
		Query: `SELECT ROW_NUMBER() OVER (ORDER BY s2 ASC) idx, i2, s2 FROM othertable WHERE s2 <> 'second' ORDER BY i2 ASC`,
		ExpectedPlan: "Sort(othertable.i2 ASC)\n" +
			" └─ Project(row_number() over ( order by [othertable.s2, idx=0, type=TEXT, nullable=false] ASC) as idx, othertable.i2, othertable.s2)\n" +
			"     └─ Window(row_number() over ( order by [othertable.s2, idx=0, type=TEXT, nullable=false] ASC), othertable.i2, othertable.s2)\n" +
			"         └─ Filter(NOT((othertable.s2 = \"second\")))\n" +
			"             └─ Projected table access on [i2 s2]\n" +
			"                 └─ IndexedTableAccess(othertable on [othertable.s2] with ranges: [{(second, ∞)}, {(-∞, second)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM (SELECT ROW_NUMBER() OVER (ORDER BY s2 ASC) idx, i2, s2 FROM othertable ORDER BY i2 ASC) a WHERE s2 <> 'second'`,
		ExpectedPlan: "SubqueryAlias(a)\n" +
			" └─ Filter(NOT((othertable.s2 = \"second\")))\n" +
			"     └─ Sort(othertable.i2 ASC)\n" +
			"         └─ Project(row_number() over ( order by [othertable.s2, idx=0, type=TEXT, nullable=false] ASC) as idx, othertable.i2, othertable.s2)\n" +
			"             └─ Window(row_number() over ( order by [othertable.s2, idx=0, type=TEXT, nullable=false] ASC), othertable.i2, othertable.s2)\n" +
			"                 └─ Projected table access on [s2 i2]\n" +
			"                     └─ Table(othertable)\n" +
			"",
	},
	{
		Query: `SELECT ROW_NUMBER() OVER (ORDER BY s2 ASC) idx, i2, s2 FROM othertable WHERE i2 < 2 OR i2 > 2 ORDER BY i2 ASC`,
		ExpectedPlan: "Sort(othertable.i2 ASC)\n" +
			" └─ Project(row_number() over ( order by [othertable.s2, idx=0, type=TEXT, nullable=false] ASC) as idx, othertable.i2, othertable.s2)\n" +
			"     └─ Window(row_number() over ( order by [othertable.s2, idx=0, type=TEXT, nullable=false] ASC), othertable.i2, othertable.s2)\n" +
			"         └─ Filter((othertable.i2 < 2) OR (othertable.i2 > 2))\n" +
			"             └─ Projected table access on [i2 s2]\n" +
			"                 └─ IndexedTableAccess(othertable on [othertable.i2] with ranges: [{(-∞, 2)}, {(2, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM (SELECT ROW_NUMBER() OVER (ORDER BY s2 ASC) idx, i2, s2 FROM othertable ORDER BY i2 ASC) a WHERE i2 < 2 OR i2 > 2`,
		ExpectedPlan: "SubqueryAlias(a)\n" +
			" └─ Filter((othertable.i2 < 2) OR (othertable.i2 > 2))\n" +
			"     └─ Sort(othertable.i2 ASC)\n" +
			"         └─ Project(row_number() over ( order by [othertable.s2, idx=0, type=TEXT, nullable=false] ASC) as idx, othertable.i2, othertable.s2)\n" +
			"             └─ Window(row_number() over ( order by [othertable.s2, idx=0, type=TEXT, nullable=false] ASC), othertable.i2, othertable.s2)\n" +
			"                 └─ Projected table access on [i2 s2]\n" +
			"                     └─ Table(othertable)\n" +
			"",
	},
	{
		Query: `SELECT t, n, lag(t, 1, t+1) over (partition by n) FROM bigtable`,
		ExpectedPlan: "Project(bigtable.t, bigtable.n, lag(bigtable.t, 1, (bigtable.t + 1)) over ( partition by bigtable.n) as lag(t, 1, t+1) over (partition by n))\n" +
			" └─ Window(bigtable.t, bigtable.n, lag(bigtable.t, 1, (bigtable.t + 1)) over ( partition by bigtable.n))\n" +
			"     └─ Projected table access on [t n]\n" +
			"         └─ Table(bigtable)\n" +
			"",
	},
	{
		Query: `select i, row_number() over (w3) from mytable window w1 as (w2), w2 as (), w3 as (w1)`,
		ExpectedPlan: "Project(mytable.i, row_number() over () as row_number() over (w3))\n" +
			" └─ Window(mytable.i, row_number() over ())\n" +
			"     └─ Projected table access on [i]\n" +
			"         └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `select i, row_number() over (w1 partition by s) from mytable window w1 as (order by i asc)`,
		ExpectedPlan: "Project(mytable.i, row_number() over ( partition by mytable.s order by [mytable.i, idx=0, type=BIGINT, nullable=false] ASC) as row_number() over (w1 partition by s))\n" +
			" └─ Window(mytable.i, row_number() over ( partition by mytable.s order by [mytable.i, idx=0, type=BIGINT, nullable=false] ASC))\n" +
			"     └─ Projected table access on [i s]\n" +
			"         └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `DELETE FROM two_pk WHERE c1 > 1`,
		ExpectedPlan: "Delete\n" +
			" └─ Filter(two_pk.c1 > 1)\n" +
			"     └─ Table(two_pk)\n" +
			"",
	},
	{
		Query: `DELETE FROM two_pk WHERE pk1 = 1 AND pk2 = 2`,
		ExpectedPlan: "Delete\n" +
			" └─ Filter((two_pk.pk1 = 1) AND (two_pk.pk2 = 2))\n" +
			"     └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2] with ranges: [{[1, 1], [2, 2]}])\n" +
			"",
	},
	{
		Query: `UPDATE two_pk SET c1 = 1 WHERE c1 > 1`,
		ExpectedPlan: "Update\n" +
			" └─ UpdateSource(SET two_pk.c1 = 1)\n" +
			"     └─ Filter(two_pk.c1 > 1)\n" +
			"         └─ Table(two_pk)\n" +
			"",
	},
	{
		Query: `UPDATE two_pk SET c1 = 1 WHERE pk1 = 1 AND pk2 = 2`,
		ExpectedPlan: "Update\n" +
			" └─ UpdateSource(SET two_pk.c1 = 1)\n" +
			"     └─ Filter((two_pk.pk1 = 1) AND (two_pk.pk2 = 2))\n" +
			"         └─ IndexedTableAccess(two_pk on [two_pk.pk1,two_pk.pk2] with ranges: [{[1, 1], [2, 2]}])\n" +
			"",
	},
	{
		Query: `UPDATE /*+ JOIN_ORDER(two_pk, one_pk) */ one_pk JOIN two_pk on one_pk.pk = two_pk.pk1 SET two_pk.c1 = two_pk.c1 + 1`,
		ExpectedPlan: "Update\n" +
			" └─ Update Join\n" +
			"     └─ UpdateSource(SET two_pk.c1 = (two_pk.c1 + 1))\n" +
			"         └─ Project(one_pk.pk, one_pk.c1, one_pk.c2, one_pk.c3, one_pk.c4, one_pk.c5, two_pk.pk1, two_pk.pk2, two_pk.c1, two_pk.c2, two_pk.c3, two_pk.c4, two_pk.c5)\n" +
			"             └─ IndexedJoin(one_pk.pk = two_pk.pk1)\n" +
			"                 ├─ Table(two_pk)\n" +
			"                 └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"",
	},
	{
		Query: `UPDATE one_pk INNER JOIN (SELECT * FROM two_pk) as t2 on one_pk.pk = t2.pk1 SET one_pk.c1 = one_pk.c1 + 1, one_pk.c2 = one_pk.c2 + 1`,
		ExpectedPlan: "Update\n" +
			" └─ Update Join\n" +
			"     └─ UpdateSource(SET one_pk.c1 = (one_pk.c1 + 1),SET one_pk.c2 = (one_pk.c2 + 1))\n" +
			"         └─ Project(one_pk.pk, one_pk.c1, one_pk.c2, one_pk.c3, one_pk.c4, one_pk.c5, t2.pk1, t2.pk2, t2.c1, t2.c2, t2.c3, t2.c4, t2.c5)\n" +
			"             └─ IndexedJoin(one_pk.pk = t2.pk1)\n" +
			"                 ├─ SubqueryAlias(t2)\n" +
			"                 │   └─ Projected table access on [pk1 pk2 c1 c2 c3 c4 c5]\n" +
			"                 │       └─ Table(two_pk)\n" +
			"                 └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM invert_pk as a, invert_pk as b WHERE a.y = b.z`,
		ExpectedPlan: "Project(a.x, a.y, a.z)\n" +
			" └─ IndexedJoin(a.y = b.z)\n" +
			"     ├─ TableAlias(b)\n" +
			"     │   └─ Table(invert_pk)\n" +
			"     └─ TableAlias(a)\n" +
			"         └─ IndexedTableAccess(invert_pk on [invert_pk.y,invert_pk.z,invert_pk.x])\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM invert_pk as a, invert_pk as b WHERE a.y = b.z AND a.z = 2`,
		ExpectedPlan: "Project(a.x, a.y, a.z)\n" +
			" └─ IndexedJoin(a.y = b.z)\n" +
			"     ├─ TableAlias(b)\n" +
			"     │   └─ Table(invert_pk)\n" +
			"     └─ Filter(a.z = 2)\n" +
			"         └─ TableAlias(a)\n" +
			"             └─ IndexedTableAccess(invert_pk on [invert_pk.y,invert_pk.z,invert_pk.x])\n" +
			"",
	},
	{
		Query: `SELECT * FROM invert_pk WHERE y = 0`,
		ExpectedPlan: "Filter(invert_pk.y = 0)\n" +
			" └─ Projected table access on [x y z]\n" +
			"     └─ IndexedTableAccess(invert_pk on [invert_pk.y,invert_pk.z,invert_pk.x] with ranges: [{[0, 0], (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM invert_pk WHERE y >= 0`,
		ExpectedPlan: "Filter(invert_pk.y >= 0)\n" +
			" └─ Projected table access on [x y z]\n" +
			"     └─ IndexedTableAccess(invert_pk on [invert_pk.y,invert_pk.z,invert_pk.x] with ranges: [{[0, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM invert_pk WHERE y >= 0 AND z < 1`,
		ExpectedPlan: "Filter((invert_pk.y >= 0) AND (invert_pk.z < 1))\n" +
			" └─ Projected table access on [x y z]\n" +
			"     └─ IndexedTableAccess(invert_pk on [invert_pk.y,invert_pk.z,invert_pk.x] with ranges: [{[0, ∞), (-∞, 1), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM one_pk WHERE pk IN (1)`,
		ExpectedPlan: "Filter(one_pk.pk HASH IN (1))\n" +
			" └─ Projected table access on [pk c1 c2 c3 c4 c5]\n" +
			"     └─ IndexedTableAccess(one_pk on [one_pk.pk] with ranges: [{[1, 1]}])\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM one_pk a CROSS JOIN one_pk c LEFT JOIN one_pk b ON b.pk = c.pk and b.pk = a.pk`,
		ExpectedPlan: "Project(a.pk, a.c1, a.c2, a.c3, a.c4, a.c5)\n" +
			" └─ LeftIndexedJoin((b.pk = c.pk) AND (b.pk = a.pk))\n" +
			"     ├─ CrossJoin\n" +
			"     │   ├─ TableAlias(a)\n" +
			"     │   │   └─ Table(one_pk)\n" +
			"     │   └─ TableAlias(c)\n" +
			"     │       └─ Table(one_pk)\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM one_pk a CROSS JOIN one_pk c RIGHT JOIN one_pk b ON b.pk = c.pk and b.pk = a.pk`,
		ExpectedPlan: "Project(a.pk, a.c1, a.c2, a.c3, a.c4, a.c5)\n" +
			" └─ RightJoin((b.pk = c.pk) AND (b.pk = a.pk))\n" +
			"     ├─ CrossJoin\n" +
			"     │   ├─ Projected table access on [pk c1 c2 c3 c4 c5]\n" +
			"     │   │   └─ TableAlias(a)\n" +
			"     │   │       └─ Table(one_pk)\n" +
			"     │   └─ Projected table access on [pk]\n" +
			"     │       └─ TableAlias(c)\n" +
			"     │           └─ Table(one_pk)\n" +
			"     └─ Projected table access on [pk]\n" +
			"         └─ TableAlias(b)\n" +
			"             └─ Table(one_pk)\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM one_pk a CROSS JOIN one_pk c INNER JOIN one_pk b ON b.pk = c.pk and b.pk = a.pk`,
		ExpectedPlan: "Project(a.pk, a.c1, a.c2, a.c3, a.c4, a.c5)\n" +
			" └─ IndexedJoin((b.pk = c.pk) AND (b.pk = a.pk))\n" +
			"     ├─ CrossJoin\n" +
			"     │   ├─ TableAlias(a)\n" +
			"     │   │   └─ Table(one_pk)\n" +
			"     │   └─ TableAlias(c)\n" +
			"     │       └─ Table(one_pk)\n" +
			"     └─ TableAlias(b)\n" +
			"         └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM one_pk a CROSS JOIN one_pk b INNER JOIN one_pk c ON b.pk = c.pk LEFT JOIN one_pk d ON c.pk = d.pk`,
		ExpectedPlan: "Project(a.pk, a.c1, a.c2, a.c3, a.c4, a.c5)\n" +
			" └─ LeftIndexedJoin(c.pk = d.pk)\n" +
			"     ├─ IndexedJoin(b.pk = c.pk)\n" +
			"     │   ├─ CrossJoin\n" +
			"     │   │   ├─ TableAlias(a)\n" +
			"     │   │   │   └─ Table(one_pk)\n" +
			"     │   │   └─ TableAlias(b)\n" +
			"     │   │       └─ Table(one_pk)\n" +
			"     │   └─ TableAlias(c)\n" +
			"     │       └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"     └─ TableAlias(d)\n" +
			"         └─ IndexedTableAccess(one_pk on [one_pk.pk])\n" +
			"",
	},
	{
		Query: `SELECT a.* FROM one_pk a CROSS JOIN one_pk c INNER JOIN (select * from one_pk) b ON b.pk = c.pk`,
		ExpectedPlan: "Project(a.pk, a.c1, a.c2, a.c3, a.c4, a.c5)\n" +
			" └─ InnerJoin(b.pk = c.pk)\n" +
			"     ├─ CrossJoin\n" +
			"     │   ├─ TableAlias(a)\n" +
			"     │   │   └─ Table(one_pk)\n" +
			"     │   └─ TableAlias(c)\n" +
			"     │       └─ Table(one_pk)\n" +
			"     └─ HashLookup(child: (b.pk), lookup: (c.pk))\n" +
			"         └─ CachedResults\n" +
			"             └─ SubqueryAlias(b)\n" +
			"                 └─ Projected table access on [pk c1 c2 c3 c4 c5]\n" +
			"                     └─ Table(one_pk)\n" +
			"",
	},
	{
		Query: `SELECT * FROM tabletest join mytable mt INNER JOIN othertable ot ON tabletest.i = ot.i2 order by 1,3,6`,
		ExpectedPlan: "Sort(tabletest.i ASC, mt.i ASC, ot.i2 ASC)\n" +
			" └─ IndexedJoin(tabletest.i = ot.i2)\n" +
			"     ├─ CrossJoin\n" +
			"     │   ├─ Table(tabletest)\n" +
			"     │   └─ TableAlias(mt)\n" +
			"     │       └─ Table(mytable)\n" +
			"     └─ TableAlias(ot)\n" +
			"         └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"",
	},
	{
		Query: `select a.pk, c.v2 from one_pk_three_idx a cross join one_pk_three_idx b right join one_pk_three_idx c on b.pk = c.v1 where b.pk = 0 and c.v2 = 0;`,
		ExpectedPlan: "Project(a.pk, c.v2)\n" +
			" └─ Filter(b.pk = 0)\n" +
			"     └─ RightJoin(b.pk = c.v1)\n" +
			"         ├─ CrossJoin\n" +
			"         │   ├─ Projected table access on [pk]\n" +
			"         │   │   └─ TableAlias(a)\n" +
			"         │   │       └─ Table(one_pk_three_idx)\n" +
			"         │   └─ Projected table access on [pk]\n" +
			"         │       └─ TableAlias(b)\n" +
			"         │           └─ Table(one_pk_three_idx)\n" +
			"         └─ Filter(c.v2 = 0)\n" +
			"             └─ Projected table access on [v2 v1]\n" +
			"                 └─ TableAlias(c)\n" +
			"                     └─ Table(one_pk_three_idx)\n" +
			"",
	},
	{
		Query: `select a.pk, c.v2 from one_pk_three_idx a cross join one_pk_three_idx b left join one_pk_three_idx c on b.pk = c.v1 where b.pk = 0 and a.v2 = 1;`,
		ExpectedPlan: "Project(a.pk, c.v2)\n" +
			" └─ LeftIndexedJoin(b.pk = c.v1)\n" +
			"     ├─ CrossJoin\n" +
			"     │   ├─ Filter(a.v2 = 1)\n" +
			"     │   │   └─ TableAlias(a)\n" +
			"     │   │       └─ Table(one_pk_three_idx)\n" +
			"     │   └─ Filter(b.pk = 0)\n" +
			"     │       └─ TableAlias(b)\n" +
			"     │           └─ IndexedTableAccess(one_pk_three_idx on [one_pk_three_idx.pk] with ranges: [{[0, 0]}])\n" +
			"     └─ TableAlias(c)\n" +
			"         └─ IndexedTableAccess(one_pk_three_idx on [one_pk_three_idx.v1,one_pk_three_idx.v2,one_pk_three_idx.v3])\n" +
			"",
	},
	{
		Query: `with a as (select a.i, a.s from mytable a CROSS JOIN mytable b) select * from a RIGHT JOIN mytable c on a.i+1 = c.i-1;`,
		ExpectedPlan: "RightJoin((a.i + 1) = (c.i - 1))\n" +
			" ├─ CachedResults\n" +
			" │   └─ SubqueryAlias(a)\n" +
			" │       └─ Project(a.i, a.s)\n" +
			" │           └─ CrossJoin\n" +
			" │               ├─ Projected table access on [i s]\n" +
			" │               │   └─ TableAlias(a)\n" +
			" │               │       └─ Table(mytable)\n" +
			" │               └─ TableAlias(b)\n" +
			" │                   └─ Table(mytable)\n" +
			" └─ TableAlias(c)\n" +
			"     └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `select a.* from mytable a RIGHT JOIN mytable b on a.i = b.i+1 LEFT JOIN mytable c on a.i = c.i-1 RIGHT JOIN mytable d on b.i = d.i;`,
		ExpectedPlan: "Project(a.i, a.s)\n" +
			" └─ RightIndexedJoin(b.i = d.i)\n" +
			"     ├─ TableAlias(d)\n" +
			"     │   └─ Table(mytable)\n" +
			"     └─ LeftIndexedJoin(a.i = (c.i - 1))\n" +
			"         ├─ RightIndexedJoin(a.i = (b.i + 1))\n" +
			"         │   ├─ TableAlias(b)\n" +
			"         │   │   └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"         │   └─ TableAlias(a)\n" +
			"         │       └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"         └─ TableAlias(c)\n" +
			"             └─ Table(mytable)\n" +
			"",
	},
	{
		Query: `select a.*,b.* from mytable a RIGHT JOIN othertable b on a.i = b.i2+1 LEFT JOIN mytable c on a.i = c.i-1 LEFT JOIN othertable d on b.i2 = d.i2;`,
		ExpectedPlan: "Project(a.i, a.s, b.s2, b.i2)\n" +
			" └─ LeftIndexedJoin(b.i2 = d.i2)\n" +
			"     ├─ LeftIndexedJoin(a.i = (c.i - 1))\n" +
			"     │   ├─ RightIndexedJoin(a.i = (b.i2 + 1))\n" +
			"     │   │   ├─ TableAlias(b)\n" +
			"     │   │   │   └─ Table(othertable)\n" +
			"     │   │   └─ TableAlias(a)\n" +
			"     │   │       └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"     │   └─ TableAlias(c)\n" +
			"     │       └─ Table(mytable)\n" +
			"     └─ TableAlias(d)\n" +
			"         └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"",
	},
	{
		Query: `select a.*,b.* from mytable a RIGHT JOIN othertable b on a.i = b.i2+1 RIGHT JOIN mytable c on a.i = c.i-1 LEFT JOIN othertable d on b.i2 = d.i2;`,
		ExpectedPlan: "Project(a.i, a.s, b.s2, b.i2)\n" +
			" └─ LeftIndexedJoin(b.i2 = d.i2)\n" +
			"     ├─ RightIndexedJoin(a.i = (c.i - 1))\n" +
			"     │   ├─ TableAlias(c)\n" +
			"     │   │   └─ Table(mytable)\n" +
			"     │   └─ RightIndexedJoin(a.i = (b.i2 + 1))\n" +
			"     │       ├─ TableAlias(b)\n" +
			"     │       │   └─ Table(othertable)\n" +
			"     │       └─ TableAlias(a)\n" +
			"     │           └─ IndexedTableAccess(mytable on [mytable.i])\n" +
			"     └─ TableAlias(d)\n" +
			"         └─ IndexedTableAccess(othertable on [othertable.i2])\n" +
			"",
	},
	{
		Query: `select i.pk, j.v3 from one_pk_two_idx i JOIN one_pk_three_idx j on i.v1 = j.pk;`,
		ExpectedPlan: "Project(i.pk, j.v3)\n" +
			" └─ IndexedJoin(i.v1 = j.pk)\n" +
			"     ├─ TableAlias(i)\n" +
			"     │   └─ Table(one_pk_two_idx)\n" +
			"     └─ TableAlias(j)\n" +
			"         └─ IndexedTableAccess(one_pk_three_idx on [one_pk_three_idx.pk])\n" +
			"",
	},
	{
		Query: `select i.pk, j.v3, k.c1 from one_pk_two_idx i JOIN one_pk_three_idx j on i.v1 = j.pk JOIN one_pk k on j.v3 = k.pk;`,
		ExpectedPlan: "Project(i.pk, j.v3, k.c1)\n" +
			" └─ IndexedJoin(j.v3 = k.pk)\n" +
			"     ├─ TableAlias(k)\n" +
			"     │   └─ Table(one_pk)\n" +
			"     └─ IndexedJoin(i.v1 = j.pk)\n" +
			"         ├─ TableAlias(i)\n" +
			"         │   └─ Table(one_pk_two_idx)\n" +
			"         └─ TableAlias(j)\n" +
			"             └─ IndexedTableAccess(one_pk_three_idx on [one_pk_three_idx.pk])\n" +
			"",
	},
	{
		Query: `select i.pk, j.v3 from (one_pk_two_idx i JOIN one_pk_three_idx j on((i.v1 = j.pk)));`,
		ExpectedPlan: "Project(i.pk, j.v3)\n" +
			" └─ IndexedJoin(i.v1 = j.pk)\n" +
			"     ├─ TableAlias(i)\n" +
			"     │   └─ Table(one_pk_two_idx)\n" +
			"     └─ TableAlias(j)\n" +
			"         └─ IndexedTableAccess(one_pk_three_idx on [one_pk_three_idx.pk])\n" +
			"",
	},
	{
		Query: `select i.pk, j.v3, k.c1 from ((one_pk_two_idx i JOIN one_pk_three_idx j on ((i.v1 = j.pk))) JOIN one_pk k on((j.v3 = k.pk)));`,
		ExpectedPlan: "Project(i.pk, j.v3, k.c1)\n" +
			" └─ IndexedJoin(j.v3 = k.pk)\n" +
			"     ├─ TableAlias(k)\n" +
			"     │   └─ Table(one_pk)\n" +
			"     └─ IndexedJoin(i.v1 = j.pk)\n" +
			"         ├─ TableAlias(i)\n" +
			"         │   └─ Table(one_pk_two_idx)\n" +
			"         └─ TableAlias(j)\n" +
			"             └─ IndexedTableAccess(one_pk_three_idx on [one_pk_three_idx.pk])\n" +
			"",
	},
	{
		Query: `select i.pk, j.v3, k.c1 from (one_pk_two_idx i JOIN one_pk_three_idx j on ((i.v1 = j.pk)) JOIN one_pk k on((j.v3 = k.pk)))`,
		ExpectedPlan: "Project(i.pk, j.v3, k.c1)\n" +
			" └─ IndexedJoin(j.v3 = k.pk)\n" +
			"     ├─ TableAlias(k)\n" +
			"     │   └─ Table(one_pk)\n" +
			"     └─ IndexedJoin(i.v1 = j.pk)\n" +
			"         ├─ TableAlias(i)\n" +
			"         │   └─ Table(one_pk_two_idx)\n" +
			"         └─ TableAlias(j)\n" +
			"             └─ IndexedTableAccess(one_pk_three_idx on [one_pk_three_idx.pk])\n" +
			"",
	},
	{
		Query: `select a.* from one_pk_two_idx a RIGHT JOIN (one_pk_two_idx i JOIN one_pk_three_idx j on i.v1 = j.pk) on a.pk = i.v1 LEFT JOIN (one_pk_two_idx k JOIN one_pk_three_idx l on k.v1 = l.pk) on a.pk = l.v2;`,
		ExpectedPlan: "Project(a.pk, a.v1, a.v2)\n" +
			" └─ LeftIndexedJoin(a.pk = l.v2)\n" +
			"     ├─ RightIndexedJoin(a.pk = i.v1)\n" +
			"     │   ├─ IndexedJoin(i.v1 = j.pk)\n" +
			"     │   │   ├─ TableAlias(i)\n" +
			"     │   │   │   └─ Table(one_pk_two_idx)\n" +
			"     │   │   └─ TableAlias(j)\n" +
			"     │   │       └─ IndexedTableAccess(one_pk_three_idx on [one_pk_three_idx.pk])\n" +
			"     │   └─ TableAlias(a)\n" +
			"     │       └─ IndexedTableAccess(one_pk_two_idx on [one_pk_two_idx.pk])\n" +
			"     └─ IndexedJoin(k.v1 = l.pk)\n" +
			"         ├─ TableAlias(k)\n" +
			"         │   └─ Table(one_pk_two_idx)\n" +
			"         └─ TableAlias(l)\n" +
			"             └─ IndexedTableAccess(one_pk_three_idx on [one_pk_three_idx.pk])\n" +
			"",
	},
	{
		Query: `select a.* from one_pk_two_idx a LEFT JOIN (one_pk_two_idx i JOIN one_pk_three_idx j on i.pk = j.v3) on a.pk = i.pk RIGHT JOIN (one_pk_two_idx k JOIN one_pk_three_idx l on k.v2 = l.v3) on a.v1 = l.v2;`,
		ExpectedPlan: "Project(a.pk, a.v1, a.v2)\n" +
			" └─ RightIndexedJoin(a.v1 = l.v2)\n" +
			"     ├─ IndexedJoin(k.v2 = l.v3)\n" +
			"     │   ├─ TableAlias(k)\n" +
			"     │   │   └─ Table(one_pk_two_idx)\n" +
			"     │   └─ TableAlias(l)\n" +
			"     │       └─ Table(one_pk_three_idx)\n" +
			"     └─ LeftIndexedJoin(a.pk = i.pk)\n" +
			"         ├─ TableAlias(a)\n" +
			"         │   └─ IndexedTableAccess(one_pk_two_idx on [one_pk_two_idx.v1])\n" +
			"         └─ IndexedJoin(i.pk = j.v3)\n" +
			"             ├─ TableAlias(j)\n" +
			"             │   └─ Table(one_pk_three_idx)\n" +
			"             └─ TableAlias(i)\n" +
			"                 └─ IndexedTableAccess(one_pk_two_idx on [one_pk_two_idx.pk])\n" +
			"",
	},
}
