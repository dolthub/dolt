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
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"

	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dfunctions"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dtables"
)

var ViewsWithAsOfScriptTest = queries.ScriptTest{
	SkipPrepared: true,
	Name:         "Querying a view with a union using an as of expression",
	SetUpScript: []string{
		"CALL dolt_commit('--allow-empty', '-m', 'cm0');",

		"CREATE TABLE t1 (pk int PRIMARY KEY AUTO_INCREMENT, c0 int);",
		"CALL dolt_add('.')",
		"CALL dolt_commit('-am', 'cm1');",
		"INSERT INTO t1 (c0) VALUES (1), (2);",
		"CALL dolt_commit('-am', 'cm2');",

		"CREATE TABLE t2 (pk int PRIMARY KEY AUTO_INCREMENT, vc varchar(100));",
		"CALL dolt_add('.')",
		"CALL dolt_commit('-am', 'cm3');",
		"INSERT INTO t2 (vc) VALUES ('one'), ('two');",
		"CALL dolt_commit('-am', 'cm4');",

		"CREATE VIEW v1 as select * from t1 union select * from t2",
		"call dolt_add('.');",
		"CALL dolt_commit('-am', 'cm5');",
	},
	Assertions: []queries.ScriptTestAssertion{
		{
			Query:    "select * from v1",
			Expected: []sql.Row{{1, "1"}, {2, "2"}, {1, "one"}, {2, "two"}},
		},
		{
			Query:    "select * from v1 as of 'HEAD'",
			Expected: []sql.Row{{1, "1"}, {2, "2"}, {1, "one"}, {2, "two"}},
		},
		{
			Query:    "select * from v1 as of 'HEAD~1'",
			Expected: []sql.Row{{1, "1"}, {2, "2"}, {1, "one"}, {2, "two"}},
		},
		{
			Query:    "select * from v1 as of 'HEAD~2'",
			Expected: []sql.Row{{1, "1"}, {2, "2"}},
		},
		{
			// At this point table t1 doesn't exist yet, so the view should return an error
			Query:          "select * from v1 as of 'HEAD~3'",
			ExpectedErrStr: "table not found: t2, maybe you mean t1?",
		},
		{
			Query:          "select * from v1 as of 'HEAD~4'",
			ExpectedErrStr: "table not found: t2, maybe you mean t1?",
		},
		{
			Query:          "select * from v1 as of 'HEAD~5'",
			ExpectedErrStr: "table not found: t1",
		},
	},
}

var ShowCreateTableAsOfScriptTest = queries.ScriptTest{
	Name: "Show create table as of",
	SetUpScript: []string{
		"set @Commit0 = hashof('main');",
		"create table a (pk int primary key, c1 int);",
		"call dolt_add('.');",
		"set @Commit1 = dolt_commit('-am', 'creating table a');",
		"alter table a add column c2 varchar(20);",
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
					"  `c2` varchar(20),\n" +
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
					"  `c2` varchar(20),\n" +
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
		"call dolt_add('.');",
		"set @Commit1 = dolt_commit('-am', 'creating table a');",
		"alter table a add column c2 varchar(20);",
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
				{"pk", "int", "NO", "PRI", "NULL", ""},
				{"c1", "int", "YES", "", "NULL", ""},
			},
		},
		{
			Query: "describe a as of @Commit2;",
			Expected: []sql.Row{
				{"pk", "int", "NO", "PRI", "NULL", ""},
				{"c1", "int", "YES", "", "NULL", ""},
				{"c2", "varchar(20)", "YES", "", "NULL", ""},
			},
		},
		{
			Query: "describe a as of @Commit3;",
			Expected: []sql.Row{
				{"pk", "int", "NO", "PRI", "NULL", ""},
				{"c2", "varchar(20)", "YES", "", "NULL", ""},
			},
		},
	},
}

var DoltRevisionDbScripts = []queries.ScriptTest{
	{
		Name: "database revision specs: Ancestor references",
		SetUpScript: []string{
			"create table t01 (pk int primary key, c1 int)",
			"call dolt_add('t01');",
			"call dolt_commit('-am', 'creating table t01 on main');",
			"call dolt_branch('branch1');",
			"insert into t01 values (1, 1), (2, 2);",
			"call dolt_commit('-am', 'adding rows to table t01 on main');",
			"insert into t01 values (3, 3);",
			"call dolt_commit('-am', 'adding another row to table t01 on main');",
			"call dolt_tag('tag1');",
			"call dolt_checkout('branch1');",
			"insert into t01 values (100, 100), (200, 200);",
			"call dolt_commit('-am', 'inserting rows in t01 on branch1');",
			"insert into t01 values (1000, 1000);",
			"call dolt_commit('-am', 'inserting another row in t01 on branch1');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "show databases;",
				Expected: []sql.Row{{"mydb"}, {"information_schema"}, {"mysql"}},
			},
			{
				Query:    "use `mydb/tag1~`;",
				Expected: []sql.Row{},
			},
			{
				// The database name should be the resolved commit, not the revision spec we started with.
				// We can't easily match the exact commit in these tests, so match against a commit hash pattern.
				Query:    "select database() regexp '^mydb/[0-9a-v]{32}$', database() = 'mydb/tag1~';",
				Expected: []sql.Row{{true, false}},
			},
			{
				Query:    "select * from t01;",
				Expected: []sql.Row{{1, 1}, {2, 2}},
			},
			{
				Query:    "select * from `mydb/tag1^`.t01;",
				Expected: []sql.Row{{1, 1}, {2, 2}},
			},
			{
				// Only merge commits are valid for ^2 ancestor spec
				Query:          "select * from `mydb/tag1^2`.t01;",
				ExpectedErrStr: "invalid ancestor spec",
			},
			{
				Query:    "select * from `mydb/tag1~1`.t01;",
				Expected: []sql.Row{{1, 1}, {2, 2}},
			},
			{
				Query:    "select * from `mydb/tag1~2`.t01;",
				Expected: []sql.Row{},
			},
			{
				Query:       "select * from `mydb/tag1~3`.t01;",
				ExpectedErr: sql.ErrTableNotFound,
			},
			{
				Query:          "select * from `mydb/tag1~20`.t01;",
				ExpectedErrStr: "invalid ancestor spec",
			},
			{
				Query:    "select * from `mydb/branch1~`.t01;",
				Expected: []sql.Row{{100, 100}, {200, 200}},
			},
			{
				Query:    "select * from `mydb/branch1^`.t01;",
				Expected: []sql.Row{{100, 100}, {200, 200}},
			},
			{
				Query:    "select * from `mydb/branch1~2`.t01;",
				Expected: []sql.Row{},
			},
			{
				Query:       "select * from `mydb/branch1~3`.t01;",
				ExpectedErr: sql.ErrTableNotFound,
			},
		},
	},
	{
		Name: "database revision specs: tag-qualified revision spec",
		SetUpScript: []string{
			"create table t01 (pk int primary key, c1 int)",
			"call dolt_add('.')",
			"call dolt_commit('-am', 'creating table t01 on main');",
			"insert into t01 values (1, 1), (2, 2);",
			"call dolt_commit('-am', 'adding rows to table t01 on main');",
			"call dolt_tag('tag1');",
			"insert into t01 values (3, 3);",
			"call dolt_commit('-am', 'adding another row to table t01 on main');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "show databases;",
				Expected: []sql.Row{{"mydb"}, {"information_schema"}, {"mysql"}},
			},
			{
				Query:    "use mydb/tag1;",
				Expected: []sql.Row{},
			},
			{
				Query:    "select database();",
				Expected: []sql.Row{{"mydb/tag1"}},
			},
			{
				Query:    "show databases;",
				Expected: []sql.Row{{"mydb"}, {"information_schema"}, {"mydb/tag1"}, {"mysql"}},
			},
			{
				Query:    "select * from t01;",
				Expected: []sql.Row{{1, 1}, {2, 2}},
			},
			{
				Query:          "call dolt_reset();",
				ExpectedErrStr: "unable to reset HEAD in read-only databases",
			},
			{
				Query:    "call dolt_checkout('main');",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "select database();",
				Expected: []sql.Row{{"mydb"}},
			},
			{
				Query:    "select active_branch();",
				Expected: []sql.Row{{"main"}},
			},
			{
				Query:    "use mydb;",
				Expected: []sql.Row{},
			},
			{
				Query:    "select database();",
				Expected: []sql.Row{{"mydb"}},
			},
			{
				Query:    "show databases;",
				Expected: []sql.Row{{"mydb"}, {"information_schema"}, {"mysql"}},
			},
		},
	},
	{
		Name: "database revision specs: branch-qualified revision spec",
		SetUpScript: []string{
			"create table t01 (pk int primary key, c1 int)",
			"call dolt_add('.')",
			"call dolt_commit('-am', 'creating table t01 on main');",
			"insert into t01 values (1, 1), (2, 2);",
			"call dolt_commit('-am', 'adding rows to table t01 on main');",
			"call dolt_branch('branch1');",
			"insert into t01 values (3, 3);",
			"call dolt_commit('-am', 'adding another row to table t01 on main');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "use mydb/branch1;",
				Expected: []sql.Row{},
			},
			{
				Query:    "show databases;",
				Expected: []sql.Row{{"mydb"}, {"information_schema"}, {"mydb/branch1"}, {"mysql"}},
			},
			{
				Query:    "select database();",
				Expected: []sql.Row{{"mydb/branch1"}},
			},
			{
				Query:    "select * from t01",
				Expected: []sql.Row{{1, 1}, {2, 2}},
			},
			{
				Query:    "call dolt_checkout('main');",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "show databases;",
				Expected: []sql.Row{{"mydb"}, {"information_schema"}, {"mysql"}},
			},
			{
				Query:    "select database();",
				Expected: []sql.Row{{"mydb"}},
			},
			{
				Query:    "use mydb/branch1;",
				Expected: []sql.Row{},
			},
			{
				Query:    "call dolt_reset();",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "select database();",
				Expected: []sql.Row{{"mydb/branch1"}},
			},
			{
				Query:    "show databases;",
				Expected: []sql.Row{{"mydb"}, {"information_schema"}, {"mydb/branch1"}, {"mysql"}},
			},
			{
				Query:    "create table working_set_table(pk int primary key);",
				Expected: []sql.Row{{sql.NewOkResult(0)}},
			},
			{
				// Create a table in the working set to verify the main db
				Query:    "select table_name from dolt_diff where commit_hash='WORKING';",
				Expected: []sql.Row{{"working_set_table"}},
			},
			{
				Query:    "use mydb;",
				Expected: []sql.Row{},
			},
			{
				Query:    "select table_name from dolt_diff where commit_hash='WORKING';",
				Expected: []sql.Row{},
			},
			{
				Query:    "call dolt_checkout('branch1');",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "select table_name from dolt_diff where commit_hash='WORKING';",
				Expected: []sql.Row{{"working_set_table"}},
			},
		},
	},
}

// DoltScripts are script tests specific to Dolt (not the engine in general), e.g. by involving Dolt functions. Break
// this slice into others with good names as it grows.
var DoltScripts = []queries.ScriptTest{
	{
		Name: "test null filtering in secondary indexes (https://github.com/dolthub/dolt/issues/4199)",
		SetUpScript: []string{
			"create table t (pk int primary key auto_increment, d datetime, index index1 (d));",
			"insert into t (d) values (NOW()), (NOW());",
			"insert into t (d) values (NULL), (NULL);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select count(*) from t where d is not null",
				Expected: []sql.Row{{2}},
			},
			{
				Query:    "select count(*) from t where d is null",
				Expected: []sql.Row{{2}},
			},
			{
				// Test the null-safe equals operator
				Query:    "select count(*) from t where d <=> NULL",
				Expected: []sql.Row{{2}},
			},
			{
				// Test the null-safe equals operator
				Query:    "select count(*) from t where not(d <=> null)",
				Expected: []sql.Row{{2}},
			},
			{
				// Test an IndexedJoin
				Query:    "select count(ifnull(t.d, 1)) from t, t as t2 where t.d is not null and t.pk = t2.pk and t2.d is not null;",
				Expected: []sql.Row{{2}},
			},
			{
				// Test an IndexedJoin
				Query:    "select count(ifnull(t.d, 1)) from t, t as t2 where t.d is null and t.pk = t2.pk and t2.d is null;",
				Expected: []sql.Row{{2}},
			},
			{
				// Test an IndexedJoin
				Query:    "select count(ifnull(t.d, 1)) from t, t as t2 where t.d is null and t.pk = t2.pk and t2.d is not null;",
				Expected: []sql.Row{{0}},
			},
		},
	},
	{
		Name: "test backticks in index name (https://github.com/dolthub/dolt/issues/3776)",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 int)",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "alter table t add index ```i```(c1);",
				Expected: []sql.Row{{sql.OkResult{}}},
			},
			{
				Query: "show create table t;",
				Expected: []sql.Row{{"t",
					"CREATE TABLE `t` (\n" +
						"  `pk` int NOT NULL,\n" +
						"  `c1` int,\n" +
						"  PRIMARY KEY (`pk`),\n" +
						"  KEY ```i``` (`c1`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "test as of indexed join (https://github.com/dolthub/dolt/issues/2189)",
		SetUpScript: []string{
			"create table a (pk int primary key, c1 int)",
			"call DOLT_ADD('.')",
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
						"  `b` varchar(10) NOT NULL DEFAULT 'abc',\n" +
						"  PRIMARY KEY (`a`),\n" +
						"  KEY `t1b` (`b`),\n" +
						"  CONSTRAINT `ck1` CHECK (`b` LIKE '%abc%')\n" +
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
						"CREATE PROCEDURE dolt_checkout() SELECT 'External stored procedure';",
						"utf8mb4",
						"utf8mb4_0900_bin",
						"utf8mb4_0900_bin",
					},
				},
			},
		},
	},
	{
		Name: "Prepared ASOF",
		SetUpScript: []string{
			"create table test (pk int primary key, c1 int)",
			"call dolt_add('.')",
			"insert into test values (0,0), (1,1);",
			"set @Commit1 = dolt_commit('-am', 'creating table');",
			"call dolt_branch('-c', 'main', 'newb')",
			"alter table test add column c2 int;",
			"set @Commit2 = dolt_commit('-am', 'alter table');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from test as of 'HEAD~' where pk=?;",
				Expected: []sql.Row{{0, 0}},
				Bindings: map[string]sql.Expression{
					"v1": expression.NewLiteral(0, sql.Int8),
				},
			},
			{
				Query:    "select * from test as of hashof('HEAD') where pk=?;",
				Expected: []sql.Row{{1, 1, nil}},
				Bindings: map[string]sql.Expression{
					"v1": expression.NewLiteral(1, sql.Int8),
				},
			},
			{
				Query:    "select * from test as of @Commit1 where pk=?;",
				Expected: []sql.Row{{0, 0}},
				Bindings: map[string]sql.Expression{
					"v1": expression.NewLiteral(0, sql.Int8),
				},
			},
			{
				Query:    "select * from test as of @Commit2 where pk=?;",
				Expected: []sql.Row{{0, 0, nil}},
				Bindings: map[string]sql.Expression{
					"v1": expression.NewLiteral(0, sql.Int8),
				},
			},
		},
	},
	{
		Name: "blame: composite pk ordered output with correct header (bats repro)",
		SetUpScript: []string{
			"CREATE TABLE t(pk varchar(20), val int)",
			"ALTER TABLE t ADD PRIMARY KEY (pk, val)",
			"INSERT INTO t VALUES ('zzz',4),('mult',1),('sub',2),('add',5)",
			"CALL dadd('.');",
			"CALL dcommit('-am', 'add rows');",
			"INSERT INTO t VALUES ('dolt',0),('alt',12),('del',8),('ctl',3)",
			"CALL dcommit('-am', 'add more rows');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT pk, val, message FROM dolt_blame_t",
				Expected: []sql.Row{
					{"add", 5, "add rows"},
					{"alt", 12, "add more rows"},
					{"ctl", 3, "add more rows"},
					{"del", 8, "add more rows"},
					{"dolt", 0, "add more rows"},
					{"mult", 1, "add rows"},
					{"sub", 2, "add rows"},
					{"zzz", 4, "add rows"},
				},
			},
		},
	},
	{
		Name: "Nautobot FOREIGN KEY panic repro",
		SetUpScript: []string{
			"CREATE TABLE `auth_user` (" +
				"	`password` varchar(128) NOT NULL," +
				"	`last_login` datetime," +
				"	`is_superuser` tinyint NOT NULL," +
				"	`username` varchar(150) NOT NULL," +
				"	`first_name` varchar(150) NOT NULL," +
				"	`last_name` varchar(150) NOT NULL," +
				"	`email` varchar(254) NOT NULL," +
				"	`is_staff` tinyint NOT NULL," +
				"	`is_active` tinyint NOT NULL," +
				"	`date_joined` datetime NOT NULL," +
				"	`id` char(32) NOT NULL," +
				"	`config_data` json NOT NULL," +
				"	PRIMARY KEY (`id`)," +
				"	UNIQUE KEY `username` (`username`)" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin",
			"CREATE TABLE `users_token` (" +
				"	`id` char(32) NOT NULL," +
				"	`created` datetime NOT NULL," +
				"	`expires` datetime," +
				"	`key` varchar(40) NOT NULL," +
				"	`write_enabled` tinyint NOT NULL," +
				"	`description` varchar(200) NOT NULL," +
				"	`user_id` char(32) NOT NULL," +
				"	PRIMARY KEY (`id`)," +
				"	UNIQUE KEY `key` (`key`)," +
				"	KEY `users_token_user_id_af964690` (`user_id`)," +
				"	CONSTRAINT `users_token_user_id_af964690_fk_auth_user_id` FOREIGN KEY (`user_id`) REFERENCES `auth_user` (`id`)" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
			"INSERT INTO `auth_user` (`password`,`last_login`,`is_superuser`,`username`,`first_name`,`last_name`,`email`,`is_staff`,`is_active`,`date_joined`,`id`,`config_data`)" +
				"VALUES ('pbkdf2_sha256$216000$KRpZeDPgwc5E$vl/2hwrmtnckaBT0A8pf63Ph+oYuCHYI7qozMTZihTo=',NULL,1,'admin','','','admin@example.com',1,1,'2022-08-30 18:27:21.810049','1056443cc03446c592fa4c06bb06a1a6','{}');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "INSERT INTO `users_token` (`id`, `user_id`, `created`, `expires`, `key`, `write_enabled`, `description`) " +
					"VALUES ('acc2e157db2845a79221cc654b1dcecc', '1056443cc03446c592fa4c06bb06a1a6', '2022-08-30 18:27:21.948487', NULL, '0123456789abcdef0123456789abcdef01234567', 1, '');",
				Expected: []sql.Row{{sql.OkResult{RowsAffected: 0x1, InsertID: 0x0}}},
			},
		},
	},
	{
		Name: "dolt_schemas schema",
		SetUpScript: []string{
			"CREATE TABLE viewtest(v1 int, v2 int)",
			"CREATE VIEW view1 AS SELECT v1 FROM viewtest",
			"CREATE VIEW view2 AS SELECT v2 FROM viewtest",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT type, name, fragment, id FROM dolt_schemas ORDER BY 1, 2",
				Expected: []sql.Row{
					{"view", "view1", "SELECT v1 FROM viewtest", int64(1)},
					{"view", "view2", "SELECT v2 FROM viewtest", int64(2)},
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
		Name: "table function privilege checking",
		SetUpScript: []string{
			"CREATE TABLE mydb.test (pk BIGINT PRIMARY KEY);",
			"CREATE TABLE mydb.test2 (pk BIGINT PRIMARY KEY);",
			"CALL DOLT_ADD('.')",
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
				Query:       "SELECT * FROM dolt_diff('main~', 'main', 'test');",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				// Without access to the database, dolt_diff with dots should fail with a database access error
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM dolt_diff('main~..main', 'test');",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				// Without access to the database, dolt_diff_summary should fail with a database access error
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM dolt_diff_summary('main~', 'main', 'test');",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				// Without access to the database, dolt_diff_summary with dots should fail with a database access error
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM dolt_diff_summary('main~..main', 'test');",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				// Without access to the database, dolt_log should fail with a database access error
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM dolt_log('main');",
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
				Query:    "SELECT COUNT(*) FROM dolt_diff('main~', 'main', 'test');",
				Expected: []sql.Row{{1}},
			},
			{
				// After granting access to mydb.test, dolt_diff with dots should work
				User:     "tester",
				Host:     "localhost",
				Query:    "SELECT COUNT(*) FROM dolt_diff('main~..main', 'test');",
				Expected: []sql.Row{{1}},
			},
			{
				// With access to the db, but not the table, dolt_diff should fail
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM dolt_diff('main~', 'main', 'test2');",
				ExpectedErr: sql.ErrPrivilegeCheckFailed,
			},
			{
				// With access to the db, but not the table, dolt_diff with dots should fail
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM dolt_diff('main~..main', 'test2');",
				ExpectedErr: sql.ErrPrivilegeCheckFailed,
			},
			{
				// With access to the db, but not the table, dolt_diff_summary should fail
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM dolt_diff_summary('main~', 'main', 'test2');",
				ExpectedErr: sql.ErrPrivilegeCheckFailed,
			},
			{
				// With access to the db, but not the table, dolt_diff_summary with dots should fail
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM dolt_diff_summary('main~...main', 'test2');",
				ExpectedErr: sql.ErrPrivilegeCheckFailed,
			},
			{
				// With access to the db, dolt_diff_summary should fail for all tables if no access any of tables
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM dolt_diff_summary('main~', 'main');",
				ExpectedErr: sql.ErrPrivilegeCheckFailed,
			},
			{
				// With access to the db, dolt_diff_summary with dots should fail for all tables if no access any of tables
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM dolt_diff_summary('main~...main');",
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
				Query:       "SELECT * FROM dolt_diff('main~', 'main', 'test');",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				// After revoking access, dolt_diff with dots should fail
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM dolt_diff('main~..main', 'test');",
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
				Query:    "SELECT COUNT(*) FROM dolt_diff('main~', 'main', 'test');",
				Expected: []sql.Row{{1}},
			},
			{
				// After granting access to the entire db, dolt_diff should work
				User:     "tester",
				Host:     "localhost",
				Query:    "SELECT COUNT(*) FROM dolt_diff('main~..main', 'test');",
				Expected: []sql.Row{{1}},
			},
			{
				// After granting access to the entire db, dolt_diff_summary should work
				User:     "tester",
				Host:     "localhost",
				Query:    "SELECT COUNT(*) FROM dolt_diff_summary('main~', 'main');",
				Expected: []sql.Row{{1}},
			},
			{
				// After granting access to the entire db, dolt_diff_summary with dots should work
				User:     "tester",
				Host:     "localhost",
				Query:    "SELECT COUNT(*) FROM dolt_diff_summary('main~...main');",
				Expected: []sql.Row{{1}},
			},
			{
				// After granting access to the entire db, dolt_log should work
				User:     "tester",
				Host:     "localhost",
				Query:    "SELECT COUNT(*) FROM dolt_log('main');",
				Expected: []sql.Row{{4}},
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
				Query:       "SELECT * FROM dolt_diff('main~', 'main', 'test');",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				// After revoking access, dolt_diff with dots should fail
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM dolt_diff('main~...main', 'test');",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				// After revoking access, dolt_diff_summary should fail
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM dolt_diff_summary('main~', 'main', 'test');",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				// After revoking access, dolt_log should fail
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM dolt_log('main');",
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
				Query:    "SELECT COUNT(*) FROM dolt_diff('main~', 'main', 'test');",
				Expected: []sql.Row{{1}},
			},
			{
				// After granting global access to *.*, dolt_diff should work
				User:     "tester",
				Host:     "localhost",
				Query:    "SELECT COUNT(*) FROM dolt_diff('main~...main', 'test');",
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
				Query:       "SELECT * FROM dolt_diff('main~', 'main', 'test');",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				// After revoking global access, dolt_diff with dots should fail
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM dolt_diff('main~..main', 'test');",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
		},
	},
}

// HistorySystemTableScriptTests contains working tests for both prepared and non-prepared
var HistorySystemTableScriptTests = []queries.ScriptTest{
	{
		Name: "empty table",
		SetUpScript: []string{
			"create table t (n int, c varchar(20));",
			"call dolt_add('.')",
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
			"create table foo1 (n int, de varchar(20));",
			"insert into foo1 values (1, 'Ein'), (2, 'Zwei'), (3, 'Drei');",
			"call dolt_add('.')",
			"set @Commit1 = dolt_commit('-am', 'inserting into foo1', '--date', '2022-08-06T12:00:00');",

			"update foo1 set de='Eins' where n=1;",
			"set @Commit2 = dolt_commit('-am', 'updating data in foo1', '--date', '2022-08-06T12:00:01');",

			"insert into foo1 values (4, 'Vier');",
			"set @Commit3 = dolt_commit('-am', 'inserting data in foo1', '--date', '2022-08-06T12:00:02');",
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
			"create table t1 (n int primary key, de varchar(20));",
			"call dolt_add('.')",
			"insert into t1 values (1, 'Eins'), (2, 'Zwei'), (3, 'Drei');",
			"set @Commit1 = dolt_commit('-am', 'inserting into t1', '--date', '2022-08-06T12:00:01');",

			"alter table t1 add column fr varchar(20);",
			"insert into t1 values (4, 'Vier', 'Quatre');",
			"set @Commit2 = dolt_commit('-am', 'adding column and inserting data in t1', '--date', '2022-08-06T12:00:02');",

			"update t1 set fr='Un' where n=1;",
			"update t1 set fr='Deux' where n=2;",
			"set @Commit3 = dolt_commit('-am', 'updating data in t1', '--date', '2022-08-06T12:00:03');",

			"update t1 set de=concat(de, ', meine herren') where n>1;",
			"set @Commit4 = dolt_commit('-am', 'be polite when you address a gentleman', '--date', '2022-08-06T12:00:04');",

			"delete from t1 where n=2;",
			"set @Commit5 = dolt_commit('-am', 'we don''t need the number 2', '--date', '2022-08-06T12:00:05');",
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
			"call dolt_add('.')",
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
					{"Filter(dolt_history_t1.pk = 3)"},
					{" └─ IndexedTableAccess(dolt_history_t1)"},
					{"     ├─ index: [dolt_history_t1.pk]"},
					{"     ├─ filters: [{[3, 3]}]"},
					{"     └─ columns: [pk c]"},
				},
			},
			{
				Query: "explain select pk, c from dolt_history_t1 where pk = 3 and committer = 'someguy'",
				Expected: []sql.Row{
					{"Project"},
					{" ├─ columns: [dolt_history_t1.pk, dolt_history_t1.c]"},
					{" └─ Filter((dolt_history_t1.pk = 3) AND (dolt_history_t1.committer = 'someguy'))"},
					{"     └─ IndexedTableAccess(dolt_history_t1)"},
					{"         ├─ index: [dolt_history_t1.pk]"},
					{"         ├─ filters: [{[3, 3]}]"},
					{"         └─ columns: [pk c committer]"},
				},
			},
		},
	},
	{
		Name: "adding an index",
		SetUpScript: []string{
			"create table t1 (pk int primary key, c int);",
			"call dolt_add('.')",
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
					{"Filter(dolt_history_t1.c = 4)"},
					{" └─ IndexedTableAccess(dolt_history_t1)"},
					{"     ├─ index: [dolt_history_t1.c]"},
					{"     ├─ filters: [{[4, 4]}]"},
					{"     └─ columns: [pk c]"},
				},
			},
			{
				Query: "explain select pk, c from dolt_history_t1 where c = 10 and committer = 'someguy'",
				Expected: []sql.Row{
					{"Project"},
					{" ├─ columns: [dolt_history_t1.pk, dolt_history_t1.c]"},
					{" └─ Filter((dolt_history_t1.c = 10) AND (dolt_history_t1.committer = 'someguy'))"},
					{"     └─ IndexedTableAccess(dolt_history_t1)"},
					{"         ├─ index: [dolt_history_t1.c]"},
					{"         ├─ filters: [{[10, 10]}]"},
					{"         └─ columns: [pk c committer]"},
				},
			},
		},
	},
	{
		Name: "primary key table: non-pk column drops and adds",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 int, c2 varchar(20));",
			"call dolt_add('.')",
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
			"create table t (pk int primary key, c1 int, c2 varchar(20));",
			"call dolt_add('.')",
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
			"create table t (pk int primary key, c1 int, c2 varchar(20));",
			"call dolt_add('.')",
			"insert into t values (1, 2, '3'), (4, 5, '6');",
			"set @Commit1 = DOLT_COMMIT('-am', 'creating table t');",

			"alter table t rename to t2;",
			"call dolt_add('.')",
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
			"create table t (pk int primary key, c1 int, c2 varchar(20));",
			"call dolt_add('.')",
			"insert into t values (1, 2, '3'), (4, 5, '6');",
			"set @Commit1 = DOLT_COMMIT('-am', 'creating table t');",

			"drop table t;",
			"set @Commit2 = DOLT_COMMIT('-am', 'dropping table t');",

			"create table t (pk int primary key, c1 int);",
			"call dolt_add('.')",
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
	{
		Name: "dolt_history table with AS OF",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 int, c2 varchar(20));",
			"call dolt_add('-A');",
			"call dolt_commit('-m', 'creating table t');",
			"insert into t values (1, 2, '3'), (4, 5, '6');",
			"call dolt_commit('-am', 'added values');",
			"insert into t values (11, 22, '3'), (44, 55, '6');",
			"call dolt_commit('-am', 'added values again');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select count(*) from dolt_history_t;",
				Expected: []sql.Row{{6}}, // 2 + 4
			},
			{
				Query:    "select count(*) from dolt_history_t AS OF 'head^';",
				Expected: []sql.Row{{2}}, // 2
			},
			{
				Query: "select message from dolt_log;",
				Expected: []sql.Row{
					{"added values again"},
					{"added values"},
					{"creating table t"},
					{"checkpoint enginetest database mydb"},
					{"Initialize data repository"},
				},
			},
		},
	},
	{
		SkipPrepared: true,
		Name:         "dolt_history table with AS OF",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 int, c2 varchar(20));",
			"call dolt_add('-A');",
			"call dolt_commit('-m', 'creating table t');",
			"insert into t values (1, 2, '3'), (4, 5, '6');",
			"call dolt_commit('-am', 'added values');",
			"insert into t values (11, 22, '3'), (44, 55, '6');",
			"call dolt_commit('-am', 'added values again');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select message from dolt_log AS OF 'head^';",
				Expected: []sql.Row{
					{"added values"},
					{"creating table t"},
					{"checkpoint enginetest database mydb"},
					{"Initialize data repository"},
				},
			},
		},
	},
	{
		Name: "dolt_history table with enums",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 enum('foo','bar'));",
			"call dolt_add('-A');",
			"call dolt_commit('-m', 'creating table t');",
			"insert into t values (1, 'foo');",
			"call dolt_commit('-am', 'added values');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select c1 from dolt_history_t;",
				Expected: []sql.Row{
					{uint64(1)},
				},
			},
		},
	},
	{
		Name: "dolt_history table index lookup",
		SetUpScript: []string{
			"create table yx (y int, x int primary key);",
			"call dolt_add('.');",
			"call dolt_commit('-m', 'creating table');",
			"insert into yx values (0, 1);",
			"call dolt_commit('-am', 'add data');",
			"insert into yx values (2, 3);",
			"call dolt_commit('-am', 'add data');",
			"insert into yx values (4, 5);",
			"call dolt_commit('-am', 'add data');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select count(x) from dolt_history_yx where x = 1;",
				Expected: []sql.Row{
					{3},
				},
			},
		},
	},
}

// BrokenHistorySystemTableScriptTests contains tests that work for non-prepared, but don't work
// for prepared queries.
var BrokenHistorySystemTableScriptTests = []queries.ScriptTest{
	{
		Name: "dolt_history table with AS OF",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 int, c2 varchar(20));",
			"call dolt_add('-A');",
			"call dolt_commit('-m', 'creating table t');",
			"insert into t values (1, 2, '3'), (4, 5, '6');",
			"call dolt_commit('-am', 'added values');",
			"insert into t values (11, 22, '3'), (44, 55, '6');",
			"call dolt_commit('-am', 'added values again');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select message from dolt_log AS OF 'head^';",
				Expected: []sql.Row{
					{"added values"},
					{"creating table t"},
					{"checkpoint enginetest database mydb"},
					{"Initialize data repository"},
				},
			},
		},
	},
}

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
			"call dolt_add('.')",
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
			"CALL DOLT_ADD('.')",
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
			"CALL DOLT_ADD('.')",
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
			"call dolt_add('.')",
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
			"call dolt_add('.')",
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
			"call dolt_add('.')",
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
			"call dolt_add('.')",
			"insert into t values (1, 2), (3, 4);",
			"set @Commit1 = (select DOLT_COMMIT('-am', 'creating table t'));",

			"drop table t;",
			"set @Commit2 = (select DOLT_COMMIT('-am', 'dropping table t'));",

			"create table t (pk int primary key, c int);",
			"call dolt_add('.')",
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
			"call dolt_add('.')",
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
			"call dolt_add('.')",
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
			"call dolt_add('.')",
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
			"call dolt_add('.')",
			"insert into t values (1, 2), (3, 4);",
			"set @Commit1 = (select DOLT_COMMIT('-am', 'creating table t'));",

			"alter table t drop column c;",
			"set @Commit2 = (select DOLT_COMMIT('-am', 'dropping column c'));",

			"alter table t add column c varchar(20);",
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
			"create table t (pk int primary key, c varchar(20));",
			"call dolt_add('.')",
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
			"call dolt_add('.')",
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
			"call dolt_add('.')",
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
			"CREATE TABLE t (pk int PRIMARY KEY, commit varchar(20));",
			"CALL DOLT_ADD('.')",
			"CALL dolt_commit('-am', 'creating table t');",
			"INSERT INTO t VALUES (1, 'hi');",
			"CALL dolt_commit('-am', 'insert data');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT to_pk, char_length(to_commit), from_pk, char_length(from_commit), diff_type from dolt_diff_t;",
				Expected: []sql.Row{{1, 32, nil, 32, "added"}},
			},
		},
	},
	{
		Name: "selecting to_pk columns",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 int, c2 int);",
			"call dolt_add('.')",
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
			"call dolt_add('.')",
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
	{
		Name: "Diff table shows diffs across primary key renames",
		SetUpScript: []string{
			"CREATE TABLE t (pk1 int PRIMARY KEY);",
			"INSERT INTO t values (1);",
			"CREATE table t2 (pk1a int, pk1b int, PRIMARY KEY (pk1a, pk1b));",
			"CALL DOLT_ADD('.')",
			"INSERT INTO t2 values (2, 2);",
			"CALL DOLT_COMMIT('-am', 'initial');",

			"ALTER TABLE t RENAME COLUMN pk1 to pk2",
			"ALTER TABLE t2 RENAME COLUMN pk1a to pk2a",
			"ALTER TABLE t2 RENAME COLUMN pk1b to pk2b",
			"CALL DOLT_COMMIT('-am', 'rename primary key')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT from_pk2, to_pk2, diff_type from dolt_diff_t;",
				Expected: []sql.Row{{nil, 1, "added"}},
			},
			{
				Query:    "SELECT from_pk2a, from_pk2b, to_pk2a, to_pk2b, diff_type from dolt_diff_t2;",
				Expected: []sql.Row{{nil, nil, 2, 2, "added"}},
			},
		},
	},
	{
		Name: "add multiple columns, then set and unset a value. Should not show a diff",
		SetUpScript: []string{
			"CREATE table t (pk int primary key);",
			"Insert into t values (1);",
			"alter table t add column col1 int;",
			"alter table t add column col2 int;",
			"CALL DOLT_ADD('.');",
			"CALL DOLT_COMMIT('-am', 'setup');",
			"UPDATE t set col1 = 1 where pk = 1;",
			"UPDATE t set col1 = null where pk = 1;",
			"CALL DOLT_COMMIT('--allow-empty', '-am', 'fix short tuple');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT to_pk, to_col1, from_pk, from_col1, diff_type from dolt_diff_t;",
				Expected: []sql.Row{{1, nil, nil, nil, "added"}},
			},
		},
	},
}

var Dolt1DiffSystemTableScripts = []queries.ScriptTest{
	{
		Name: "Diff table stops creating diff partitions when any primary key type has changed",
		SetUpScript: []string{
			"CREATE TABLE t (pk1 VARCHAR(100), pk2 VARCHAR(100), PRIMARY KEY (pk1, pk2));",
			"CALL DOLT_ADD('.')",
			"INSERT INTO t VALUES ('1', '1');",
			"CALL DOLT_COMMIT('-am', 'setup');",

			"ALTER TABLE t MODIFY COLUMN pk2 VARCHAR(101)",
			"CALL DOLT_COMMIT('-am', 'modify column type');",

			"INSERT INTO t VALUES ('2', '2');",
			"CALL DOLT_COMMIT('-am', 'insert new row');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT to_pk1, to_pk2, from_pk1, from_pk2, diff_type from dolt_diff_t;",
				Expected: []sql.Row{{"2", "2", nil, nil, "added"}},
			},
		},
	},
}

var DiffTableFunctionScriptTests = []queries.ScriptTest{
	{
		Name: "invalid arguments",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 varchar(20), c2 varchar(20));",
			"call dolt_add('.')",
			"set @Commit1 = dolt_commit('-am', 'creating table t');",

			"insert into t values(1, 'one', 'two'), (2, 'two', 'three');",
			"set @Commit2 = dolt_commit('-am', 'inserting into t');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:       "SELECT * from dolt_diff();",
				ExpectedErr: sql.ErrInvalidArgumentNumber,
			},
			{
				Query:       "SELECT * from dolt_diff('t');",
				ExpectedErr: sql.ErrInvalidArgumentNumber,
			},
			{
				Query:       "SELECT * from dolt_diff(@Commit1, 't');",
				ExpectedErr: sql.ErrInvalidArgumentNumber,
			},
			{
				Query:       "SELECT * from dolt_diff(@Commit1, @Commit2, 'extra', 't');",
				ExpectedErr: sql.ErrInvalidArgumentNumber,
			},
			{
				Query:       "SELECT * from dolt_diff(null, null, null);",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:       "SELECT * from dolt_diff(@Commit1, @Commit2, 123);",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:       "SELECT * from dolt_diff(123, @Commit2, 't');",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:       "SELECT * from dolt_diff(@Commit1, 123, 't');",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:       "SELECT * from dolt_diff(@Commit1, @Commit2, 'doesnotexist');",
				ExpectedErr: sql.ErrTableNotFound,
			},
			{
				Query:          "SELECT * from dolt_diff('fakefakefakefakefakefakefakefake', @Commit2, 't');",
				ExpectedErrStr: "target commit not found",
			},
			{
				Query:          "SELECT * from dolt_diff(@Commit1, 'fake-branch', 't');",
				ExpectedErrStr: "branch not found: fake-branch",
			},
			{
				Query:       "SELECT * from dolt_diff(@Commit1, concat('fake', '-', 'branch'), 't');",
				ExpectedErr: sqle.ErrInvalidNonLiteralArgument,
			},
			{
				Query:       "SELECT * from dolt_diff(hashof('main'), @Commit2, 't');",
				ExpectedErr: sqle.ErrInvalidNonLiteralArgument,
			},
			{
				Query:       "SELECT * from dolt_diff(hashof('main'), @Commit2, LOWER('T'));",
				ExpectedErr: sqle.ErrInvalidNonLiteralArgument,
			},

			{
				Query:       "SELECT * from dolt_diff('main..main~');",
				ExpectedErr: sql.ErrInvalidArgumentNumber,
			},
			{
				Query:       "SELECT * from dolt_diff('main..main~', 'extra', 't');",
				ExpectedErr: sql.ErrInvalidArgumentNumber,
			},
			{
				Query:       "SELECT * from dolt_diff('main..main^', 123);",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:       "SELECT * from dolt_diff('main..main~', 'doesnotexist');",
				ExpectedErr: sql.ErrTableNotFound,
			},
			{
				Query:          "SELECT * from dolt_diff('fakefakefakefakefakefakefakefake..main', 't');",
				ExpectedErrStr: "target commit not found",
			},
			{
				Query:          "SELECT * from dolt_diff('main..fakefakefakefakefakefakefakefake', 't');",
				ExpectedErrStr: "target commit not found",
			},
			{
				Query:          "SELECT * from dolt_diff('fakefakefakefakefakefakefakefake...main', 't');",
				ExpectedErrStr: "target commit not found",
			},
			{
				Query:          "SELECT * from dolt_diff('main...fakefakefakefakefakefakefakefake', 't');",
				ExpectedErrStr: "target commit not found",
			},
			{
				Query:       "SELECT * from dolt_diff('main..main~', LOWER('T'));",
				ExpectedErr: sqle.ErrInvalidNonLiteralArgument,
			},
		},
	},
	{
		Name: "basic case",
		SetUpScript: []string{
			"set @Commit0 = HashOf('HEAD');",

			"create table t (pk int primary key, c1 varchar(20), c2 varchar(20));",
			"call dolt_add('.')",
			"set @Commit1 = dolt_commit('-am', 'creating table t');",

			"insert into t values(1, 'one', 'two');",
			"set @Commit2 = dolt_commit('-am', 'inserting into table t');",

			"create table t2 (pk int primary key, c1 varchar(20), c2 varchar(20));",
			"call dolt_add('.')",
			"insert into t2 values(100, 'hundred', 'hundert');",
			"set @Commit3 = dolt_commit('-am', 'inserting into table t2');",

			"insert into t values(2, 'two', 'three'), (3, 'three', 'four');",
			"update t set c1='uno', c2='dos' where pk=1;",
			"set @Commit4 = dolt_commit('-am', 'inserting into table t');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type from dolt_diff(@Commit1, @Commit2, 't');",
				Expected: []sql.Row{{1, "one", "two", nil, nil, nil, "added"}},
			},
			{
				Query:    "SELECT COUNT(*) from dolt_diff(@Commit2, @Commit3, 't');",
				Expected: []sql.Row{{0}},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type from dolt_diff(@Commit3, @Commit4, 't');",
				Expected: []sql.Row{
					{1, "uno", "dos", 1, "one", "two", "modified"},
					{2, "two", "three", nil, nil, nil, "added"},
					{3, "three", "four", nil, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type from dolt_diff(@Commit4, @Commit3, 't');",
				Expected: []sql.Row{
					{1, "one", "two", 1, "uno", "dos", "modified"},
					{nil, nil, nil, 2, "two", "three", "removed"},
					{nil, nil, nil, 3, "three", "four", "removed"},
				},
			},
			{
				// Table t2 had no changes between Commit3 and Commit4, so results should be empty
				Query:    "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type  from dolt_diff(@Commit3, @Commit4, 'T2');",
				Expected: []sql.Row{},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type  from dolt_diff(@Commit1, @Commit4, 't');",
				Expected: []sql.Row{
					{1, "uno", "dos", nil, nil, nil, "added"},
					{2, "two", "three", nil, nil, nil, "added"},
					{3, "three", "four", nil, nil, nil, "added"},
				},
			},
			{
				// Reverse the to/from commits to see the diff from the other direction
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type  from dolt_diff(@Commit4, @Commit1, 'T');",
				Expected: []sql.Row{
					{nil, nil, nil, 1, "uno", "dos", "removed"},
					{nil, nil, nil, 2, "two", "three", "removed"},
					{nil, nil, nil, 3, "three", "four", "removed"},
				},
			},
			{
				Query: `
SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type
from dolt_diff(@Commit1, @Commit2, 't')
inner join t on to_pk = t.pk;`,
				Expected: []sql.Row{{1, "one", "two", nil, nil, nil, "added"}},
			},
		},
	},
	{
		Name: "WORKING and STAGED",
		SetUpScript: []string{
			"set @Commit0 = HashOf('HEAD');",

			"create table t (pk int primary key, c1 text, c2 text);",
			"call dolt_add('.')",
			"insert into t values (1, 'one', 'two'), (2, 'three', 'four');",
			"set @Commit1 = dolt_commit('-am', 'inserting two rows into table t');",

			"insert into t values (3, 'five', 'six');",
			"delete from t where pk = 2",
			"update t set c2 = '100' where pk = 1",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT from_pk, from_c1, from_c2, to_pk, to_c1, to_c2, diff_type from dolt_diff(@Commit1, 'WORKING', 't') order by coalesce(from_pk, to_pk)",
				Expected: []sql.Row{
					{1, "one", "two", 1, "one", "100", "modified"},
					{2, "three", "four", nil, nil, nil, "removed"},
					{nil, nil, nil, 3, "five", "six", "added"},
				},
			},
			{
				Query: "SELECT from_pk, from_c1, from_c2, to_pk, to_c1, to_c2, diff_type from dolt_diff('STAGED', 'WORKING', 't') order by coalesce(from_pk, to_pk);",
				Expected: []sql.Row{
					{1, "one", "two", 1, "one", "100", "modified"},
					{2, "three", "four", nil, nil, nil, "removed"},
					{nil, nil, nil, 3, "five", "six", "added"},
				},
			},
			{
				Query: "SELECT from_pk, from_c1, from_c2, to_pk, to_c1, to_c2, diff_type from dolt_diff('STAGED..WORKING', 't') order by coalesce(from_pk, to_pk);",
				Expected: []sql.Row{
					{1, "one", "two", 1, "one", "100", "modified"},
					{2, "three", "four", nil, nil, nil, "removed"},
					{nil, nil, nil, 3, "five", "six", "added"},
				},
			},
			{
				Query: "SELECT from_pk, from_c1, from_c2, to_pk, to_c1, to_c2, diff_type from dolt_diff('WORKING', 'STAGED', 't') order by coalesce(from_pk, to_pk);",
				Expected: []sql.Row{
					{1, "one", "100", 1, "one", "two", "modified"},
					{nil, nil, nil, 2, "three", "four", "added"},
					{3, "five", "six", nil, nil, nil, "removed"},
				},
			},
			{
				Query:    "SELECT from_pk, from_c1, from_c2, to_pk, to_c1, to_c2, diff_type from dolt_diff('WORKING', 'WORKING', 't') order by coalesce(from_pk, to_pk);",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT from_pk, from_c1, from_c2, to_pk, to_c1, to_c2, diff_type from dolt_diff('WORKING..WORKING', 't') order by coalesce(from_pk, to_pk);",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT from_pk, from_c1, from_c2, to_pk, to_c1, to_c2, diff_type from dolt_diff('STAGED', 'STAGED', 't') order by coalesce(from_pk, to_pk);",
				Expected: []sql.Row{},
			},
			{
				Query:            "call dolt_add('.')",
				SkipResultsCheck: true,
			},
			{
				Query:    "SELECT from_pk, from_c1, from_c2, to_pk, to_c1, to_c2, diff_type from dolt_diff('WORKING', 'STAGED', 't') order by coalesce(from_pk, to_pk);",
				Expected: []sql.Row{},
			},
			{
				Query: "SELECT from_pk, from_c1, from_c2, to_pk, to_c1, to_c2, diff_type from dolt_diff('HEAD', 'STAGED', 't') order by coalesce(from_pk, to_pk);",
				Expected: []sql.Row{
					{1, "one", "two", 1, "one", "100", "modified"},
					{2, "three", "four", nil, nil, nil, "removed"},
					{nil, nil, nil, 3, "five", "six", "added"},
				},
			},
		},
	},
	{
		Name: "diff with branch refs",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 varchar(20), c2 varchar(20));",
			"call dolt_add('.')",
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
				Query: "SELECT to_pk, to_c1, from_pk, from_c1, from_c2, diff_type from dolt_diff('main', 'branch1', 't');",
				Expected: []sql.Row{
					{nil, nil, 1, "one", "two", "removed"},
					{2, "two", 2, "two", "three", "modified"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, from_pk, from_c1, from_c2, diff_type from dolt_diff('main..branch1', 't');",
				Expected: []sql.Row{
					{nil, nil, 1, "one", "two", "removed"},
					{2, "two", 2, "two", "three", "modified"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, diff_type from dolt_diff('branch1', 'main', 't');",
				Expected: []sql.Row{
					{1, "one", "two", nil, nil, "added"},
					{2, "two", "three", 2, "two", "modified"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, diff_type from dolt_diff('branch1..main', 't');",
				Expected: []sql.Row{
					{1, "one", "two", nil, nil, "added"},
					{2, "two", "three", 2, "two", "modified"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, from_pk, from_c1, from_c2, diff_type from dolt_diff('main~', 'branch1', 't');",
				Expected: []sql.Row{
					{nil, nil, 1, "one", "two", "removed"},
					{2, "two", nil, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, from_pk, from_c1, from_c2, diff_type from dolt_diff('main~..branch1', 't');",
				Expected: []sql.Row{
					{nil, nil, 1, "one", "two", "removed"},
					{2, "two", nil, nil, nil, "added"},
				},
			},

			// Three dot
			{
				Query: "SELECT to_pk, to_c1, from_pk, from_c1, from_c2, diff_type from dolt_diff('main...branch1', 't');",
				Expected: []sql.Row{
					{nil, nil, 1, "one", "two", "removed"},
					{2, "two", nil, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, diff_type from dolt_diff('branch1...main', 't');",
				Expected: []sql.Row{
					{2, "two", "three", nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, from_pk, from_c1, from_c2, diff_type from dolt_diff('main~...branch1', 't');",
				Expected: []sql.Row{
					{nil, nil, 1, "one", "two", "removed"},
					{2, "two", nil, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, from_pk, from_c1, from_c2, diff_type from dolt_diff('main...branch1~', 't');",
				Expected: []sql.Row{
					{nil, nil, 1, "one", "two", "removed"},
				},
			},
		},
	},
	{
		Name: "schema modification: drop and recreate column with same type",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 varchar(20), c2 varchar(20));",
			"call dolt_add('.')",
			"set @Commit1 = dolt_commit('-am', 'creating table t');",

			"insert into t values(1, 'one', 'two'), (2, 'two', 'three');",
			"set @Commit2 = dolt_commit('-am', 'inserting into t');",

			"alter table t drop column c2;",
			"set @Commit3 = dolt_commit('-am', 'dropping column c2');",

			"alter table t add column c2 varchar(20);",
			"insert into t values (3, 'three', 'four');",
			"update t set c2='foo' where pk=1;",
			"set @Commit4 = dolt_commit('-am', 'adding column c2, inserting, and updating data');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type from dolt_diff(@Commit1, @Commit2, 't');",
				Expected: []sql.Row{
					{1, "one", "two", nil, nil, nil, "added"},
					{2, "two", "three", nil, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, from_pk, from_c1, from_c2, diff_type from dolt_diff(@Commit2, @Commit3, 't');",
				Expected: []sql.Row{
					{1, "one", 1, "one", "two", "modified"},
					{2, "two", 2, "two", "three", "modified"},
				},
			},
			{
				Query:       "SELECT to_c2 from dolt_diff(@Commit2, @Commit3, 't');",
				ExpectedErr: sql.ErrColumnNotFound,
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, diff_type from dolt_diff(@Commit3, @Commit4, 't');",
				Expected: []sql.Row{
					{1, "one", "foo", 1, "one", "modified"},
					// This row doesn't show up as changed because adding a column doesn't touch the row data.
					//{2, "two", nil, 2, "two", "modified"},
					{3, "three", "four", nil, nil, "added"},
				},
			},
			{
				Query:       "SELECT from_c2 from dolt_diff(@Commit3, @Commit4, 't');",
				ExpectedErr: sql.ErrColumnNotFound,
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type from dolt_diff(@Commit1, @Commit4, 't');",
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
			"create table t (pk int primary key, c1 varchar(20), c2 int);",
			"call dolt_add('.')",
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
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type from dolt_diff(@Commit1, @Commit2, 't');",
				Expected: []sql.Row{
					{1, "one", -1, nil, nil, nil, "added"},
					{2, "two", -2, nil, nil, nil, "added"},
				},
			},
			{
				Query:       "SELECT to_c2 from dolt_diff(@Commit2, @Commit3, 't');",
				ExpectedErr: sql.ErrColumnNotFound,
			},
			{
				Query:    "SELECT to_pk, to_c1, to_c3, from_pk, from_c1, from_c2, diff_type from dolt_diff(@Commit2, @Commit3, 't');",
				Expected: []sql.Row{},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c3, from_pk, from_c1, from_c3, diff_type from dolt_diff(@Commit3, @Commit4, 't');",
				Expected: []sql.Row{
					{3, "three", -3, nil, nil, nil, "added"},
					{1, "one", 1, 1, "one", -1, "modified"},
				},
			},
			{
				Query:       "SELECT from_c2 from dolt_diff(@Commit4, @Commit5, 't');",
				ExpectedErr: sql.ErrColumnNotFound,
			},
			{
				Query:       "SELECT to_c3 from dolt_diff(@Commit4, @Commit5, 't');",
				ExpectedErr: sql.ErrColumnNotFound,
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c3, diff_type from dolt_diff(@Commit4, @Commit5, 't');",
				Expected: []sql.Row{
					{4, "four", -4, nil, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type from dolt_diff(@Commit1, @Commit5, 't');",
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
			"create table t (pk int primary key, c1 varchar(20), c2 varchar(20));",
			"call dolt_add('.')",
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
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type from dolt_diff(@Commit1, @Commit2, 't');",
				Expected: []sql.Row{
					{1, "one", "asdf", nil, nil, nil, "added"},
					{2, "two", "2", nil, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, from_pk, from_c1, from_c2, diff_type from dolt_diff(@Commit2, @Commit3, 't');",
				Expected: []sql.Row{
					{1, "one", 1, "one", "asdf", "modified"},
					{2, "two", 2, "two", "2", "modified"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, from_pk, from_c1, diff_type from dolt_diff(@Commit3, @Commit4, 't');",
				Expected: []sql.Row{
					{3, "three", nil, nil, "added"},
					{1, "fdsa", 1, "one", "modified"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, diff_type from dolt_diff(@Commit4, @Commit5, 't');",
				Expected: []sql.Row{
					{4, "four", -4, nil, nil, "added"},
				},
			},
			{
				Query: "SELECT to_pk, to_c1, to_c2, from_pk, from_c1, from_c2, diff_type from dolt_diff(@Commit1, @Commit5, 't');",
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
		Name: "new table",
		SetUpScript: []string{
			"create table t1 (a int primary key, b int)",
			"insert into t1 values (1,2)",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select to_a, to_b, from_commit, to_commit, diff_type from dolt_diff('HEAD', 'WORKING', 't1')",
				Expected: []sql.Row{{1, 2, "HEAD", "WORKING", "added"}},
			},
			{
				Query:       "select to_a, from_b, from_commit, to_commit, diff_type from dolt_diff('HEAD', 'WORKING', 't1')",
				ExpectedErr: sql.ErrColumnNotFound,
			},
			{
				Query:    "select from_a, from_b, from_commit, to_commit, diff_type from dolt_diff('WORKING', 'HEAD', 't1')",
				Expected: []sql.Row{{1, 2, "WORKING", "HEAD", "removed"}},
			},
		},
	},
	{
		Name: "dropped table",
		SetUpScript: []string{
			"create table t1 (a int primary key, b int)",
			"call dolt_add('.')",
			"insert into t1 values (1,2)",
			"call dolt_commit('-am', 'new table')",
			"drop table t1",
			"call dolt_commit('-am', 'dropped table')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select from_a, from_b, from_commit, to_commit, diff_type from dolt_diff('HEAD~', 'HEAD', 't1')",
				Expected: []sql.Row{{1, 2, "HEAD~", "HEAD", "removed"}},
			},
			{
				Query:    "select from_a, from_b, from_commit, to_commit, diff_type from dolt_diff('HEAD~..HEAD', 't1')",
				Expected: []sql.Row{{1, 2, "HEAD~", "HEAD", "removed"}},
			},
		},
	},
	{
		Name: "renamed table",
		SetUpScript: []string{
			"create table t1 (a int primary key, b int)",
			"call dolt_add('.')",
			"insert into t1 values (1,2)",
			"call dolt_commit('-am', 'new table')",
			"alter table t1 rename to t2",
			"call dolt_add('.')",
			"insert into t2 values (3,4)",
			"call dolt_commit('-am', 'renamed table')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select to_a, to_b, from_commit, to_commit, diff_type from dolt_diff('HEAD~', 'HEAD', 't2')",
				Expected: []sql.Row{{3, 4, "HEAD~", "HEAD", "added"}},
			},
			{
				Query:    "select to_a, to_b, from_commit, to_commit, diff_type from dolt_diff('HEAD~..HEAD', 't2')",
				Expected: []sql.Row{{3, 4, "HEAD~", "HEAD", "added"}},
			},
			{
				// Maybe confusing? We match the old table name as well
				Query:    "select to_a, to_b, from_commit, to_commit, diff_type from dolt_diff('HEAD~', 'HEAD', 't1')",
				Expected: []sql.Row{{3, 4, "HEAD~", "HEAD", "added"}},
			},
		},
	},
	{
		Name: "Renaming a primary key column shows PK values in both the to and from columns",
		SetUpScript: []string{
			"CREATE TABLE t1 (pk int PRIMARY KEY, col1 int);",
			"INSERT INTO t1 VALUES (1, 1);",
			"CREATE TABLE t2 (pk1a int, pk1b int, col1 int, PRIMARY KEY (pk1a, pk1b));",
			"INSERT INTO t2 VALUES (1, 1, 1);",
			"CALL DOLT_ADD('.')",
			"CALL DOLT_COMMIT('-am', 'initial');",

			"ALTER TABLE t1 RENAME COLUMN pk to pk2;",
			"UPDATE t1 set col1 = 100;",
			"ALTER TABLE t2 RENAME COLUMN pk1a to pk2a;",
			"ALTER TABLE t2 RENAME COLUMN pk1b to pk2b;",
			"UPDATE t2 set col1 = 100;",
			"CALL DOLT_COMMIT('-am', 'edit');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select to_pk2, to_col1, from_pk, from_col1, diff_type from dolt_diff('HEAD~', 'HEAD', 't1')",
				Expected: []sql.Row{{1, 100, 1, 1, "modified"}},
			},
			{
				Query:    "select to_pk2, to_col1, from_pk, from_col1, diff_type from dolt_diff('HEAD~..HEAD', 't1')",
				Expected: []sql.Row{{1, 100, 1, 1, "modified"}},
			},
			{
				Query:    "select to_pk2a, to_pk2b, to_col1, from_pk1a, from_pk1b, from_col1, diff_type from dolt_diff('HEAD~', 'HEAD', 't2');",
				Expected: []sql.Row{{1, 1, 100, 1, 1, 1, "modified"}},
			},
			{
				Query:    "select to_pk2a, to_pk2b, to_col1, from_pk1a, from_pk1b, from_col1, diff_type from dolt_diff('HEAD~..HEAD', 't2');",
				Expected: []sql.Row{{1, 1, 100, 1, 1, 1, "modified"}},
			},
		},
	},
}

var LogTableFunctionScriptTests = []queries.ScriptTest{
	{
		Name: "invalid arguments",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 varchar(20), c2 varchar(20));",
			"call dolt_add('.')",
			"set @Commit1 = dolt_commit('-am', 'creating table t');",

			"insert into t values(1, 'one', 'two'), (2, 'two', 'three');",
			"set @Commit2 = dolt_commit('-am', 'inserting into t');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:       "SELECT * from dolt_log(@Commit1, @Commit2, 't');",
				ExpectedErr: sql.ErrInvalidArgumentNumber,
			},
			{
				Query:       "SELECT * from dolt_log(null);",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:       "SELECT * from dolt_log(null, null);",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:       "SELECT * from dolt_log(null, '--not', null);",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:       "SELECT * from dolt_log(@Commit1, '--not', null);",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:       "SELECT * from dolt_log(@Commit1, '--min-parents', null);",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:       "SELECT * from dolt_log(@Commit1, '--min-parents', 123);",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:       "SELECT * from dolt_log(123, @Commit1);",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:       "SELECT * from dolt_log(@Commit1, 123);",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:       "SELECT * from dolt_log(@Commit1, '--not', 123);",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:       "SELECT * from dolt_log('main..branch1', @Commit1);",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:       "SELECT * from dolt_log('^main..branch1');",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:       "SELECT * from dolt_log('^main...branch1');",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:       "SELECT * from dolt_log(@Commit1, 'main..branch1');",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:       "SELECT * from dolt_log(@Commit1, 'main...branch1');",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:       "SELECT * from dolt_log('main..branch1', '--not', @Commit1);",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:       "SELECT * from dolt_log('main...branch1', '--not', @Commit1);",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:       "SELECT * from dolt_log('^main', '--not', @Commit1);",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:       "SELECT * from dolt_log('main', '--not', '^branch1');",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:       "SELECT * from dolt_log('main', '--not', 'main..branch1');",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:       "SELECT * from dolt_log('^main', @Commit2, '--not', @Commit1);",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:       "SELECT * from dolt_log(@Commit1, @Commit2);",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:       "SELECT * from dolt_log('^main', '^branch1');",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:       "SELECT * from dolt_log('^main');",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:          "SELECT * from dolt_log('fake-branch');",
				ExpectedErrStr: "branch not found: fake-branch",
			},
			{
				Query:          "SELECT * from dolt_log('^fake-branch', 'main');",
				ExpectedErrStr: "branch not found: fake-branch",
			},
			{
				Query:          "SELECT * from dolt_log('fake-branch', '^main');",
				ExpectedErrStr: "branch not found: fake-branch",
			},
			{
				Query:          "SELECT * from dolt_log('main..fake-branch');",
				ExpectedErrStr: "branch not found: fake-branch",
			},
			{
				Query:          "SELECT * from dolt_log('main', '--not', 'fake-branch');",
				ExpectedErrStr: "branch not found: fake-branch",
			},
			{
				Query:       "SELECT * from dolt_log(concat('fake', '-', 'branch'));",
				ExpectedErr: sqle.ErrInvalidNonLiteralArgument,
			},
			{
				Query:       "SELECT * from dolt_log(hashof('main'));",
				ExpectedErr: sqle.ErrInvalidNonLiteralArgument,
			},
			{
				Query:       "SELECT * from dolt_log(@Commit3, '--not', hashof('main'));",
				ExpectedErr: sqle.ErrInvalidNonLiteralArgument,
			},
			{
				Query:       "SELECT * from dolt_log(@Commit1, LOWER(@Commit2));",
				ExpectedErr: sqle.ErrInvalidNonLiteralArgument,
			},
			{
				Query:          "SELECT parents from dolt_log();",
				ExpectedErrStr: `column "parents" could not be found in any table in scope`,
			},
			{
				Query:       "SELECT * from dolt_log('--decorate', 'invalid');",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:       "SELECT * from dolt_log('--decorate', 123);",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:       "SELECT * from dolt_log('--decorate', null);",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:          "SELECT refs from dolt_log();",
				ExpectedErrStr: `column "refs" could not be found in any table in scope`,
			},
			{
				Query:          "SELECT refs from dolt_log('--decorate', 'auto');",
				ExpectedErrStr: `column "refs" could not be found in any table in scope`,
			},
			{
				Query:          "SELECT refs from dolt_log('--decorate', 'no');",
				ExpectedErrStr: `column "refs" could not be found in any table in scope`,
			},
		},
	},
	{
		Name: "basic case with one revision",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 varchar(20), c2 varchar(20));",
			"call dolt_add('.')",
			"set @Commit1 = dolt_commit('-am', 'creating table t');",

			"insert into t values(1, 'one', 'two'), (2, 'two', 'three');",
			"set @Commit2 = dolt_commit('-am', 'inserting into t');",

			"call dolt_checkout('-b', 'new-branch')",
			"insert into t values (3, 'three', 'four');",
			"set @Commit3 = dolt_commit('-am', 'inserting into t again');",
			"call dolt_checkout('main')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT count(*) from dolt_log();",
				Expected: []sql.Row{{4}},
			},
			{
				Query:    "SELECT count(*) from dolt_log('main');",
				Expected: []sql.Row{{4}},
			},
			{
				Query:    "SELECT count(*) from dolt_log(@Commit1);",
				Expected: []sql.Row{{3}},
			},
			{
				Query:    "SELECT count(*) from dolt_log(@Commit2);",
				Expected: []sql.Row{{4}},
			},
			{
				Query:    "SELECT count(*) from dolt_log(@Commit3);",
				Expected: []sql.Row{{5}},
			},
			{
				Query:    "SELECT count(*) from dolt_log('new-branch');",
				Expected: []sql.Row{{5}},
			},
			{
				Query:    "SELECT count(*) from dolt_log('main^');",
				Expected: []sql.Row{{3}},
			},
			{
				Query:    "SELECT count(*)	 from dolt_log('main') join dolt_diff(@Commit1, @Commit2, 't') where commit_hash = to_commit;",
				Expected: []sql.Row{{2}},
			},
		},
	},
	{
		Name: "basic case with more than one revision or revision range",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 varchar(20), c2 varchar(20));",
			"call dolt_add('.');",
			"set @Commit1 = dolt_commit('-am', 'creating table t');",

			"insert into t values(1, 'one', 'two'), (2, 'two', 'three');",
			"set @Commit2 = dolt_commit('-am', 'inserting into t 2');",

			"call dolt_checkout('-b', 'new-branch');",
			"insert into t values (3, 'three', 'four');",
			"set @Commit3 = dolt_commit('-am', 'inserting into t 3');",
			"insert into t values (4, 'four', 'five');",
			"set @Commit4 = dolt_commit('-am', 'inserting into t 4');",

			"call dolt_checkout('main');",
			"insert into t values (5, 'five', 'six');",
			"set @Commit5 = dolt_commit('-am', 'inserting into t 5');",
		},
		/* Commit graph:
		          3 - 4 (new-branch)
		         /
		0 - 1 - 2 - 5 (main)
		*/
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT count(*) from dolt_log('^main', 'new-branch');",
				Expected: []sql.Row{{2}}, // 4, 3
			},
			{
				Query:    "SELECT count(*) from dolt_log('main..new-branch');",
				Expected: []sql.Row{{2}}, // 4, 3
			},
			{
				Query:    "SELECT count(*) from dolt_log('main...new-branch');",
				Expected: []sql.Row{{3}}, // 5, 4, 3
			},
			{
				Query:    "SELECT count(*) from dolt_log('new-branch', '--not', 'main');",
				Expected: []sql.Row{{2}}, // 4, 3
			},
			{
				Query:    "SELECT count(*) from dolt_log('new-branch', '^main');",
				Expected: []sql.Row{{2}}, // 4, 3
			},
			{
				Query:    "SELECT count(*) from dolt_log('^new-branch', 'main');",
				Expected: []sql.Row{{1}}, // 5
			},
			{
				Query:    "SELECT count(*) from dolt_log('main', '--not', 'new-branch');",
				Expected: []sql.Row{{1}}, // 5
			},
			{
				Query:    "SELECT count(*) from dolt_log('^main', 'main');",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "SELECT count(*) from dolt_log('main..main');",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "SELECT count(*) from dolt_log('main...main');",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "SELECT count(*) from dolt_log('main', '--not', 'main');",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "SELECT count(*) from dolt_log('^main~', 'main');",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT count(*) from dolt_log('^main^', 'main');",
				Expected: []sql.Row{{1}}, // 5
			},
			{
				Query:    "SELECT count(*) from dolt_log('^main', 'main^');",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "SELECT count(*) from dolt_log('^main', @Commit3);",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT count(*) from dolt_log('^new-branch', @Commit5);",
				Expected: []sql.Row{{1}}, // 5
			},
			{
				Query:    "SELECT count(*) from dolt_log(@Commit3, '--not', @Commit2);",
				Expected: []sql.Row{{1}}, // 3
			},
			{
				Query:    "SELECT count(*) from dolt_log(@Commit4, '--not', @Commit2);",
				Expected: []sql.Row{{2}}, // 4, 3
			},
		},
	},
	{
		Name: "basic case with one revision, row content",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 varchar(20), c2 varchar(20));",
			"call dolt_add('.')",
			"set @Commit1 = dolt_commit('-am', 'creating table t');",

			"insert into t values(1, 'one', 'two'), (2, 'two', 'three');",
			"set @Commit2 = dolt_commit('-am', 'inserting into t');",

			"call dolt_checkout('-b', 'new-branch')",
			"insert into t values (3, 'three', 'four');",
			"set @Commit3 = dolt_commit('-am', 'inserting into t again', '--author', 'John Doe <johndoe@example.com>');",
			"call dolt_checkout('main')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT commit_hash = @Commit2, commit_hash = @Commit1, committer, email, message from dolt_log();",
				Expected: []sql.Row{
					{true, false, "billy bob", "bigbillieb@fake.horse", "inserting into t"},
					{false, true, "billy bob", "bigbillieb@fake.horse", "creating table t"},
					{false, false, "billy bob", "bigbillieb@fake.horse", "checkpoint enginetest database mydb"},
					{false, false, "billy bob", "bigbillieb@fake.horse", "Initialize data repository"},
				},
			},
			{
				Query:    "SELECT commit_hash = @Commit2, committer, email, message from dolt_log('main') limit 1;",
				Expected: []sql.Row{{true, "billy bob", "bigbillieb@fake.horse", "inserting into t"}},
			},
			{
				Query:    "SELECT commit_hash = @Commit3, committer, email, message from dolt_log('new-branch') limit 1;",
				Expected: []sql.Row{{true, "John Doe", "johndoe@example.com", "inserting into t again"}},
			},
			{
				Query:    "SELECT commit_hash = @Commit1, committer, email, message from dolt_log(@Commit1) limit 1;",
				Expected: []sql.Row{{true, "billy bob", "bigbillieb@fake.horse", "creating table t"}},
			},
		},
	},
	{
		Name: "basic case with more than one revision or revision range, row content",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 varchar(20), c2 varchar(20));",
			"call dolt_add('.');",
			"set @Commit1 = dolt_commit('-am', 'creating table t');",

			"insert into t values(1, 'one', 'two'), (2, 'two', 'three');",
			"set @Commit2 = dolt_commit('-am', 'inserting into t 2');",

			"call dolt_checkout('-b', 'new-branch');",
			"insert into t values (3, 'three', 'four');",
			"set @Commit3 = dolt_commit('-am', 'inserting into t 3', '--author', 'John Doe <johndoe@example.com>');",
			"insert into t values (4, 'four', 'five');",
			"set @Commit4 = dolt_commit('-am', 'inserting into t 4', '--author', 'John Doe <johndoe@example.com>');",

			"call dolt_checkout('main');",
			"insert into t values (5, 'five', 'six');",
			"set @Commit5 = dolt_commit('-am', 'inserting into t 5');",
		},
		/* Commit graph:
		          3 - 4 (new-branch)
		         /
		0 - 1 - 2 - 5 (main)
		*/
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT commit_hash = @Commit4, commit_hash = @Commit3, committer, email, message from dolt_log('^main', 'new-branch');",
				Expected: []sql.Row{
					{true, false, "John Doe", "johndoe@example.com", "inserting into t 4"},
					{false, true, "John Doe", "johndoe@example.com", "inserting into t 3"},
				},
			},
			{
				Query: "SELECT commit_hash = @Commit4, commit_hash = @Commit3, committer, email, message from dolt_log('main..new-branch');",
				Expected: []sql.Row{
					{true, false, "John Doe", "johndoe@example.com", "inserting into t 4"},
					{false, true, "John Doe", "johndoe@example.com", "inserting into t 3"},
				},
			},
			{
				Query: "SELECT commit_hash = @Commit5, commit_hash = @Commit4, commit_hash = @Commit3, committer, email, message from dolt_log('main...new-branch');",
				Expected: []sql.Row{
					{true, false, false, "billy bob", "bigbillieb@fake.horse", "inserting into t 5"},
					{false, true, false, "John Doe", "johndoe@example.com", "inserting into t 4"},
					{false, false, true, "John Doe", "johndoe@example.com", "inserting into t 3"},
				},
			},
			{
				Query: "SELECT commit_hash = @Commit4, commit_hash = @Commit3, committer, email, message from dolt_log('new-branch', '--not', 'main');",
				Expected: []sql.Row{
					{true, false, "John Doe", "johndoe@example.com", "inserting into t 4"},
					{false, true, "John Doe", "johndoe@example.com", "inserting into t 3"},
				},
			},
			{
				Query: "SELECT commit_hash = @Commit4, commit_hash = @Commit3, committer, email, message from dolt_log('new-branch', '^main');",
				Expected: []sql.Row{
					{true, false, "John Doe", "johndoe@example.com", "inserting into t 4"},
					{false, true, "John Doe", "johndoe@example.com", "inserting into t 3"},
				},
			},
			{
				Query:    "SELECT commit_hash = @Commit5, committer, email, message from dolt_log('^new-branch', 'main');",
				Expected: []sql.Row{{true, "billy bob", "bigbillieb@fake.horse", "inserting into t 5"}},
			},
			{
				Query:    "SELECT * from dolt_log('^main', 'main');",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT commit_hash = @Commit5, committer, email, message from dolt_log('^main~', 'main');",
				Expected: []sql.Row{{true, "billy bob", "bigbillieb@fake.horse", "inserting into t 5"}},
			},
			{
				Query:    "SELECT commit_hash = @Commit5, committer, email, message from dolt_log( 'main', '--not', 'main~');",
				Expected: []sql.Row{{true, "billy bob", "bigbillieb@fake.horse", "inserting into t 5"}},
			},
			{
				Query:    "SELECT commit_hash = @Commit3, committer, email, message from dolt_log('^main', @Commit3);",
				Expected: []sql.Row{{true, "John Doe", "johndoe@example.com", "inserting into t 3"}},
			},
			{
				Query:    "SELECT commit_hash = @Commit3, committer, email, message from dolt_log(@Commit3, '--not', @Commit2);",
				Expected: []sql.Row{{true, "John Doe", "johndoe@example.com", "inserting into t 3"}},
			},
			{
				Query:    "SELECT commit_hash = @Commit5, committer, email, message from dolt_log('^new-branch', @Commit5);",
				Expected: []sql.Row{{true, "billy bob", "bigbillieb@fake.horse", "inserting into t 5"}},
			},
			{
				Query:    "SELECT commit_hash = @Commit5, committer, email, message from dolt_log(@Commit5, '--not', @Commit4);",
				Expected: []sql.Row{{true, "billy bob", "bigbillieb@fake.horse", "inserting into t 5"}},
			},
		},
	},
	{
		Name: "min parents, merges, show parents, decorate",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 int);",
			"call dolt_add('.')",
			"set @Commit1 = dolt_commit('-am', 'creating table t');",

			"call dolt_checkout('-b', 'branch1')",
			"insert into t values(0,0);",
			"set @Commit2 = dolt_commit('-am', 'inserting 0,0');",

			"call dolt_checkout('main')",
			"call dolt_checkout('-b', 'branch2')",
			"insert into t values(1,1);",
			"set @Commit3 = dolt_commit('-am', 'inserting 1,1');",

			"call dolt_checkout('main')",
			"call dolt_merge('branch1')",               // fast-forward merge
			"set @MergeCommit = dolt_merge('branch2')", // actual merge with commit
			"call dolt_tag('v1')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT commit_hash = @MergeCommit, committer, email, message from dolt_log('--merges');",
				Expected: []sql.Row{{true, "billy bob", "bigbillieb@fake.horse", "Merge branch 'branch2' into main"}},
			},
			{
				Query:    "SELECT commit_hash = @MergeCommit, committer, email, message from dolt_log('--min-parents', '2');",
				Expected: []sql.Row{{true, "billy bob", "bigbillieb@fake.horse", "Merge branch 'branch2' into main"}},
			},
			{
				Query:    "SELECT commit_hash = @MergeCommit, committer, email, message from dolt_log('main', '--min-parents', '2');",
				Expected: []sql.Row{{true, "billy bob", "bigbillieb@fake.horse", "Merge branch 'branch2' into main"}},
			},
			{
				Query:    "SELECT count(*) from dolt_log('main');",
				Expected: []sql.Row{{6}},
			},
			{
				Query:    "SELECT count(*) from dolt_log('main', '--min-parents', '1');", // Should show everything except first commit
				Expected: []sql.Row{{5}},
			},
			{
				Query:    "SELECT count(*) from dolt_log('main', '--min-parents', '1', '--merges');", // --merges overrides --min-parents
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT commit_hash = @MergeCommit, committer, email, message from dolt_log('branch1..main', '--min-parents', '2');",
				Expected: []sql.Row{{true, "billy bob", "bigbillieb@fake.horse", "Merge branch 'branch2' into main"}},
			},
			{
				Query:    "SELECT count(*) from dolt_log('--min-parents', '5');",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "SELECT commit_hash = @MergeCommit, SUBSTRING_INDEX(parents, ', ', 1) = @Commit2, SUBSTRING_INDEX(parents, ', ', -1) = @Commit3 from dolt_log('main', '--parents', '--merges');",
				Expected: []sql.Row{{true, true, true}}, // shows two parents for merge commit
			},
			{
				Query:    "SELECT commit_hash = @Commit3, parents = @Commit1 from dolt_log('branch2', '--parents') LIMIT 1;", // shows one parent for non-merge commit
				Expected: []sql.Row{{true, true}},
			},
			{
				Query:    "SELECT commit_hash = @MergeCommit, SUBSTRING_INDEX(parents, ', ', 1) = @Commit2, SUBSTRING_INDEX(parents, ', ', -1) = @Commit3 from dolt_log('branch1..main', '--parents', '--merges') LIMIT 1;",
				Expected: []sql.Row{{true, true, true}},
			},
			{
				Query:    "SELECT commit_hash = @Commit2, parents = @Commit1 from dolt_log('branch2..branch1', '--parents') LIMIT 1;",
				Expected: []sql.Row{{true, true}},
			},
			{
				Query:    "SELECT refs from dolt_log('--decorate', 'short') LIMIT 1;",
				Expected: []sql.Row{{"HEAD -> main, tag: v1"}},
			},
			{
				Query:    "SELECT refs from dolt_log('--decorate', 'full') LIMIT 1;",
				Expected: []sql.Row{{"HEAD -> refs/heads/main, tag: refs/tags/v1"}},
			},
			{
				Query:    "SELECT commit_hash = @Commit2, parents = @Commit1, refs from dolt_log('branch2..branch1', '--parents', '--decorate', 'short') LIMIT 1;",
				Expected: []sql.Row{{true, true, "HEAD -> branch1"}},
			},
		},
	},
}

var DiffSummaryTableFunctionScriptTests = []queries.ScriptTest{
	{
		Name: "invalid arguments",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 varchar(20), c2 varchar(20));",
			"call dolt_add('.')",
			"set @Commit1 = dolt_commit('-am', 'creating table t');",

			"insert into t values(1, 'one', 'two'), (2, 'two', 'three');",
			"set @Commit2 = dolt_commit('-am', 'inserting into t');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:       "SELECT * from dolt_diff_summary();",
				ExpectedErr: sql.ErrInvalidArgumentNumber,
			},
			{
				Query:       "SELECT * from dolt_diff_summary('t');",
				ExpectedErr: sql.ErrInvalidArgumentNumber,
			},
			{
				Query:       "SELECT * from dolt_diff_summary('t', @Commit1, @Commit2, 'extra');",
				ExpectedErr: sql.ErrInvalidArgumentNumber,
			},
			{
				Query:       "SELECT * from dolt_diff_summary(null, null, null);",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:       "SELECT * from dolt_diff_summary(123, @Commit1, @Commit2);",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:       "SELECT * from dolt_diff_summary('t', 123, @Commit2);",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:       "SELECT * from dolt_diff_summary('t', @Commit1, 123);",
				ExpectedErr: sql.ErrInvalidArgumentDetails,
			},
			{
				Query:          "SELECT * from dolt_diff_summary('fake-branch', @Commit2, 't');",
				ExpectedErrStr: "branch not found: fake-branch",
			},
			{
				Query:          "SELECT * from dolt_diff_summary('fake-branch..main', 't');",
				ExpectedErrStr: "branch not found: fake-branch",
			},
			{
				Query:          "SELECT * from dolt_diff_summary(@Commit1, 'fake-branch', 't');",
				ExpectedErrStr: "branch not found: fake-branch",
			},
			{
				Query:          "SELECT * from dolt_diff_summary('main..fake-branch', 't');",
				ExpectedErrStr: "branch not found: fake-branch",
			},
			{
				Query:       "SELECT * from dolt_diff_summary(@Commit1, @Commit2, 'doesnotexist');",
				ExpectedErr: sql.ErrTableNotFound,
			},
			{
				Query:       "SELECT * from dolt_diff_summary('main^..main', 'doesnotexist');",
				ExpectedErr: sql.ErrTableNotFound,
			},
			{
				Query:       "SELECT * from dolt_diff_summary(@Commit1, concat('fake', '-', 'branch'), 't');",
				ExpectedErr: sqle.ErrInvalidNonLiteralArgument,
			},
			{
				Query:       "SELECT * from dolt_diff_summary(hashof('main'), @Commit2, 't');",
				ExpectedErr: sqle.ErrInvalidNonLiteralArgument,
			},
			{
				Query:       "SELECT * from dolt_diff_summary(@Commit1, @Commit2, LOWER('T'));",
				ExpectedErr: sqle.ErrInvalidNonLiteralArgument,
			},
			{
				Query:       "SELECT * from dolt_diff_summary('main..main~', LOWER('T'));",
				ExpectedErr: sqle.ErrInvalidNonLiteralArgument,
			},
		},
	},
	{
		Name: "basic case with single table",
		SetUpScript: []string{
			"set @Commit0 = HashOf('HEAD');",
			"set @Commit1 = dolt_commit('--allow-empty', '-m', 'creating table t');",

			// create table t only
			"create table t (pk int primary key, c1 varchar(20), c2 varchar(20));",
			"call dolt_add('.')",
			"set @Commit2 = dolt_commit('-am', 'creating table t');",

			// insert 1 row into t
			"insert into t values(1, 'one', 'two');",
			"set @Commit3 = dolt_commit('-am', 'inserting 1 into table t');",

			// insert 2 rows into t and update two cells
			"insert into t values(2, 'two', 'three'), (3, 'three', 'four');",
			"update t set c1='uno', c2='dos' where pk=1;",
			"set @Commit4 = dolt_commit('-am', 'inserting 2 into table t');",

			// drop table t only
			"drop table t;",
			"set @Commit5 = dolt_commit('-am', 'drop table t');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				// table is added, no data diff, result is empty
				Query:    "SELECT * from dolt_diff_summary(@Commit1, @Commit2, 't');",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * from dolt_diff_summary(@Commit2, @Commit3, 't');",
				Expected: []sql.Row{{"t", 0, 1, 0, 0, 3, 0, 0, 0, 1, 0, 3}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary(@Commit3, @Commit4, 't');",
				Expected: []sql.Row{{"t", 0, 2, 0, 1, 6, 0, 2, 1, 3, 3, 9}},
			},
			{
				// change from and to commits
				Query:    "SELECT * from dolt_diff_summary(@Commit4, @Commit3, 't');",
				Expected: []sql.Row{{"t", 0, 0, 2, 1, 0, 6, 2, 3, 1, 9, 3}},
			},
			{
				// table is dropped
				Query:    "SELECT * from dolt_diff_summary(@Commit4, @Commit5, 't');",
				Expected: []sql.Row{{"t", 0, 0, 3, 0, 0, 9, 0, 3, 0, 9, 0}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary(@Commit1, @Commit4, 't');",
				Expected: []sql.Row{{"t", 0, 3, 0, 0, 9, 0, 0, 0, 3, 0, 9}},
			},
			{
				Query:       "SELECT * from dolt_diff_summary(@Commit1, @Commit5, 't');",
				ExpectedErr: sql.ErrTableNotFound,
			},
			{
				Query: `
SELECT *
from dolt_diff_summary(@Commit3, @Commit4, 't') 
inner join t as of @Commit3 on rows_unmodified = t.pk;`,
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "basic case with single keyless table",
		SetUpScript: []string{
			"set @Commit0 = HashOf('HEAD');",
			"set @Commit1 = dolt_commit('--allow-empty', '-m', 'creating table t');",

			// create table t only
			"create table t (id int, c1 varchar(20), c2 varchar(20));",
			"call dolt_add('.')",
			"set @Commit2 = dolt_commit('-am', 'creating table t');",

			// insert 1 row into t
			"insert into t values(1, 'one', 'two');",
			"set @Commit3 = dolt_commit('-am', 'inserting 1 into table t');",

			// insert 2 rows into t and update two cells
			"insert into t values(2, 'two', 'three'), (3, 'three', 'four');",
			"update t set c1='uno', c2='dos' where id=1;",
			"set @Commit4 = dolt_commit('-am', 'inserting 2 into table t');",

			// drop table t only
			"drop table t;",
			"set @Commit5 = dolt_commit('-am', 'drop table t');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				// table is added, no data diff, result is empty
				Query:    "SELECT * from dolt_diff_summary(@Commit1, @Commit2, 't');",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * from dolt_diff_summary(@Commit2, @Commit3, 't');",
				Expected: []sql.Row{{"t", nil, 1, 0, nil, nil, nil, nil, nil, nil, nil, nil}},
			},
			{
				// TODO : (correct result is commented out)
				//      update row for keyless table deletes the row and insert the new row
				// 		this causes row added = 3 and row deleted = 1
				Query: "SELECT * from dolt_diff_summary(@Commit3, @Commit4, 't');",
				//Expected:         []sql.Row{{"t", nil, 2, 0, nil, nil, nil, nil, nil, nil, nil, nil}},
				Expected: []sql.Row{{"t", nil, 3, 1, nil, nil, nil, nil, nil, nil, nil, nil}},
			},
			{
				Query: "SELECT * from dolt_diff_summary(@Commit4, @Commit3, 't');",
				//Expected:         []sql.Row{{"t", nil, 0, 2, nil, nil, nil, nil, nil, nil, nil, nil}},
				Expected: []sql.Row{{"t", nil, 1, 3, nil, nil, nil, nil, nil, nil, nil, nil}},
			},
			{
				// table is dropped
				Query:    "SELECT * from dolt_diff_summary(@Commit4, @Commit5, 't');",
				Expected: []sql.Row{{"t", nil, 0, 3, nil, nil, nil, nil, nil, nil, nil, nil}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary(@Commit1, @Commit4, 't');",
				Expected: []sql.Row{{"t", nil, 3, 0, nil, nil, nil, nil, nil, nil, nil, nil}},
			},
			{
				Query:       "SELECT * from dolt_diff_summary(@Commit1, @Commit5, 't');",
				ExpectedErr: sql.ErrTableNotFound,
			},
		},
	},
	{
		Name: "basic case with multiple tables",
		SetUpScript: []string{
			"set @Commit0 = HashOf('HEAD');",

			// add table t with 1 row
			"create table t (pk int primary key, c1 varchar(20), c2 varchar(20));",
			"insert into t values(1, 'one', 'two');",
			"call dolt_add('.')",
			"set @Commit1 = dolt_commit('-am', 'inserting into table t');",

			// add table t2 with 1 row
			"create table t2 (pk int primary key, c1 varchar(20), c2 varchar(20));",
			"insert into t2 values(100, 'hundred', 'hundert');",
			"call dolt_add('.')",
			"set @Commit2 = dolt_commit('-am', 'inserting into table t2');",

			// changes on both tables
			"insert into t values(2, 'two', 'three'), (3, 'three', 'four'), (4, 'four', 'five');",
			"update t set c1='uno', c2='dos' where pk=1;",
			"insert into t2 values(101, 'hundred one', 'one');",
			"set @Commit3 = dolt_commit('-am', 'inserting into table t');",

			// changes on both tables
			"delete from t where c2 = 'four';",
			"update t2 set c2='zero' where pk=100;",
			"set @Commit4 = dolt_commit('-am', 'inserting into table t');",

			// create keyless table
			"create table keyless (id int);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * from dolt_diff_summary(@Commit0, @Commit1);",
				Expected: []sql.Row{{"t", 0, 1, 0, 0, 3, 0, 0, 0, 1, 0, 3}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary(@Commit1, @Commit2);",
				Expected: []sql.Row{{"t2", 0, 1, 0, 0, 3, 0, 0, 0, 1, 0, 3}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary(@Commit2, @Commit3);",
				Expected: []sql.Row{{"t", 0, 3, 0, 1, 9, 0, 2, 1, 4, 3, 12}, {"t2", 1, 1, 0, 0, 3, 0, 0, 1, 2, 3, 6}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary(@Commit3, @Commit4);",
				Expected: []sql.Row{{"t", 3, 0, 1, 0, 0, 3, 0, 4, 3, 12, 9}, {"t2", 1, 0, 0, 1, 0, 0, 1, 2, 2, 6, 6}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary(@Commit4, @Commit2);",
				Expected: []sql.Row{{"t", 0, 0, 2, 1, 0, 6, 2, 3, 1, 9, 3}, {"t2", 0, 0, 1, 1, 0, 3, 1, 2, 1, 6, 3}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary(@Commit3, 'WORKING');",
				Expected: []sql.Row{{"t", 3, 0, 1, 0, 0, 3, 0, 4, 3, 12, 9}, {"t2", 1, 0, 0, 1, 0, 0, 1, 2, 2, 6, 6}},
			},
		},
	},
	{
		Name: "WORKING and STAGED",
		SetUpScript: []string{
			"set @Commit0 = HashOf('HEAD');",

			"create table t (pk int primary key, c1 text, c2 text);",
			"call dolt_add('.')",
			"insert into t values (1, 'one', 'two'), (2, 'three', 'four');",
			"set @Commit1 = dolt_commit('-am', 'inserting two rows into table t');",

			"insert into t values (3, 'five', 'six');",
			"delete from t where pk = 2",
			"update t set c2 = '100' where pk = 1",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * from dolt_diff_summary(@Commit1, 'WORKING', 't')",
				Expected: []sql.Row{{"t", 0, 1, 1, 1, 3, 3, 1, 2, 2, 6, 6}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary('STAGED', 'WORKING', 't')",
				Expected: []sql.Row{{"t", 0, 1, 1, 1, 3, 3, 1, 2, 2, 6, 6}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary('STAGED..WORKING', 't')",
				Expected: []sql.Row{{"t", 0, 1, 1, 1, 3, 3, 1, 2, 2, 6, 6}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary('WORKING', 'STAGED', 't')",
				Expected: []sql.Row{{"t", 0, 1, 1, 1, 3, 3, 1, 2, 2, 6, 6}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary('WORKING', 'WORKING', 't')",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * from dolt_diff_summary('WORKING..WORKING', 't')",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * from dolt_diff_summary('STAGED', 'STAGED', 't')",
				Expected: []sql.Row{},
			},
			{
				Query:            "call dolt_add('.')",
				SkipResultsCheck: true,
			},
			{
				Query:    "SELECT * from dolt_diff_summary('WORKING', 'STAGED', 't')",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * from dolt_diff_summary('HEAD', 'STAGED', 't')",
				Expected: []sql.Row{{"t", 0, 1, 1, 1, 3, 3, 1, 2, 2, 6, 6}},
			},
		},
	},
	{
		Name: "diff with branch refs",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 varchar(20), c2 varchar(20));",
			"call dolt_add('.')",
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

			"create table newtable (pk int primary key);",
			"insert into newtable values (1), (2);",
			"set @Commit7 = dolt_commit('-Am', 'new table newtable');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * from dolt_diff_summary('main', 'branch1', 't');",
				Expected: []sql.Row{{"t", 0, 0, 1, 1, 0, 4, 0, 2, 1, 6, 2}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary('main..branch1', 't');",
				Expected: []sql.Row{{"t", 0, 0, 1, 1, 0, 4, 0, 2, 1, 6, 2}},
			},
			{
				Query: "SELECT * from dolt_diff_summary('main', 'branch1');",
				Expected: []sql.Row{
					{"t", 0, 0, 1, 1, 0, 4, 0, 2, 1, 6, 2},
					{"newtable", 0, 0, 2, 0, 0, 2, 0, 2, 0, 2, 0},
				},
			},
			{
				Query: "SELECT * from dolt_diff_summary('main..branch1');",
				Expected: []sql.Row{
					{"t", 0, 0, 1, 1, 0, 4, 0, 2, 1, 6, 2},
					{"newtable", 0, 0, 2, 0, 0, 2, 0, 2, 0, 2, 0},
				},
			},
			{
				Query:    "SELECT * from dolt_diff_summary('branch1', 'main', 't');",
				Expected: []sql.Row{{"t", 0, 1, 0, 1, 4, 0, 1, 1, 2, 2, 6}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary('branch1..main', 't');",
				Expected: []sql.Row{{"t", 0, 1, 0, 1, 4, 0, 1, 1, 2, 2, 6}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary('main~2', 'branch1', 't');",
				Expected: []sql.Row{{"t", 0, 1, 1, 0, 2, 3, 0, 1, 1, 3, 2}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary('main~2..branch1', 't');",
				Expected: []sql.Row{{"t", 0, 1, 1, 0, 2, 3, 0, 1, 1, 3, 2}},
			},

			// Three dot
			{
				Query:    "SELECT * from dolt_diff_summary('main...branch1', 't');",
				Expected: []sql.Row{{"t", 0, 1, 1, 0, 2, 3, 0, 1, 1, 3, 2}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary('main...branch1');",
				Expected: []sql.Row{{"t", 0, 1, 1, 0, 2, 3, 0, 1, 1, 3, 2}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary('branch1...main', 't');",
				Expected: []sql.Row{{"t", 1, 1, 0, 0, 3, 0, 0, 1, 2, 3, 6}},
			},
			{
				Query: "SELECT * from dolt_diff_summary('branch1...main');",
				Expected: []sql.Row{
					{"t", 1, 1, 0, 0, 3, 0, 0, 1, 2, 3, 6},
					{"newtable", 0, 2, 0, 0, 2, 0, 0, 0, 2, 0, 2},
				},
			},
			{
				Query:    "SELECT * from dolt_diff_summary('branch1...main^');",
				Expected: []sql.Row{{"t", 1, 1, 0, 0, 3, 0, 0, 1, 2, 3, 6}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary('branch1...main', 'newtable');",
				Expected: []sql.Row{{"newtable", 0, 2, 0, 0, 2, 0, 0, 0, 2, 0, 2}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary('main...main', 'newtable');",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "schema modification: drop and add column",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 varchar(20), c2 varchar(20));",
			"call dolt_add('.');",
			"insert into t values (1, 'one', 'two'), (2, 'two', 'three');",
			"set @Commit1 = dolt_commit('-am', 'inserting row 1, 2 into t');",

			// drop 1 column and add 1 row
			"alter table t drop column c2;",
			"set @Commit2 = dolt_commit('-am', 'dropping column c2');",

			// drop 1 column and add 1 row
			"insert into t values (3, 'three');",
			"set @Commit3 = dolt_commit('-am', 'inserting row 3');",

			// add 1 column and 1 row and update
			"alter table t add column c2 varchar(20);",
			"insert into t values (4, 'four', 'five');",
			"update t set c2='foo' where pk=1;",
			"set @Commit4 = dolt_commit('-am', 'adding column c2, inserting, and updating data');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * from dolt_diff_summary(@Commit1, @Commit2, 't');",
				Expected: []sql.Row{{"t", 0, 0, 0, 2, 0, 2, 0, 2, 2, 6, 4}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary(@Commit2, @Commit3, 't');",
				Expected: []sql.Row{{"t", 2, 1, 0, 0, 2, 0, 0, 2, 3, 4, 6}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary(@Commit1, @Commit3, 't');",
				Expected: []sql.Row{{"t", 0, 1, 0, 2, 2, 2, 0, 2, 3, 6, 6}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary(@Commit3, @Commit4, 't');",
				Expected: []sql.Row{{"t", 2, 1, 0, 1, 6, 0, 1, 3, 4, 6, 12}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary(@Commit1, @Commit4, 't');",
				Expected: []sql.Row{{"t", 0, 2, 0, 2, 6, 0, 2, 2, 4, 6, 12}},
			},
		},
	},
	{
		Name: "schema modification: rename columns",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 varchar(20), c2 int);",
			"call dolt_add('.')",
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
				Query:    "SELECT * from dolt_diff_summary(@Commit1, @Commit2, 't');",
				Expected: []sql.Row{{"t", 0, 2, 0, 0, 6, 0, 0, 0, 2, 0, 6}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary(@Commit2, @Commit3, 't');",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * from dolt_diff_summary(@Commit3, @Commit4, 't');",
				Expected: []sql.Row{{"t", 1, 1, 0, 1, 3, 0, 1, 2, 3, 6, 9}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary(@Commit4, @Commit5, 't');",
				Expected: []sql.Row{{"t", 3, 1, 0, 0, 3, 0, 0, 3, 4, 9, 12}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary(@Commit1, @Commit5, 't');",
				Expected: []sql.Row{{"t", 0, 4, 0, 0, 12, 0, 0, 0, 4, 0, 12}},
			},
		},
	},
	{
		Name: "new table",
		SetUpScript: []string{
			"create table t1 (a int primary key, b int)",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_diff_summary('HEAD', 'WORKING')",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from dolt_diff_summary('WORKING', 'HEAD')",
				Expected: []sql.Row{},
			},
			{
				Query:            "insert into t1 values (1,2)",
				SkipResultsCheck: true,
			},
			{
				Query:    "select * from dolt_diff_summary('HEAD', 'WORKING', 't1')",
				Expected: []sql.Row{{"t1", 0, 1, 0, 0, 2, 0, 0, 0, 1, 0, 2}},
			},
			{
				Query:    "select * from dolt_diff_summary('WORKING', 'HEAD', 't1')",
				Expected: []sql.Row{{"t1", 0, 0, 1, 0, 0, 2, 0, 1, 0, 2, 0}},
			},
		},
	},
	{
		Name: "dropped table",
		SetUpScript: []string{
			"create table t1 (a int primary key, b int)",
			"call dolt_add('.')",
			"insert into t1 values (1,2)",
			"call dolt_commit('-am', 'new table')",
			"drop table t1",
			"call dolt_commit('-am', 'dropped table')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_diff_summary('HEAD~', 'HEAD', 't1')",
				Expected: []sql.Row{{"t1", 0, 0, 1, 0, 0, 2, 0, 1, 0, 2, 0}},
			},
			{
				Query:    "select * from dolt_diff_summary('HEAD', 'HEAD~', 't1')",
				Expected: []sql.Row{{"t1", 0, 1, 0, 0, 2, 0, 0, 0, 1, 0, 2}},
			},
		},
	},
	{
		Name: "renamed table",
		SetUpScript: []string{
			"create table t1 (a int primary key, b int)",
			"call dolt_add('.')",
			"insert into t1 values (1,2)",
			"call dolt_commit('-am', 'new table')",
			"alter table t1 rename to t2",
			"call dolt_add('.')",
			"insert into t2 values (3,4)",
			"call dolt_commit('-am', 'renamed table')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_diff_summary('HEAD~', 'HEAD', 't2')",
				Expected: []sql.Row{{"t2", 1, 1, 0, 0, 2, 0, 0, 1, 2, 2, 4}},
			},
			{
				Query:    "select * from dolt_diff_summary('HEAD~..HEAD', 't2')",
				Expected: []sql.Row{{"t2", 1, 1, 0, 0, 2, 0, 0, 1, 2, 2, 4}},
			},
			{
				// Old table name can be matched as well
				Query:    "select * from dolt_diff_summary('HEAD~', 'HEAD', 't1')",
				Expected: []sql.Row{{"t1", 1, 1, 0, 0, 2, 0, 0, 1, 2, 2, 4}},
			},
			{
				// Old table name can be matched as well
				Query:    "select * from dolt_diff_summary('HEAD~..HEAD', 't1')",
				Expected: []sql.Row{{"t1", 1, 1, 0, 0, 2, 0, 0, 1, 2, 2, 4}},
			},
		},
	},
	{
		Name: "add multiple columns, then set and unset a value. Should not show a diff",
		SetUpScript: []string{
			"CREATE table t (pk int primary key);",
			"Insert into t values (1);",
			"CALL DOLT_ADD('.');",
			"CALL DOLT_COMMIT('-am', 'setup');",
			"alter table t add column col1 int;",
			"alter table t add column col2 int;",
			"CALL DOLT_ADD('.');",
			"CALL DOLT_COMMIT('-am', 'add columns');",
			"UPDATE t set col1 = 1 where pk = 1;",
			"UPDATE t set col1 = null where pk = 1;",
			"CALL DOLT_COMMIT('--allow-empty', '-am', 'fix short tuple');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * from dolt_diff_summary('HEAD~2', 'HEAD');",
				Expected: []sql.Row{{"t", 1, 0, 0, 0, 2, 0, 0, 1, 1, 1, 3}},
			},
			{
				Query:    "SELECT * from dolt_diff_summary('HEAD~', 'HEAD');",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "pk set change should throw an error for 3 argument dolt_diff_summary",
		SetUpScript: []string{
			"CREATE table t (pk int primary key);",
			"INSERT INTO t values (1);",
			"CALL DOLT_COMMIT('-Am', 'table with row');",
			"ALTER TABLE t ADD col1 int not null default 0;",
			"ALTER TABLE t drop primary key;",
			"ALTER TABLE t add primary key (pk, col1);",
			"CALL DOLT_COMMIT('-am', 'add secondary column with primary key');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "SELECT * from dolt_diff_summary('HEAD~', 'HEAD', 't');",
				ExpectedErrStr: "failed to compute diff summary for table t: primary key set changed",
			},
		},
	},
	{
		Name: "pk set change should report warning for 2 argument dolt_diff_summary",
		SetUpScript: []string{
			"CREATE table t (pk int primary key);",
			"INSERT INTO t values (1);",
			"CREATE table t2 (pk int primary key);",
			"INSERT INTO t2 values (2);",
			"CALL DOLT_COMMIT('-Am', 'multiple tables');",
			"ALTER TABLE t ADD col1 int not null default 0;",
			"ALTER TABLE t drop primary key;",
			"ALTER TABLE t add primary key (pk, col1);",
			"INSERT INTO t2 values (3), (4), (5);",
			"CALL DOLT_COMMIT('-am', 'add secondary column with primary key to t');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT * from dolt_diff_summary('HEAD~', 'HEAD')",
				Expected: []sql.Row{
					{"t", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					{"t2", 1, 3, 0, 0, 3, 0, 0, 1, 4, 1, 4},
				},
				ExpectedWarning:       dtables.PrimaryKeyChangeWarningCode,
				ExpectedWarningsCount: 1,
			},
		},
	},
}

var LargeJsonObjectScriptTests = []queries.ScriptTest{
	{
		Name: "JSON under max length limit",
		SetUpScript: []string{
			"create table t (j JSON)",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    `insert into t set j= concat('[', repeat('"word",', 10000000), '"word"]')`,
				Expected: []sql.Row{{sql.OkResult{RowsAffected: 1}}},
			},
		},
	},
	{
		Name: "JSON over max length limit",
		SetUpScript: []string{
			"create table t (j JSON)",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:       `insert into t set j= concat('[', repeat('"word",', 50000000), '"word"]')`,
				ExpectedErr: sql.ErrLengthTooLarge,
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
			"call dolt_add('.')",
			"insert into regularTable values (1, 2, 3), (2, 3, 4);",
			"insert into droppedTable values (1, 2, 3), (2, 3, 4);",
			"set @Commit1 = (select DOLT_COMMIT('-am', 'Creating tables x and y'));",

			// changeSet: STAGED; data change: false; schema change: true
			"create table addedTable (a int primary key, b int, c int);",
			"call DOLT_ADD('addedTable');",
			// changeSet: STAGED; data change: true; schema change: true
			"drop table droppedTable;",
			"call DOLT_ADD('droppedTable');",
			// changeSet: WORKING; data change: false; schema change: true
			"rename table renamedEmptyTable to newRenamedEmptyTable",
			// changeSet: WORKING; data change: true; schema change: false
			"insert into regularTable values (3, 4, 5);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM DOLT_DIFF;",
				Expected: []sql.Row{{7}},
			},
			{
				Query:    "SELECT COUNT(*) FROM DOLT_DIFF WHERE commit_hash = @Commit1;",
				Expected: []sql.Row{{3}},
			},
			{
				Query:    "SELECT * FROM DOLT_DIFF WHERE commit_hash = @Commit1 AND committer <> 'billy bob';",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT commit_hash, committer FROM DOLT_DIFF WHERE commit_hash <> @Commit1 AND committer = 'billy bob' AND commit_hash NOT IN ('WORKING','STAGED');",
				Expected: []sql.Row{},
			},
			{
				Query: "SELECT commit_hash, table_name FROM DOLT_DIFF WHERE commit_hash <> @Commit1 AND commit_hash NOT IN ('STAGED') ORDER BY table_name;",
				Expected: []sql.Row{
					{"WORKING", "newRenamedEmptyTable"},
					{"WORKING", "regularTable"},
				},
			},
			{
				Query: "SELECT commit_hash, table_name FROM DOLT_DIFF WHERE commit_hash <> @Commit1 OR committer <> 'billy bob' ORDER BY table_name;",
				Expected: []sql.Row{
					{"STAGED", "addedTable"},
					{"STAGED", "droppedTable"},
					{"WORKING", "newRenamedEmptyTable"},
					{"WORKING", "regularTable"},
				},
			},
			{
				Query: "SELECT * FROM DOLT_DIFF WHERE COMMIT_HASH in ('WORKING', 'STAGED') ORDER BY table_name;",
				Expected: []sql.Row{
					{"STAGED", "addedTable", nil, nil, nil, nil, false, true},
					{"STAGED", "droppedTable", nil, nil, nil, nil, true, true},
					{"WORKING", "newRenamedEmptyTable", nil, nil, nil, nil, false, true},
					{"WORKING", "regularTable", nil, nil, nil, nil, true, false},
				},
			},
		},
	},
	{
		Name: "basic case with three tables",
		SetUpScript: []string{
			"create table x (a int primary key, b int, c int);",
			"create table y (a int primary key, b int, c int);",
			"call dolt_add('.')",
			"insert into x values (1, 2, 3), (2, 3, 4);",
			"set @Commit1 = (select DOLT_COMMIT('-am', 'Creating tables x and y'));",

			"create table z (a int primary key, b int, c int);",
			"call dolt_add('.')",
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
				Query:    "select table_name, schema_change, data_change from DOLT_DIFF where commit_hash = @Commit1",
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
			"call dolt_add('.')",
			"insert into x values (1, 2, 3), (2, 3, 4)",
			"set @Commit1 = (select DOLT_COMMIT('-am', 'Creating tables x and y'))",

			"create table z (a int primary key, b int, c int)",
			"call dolt_add('.')",
			"insert into z values (100, 101, 102)",
			"set @Commit2 = (select DOLT_COMMIT('-am', 'Creating tables z'))",

			"rename table x to x1",
			"call dolt_add('.')",
			"insert into x1 values (1000, 1001, 1002);",
			"set @Commit3 = (select DOLT_COMMIT('-am', 'Renaming table x to x1 and inserting data'))",

			"rename table x1 to x2",
			"call dolt_add('.')",
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
			"call dolt_add('.')",
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
			"call dolt_add('.')",
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
			"call dolt_add('.')",
			"insert into x values (1, 2, 3), (2, 3, 4)",
			"set @Commit1 = (select DOLT_COMMIT('-am', 'Creating tables x and y'))",

			"select dolt_checkout('-b', 'branch2')",
			"create table z (a int primary key, b int, c int)",
			"call dolt_add('.')",
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
			"call dolt_add('.')",
			"insert into x values (1, 2, 3), (2, 3, 4)",
			"set @Commit1 = (select DOLT_COMMIT('-am', 'Creating tables x and y'))",

			"select dolt_checkout('-b', 'branch2')",
			"create table z (a int primary key, b int, c int)",
			"call dolt_add('.')",
			"insert into z values (100, 101, 102)",
			"set @Commit2 = (select DOLT_COMMIT('-am', 'Creating tables z'))",

			"select DOLT_MERGE('branch1', '--no-commit')",
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
			"call dolt_add('.')",
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
			"call dolt_add('.')",
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
			"call dolt_add('.')",
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
			"call dolt_add('.')",
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
			"call dolt_add('.')",
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
			"call dolt_add('.')",
			"insert into t values (1, 2), (3, 4);",
			"set @Commit1 = DOLT_COMMIT('-am', 'creating table t');",

			"alter table t drop column c;",
			"set @Commit2 = DOLT_COMMIT('-am', 'dropping column c');",

			"alter table t add column c varchar(20);",
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
			"create table t (pk int primary key, c varchar(20));",
			"call dolt_add('.')",
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
			"call dolt_add('.')",
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

var DoltTagTestScripts = []queries.ScriptTest{
	{
		Name: "dolt-tag: SQL create tags",
		SetUpScript: []string{
			"CREATE TABLE test(pk int primary key);",
			"CALL DOLT_ADD('.')",
			"INSERT INTO test VALUES (0),(1),(2);",
			"CALL DOLT_COMMIT('-am','created table test')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_TAG('v1', 'HEAD')",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "SELECT tag_name, IF(CHAR_LENGTH(tag_hash) < 0, NULL, 'not null'), tagger, email, IF(date IS NULL, NULL, 'not null'), message from dolt_tags",
				Expected: []sql.Row{{"v1", "not null", "billy bob", "bigbillieb@fake.horse", "not null", ""}},
			},
			{
				Query:    "CALL DOLT_TAG('v2', '-m', 'create tag v2')",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "SELECT tag_name, message from dolt_tags",
				Expected: []sql.Row{{"v1", ""}, {"v2", "create tag v2"}},
			},
		},
	},
	{
		Name: "dolt-tag: SQL delete tags",
		SetUpScript: []string{
			"CREATE TABLE test(pk int primary key);",
			"CALL DOLT_ADD('.')",
			"INSERT INTO test VALUES (0),(1),(2);",
			"CALL DOLT_COMMIT('-am','created table test')",
			"CALL DOLT_TAG('v1', '-m', 'create tag v1')",
			"CALL DOLT_TAG('v2', '-m', 'create tag v2')",
			"CALL DOLT_TAG('v3', '-m', 'create tag v3')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT tag_name, message from dolt_tags",
				Expected: []sql.Row{{"v1", "create tag v1"}, {"v2", "create tag v2"}, {"v3", "create tag v3"}},
			},
			{
				Query:    "CALL DOLT_TAG('-d','v1')",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "SELECT tag_name, message from dolt_tags",
				Expected: []sql.Row{{"v2", "create tag v2"}, {"v3", "create tag v3"}},
			},
			{
				Query:    "CALL DOLT_TAG('-d','v2','v3')",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "SELECT tag_name, message from dolt_tags",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "dolt-tag: SQL use a tag as a ref for merge",
		SetUpScript: []string{
			"CREATE TABLE test(pk int primary key);",
			"CALL DOLT_ADD('.')",
			"INSERT INTO test VALUES (0),(1),(2);",
			"CALL DOLT_COMMIT('-am','created table test')",
			"DELETE FROM test WHERE pk = 0",
			"INSERT INTO test VALUES (3)",
			"CALL DOLT_COMMIT('-am','made changes')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_TAG('v1','HEAD')",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "CALL DOLT_CHECKOUT('-b','other','HEAD^')",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "INSERT INTO test VALUES (8), (9)",
				Expected: []sql.Row{{sql.OkResult{RowsAffected: 2}}},
			},
			{
				Query:            "CALL DOLT_COMMIT('-am','made changes in other')",
				SkipResultsCheck: true,
			},
			{
				Query:    "CALL DOLT_MERGE('v1')",
				Expected: []sql.Row{{0, 0}},
			},
			{
				Query:    "SELECT * FROM test",
				Expected: []sql.Row{{1}, {2}, {3}, {8}, {9}},
			},
		},
	},
}

var DoltRemoteTestScripts = []queries.ScriptTest{
	{
		Name: "dolt-remote: SQL add remotes",
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL DOLT_REMOTE('add','origin','file://../test')",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "SELECT name, IF(CHAR_LENGTH(url) < 0, NULL, 'not null'), fetch_specs, params FROM DOLT_REMOTES",
				Expected: []sql.Row{{"origin", "not null", sql.MustJSON(`["refs/heads/*:refs/remotes/origin/*"]`), sql.MustJSON(`{}`)}},
			},
			{
				Query:          "CALL DOLT_REMOTE()",
				ExpectedErrStr: "error: invalid argument, use 'dolt_remotes' system table to list remotes",
			},
			{
				Query:          "CALL DOLT_REMOTE('origin')",
				ExpectedErrStr: "error: invalid argument",
			},
			{
				Query:          "INSERT INTO dolt_remotes (name, url) VALUES ('origin', 'file://../test')",
				ExpectedErrStr: "the dolt_remotes table is read-only; use the dolt_remote stored procedure to edit remotes",
			},
		},
	},
	{
		Name: "dolt-remote: SQL remove remotes",
		SetUpScript: []string{
			"CALL DOLT_REMOTE('add','origin1','file://.')",
			"CALL DOLT_REMOTE('add','origin2','aws://[dynamo_db_table:s3_bucket]/repo_name')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT name, IF(CHAR_LENGTH(url) < 0, NULL, 'not null'), fetch_specs, params FROM DOLT_REMOTES",
				Expected: []sql.Row{
					{"origin1", "not null", sql.MustJSON(`["refs/heads/*:refs/remotes/origin1/*"]`), sql.MustJSON(`{}`)},
					{"origin2", "not null", sql.MustJSON(`["refs/heads/*:refs/remotes/origin2/*"]`), sql.MustJSON(`{}`)}},
			},
			{
				Query:    "CALL DOLT_REMOTE('remove','origin2')",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "SELECT name, IF(CHAR_LENGTH(url) < 0, NULL, 'not null'), fetch_specs, params FROM DOLT_REMOTES",
				Expected: []sql.Row{{"origin1", "not null", sql.MustJSON(`["refs/heads/*:refs/remotes/origin1/*"]`), sql.MustJSON(`{}`)}},
			},
			// 'origin1' remote must exist in order this error to be returned; otherwise, no error from EOF
			{
				Query:          "DELETE FROM dolt_remotes WHERE name = 'origin1'",
				ExpectedErrStr: "the dolt_remotes table is read-only; use the dolt_remote stored procedure to edit remotes",
			},
		},
	},
	{
		Name: "dolt-remote: multi-repo test",
		SetUpScript: []string{
			"create database one",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "use one;",
				Expected: []sql.Row{},
			},
			{
				Query:    "CALL DOLT_REMOTE('add','test01','file:///foo');",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "select count(*) from dolt_remotes where name='test01';",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "use mydb;",
				Expected: []sql.Row{},
			},
			{
				Query:    "select count(*) from dolt_remotes where name='test01';",
				Expected: []sql.Row{{0}},
			},
		},
	},
}

// DoltAutoIncrementTests is tests of dolt's global auto increment logic
var DoltAutoIncrementTests = []queries.ScriptTest{
	{
		Name: "insert on different branches",
		SetUpScript: []string{
			"create table t (a int primary key auto_increment, b int)",
			"call dolt_add('.')",
			"call dolt_commit('-am', 'empty table')",
			"call dolt_branch('branch1')",
			"call dolt_branch('branch2')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "insert into t (b) values (1), (2)",
				Expected: []sql.Row{{sql.OkResult{RowsAffected: 2, InsertID: 1}}},
			},
			{
				Query:            "call dolt_commit('-am', 'two values on main')",
				SkipResultsCheck: true,
			},
			{
				Query:            "call dolt_checkout('branch1')",
				SkipResultsCheck: true,
			},
			{
				Query:    "insert into t (b) values (3), (4)",
				Expected: []sql.Row{{sql.OkResult{RowsAffected: 2, InsertID: 3}}},
			},
			{
				Query: "select * from t order by a",
				Expected: []sql.Row{
					{3, 3},
					{4, 4},
				},
			},
			{
				Query:            "call dolt_commit('-am', 'two values on branch1')",
				SkipResultsCheck: true,
			},
			{
				Query:            "call dolt_checkout('branch2')",
				SkipResultsCheck: true,
			},
			{
				Query:    "insert into t (b) values (5), (6)",
				Expected: []sql.Row{{sql.OkResult{RowsAffected: 2, InsertID: 5}}},
			},
			{
				Query: "select * from t order by a",
				Expected: []sql.Row{
					{5, 5},
					{6, 6},
				},
			},
		},
	},
	{
		Name: "drop table",
		SetUpScript: []string{
			"create table t (a int primary key auto_increment, b int)",
			"call dolt_add('.')",
			"call dolt_commit('-am', 'empty table')",
			"call dolt_branch('branch1')",
			"call dolt_branch('branch2')",
			"insert into t (b) values (1), (2)",
			"call dolt_commit('-am', 'two values on main')",
			"call dolt_checkout('branch1')",
			"insert into t (b) values (3), (4)",
			"call dolt_commit('-am', 'two values on branch1')",
			"call dolt_checkout('branch2')",
			"insert into t (b) values (5), (6)",
			"call dolt_checkout('branch1')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "drop table t",
				Expected: []sql.Row{{sql.NewOkResult(0)}},
			},
			{
				Query:            "call dolt_checkout('main')",
				SkipResultsCheck: true,
			},
			{
				// highest value in any branch is 6
				Query:    "insert into t (b) values (7), (8)",
				Expected: []sql.Row{{sql.OkResult{RowsAffected: 2, InsertID: 7}}},
			},
			{
				Query: "select * from t order by a",
				Expected: []sql.Row{
					{1, 1},
					{2, 2},
					{7, 7},
					{8, 8},
				},
			},
			{
				Query:    "drop table t",
				Expected: []sql.Row{{sql.NewOkResult(0)}},
			},
			{
				Query:            "call dolt_checkout('branch2')",
				SkipResultsCheck: true,
			},
			{
				// highest value in any branch is still 6 (dropped table above)
				Query:    "insert into t (b) values (7), (8)",
				Expected: []sql.Row{{sql.OkResult{RowsAffected: 2, InsertID: 7}}},
			},
			{
				Query: "select * from t order by a",
				Expected: []sql.Row{
					{5, 5},
					{6, 6},
					{7, 7},
					{8, 8},
				},
			},
			{
				Query:    "drop table t",
				Expected: []sql.Row{{sql.NewOkResult(0)}},
			},
			{
				Query:            "create table t (a int primary key auto_increment, b int)",
				SkipResultsCheck: true,
			},
			{
				// no value on any branch
				Query:    "insert into t (b) values (1), (2)",
				Expected: []sql.Row{{sql.OkResult{RowsAffected: 2, InsertID: 1}}},
			},
			{
				Query: "select * from t order by a",
				Expected: []sql.Row{
					{1, 1},
					{2, 2},
				},
			},
		},
	},
}

var BrokenAutoIncrementTests = []queries.ScriptTest{
	{
		// truncate table doesn't reset the persisted auto increment counter of tables on other branches, which leads to
		// the value not resetting to 1 after a truncate if the table exists on other branches, even if truncated on every
		// branch
		Name: "truncate table",
		SetUpScript: []string{
			"create table t (a int primary key auto_increment, b int)",
			"call dolt_add('.')",
			"call dolt_commit('-am', 'empty table')",
			"call dolt_branch('branch1')",
			"call dolt_branch('branch2')",
			"insert into t (b) values (1), (2)",
			"call dolt_commit('-am', 'two values on main')",
			"call dolt_checkout('branch1')",
			"insert into t (b) values (3), (4)",
			"call dolt_commit('-am', 'two values on branch1')",
			"call dolt_checkout('branch2')",
			"insert into t (b) values (5), (6)",
			"call dolt_checkout('branch1')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "truncate table t",
				Expected: []sql.Row{{sql.NewOkResult(2)}},
			},
			{
				Query:            "call dolt_checkout('main')",
				SkipResultsCheck: true,
			},
			{
				// highest value in any branch is 6
				Query:    "insert into t (b) values (7), (8)",
				Expected: []sql.Row{{sql.OkResult{RowsAffected: 2, InsertID: 7}}},
			},
			{
				Query: "select * from t order by a",
				Expected: []sql.Row{
					{1, 1},
					{2, 2},
					{7, 7},
					{8, 8},
				},
			},
			{
				Query:    "truncate table t",
				Expected: []sql.Row{{sql.NewOkResult(4)}},
			},
			{
				Query:            "call dolt_checkout('branch2')",
				SkipResultsCheck: true,
			},
			{
				// highest value in any branch is still 6 (truncated table above)
				Query:    "insert into t (b) values (7), (8)",
				Expected: []sql.Row{{sql.OkResult{RowsAffected: 2, InsertID: 7}}},
			},
			{
				Query: "select * from t order by a",
				Expected: []sql.Row{
					{5, 5},
					{6, 6},
					{7, 7},
					{8, 8},
				},
			},
			{
				Query:    "truncate table t",
				Expected: []sql.Row{{sql.NewOkResult(4)}},
			},
			{
				// no value on any branch
				Query:    "insert into t (b) values (1), (2)",
				Expected: []sql.Row{{sql.OkResult{RowsAffected: 2, InsertID: 1}}},
			},
			{
				Query: "select * from t order by a",
				Expected: []sql.Row{
					{1, 1},
					{2, 2},
				},
			},
		},
	},
}

var DoltCommitTests = []queries.ScriptTest{
	{
		Name: "CALL DOLT_COMMIT('-ALL') adds all tables (including new ones) to the commit.",
		SetUpScript: []string{
			"CREATE table t (pk int primary key);",
			"INSERT INTO t VALUES (1);",
			"CALL DOLT_ADD('t');",
			"CALL DOLT_COMMIT('-m', 'add table t');",
			"CALL DOLT_RESET('--hard');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * from t;",
				Expected: []sql.Row{{1}},
			},
			// update a table
			{
				Query:    "DELETE from t where pk = 1;",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:            "CALL DOLT_COMMIT('-ALL', '-m', 'update table terminator');",
				SkipResultsCheck: true,
			},
			// check last commit
			{
				Query:    "select message from dolt_log limit 1",
				Expected: []sql.Row{{"update table terminator"}},
			},
			// amend last commit
			{
				Query:            "CALL DOLT_COMMIT('-amend', '-m', 'update table t');",
				SkipResultsCheck: true,
			},
			// check amended commit
			{
				Query:    "select message from dolt_log limit 1",
				Expected: []sql.Row{{"update table t"}},
			},
			{
				Query:    "CALL DOLT_RESET('--hard');",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "SELECT * from t;",
				Expected: []sql.Row{},
			},
			// delete a table
			{
				Query:    "DROP TABLE t;",
				Expected: []sql.Row{{sql.NewOkResult(0)}},
			},
			{
				Query:            "CALL DOLT_COMMIT('-Am', 'drop table t');",
				SkipResultsCheck: true,
			},
			{
				Query:    "CALL DOLT_RESET('--hard');",
				Expected: []sql.Row{{0}},
			},
			{
				Query:       "SELECT * from t;",
				ExpectedErr: sql.ErrTableNotFound,
			},
			// create a table
			{
				Query:    "CREATE table t2 (pk int primary key);",
				Expected: []sql.Row{{sql.NewOkResult(0)}},
			},
			{
				Query:            "CALL DOLT_COMMIT('-Am', 'add table 21');",
				SkipResultsCheck: true,
			},
			// amend last commit
			{
				Query:            "CALL DOLT_COMMIT('-amend', '-m', 'add table 2');",
				SkipResultsCheck: true,
			},
			// check amended commit
			{
				Query:    "select message from dolt_log limit 1",
				Expected: []sql.Row{{"add table 2"}},
			},
			{
				Query:    "CALL DOLT_RESET('--hard');",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "SELECT * from t2;",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "dolt commit works with arguments",
		SetUpScript: []string{
			"CREATE table t (pk int primary key);",
			"INSERT INTO t VALUES (1);",
			"CALL DOLT_ADD('t');",
			"CALL DOLT_COMMIT('-m', concat('author: ','somebody'));",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT message from dolt_log where message = 'author: somebody'",
				Expected: []sql.Row{
					{"author: somebody"},
				},
			},
		},
	},
	{
		Name: "CALL DOLT_COMMIT('-amend') works to update commit message",
		SetUpScript: []string{
			"SET @@AUTOCOMMIT=0;",
			"CREATE TABLE test (id INT PRIMARY KEY );",
			"INSERT INTO test (id) VALUES (2)",
			"CALL DOLT_ADD('.');",
			"CALL DOLT_COMMIT('-m', 'original commit message');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT  message FROM dolt_log;",
				Expected: []sql.Row{
					{"original commit message"},
					{"author: somebody"},
					{"add table 2"},
					{"drop table t"},
					{"update table t"},
					{"add table t"},
					{"checkpoint enginetest database mydb"},
					{"Initialize data repository"},
				},
			},
			{
				Query:    "SELECT to_id, from_id, diff_type FROM dolt_diff_test;",
				Expected: []sql.Row{{2, nil, "added"}},
			},
			{
				Query:            "CALL DOLT_COMMIT('--amend', '-m', 'amended commit message');",
				SkipResultsCheck: true, // commit hash is being returned, skip check
			},
			{
				Query: "SELECT  message FROM dolt_log;",
				Expected: []sql.Row{
					{"amended commit message"},
					{"author: somebody"},
					{"add table 2"},
					{"drop table t"},
					{"update table t"},
					{"add table t"},
					{"checkpoint enginetest database mydb"},
					{"Initialize data repository"},
				},
			},
			{
				Query:    "SELECT to_id, from_id, diff_type FROM dolt_diff_test;",
				Expected: []sql.Row{{2, nil, "added"}},
			},
		},
	},
	{
		Name: "CALL DOLT_COMMIT('-amend') works to add changes to a commit",
		SetUpScript: []string{
			"SET @@AUTOCOMMIT=0;",
			"INSERT INTO test (id) VALUES (3)",
			"CALL DOLT_ADD('.');",
			"CALL DOLT_COMMIT('-m', 'original commit message for adding changes to a commit');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT to_id, from_id, diff_type FROM dolt_diff_test;",
				Expected: []sql.Row{
					{3, nil, "added"},
					{2, nil, "added"},
				},
			},
			{
				Query:    "SELECT COUNT(*) FROM dolt_status;",
				Expected: []sql.Row{{0}},
			},
			{
				Query: "SELECT  message FROM dolt_log;",
				Expected: []sql.Row{
					{"original commit message for adding changes to a commit"},
					{"amended commit message"},
					{"author: somebody"},
					{"add table 2"},
					{"drop table t"},
					{"update table t"},
					{"add table t"},
					{"checkpoint enginetest database mydb"},
					{"Initialize data repository"},
				},
			},
			{
				Query:    "INSERT INTO test (id) VALUES (4)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "SELECT COUNT(*) FROM dolt_status;",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "CALL DOLT_ADD('.');",
				Expected: []sql.Row{{0}},
			},
			{
				Query:            "CALL DOLT_COMMIT('--amend');",
				SkipResultsCheck: true, // commit hash is being returned, skip check
			},
			{
				Query: "SELECT message FROM dolt_log;",
				Expected: []sql.Row{
					{"original commit message for adding changes to a commit"},
					{"amended commit message"},
					{"author: somebody"},
					{"add table 2"},
					{"drop table t"},
					{"update table t"},
					{"add table t"},
					{"checkpoint enginetest database mydb"},
					{"Initialize data repository"},
				},
			},
			{
				Query: "SELECT to_id, from_id, diff_type FROM dolt_diff_test;",
				Expected: []sql.Row{
					{4, nil, "added"},
					{3, nil, "added"},
					{2, nil, "added"},
				},
			},
			{
				Query:    "INSERT INTO test (id) VALUES (5)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "SELECT COUNT(*) FROM dolt_status;",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "CALL DOLT_ADD('.');",
				Expected: []sql.Row{{0}},
			},
			{
				Query:            "CALL DOLT_COMMIT('--amend', '-m', 'amended commit with added changes');",
				SkipResultsCheck: true, // commit hash is being returned, skip check
			},
			{
				Query:    "SELECT COUNT(*) FROM dolt_status;",
				Expected: []sql.Row{{0}},
			},
			{
				Query: "SELECT message FROM dolt_log;",
				Expected: []sql.Row{
					{"amended commit with added changes"},
					{"amended commit message"},
					{"author: somebody"},
					{"add table 2"},
					{"drop table t"},
					{"update table t"},
					{"add table t"},
					{"checkpoint enginetest database mydb"},
					{"Initialize data repository"},
				},
			},
			{
				Query: "SELECT to_id, from_id, diff_type FROM dolt_diff_test;",
				Expected: []sql.Row{
					{5, nil, "added"},
					{4, nil, "added"},
					{3, nil, "added"},
					{2, nil, "added"},
				},
			},
		},
	},
	{
		Name: "CALL DOLT_COMMIT('-amend') works to remove changes from a commit",
		SetUpScript: []string{
			"SET @@AUTOCOMMIT=0;",
			"INSERT INTO test (id) VALUES (6)",
			"INSERT INTO test (id) VALUES (7)",
			"CALL DOLT_ADD('.');",
			"CALL DOLT_COMMIT('-m', 'original commit message');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * FROM test;",
				Expected: []sql.Row{{2}, {3}, {4}, {5}, {6}, {7}},
			},
			{
				Query: "SELECT to_id, from_id, diff_type FROM dolt_diff_test;",
				Expected: []sql.Row{
					{7, nil, "added"},
					{6, nil, "added"},
					{5, nil, "added"},
					{4, nil, "added"},
					{3, nil, "added"},
					{2, nil, "added"},
				},
			},
			{
				Query:    "DELETE FROM test WHERE id = 6",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "CALL DOLT_ADD('.');",
				Expected: []sql.Row{{0}},
			},
			{
				Query:            "CALL DOLT_COMMIT('--amend', '-m', 'amended commit with removed changes');",
				SkipResultsCheck: true, // commit hash is being returned, skip check
			},
			{
				Query:    "SELECT * FROM test;",
				Expected: []sql.Row{{2}, {3}, {4}, {5}, {7}},
			},
			{
				Query: "SELECT message FROM dolt_log;",
				Expected: []sql.Row{
					{"amended commit with removed changes"},
					{"amended commit with added changes"},
					{"amended commit message"},
					{"author: somebody"},
					{"add table 2"},
					{"drop table t"},
					{"update table t"},
					{"add table t"},
					{"checkpoint enginetest database mydb"},
					{"Initialize data repository"},
				},
			},
			{
				Query: "SELECT to_id, from_id, diff_type FROM dolt_diff_test;",
				Expected: []sql.Row{
					{7, nil, "added"},
					{5, nil, "added"},
					{4, nil, "added"},
					{3, nil, "added"},
					{2, nil, "added"},
				},
			},
		},
	},
	{
		Name: "CALL DOLT_COMMIT('-amend') works to update a merge commit",
		SetUpScript: []string{
			"SET @@AUTOCOMMIT=0;",

			"CREATE TABLE test2 (id INT PRIMARY KEY, id2 INT);",
			"CALL DOLT_ADD('.');",
			"CALL DOLT_COMMIT('-m', 'original table');",

			"CALL DOLT_CHECKOUT('-b','test-branch');",
			"INSERT INTO test2 (id, id2) VALUES (0, 2)",
			"CALL DOLT_ADD('.');",
			"CALL DOLT_COMMIT('-m', 'conflicting commit message');",

			"CALL DOLT_CHECKOUT('main');",
			"INSERT INTO test2 (id, id2) VALUES (0, 1)",
			"CALL DOLT_ADD('.');",
			"CALL DOLT_COMMIT('-m', 'original commit message');",

			"CALL DOLT_MERGE('test-branch');",
			"CALL DOLT_CONFLICTS_RESOLVE('--theirs', '.');",
			"CALL DOLT_COMMIT('-m', 'final merge');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "CALL DOLT_COMMIT('--amend', '-m', 'new merge');",
				SkipResultsCheck: true, // commit hash is being returned, skip check
			},
			{
				Query: "SELECT message FROM dolt_log;",
				Expected: []sql.Row{
					{"new merge"},
					{"original commit message"},
					{"conflicting commit message"},
					{"original table"},
					{"amended commit with removed changes"},
					{"amended commit with added changes"},
					{"amended commit message"},
					{"author: somebody"},
					{"add table 2"},
					{"drop table t"},
					{"update table t"},
					{"add table t"},
					{"checkpoint enginetest database mydb"},
					{"Initialize data repository"},
				},
			},
			{
				Query:    "SET @hash=(SELECT commit_hash FROM dolt_log LIMIT 1);",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "SELECT COUNT(parent_hash) FROM dolt_commit_ancestors WHERE commit_hash= @hash;",
				Expected: []sql.Row{{2}},
			},
		},
	},
}

var DoltIndexPrefixScripts = []queries.ScriptTest{
	{
		Name: "inline secondary indexes with collation",
		SetUpScript: []string{
			"create table t (i int primary key, v1 varchar(10), v2 varchar(10), unique index (v1(3),v2(5))) collate utf8mb4_0900_ai_ci",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "show create table t",
				Expected: []sql.Row{{"t", "CREATE TABLE `t` (\n  `i` int NOT NULL,\n  `v1` varchar(10) COLLATE utf8mb4_0900_ai_ci,\n  `v2` varchar(10) COLLATE utf8mb4_0900_ai_ci,\n  PRIMARY KEY (`i`),\n  UNIQUE KEY `v1v2` (`v1`(3),`v2`(5))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"}},
			},
			{
				Query:    "insert into t values (0, 'a', 'a'), (1, 'ab','ab'), (2, 'abc', 'abc'), (3, 'abcde', 'abcde')",
				Expected: []sql.Row{{sql.NewOkResult(4)}},
			},
			{
				Query:       "insert into t values (99, 'ABC', 'ABCDE')",
				ExpectedErr: sql.ErrUniqueKeyViolation,
			},
			{
				Query:          "insert into t values (99, 'ABC', 'ABCDE')",
				ExpectedErrStr: "duplicate unique key given: [ABC,ABCDE]",
			},
			{
				Query:       "insert into t values (99, 'ABC123', 'ABCDE123')",
				ExpectedErr: sql.ErrUniqueKeyViolation,
			},
			{
				Query:          "insert into t values (99, 'ABC123', 'ABCDE123')",
				ExpectedErrStr: "duplicate unique key given: [ABC,ABCDE]",
			},
			{
				Query: "select * from t where v1 = 'A'",
				Expected: []sql.Row{
					{0, "a", "a"},
				},
			},
			{
				Query: "explain select * from t where v1 = 'A'",
				Expected: []sql.Row{
					{"Filter(t.v1 = 'A')"},
					{" └─ IndexedTableAccess(t)"},
					{"     ├─ index: [t.v1,t.v2]"},
					{"     ├─ filters: [{[A, A], [NULL, ∞)}]"},
					{"     └─ columns: [i v1 v2]"},
				},
			},
			{
				Query: "select * from t where v1 = 'ABC'",
				Expected: []sql.Row{
					{2, "abc", "abc"},
				},
			},
			{
				Query: "explain select * from t where v1 = 'ABC'",
				Expected: []sql.Row{
					{"Filter(t.v1 = 'ABC')"},
					{" └─ IndexedTableAccess(t)"},
					{"     ├─ index: [t.v1,t.v2]"},
					{"     ├─ filters: [{[ABC, ABC], [NULL, ∞)}]"},
					{"     └─ columns: [i v1 v2]"},
				},
			},
			{
				Query:    "select * from t where v1 = 'ABCD'",
				Expected: []sql.Row{},
			},
			{
				Query: "explain select * from t where v1 = 'ABCD'",
				Expected: []sql.Row{
					{"Filter(t.v1 = 'ABCD')"},
					{" └─ IndexedTableAccess(t)"},
					{"     ├─ index: [t.v1,t.v2]"},
					{"     ├─ filters: [{[ABCD, ABCD], [NULL, ∞)}]"},
					{"     └─ columns: [i v1 v2]"},
				},
			},
			{
				Query: "select * from t where v1 > 'A' and v1 < 'ABCDE'",
				Expected: []sql.Row{
					{1, "ab", "ab"},
					{2, "abc", "abc"},
				},
			},
			{
				Query: "explain select * from t where v1 > 'A' and v1 < 'ABCDE'",
				Expected: []sql.Row{
					{"Filter((t.v1 > 'A') AND (t.v1 < 'ABCDE'))"},
					{" └─ IndexedTableAccess(t)"},
					{"     ├─ index: [t.v1,t.v2]"},
					{"     ├─ filters: [{(A, ABCDE), [NULL, ∞)}]"},
					{"     └─ columns: [i v1 v2]"},
				},
			},
			{
				Query: "select * from t where v1 > 'A' and v2 < 'ABCDE'",
				Expected: []sql.Row{
					{1, "ab", "ab"},
					{2, "abc", "abc"},
				},
			},
			{
				Query: "explain select * from t where v1 > 'A' and v2 < 'ABCDE'",
				Expected: []sql.Row{
					{"Filter((t.v1 > 'A') AND (t.v2 < 'ABCDE'))"},
					{" └─ IndexedTableAccess(t)"},
					{"     ├─ index: [t.v1,t.v2]"},
					{"     ├─ filters: [{(A, ∞), (NULL, ABCDE)}]"},
					{"     └─ columns: [i v1 v2]"},
				},
			},
			{
				Query: "update t set v1 = concat(v1, 'Z') where v1 >= 'A'",
				Expected: []sql.Row{
					{sql.OkResult{RowsAffected: 4, InsertID: 0, Info: plan.UpdateInfo{Matched: 4, Updated: 4}}},
				},
			},
			{
				Query: "explain update t set v1 = concat(v1, 'Z') where v1 >= 'A'",
				Expected: []sql.Row{
					{"Update"},
					{" └─ UpdateSource(SET t.v1 = concat(t.v1, 'Z'))"},
					{"     └─ Filter(t.v1 >= 'A')"},
					{"         └─ IndexedTableAccess(t)"},
					{"             ├─ index: [t.v1,t.v2]"},
					{"             └─ filters: [{[A, ∞), [NULL, ∞)}]"},
				},
			},
			{
				Query: "select * from t",
				Expected: []sql.Row{
					{0, "aZ", "a"},
					{1, "abZ", "ab"},
					{2, "abcZ", "abc"},
					{3, "abcdeZ", "abcde"},
				},
			},
			{
				Query: "delete from t where v1 >= 'A'",
				Expected: []sql.Row{
					{sql.OkResult{RowsAffected: 4}},
				},
			},
			{
				Query: "explain delete from t where v1 >= 'A'",
				Expected: []sql.Row{
					{"Delete"},
					{" └─ Filter(t.v1 >= 'A')"},
					{"     └─ IndexedTableAccess(t)"},
					{"         ├─ index: [t.v1,t.v2]"},
					{"         └─ filters: [{[A, ∞), [NULL, ∞)}]"},
				},
			},
			{
				Query:    "select * from t",
				Expected: []sql.Row{},
			},
		},
	},
	// TODO: these should eventually go in GMS, but it doesn't currently support index rewrite on column modify
	{
		Name: "drop prefix lengths when modifying column to non string type",
		SetUpScript: []string{
			"create table t (j varchar(100), index (j(10)))",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "alter table t modify column j int",
				Expected: []sql.Row{{sql.OkResult{}}},
			},
			{
				Query:    "show create table t",
				Expected: []sql.Row{{"t", "CREATE TABLE `t` (\n  `j` int,\n  KEY `j` (`j`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "drop prefix length when modifying columns to invalid string type",
		SetUpScript: []string{
			"create table t (j varchar(100), index (j(10)))",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "alter table t modify column j varchar(2)",
				Expected: []sql.Row{{sql.OkResult{}}},
			},
			{
				Query:    "show create table t",
				Expected: []sql.Row{{"t", "CREATE TABLE `t` (\n  `j` varchar(2),\n  KEY `j` (`j`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "preserve prefix length when modifying column to valid string type",
		SetUpScript: []string{
			"create table t (j varchar(100), index (j(10)))",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "alter table t modify column j varchar(200)",
				Expected: []sql.Row{{sql.OkResult{}}},
			},
			{
				Query:    "show create table t",
				Expected: []sql.Row{{"t", "CREATE TABLE `t` (\n  `j` varchar(200),\n  KEY `j` (`j`(10))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "preserve prefix lengths when there are other unchanged prefix lengths",
		SetUpScript: []string{
			"create table t (i varchar(100), j varchar(100), index (i(10), j(10)))",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "alter table t modify column j int",
				Expected: []sql.Row{{sql.OkResult{}}},
			},
			{
				Query:    "show create table t",
				Expected: []sql.Row{{"t", "CREATE TABLE `t` (\n  `i` varchar(100),\n  `j` int,\n  KEY `ij` (`i`(10),`j`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "prefix length too long",
		SetUpScript: []string{
			"create table t (i blob, index(i(3072)))",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:       "alter table t modify column i text",
				ExpectedErr: sql.ErrKeyTooLong,
			},
		},
	},
}
