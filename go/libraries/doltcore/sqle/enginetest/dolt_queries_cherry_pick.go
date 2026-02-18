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
	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/sql"
)

var DoltCherryPickTests = []queries.ScriptTest{
	{
		Name: "error cases: basic validation",
		SetUpScript: []string{
			"create table t (pk int primary key, v varchar(100));",
			"call dolt_commit('-Am', 'create table t');",
			"call dolt_checkout('-b', 'branch1');",
			"insert into t values (1, \"one\");",
			"call dolt_commit('-am', 'adding row 1');",
			"set @commit1 = hashof('HEAD');",
			"call dolt_checkout('main');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "CALL Dolt_Cherry_Pick('HEAD~100');",
				ExpectedErrStr: "invalid ancestor spec",
			},
			{
				Query:          "CALL Dolt_Cherry_Pick('abcdaaaaaaaaaaaaaaaaaaaaaaaaaaaa');",
				ExpectedErrStr: "target commit not found",
			},
			{
				Query:          "CALL Dolt_Cherry_Pick('--abort');",
				ExpectedErrStr: "error: There is no cherry-pick merge to abort",
			},
		},
	},
	{
		Name: "error cases: merge commits cannot be cherry-picked",
		SetUpScript: []string{
			"create table t (pk int primary key, v varchar(100));",
			"call dolt_commit('-Am', 'create table t');",
			"call dolt_checkout('-b', 'branch1');",
			"insert into t values (1, \"one\");",
			"call dolt_commit('-am', 'adding row 1');",
			"set @commit1 = hashof('HEAD');",
			"call dolt_checkout('main');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL dolt_merge('--no-ff', 'branch1');",
				Expected: []sql.Row{{doltCommit, 0, 0, "merge successful"}},
			},
			{
				Query:          "CALL dolt_cherry_pick('HEAD');",
				ExpectedErrStr: "cherry-picking a merge commit is not supported",
			},
		},
	},
	{
		Name: "error cases: error with staged or unstaged changes ",
		SetUpScript: []string{
			"create table t (pk int primary key, v varchar(100));",
			"call dolt_commit('-Am', 'create table t');",
			"call dolt_checkout('-b', 'branch1');",
			"insert into t values (1, \"one\");",
			"call dolt_commit('-am', 'adding row 1');",
			"set @commit1 = hashof('HEAD');",
			"call dolt_checkout('main');",
			"INSERT INTO t VALUES (100, 'onehundy');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "CALL Dolt_Cherry_Pick(@commit1);",
				ExpectedErrStr: "cannot cherry-pick with uncommitted changes",
			},
			{
				Query:    "call dolt_add('t');",
				Expected: []sql.Row{{0}},
			},
			{
				Query:          "CALL Dolt_Cherry_Pick(@commit1);",
				ExpectedErrStr: "cannot cherry-pick with uncommitted changes",
			},
		},
	},
	{
		Name: "error cases: different primary keys",
		SetUpScript: []string{
			"create table t (pk int primary key, v varchar(100));",
			"call dolt_commit('-Am', 'create table t');",
			"call dolt_checkout('-b', 'branch1');",
			"ALTER TABLE t DROP PRIMARY KEY, ADD PRIMARY KEY (pk, v);",
			"call dolt_commit('-am', 'adding row 1');",
			"set @commit1 = hashof('HEAD');",
			"call dolt_checkout('main');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "CALL Dolt_Cherry_Pick(@commit1);",
				ExpectedErrStr: "error: cannot merge because table t has different primary keys",
			},
		},
	},
	{
		Name: "basic case",
		SetUpScript: []string{
			"create table t (pk int primary key, v varchar(100));",
			"call dolt_commit('-Am', 'create table t');",
			"call dolt_checkout('-b', 'branch1');",
			"insert into t values (1, \"one\");",
			"call dolt_commit('-am', 'adding row 1');",
			"set @commit1 = hashof('HEAD');",
			"insert into t values (2, \"two\");",
			"call dolt_commit('-am', 'adding row 2');",
			"set @commit2 = hashof('HEAD');",
			"call dolt_checkout('main');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * FROM t;",
				Expected: []sql.Row{},
			},
			{
				Query:    "call dolt_cherry_pick(@commit2);",
				Expected: []sql.Row{{doltCommit, 0, 0, 0}},
			},
			{
				Query:    "SELECT * FROM t;",
				Expected: []sql.Row{{2, "two"}},
			},
			{
				Query:    "call dolt_cherry_pick(@commit1);",
				Expected: []sql.Row{{doltCommit, 0, 0, 0}},
			},
			{
				Query:    "SELECT * FROM t order by pk;",
				Expected: []sql.Row{{1, "one"}, {2, "two"}},
			},
			{
				// Assert that our new commit only has one parent (i.e. not a merge commit)
				Query:    "select count(*) from dolt_commit_ancestors where commit_hash = hashof('HEAD');",
				Expected: []sql.Row{{1}},
			},
		},
	},
	{
		Name: "keyless table",
		SetUpScript: []string{
			"call dolt_checkout('main');",
			"CREATE TABLE keyless (id int, name varchar(10));",
			"call dolt_commit('-Am', 'create table keyless on main');",
			"call dolt_checkout('-b', 'branch1');",
			"INSERT INTO keyless VALUES (1,'1'), (2,'3');",
			"call dolt_commit('-am', 'insert rows into keyless table on branch1');",
			"call dolt_checkout('main');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * FROM keyless;",
				Expected: []sql.Row{},
			},
			{
				Query:    "CALL DOLT_CHERRY_PICK('branch1');",
				Expected: []sql.Row{{doltCommit, 0, 0, 0}},
			},
			{
				Query:    "SELECT * FROM keyless;",
				Expected: []sql.Row{{1, "1"}, {2, "3"}},
			},
		},
	},
	{
		Name: "schema change: CREATE TABLE",
		SetUpScript: []string{
			"call dolt_checkout('-b', 'branch1');",
			"CREATE TABLE table_a (pk BIGINT PRIMARY KEY, v varchar(10));",
			"INSERT INTO table_a VALUES (11, 'aa'), (22, 'ab');",
			"call dolt_commit('-Am', 'create table table_a');",
			"set @commit1 = hashof('HEAD');",
			"call dolt_checkout('main');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SHOW TABLES;",
				Expected: []sql.Row{},
			},
			{
				Query:    "call dolt_cherry_pick(@commit1);",
				Expected: []sql.Row{{doltCommit, 0, 0, 0}},
			},
			{
				// Assert that our new commit only has one parent (i.e. not a merge commit)
				Query:    "select count(*) from dolt_commit_ancestors where commit_hash = hashof('HEAD');",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SHOW TABLES;",
				Expected: []sql.Row{{"table_a"}},
			},
			{
				Query:    "SELECT * FROM table_a;",
				Expected: []sql.Row{{11, "aa"}, {22, "ab"}},
			},
		},
	},
	{
		Name: "schema change: DROP TABLE",
		SetUpScript: []string{
			"CREATE TABLE dropme (pk BIGINT PRIMARY KEY, v varchar(10));",
			"INSERT INTO dropme VALUES (11, 'aa'), (22, 'ab');",
			"call dolt_commit('-Am', 'create table dropme');",
			"call dolt_checkout('-b', 'branch1');",
			"drop table dropme;",
			"call dolt_commit('-Am', 'drop table dropme');",
			"set @commit1 = hashof('HEAD');",
			"call dolt_checkout('main');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SHOW TABLES;",
				Expected: []sql.Row{{"dropme"}},
			},
			{
				Query:    "call dolt_cherry_pick(@commit1);",
				Expected: []sql.Row{{doltCommit, 0, 0, 0}},
			},
			{
				Query:    "SHOW TABLES;",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "schema change: ALTER TABLE ADD COLUMN",
		SetUpScript: []string{
			"create table test(pk int primary key);",
			"call dolt_commit('-Am', 'create table test on main');",
			"call dolt_checkout('-b', 'branch1');",
			"ALTER TABLE test ADD COLUMN v VARCHAR(100);",
			"call dolt_commit('-am', 'add column v to test on branch1');",
			"set @commit1 = hashof('HEAD');",
			"call dolt_checkout('main');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_cherry_pick(@commit1);",
				Expected: []sql.Row{{doltCommit, 0, 0, 0}},
			},
			{
				Query:    "SHOW CREATE TABLE test;",
				Expected: []sql.Row{{"test", "CREATE TABLE `test` (\n  `pk` int NOT NULL,\n  `v` varchar(100),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "schema change: ALTER TABLE DROP COLUMN",
		SetUpScript: []string{
			"create table test(pk int primary key, v varchar(100));",
			"call dolt_commit('-Am', 'create table test on main');",
			"call dolt_checkout('-b', 'branch1');",
			"ALTER TABLE test DROP COLUMN v;",
			"call dolt_commit('-am', 'drop column v from test on branch1');",
			"set @commit1 = hashof('HEAD');",
			"call dolt_checkout('main');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_cherry_pick(@commit1);",
				Expected: []sql.Row{{doltCommit, 0, 0, 0}},
			},
			{
				Query:    "SHOW CREATE TABLE test;",
				Expected: []sql.Row{{"test", "CREATE TABLE `test` (\n  `pk` int NOT NULL,\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "schema change: ALTER TABLE RENAME COLUMN",
		SetUpScript: []string{
			"create table test(pk int primary key, v1 varchar(100));",
			"call dolt_commit('-Am', 'create table test on main');",
			"call dolt_checkout('-b', 'branch1');",
			"ALTER TABLE test RENAME COLUMN v1 to v2;",
			"call dolt_commit('-am', 'rename column v1 to v2 in test on branch1');",
			"set @commit1 = hashof('HEAD');",
			"call dolt_checkout('main');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_cherry_pick(@commit1);",
				Expected: []sql.Row{{doltCommit, 0, 0, 0}},
			},
			{
				Query:    "SHOW CREATE TABLE test;",
				Expected: []sql.Row{{"test", "CREATE TABLE `test` (\n  `pk` int NOT NULL,\n  `v2` varchar(100),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "abort (@@autocommit=0)",
		SetUpScript: []string{
			"SET @@autocommit=0;",
			"create table t (pk int primary key, v varchar(100));",
			"insert into t values (1, 'one');",
			"call dolt_commit('-Am', 'create table t');",
			"call dolt_checkout('-b', 'branch1');",
			"update t set v=\"uno\" where pk=1;",
			"call dolt_commit('-Am', 'updating row 1 -> uno');",
			"alter table t drop column v;",
			"call dolt_commit('-am', 'drop column v');",
			"call dolt_checkout('main');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_cherry_pick(hashof('branch1'));",
				Expected: []sql.Row{{"", 1, 0, 0}},
			},
			{
				Query:    "select * from dolt_conflicts;",
				Expected: []sql.Row{{"t", uint64(1)}},
			},
			{
				Query: "select base_pk, base_v, our_pk, our_diff_type, their_pk, their_diff_type from dolt_conflicts_t;",
				Expected: []sql.Row{
					{1, "uno", 1, "modified", 1, "modified"},
				},
			},
			{
				Query:    "call dolt_cherry_pick('--abort');",
				Expected: []sql.Row{{"", 0, 0, 0}},
			},
			{
				Query:    "select * from dolt_conflicts;",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{1, "one"}},
			},
		},
	},
	{
		Name: "abort (@@autocommit=1)",
		SetUpScript: []string{
			"SET @@autocommit=1;",
			"SET @@dolt_allow_commit_conflicts=1;",
			"create table t (pk int primary key, v varchar(100));",
			"insert into t values (1, 'one');",
			"call dolt_commit('-Am', 'create table t');",
			"call dolt_checkout('-b', 'branch1');",
			"update t set v=\"uno\" where pk=1;",
			"call dolt_commit('-Am', 'updating row 1 -> uno');",
			"alter table t drop column v;",
			"call dolt_commit('-am', 'drop column v');",
			"call dolt_checkout('main');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_cherry_pick(hashof('branch1'));",
				Expected: []sql.Row{{"", 1, 0, 0}},
			},
			{
				Query:    "select * from dolt_conflicts;",
				Expected: []sql.Row{{"t", uint64(1)}},
			},
			{
				Query: "select base_pk, base_v, our_pk, our_diff_type, their_pk, their_diff_type from dolt_conflicts_t;",
				Expected: []sql.Row{
					{1, "uno", 1, "modified", 1, "modified"},
				},
			},
			{
				Query:    "call dolt_cherry_pick('--abort');",
				Expected: []sql.Row{{"", 0, 0, 0}},
			},
			{
				Query:    "select * from dolt_conflicts;",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{1, "one"}},
			},
		},
	},
	{
		Name: "conflict resolution (@@autocommit=0)",
		SetUpScript: []string{
			"SET @@autocommit=0;",
			"create table t (pk int primary key, v varchar(100));",
			"insert into t values (1, 'one');",
			"call dolt_commit('-Am', 'create table t');",
			"call dolt_checkout('-b', 'branch1');",
			"update t set v=\"uno\" where pk=1;",
			"call dolt_commit('-Am', 'updating row 1 -> uno');",
			"alter table t drop column v;",
			"call dolt_commit('-am', 'drop column v');",
			"call dolt_checkout('main');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_cherry_pick(hashof('branch1'));",
				Expected: []sql.Row{{"", 1, 0, 0}},
			},
			{
				Query:    "select * from dolt_conflicts;",
				Expected: []sql.Row{{"t", uint64(1)}},
			},
			{
				Query:    "select * from dolt_status",
				Expected: []sql.Row{{"t", byte(0), "modified"}, {"t", byte(0), "conflict"}},
			},
			{
				Query: "select base_pk, base_v, our_pk, our_diff_type, their_pk, their_diff_type from dolt_conflicts_t;",
				Expected: []sql.Row{
					{1, "uno", 1, "modified", 1, "modified"},
				},
			},
			{
				Query:    "call dolt_conflicts_resolve('--ours', 't');",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "select * from dolt_status",
				Expected: []sql.Row{{"t", byte(0), "modified"}},
			},
			{
				Query:    "select * from dolt_conflicts;",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "call dolt_commit('-am', 'committing cherry-pick');",
				Expected: []sql.Row{{doltCommit}},
			},
			{
				// Assert that our new commit only has one parent (i.e. not a merge commit)
				Query:    "select count(*) from dolt_commit_ancestors where commit_hash = hashof('HEAD');",
				Expected: []sql.Row{{1}},
			},
		},
	},
	{
		Name: "conflict resolution (@@autocommit=1)",
		SetUpScript: []string{
			"set @@autocommit=1;",
			"SET @@dolt_allow_commit_conflicts=1;",
			"create table t (pk int primary key, c1 varchar(100));",
			"call dolt_commit('-Am', 'creating table t');",
			"insert into t values (1, \"one\");",
			"call dolt_commit('-Am', 'inserting row 1');",
			"SET @commit1 = hashof('HEAD');",
			"update t set c1=\"uno\" where pk=1;",
			"call dolt_commit('-Am', 'updating row 1 -> uno');",
			"update t set c1=\"ein\" where pk=1;",
			"call dolt_commit('-Am', 'updating row 1 -> ein');",
			"SET @commit2 = hashof('HEAD');",
			"call dolt_reset('--hard', @commit1);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * from dolt_status;",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * from t;",
				Expected: []sql.Row{{1, "one"}},
			},
			{
				Query:    `CALL dolt_cherry_pick(@commit2);`,
				Expected: []sql.Row{{"", 1, 0, 0}},
			},
			{
				Query:    `SELECT * FROM dolt_conflicts;`,
				Expected: []sql.Row{{"t", uint64(1)}},
			},
			{
				Query:    `commit;`,
				Expected: []sql.Row{},
			},
			{
				Query:    `SELECT * FROM dolt_conflicts;`,
				Expected: []sql.Row{{"t", uint64(1)}},
			},
			{
				Query:    `SELECT base_pk, base_c1, our_pk, our_c1, their_diff_type, their_pk, their_c1 FROM dolt_conflicts_t;`,
				Expected: []sql.Row{{1, "uno", 1, "one", "modified", 1, "ein"}},
			},
			{
				Query:    `SELECT * FROM t;`,
				Expected: []sql.Row{{1, "one"}},
			},
			{
				Query:    `call dolt_conflicts_resolve('--theirs', 't');`,
				Expected: []sql.Row{{0}},
			},
			{
				Query:    `SELECT * FROM t;`,
				Expected: []sql.Row{{1, "ein"}},
			},
			{
				Query:    "call dolt_commit('-am', 'committing cherry-pick');",
				Expected: []sql.Row{{doltCommit}},
			},
			{
				// Assert that our new commit only has one parent (i.e. not a merge commit)
				Query:    "select count(*) from dolt_commit_ancestors where commit_hash = hashof('HEAD');",
				Expected: []sql.Row{{1}},
			},
		},
	},
	{
		Name: "abort (@@autocommit=1) with ignored table",
		SetUpScript: []string{
			"INSERT INTO dolt_ignore VALUES ('generated_*', 1);",
			"CREATE TABLE generated_foo (pk int PRIMARY KEY);",
			"CREATE TABLE generated_bar (pk int PRIMARY KEY);",
			"insert into generated_foo values (1);",
			"insert into generated_bar values (1);",
			"SET @@autocommit=1;",
			"SET @@dolt_allow_commit_conflicts=1;",
			"create table t (pk int primary key, v varchar(100));",
			"insert into t values (1, 'one');",
			"call dolt_add('--force', 'generated_bar');",
			"call dolt_commit('-Am', 'create table t');",
			"call dolt_checkout('-b', 'branch1');",
			"update t set v=\"uno\" where pk=1;",
			"call dolt_commit('-Am', 'updating row 1 -> uno');",
			"alter table t drop column v;",
			"call dolt_commit('-am', 'drop column v');",
			"call dolt_checkout('main');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_cherry_pick(hashof('branch1'));",
				Expected: []sql.Row{{"", 1, 0, 0}},
			},
			{
				Query:    "select * from dolt_conflicts;",
				Expected: []sql.Row{{"t", uint64(1)}},
			},
			{
				Query: "select base_pk, base_v, our_pk, our_diff_type, their_pk, their_diff_type from dolt_conflicts_t;",
				Expected: []sql.Row{
					{1, "uno", 1, "modified", 1, "modified"},
				},
			},
			{
				Query: "insert into generated_foo values (2);",
			},
			/*
				// TODO: https://github.com/dolthub/dolt/issues/7411
				// see below
				{
					Query: "insert into generated_bar values (2);",
				},
			*/
			{
				Query:    "call dolt_cherry_pick('--abort');",
				Expected: []sql.Row{{"", 0, 0, 0}},
			},
			{
				Query:    "select * from dolt_conflicts;",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{1, "one"}},
			},
			{
				// An ignored table should still be present (and unstaged) after aborting the merge.
				Query:    "select * from dolt_status;",
				Expected: []sql.Row{{"generated_foo", byte(0), "new table"}},
			},
			{
				// Changes made to the table during the merge should not be reverted.
				Query:    "select * from generated_foo;",
				Expected: []sql.Row{{1}, {2}},
			},
			/*{
				// TODO: https://github.com/dolthub/dolt/issues/7411
				// The table that was force-added should be treated like any other table
				// and reverted to its state before the merge began.
				Query:    "select * from generated_bar;",
				Expected: []sql.Row{{1}},
			},*/
		},
	},
}
