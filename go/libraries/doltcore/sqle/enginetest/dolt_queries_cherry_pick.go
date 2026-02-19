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
	"time"

	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// timeValidator validates that a value is a time.Time with the expected date/time
type timeValidator struct {
	expectedTime time.Time
}

var _ enginetest.CustomValueValidator = &timeValidator{}

func (tv *timeValidator) Validate(val interface{}) (bool, error) {
	t, ok := val.(time.Time)
	if !ok {
		return false, nil
	}
	return t.Equal(tv.expectedTime), nil
}

func timeEquals(dateStr string) *timeValidator {
	t, _ := time.Parse("2006-01-02T15:04:05Z", dateStr)
	return &timeValidator{expectedTime: t}
}

var DoltCherryPickTests = []queries.ScriptTest{
	{
		Name: "error cases: basic validation",
		SetUpScript: []string{
			"create table t (pk int primary key, v varchar(100));",
			"call dolt_commit('-Am', 'create table t');",
			"call dolt_checkout('-b', 'branch1');",
			"insert into t values (1, \"one\");",
			"call dolt_commit('-am', 'adding row 1');",
			"set @commit1 = dolt_hashof('HEAD');",
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
			"set @commit1 = dolt_hashof('HEAD');",
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
			"set @commit1 = dolt_hashof('HEAD');",
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
			"set @commit1 = dolt_hashof('HEAD');",
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
			"set @commit1 = dolt_hashof('HEAD');",
			"insert into t values (2, \"two\");",
			"call dolt_commit('-am', 'adding row 2');",
			"set @commit2 = dolt_hashof('HEAD');",
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
				Query:    "select count(*) from dolt_commit_ancestors where commit_hash = dolt_hashof('HEAD');",
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
			"set @commit1 = dolt_hashof('HEAD');",
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
				Query:    "select count(*) from dolt_commit_ancestors where commit_hash = dolt_hashof('HEAD');",
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
			"set @commit1 = dolt_hashof('HEAD');",
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
			"set @commit1 = dolt_hashof('HEAD');",
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
			"set @commit1 = dolt_hashof('HEAD');",
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
			"set @commit1 = dolt_hashof('HEAD');",
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
				Query:    "call dolt_cherry_pick(dolt_hashof('branch1'));",
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
				Query:    "call dolt_cherry_pick(dolt_hashof('branch1'));",
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
				Query:    "call dolt_cherry_pick(dolt_hashof('branch1'));",
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
				Query:    "select count(*) from dolt_commit_ancestors where commit_hash = dolt_hashof('HEAD');",
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
				Query:    "select count(*) from dolt_commit_ancestors where commit_hash = dolt_hashof('HEAD');",
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
				Query:    "call dolt_cherry_pick(dolt_hashof('branch1'));",
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
	{
		Name: "cherry-pick --continue: successful conflict resolution workflow",
		SetUpScript: []string{
			"create table t (pk int primary key, v varchar(100));",
			"call dolt_commit('-Am', 'create table t');",
			"call dolt_checkout('-b', 'branch1');",
			"insert into t values (1, 'branch1_value');",
			"call dolt_commit('-am', 'add row from branch1', '--author', 'Test User <test@example.com>', '--date', '2022-01-01T12:00:00');",
			"set @commit1 = dolt_hashof('HEAD');",
			"call dolt_checkout('main');",
			"insert into t values (1, 'main_value');",
			"call dolt_commit('-am', 'add row from main');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "set @@dolt_allow_commit_conflicts = 1;",
				Expected: []sql.Row{{types.OkResult{}}},
			},
			{
				Query:    "call dolt_cherry_pick(@commit1);",
				Expected: []sql.Row{{"", 1, 0, 0}},
			},
			{
				Query:    "select * from dolt_conflicts;",
				Expected: []sql.Row{{"t", uint64(1)}},
			},
			{
				Query: "select our_pk, our_v, their_pk, their_v from dolt_conflicts_t;",
				Expected: []sql.Row{
					{1, "main_value", 1, "branch1_value"},
				},
			},
			{
				Query:    "call dolt_cherry_pick('--continue');",
				Expected: []sql.Row{{"", 1, 0, 0}},
			},
			{
				Query:            "delete from dolt_conflicts_t",
				SkipResultsCheck: true,
			},
			{
				Query:    "update t set v = 'resolved_value' where pk = 1;",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			{
				Query:    "call dolt_add('t');",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "call dolt_cherry_pick('--continue');",
				Expected: []sql.Row{{doltCommit, 0, 0, 0}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{1, "resolved_value"}},
			},
			{
				Query:    "select committer, email, message, date from dolt_log limit 1;",
				Expected: []sql.Row{{"Test User", "test@example.com", "add row from branch1", timeEquals("2022-01-01T12:00:00Z")}},
			},
		},
	},
	{
		Name: "cherry-pick --continue not in a cherry-pick state",
		SetUpScript: []string{
			"create table t (pk int primary key, v varchar(100));",
			"call dolt_commit('-Am', 'create table t');",
			"call dolt_checkout('-b', 'branch1');",
			"insert into t values (1, 'one');",
			"call dolt_commit('-am', 'add row from branch1');",
			"set @commit1 = dolt_hashof('HEAD');",
			"call dolt_checkout('main');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "call dolt_cherry_pick('--continue');",
				ExpectedErrStr: "error: There is no cherry-pick merge to continue",
			},
		},
	},
	{
		Name: "cherry-pick --continue: multiple table conflicts",
		SetUpScript: []string{
			"create table t1 (pk int primary key, v varchar(100));",
			"create table t2 (pk int primary key, v varchar(100));",
			"call dolt_commit('-Am', 'create tables');",
			"call dolt_checkout('-b', 'branch1');",
			"insert into t1 values (1, 'branch1_t1');",
			"insert into t2 values (1, 'branch1_t2');",
			"call dolt_commit('-am', 'add rows from branch1', '--author', 'Branch User <branch@example.com>', '--date', '2022-02-01T10:30:00');",
			"set @commit1 = dolt_hashof('HEAD');",
			"call dolt_checkout('main');",
			"insert into t1 values (1, 'main_t1');",
			"insert into t2 values (1, 'main_t2');",
			"call dolt_commit('-am', 'add rows from main');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "set @@dolt_allow_commit_conflicts = 1;",
				Expected: []sql.Row{{types.OkResult{}}},
			},
			{
				Query:    "call dolt_cherry_pick(@commit1);",
				Expected: []sql.Row{{"", 2, 0, 0}},
			},
			{
				Query:    "select `table` from dolt_conflicts order by `table`;",
				Expected: []sql.Row{{"t1"}, {"t2"}},
			},
			{
				Query:            "update t1 set v = 'resolved_t1' where pk = 1;",
				SkipResultsCheck: true,
			},
			{
				Query:            "delete from dolt_conflicts_t1;",
				SkipResultsCheck: true,
			},
			{
				Query:            "call dolt_add('t1');",
				SkipResultsCheck: true,
			},
			{
				// Should still have one remaining conflict in t2.
				Query:    "call dolt_cherry_pick('--continue');",
				Expected: []sql.Row{{"", 1, 0, 0}},
			},
			{
				Query:            "update t2 set v = 'resolved_t2' where pk = 1;",
				SkipResultsCheck: true,
			},
			{
				Query:            "delete from dolt_conflicts_t2;",
				SkipResultsCheck: true,
			},
			{
				Query:            "call dolt_add('t2');",
				SkipResultsCheck: true,
			},
			{
				Query:    "call dolt_cherry_pick('--continue');",
				Expected: []sql.Row{{doltCommit, 0, 0, 0}},
			},
			{
				Query:    "select * from t1;",
				Expected: []sql.Row{{1, "resolved_t1"}},
			},
			{
				Query:    "select * from t2;",
				Expected: []sql.Row{{1, "resolved_t2"}},
			},
			{
				Query:    "select * from dolt_conflicts;",
				Expected: []sql.Row{},
			},
			{
				Query:    "select committer, email, message, date from dolt_log limit 1;",
				Expected: []sql.Row{{"Branch User", "branch@example.com", "add rows from branch1", timeEquals("2022-02-01T10:30:00Z")}},
			},
		},
	},
	{
		Name: "cherry-pick --continue: mutually exclusive with --abort",
		SetUpScript: []string{
			"create table t (pk int primary key, v varchar(100));",
			"call dolt_commit('-Am', 'create table t');",
			"call dolt_checkout('-b', 'branch1');",
			"insert into t values (1, 'branch1_value');",
			"call dolt_commit('-am', 'add row from branch1');",
			"set @commit1 = dolt_hashof('HEAD');",
			"call dolt_checkout('main');",
			"insert into t values (1, 'main_value');",
			"call dolt_commit('-am', 'add row from main');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "set @@dolt_allow_commit_conflicts = 1;",
				Expected: []sql.Row{{types.OkResult{}}},
			},
			{
				Query:    "call dolt_cherry_pick(@commit1);",
				Expected: []sql.Row{{"", 1, 0, 0}},
			},
			{
				Query:          "call dolt_cherry_pick('--continue', '--abort');",
				ExpectedErrStr: "error: --continue and --abort are mutually exclusive",
			},
			{
				Query:          "call dolt_cherry_pick('--abort', '--continue');",
				ExpectedErrStr: "error: --continue and --abort are mutually exclusive",
			},
		},
	},
	{
		Name: "cherry-pick: constraint violations only (no merge state)",
		SetUpScript: []string{
			"create table t (pk int primary key, v varchar(100));",
			"call dolt_commit('-Am', 'create table t');",
			"call dolt_checkout('-b', 'branch1');",
			// On branch1, insert a value that will violate constraint"
			"insert into t values (1, 'forbidden');",
			"call dolt_commit('-am', 'add forbidden value');",
			"set @commit1 = dolt_hashof('HEAD');",
			"call dolt_checkout('main');",
			// Add constraint on main after the branch
			"alter table t add CONSTRAINT chk_not_forbidden CHECK (v != 'forbidden');",
			"call dolt_commit('-am', 'add check constraint');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "set @@dolt_allow_commit_conflicts = 1;",
				Expected: []sql.Row{{types.OkResult{}}},
			},
			{
				Query:    "set @@dolt_force_transaction_commit = 1;",
				Expected: []sql.Row{{types.OkResult{}}},
			},
			{
				Query:    "call dolt_cherry_pick(@commit1);",
				Expected: []sql.Row{{"", 0, 0, 1}}, // 1 constraint violation
			},
			{
				Query:    "select violation_type, pk, v from dolt_constraint_violations_t;",
				Expected: []sql.Row{{"check constraint", 1, "forbidden"}},
			},
			{
				// Try to continue with constraint violations still present
				Query:    "call dolt_cherry_pick('--continue');",
				Expected: []sql.Row{{"", 0, 0, 1}},  // Still has constraint violation
			},
			{
				// Fix the violation
				Query:    "update t set v = 'allowed' where pk = 1;",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			{
				Query:            "delete from dolt_constraint_violations_t;",
				SkipResultsCheck: true,
			},
			{
				Query:    "call dolt_add('t');",
				Expected: []sql.Row{{0}},
			},
			{
				// Now continue should succeed and preserve original commit metadata
				Query:    "call dolt_cherry_pick('--continue');",
				Expected: []sql.Row{{doltCommit, 0, 0, 0}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{1, "allowed"}},
			},
		},
	},
	{
		Name: "cherry-pick --continue: with both conflicts and constraint violations",
		SetUpScript: []string{
			"create table t (pk int primary key, v varchar(100));",
			"insert into t values (1, 'initial');",
			"call dolt_commit('-Am', 'create table t and row');",
			"call dolt_checkout('-b', 'branch1');",
			"-- On branch1, modify existing row and add new row with value that will violate constraint",
			"update t set v = 'branch1_value' where pk = 1;",
			"insert into t values (2, 'forbidden');",
			"call dolt_commit('-am', 'modify row 1 and add row 2 with forbidden value');",
			"set @commit1 = dolt_hashof('HEAD');",
			"call dolt_checkout('main');",
			"-- On main, change row 1 to create conflict and add constraint",
			"update t set v = 'main_value' where pk = 1;",
			"alter table t add CONSTRAINT chk_not_forbidden CHECK (v != 'forbidden');",
			"call dolt_commit('-am', 'main changes and add constraint');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "set @@dolt_allow_commit_conflicts = 1;",
				Expected: []sql.Row{{types.OkResult{}}},
			},
			{
				Query:    "set @@dolt_force_transaction_commit = 1;",
				Expected: []sql.Row{{types.OkResult{}}},
			},
			{
				Query:    "call dolt_cherry_pick(@commit1);",
				Expected: []sql.Row{{"", 1, 0, 1}}, // 1 data conflict, 1 constraint violation
			},
			{
				Query:    "select * from dolt_conflicts;",
				Expected: []sql.Row{{"t", uint64(1)}},
			},
			{
				Query:    "select violation_type, pk, v from dolt_constraint_violations_t;",
				Expected: []sql.Row{{"check constraint", 2, "forbidden"}},
			},
			{
				// Try to continue with both conflicts and violations
				Query:    "call dolt_cherry_pick('--continue');",
				Expected: []sql.Row{{"", 1, 0, 1}}, // Still has both issues
			},
			{
				// Resolve the conflict
				Query:            "update t set v = 'resolved_value' where pk = 1;",
				SkipResultsCheck: true,
			},
			{
				Query:            "delete from dolt_conflicts_t;",
				SkipResultsCheck: true,
			},
			{
				Query:            "call dolt_add('t');",
				SkipResultsCheck: true,
			},
			{
				// Try again - still has constraint violation
				Query:    "call dolt_cherry_pick('--continue');",
				Expected: []sql.Row{{"", 0, 0, 1}}, // Only constraint violation remains
			},
			{
				// Fix the constraint violation
				Query:            "delete from t where pk = 2;",
				SkipResultsCheck: true,
			},
			{
				Query:            "delete from dolt_constraint_violations_t;",
				SkipResultsCheck: true,
			},
			{
				Query:            "call dolt_add('t');",
				SkipResultsCheck: true,
			},
			{
				// Now continue should succeed
				Query:    "call dolt_cherry_pick('--continue');",
				Expected: []sql.Row{{doltCommit, 0, 0, 0}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{1, "resolved_value"}},
			},
		},
	},
}
