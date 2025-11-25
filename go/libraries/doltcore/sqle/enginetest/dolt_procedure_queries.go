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
	"os"
	"path/filepath"

	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/env"
)

func init() {
	DoltProcedureTests = append(DoltProcedureTests, BackupsProcedureScripts...)
}

// fileUrl returns a file:// URL path.
func fileUrl(path string) string {
	path = filepath.Join(os.TempDir(), path)
	return "file://" + filepath.ToSlash(filepath.Clean(path))
}

// awsUrl returns an aws:// URL with the given dynamo table, S3 bucket, and database path.
func awsUrl(dynamoTable, s3Bucket, path string) string {
	return fmt.Sprintf("aws://[%s:%s]/%s", dynamoTable, s3Bucket, path)
}

var BackupsProcedureScripts = []queries.ScriptTest{
	{
		Name: "dolt_backup add",
		SetUpScript: []string{
			fmt.Sprintf("call dolt_backup('add', 'bak1', '%s');", fileUrl("dolt_backup1")),
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    fmt.Sprintf("call dolt_backup('add', 'bak2', '%s');", fileUrl("dolt_backup2")),
				Expected: []sql.Row{{0}},
			},
			{
				Query: "select * from dolt_backups order by name;",
				Expected: []sql.Row{
					{"bak1", fileUrl("dolt_backup1"), gmstypes.JSONDocument{Val: map[string]interface{}{}}},
					{"bak2", fileUrl("dolt_backup2"), gmstypes.JSONDocument{Val: map[string]interface{}{}}},
				},
			},
			{
				// Invalid URLs are accepted but will fail when used in 'sync'.
				Query:    "call dolt_backup('add', 'bak3', 'invalid://url');",
				Expected: []sql.Row{{0}},
			},
			{
				Query: "select * from dolt_backups order by name",
				Expected: []sql.Row{
					{"bak1", fileUrl("dolt_backup1"), gmstypes.JSONDocument{Val: map[string]interface{}{}}},
					{"bak2", fileUrl("dolt_backup2"), gmstypes.JSONDocument{Val: map[string]interface{}{}}},
					{"bak3", "invalid://url", gmstypes.JSONDocument{Val: map[string]interface{}{}}},
				},
			},
			{
				Query:    fmt.Sprintf("call dolt_backup('add', 'aws_params', '%s', '--aws-region=<region>', '--aws-creds-type=file', '--aws-creds-file=<file>', '--aws-creds-profile=<profile>');", awsUrl("test-dynamo", "test-bucket", "testdb-params")),
				Expected: []sql.Row{{0}},
			},
			{
				Query:    fmt.Sprintf("call dolt_backup('add', 'aws_partial', '%s', '--aws-region=eu-west-1', '--aws-creds-profile=<profile>');", awsUrl("test-dynamo", "test-bucket", "testdb-partial")),
				Expected: []sql.Row{{0}},
			},
			{
				Query: "select * from dolt_backups where url like 'aws://%' order by name;",
				Expected: []sql.Row{
					{
						"aws_params",
						awsUrl("test-dynamo", "test-bucket", "testdb-params"),
						gmstypes.JSONDocument{
							Val: map[string]interface{}{
								"aws-region":        "<region>",
								"aws-creds-type":    "file",
								"aws-creds-file":    "<file>",
								"aws-creds-profile": "<profile>",
							},
						},
					},
					{
						"aws_partial",
						awsUrl("test-dynamo", "test-bucket", "testdb-partial"),
						gmstypes.JSONDocument{
							Val: map[string]interface{}{
								"aws-creds-profile": "<profile>",
								"aws-region":        "eu-west-1",
							},
						},
					},
				},
			},
			{
				Query:       fmt.Sprintf("call dolt_backup('add', 'aws_conflict', '%s', '--aws-region=<region>', '--aws-creds-type=file', '--aws-creds-file=<file>', '--aws-creds-profile=<profile>');", awsUrl("test-dynamo", "test-bucket", "testdb-params")),
				ExpectedErr: env.ErrRemoteAddressConflict,
			},
			{
				Query:          fmt.Sprintf("call dolt_backup('add', 'aws_conflict', '%s', '--aws-creds-type=<err>');", awsUrl("test-dynamo", "test-bucket", "testdb-params")),
				ExpectedErrStr: "<err> is not a valid option for 'aws-creds-type'. valid options are: role|env|file",
			},
			{
				Query:          "call dolt_backup('add', 'bak2');",
				ExpectedErrStr: "usage: dolt_backup('add', 'name', 'url', ['--aws-region=<region>'], ['--aws-creds-type=<type>'], ['--aws-creds-file=<file>'], ['--aws-creds-profile=<profile>'])",
			},
			{
				Query:          "call dolt_backup('add');",
				ExpectedErrStr: "usage: dolt_backup('add', 'name', 'url', ['--aws-region=<region>'], ['--aws-creds-type=<type>'], ['--aws-creds-file=<file>'], ['--aws-creds-profile=<profile>'])",
			},
			{
				Query:          fmt.Sprintf("call dolt_backup('add', 'bak1', '%s');", fileUrl("dolt_backup1")),
				ExpectedErrStr: "backup 'bak1' already exists",
			},
			{
				Query:          fmt.Sprintf("call dolt_backup('add', '', '%s');", fileUrl("dolt_backup2")),
				ExpectedErrStr: "backup name '' is invalid",
			},
			{
				Query:          "call dolt_backup('add', 'bak2', '');",
				ExpectedErrStr: "backup URL '' is invalid",
			},
			{
				Query:          fmt.Sprintf("call dolt_backup('add', 'backup with spaces', '%s');", fileUrl("dolt_backup2")),
				ExpectedErrStr: "backup name 'backup with spaces' is invalid",
			},
			{
				Query:          fmt.Sprintf("call dolt_backup('add', 'backup/slash', '%s');", fileUrl("dolt_backup2")),
				ExpectedErrStr: "backup name 'backup/slash' is invalid",
			},
		},
	},
	{
		Name: "dolt_backup remove",
		SetUpScript: []string{
			fmt.Sprintf("call dolt_backup('add', 'bak1', '%s');", fileUrl("dolt_backup1")),
			fmt.Sprintf("call dolt_backup('add', 'bak2', '%s');", fileUrl("dolt_backup2")),
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select * from dolt_backups order by name;",
				Expected: []sql.Row{
					{"bak1", fileUrl("dolt_backup1"), gmstypes.JSONDocument{Val: map[string]interface{}{}}},
					{"bak2", fileUrl("dolt_backup2"), gmstypes.JSONDocument{Val: map[string]interface{}{}}},
				},
			},
			{
				Query:    "call dolt_backup('rm', 'bak2');",
				Expected: []sql.Row{{0}},
			},
			{
				Query: "select * from dolt_backups order by name;",
				Expected: []sql.Row{
					{"bak1", fileUrl("dolt_backup1"), gmstypes.JSONDocument{Val: map[string]interface{}{}}},
				},
			},
			{
				Query:    fmt.Sprintf("call dolt_backup('add', 'bak2', '%s');", fileUrl("dolt_backup2")),
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "call dolt_backup('remove', 'bak1');",
				Expected: []sql.Row{{0}},
			},
			{
				Query: "select * from dolt_backups;",
				Expected: []sql.Row{
					{"bak2", fileUrl("dolt_backup2"), gmstypes.JSONDocument{Val: map[string]interface{}{}}},
				},
			},
			{
				Query:    "create table t (t text);",
				Expected: []sql.Row{{gmstypes.OkResult{}}},
			},
			{
				// Testing that remove only affects the dolt_backups table, but keeps the original backup intact.
				Query:    "call dolt_backup('sync', 'bak2')",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "call dolt_backup('remove', 'bak2');",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "drop table t;",
				Expected: []sql.Row{{gmstypes.OkResult{}}},
			},
			{
				Query:    fmt.Sprintf("call dolt_backup('restore', '%s', 'restored_db');", fileUrl("dolt_backup2")),
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "select * from restored_db.t;",
				Expected: []sql.Row{},
			},
			{
				Query:          "call dolt_backup('remove', 'nonexistent');",
				ExpectedErrStr: "backup 'nonexistent' not found",
			},
			{
				Query:          "call dolt_backup('remove');",
				ExpectedErrStr: "usage: dolt_backup('remove', 'name')",
			},
			{
				Query:          "call dolt_backup('remove', 'bak1', 'extra');",
				ExpectedErrStr: "usage: dolt_backup('remove', 'name')",
			},
			{
				Query:          "call dolt_backup('remove', '');",
				ExpectedErrStr: "backup '' not found",
			},
		},
	},
	{
		Name: "dolt_backup sync",
		SetUpScript: []string{
			fmt.Sprintf("call dolt_backup('add', 'bak1', '%s');", fileUrl("dolt_backup1")),
			"create table t(a int primary key, b int);",
			"insert into t values (1, 100), (2, 200);",
			"call dolt_add('t');",
			"call dolt_commit('-m', 'initial commit');",
			"call dolt_backup('add', 'invalid_backup', 'invalid://url');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call dolt_backup('sync', 'bak1');",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{1, 100}, {2, 200}},
			},
			{
				Query:          "call dolt_backup('sync', 'nonexistent');",
				ExpectedErrStr: "backup 'nonexistent' not found",
			},
			{
				Query:          "call dolt_backup('sync');",
				ExpectedErrStr: "usage: dolt_backup('sync', 'name')",
			},
			{
				Query:          "call dolt_backup('sync', 'dolt_backup1', 'extra');",
				ExpectedErrStr: "usage: dolt_backup('sync', 'name')",
			},
			{
				Query:          "call dolt_backup('sync', 'invalid_backup');",
				ExpectedErrStr: "unknown url scheme: 'invalid'",
			},
			{
				Query:          "call dolt_backup('sync', '');",
				ExpectedErrStr: "backup '' not found",
			},
		},
	},
	{
		Name: "dolt_backup sync-url",
		SetUpScript: []string{
			fmt.Sprintf("call dolt_backup('add', 'bak1', '%s');", fileUrl("dolt_backup1")),
			"create table t(a int primary key, b int);",
			"insert into t values (1, 100), (2, 200);",
			"call dolt_add('t');",
			"call dolt_commit('-m', 'initial commit');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    fmt.Sprintf("call dolt_backup('sync-url', '%s');", fileUrl("dolt_backup2")),
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{1, 100}, {2, 200}},
			},
			{
				Query:    fmt.Sprintf("call dolt_backup('sync-url', '%s');", fileUrl("dolt_backup1")),
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{1, 100}, {2, 200}},
			},
			{
				Query:          "call dolt_backup('sync-url');",
				ExpectedErrStr: "usage: dolt_backup('sync-url', 'remote_url', ['--aws-region=<region>'], ['--aws-creds-type=<type>'], ['--aws-creds-file=<file>'], ['--aws-creds-profile=<profile>'])",
			},
			{
				Query:          "call dolt_backup('sync-url', '', 'extra');",
				ExpectedErrStr: "usage: dolt_backup('sync-url', 'remote_url', ['--aws-region=<region>'], ['--aws-creds-type=<type>'], ['--aws-creds-file=<file>'], ['--aws-creds-profile=<profile>'])",
			},
			{
				Query:          "call dolt_backup('sync-url', 'invalid://url');",
				ExpectedErrStr: "unknown url scheme: 'invalid'",
			},
			{
				Query:          "call dolt_backup('sync-url', '');",
				ExpectedErrStr: "backup URL '' is invalid",
			},
		},
	},
	{
		Name: "dolt_backup restore",
		SetUpScript: []string{
			fmt.Sprintf("call dolt_backup('add', 'dolt_backup1', '%s');", fileUrl("dolt_backup1")),
			"call dolt_backup('sync', 'dolt_backup1');",
			"create table t(a int primary key, b int);",
			"insert into t values (1, 100), (2, 200);",
			"call dolt_add('t');",
			"call dolt_commit('-m', 'restore this commit');",
			fmt.Sprintf("call dolt_backup('add', 'dolt_backup2', '%s');", fileUrl("dolt_backup2")),
			"call dolt_backup('sync', 'dolt_backup2');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    fmt.Sprintf("call dolt_backup('restore', '%s', 'restored_db');", fileUrl("dolt_backup2")),
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "select * from restored_db.t order by a;",
				Expected: []sql.Row{{1, 100}, {2, 200}},
			},
			{
				Query:    "select message from restored_db.dolt_log order by commit_order;",
				Expected: []sql.Row{{"Initialize data repository"}, {"checkpoint enginetest database mydb"}, {"restore this commit"}},
			},
			{
				Query:          "call dolt_backup('restore');",
				ExpectedErrStr: "usage: dolt_backup('restore', 'remote_url', 'new_db_name', ['--aws-region=<region>'], ['--aws-creds-type=<type>'], ['--aws-creds-file=<file>'], ['--aws-creds-profile=<profile>'])",
			},
			{
				Query:          fmt.Sprintf("call dolt_backup('restore', '%s');", fileUrl("dolt_backup1")),
				ExpectedErrStr: "usage: dolt_backup('restore', 'remote_url', 'new_db_name', ['--aws-region=<region>'], ['--aws-creds-type=<type>'], ['--aws-creds-file=<file>'], ['--aws-creds-profile=<profile>'])",
			},
			{
				Query:          fmt.Sprintf("call dolt_backup('restore', '%s', 'restored_db');", fileUrl("dolt_backup2")),
				ExpectedErrStr: "database 'restored_db' already exists, use '--force' to overwrite",
			},
			{
				Query:    fmt.Sprintf("call dolt_backup('restore', '%s', 'restored_db', '--force');", fileUrl("dolt_backup1")),
				Expected: []sql.Row{{0}},
			},
			{
				Query:       "select * from restored_db.t",
				ExpectedErr: sql.ErrTableNotFound,
			},
			{
				Query:    "select message from restored_db.dolt_log order by commit_order;",
				Expected: []sql.Row{{"Initialize data repository"}, {"checkpoint enginetest database mydb"}},
			},
			{
				Query:          "call dolt_backup('restore', 'invalid://url', 'restored_db2');",
				ExpectedErrStr: "unknown url scheme: 'invalid'",
			},
		},
	},
	{
		Name: "dolt_backup error",
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          fmt.Sprintf("call dolt_backup('invalid', 'dolt_backup1', '%s');", fileUrl("dolt_backup1")),
				ExpectedErrStr: "unrecognized dolt_backup parameter 'invalid'",
			},
			{
				Query:          "call dolt_backup();",
				ExpectedErrStr: "use 'dolt_backups' table to list backups",
			},
			{
				Query:          "call dolt_backup('--verbose');",
				ExpectedErrStr: "use 'dolt_backups' table to list backups",
			},
		},
	},
}

var DoltProcedureTests = []queries.ScriptTest{
	{
		Name: "dolt_commit in a loop",
		SetUpScript: []string{
			"create table t(a int primary key auto_increment, b int);",
			"call dolt_commit('-Am', 'new table');",
			`create procedure commit_many()
begin
  declare i int default 1;
	commits: loop
		insert into t(b) values (i);
		call dolt_commit('-am', concat('inserted row ', cast (i as char)));
		if i >= 10 then
			leave commits;
		end if;
		set i = i + 1;
	end loop commits;
end
`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "call commit_many();",
				SkipResultsCheck: true, // return value is a bit odd, needs investigation
			},
			{
				Query:    "select * from t",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}, {6, 6}, {7, 7}, {8, 8}, {9, 9}, {10, 10}},
			},
			{
				Query:    "select count(*) from dolt_log;",
				Expected: []sql.Row{{13}}, // init, setup for test harness, initial commit in setup script, 10 commits in procedure
			},
			{
				Query:    "select * from t as of `HEAD~5`",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}},
			},
		},
	},
	{
		Name: "dolt_branch in a loop",
		SetUpScript: []string{
			"create table t(a int primary key auto_increment, b int);",
			"call dolt_commit('-Am', 'new table');",
			`create procedure branches()
begin
  declare i int default 1;
	commits: loop
		insert into t(b) values (i);
		call dolt_branch(concat('branch', i));
		if i >= 4 then
			leave commits;
		end if;
		set i = i + 1;
	end loop commits;
end
`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "call branches();",
				SkipResultsCheck: true, // return value is a bit odd, needs investigation
			},
			{
				Query:    "select name from dolt_branches order by 1",
				Expected: []sql.Row{{"branch1"}, {"branch2"}, {"branch3"}, {"branch4"}, {"main"}},
			},
			{
				Query:    "select * from t order by 1",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}, {4, 4}},
			},
		},
	},
	{
		Name: "dolt_branch in a loop, insert after branch",
		SetUpScript: []string{
			"create table t(a int primary key auto_increment, b int);",
			"call dolt_commit('-Am', 'new table');",
			`create procedure branches()
begin
  declare i int default 1;
	commits: loop
		call dolt_branch(concat('branch', i));
		insert into t(b) values (i);
		if i >= 4 then
			leave commits;
		end if;
		set i = i + 1;
	end loop commits;
end
`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "call branches();",
				SkipResultsCheck: true, // return value is a bit odd, needs investigation
			},
			{
				Query:    "select name from dolt_branches order by 1",
				Expected: []sql.Row{{"branch1"}, {"branch2"}, {"branch3"}, {"branch4"}, {"main"}},
			},
			{
				Query:    "select * from t order by 1",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}, {4, 4}},
			},
		},
	},
	{
		Name: "dolt_branch in conditional chain",
		SetUpScript: []string{
			"create table t(a int primary key auto_increment, b int);",
			"call dolt_commit('-Am', 'new table');",
			`create procedure branches()
begin
  declare i int default 1;
	commits: loop
		if i = 1 then
			insert into t(b) values (i);
			call dolt_branch('branch1');
		elseif i = 2 then
			call dolt_branch('branch2');
			insert into t(b) values (i);
		elseif i = 3 then
			insert into t(b) values (i);
			call dolt_branch('branch3');
		else 
			call dolt_branch('branch4');
			insert into t(b) values (i);
		end if;
		if i >= 4 then
			leave commits;
		end if;
		set i = i + 1;
	end loop commits;
end
`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "call branches();",
				SkipResultsCheck: true, // return value is a bit odd, needs investigation
			},
			{
				Query:    "select name from dolt_branches order by 1",
				Expected: []sql.Row{{"branch1"}, {"branch2"}, {"branch3"}, {"branch4"}, {"main"}},
			},
			{
				Query:    "select * from t order by 1",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}, {4, 4}},
			},
		},
	},
	{
		Name: "dolt_branch in case statement",
		SetUpScript: []string{
			"create table t(a int primary key auto_increment, b int);",
			"call dolt_commit('-Am', 'new table');",
			`create procedure branches()
begin
  declare i int default 1;
	commits: loop
		case i
	  when 1 then
			insert into t(b) values (i);
			call dolt_branch('branch1');
		when 2 then
			call dolt_branch('branch2');
			insert into t(b) values (i);
		when 3 then
			insert into t(b) values (i);
			call dolt_branch('branch3');
		else 
			call dolt_branch('branch4');
			insert into t(b) values (i);
		end case;
		if i >= 4 then
			leave commits;
		end if;
		set i = i + 1;
	end loop commits;
end
`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "call branches();",
				SkipResultsCheck: true, // return value is a bit odd, needs investigation
			},
			{
				Query:    "select name from dolt_branches order by 1",
				Expected: []sql.Row{{"branch1"}, {"branch2"}, {"branch3"}, {"branch4"}, {"main"}},
			},
			{
				Query:    "select * from t order by 1",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}, {4, 4}},
			},
		},
	},
	{
		Name: "dolt_branch in trigger",
		SetUpScript: []string{
			"create table t(a int primary key auto_increment, b int);",
			"create table t2(a int primary key auto_increment, b int);",
			"call dolt_commit('-Am', 'new table');",
			`create trigger branch_trigger after insert on t for each row
begin
  declare i int default 1;
	commits: loop
		case i
	  when 1 then
			insert into t2(b) values (i);
			call dolt_branch('branch1');
		when 2 then
			call dolt_branch('branch2');
			insert into t2(b) values (i);
		when 3 then
			insert into t2(b) values (i);
			call dolt_branch('branch3');
		else 
			call dolt_branch('branch4');
			insert into t2(b) values (i);
		end case;
		if i >= 4 then
			leave commits;
		end if;
		set i = i + 1;
	end loop commits;
end
`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "insert into t values (1, 1);",
				Expected: []sql.Row{
					{gmstypes.OkResult{RowsAffected: 1, InsertID: 1}},
				},
			},
			{
				Query: "select name from dolt_branches order by 1",
				Expected: []sql.Row{
					{"branch1"},
					{"branch2"},
					{"branch3"},
					{"branch4"},
					{"main"},
				},
			},
			{
				// For some reason, calling stored procedures disables inserts
				Skip:  true,
				Query: "select * from t2 order by 1",
				Expected: []sql.Row{
					{1, 1},
					{2, 2},
					{3, 3},
					{4, 4},
				},
			},
		},
	},
	{
		Name: "checkout new branch, insert, and commit in procedure",
		SetUpScript: []string{
			"create table t(a int primary key auto_increment, b int);",
			"call dolt_commit('-Am', 'new table');",
			`create procedure edit_on_branch()
begin
	call dolt_checkout('-b', 'branch1');
	insert into t(b) values (100);
	call dolt_commit('-am', 'new row');
	call dolt_checkout('main');
end
`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "call edit_on_branch();",
				Skip:             true,
				SkipResultsCheck: true, // return value is a bit odd, needs investigation
			},
			{
				Query:    "select active_branch()",
				Skip:     true,
				Expected: []sql.Row{{"main"}},
			},
			{
				Query:    "select * from t order by 1",
				Skip:     true,
				Expected: []sql.Row{},
			},
			{
				Query:    "select name from dolt_branches order by 1",
				Skip:     true,
				Expected: []sql.Row{{"branch1"}, {"main"}},
			},
			{
				Query:    "select * from `mydb/branch1`.t order by 1",
				Skip:     true,
				Expected: []sql.Row{{1, 100}},
			},
		},
	},
	{
		Name: "checkout existing branch and commit in procedure",
		SetUpScript: []string{
			"create table t(a int primary key auto_increment, b int);",
			"call dolt_commit('-Am', 'new table');",
			"call dolt_branch('branch1');",
			`create procedure edit_on_branch()
begin
	call dolt_checkout('branch1');
	insert into t(b) values (100);
	call dolt_commit('-am', 'new row');
	call dolt_checkout('main');
end
`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "call edit_on_branch();",
				Skip:             true,
				SkipResultsCheck: true, // return value is a bit odd, needs investigation
			},
			{
				Query:    "select active_branch()",
				Skip:     true,
				Expected: []sql.Row{{"main"}},
			},
			{
				Query:    "select * from t order by 1",
				Skip:     true,
				Expected: []sql.Row{},
			},
			{
				Query:    "select name from dolt_branches order by 1",
				Skip:     true,
				Expected: []sql.Row{{"branch1"}, {"main"}},
			},
			{
				Query:    "select * from `mydb/branch1`.t order by 1",
				Skip:     true,
				Expected: []sql.Row{{1, 100}},
			},
		},
	},
	{
		Name: "merge in procedure",
		SetUpScript: []string{
			"create table t(a int primary key auto_increment, b int);",
			"call dolt_commit('-Am', 'new table');",
			"call dolt_branch('branch1');",
			"insert into t(a, b) values (1, 100);",
			"call dolt_commit('-am', 'new row');",
			"call dolt_checkout('branch1');",
			"insert into t(a, b) values (2, 200);",
			"call dolt_commit('-am', 'new row on branch1');",
			"call dolt_checkout('main');",
			`create procedure merge_branch(branchName varchar(255))
begin
	call dolt_checkout(branchName);
	call dolt_merge('--no-ff', 'main');
	call dolt_checkout('main');
end
`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "call merge_branch('branch1');",
				SkipResultsCheck: true, // return value is a bit odd, needs investigation
			},
			{
				Query:    "select active_branch()",
				Expected: []sql.Row{{"main"}},
			},
			{
				Query:    "select * from t order by 1",
				Expected: []sql.Row{{1, 100}},
			},
			{
				Query:    "select name from dolt_branches order by 1",
				Expected: []sql.Row{{"branch1"}, {"main"}},
			},
			{
				Query:    "select * from `mydb/branch1`.t order by 1",
				Expected: []sql.Row{{1, 100}, {2, 200}},
			},
		},
	},

	{
		Name: "create and call procedure which exceeds 1024 bytes",
		SetUpScript: []string{
			`CREATE PROCEDURE long_proc()
BEGIN
  DECLARE long_text TEXT;
  SET long_text = CONCAT(
			'Lorem ipsum dolor sit amet, consectetur adipiscing elit. ',
			'Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. ',
			'Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris ',
			'nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in ',
			'reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. ',
			'Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia ',
			'deserunt mollit anim id est laborum.',
			'Lorem ipsum dolor sit amet, consectetur adipiscing elit. ',
			'Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. ',
			'Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris ',
			'nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in ',
			'reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. ',
			'Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia ',
			'deserunt mollit anim id est laborum.',
			'Lorem ipsum dolor sit amet, consectetur adipiscing elit. ',
			'Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. ',
			'Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris ',
			'nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in ',
			'reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. ',
			'Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia ',
			'deserunt mollit anim id est laborum.');
  SELECT SHA2(long_text,256) AS checksum, LENGTH(long_text) AS length;
END
`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "call long_proc();",
				Expected: []sql.Row{{"a702e99e5ee2dc03095bb2efd58e28330b6ea085d036249de82977a5c0dbb4be", 1335}},
			},
		},
	},
	{
		Name: "dolt_commit with --branch flag",
		SetUpScript: []string{
			"create table t(a int primary key, b int);",
			"insert into t values (1, 100);",
			"call dolt_commit('-Am', 'initial commit');",
			"call dolt_branch('br1');",
			"call dolt_checkout('br1');",
			"insert into t values (2, 200);",
			"call dolt_checkout('main');",
			"insert into t values (3, 300);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "call dolt_commit('--branch', 'nonexistent', '-a', '-m', 'commit to nonexistent branch');",
				ExpectedErrStr: "Could not load database mydb/nonexistent",
			},
			{
				Query:       "call dolt_commit('--branch', 'br1', '-a', '-m', 'commit on br1 from main');",
				ExpectedErr: nil,
			},
			{
				// verify commit didn't change our current branch.
				Query:    "select active_branch()",
				Expected: []sql.Row{{"main"}},
			},
			{
				// Verify that commit didn't change the content of the main branch.
				Query:    "select * from t order by a",
				Expected: []sql.Row{{1, 100}, {3, 300}},
			},
			{
				// Verify that main branch is still dirty (has uncommitted changes).
				Query:    "select table_name from dolt_status where staged = 0",
				Expected: []sql.Row{{"t"}},
			},
			{
				// Verify that br1 is not dirty (no uncommitted changes).
				Query:    "select table_name from `mydb/br1`.dolt_status where staged = 0",
				Expected: []sql.Row{},
			},
			{
				// Verify that br1 contains the committed data.
				Query:    "select * from `mydb/br1`.t order by a",
				Expected: []sql.Row{{1, 100}, {2, 200}},
			},
		},
	},
	{
		Name: "dolt_add with --branch flag",
		SetUpScript: []string{
			"create table t(a int primary key, b int);",
			"insert into t values (1, 100);",
			"call dolt_commit('-Am', 'initial commit');",
			"call dolt_branch('br1');",
			"call dolt_checkout('br1');",
			"insert into t values (2, 200);",
			"call dolt_checkout('main');",
			"insert into t values (3, 300);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "call dolt_add('--branch', 'nonexistent', 't');",
				ExpectedErrStr: "Could not load database mydb/nonexistent",
			},
			{
				Query:    "call dolt_add('--branch', 'br1', 't');",
				Expected: []sql.Row{{0}},
			},
			{
				// verify add didn't change our current branch.
				Query:    "select active_branch()",
				Expected: []sql.Row{{"main"}},
			},
			{
				// Verify that main branch still has unstaged changes
				Query:    "select table_name from dolt_status where staged = 0",
				Expected: []sql.Row{{"t"}},
			},
			{
				// Verify that br1 has staged changes from the add operation
				Query:    "select table_name from `mydb/br1`.dolt_status where staged = 1",
				Expected: []sql.Row{{"t"}},
			},
			{
				// Verify that br1 contains the expected data
				Query:    "select * from `mydb/br1`.t order by a",
				Expected: []sql.Row{{1, 100}, {2, 200}},
			},
			{
				// Verify that main still contains its data
				Query:    "select * from t order by a",
				Expected: []sql.Row{{1, 100}, {3, 300}},
			},
		},
	},
}
