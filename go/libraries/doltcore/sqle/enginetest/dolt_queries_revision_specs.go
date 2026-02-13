// Copyright 2026 Dolthub, Inc.
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
				// The database name is always the requested name
				Query:    "select database()",
				Expected: []sql.Row{{"mydb/tag1~"}},
			},
			{
				Query:    "show databases;",
				Expected: []sql.Row{{"mydb"}, {"mydb/tag1~"}, {"information_schema"}, {"mysql"}},
			},
			{
				// The branch is nil in the case of a non-branch revision DB
				Query:    "select active_branch()",
				Expected: []sql.Row{{nil}},
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
			{
				Query:          "select * from dolt_hashof('HEAD^0');",
				ExpectedErrStr: "invalid ancestor spec",
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
				// The database name is always the requested name
				Query:    "select database()",
				Expected: []sql.Row{{"mydb/tag1"}},
			},
			{
				// The branch is nil in the case of a non-branch revision DB
				Query:    "select active_branch()",
				Expected: []sql.Row{{nil}},
			},
			{
				Query:    "show databases;",
				Expected: []sql.Row{{"mydb"}, {"mydb/tag1"}, {"information_schema"}, {"mysql"}},
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
				Expected: []sql.Row{{0, "Switched to branch 'main'"}},
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
				Expected: []sql.Row{{"mydb"}, {"mydb/branch1"}, {"information_schema"}, {"mysql"}},
			},
			{
				// The database name is always the requested name
				Query:    "select database()",
				Expected: []sql.Row{{"mydb/branch1"}},
			},
			{
				Query:    "select active_branch()",
				Expected: []sql.Row{{"branch1"}},
			},
			{
				Query:    "select * from t01",
				Expected: []sql.Row{{1, 1}, {2, 2}},
			},
			{
				Query:    "call dolt_checkout('main');",
				Expected: []sql.Row{{0, "Switched to branch 'main'"}},
			},
			{
				// TODO: the behavior here is a bit odd: when we call dolt_checkout, we change the current database to the
				//  base database name. But we should also consider the connection string: if you connect to a revision
				//  database, that database should always be visible.
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
				Expected: []sql.Row{{"mydb"}, {"mydb/branch1"}, {"information_schema"}, {"mysql"}},
			},
			{
				// Create a table in the working set to verify the main db
				Query:    "create table working_set_table(pk int primary key);",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
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
				Expected: []sql.Row{{0, "Switched to branch 'branch1'"}},
			},
			{
				Query:    "select table_name from dolt_diff where commit_hash='WORKING';",
				Expected: []sql.Row{{"working_set_table"}},
			},
		},
	},
	{
		Name: "database revision specs: dolt_checkout uses revision database name for DbData access",
		SetUpScript: []string{
			"create database newtest;",
			"use newtest;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select active_branch();",
				Expected: []sql.Row{{"main"}},
			},
			{
				Query:    "call dolt_checkout('-b', 'branch-to-delete');",
				Expected: []sql.Row{{0, "Switched to branch 'branch-to-delete'"}},
			},
			{
				Query:    "select active_branch();",
				Expected: []sql.Row{{"branch-to-delete"}},
			},
			{
				Query:    "use `newtest/main`;",
				Expected: []sql.Row{},
			},
			{
				Query:    "select active_branch();",
				Expected: []sql.Row{{"main"}},
			},
			{
				Query:    "call dolt_branch('-D', 'branch-to-delete');",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "select active_branch();",
				Expected: []sql.Row{{"main"}},
			},
			{
				Query:    "call dolt_checkout('-b', 'another-branch');",
				Expected: []sql.Row{{0, "Switched to branch 'another-branch'"}},
			},
			{
				Query:    "select active_branch();",
				Expected: []sql.Row{{"another-branch"}},
			},
		},
	},
	{
		Name: "database revision specs: can checkout a table",
		SetUpScript: []string{
			"call dolt_checkout('main')",
			"create table t01 (pk int primary key, c1 int)",
			"call dolt_add('t01');",
			"call dolt_commit('-am', 'creating table t01 on branch1');",
			"insert into t01 values (1, 1), (2, 2);",
			"call dolt_branch('new-branch')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "show databases;",
				Expected: []sql.Row{{"mydb"}, {"information_schema"}, {"mysql"}},
			},
			{
				Query:    "use `mydb/main`;",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from dolt_status",
				Expected: []sql.Row{{"t01", byte(0), "modified"}},
			},
			{
				Query:    "call dolt_checkout('t01')",
				Expected: []sql.Row{{0, ""}},
			},
			{
				Query: "select * from dolt_status",
				// Expected: []sql.Row{},
				SkipResultsCheck: true, // TODO: https://github.com/dolthub/dolt/issues/5816
			},
		},
	},
	// https://github.com/dolthub/dolt/issues/10382
	{
		Name: "database revision specs: prisma detects existing _prisma_migrations on branch",
		SetUpScript: []string{
			"create table t01 (pk int primary key);",
			"call dolt_add('.');",
			"call dolt_commit('-am', 'init');",
			"call dolt_branch('newbranch');",
			"use `mydb@newbranch`;",
			"set @schema = database();",
			"PREPARE stmt_list_base_tables FROM 'SELECT DISTINCT table_info.table_name AS table_name FROM information_schema.tables AS table_info JOIN information_schema.columns AS column_info ON column_info.table_name = table_info.table_name WHERE table_info.table_schema = ? AND column_info.table_schema = ? AND table_info.table_type = ''BASE TABLE'' ORDER BY table_info.table_name';",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CREATE TABLE _prisma_migrations (id VARCHAR(36) PRIMARY KEY NOT NULL, checksum VARCHAR(64) NOT NULL, finished_at DATETIME(3), migration_name VARCHAR(255) NOT NULL, logs TEXT, rolled_back_at DATETIME(3), started_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3), applied_steps_count INTEGER UNSIGNED NOT NULL DEFAULT 0);",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "SELECT id, checksum, finished_at, migration_name, logs, rolled_back_at, started_at, applied_steps_count FROM _prisma_migrations ORDER BY started_at ASC;",
				Expected: []sql.Row{},
			},
			{
				Query:    "execute stmt_list_base_tables using @schema, @schema;",
				Expected: []sql.Row{{"_prisma_migrations"}, {"t01"}},
			},
			{
				Query:    "set dolt_show_branch_databases = on;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "execute stmt_list_base_tables using @schema, @schema;",
				Expected: []sql.Row{{"_prisma_migrations"}, {"t01"}},
			},
			{
				Query:    "use `mydb/newbranch`;",
				Expected: []sql.Row{},
			},
			{
				Query:    "select database();",
				Expected: []sql.Row{{"mydb/newbranch"}},
			},
			{
				Query:    "set @schema = database();",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "execute stmt_list_base_tables using @schema, @schema;",
				Expected: []sql.Row{{"_prisma_migrations"}, {"t01"}},
			},
		},
	},
	{
		Name: "database revision specs: db revision delimiter alias '@' when dolt_enable_revision_delimiter_alias is ON",
		SetUpScript: []string{
			"create table t01 (pk int primary key, c1 int);",
			"call dolt_add('.');",
			"call dolt_commit('-am', 'creating table t01 on main');",
			"insert into t01 values (1, 1), (2, 2);",
			"call dolt_commit('-am', 'adding rows to table t01 on main');",
			"call dolt_tag('tag1');",
			"call dolt_branch('branch1');",
			"insert into t01 values (3, 3);",
			"call dolt_commit('-am', 'adding another row to table t01 on main');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "use `mydb@main`;",
				Expected: []sql.Row{},
			},
			{
				Query:    "show databases;",
				Expected: []sql.Row{{"information_schema"}, {"mydb"}, {"mydb@main"}, {"mysql"}},
			},
			{
				Query:    "use `mydb@branch1`;",
				Expected: []sql.Row{},
			},
			{
				Query:    "show databases;",
				Expected: []sql.Row{{"information_schema"}, {"mydb"}, {"mydb@branch1"}, {"mysql"}},
			},
			{
				Query:    "select database();",
				Expected: []sql.Row{{"mydb@branch1"}},
			},
			{
				Query:    "set dolt_show_branch_databases = on;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "show databases;",
				Expected: []sql.Row{{"information_schema"}, {"mydb"}, {"mydb/main"}, {"mydb@branch1"}, {"mysql"}},
			},
			{
				Query:    "select active_branch();",
				Expected: []sql.Row{{"branch1"}},
			},
			{
				Query:    "select * from t01;",
				Expected: []sql.Row{{1, 1}, {2, 2}},
			},
			{
				Query:    "select column_name from information_schema.columns where table_schema = database() and table_name = 't01' order by ordinal_position;",
				Expected: []sql.Row{{"pk"}, {"c1"}},
			},
			{
				Query:    "select table_name from information_schema.tables where table_schema = database() and table_name = 't01';",
				Expected: []sql.Row{{"t01"}},
			},
			{
				Query:    "use mydb;",
				Expected: []sql.Row{},
			},
			{
				Query:    "show databases;",
				Expected: []sql.Row{{"information_schema"}, {"mydb"}, {"mydb/main"}, {"mydb/branch1"}, {"mysql"}},
			},
			{
				Query:    "select * from `mydb@branch1`.t01;",
				Expected: []sql.Row{{1, 1}, {2, 2}},
			},
			{
				Query:    "select * from `mydb@tag1`.t01;",
				Expected: []sql.Row{{1, 1}, {2, 2}},
			},
			{
				Query:          "drop database `mydb@branch1`;",
				ExpectedErrStr: "unable to drop revision database: mydb@branch1",
			},
			{
				Query:          "create database `mydb@branch1`;",
				ExpectedErrStr: "can't create database mydb@branch1; database exists",
			},
			{
				Query:    "use `mydb@branch1`;",
				Expected: []sql.Row{},
			},
			{
				Query:    "show databases;",
				Expected: []sql.Row{{"information_schema"}, {"mydb"}, {"mydb/main"}, {"mydb@branch1"}, {"mysql"}},
			},
			{
				Query:    "create table parent(id int primary key);",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "create table child(id int primary key, pid int, foreign key (pid) references `mydb@branch1`.parent(id));",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "insert into parent values (1);",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "insert into child values (1, 1);",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "use `mydb/branch1`;",
				Expected: []sql.Row{},
			},
			{
				Query:    "show databases;",
				Expected: []sql.Row{{"information_schema"}, {"mydb"}, {"mydb/main"}, {"mydb/branch1"}, {"mysql"}},
			},
			{
				Query:    "select database();",
				Expected: []sql.Row{{"mydb/branch1"}},
			},
			{
				Query:    "select * from t01;",
				Expected: []sql.Row{{1, 1}, {2, 2}},
			},
			{
				Query:    "select column_name from information_schema.columns where table_schema = database() and table_name = 't01' order by ordinal_position;",
				Expected: []sql.Row{{"pk"}, {"c1"}},
			},
			{
				Query:    "select table_name from information_schema.tables where table_schema = database() and table_name = 't01';",
				Expected: []sql.Row{{"t01"}},
			},
			{
				Query:    "select * from parent;",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "select * from child;",
				Expected: []sql.Row{{1, 1}},
			},
			{
				Query:    "use mydb;",
				Expected: []sql.Row{},
			},
			{
				Query:       "create schema `mydb@branch1`;",
				ExpectedErr: sql.ErrDatabaseExists,
			},
		},
	},
	{
		Name: "database revision specs: db revision delimiter alias '@' when dolt_enable_revision_delimiter_alias is OFF",
		SetUpScript: []string{
			"create table t01 (pk int primary key, c1 int);",
			"call dolt_add('.');",
			"call dolt_commit('-am', 'creating table t01 on main');",
			"call dolt_branch('branch1');",
			"set dolt_enable_revision_delimiter_alias = 0;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:       "use `mydb@branch1`;",
				ExpectedErr: sql.ErrDatabaseNotFound,
			},
			{
				Query:       "select * from `mydb@branch1`.t01;",
				ExpectedErr: sql.ErrDatabaseNotFound,
			},
			{
				Query:    "use `mydb/branch1`;",
				Expected: []sql.Row{},
			},
			{
				Query:       "use `mydb@branch1`;",
				ExpectedErr: sql.ErrDatabaseNotFound,
			},
			{
				Query:    "create table t02(pk int primary key);",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "select * from t02;",
				Expected: []sql.Row{},
			},
			{
				Query: "call dolt_commit('-Am', 'add t2 table to branch');",
			},
			{
				Query:    "use `mydb/main`;",
				Expected: []sql.Row{},
			},
			{
				Query:    "create schema `mydb@branch1`;",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "use `mydb@branch1`;",
				Expected: []sql.Row{},
			},
			{
				Query:       "select * from t02;",
				ExpectedErr: sql.ErrTableNotFound,
			},
			{
				Query:    "show databases;",
				Expected: []sql.Row{{"information_schema"}, {"mydb"}, {"mydb@branch1"}, {"mysql"}},
			},
			{
				Query:    "set dolt_show_branch_databases = on;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "show databases;",
				Expected: []sql.Row{{"information_schema"}, {"mydb"}, {"mydb/branch1"}, {"mydb/main"}, {"mydb@branch1"}, {"mydb@branch1/main"}, {"mysql"}},
			},
			{
				Query:    "set dolt_enable_revision_delimiter_alias = 1;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "select * from t02;",
				Expected: []sql.Row{},
			},
			{
				Query:    "show databases;",
				Expected: []sql.Row{{"information_schema"}, {"mydb"}, {"mydb/main"}, {"mydb@branch1"}, {"mysql"}},
			},
			{
				Query:    "set dolt_enable_revision_delimiter_alias = 0;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "show databases;",
				Expected: []sql.Row{{"information_schema"}, {"mydb"}, {"mydb/branch1"}, {"mydb/main"}, {"mydb@branch1"}, {"mydb@branch1/main"}, {"mysql"}},
			},
		},
	},
	{
		Name: "database revision specs: commit id with revision delimiter alias '@'",
		SetUpScript: []string{
			"create table t01 (pk int primary key, c1 int);",
			"call dolt_add('.');",
			"call dolt_commit('-am', 'creating table t01 on main');",
			"set @h = (select hashof('main') limit 1);",
			"set @use_sql = concat('use `mydb@', @h, '`');",
			"prepare use_stmt from @use_sql;",
			"insert into t01 values (1, 1), (2, 2);",
			"call dolt_commit('-am', 'adding rows to table t01 on main');",
			"set @h = (select hashof('main') limit 1);",
			"set @select_sql = concat('select * from `mydb@', @h, '`.t01');",
			"prepare select_stmt from @select_sql;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "execute use_stmt;",
				Expected: []sql.Row{},
			},
			{
				Query:    "select length(database());",
				Expected: []sql.Row{{37}},
			},
			{
				Query:    "select * from t01;",
				Expected: []sql.Row{},
			},
			{
				Query:    "execute select_stmt;",
				Expected: []sql.Row{{1, 1}, {2, 2}},
			},
		},
	},
}
