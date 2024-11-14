// Copyright 2024 Dolthub, Inc.
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

var DoltWorkspaceScriptTests = []queries.ScriptTest{
	{
		Name: "dolt_workspace_* multiple edits of a single row",
		SetUpScript: []string{
			"create table tbl (pk int primary key, val int);",
			"call dolt_commit('-Am', 'creating table t');",

			"insert into tbl values (42,42);",
			"insert into tbl values (43,43);",
			"call dolt_commit('-am', 'inserting 2 rows at HEAD');",

			"update tbl set val=51 where pk=42;",
			"call dolt_add('tbl');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select * from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, true, "modified", 42, 51, 42, 42},
				},
			},
			{
				// Test case-insensitive table name
				Query: "select * from dolt_workspace_TBL",
				Expected: []sql.Row{
					{0, true, "modified", 42, 51, 42, 42},
				},
			},
			{
				Query: "update tbl set val= 108 where pk = 42;",
			},
			{
				Query: "select * from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, true, "modified", 42, 51, 42, 42},
					{1, false, "modified", 42, 108, 42, 51},
				},
			},
			{
				Query: "call dolt_add('tbl');",
			},
			{
				Query: "select * from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, true, "modified", 42, 108, 42, 42},
				},
			},
		},
	},
	{
		Name: "dolt_workspace_* single unstaged row",
		SetUpScript: []string{
			"create table tbl (pk int primary key, val int);",
			"call dolt_commit('-Am', 'creating table t');",

			"insert into tbl values (42,42);",
			"insert into tbl values (43,43);",
			"call dolt_commit('-am', 'inserting 2 rows at HEAD');",

			"update tbl set val=51 where pk=42;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select * from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, false, "modified", 42, 51, 42, 42},
				},
			},
		},
	},
	{
		Name: "dolt_workspace_* inserted row",
		SetUpScript: []string{
			"create table tbl (pk int primary key, val int);",
			"call dolt_commit('-Am', 'creating table t');",

			"insert into tbl values (42,42);",
			"insert into tbl values (43,43);",
			"call dolt_commit('-am', 'inserting 2 rows at HEAD');",
			"insert into tbl values (44,44);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select * from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, false, "added", 44, 44, nil, nil},
				},
			},
			{
				Query: "call dolt_add('tbl');",
			},
			{
				Query: "select * from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, true, "added", 44, 44, nil, nil},
				},
			},
			{
				Query: "update tbl set val = 108 where pk = 44;",
			},
			{
				Query: "select * from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, true, "added", 44, 44, nil, nil},
					{1, false, "modified", 44, 108, 44, 44},
				},
			},
		},
	},
	{
		Name: "dolt_workspace_* deleted row",
		SetUpScript: []string{
			"create table tbl (pk int primary key, val int);",
			"call dolt_commit('-Am', 'creating table t');",

			"insert into tbl values (42,42);",
			"insert into tbl values (43,43);",
			"call dolt_commit('-am', 'inserting 2 rows at HEAD');",
			"delete from tbl where pk = 42;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select * from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, false, "removed", nil, nil, 42, 42},
				},
			},
			{
				Query: "call dolt_add('tbl');",
			},
			{
				Query: "select * from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, true, "removed", nil, nil, 42, 42},
				},
			},
		},
	},
	{
		Name: "dolt_workspace_* clean workspace",
		SetUpScript: []string{
			"create table tbl (pk int primary key, val int);",
			"call dolt_commit('-Am', 'creating table t');",

			"insert into tbl values (42,42);",
			"insert into tbl values (43,43);",
			"call dolt_commit('-am', 'inserting 2 rows at HEAD');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select * from dolt_workspace_tbl",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from dolt_workspace_unknowntable",
				Expected: []sql.Row{},
			},
		},
	},

	{
		Name: "dolt_workspace_* created table",
		SetUpScript: []string{
			"create table tbl (pk int primary key, val int);",
			"insert into tbl values (42,42);",
			"insert into tbl values (43,43);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select * from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, false, "added", 42, 42, nil, nil},
					{1, false, "added", 43, 43, nil, nil},
				},
			},
			{
				Query: "call dolt_add('tbl');",
			},
			{
				Query: "select * from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, true, "added", 42, 42, nil, nil},
					{1, true, "added", 43, 43, nil, nil},
				},
			},
		},
	},
	{
		Name: "dolt_workspace_* dropped table",
		SetUpScript: []string{
			"create table tbl (pk int primary key, val int);",
			"call dolt_commit('-Am', 'creating table t');",

			"insert into tbl values (42,42);",
			"insert into tbl values (43,43);",
			"call dolt_commit('-am', 'inserting rows 3 rows at HEAD');",
			"drop table tbl",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select * from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, false, "removed", nil, nil, 42, 42},
					{1, false, "removed", nil, nil, 43, 43},
				},
			},
			{
				Query: "call dolt_add('tbl');",
			},
			{
				Query: "select * from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, true, "removed", nil, nil, 42, 42},
					{1, true, "removed", nil, nil, 43, 43},
				},
			},
		},
	},

	{
		Name: "dolt_workspace_* keyless table",
		SetUpScript: []string{
			"create table tbl (x int, y int);",
			"insert into tbl values (42,42);",
			"insert into tbl values (42,42);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select * from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, false, "added", 42, 42, nil, nil},
					{1, false, "added", 42, 42, nil, nil},
				},
			},

			{
				Query: "call dolt_add('tbl');",
			},
			{
				Query: "select * from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, true, "added", 42, 42, nil, nil},
					{1, true, "added", 42, 42, nil, nil},
				},
			},
			{
				Query: "insert into tbl values (42,42);",
			},
			{
				Query: "select * from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, true, "added", 42, 42, nil, nil},
					{1, true, "added", 42, 42, nil, nil},
					{2, false, "added", 42, 42, nil, nil},
				},
			},
		},
	},
	{
		Name: "dolt_workspace_* schema change",
		SetUpScript: []string{
			"create table tbl (pk int primary key, val int);",
			"call dolt_commit('-Am', 'creating table t');",

			"insert into tbl values (42,42);",
			"insert into tbl values (43,43);",
			"call dolt_commit('-am', 'inserting rows 3 rows at HEAD');",

			"update tbl set val=51 where pk=42;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select * from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, false, "modified", 42, 51, 42, 42},
				},
			},

			{
				Query: "ALTER TABLE tbl ADD COLUMN newcol CHAR(36)",
			},
			{
				Query: "select * from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, false, "modified", 42, 51, nil, 42, 42},
				},
			},
			{
				Query: "call dolt_add('tbl')",
			},
			{
				Query: "select * from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, true, "modified", 42, 51, nil, 42, 42},
				},
			},
			/* Three schemas are possible by having a schema change staged then altering the schema again.
			   Currently, it's unclear if/how dolt_workspace_* can/should present this since it's all about data changes, not schema changes.
				{
					Query: "ALTER TABLE tbl ADD COLUMN newcol2 float",
				},
				{
					Query: "select * from dolt_workspace_tbl",
					Expected: []sql.Row{
						{0, true, "modified", 42, 51, nil, 42, 42},
					},
				},
				{
					Query: "update tbl set val=59 where pk=42",
				},
				{
					Query: "select * from dolt_workspace_tbl",
					Expected: []sql.Row{
						{0, true, "modified", 42, 51, nil, 42, 42},
						{1, false, "modified", 42, 59, nil, nil, 42, 42}, //
					},
				},
			*/
		},
	},
	{
		Name: "dolt_workspace_* prevent illegal updates",
		SetUpScript: []string{
			"create table tbl (pk int primary key, val int);",
			"insert into tbl values (42,42);",
			"call dolt_commit('-Am', 'creating table tbl');",
			"update tbl set val=51 where pk=42;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "update dolt_workspace_tbl set to_val = 108 where id = 0;",
				ExpectedErrStr: "only update of column 'staged' is allowed",
			},
		},
	},
	{
		Name: "dolt_workspace_* modifies promote to staging",
		SetUpScript: []string{
			"create table tbl (pk int primary key, x int, y int);",
			"insert into tbl values (41,42,43);",
			"insert into tbl values (50,51,52);",
			"call dolt_commit('-Am', 'creating table tbl');",
			"update tbl set x=23",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select * from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, false, "modified", 41, 23, 43, 41, 42, 43},
					{1, false, "modified", 50, 23, 52, 50, 51, 52},
				},
			},
			{
				Query: "update dolt_workspace_tbl set staged = true where to_pk = 41;",
			},
			{
				Query: "select * from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, true, "modified", 41, 23, 43, 41, 42, 43},
					{1, false, "modified", 50, 23, 52, 50, 51, 52},
				},
			},
			{
				Query: "update dolt_workspace_tbl set staged = 1 where staged = 0;",
			},
			{
				Query: "select * from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, true, "modified", 41, 23, 43, 41, 42, 43},
					{1, true, "modified", 50, 23, 52, 50, 51, 52},
				},
			},
			{
				Query: "update tbl set y=81",
			},
			{
				Query: "select * from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, true, "modified", 41, 23, 43, 41, 42, 43},
					{1, true, "modified", 50, 23, 52, 50, 51, 52},
					{2, false, "modified", 41, 23, 81, 41, 23, 43},
					{3, false, "modified", 50, 23, 81, 50, 23, 52},
				},
			},
			{
				Query: "select sum(y) from tbl AS OF STAGED",
				Expected: []sql.Row{
					{float64(95)},
				},
			},
			{
				Query: "select sum(y) from tbl AS OF WORKING",
				Expected: []sql.Row{
					{float64(162)},
				},
			},
			{
				// add everything.
				Query: "update dolt_workspace_tbl set staged = 1",
			},
			{
				Query: "select * from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, true, "modified", 41, 23, 81, 41, 42, 43},
					{1, true, "modified", 50, 23, 81, 50, 51, 52},
				},
			},
		},
	},
	{
		Name: "dolt_workspace_* inserts promote to staging",
		SetUpScript: []string{
			"create table tbl (pk int primary key, x int, y int);",
			"call dolt_commit('-Am', 'creating table tbl');",
			"insert into tbl values (41,42,43);",
			"insert into tbl values (50,51,52);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select * from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, false, "added", 41, 42, 43, nil, nil, nil},
					{1, false, "added", 50, 51, 52, nil, nil, nil},
				},
			},
			{
				Query: "update dolt_workspace_tbl set staged = true where to_pk = 41;",
			},
			{
				Query: "select * from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, true, "added", 41, 42, 43, nil, nil, nil},
					{1, false, "added", 50, 51, 52, nil, nil, nil},
				},
			},
			{
				Query: "update dolt_workspace_tbl set staged = 1 where staged = 0;",
			},
			{
				Query: "select * from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, true, "added", 41, 42, 43, nil, nil, nil},
					{1, true, "added", 50, 51, 52, nil, nil, nil},
				},
			},
			{
				Query: "update tbl set x=81",
			},
			{
				Query: "select * from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, true, "added", 41, 42, 43, nil, nil, nil},
					{1, true, "added", 50, 51, 52, nil, nil, nil},
					{2, false, "modified", 41, 81, 43, 41, 42, 43},
					{3, false, "modified", 50, 81, 52, 50, 51, 52},
				},
			},
			{
				// add everything.
				Query: "update dolt_workspace_tbl set staged = 1",
			},
			{
				Query: "select * from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, true, "added", 41, 81, 43, nil, nil, nil},
					{1, true, "added", 50, 81, 52, nil, nil, nil},
				},
			},
		},
	},
	{
		Name: "dolt_workspace_* deletes promote to staging",
		SetUpScript: []string{
			"create table tbl (pk int primary key);",
			"insert into tbl values (41);",
			"insert into tbl values (50);",
			"call dolt_commit('-Am', 'creating table tbl');",
			"delete from tbl",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select * from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, false, "removed", nil, 41},
					{1, false, "removed", nil, 50},
				},
			},
			{
				Query: "update dolt_workspace_tbl set staged = true where id = 0;",
			},
			{
				Query: "select * from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, true, "removed", nil, 41},
					{1, false, "removed", nil, 50},
				},
			},
			{
				Query: "update dolt_workspace_tbl set staged = 1 where staged = 0;",
			},
			{
				Query: "select * from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, true, "removed", nil, 41},
					{1, true, "removed", nil, 50},
				},
			},
			{
				Query: "insert into tbl values (41);",
			},
			{
				Query: "select * from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, true, "removed", nil, 41},
					{1, true, "removed", nil, 50},
					{2, false, "added", 41, nil},
				},
			},
			{
				// add everything. Insert of 41 should negate the staged remove.
				Query: "update dolt_workspace_tbl set staged = 1",
			},
			{
				Query: "select * from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, true, "removed", nil, 50},
				},
			},
		},
	},
	{
		Name: "dolt_workspace_* modifies downgrade from staging",
		SetUpScript: []string{
			"create table tbl (pk int primary key, v char(36));",
			"insert into tbl values (42, UUID());",
			"insert into tbl values (23, UUID());",
			"call dolt_commit('-Am', 'creating table tbl');",
			"update tbl set v = UUID();",
			"call dolt_add('tbl')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select id, staged, diff_type, to_pk from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, true, "modified", 23},
					{1, true, "modified", 42},
				},
			},
			{
				Query: "update dolt_workspace_tbl set staged = false where id = 0",
			},
			{
				Query: "select id, staged, diff_type, to_pk from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, true, "modified", 42},
					{1, false, "modified", 23},
				},
			},
			{
				Query: "update dolt_workspace_tbl set staged = 0 where staged = TRUE",
			},
			{
				Query: "select id, staged, diff_type, to_pk from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, false, "modified", 23},
					{1, false, "modified", 42},
				},
			},
		},
	},
	{
		Name: "dolt_workspace_* inserts downgrade from staging",
		SetUpScript: []string{
			"create table tbl (pk int primary key, v char(36));",
			"call dolt_commit('-Am', 'creating table tbl');",
			"insert into tbl values (42, UUID());",
			"insert into tbl values (23, UUID());",
			"call dolt_add('tbl')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select id, staged, diff_type, to_pk from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, true, "added", 23},
					{1, true, "added", 42},
				},
			},
			{
				Query: "update dolt_workspace_tbl set staged = false where id = 0",
			},
			{
				Query: "select id, staged, diff_type, to_pk from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, true, "added", 42},
					{1, false, "added", 23},
				},
			},
			{
				Query: "update dolt_workspace_tbl set staged = 0 where staged = TRUE",
			},
			{
				Query: "select id, staged, diff_type, to_pk from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, false, "added", 23},
					{1, false, "added", 42},
				},
			},
		},
	},
	{
		Name: "dolt_workspace_* deletes downgrade from staging",
		SetUpScript: []string{
			"create table tbl (pk int primary key, v char(36));",
			"insert into tbl values (42, UUID());",
			"insert into tbl values (23, UUID());",
			"call dolt_commit('-Am', 'creating table tbl');",
			"delete from tbl",
			"call dolt_add('tbl')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select id, staged, diff_type, from_pk, to_pk from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, true, "removed", 23, nil},
					{1, true, "removed", 42, nil},
				},
			},
			{
				Query: "update dolt_workspace_tbl set staged = false where from_pk = 23",
			},
			{
				Query: "select id, staged, diff_type, from_pk, to_pk from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, true, "removed", 42, nil},
					{1, false, "removed", 23, nil},
				},
			},
			{
				Query: "update dolt_workspace_tbl set staged = FALSE", // Unstage everything.
			},
			{
				Query: "select id, staged, diff_type, from_pk, to_pk from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, false, "removed", 23, nil},
					{1, false, "removed", 42, nil},
				},
			},
		},
	},
	{
		Name: "dolt_workspace_* complicated mixed updates",
		SetUpScript: []string{
			"create table tbl (pk int primary key, v char(36));",
			"insert into tbl values (42, UUID());",
			"insert into tbl values (23, UUID());",
			"call dolt_commit('-Am', 'creating table tbl');",
			"update tbl set v = UUID() where pk = 42;",
			"delete from tbl where pk = 23;",
			"call dolt_add('tbl')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select id, staged, diff_type, from_pk, to_pk from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, true, "removed", 23, nil},
					{1, true, "modified", 42, 42},
				},
			},
			{
				Query: "update tbl set v = UUID() where pk = 42",
			},
			{
				Query: "select id, staged, diff_type, from_pk, to_pk from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, true, "removed", 23, nil},
					{1, true, "modified", 42, 42},
					{2, false, "modified", 42, 42},
				},
			},
			{
				Query: "insert into tbl values (23, UUID())",
			},
			{
				Query: "select id, staged, diff_type, from_pk, to_pk from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, true, "removed", 23, nil},
					{1, true, "modified", 42, 42},
					{2, false, "added", nil, 23},
					{3, false, "modified", 42, 42},
				},
			},
			{
				Query: "update dolt_workspace_tbl set staged = 1 where id = 2",
			},
			{
				Query: "select id, staged, diff_type, from_pk, to_pk from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, true, "modified", 23, 23},
					{1, true, "modified", 42, 42},
					{2, false, "modified", 42, 42},
				},
			},
		},
	},
	{
		Name: "dolt_workspace_* downgrades keep working changes",
		SetUpScript: []string{
			`create table tbl (pk int primary key, v varchar(20))`,
			`insert into tbl values (42, "inserted")`,
			`call dolt_commit('-Am', 'creating table tbl')`,
			`update tbl set v = "staged"`,
			`call dolt_add('tbl')`,
			`update tbl set v = "working"`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select * from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, true, "modified", 42, "staged", 42, "inserted"},
					{1, false, "modified", 42, "working", 42, "staged"},
				},
			},
			{
				Query: "update dolt_workspace_tbl set staged = false where id = 0",
			},
			{
				// Removing the staged row should not affect the working row's final value, but it will change the from_ value.
				Query: "select * from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, false, "modified", 42, "working", 42, "inserted"},
				},
			},
		},
	},
	{
		Name: "dolt_workspace_* keyless tables",
		SetUpScript: []string{
			"create table tbl (val int);",
			"insert into tbl values (42);",
			"insert into tbl values (42);",
			"insert into tbl values (42);",
			"insert into tbl values (51);",
			"insert into tbl values (51);",
			"call dolt_commit('-Am', 'creating table tbl')",
			"update tbl set val=51 where val=42 limit 1",
			"call dolt_add('tbl')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select * from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, true, "added", 51, nil},
					{1, true, "removed", nil, 42},
				},
			},
			{
				Query: "select val, count(*) as num from tbl AS OF STAGED  group by val order by val",
				Expected: []sql.Row{
					{42, 2},
					{51, 3},
				},
			},
			{
				Query: "update dolt_workspace_tbl set staged = false where id = 0",
			},
			{
				Query: "select * from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, true, "removed", nil, 42},
					{1, false, "added", 51, nil},
				},
			},
			{
				Query: "update tbl set val=23",
			},
			{
				Query: "select val, count(*) as num from tbl AS OF STAGED  group by val order by val",
				Expected: []sql.Row{
					{42, 2},
					{51, 2},
				},
			},
			{
				Query: "select val, count(*) as num from tbl AS OF WORKING group by val order by val",
				Expected: []sql.Row{
					{23, 5},
				},
			},
			{
				Query: "select * from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, true, "removed", nil, 42},
					{1, false, "added", 23, nil},
					{2, false, "added", 23, nil},
					{3, false, "added", 23, nil},
					{4, false, "added", 23, nil},
					{5, false, "added", 23, nil},
					{6, false, "removed", nil, 51},
					{7, false, "removed", nil, 51},
					{8, false, "removed", nil, 42},
					{9, false, "removed", nil, 42},
				},
			},
			{
				Query: "update dolt_workspace_tbl set staged = true where id % 2 = 0",
			},
			{
				Query: "select * from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, true, "added", 23, nil},
					{1, true, "added", 23, nil},
					{2, true, "removed", nil, 51},
					{3, true, "removed", nil, 42},
					{4, true, "removed", nil, 42},
					{5, false, "added", 23, nil},
					{6, false, "added", 23, nil},
					{7, false, "added", 23, nil},
					{8, false, "removed", nil, 51},
					{9, false, "removed", nil, 42},
				},
			},
			{
				Query: "select val, count(*) as num from tbl AS OF STAGED  group by val order by val",
				Expected: []sql.Row{
					{23, 2},
					{42, 1},
					{51, 1},
				},
			},
			{
				Query: "select val, count(*) as num from tbl AS OF WORKING group by val order by val",
				Expected: []sql.Row{
					{23, 5},
				},
			},
		},
	},

	{
		Name: "dolt_workspace_* delete forbidden on staged rows",
		SetUpScript: []string{
			"create table tbl (pk int primary key, val int);",
			"call dolt_commit('-Am', 'creating table t');",
			"insert into tbl values (42,42);",
			"insert into tbl values (43,43);",
			"call dolt_add('tbl');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select * from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, true, "added", 42, 42, nil, nil},
					{1, true, "added", 43, 43, nil, nil},
				},
			},
			{
				Query:          "delete from dolt_workspace_tbl where id = 0;",
				ExpectedErrStr: "cannot delete staged rows from workspace",
			},
			{
				Query: "update dolt_workspace_tbl set staged = false where to_pk = 42;",
			},
			{
				Query: "delete from dolt_workspace_tbl where to_pk = 42;",
			},
			{
				Query: "select * from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, true, "added", 43, 43, nil, nil},
				},
			},
		},
	},
	{
		Name: "dolt_workspace_* delete from workspace to restore deleted row",
		SetUpScript: []string{
			"create table tbl (pk int primary key, val int);",
			"insert into tbl values (42,42)",
			"insert into tbl values (43,43)",
			"call dolt_add('tbl');",
			"call dolt_commit('-Am', 'creating table tbl')",
			"delete from tbl",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "delete from dolt_workspace_tbl where id = 0",
			},
			{
				Query: "select * from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, false, "removed", nil, nil, 43, 43},
				},
			},
			{
				Query: "select * from tbl",
				Expected: []sql.Row{
					{42, 42},
				},
			},
		},
	},
	{
		Name: "dolt_workspace_* delete from workspace to remove new row",
		SetUpScript: []string{
			"create table tbl (pk int primary key, val int);",
			"call dolt_commit('-Am', 'creating table tbl')",
			"insert into tbl values (42,42)",
			"insert into tbl values (43,43)",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "delete from dolt_workspace_tbl where to_pk = 42",
			},
			{
				Query: "select * from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, false, "added", 43, 43, nil, nil},
				},
			},
			{
				Query: "select * from tbl",
				Expected: []sql.Row{
					{43, 43},
				},
			},
		},
	},
	{
		Name: "dolt_workspace_* delete from workspace to revert and update",
		SetUpScript: []string{
			"create table tbl (pk int primary key, val int);",
			"insert into tbl values (42,42)",
			"insert into tbl values (43,43)",
			"call dolt_commit('-Am', 'creating table tbl')",
			"update tbl set val=val*2",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select * from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, false, "modified", 42, 84, 42, 42},
					{1, false, "modified", 43, 86, 43, 43},
				},
			},
			{
				Query: "delete from dolt_workspace_tbl where to_pk = 42",
			},
			{
				Query: "select * from dolt_workspace_tbl",
				Expected: []sql.Row{
					{0, false, "modified", 43, 86, 43, 43},
				},
			},
			{
				Query: "select * from tbl",
				Expected: []sql.Row{
					{42, 42}, // 42 is unchanged.
					{43, 86},
				},
			},
		},
	},
}
