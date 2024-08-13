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
		},
	},
	{
		Name: "dolt_workspace_* inserted row",
		SetUpScript: []string{
			"create table tbl (pk int primary key, val int);",
			"call dolt_commit('-Am', 'creating table t');",

			"insert into tbl values (42,42);",
			"insert into tbl values (43,43);",
			"call dolt_commit('-am', 'inserting rows 3 rows at HEAD');",
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
			"call dolt_commit('-am', 'inserting rows 3 rows at HEAD');",
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
			"call dolt_commit('-am', 'inserting rows 3 rows at HEAD');",
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
}
