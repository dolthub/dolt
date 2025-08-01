// Copyright 2025 Dolthub, Inc.
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

var DoltQueryCatalogScripts = []queries.ScriptTest{
	{
		Name: "can insert into dolt query catalog",
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "insert into dolt_query_catalog values ('show', 1, 'show', 'show tables;', '')",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 1}},
				},
			},
		},
	},
	{
		Name: "can drop dolt query catalog, cannot drop twice",
		SetUpScript: []string{
			"INSERT INTO dolt_query_catalog VALUES ('show', 1, 'show', 'show tables;', '')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "drop table dolt_query_catalog",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:          "drop table dolt_query_catalog",
				ExpectedErrStr: "table not found: dolt_query_catalog",
			},
		},
	},
	{
		Name: "can run delete from on dolt query catalog",
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "DELETE FROM dolt_query_catalog",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 0}},
				},
			},
		},
	},
	{
		Name: "select from dolt_query_catalog",
		SetUpScript: []string{
			"INSERT INTO dolt_query_catalog VALUES ('show', 1, 'show', 'show tables;', 'my message')",
			"INSERT INTO dolt_query_catalog VALUES ('get commits', 2, 'get commits', 'select * from dolt_commits;', '')",
			"INSERT INTO dolt_query_catalog VALUES ('get branches', 3, 'get branches', 'select * from dolt_branches;', '')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT * FROM dolt_query_catalog",
				Expected: []sql.Row{
					{"show", 1, "show", "show tables;", "my message"},
					{"get commits", 2, "get commits", "select * from dolt_commits;", ""},
					{"get branches", 3, "get branches", "select * from dolt_branches;", ""},
				},
			},
			{
				Query: "SELECT * FROM dolt_query_catalog where display_order = 2",
				Expected: []sql.Row{
					{"get commits", 2, "get commits", "select * from dolt_commits;", ""},
				},
			},
		},
	},
	{
		Name: "can replace row in dolt_query_catalog",
		SetUpScript: []string{
			"INSERT INTO dolt_query_catalog VALUES ('test', 1, 'test', 'show tables;', '')",
			"INSERT INTO dolt_query_catalog VALUES ('test2', 2, 'test2', 'select * from dolt_commits;', '')",
			"REPLACE INTO dolt_query_catalog VALUES ('test', 1, 'new name', 'describe dolt_query_catalog;', 'a new message')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT * FROM dolt_query_catalog",
				Expected: []sql.Row{
					{"test", 1, "new name", "describe dolt_query_catalog;", "a new message"},
					{"test2", 2, "test2", "select * from dolt_commits;", ""},
				},
			},
		},
	},
	{
		Name: "can update dolt query catalog",
		SetUpScript: []string{
			"INSERT INTO dolt_query_catalog VALUES ('show', 1, 'show', 'show tables;', '')",
			"UPDATE dolt_query_catalog SET display_order = display_order + 1",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT * FROM dolt_query_catalog",
				Expected: []sql.Row{
					{"show", 2, "show", "show tables;", ""},
				},
			},
		},
	},
	{
		Name: "can use 'as of' on dolt_query_catalog",
		SetUpScript: []string{
			"INSERT INTO dolt_query_catalog VALUES ('show', 1, 'show', 'show tables;', '')",
			"CALL DOLT_COMMIT('-A','-m', 'first commit')",
			"INSERT INTO dolt_query_catalog VALUES ('select', 2, 'select', 'select * from dolt_query_catalog;', '')",
			"CALL DOLT_COMMIT('-A', '-m', 'second commit')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * FROM dolt_query_catalog as of 'HEAD~2'",
				Expected: []sql.Row{},
			},
			{
				Query: "SELECT * FROM dolt_query_catalog as of 'HEAD'",
				Expected: []sql.Row{
					{"show", 1, "show", "show tables;", ""},
					{"select", 2, "select", "select * from dolt_query_catalog;", ""},
				},
			},
		},
	},
}
