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

var DoltTestTableScripts = []queries.ScriptTest{
	{
		Name: "can insert into dolt tests",
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "INSERT INTO dolt_tests VALUES ('validate tables', 'no tables', 'show tables;', 'expected_rows == 0;')",
			},
		},
	},
	{
		Name: "can drop dolt tests table, cannot drop twice",
		SetUpScript: []string{
			"INSERT INTO dolt_tests VALUES ('validate tables', 'no tables', 'show tables;', 'expected_rows == 0;')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "DROP TABLE dolt_tests;",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query:          "DROP TABLE dolt_tests;",
				ExpectedErrStr: "table not found: dolt_tests",
			},
		},
	},
	{
		Name: "can call delete from on dolt tests table",
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "DELETE FROM dolt_tests;",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
		},
	},
	{
		Name: "select from dolt_tests tables",
		SetUpScript: []string{
			"INSERT INTO dolt_tests VALUES ('validate tables', 'one table', 'show tables;', 'expected_rows == 1');",
			"INSERT INTO dolt_tests VALUES ('validate tables', 'numbers table exists', 'show tables like ''numbers''', 'expected_rows == 1')",
			"INSERT INTO dolt_tests VALUES ('numbers table validation', 'numbers schema', 'describe numbers', 'expected_rows == 1')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT * FROM dolt_tests;",
				Expected: []sql.Row{
					{"validate tables", "one table", "show tables;", "expected_rows == 1"},
					{"validate tables", "numbers table exists", "show tables like 'numbers'", "expected_rows == 1"},
					{"numbers table validation", "numbers schema", "describe numbers", "expected_rows == 1"},
				},
			},
			{
				Query: "SELECT * FROM dolt_tests where test_group = 'validate tables'",
				Expected: []sql.Row{
					{"validate tables", "one table", "show tables;", "expected_rows == 1"},
					{"validate tables", "numbers table exists", "show tables like 'numbers'", "expected_rows == 1"},
				},
			},
		},
	},
	{
		Name: "can replace row in dolt_tests table",
		SetUpScript: []string{
			"INSERT INTO dolt_tests VALUES ('validate tables', 'one table', 'show tables;', 'expected_rows == 1')",
			"INSERT INTO dolt_tests VALUES ('validate tables', 'numbers table exists', 'show tables like ''numbers'';', 'expected_rows == 0')",
			"REPLACE INTO dolt_tests VALUES ('validate tables', 'numbers table exists', 'show tables like ''numbers'';', 'expected_rows == 1')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT * FROM dolt_tests",
				Expected: []sql.Row{
					{"validate tables", "numbers table exists", "show tables like 'numbers';", "expected_rows == 1"},
					{"validate tables", "one table", "show tables;", "expected_rows == 1"},
				},
			},
		},
	},
	{
		Name: "can use 'as of' on dolt_tests table",
		SetUpScript: []string{
			"INSERT INTO dolt_tests VALUES ('validate tables', 'one table', 'show tables;', 'expected_rows == 1')",
			"CALL DOLT_COMMIT('-A','-m', 'first commit')",
			"INSERT INTO dolt_tests VALUES ('validate tables', 'numbers table exists', 'show tables like ''numbers'';', 'expected_rows == 1')",
			"CALL DOLT_COMMIT('-A', '-m', 'second commit')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * FROM dolt_tests as of 'HEAD~2'",
				Expected: []sql.Row{},
			},
			{
				Query: "SELECT * FROM dolt_tests as of 'HEAD~1'",
				Expected: []sql.Row{
					{"validate tables", "one table", "show tables;", "expected_rows == 1"},
				},
			},
			{
				Query: "SELECT * FROM dolt_tests as of 'HEAD'",
				Expected: []sql.Row{
					{"validate tables", "one table", "show tables;", "expected_rows == 1"},
					{"validate tables", "numbers table exists", "show tables like 'numbers';", "expected_rows == 1"},
				},
			},
		},
	},
}
