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
				Query: "INSERT INTO dolt_tests VALUES ('no tables', 'validate tables' , 'show tables;', 'expected_rows', '==', '0')",
			},
		},
	},
	{
		Name: "can drop dolt tests table, cannot drop twice",
		SetUpScript: []string{
			"INSERT INTO dolt_tests VALUES ('no tables', 'validate tables', 'show tables;', 'expected_rows', '==', '0')",
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
			"INSERT INTO dolt_tests VALUES ('one table', 'validate tables', 'show tables;', 'expected_rows', '==', '1');",
			"INSERT INTO dolt_tests VALUES ('numbers table exists', 'validate tables', 'show tables like ''numbers''', 'expected_rows', '==', '1')",
			"INSERT INTO dolt_tests VALUES ('numbers schema', 'numbers table validation', 'describe numbers', 'expected_rows', '==', '1')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT * FROM dolt_tests;",
				Expected: []sql.Row{
					{"one table", "validate tables", "show tables;", "expected_rows", "==", "1"},
					{"numbers table exists", "validate tables", "show tables like 'numbers'", "expected_rows", "==", "1"},
					{"numbers schema", "numbers table validation", "describe numbers", "expected_rows", "==", "1"},
				},
			},
			{
				Query: "SELECT * FROM dolt_tests where test_group = 'validate tables'",
				Expected: []sql.Row{
					{"one table", "validate tables", "show tables;", "expected_rows", "==", "1"},
					{"numbers table exists", "validate tables", "show tables like 'numbers'", "expected_rows", "==", "1"},
				},
			},
		},
	},
	{
		Name: "can replace row in dolt_tests table",
		SetUpScript: []string{
			"INSERT INTO dolt_tests VALUES ('one table', 'validate tables', 'show tables;', 'expected_rows', '==', '1')",
			"INSERT INTO dolt_tests VALUES ('numbers table exists', 'validate tables', 'show tables like ''numbers'';', 'expected_rows', '==', '1')",
			"REPLACE INTO dolt_tests VALUES ('numbers table exists', 'validate tables', 'show tables like ''numbers'';', 'expected_rows', '==', '1')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT * FROM dolt_tests",
				Expected: []sql.Row{
					{"numbers table exists", "validate tables", "show tables like 'numbers';", "expected_rows", "==", "1"},
					{"one table", "validate tables", "show tables;", "expected_rows", "==", "1"},
				},
			},
		},
	},
	{
		Name: "can use 'as of' on dolt_tests table",
		SetUpScript: []string{
			"INSERT INTO dolt_tests VALUES ('one table', 'validate tables', 'show tables;', 'expected_rows', '==', '1')",
			"CALL DOLT_COMMIT('-A','-m', 'first commit')",
			"INSERT INTO dolt_tests VALUES ('numbers table exists', 'validate tables', 'show tables like ''numbers'';', 'expected_rows', '==', '1')",
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
					{"one table", "validate tables", "show tables;", "expected_rows", "==", "1"},
				},
			},
			{
				Query: "SELECT * FROM dolt_tests as of 'HEAD'",
				Expected: []sql.Row{
					{"one table", "validate tables", "show tables;", "expected_rows", "==", "1"},
					{"numbers table exists", "validate tables", "show tables like 'numbers';", "expected_rows", "==", "1"},
				},
			},
		},
	},
}

var DoltTestRunFunctionScripts = []queries.ScriptTest{
	{
		Name: "Can run dolt unit tests on each assertion type",
		SetUpScript: []string{
			"CREATE TABLE test (i int)",
			"INSERT INTO test VALUES (1)",
			"INSERT INTO dolt_tests VALUES ('row tests', 'should pass', 'show tables;', 'expected_rows == 1'), ('row tests', 'should fail', 'show tables;', 'expected_rows == 2')",
			"INSERT INTO dolt_tests VALUES ('column tests', 'should pass', 'select * from test;', 'expected_columns == 1'), ('column tests', " +
				"'should fail', 'select * from test;', 'expected_columns == 2')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT * FROM dolt_test_run('row tests')",
				Expected: []sql.Row{
					{"row tests", "should fail", "show tables;", "FAIL", "Assertion failed: expected row count equal to 2, got 1"},
					{"row tests", "should pass", "show tables;", "PASS", ""},
				},
			},
			{
				Query: "SELECT * FROM dolt_test_run('column tests')",
				Expected: []sql.Row{
					{"column tests", "should fail", "select * from test;", "FAIL", "Assertion failed: expected column count equal to 2, got 1"},
					{"column tests", "should pass", "select * from test;", "PASS", ""},
				},
			},
		},
	},
	{
		Name: "Can run dolt unit tests on each comparison type",
		SetUpScript: []string{
			"CREATE TABLE test (i int)",
			"INSERT INTO dolt_tests VALUES ('comparison tests', 'equal to', 'show tables;', 'expected_rows == 1'), " +
				"('comparison tests', 'not equal to', 'show tables;', 'expected_rows != 2'), " +
				"('comparison tests', 'less than', 'show tables;', 'expected_rows < 2'), " +
				"('comparison tests', 'less than or equal to', 'show tables;', 'expected_rows <= 1'), " +
				"('comparison tests', 'greater than', 'show tables;', 'expected_rows > 0'), " +
				"('comparison tests', 'greater than or equal to', 'show tables;','expected_rows >= 0')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT * FROM dolt_test_run('comparison tests')",
				Expected: []sql.Row{
					{"comparison tests", "equal to", "show tables;", "PASS", ""},
					{"comparison tests", "greater than", "show tables;", "PASS", ""},
					{"comparison tests", "greater than or equal to", "show tables;", "PASS", ""},
					{"comparison tests", "less than", "show tables;", "PASS", ""},
					{"comparison tests", "less than or equal to", "show tables;", "PASS", ""},
					{"comparison tests", "not equal to", "show tables;", "PASS", ""},
				},
			},
		},
	},
	{
		Name: "Bad queries fail gracefully when running dolt unit tests",
		SetUpScript: []string{
			"INSERT INTO dolt_tests VALUES ('bad query tests', 'bad query test', 'select * from invalid;', 'expected_rows == 0')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT * FROM dolt_test_run('bad query tests')",
				Expected: []sql.Row{
					{"bad query tests", "bad query test", "select * from invalid;", "FAIL", "Query error: table not found: invalid"},
				},
			},
		},
	},
	{
		Name: "Cannot use write queries when running dolt unit tests",
		SetUpScript: []string{
			"CREATE TABLE test (i int)",
			"INSERT INTO dolt_tests VALUES ('write queries', 'insert query', 'insert into dolt_tests values (1);', 'expected_rows == 0'), " +
				"('write queries', 'drop table query', 'drop table test;', 'expected_rows == 0')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT * FROM dolt_test_run('write queries')",
				Expected: []sql.Row{
					{"write queries", "drop table query", "drop table test;", "FAIL", "Cannot execute write queries"},
					{"write queries", "insert query", "insert into dolt_tests values (1);", "FAIL", "Cannot execute write queries"},
				},
			},
		},
	},
	{
		Name: "Invalid assertions",
		SetUpScript: []string{
			"INSERT INTO dolt_tests VALUES ('bad assertions', 'nonexistent assertion', 'show tables;', 'invalid == 0'), " +
				"('bad assertions', 'bad assertion formatting', 'show tables;', 'expected_rows==0')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT * FROM dolt_test_run('bad assertions')",
				Expected: []sql.Row{
					{"bad assertions", "bad assertion formatting", "show tables;", "FAIL", "Unexpected assertion format"},
					{"bad assertions", "nonexistent assertion", "show tables;", "FAIL", "'invalid' is not a valid assertion type"},
				},
			},
		},
	},
	{
		Name: "nonexistant test suite when running dolt unit tests",
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "SELECT * FROM dolt_test_run('invalid')",
				ExpectedErrStr: "could not find tests for test group with name: 'invalid'",
			},
		},
	},
}
