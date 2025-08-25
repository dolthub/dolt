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
		Name: "No args into dolt test run",
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "SELECT * FROM dolt_test_run()",
				ExpectedErrStr: "function 'dolt_test_run' expected 1 or more arguments, 0 received",
			},
		},
	},
	{
		Name: "Invalid args into dolt test run",
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "SELECT * FROM dolt_test_run('invalid')",
				ExpectedErrStr: "invalid input to dolt_test_run: invalid",
			},
		},
	},
	{
		Name: "Valid and invalid arguments into dolt test run",
		SetUpScript: []string{
			"INSERT INTO dolt_tests VALUES ('valid argument', 'argument tests', 'show tables;', 'expected_rows', '==', '0')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "SELECT * FROM dolt_test_run('test valid argument', 'invalid')",
				ExpectedErrStr: "invalid input to dolt_test_run: invalid",
			},
		},
	},
	{
		Name: "Can expect rows and columns",
		SetUpScript: []string{
			"INSERT INTO dolt_tests VALUES ('should pass rows', 'row tests', 'show tables;', 'expected_rows', '==', '0'), " +
				"('should fail rows', 'row tests', 'show tables;', 'expected_rows', '!=', '0'), " +
				"('should pass columns', 'column tests', 'show tables;', 'expected_columns', '==', '0'), " +
				"('should fail columns', 'column tests', 'show tables;', 'expected_columns', '!=', '0')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT * FROM dolt_test_run('group row tests')",
				Expected: []sql.Row{
					{"should fail rows", "row tests", "show tables;", "FAIL", "Assertion failed: expected row count not equal to 0, got 0"},
					{"should pass rows", "row tests", "show tables;", "PASS", ""},
				},
			},
			{
				Query: "SELECT * FROM dolt_test_run('group column tests')",
				Expected: []sql.Row{
					{"should fail columns", "column tests", "show tables;", "FAIL", "Assertion failed: expected column count not equal to 0, got 0"},
					{"should pass columns", "column tests", "show tables;", "PASS", ""},
				},
			},
		},
	},
	{
		Name: "Can expect single integer",
		SetUpScript: []string{
			"CREATE TABLE test_table (number int)",
			"INSERT INTO test_table VALUES (1)",
			"INSERT INTO dolt_tests VALUES ('should fail', 'single value tests', 'select number from test_table;', 'expected_single_value', '!=','1'), " +
				"('should pass', 'single value tests', 'select number from test_table;', 'expected_single_value', '==', '1')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT * FROM dolt_test_run('group single value tests')",
				Expected: []sql.Row{
					{"should fail", "single value tests", "select number from test_table;", "FAIL", "Assertion failed: expected single value not equal to 1, got 1"},
					{"should pass", "single value tests", "select number from test_table;", "PASS", ""},
				},
			},
		},
	},
	{
		Name: "Can expect single string",
		SetUpScript: []string{
			"CREATE TABLE test_table (number text)",
			"INSERT INTO test_table VALUES ('String')",
			"INSERT INTO dolt_tests VALUES ('should pass', 'single value tests', 'select number from test_table;', 'expected_single_value', '==', 'String'), " +
				"('should fail', 'single value tests', 'select number from test_table;', 'expected_single_value', '!=', 'String')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT * FROM dolt_test_run('group single value tests')",
				Expected: []sql.Row{
					{"should fail", "single value tests", "select number from test_table;", "FAIL", "Assertion failed: expected single value not equal to String, got String"},
					{"should pass", "single value tests", "select number from test_table;", "PASS", ""},
				},
			},
		},
	},
	{
		Name: "Can expect single date",
		SetUpScript: []string{
			"CREATE TABLE test (d DATE)",
			"INSERT INTO test VALUES ('2025-08-22')",
			"INSERT INTO dolt_tests VALUES ('should pass', 'single value tests', 'select * from test;', 'expected_single_value', '<', '2025-08-23'), " +
				"('should fail', 'single value tests', 'select * from test;', 'expected_single_value', '>', '2025-08-23')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT * FROM dolt_test_run('group single value tests')",
				Expected: []sql.Row{
					{"should pass", "single value tests", "select * from test;", "PASS", ""},
					{"should fail", "single value tests", "select * from test;", "FAIL", "Assertion failed: expected single value greater than 2025-08-23, got 2025-08-22"},
				},
			},
		},
	},
	{
		Name: "Can expect single float",
		SetUpScript: []string{
			"CREATE TABLE test (a float)",
			"INSERT INTO test VALUES (3.14159)",
			"INSERT INTO dolt_tests VALUES ('should pass', 'single value tests', 'select * from test;', 'expected_single_value', '<', '3.2'), " +
				"('should fail', 'single value tests', 'select * from test;', 'expected_single_value', '>', '3.2') ",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT * FROM dolt_test_run('group single value tests')",
				Expected: []sql.Row{
					{"should pass", "single value tests", "select * from test;", "PASS", ""},
					{"should fail", "single value tests", "select * from test;", "FAIL", "assertion failed: expected single value greater than 3.2, got 3.14159"},
				},
			},
		},
	},
	{
		Name: "Single value will not accept multiple values",
		SetUpScript: []string{
			"INSERT INTO dolt_tests VALUES ('should fail', '', 'select * from dolt_log;', 'expected_single_value', '==', '0')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT * FROM dolt_test_run('test should fail')",
				Expected: []sql.Row{
					{"should fail", "", "select * from dolt_log;", "FAIL", "expected_single_value expects exactly one cell"},
				},
			},
		},
	},
	{
		Name: "Can run dolt unit tests on each comparison type",
		SetUpScript: []string{
			"CREATE TABLE test (i int)",
			"INSERT INTO dolt_tests VALUES ('equal to', 'comparison tests', 'show tables;', 'expected_rows', '==', '1'), " +
				"('not equal to', 'comparison tests', 'show tables;', 'expected_rows', '!=', '2'), " +
				"('less than', 'comparison tests', 'show tables;', 'expected_rows', '<', '2'), " +
				"('less than or equal to', 'comparison tests', 'show tables;', 'expected_rows', '<=', '1'), " +
				"('greater than', 'comparison tests', 'show tables;', 'expected_rows', '>', '0'), " +
				"('greater than or equal to', 'comparison tests', 'show tables;','expected_rows', '>=', '0')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT * FROM dolt_test_run('group comparison tests')",
				Expected: []sql.Row{
					{"equal to", "comparison tests", "show tables;", "PASS", ""},
					{"greater than", "comparison tests", "show tables;", "PASS", ""},
					{"greater than or equal to", "comparison tests", "show tables;", "PASS", ""},
					{"less than", "comparison tests", "show tables;", "PASS", ""},
					{"less than or equal to", "comparison tests", "show tables;", "PASS", ""},
					{"not equal to", "comparison tests", "show tables;", "PASS", ""},
				},
			},
		},
	},
	{
		Name: "Can use wildcard, multiple arguments to run all dolt unit tests",
		SetUpScript: []string{
			"INSERT INTO dolt_tests VALUES ('grouped test', 'wildcard tests', 'show tables;', 'expected_rows', '==', '0'), " +
				"('second grouped test', 'wildcard tests', 'show tables;', 'expected_rows', '==', '0'), " +
				"('ungrouped test', '', 'show tables;', 'expected_rows', '==', '0')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT * FROM dolt_test_run('*')",
				Expected: []sql.Row{
					{"grouped test", "wildcard tests", "show tables;", "PASS", ""},
					{"second grouped test", "wildcard tests", "show tables;", "PASS", ""},
					{"ungrouped test", "", "show tables;", "PASS", ""},
				},
			},
			{
				Query: "SELECT * FROM dolt_test_run('test ungrouped test', 'group wildcard tests')",
				Expected: []sql.Row{
					{"grouped test", "wildcard tests", "show tables;", "PASS", ""},
					{"second grouped test", "wildcard tests", "show tables;", "PASS", ""},
					{"ungrouped test", "", "show tables;", "PASS", ""},
				},
			},
		},
	},
	{
		Name: "Can not run multiple queries in one test",
		SetUpScript: []string{
			"INSERT INTO dolt_tests VALUES ('should fail', '', 'select * from dolt_log; show tables;', 'expected_orws', '==', '0')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT * FROM dolt_test_run('test should fail')",
				Expected: []sql.Row{
					{"should fail", "", "select * from dolt_log; show tables;", "FAIL", "Cannot execute multiple queries"},
				},
			},
		},
	},
	{
		Name: "Can not test write queries",
		SetUpScript: []string{
			"INSERT INTO dolt_tests VALUES ('should fail', '', 'create table test (i int)', 'expected_rows', '==', '1')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT * FROM dolt_test_run('test should fail')",
				Expected: []sql.Row{
					{"should fail", "", "create table test (i int)", "FAIL", "Cannot execute write queries"},
				},
			},
		},
	},
}
