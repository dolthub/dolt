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
		Name: "Invalid args into dolt test run",
		SetUpScript: []string{
			"INSERT INTO dolt_tests VALUES ('valid argument', 'argument tests', 'show tables;', 'expected_rows', '==', '0')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "SELECT * FROM dolt_test_run('invalid')",
				ExpectedErrStr: "could not find tests for argument: invalid",
			},
			{
				Query:          "SELECT * FROM dolt_test_run('valid argument', 'invalid')",
				ExpectedErrStr: "could not find tests for argument: invalid",
			},
		},
	},
	{
		Name: "Delimiter is optional for dolt_test_run",
		SetUpScript: []string{
			"INSERT INTO dolt_tests VALUES ('should pass', 'delimiter tests', 'show tables', 'expected_rows', '==', '0'), " +
				"('should also pass', 'delimiter tests', 'show tables;', 'expected_rows', '==', '0')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT * FROM dolt_test_run('delimiter tests')",
				Expected: []sql.Row{
					{"should also pass", "delimiter tests", "show tables;", "PASS", ""},
					{"should pass", "delimiter tests", "show tables", "PASS", ""},
				},
			},
		},
	},
	{
		Name: "Null test group functions correctly",
		SetUpScript: []string{
			"INSERT INTO dolt_tests VALUES ('should pass', NULL, 'select * from dolt_log;', 'expected_rows', '>=', '1')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT * FROM dolt_test_run('should pass')",
				Expected: []sql.Row{
					{"should pass", "", "select * from dolt_log;", "PASS", ""},
				},
			},
		},
	},
	{
		Name: "Simple row and column tests",
		SetUpScript: []string{
			"INSERT INTO dolt_tests VALUES ('should pass rows', 'row tests', 'select * from dolt_branches;', 'expected_rows', '!=', '4'), " +
				"('should fail rows', 'row tests', 'select * from dolt_branches;', 'expected_rows', '==', '4'), " +
				"('expect integer for rows', 'row tests', 'select * from dolt_branches;', 'expected_rows', '==', '0.5'), " +
				"('should pass columns', 'column tests', 'select * from dolt_branches;', 'expected_columns', '!=', '5'), " +
				"('should fail columns', 'column tests', 'select * from dolt_branches;', 'expected_columns', '==', '7'), " +
				"('expect integer for columns', 'column tests', 'select * from dolt_branches;', 'expected_columns', '==', '0.5')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT * FROM dolt_test_run('row tests')",
				Expected: []sql.Row{
					{"expect integer for rows", "row tests", "select * from dolt_branches;", "FAIL", "cannot run assertion on non integer value: 0.5"},
					{"should fail rows", "row tests", "select * from dolt_branches;", "FAIL", "Assertion failed: expected_rows equal to 4, got 1"},
					{"should pass rows", "row tests", "select * from dolt_branches;", "PASS", ""},
				},
			},
			{
				Query: "SELECT * FROM dolt_test_run('column tests')",
				Expected: []sql.Row{
					{"expect integer for columns", "column tests", "select * from dolt_branches;", "FAIL", "cannot run assertion on non integer value: 0.5"},
					{"should fail columns", "column tests", "select * from dolt_branches;", "FAIL", "Assertion failed: expected_columns equal to 7, got 9"},
					{"should pass columns", "column tests", "select * from dolt_branches;", "PASS", ""},
				},
			},
		},
	},
	{
		Name: "Can expect single integer",
		SetUpScript: []string{
			"CREATE TABLE test_table (number int)",
			"INSERT INTO test_table VALUES (1)",
			"INSERT INTO dolt_tests VALUES ('should fail', 'single value tests', 'select number from test_table;', 'expected_single_value', '==','5'), " +
				"('should pass', 'single value tests', 'select number from test_table;', 'expected_single_value', '==', '1')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT * FROM dolt_test_run('single value tests')",
				Expected: []sql.Row{
					{"should fail", "single value tests", "select number from test_table;", "FAIL", "Assertion failed: expected_single_value equal to 5, got 1"},
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
				Query: "SELECT * FROM dolt_test_run('single value tests')",
				Expected: []sql.Row{
					{"should fail", "single value tests", "select number from test_table;", "FAIL", "Assertion failed: expected_single_value not equal to String, got String"},
					{"should pass", "single value tests", "select number from test_table;", "PASS", ""},
				},
			},
		},
	},
	{
		Name: "Can expect multiple date formats",
		SetUpScript: []string{
			"CREATE TABLE test (base DATE, withtime DATETIME)",
			"INSERT INTO test VALUES ('2025-08-22', '2025-08-22 09:00:00')",
			"INSERT INTO dolt_tests VALUES ('only date pass', 'base tests', 'select base from test;', 'expected_single_value', '<', '2025-08-23'), " +
				"('only date fail', 'base tests', 'select base from test;', 'expected_single_value', '>', '2025-08-23'), " +
				"('datetime pass', 'datetime tests', 'select withtime from test;', 'expected_single_value', '<=', '2025-08-22 09:00:00'), " +
				"('datetime fail', 'datetime tests', 'select withtime from test;', 'expected_single_value', '>', '2025-08-22 09:00:01')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT * FROM dolt_test_run('base tests')",
				Expected: []sql.Row{
					{"only date fail", "base tests", "select base from test;", "FAIL", "Assertion failed: expected_single_value greater than 2025-08-23, got 2025-08-22"},
					{"only date pass", "base tests", "select base from test;", "PASS", ""},
				},
			},
			{
				Query: "SELECT * FROM dolt_test_run('datetime tests')",
				Expected: []sql.Row{
					{"datetime fail", "datetime tests", "select withtime from test;", "FAIL", "Assertion failed: expected_single_value greater than 2025-08-22 09:00:01, got 2025-08-22 09:00:00"},
					{"datetime pass", "datetime tests", "select withtime from test;", "PASS", ""},
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
				Query: "SELECT * FROM dolt_test_run('single value tests')",
				Expected: []sql.Row{
					{"should fail", "single value tests", "select * from test;", "FAIL", "Assertion failed: expected_single_value greater than 3.2, got 3.14159"},
					{"should pass", "single value tests", "select * from test;", "PASS", ""},
				},
			},
		},
	},
	{
		Name: "Can expect single decimal",
		SetUpScript: []string{
			"CREATE TABLE decimals (d DECIMAL(10,2))",
			"INSERT INTO decimals VALUES (10.4)",
			"INSERT INTO dolt_tests VALUES ('should pass', 'decimal tests', 'select * from decimals;', 'expected_single_value', '<', '10.5'), " +
				"('should fail', 'decimal tests', 'select * from decimals;', 'expected_single_value', '>', '10.5'), " +
				"('can compare to integer', 'decimal tests', 'select * from decimals;', 'expected_single_value', '>', '10')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT * FROM dolt_test_run('decimal tests')",
				Expected: []sql.Row{
					{"can compare to integer", "decimal tests", "select * from decimals;", "PASS", ""},
					{"should fail", "decimal tests", "select * from decimals;", "FAIL", "Assertion failed: expected_single_value greater than 10.5, got 10.4"},
					{"should pass", "decimal tests", "select * from decimals;", "PASS", ""},
				},
			},
		},
	},
	{
		Name: "Can handle null values correctly",
		SetUpScript: []string{
			"CREATE TABLE numbers (i int, t text)",
			"INSERT INTO numbers VALUES (NULL, NULL)",
			"INSERT INTO dolt_tests (test_name, test_query, assertion_type, assertion_comparator) VALUES " +
				"('simple null int equality', 'SELECT i FROM numbers', 'expected_single_value', '=='), " +
				"('simple null string equality', 'SELECT t FROM numbers', 'expected_single_value', '=='), " +
				"('simple null inequality', 'SELECT i FROM numbers', 'expected_single_value', '!=')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT * FROM dolt_test_run('*')",
				Expected: []sql.Row{
					{"simple null inequality", "", "SELECT i FROM numbers", "FAIL", "Assertion failed: expected_single_value not equal to NULL, got NULL"},
					{"simple null int equality", "", "SELECT i FROM numbers", "PASS", ""},
					{"simple null string equality", "", "SELECT t FROM numbers", "PASS", ""},
				},
			},
		},
	},
	{
		Name: "Single value will not accept multiple values",
		SetUpScript: []string{
			"CREATE TABLE numbers (i int)",
			"INSERT INTO numbers VALUES (1),(2)",
			"INSERT INTO dolt_tests VALUES ('should fail, many columns', 'not one cell', 'select * from dolt_log;', 'expected_single_value', '==', '0')",
			"INSERT INTO dolt_tests VALUES ('should fail, no rows', 'not one cell', 'show tables like ''invalid'';', 'expected_single_value', '==', '0')",
			"INSERT INTO dolt_tests VALUES ('should fail, many rows', 'not one cell', 'select * from numbers;', 'expected_single_value', '==', '0')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT * FROM dolt_test_run('not one cell')",
				Expected: []sql.Row{
					{"should fail, many columns", "not one cell", "select * from dolt_log;", "FAIL", "expected_single_value expects exactly one cell. Received multiple columns"},
					{"should fail, many rows", "not one cell", "select * from numbers;", "FAIL", "expected_single_value expects exactly one cell. Received multiple rows"},
					{"should fail, no rows", "not one cell", "show tables like 'invalid';", "FAIL", "expected_single_value expects exactly one cell. Received 0 rows"},
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
				Query: "SELECT * FROM dolt_test_run('comparison tests')",
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
				Query: "SELECT * FROM dolt_test_run()",
				Expected: []sql.Row{
					{"grouped test", "wildcard tests", "show tables;", "PASS", ""},
					{"second grouped test", "wildcard tests", "show tables;", "PASS", ""},
					{"ungrouped test", "", "show tables;", "PASS", ""},
				},
			},
			{
				Query: "SELECT * FROM dolt_test_run('ungrouped test', 'wildcard tests')",
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
				Query: "SELECT * FROM dolt_test_run('should fail')",
				Expected: []sql.Row{
					{"should fail", "", "select * from dolt_log; show tables;", "FAIL", "Can only run exactly one query"},
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
				Query: "SELECT * FROM dolt_test_run('should fail')",
				Expected: []sql.Row{
					{"should fail", "", "create table test (i int)", "FAIL", "Cannot execute write queries"},
				},
			},
		},
	},
	{
		Name: "Query errors correctly reported",
		SetUpScript: []string{
			"INSERT INTO dolt_tests VALUES ('should fail', '', 'select * from invalid', 'expected_single_value', '==', '1')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT * FROM dolt_test_run('should fail')",
				Expected: []sql.Row{
					{"should fail", "", "select * from invalid", "FAIL", "query error: table not found: invalid"},
				},
			},
		},
	},
}
