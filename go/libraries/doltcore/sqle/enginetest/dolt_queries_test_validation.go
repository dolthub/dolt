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

var DoltTestValidationScripts = []queries.ScriptTest{
	{
		Name: "test validation system variables exist and have correct defaults",
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SHOW GLOBAL VARIABLES LIKE 'dolt_commit_run_test_groups'",
				Expected: []sql.Row{
					{"dolt_commit_run_test_groups", ""},
				},
			},
			{
				Query: "SHOW GLOBAL VARIABLES LIKE 'dolt_push_run_test_groups'",
				Expected: []sql.Row{
					{"dolt_push_run_test_groups", ""},
				},
			},
		},
	},
	{
		Name: "test validation system variables can be set",
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SET GLOBAL dolt_commit_run_test_groups = '*'",
				Expected: []sql.Row{{types.OkResult{}}},
			},
			{
				Query: "SHOW GLOBAL VARIABLES LIKE 'dolt_commit_run_test_groups'",
				Expected: []sql.Row{
					{"dolt_commit_run_test_groups", "*"},
				},
			},
			{
				Query:    "SET GLOBAL dolt_commit_run_test_groups = 'unit,integration'",
				Expected: []sql.Row{{types.OkResult{}}},
			},
			{
				Query: "SHOW GLOBAL VARIABLES LIKE 'dolt_commit_run_test_groups'",
				Expected: []sql.Row{
					{"dolt_commit_run_test_groups", "unit,integration"},
				},
			},
			{
				Query:    "SET GLOBAL dolt_push_run_test_groups = '*'",
				Expected: []sql.Row{{types.OkResult{}}},
			},
			{
				Query: "SHOW GLOBAL VARIABLES LIKE 'dolt_push_run_test_groups'",
				Expected: []sql.Row{
					{"dolt_push_run_test_groups", "*"},
				},
			},
		},
	},
	{
		Name: "commit with test validation enabled - all tests pass",
		SetUpScript: []string{
			"SET GLOBAL dolt_commit_run_test_groups = '*'",
			"CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100) NOT NULL, email VARCHAR(100))",
			"INSERT INTO users VALUES (1, 'Alice', 'alice@example.com'), (2, 'Bob', 'bob@example.com')",
			"INSERT INTO dolt_tests (test_name, test_group, test_query, assertion_type, assertion_comparator, assertion_value) VALUES " +
				"('test_users_count', 'unit', 'SELECT COUNT(*) FROM users', 'expected_single_value', '==', '2'), " +
				"('test_alice_exists', 'unit', 'SELECT COUNT(*) FROM users WHERE name = \"Alice\"', 'expected_single_value', '==', '1')",
			"CALL dolt_add('.')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "CALL dolt_commit('-m', 'Commit with passing tests')",
				SkipResultsCheck: true,
			},
		},
	},
	{
		Name: "commit with test validation enabled - tests fail, commit aborted",
		SetUpScript: []string{
			"SET GLOBAL dolt_commit_run_test_groups = '*'",
			"CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100) NOT NULL, email VARCHAR(100))",
			"INSERT INTO users VALUES (1, 'Alice', 'alice@example.com'), (2, 'Bob', 'bob@example.com')",
			"INSERT INTO dolt_tests (test_name, test_group, test_query, assertion_type, assertion_comparator, assertion_value) VALUES " +
				"('test_users_count', 'unit', 'SELECT COUNT(*) FROM users', 'expected_single_value', '==', '2'), " +
				"('test_will_fail', 'integration', 'SELECT COUNT(*) FROM users', 'expected_single_value', '==', '999')",
			"CALL dolt_add('.')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "CALL dolt_commit('-m', 'Commit that should fail validation')",
				SkipResultsCheck: true,
			},
		},
	},
	{
		Name: "commit with test validation - specific test groups",
		SetUpScript: []string{
			"SET GLOBAL dolt_commit_run_test_groups = 'unit'",
			"CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100) NOT NULL, email VARCHAR(100))",
			"INSERT INTO users VALUES (1, 'Alice', 'alice@example.com'), (2, 'Bob', 'bob@example.com')",
			"INSERT INTO dolt_tests (test_name, test_group, test_query, assertion_type, assertion_comparator, assertion_value) VALUES " +
				"('test_users_count', 'unit', 'SELECT COUNT(*) FROM users', 'expected_single_value', '==', '2'), " +
				"('test_will_fail', 'integration', 'SELECT COUNT(*) FROM users', 'expected_single_value', '==', '999')",
			"CALL dolt_add('.')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:       "CALL dolt_commit('-m', 'Commit with unit tests only')",
				SkipResultsCheck: true,
			},
		},
	},
	{
		Name: "commit with --skip-tests flag bypasses validation",
		SetUpScript: []string{
			"SET GLOBAL dolt_commit_run_test_groups = '*'",
			"CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100) NOT NULL, email VARCHAR(100))",
			"INSERT INTO users VALUES (1, 'Alice', 'alice@example.com'), (2, 'Bob', 'bob@example.com')",
			"INSERT INTO dolt_tests (test_name, test_group, test_query, assertion_type, assertion_comparator, assertion_value) VALUES " +
				"('test_will_fail', 'integration', 'SELECT COUNT(*) FROM users', 'expected_single_value', '==', '999')",
			"CALL dolt_add('.')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:       "CALL dolt_commit('--skip-tests', '-m', 'Commit skipping tests')",
				SkipResultsCheck: true,
			},
		},
	},
	{
		Name: "cherry-pick with test validation enabled - tests pass",
		SetUpScript: []string{
			"SET GLOBAL dolt_commit_run_test_groups = '*'",
			"CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100) NOT NULL, email VARCHAR(100))",
			"INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')",
			"INSERT INTO dolt_tests (test_name, test_group, test_query, assertion_type, assertion_comparator, assertion_value) VALUES " +
				"('test_alice_exists', 'unit', 'SELECT COUNT(*) FROM users WHERE name = \"Alice\"', 'expected_single_value', '==', '1')",
			"CALL dolt_add('.')",
			"CALL dolt_commit('-m', 'Initial commit')",
			"CALL dolt_checkout('-b', 'feature')",
			"INSERT INTO users VALUES (2, 'Bob', 'bob@example.com')",
			"UPDATE dolt_tests SET assertion_value = '2' WHERE test_name = 'test_alice_exists'",
			"CALL dolt_add('.')",
			"call dolt_commit_hash_out(@commit_hash, '-m', 'Add Bob and update test')",
			"CALL dolt_checkout('main')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:       "CALL dolt_cherry_pick(@commit_hash)",
				SkipResultsCheck: true,
			},
		},
	},
	{
		Name: "cherry-pick with test validation enabled - tests fail, aborted",
		SetUpScript: []string{
			"SET GLOBAL dolt_commit_run_test_groups = '*'",
			"CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100) NOT NULL, email VARCHAR(100))",
			"INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')",
			"INSERT INTO dolt_tests (test_name, test_group, test_query, assertion_type, assertion_comparator, assertion_value) VALUES " +
				"('test_users_count', 'unit', 'SELECT COUNT(*) FROM users', 'expected_single_value', '==', '1')",
			"CALL dolt_add('.')",
			"CALL dolt_commit('-m', 'Initial commit')",
			"CALL dolt_checkout('-b', 'feature')",
			"INSERT INTO users VALUES (2, 'Bob', 'bob@example.com')",
			"CALL dolt_add('.')",
			"call dolt_commit_hash_out(@commit_hash, '-m', 'Add Bob but dont update test')",
			"CALL dolt_checkout('main')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:       "CALL dolt_cherry_pick(@commit_hash)",
				SkipResultsCheck: true,
			},
		},
	},
	{
		Name: "cherry-pick with --skip-tests flag bypasses validation",
		SetUpScript: []string{
			"SET GLOBAL dolt_commit_run_test_groups = '*'",
			"CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100) NOT NULL, email VARCHAR(100))",
			"INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')",
			"INSERT INTO dolt_tests (test_name, test_group, test_query, assertion_type, assertion_comparator, assertion_value) VALUES " +
				"('test_users_count', 'unit', 'SELECT COUNT(*) FROM users', 'expected_single_value', '==', '1')",
			"CALL dolt_add('.')",
			"CALL dolt_commit('-m', 'Initial commit')",
			"CALL dolt_checkout('-b', 'feature')",
			"INSERT INTO users VALUES (2, 'Bob', 'bob@example.com')",
			"CALL dolt_add('.')",
			"call dolt_commit_hash_out(@commit_hash, '-m', 'Add Bob but dont update test')",
			"CALL dolt_checkout('main')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:       "CALL dolt_cherry_pick('--skip-tests', @commit_hash)",
				SkipResultsCheck: true,
			},
		},
	},
	{
		Name: "rebase with test validation enabled - tests pass",
		SetUpScript: []string{
			"SET GLOBAL dolt_commit_run_test_groups = '*'",
			"CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100) NOT NULL, email VARCHAR(100))",
			"INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')",
			"INSERT INTO dolt_tests (test_name, test_group, test_query, assertion_type, assertion_comparator, assertion_value) VALUES " +
				"('test_users_count', 'unit', 'SELECT COUNT(*) FROM users', 'expected_single_value', '==', '1')",
			"CALL dolt_add('.')",
			"CALL dolt_commit('-m', 'Initial commit')",
			"CALL dolt_checkout('-b', 'feature')",
			"INSERT INTO users VALUES (2, 'Bob', 'bob@example.com')",
			"UPDATE dolt_tests SET assertion_value = '2' WHERE test_name = 'test_users_count'",
			"CALL dolt_add('.')",
			"CALL dolt_commit('-m', 'Add Bob and update test')",
			"CALL dolt_checkout('main')",
			"INSERT INTO users VALUES (3, 'Charlie', 'charlie@example.com')",
			"CALL dolt_add('.')",
			"CALL dolt_commit('-m', 'Add Charlie')",
			"CALL dolt_checkout('feature')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:       "CALL dolt_rebase('main')",
				SkipResultsCheck: true,
			},
		},
	},
	{
		Name: "rebase with test validation enabled - tests fail, aborted",
		SetUpScript: []string{
			"SET GLOBAL dolt_commit_run_test_groups = '*'",
			"CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100) NOT NULL, email VARCHAR(100))",
			"INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')",
			"INSERT INTO dolt_tests (test_name, test_group, test_query, assertion_type, assertion_comparator, assertion_value) VALUES " +
				"('test_users_count', 'unit', 'SELECT COUNT(*) FROM users', 'expected_single_value', '==', '1')",
			"CALL dolt_add('.')",
			"CALL dolt_commit('-m', 'Initial commit')",
			"CALL dolt_checkout('-b', 'feature')",
			"INSERT INTO users VALUES (2, 'Bob', 'bob@example.com')",
			"CALL dolt_add('.')",
			"CALL dolt_commit('-m', 'Add Bob but dont update test')",
			"CALL dolt_checkout('main')",
			"INSERT INTO users VALUES (3, 'Charlie', 'charlie@example.com')",
			"CALL dolt_add('.')",
			"CALL dolt_commit('-m', 'Add Charlie')",
			"CALL dolt_checkout('feature')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:       "CALL dolt_rebase('main')",
				SkipResultsCheck: true,
			},
		},
	},
	{
		Name: "rebase with --skip-tests flag bypasses validation",
		SetUpScript: []string{
			"SET GLOBAL dolt_commit_run_test_groups = '*'",
			"CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100) NOT NULL, email VARCHAR(100))",
			"INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')",
			"INSERT INTO dolt_tests (test_name, test_group, test_query, assertion_type, assertion_comparator, assertion_value) VALUES " +
				"('test_users_count', 'unit', 'SELECT COUNT(*) FROM users', 'expected_single_value', '==', '1')",
			"CALL dolt_add('.')",
			"CALL dolt_commit('-m', 'Initial commit')",
			"CALL dolt_checkout('-b', 'feature')",
			"INSERT INTO users VALUES (2, 'Bob', 'bob@example.com')",
			"CALL dolt_add('.')",
			"CALL dolt_commit('-m', 'Add Bob but dont update test')",
			"CALL dolt_checkout('main')",
			"INSERT INTO users VALUES (3, 'Charlie', 'charlie@example.com')",
			"CALL dolt_add('.')",
			"CALL dolt_commit('-m', 'Add Charlie')",
			"CALL dolt_checkout('feature')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:       "CALL dolt_rebase('--skip-tests', 'main')",
				SkipResultsCheck: true,
			},
		},
	},
	{
		Name: "test validation with no dolt_tests table - no validation occurs",
		SetUpScript: []string{
			"SET GLOBAL dolt_commit_run_test_groups = '*'",
			"CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100) NOT NULL, email VARCHAR(100))",
			"INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')",
			"CALL dolt_add('.')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:       "CALL dolt_commit('-m', 'Commit without dolt_tests table')",
				SkipResultsCheck: true,
			},
		},
	},
	{
		Name: "test validation with empty dolt_tests table - no validation occurs",
		SetUpScript: []string{
			"SET GLOBAL dolt_commit_run_test_groups = '*'",
			"CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100) NOT NULL, email VARCHAR(100))",
			"INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')",
			"DELETE FROM dolt_tests",
			"CALL dolt_add('.')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:       "CALL dolt_commit('-m', 'Commit with empty dolt_tests table')",
				SkipResultsCheck: true,
			},
		},
	},
	{
		Name: "test validation with mixed test groups - only specified groups run",
		SetUpScript: []string{
			"SET GLOBAL dolt_commit_run_test_groups = 'unit'",
			"CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100) NOT NULL, email VARCHAR(100))",
			"INSERT INTO users VALUES (1, 'Alice', 'alice@example.com'), (2, 'Bob', 'bob@example.com')",
			"INSERT INTO dolt_tests (test_name, test_group, test_query, assertion_type, assertion_comparator, assertion_value) VALUES " +
				"('test_users_unit', 'unit', 'SELECT COUNT(*) FROM users', 'expected_single_value', '==', '2'), " +
				"('test_users_integration', 'integration', 'SELECT COUNT(*) FROM users', 'expected_single_value', '==', '999')",
			"CALL dolt_add('.')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:       "CALL dolt_commit('-m', 'Commit with unit tests only - should pass')",
				SkipResultsCheck: true,
			},
		},
	},
	{
		Name: "test validation error message includes test details",
		SetUpScript: []string{
			"SET GLOBAL dolt_commit_run_test_groups = '*'",
			"CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100) NOT NULL, email VARCHAR(100))",
			"INSERT INTO users VALUES (1, 'Alice', 'alice@example.com'), (2, 'Bob', 'bob@example.com')",
			"INSERT INTO dolt_tests (test_name, test_group, test_query, assertion_type, assertion_comparator, assertion_value) VALUES " +
				"('test_specific_failure', 'unit', 'SELECT COUNT(*) FROM users', 'expected_single_value', '==', '999')",
			"CALL dolt_add('.')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:       "CALL dolt_commit('-m', 'Commit with specific test failure')",
				SkipResultsCheck: true,
			},
		},
	},
}

// Test validation for push operations (when implemented)
var DoltPushTestValidationScripts = []queries.ScriptTest{
	{
		Name: "push with test validation enabled - tests pass",
		SetUpScript: []string{
			"SET GLOBAL dolt_push_run_test_groups = '*'",
			"CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100) NOT NULL, email VARCHAR(100))",
			"INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')",
			"INSERT INTO dolt_tests (test_name, test_group, test_query, assertion_type, assertion_comparator, assertion_value) VALUES " +
				"('test_alice_exists', 'unit', 'SELECT COUNT(*) FROM users WHERE name = \"Alice\"', 'expected_single_value', '==', '1')",
			"CALL dolt_add('.')",
			"CALL dolt_commit('-m', 'Initial commit')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "CALL dolt_push('origin', 'main')",
				ExpectedErrStr: "remote 'origin' not found", // Expected since we don't have a real remote
			},
		},
	},
	/*
		{
			Name: "push with --skip-tests flag bypasses validation",
			SetUpScript: []string{
				"SET GLOBAL dolt_push_run_test_groups = '*'",
				"CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100) NOT NULL, email VARCHAR(100))",
				"INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')",
				"INSERT INTO dolt_tests (test_name, test_group, test_query, assertion_type, assertion_comparator, assertion_value) VALUES " +
					"('test_will_fail', 'unit', 'SELECT COUNT(*) FROM users', 'expected_single_value', '==', '999')",
				"CALL dolt_add('.')",
				"CALL dolt_commit('-m', 'Initial commit')",
			},
			Assertions: []queries.ScriptTestAssertion{
				{
					Query:          "CALL dolt_push('--skip-tests', 'origin', 'main')",
					ExpectedErrStr: "remote 'origin' not found", // Expected since we don't have a real remote
				},
			},
		},
	*/
}
