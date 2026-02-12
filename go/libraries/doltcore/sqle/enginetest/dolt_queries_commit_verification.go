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
	"regexp"

	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/store/hash"
)

// commitHashValidator validates commit hash format (32 character hex)
type commitHashValidator struct{}

var _ enginetest.CustomValueValidator = &commitHashValidator{}

func (chv *commitHashValidator) Validate(val interface{}) (bool, error) {
	h, ok := val.(string)
	if !ok {
		return false, nil
	}

	_, ok = hash.MaybeParse(h)
	return ok, nil
}

// successfulRebaseMessageValidator validates successful rebase message format
type successfulRebaseMessageValidator struct{}

var _ enginetest.CustomValueValidator = &successfulRebaseMessageValidator{}
var successfulRebaseRegex = regexp.MustCompile(`^Successfully rebased.*`)

func (srmv *successfulRebaseMessageValidator) Validate(val interface{}) (bool, error) {
	message, ok := val.(string)
	if !ok {
		return false, nil
	}
	return successfulRebaseRegex.MatchString(message), nil
}

var commitHash = &commitHashValidator{}
var successfulRebaseMessage = &successfulRebaseMessageValidator{}

var DoltCommitVerificationScripts = []queries.ScriptTest{
	{
		Name: "test verification system variables exist and have correct defaults",
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SHOW GLOBAL VARIABLES LIKE 'dolt_commit_verification_groups'",
				Expected: []sql.Row{
					{"dolt_commit_verification_groups", ""},
				},
			},
			{ // Test harness bleeds GLOBAL variable changes across tests, so reset after each test.
				Query:            "SET GLOBAL dolt_commit_verification_groups = ''",
				SkipResultsCheck: true,
			},
		},
	},
	{
		Name: "test verification system variables can be set",
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SET GLOBAL dolt_commit_verification_groups = '*'",
				Expected: []sql.Row{{types.OkResult{}}},
			},
			{
				Query: "SHOW GLOBAL VARIABLES LIKE 'dolt_commit_verification_groups'",
				Expected: []sql.Row{
					{"dolt_commit_verification_groups", "*"},
				},
			},
			{
				Query:    "SET GLOBAL dolt_commit_verification_groups = 'unit,integration'",
				Expected: []sql.Row{{types.OkResult{}}},
			},
			{
				Query: "SHOW GLOBAL VARIABLES LIKE 'dolt_commit_verification_groups'",
				Expected: []sql.Row{
					{"dolt_commit_verification_groups", "unit,integration"},
				},
			},
			{ // Test harness bleeds GLOBAL variable changes across tests, so reset after each test.
				Query:            "SET GLOBAL dolt_commit_verification_groups = ''",
				SkipResultsCheck: true,
			},
		},
	},
	{
		Name: "commit verification enabled - all tests pass",
		SetUpScript: []string{
			"SET GLOBAL dolt_commit_verification_groups = '*'",
			"CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100) NOT NULL, email VARCHAR(100))",
			"INSERT INTO users VALUES (1, 'Alice', 'alice@example.com'), (2, 'Bob', 'bob@example.com')",
			"INSERT INTO dolt_tests (test_name, test_group, test_query, assertion_type, assertion_comparator, assertion_value) VALUES " +
				"('test_users_count', 'unit', 'SELECT COUNT(*) FROM users', 'expected_single_value', '==', '2'), " +
				"('test_alice_exists', 'unit', 'SELECT COUNT(*) FROM users WHERE name = \"Alice\"', 'expected_single_value', '==', '1')",
			"CALL dolt_add('.')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "CALL dolt_commit('-m', 'Commit with passing tests')",
				ExpectedColumns: sql.Schema{
					{Name: "hash", Type: types.LongText, Nullable: false},
				},
				Expected: []sql.Row{{commitHash}},
			},
			{ // Test harness bleeds GLOBAL variable changes across tests, so reset after each test.
				Query:            "SET GLOBAL dolt_commit_verification_groups = ''",
				SkipResultsCheck: true,
			},
		},
	},
	{
		Name: "commit verification enabled - tests fail, commit aborted",
		SetUpScript: []string{
			"SET GLOBAL dolt_commit_verification_groups = '*'",
			"CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100) NOT NULL, email VARCHAR(100))",
			"INSERT INTO users VALUES (1, 'Alice', 'alice@example.com'), (2, 'Bob', 'bob@example.com')",
			"INSERT INTO dolt_tests (test_name, test_group, test_query, assertion_type, assertion_comparator, assertion_value) VALUES " +
				"('test_users_count', 'unit', 'SELECT COUNT(*) FROM users', 'expected_single_value', '==', '2'), " +
				"('test_will_fail', 'integration', 'SELECT COUNT(*) FROM users', 'expected_single_value', '==', '999')",
			"CALL dolt_add('.')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "CALL dolt_commit('-m', 'Commit that should fail verification')",
				ExpectedErrStr: "commit verification failed: test_will_fail (Assertion failed: expected_single_value equal to 999, got 2)",
			},
			{
				Query:    "CALL dolt_commit('--skip-verification','-m', 'skip verification')",
				Expected: []sql.Row{{commitHash}},
			},
			{ // Test harness bleeds GLOBAL variable changes across tests, so reset after each test.
				Query:            "SET GLOBAL dolt_commit_verification_groups = ''",
				SkipResultsCheck: true,
			},
		},
	},
	{
		Name: "commit with test verification - specific test groups",
		SetUpScript: []string{
			"SET GLOBAL dolt_commit_verification_groups = 'unit'",
			"CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100) NOT NULL, email VARCHAR(100))",
			"INSERT INTO users VALUES (1, 'Alice', 'alice@example.com'), (2, 'Bob', 'bob@example.com')",
			"INSERT INTO dolt_tests (test_name, test_group, test_query, assertion_type, assertion_comparator, assertion_value) VALUES " +
				"('test_users_count', 'unit', 'SELECT COUNT(*) FROM users', 'expected_single_value', '==', '2'), " +
				"('test_will_fail', 'integration', 'SELECT COUNT(*) FROM users', 'expected_single_value', '==', '999')",
			"CALL dolt_add('.')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL dolt_commit('-m', 'Commit with unit tests only')",
				Expected: []sql.Row{{commitHash}},
			},
			{
				Query:            "SET GLOBAL dolt_commit_verification_groups = 'integration'",
				SkipResultsCheck: true,
			},
			{
				Query:          "CALL dolt_commit('--allow-empty', '--amend', '-m', 'fail please')",
				ExpectedErrStr: "commit verification failed: test_will_fail (Assertion failed: expected_single_value equal to 999, got 2)",
			},
			{
				Query:    "CALL dolt_commit('--allow-empty', '--amend', '--skip-verification', '-m', 'skip the tests')",
				Expected: []sql.Row{{commitHash}},
			},
			{ // Test harness bleeds GLOBAL variable changes across tests, so reset after each test.
				Query:            "SET GLOBAL dolt_commit_verification_groups = ''",
				SkipResultsCheck: true,
			},
		},
	},
	{
		Name: "cherry-pick with test verification enabled - tests pass",
		SetUpScript: []string{
			"SET GLOBAL dolt_commit_verification_groups = '*'",
			"CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100) NOT NULL, email VARCHAR(100))",
			"INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')",
			"INSERT INTO dolt_tests (test_name, test_group, test_query, assertion_type, assertion_comparator, assertion_value) VALUES " +
				"('test_user_count_update', 'unit', 'SELECT COUNT(*) FROM users', 'expected_single_value', '==', '1')",
			"CALL dolt_add('.')",
			"CALL dolt_commit('--skip-verification', '-m', 'add test')",
			"CALL dolt_checkout('-b', 'feature')",
			"INSERT INTO users VALUES (2, 'Bob', 'bob@example.com')",
			"UPDATE dolt_tests SET assertion_value = '2' WHERE test_name = 'test_user_count_update'",
			"CALL dolt_add('.')",
			"call dolt_commit_hash_out(@commit_1_hash,'--skip-verification', '-m', 'Add Bob and update test')",
			"INSERT INTO users VALUES (3, 'Charlie', 'chuck@exampl.com')",
			"CALL dolt_add('.')",
			"call dolt_commit_hash_out(@commit_2_hash,'--skip-verification', '-m', 'Add Charlie')",
			"CALL dolt_checkout('main')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL dolt_cherry_pick(@commit_1_hash)",
				Expected: []sql.Row{{commitHash, int64(0), int64(0), int64(0)}},
			},
			{
				Query:          "CALL dolt_cherry_pick(@commit_2_hash)",
				ExpectedErrStr: "commit verification failed: test_user_count_update (Assertion failed: expected_single_value equal to 2, got 3)",
			},
			{ // Test harness bleeds GLOBAL variable changes across tests, so reset after each test.
				Query:            "SET GLOBAL dolt_commit_verification_groups = ''",
				SkipResultsCheck: true,
			},
		},
	},
	{
		Name: "cherry-pick with test verification enabled - tests fail, aborted",
		SetUpScript: []string{
			"SET GLOBAL dolt_commit_verification_groups = '*'",
			"CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100) NOT NULL, email VARCHAR(100))",
			"INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')",
			"INSERT INTO dolt_tests (test_name, test_group, test_query, assertion_type, assertion_comparator, assertion_value) VALUES " +
				"('test_users_count', 'unit', 'SELECT COUNT(*) FROM users', 'expected_single_value', '==', '1')",
			"CALL dolt_add('.')",
			"CALL dolt_commit('-m', 'Initial commit')",
			"CALL dolt_checkout('-b', 'feature')",
			"INSERT INTO users VALUES (2, 'Bob', 'bob@example.com')",
			"CALL dolt_add('.')",
			"call dolt_commit_hash_out(@commit_hash,'--skip-verification', '-m', 'Add Bob but dont update test')",
			"CALL dolt_checkout('main')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "CALL dolt_cherry_pick(@commit_hash)",
				ExpectedErrStr: "commit verification failed: test_users_count (Assertion failed: expected_single_value equal to 1, got 2)",
			},
			{
				Query:    "CALL dolt_cherry_pick('--skip-verification', @commit_hash)",
				Expected: []sql.Row{{commitHash, int64(0), int64(0), int64(0)}},
			},
			{
				Query: "select * from dolt_test_run('*')",
				Expected: []sql.Row{
					{"test_users_count", "unit", "SELECT COUNT(*) FROM users", "FAIL", "Assertion failed: expected_single_value equal to 1, got 2"},
				},
			},
			{ // Test harness bleeds GLOBAL variable changes across tests, so reset after each test.
				Query:            "SET GLOBAL dolt_commit_verification_groups = ''",
				SkipResultsCheck: true,
			},
		},
	},
	{
		Name: "rebase with test verification enabled - tests pass",
		SetUpScript: []string{
			"SET GLOBAL dolt_commit_verification_groups = '*'",
			"CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100) NOT NULL, email VARCHAR(100))",
			"INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')",
			"INSERT INTO dolt_tests (test_name, test_group, test_query, assertion_type, assertion_comparator, assertion_value) VALUES " +
				"('test_users_count', 'unit', 'SELECT COUNT(*) FROM users', 'expected_single_value', '==', '1')",
			"CALL dolt_add('.')",
			"CALL dolt_commit('-m', 'Initial commit')",
			"DELETE FROM users where id = 1",
			"INSERT INTO users VALUES (1, 'Zed', 'zed@example.com')",
			"CALL dolt_commit('-am', 'drop Alice, add Zed')", // tests still pass here.
			"CALL dolt_checkout('-b', 'feature', 'HEAD~1')",
			"INSERT INTO users VALUES (2, 'Bob', 'bob@example.com')",
			"UPDATE dolt_tests SET assertion_value = '2' WHERE test_name = 'test_users_count'",
			"CALL dolt_add('.')",
			"CALL dolt_commit('-m', 'Add Bob and update test')",
			"INSERT INTO users VALUES (3, 'Charlie', 'charlie@example.com')",
			"UPDATE dolt_tests SET assertion_value = '3' WHERE test_name = 'test_users_count'",
			"CALL dolt_add('.')",
			"CALL dolt_commit('-m', 'Add Charlie, update test')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL dolt_rebase('main')",
				Expected: []sql.Row{{int64(0), successfulRebaseMessage}},
			},
			{ // Test harness bleeds GLOBAL variable changes across tests, so reset after each test.
				Query:            "SET GLOBAL dolt_commit_verification_groups = ''",
				SkipResultsCheck: true,
			},
		},
	},

	{
		Name: "rebase with test verification enabled - tests fail, aborted",
		SetUpScript: []string{
			"SET GLOBAL dolt_commit_verification_groups = '*'",
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
			"CALL dolt_commit('-m', 'Add Bob but dont update test')",
			"CALL dolt_checkout('main')",
			"INSERT INTO users VALUES (3, 'Charlie', 'charlie@example.com')",
			"CALL dolt_add('.')",
			"CALL dolt_commit('--skip-verification', '-m', 'Add Charlie')", // this will trip the existing test.
			"CALL dolt_checkout('feature')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "CALL dolt_rebase('main')",
				ExpectedErrStr: "commit verification failed: test_users_count (Assertion failed: expected_single_value equal to 2, got 3)",
			},
			{
				Query:    "CALL dolt_rebase('--abort')",
				Expected: []sql.Row{{0, "Interactive rebase aborted"}},
			},
			{
				Query:    "CALL dolt_rebase('--skip-verification', 'main')",
				Expected: []sql.Row{{int64(0), successfulRebaseMessage}},
			},
			{
				Query: "select * from dolt_test_run('*')",
				Expected: []sql.Row{
					{"test_users_count", "unit", "SELECT COUNT(*) FROM users", "FAIL", "Assertion failed: expected_single_value equal to 2, got 3"},
				},
			},
			{ // Test harness bleeds GLOBAL variable changes across tests, so reset after each test.
				Query:            "SET GLOBAL dolt_commit_verification_groups = ''",
				SkipResultsCheck: true,
			},
		},
	},
	{
		Name: "interactive rebase with --skip-verification flag should persist across continue operations",
		SetUpScript: []string{
			"SET GLOBAL dolt_commit_verification_groups = '*'",
			"CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100) NOT NULL, email VARCHAR(100))",
			"INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')",
			"INSERT INTO dolt_tests (test_name, test_group, test_query, assertion_type, assertion_comparator, assertion_value) VALUES " +
				"('test_users_count', 'unit', 'SELECT COUNT(*) FROM users', 'expected_single_value', '==', '1')",
			"CALL dolt_add('.')",
			"CALL dolt_commit('--skip-verification', '-m', 'Initial commit')",
			"CALL dolt_checkout('-b', 'feature')",
			"INSERT INTO users VALUES (2, 'Bob', 'bob@example.com')",
			"CALL dolt_add('.')",
			"CALL dolt_commit('--skip-verification', '-m', 'Add Bob but dont update test')", // This will cause test to fail
			"INSERT INTO users VALUES (3, 'Charlie', 'charlie@example.com')",
			"CALL dolt_add('.')",
			"CALL dolt_commit('--skip-verification', '-m', 'Add Charlie')",
			"CALL dolt_checkout('main')",
			"INSERT INTO users VALUES (4, 'David', 'david@example.com')", // Add a commit to main to create divergence
			"CALL dolt_add('.')",
			"CALL dolt_commit('--skip-verification', '-m', 'Add David on main')",
			"CALL dolt_checkout('feature')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL dolt_rebase('--interactive', '--skip-verification', 'main')",
				Expected: []sql.Row{{0, "interactive rebase started on branch dolt_rebase_feature; adjust the rebase plan in the dolt_rebase table, then continue rebasing by calling dolt_rebase('--continue')"}},
			},
			{
				Query:    "CALL dolt_rebase('--continue')", // This should NOT require --skip-verification flag but should still skip tests
				Expected: []sql.Row{{int64(0), successfulRebaseMessage}},
			},
			{ // Test harness bleeds GLOBAL variable changes across tests, so reset after each test.
				Query:            "SET GLOBAL dolt_commit_verification_groups = ''",
				SkipResultsCheck: true,
			},
		},
	},
	{
		Name: "test verification with no dolt_tests errors",
		SetUpScript: []string{
			"SET GLOBAL dolt_commit_verification_groups = '*'",
			"CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100) NOT NULL, email VARCHAR(100))",
			"INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')",
			"CALL dolt_add('.')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "CALL dolt_commit('-m', 'Commit without dolt_tests table')",
				ExpectedErrStr: "failed to run dolt_test_run for group *: could not find tests for argument: *",
			},
			{ // Test harness bleeds GLOBAL variable changes across tests, so reset after each test.
				Query:            "SET GLOBAL dolt_commit_verification_groups = ''",
				SkipResultsCheck: true,
			},
		},
	},
	{
		Name: "test verification with mixed test groups - only specified groups run",
		SetUpScript: []string{
			"SET GLOBAL dolt_commit_verification_groups = 'unit'",
			"CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100) NOT NULL, email VARCHAR(100))",
			"INSERT INTO users VALUES (1, 'Alice', 'alice@example.com'), (2, 'Bob', 'bob@example.com')",
			"INSERT INTO dolt_tests (test_name, test_group, test_query, assertion_type, assertion_comparator, assertion_value) VALUES " +
				"('test_users_unit', 'unit', 'SELECT COUNT(*) FROM users', 'expected_single_value', '==', '2'), " +
				"('test_users_integration', 'integration', 'SELECT COUNT(*) FROM users', 'expected_single_value', '==', '999')",
			"CALL dolt_add('.')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL dolt_commit('-m', 'Commit with unit tests only - should pass')",
				Expected: []sql.Row{{commitHash}},
			},
			{ // Test harness bleeds GLOBAL variable changes across tests, so reset after each test.
				Query:            "SET GLOBAL dolt_commit_verification_groups = ''",
				SkipResultsCheck: true,
			},
		},
	},
	{
		Name: "test verification error message includes test details",
		SetUpScript: []string{
			"SET GLOBAL dolt_commit_verification_groups = '*'",
			"CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100) NOT NULL, email VARCHAR(100))",
			"INSERT INTO users VALUES (1, 'Alice', 'alice@example.com'), (2, 'Bob', 'bob@example.com')",
			"INSERT INTO dolt_tests (test_name, test_group, test_query, assertion_type, assertion_comparator, assertion_value) VALUES " +
				"('test_specific_failure', 'unit', 'SELECT COUNT(*) FROM users', 'expected_single_value', '==', '999')",
			"CALL dolt_add('.')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "CALL dolt_commit('-m', 'Commit with specific test failure')",
				ExpectedErrStr: "commit verification failed: test_specific_failure (Assertion failed: expected_single_value equal to 999, got 2)",
			},
			{ // Test harness bleeds GLOBAL variable changes across tests, so reset after each test.
				Query:            "SET GLOBAL dolt_commit_verification_groups = ''",
				SkipResultsCheck: true,
			},
		},
	},
	{
		Name: "merge with test verification enabled - tests pass",
		SetUpScript: []string{
			"SET GLOBAL dolt_commit_verification_groups = '*'",
			"CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100) NOT NULL, email VARCHAR(100))",
			"INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')",
			"INSERT INTO dolt_tests (test_name, test_group, test_query, assertion_type, assertion_comparator, assertion_value) VALUES " +
				"('test_alice_exists', 'unit', 'SELECT COUNT(*) FROM users WHERE name = \"Alice\"', 'expected_single_value', '==', '1')",
			"CALL dolt_add('.')",
			"CALL dolt_commit('-m', 'Initial commit')",
			"CALL dolt_checkout('-b', 'feature')",
			"INSERT INTO users VALUES (2, 'Bob', 'bob@example.com')",
			"INSERT INTO dolt_tests (test_name, test_group, test_query, assertion_type, assertion_comparator, assertion_value) VALUES " +
				"('test_bob_exists', 'unit', 'SELECT COUNT(*) FROM users WHERE name = \"Bob\"', 'expected_single_value', '==', '1')",
			"CALL dolt_add('.')",
			"CALL dolt_commit('--skip-verification', '-m', 'Add Bob')",
			"CALL dolt_checkout('main')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL dolt_merge('feature')",
				Expected: []sql.Row{{commitHash, int64(1), int64(0), "merge successful"}},
			},
			{ // Test harness bleeds GLOBAL variable changes across tests, so reset after each test.
				Query:            "SET GLOBAL dolt_commit_verification_groups = ''",
				SkipResultsCheck: true,
			},
		},
	},
	{
		Name: "merge with test verification enabled - tests fail, merge aborted",
		SetUpScript: []string{
			"SET GLOBAL dolt_commit_verification_groups = '*'",
			"CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100) NOT NULL, email VARCHAR(100))",
			"INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')",
			"INSERT INTO dolt_tests (test_name, test_group, test_query, assertion_type, assertion_comparator, assertion_value) VALUES " +
				"('test_will_fail', 'unit', 'SELECT COUNT(*) FROM users', 'expected_single_value', '==', '999')",
			"CALL dolt_add('.')",
			"CALL dolt_commit('--skip-verification', '-m', 'Initial commit with failing test')",
			"CALL dolt_checkout('-b', 'feature')",
			"INSERT INTO users VALUES (2, 'Bob', 'bob@example.com')",
			"CALL dolt_add('.')",
			"CALL dolt_commit('--skip-verification', '-m', 'Add Bob')",
			"CALL dolt_checkout('main')",
			"INSERT INTO users VALUES (3, 'Charlie', 'charlie@example.com')",
			"CALL dolt_add('.')",
			"CALL dolt_commit('--skip-verification', '-m', 'Add Charlie to force non-FF merge')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "CALL dolt_merge('feature')",
				ExpectedErrStr: "commit verification failed: test_will_fail (Assertion failed: expected_single_value equal to 999, got 3)",
			},
			{ // Test harness bleeds GLOBAL variable changes across tests, so reset after each test.
				Query:            "SET GLOBAL dolt_commit_verification_groups = ''",
				SkipResultsCheck: true,
			},
		},
	},
	{
		Name: "merge with --skip-verification flag bypasses verification",
		SetUpScript: []string{
			"SET GLOBAL dolt_commit_verification_groups = '*'",
			"CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100) NOT NULL, email VARCHAR(100))",
			"INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')",
			"INSERT INTO dolt_tests (test_name, test_group, test_query, assertion_type, assertion_comparator, assertion_value) VALUES " +
				"('test_will_fail', 'unit', 'SELECT COUNT(*) FROM users', 'expected_single_value', '==', '999')",
			"CALL dolt_add('.')",
			"CALL dolt_commit('--skip-verification', '-m', 'Initial commit with failing test')",
			"CALL dolt_checkout('-b', 'feature')",
			"INSERT INTO users VALUES (2, 'Bob', 'bob@example.com')",
			"CALL dolt_add('.')",
			"CALL dolt_commit('--skip-verification', '-m', 'Add Bob')",
			"CALL dolt_checkout('main')",
			"INSERT INTO users VALUES (3, 'Charlie', 'charlie@example.com')",
			"CALL dolt_add('.')",
			"CALL dolt_commit('--skip-verification', '-m', 'Add Charlie to force non-FF merge')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "CALL dolt_merge('--skip-verification', 'feature')",
				Expected: []sql.Row{{commitHash, int64(0), int64(0), "merge successful"}},
			},
			{
				Query: "select * from dolt_test_run('*')",
				Expected: []sql.Row{
					{"test_will_fail", "unit", "SELECT COUNT(*) FROM users", "FAIL", "Assertion failed: expected_single_value equal to 999, got 3"},
				},
			},
			{ // Test harness bleeds GLOBAL variable changes across tests, so reset after each test.
				Query:            "SET GLOBAL dolt_commit_verification_groups = ''",
				SkipResultsCheck: true,
			},
		},
	},
}
