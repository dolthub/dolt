#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    
    dolt sql <<SQL
CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100)
);
INSERT INTO users VALUES (1, 'Alice', 'alice@example.com');
CALL DOLT_ADD('.');
CALL DOLT_COMMIT('-m', 'Initial commit');
SQL
}

getHeadHash() {
  run dolt sql -r csv -q "select commit_hash from dolt_log limit 1 offset 0;"
  [ "$status" -eq 0 ] || return 1
  echo "${lines[1]}"
}

@test "commit verification: system variables can be set" {
    run dolt sql -q "SET @@PERSIST.dolt_commit_verification_groups = '*'"
    [ "$status" -eq 0 ]
    
    run dolt sql -q "SHOW GLOBAL VARIABLES LIKE 'dolt_commit_verification_groups'"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "*" ]]
}

@test "commit verification: commit with tests enabled - all tests pass" {
    dolt sql -q "SET @@PERSIST.dolt_commit_verification_groups = '*'"
    
    dolt sql <<SQL
INSERT INTO dolt_tests (test_name, test_group, test_query, assertion_type, assertion_comparator, assertion_value) VALUES 
('test_users_count', 'unit', 'SELECT COUNT(*) FROM users', 'expected_single_value', '==', '1'),
('test_alice_exists', 'unit', 'SELECT COUNT(*) FROM users WHERE name = "Alice"', 'expected_single_value', '==', '1');
SQL

    dolt add .
    
    run dolt commit -m "Commit with passing tests"
    [ "$status" -eq 0 ]
}

@test "commit verification: abort commit, then skip verification to bypass" {
    dolt sql -q "SET @@PERSIST.dolt_commit_verification_groups = '*'"
    
    dolt sql <<SQL
INSERT INTO dolt_tests (test_name, test_group, test_query, assertion_type, assertion_comparator, assertion_value) VALUES 
('test_will_fail', 'unit', 'SELECT COUNT(*) FROM users', 'expected_single_value', '==', '999');
SQL

    dolt add .
    
    run dolt commit -m "Commit that should fail verification"
    [ "$status" -ne 0 ]
    [[ "$output" =~ "commit verification failed" ]]
    [[ "$output" =~ "test_will_fail" ]]
    [[ "$output" =~ "Expected '999' but got '1'" ]]

    run dolt commit --skip-verification -m "Skip verification commit"
    [ "$status" -eq 0 ]
}

@test "commit verification: specific test groups - only specified groups run" {
    dolt sql -q "SET @@PERSIST.dolt_commit_verification_groups = 'unit'"
    
    # Add tests in different groups
    dolt sql <<SQL
INSERT INTO dolt_tests (test_name, test_group, test_query, assertion_type, assertion_comparator, assertion_value) VALUES 
('test_users_unit', 'unit', 'SELECT COUNT(*) FROM users', 'expected_single_value', '==', '1'),
('test_users_integration', 'integration', 'SELECT COUNT(*) FROM users', 'expected_single_value', '==', '999');
SQL

    dolt add .
    
    # Commit should succeed because only unit tests run (integration test that would fail is ignored)
    run dolt commit -m "Commit with unit tests only"
    [ "$status" -eq 0 ]
}

@test "commit verification: merge with tests enabled - tests pass" {
    dolt sql -q "SET @@PERSIST.dolt_commit_verification_groups = '*'"
    
    dolt sql <<SQL
INSERT INTO dolt_tests (test_name, test_group, test_query, assertion_type, assertion_comparator, assertion_value) VALUES 
('test_alice_exists', 'unit', 'SELECT COUNT(*) FROM users WHERE name = "Alice"', 'expected_single_value', '==', '1');
SQL
    dolt add .
    dolt commit -m "Initial commit"
    
    dolt checkout -b feature
    dolt sql -q "INSERT INTO users VALUES (2, 'Bob', 'bob@example.com')"
    dolt sql <<SQL
INSERT INTO dolt_tests (test_name, test_group, test_query, assertion_type, assertion_comparator, assertion_value) VALUES 
('test_bob_exists', 'unit', 'SELECT COUNT(*) FROM users WHERE name = "Bob"', 'expected_single_value', '==', '1');
SQL
    dolt add .
    dolt commit -m "Add Bob"
    
    dolt checkout main
    run dolt merge feature
    [ "$status" -eq 0 ]
}

@test "commit verification: merge with tests enabled - tests fail, merge aborted" {
    dolt sql -q "SET @@PERSIST.dolt_commit_verification_groups = '*'"
    
    dolt sql <<SQL
INSERT INTO dolt_tests (test_name, test_group, test_query, assertion_type, assertion_comparator, assertion_value) VALUES 
('test_will_fail', 'unit', 'SELECT COUNT(*) FROM users', 'expected_single_value', '==', '999');
SQL
    dolt add .
    dolt commit --skip-verification -m "Initial commit with failing test"
    
    dolt checkout -b feature
    dolt sql -q "INSERT INTO users VALUES (2, 'Bob', 'bob@example.com')"
    dolt add .
    dolt commit --skip-verification -m "Add Bob"
    
    # Add Charlie to main to force non-fast-forward merge
    dolt checkout main
    dolt sql -q "INSERT INTO users VALUES (3, 'Charlie', 'charlie@example.com')"
    dolt add .
    dolt commit --skip-verification -m "Add Charlie"
    
    run dolt merge feature
    [ "$status" -ne 0 ]
    [[ "$output" =~ "commit verification failed" ]]
    [[ "$output" =~ "test_will_fail" ]]
    [[ "$output" =~ "Expected '999' but got '3'" ]]

    run dolt merge --skip-verification feature
    [ "$status" -eq 0 ]
}

@test "commit verification: cherry-pick with tests enabled - tests pass" {
    dolt sql -q "SET @@PERSIST.dolt_commit_verification_groups = '*'"
    
    dolt sql <<SQL
INSERT INTO dolt_tests (test_name, test_group, test_query, assertion_type, assertion_comparator, assertion_value) VALUES 
('test_user_count_update', 'unit', 'SELECT COUNT(*) FROM users', 'expected_single_value', '==', '1');
SQL
    dolt add .
    dolt commit --skip-verification -m "Add test"
    
    dolt checkout -b feature
    dolt sql -q "INSERT INTO users VALUES (2, 'Bob', 'bob@example.com')"
    dolt sql -q "UPDATE dolt_tests SET assertion_value = '2' WHERE test_name = 'test_user_count_update'"
    dolt add .
    dolt commit --skip-verification -m "Add Bob and update test"
    commit_hash=$(getHeadHash)
    
    dolt checkout main
    run dolt cherry-pick $commit_hash
    [ "$status" -eq 0 ]
}

@test "commit verification: cherry-pick with tests enabled - tests fail, aborted" {
    dolt sql -q "SET @@PERSIST.dolt_commit_verification_groups = '*'"
    
    dolt sql <<SQL
INSERT INTO dolt_tests (test_name, test_group, test_query, assertion_type, assertion_comparator, assertion_value) VALUES 
('test_users_count', 'unit', 'SELECT COUNT(*) FROM users', 'expected_single_value', '==', '1');
SQL
    dolt add .
    dolt commit -m "Initial commit"
    
    dolt checkout -b feature
    dolt sql -q "INSERT INTO users VALUES (2, 'Bob', 'bob@example.com')"
    dolt add .
    dolt commit --skip-verification -m "Add Bob but don't update test"
    commit_hash=$(getHeadHash)
    
    dolt checkout main
    run dolt cherry-pick $commit_hash
    [ "$status" -ne 0 ]
    [[ "$output" =~ "commit verification failed" ]]
    [[ "$output" =~ "test_users_count" ]]
    [[ "$output" =~ "Expected '1' but got '2'" ]]

    run dolt cherry-pick --skip-verification $commit_hash
    [ "$status" -eq 0 ]
}

@test "commit verification: rebase with tests enabled - tests pass" {
    dolt sql -q "SET @@PERSIST.dolt_commit_verification_groups = '*'"
    
    dolt sql <<SQL
INSERT INTO dolt_tests (test_name, test_group, test_query, assertion_type, assertion_comparator, assertion_value) VALUES 
('test_users_count', 'unit', 'SELECT COUNT(*) FROM users', 'expected_single_value', '==', '1');
SQL
    dolt add .
    dolt commit -m "Initial commit"
    
    dolt sql -q "UPDATE users SET name = 'Zed' WHERE id = 1"
    dolt commit -am "Update Alice to Zed"  # Tests still pass
    
    dolt checkout -b feature HEAD~1
    dolt sql -q "INSERT INTO users VALUES (2, 'Bob', 'bob@example.com')"
    dolt sql -q "UPDATE dolt_tests SET assertion_value = '2' WHERE test_name = 'test_users_count'"
    dolt add .
    dolt commit -m "Add Bob and update test"
    
    dolt sql -q "INSERT INTO users VALUES (3, 'Charlie', 'charlie@example.com')"
    dolt sql -q "UPDATE dolt_tests SET assertion_value = '3' WHERE test_name = 'test_users_count'"
    dolt add .
    dolt commit -m "Add Charlie, update test"
    
    run dolt rebase main
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully rebased" ]]
}

@test "commit verification: rebase with tests enabled - tests fail, aborted" {
    skip "Rebase restart of workflow on failed verification is currently busted."

    dolt sql -q "SET @@PERSIST.dolt_commit_verification_groups = '*'"
    
    dolt sql <<SQL
INSERT INTO dolt_tests (test_name, test_group, test_query, assertion_type, assertion_comparator, assertion_value) VALUES 
('test_users_count', 'unit', 'SELECT COUNT(*) FROM users', 'expected_single_value', '==', '1');
SQL
    dolt add .
    dolt commit -m "Initial commit"
    
    dolt checkout -b feature
    dolt sql -q "INSERT INTO users VALUES (2, 'Bob', 'bob@example.com')"
    dolt sql -q "UPDATE dolt_tests SET assertion_value = '2' WHERE test_name = 'test_users_count'"
    dolt add .
    dolt commit -m "Add Bob and update test"
    
    dolt checkout main
    dolt sql -q "INSERT INTO users VALUES (3, 'Charlie', 'charlie@example.com')"
    dolt add .
    dolt commit --skip-verification -m "Add Charlie"
    
    dolt checkout feature
    
    run dolt rebase main
    [ "$status" -ne 0 ]
    [[ "$output" =~ "commit verification failed" ]]
    [[ "$output" =~ "test_users_count" ]]
    [[ "$output" =~ "Expected '2' but got '3'" ]]

    run dolt rebase --skip-verification main
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully rebased" ]]
}
