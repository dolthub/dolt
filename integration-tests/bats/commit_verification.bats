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

@test "commit_verification: system variables can be set" {
    run dolt sql -q "SET @@PERSIST.dolt_commit_verification_groups = '*'"
    [ "$status" -eq 0 ]
    
    run dolt sql -q "SHOW GLOBAL VARIABLES LIKE 'dolt_commit_verification_groups'"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "*" ]] || false
}

@test "commit_verification: commit with tests enabled - all tests pass" {
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

@test "commit_verification: abort commit, then skip verification to bypass" {
    dolt sql -q "SET @@PERSIST.dolt_commit_verification_groups = '*'"
    
    dolt sql <<SQL
INSERT INTO dolt_tests (test_name, test_group, test_query, assertion_type, assertion_comparator, assertion_value) VALUES 
('test_will_fail', 'unit', 'SELECT COUNT(*) FROM users', 'expected_single_value', '==', '999');
SQL

    dolt add .
    
    run dolt commit -m "Commit that should fail verification"
    [ "$status" -ne 0 ]
    [[ "$output" =~ "commit verification failed" ]] || false
    [[ "$output" =~ "test_will_fail" ]] || false
    [[ "$output" =~ "expected_single_value equal to 999, got 1" ]] || false

    run dolt commit --skip-verification -m "Skip verification commit"
    [ "$status" -eq 0 ]
}

@test "commit_verification: specific test groups - only specified groups run" {
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

@test "commit_verification: merge with tests enabled - tests pass" {
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

@test "commit_verification: merge with tests enabled - tests fail, can abort and retry with --skip-verification" {
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
    [[ "$output" =~ "commit verification failed" ]] || false
    [[ "$output" =~ "test_will_fail" ]] || false
    [[ "$output" =~ "expected_single_value equal to 999, got 3" ]] || false

    # Merge state is preserved; abort before retrying with --skip-verification
    run dolt merge --abort
    [ "$status" -eq 0 ]

    run dolt merge --skip-verification feature
    [ "$status" -eq 0 ]
}

@test "commit_verification: cherry-pick with tests enabled - tests pass" {
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

@test "commit_verification: cherry-pick with tests enabled - tests fail, can abort and retry with --skip-verification" {
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
    [[ "$output" =~ "commit verification failed" ]] || false
    [[ "$output" =~ "test_users_count" ]] || false
    [[ "$output" =~ "expected_single_value equal to 1, got 2" ]] || false
    [[ "$output" =~ "commit verification failed" ]] || false
    [[ "$output" =~ "dolt cherry-pick --continue" ]] || false
    [[ "$output" =~ "dolt cherry-pick --abort" ]] || false

    # Cherry-pick state is preserved; abort before retrying with --skip-verification
    run dolt cherry-pick --abort
    [ "$status" -eq 0 ]
    run dolt cherry-pick --skip-verification $commit_hash
    [ "$status" -eq 0 ]
}

@test "commit_verification: rebase with tests enabled - tests pass" {
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
    [[ "$output" =~ "Successfully rebased" ]] || false
}

## Common pattern to seed a commit with a single test which enforces that there is only one row in the user table.
add_dolt_test() {
  dolt sql <<SQL
    INSERT INTO dolt_tests ( test_name, test_group, test_query, assertion_type, assertion_comparator, assertion_value )
    VALUES (
      'user_count',
      'unit',
      'SELECT COUNT(*) FROM users',
      'expected_single_value',
      '==',
      '1'
    );
SQL

  dolt add .
  dolt commit -m "Add verification test"
}


@test "commit_verification: rebase with tests enabled - tests fail, can abort and restart with --skip-verification" {
    dolt sql -q "SET @@PERSIST.dolt_commit_verification_groups = '*'"

    add_dolt_test

    dolt checkout -b feature
    dolt sql -q "INSERT INTO users VALUES (2, 'Bob', 'bob@example.com')"
    dolt sql -q "UPDATE dolt_tests SET assertion_value = '2' WHERE test_name = 'user_count'"
    dolt add .
    dolt commit -m "Add Bob and update test"

    dolt checkout main
    dolt sql -q "INSERT INTO users VALUES (3, 'Charlie', 'charlie@example.com')"
    dolt add .
    dolt commit --skip-verification -m "Add Charlie"

    dolt checkout feature

    run dolt rebase main
    [ "$status" -ne 0 ]
    [[ "$output" =~ "commit verification failed" ]] || false
    [[ "$output" =~ "user_count" ]] || false

    # Abort the failed rebase and retry with --skip-verification
    run dolt rebase --abort
    [ "$status" -eq 0 ]

    run dolt rebase --skip-verification main
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully rebased" ]] || false
}

@test "commit_verification: cherry-pick verification failure halts with dirty state preserved" {
    dolt sql -q "SET @@PERSIST.dolt_commit_verification_groups = '*';"

    add_dolt_test

    dolt checkout -b feature
    dolt sql -q "INSERT INTO users VALUES (2, 'Bob', 'bob@example.com')"
    dolt add .
    dolt commit --skip-verification -m "Add Bob without updating test"
    CHERRY_HASH=$(getHeadHash)

    dolt checkout main

    run dolt cherry-pick "$CHERRY_HASH"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "commit verification failed" ]] || false
    [[ "$output" =~ "user_count" ]] || false
    [[ "$output" =~ "expected_single_value equal to 1, got 2" ]] || false
    [[ "$output" =~ "dolt cherry-pick --continue" ]] || false
    [[ "$output" =~ "dolt cherry-pick --abort" ]] || false

    # Dirty state is preserved: users table should be staged
    run dolt sql -r csv -q "SELECT * FROM dolt_status"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "users,1,modified" ]] || false

    # --continue without fixing still fails
    run dolt cherry-pick --continue
    [ "$status" -eq 1 ]
    [[ "$output" =~ "commit verification failed" ]] || false
    [[ "$output" =~ "user_count" ]] || false
    [[ "$output" =~ "expected_single_value equal to 1, got 2" ]] || false
    [[ "$output" =~ "dolt cherry-pick --continue" ]] || false
    [[ "$output" =~ "dolt cherry-pick --abort" ]] || false

    # --abort restores clean state
    run dolt cherry-pick --abort
    [ "$status" -eq 0 ]

    run dolt status
    [ "$status" -eq 0 ]
    ! [[ "$output" =~ "users" ]] || false

    run dolt sql -r csv -q "SELECT * FROM users"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,Alice,alice@example.com" ]] || false
    [[ "${#lines[@]}" -eq 2 ]] || false
}

@test "commit_verification: cherry-pick fix data then --continue succeeds" {
    dolt sql -q "SET @@PERSIST.dolt_commit_verification_groups = '*';"

    add_dolt_test

    dolt checkout -b feature
    dolt sql -q "INSERT INTO users VALUES (2, 'Bob', 'bob@example.com')"
    dolt add .
    dolt commit --skip-verification -m "Add Bob without updating test"
    CHERRY_HASH=$(getHeadHash)

    dolt checkout main

    run dolt cherry-pick "$CHERRY_HASH"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "commit verification failed" ]] || false
    [[ "$output" =~ "user_count" ]] || false
    [[ "$output" =~ "expected_single_value equal to 1, got 2" ]] || false
    [[ "$output" =~ "dolt cherry-pick --continue" ]] || false
    [[ "$output" =~ "dolt cherry-pick --abort" ]] || false

    # Fix the test expectation and stage it
    dolt sql -q "UPDATE dolt_tests SET assertion_value = '2' WHERE test_name = 'user_count';"
    dolt add .

    # --continue should now succeed
    run dolt cherry-pick --continue
    [ "$status" -eq 0 ]

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "nothing to commit" ]] || false

    run dolt sql -r csv -q "SELECT * FROM users"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,Alice,alice@example.com" ]] || false
    [[ "$output" =~ "2,Bob,bob@example.com" ]] || false
    [[ "${#lines[@]}" -eq 3 ]] || false
}

@test "commit_verification: merge verification failure halts with dirty state preserved" {
    dolt sql -q "SET @@PERSIST.dolt_commit_verification_groups = '*';"

    add_dolt_test

    dolt checkout -b feature
    dolt sql -q "INSERT INTO users VALUES (2, 'Bob', 'bob@example.com')"
    dolt add .
    dolt commit --skip-verification -m "Add Bob without updating test"
    dolt checkout main
    dolt sql -q "INSERT INTO users VALUES (3, 'Charlie', 'charlie@example.com')"
    dolt add .
    dolt commit --skip-verification -m "Add Charlie to force non-FF merge"

    run dolt merge feature
    [ "$status" -eq 1 ]
    [[ "$output" =~ "commit verification failed" ]] || false
    [[ "$output" =~ "user_count" ]] || false
    [[ "$output" =~ "expected_single_value equal to 1, got 3" ]] || false

    # Dirty state is preserved: users table should be staged
    run dolt sql -r csv -q "SELECT * FROM dolt_status"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "users,1,modified" ]] || false

    # Merge does not have a --continue option, the expectation is you modify the data, stage, commit.
    # Addres by updating the test.
    dolt sql -q "UPDATE dolt_tests SET assertion_value = '3' WHERE test_name = 'user_count';"
    dolt add .

    run dolt commit -m "Complete merge after fixing test"
    [ "$status" -eq 0 ]
}

@test "commit_verification: rebase verification failure halts with dirty state preserved" {
    dolt sql -q "SET @@PERSIST.dolt_commit_verification_groups = '*';"

    add_dolt_test

    # Create divergence: main gets Charlie, b1 branch gets Bob without updating test
    dolt sql -q "INSERT INTO users VALUES (3, 'Charlie', 'charlie@example.com');"
    dolt add .
    dolt commit --skip-verification -m "Add Charlie to main"
    dolt checkout -b b1 HEAD~1
    dolt sql -q "INSERT INTO users VALUES (2, 'Bob', 'bob@example.com');"
    dolt add .
    dolt commit --skip-verification -m "Add Bob without updating test"

    # Rebase b1 onto main: applying Bob's commit on top of Alice+Charlie gives 3 rows, test expects 1
    run dolt rebase main
    [ "$status" -eq 1 ]
    [[ "$output" =~ "commit verification failed" ]] || false
    [[ "$output" =~ "user_count" ]] || false
    [[ "$output" =~ "expected_single_value equal to 1, got 3" ]]
    ## NM4 - looks like we don't print anything about how to continue/abort in the rebase case....

    # Dirty state is preserved: we are on dolt_rebase_b1 with users staged
    run dolt sql -r csv -q "SELECT * FROM dolt_status"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "users,1,modified" ]] || false

    # --abort restores clean state
    run dolt rebase --abort
    [ "$status" -eq 0 ]

    run dolt status
    [ "$status" -eq 0 ]
    ! [[ "$output" =~ "rebase in progress" ]] || false
}

@test "commit_verification: rebase fix data then --continue succeeds" {
    dolt sql -q "SET @@PERSIST.dolt_commit_verification_groups = '*';"

    add_dolt_test

    dolt branch b1
    dolt sql -q "INSERT INTO users VALUES (3, 'Charlie', 'charlie@example.com');"
    dolt add .
    dolt commit --skip-verification -m "Add Charlie to main"

    dolt checkout b1
    dolt sql -q "INSERT INTO users VALUES (2, 'Bob', 'bob@example.com');"
    dolt add .
    dolt commit --skip-verification -m "Add Bob without updating test"

    run dolt rebase main
    [ "$status" -eq 1 ]
    [[ "$output" =~ "commit verification failed" ]] || false
    [[ "$output" =~ "user_count" ]] || false
    [[ "$output" =~ "expected_single_value equal to 1, got 3" ]]

    # Fix the test expectation (3 users after rebase: Alice + Charlie + Bob) and stage
    dolt sql -q "UPDATE dolt_tests SET assertion_value = '3' WHERE test_name = 'user_count';"
    dolt add .

    # --continue should now succeed
    run dolt rebase --continue
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully rebased" ]] || false

    run dolt status
    [ "$status" -eq 0 ]
    ! [[ "$output" =~ "rebase in progress" ]] || false
}
