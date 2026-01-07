#!/usr/bin/env bats
# Tests for CLI behavior when running commands from a parent directory
# that contains a child dolt database directory.
# See https://github.com/dolthub/dolt/issues/10230

load $BATS_TEST_DIRNAME/helper/common.bash
load $BATS_TEST_DIRNAME/helper/query-server-common.bash

setup() {
    skiponwindows "tests are flaky on Windows"
    if [ "$SQL_ENGINE" = "remote-engine" ]; then
      skip "This test tests local CLI behavior, SQL_ENGINE is not needed."
    fi
    setup_no_dolt_init
    # Create a child database directory
    mkdir child_db
    cd child_db
    dolt init
    dolt sql -q "CREATE TABLE test_table (pk INT PRIMARY KEY, value VARCHAR(100))"
    dolt add .
    dolt commit -m "Initial commit"
    cd ..
    # We are now in the parent directory with a child dolt database
}

teardown() {
    stop_sql_server 1 && sleep 0.5
    teardown_common
}

NOT_VALID_REPO_ERROR="The current directory is not a valid dolt repository."

# =============================================================================
# Tests WITHOUT a running SQL server
# All commands that require a repo should fail consistently from parent directory
# =============================================================================

@test "parent-directory: dolt status from parent dir without server fails" {
    run dolt status
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "parent-directory: dolt checkout from parent dir without server fails" {
    run dolt checkout -b new_branch
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "parent-directory: dolt branch from parent dir without server fails" {
    run dolt branch
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "parent-directory: dolt log from parent dir without server fails" {
    run dolt log
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "parent-directory: dolt diff from parent dir without server fails" {
    run dolt diff
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "parent-directory: dolt add from parent dir without server fails" {
    run dolt add .
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "parent-directory: dolt commit from parent dir without server fails" {
    run dolt commit -m "test"
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

# =============================================================================
# Tests WITH a running SQL server
# Commands should still fail from parent directory - they require a local repo
# =============================================================================

@test "parent-directory: dolt status from parent dir with running server fails" {
    start_multi_db_server child_db

    # Status should fail - it requires being in a dolt repo directory
    run dolt status
    [ "$status" -ne 0 ]
    [[ "$output" =~ "$NOT_VALID_REPO_ERROR" ]] || false
}

@test "parent-directory: dolt checkout from parent dir with running server fails" {
    start_multi_db_server child_db

    run dolt checkout -b new_branch
    [ "$status" -ne 0 ]
    [[ "$output" =~ "$NOT_VALID_REPO_ERROR" ]] || false
}

@test "parent-directory: dolt branch from parent dir with running server fails" {
    start_multi_db_server child_db

    run dolt branch
    [ "$status" -ne 0 ]
    [[ "$output" =~ "$NOT_VALID_REPO_ERROR" ]] || false
}

@test "parent-directory: dolt log from parent dir with running server fails" {
    start_multi_db_server child_db

    run dolt log
    [ "$status" -ne 0 ]
    [[ "$output" =~ "$NOT_VALID_REPO_ERROR" ]] || false
}

@test "parent-directory: dolt diff from parent dir with running server fails" {
    start_multi_db_server child_db

    run dolt diff
    [ "$status" -ne 0 ]
    [[ "$output" =~ "$NOT_VALID_REPO_ERROR" ]] || false
}

# =============================================================================
# Commands that do NOT require a repo should work from parent directory
# =============================================================================

@test "parent-directory: dolt sql from parent dir with running server succeeds" {
    start_multi_db_server child_db

    # SQL commands should work by connecting to the running server
    run dolt sql -q "SELECT * FROM child_db.test_table"
    [ "$status" -eq 0 ]
}

@test "parent-directory: dolt version from parent dir succeeds" {
    run dolt version
    [ "$status" -eq 0 ]
    [[ "$output" =~ "dolt version" ]] || false
}

@test "parent-directory: dolt init in parent dir succeeds" {
    run dolt init
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully initialized dolt data repository" ]] || false
}

# =============================================================================
# Commands should work correctly when run from within the child database directory
# =============================================================================

@test "parent-directory: commands in child directory work normally" {
    cd child_db

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch main" ]] || false

    run dolt checkout -b new_branch
    [ "$status" -eq 0 ]

    run dolt branch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "new_branch" ]] || false
    [[ "$output" =~ "main" ]] || false

    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Initial commit" ]] || false

    run dolt diff
    [ "$status" -eq 0 ]
}

# =============================================================================
# Test consistent behavior - all repo-requiring commands should fail the same way
# =============================================================================

@test "parent-directory: all repo-requiring commands fail consistently" {
    # All these commands should fail with the same error
    for cmd in "status" "branch" "log" "diff" "add ." "commit -m test" "checkout -b test"; do
        run dolt $cmd
        [ "$status" -ne 0 ]
        [[ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]] || false
    done
}
