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

@test "requires-repo: dolt status from parent dir without server fails" {
    run dolt status
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "requires-repo: dolt checkout from parent dir without server fails" {
    run dolt checkout -b new_branch
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "requires-repo: dolt branch from parent dir without server fails" {
    run dolt branch
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "requires-repo: dolt log from parent dir without server fails" {
    run dolt log
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "requires-repo: dolt diff from parent dir without server fails" {
    run dolt diff
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "requires-repo: dolt add from parent dir without server fails" {
    run dolt add .
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "requires-repo: dolt commit from parent dir without server fails" {
    run dolt commit -m "test"
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

# =============================================================================
# Tests WITH a running SQL server
# Commands should still fail from parent directory - they require a local repo
# =============================================================================

@test "requires-repo: dolt status from parent dir with running server fails" {
    start_multi_db_server child_db

    # Status should fail - it requires being in a dolt repo directory
    run dolt status
    [ "$status" -ne 0 ]
    [[ "$output" =~ "$NOT_VALID_REPO_ERROR" ]] || false
}

@test "requires-repo: dolt checkout from parent dir with running server fails" {
    start_multi_db_server child_db

    run dolt checkout -b new_branch
    [ "$status" -ne 0 ]
    [[ "$output" =~ "$NOT_VALID_REPO_ERROR" ]] || false
}

@test "requires-repo: dolt branch from parent dir with running server fails" {
    start_multi_db_server child_db

    run dolt branch
    [ "$status" -ne 0 ]
    [[ "$output" =~ "$NOT_VALID_REPO_ERROR" ]] || false
}

@test "requires-repo: dolt log from parent dir with running server fails" {
    start_multi_db_server child_db

    run dolt log
    [ "$status" -ne 0 ]
    [[ "$output" =~ "$NOT_VALID_REPO_ERROR" ]] || false
}

@test "requires-repo: dolt diff from parent dir with running server fails" {
    start_multi_db_server child_db

    run dolt diff
    [ "$status" -ne 0 ]
    [[ "$output" =~ "$NOT_VALID_REPO_ERROR" ]] || false
}

# =============================================================================
# Commands that do NOT require a repo should work from parent directory
# =============================================================================

@test "requires-repo: dolt sql from parent dir with running server succeeds" {
    start_multi_db_server child_db

    # SQL commands should work by connecting to the running server
    run dolt sql -q "SELECT * FROM child_db.test_table"
    [ "$status" -eq 0 ]
}

@test "requires-repo: dolt version from parent dir succeeds" {
    run dolt version
    [ "$status" -eq 0 ]
    [[ "$output" =~ "dolt version" ]] || false
}

@test "requires-repo: dolt init in new dir succeeds" {
    # Test that dolt init works in a new directory (doesn't require existing repo)
    mkdir new_init_test
    cd new_init_test
    run dolt init
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully initialized dolt data repository" ]] || false
    cd ..
    rm -rf new_init_test
}

# =============================================================================
# Commands should work correctly when run from within the child database directory
# =============================================================================

@test "requires-repo: commands in child directory work normally" {
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

@test "requires-repo: all repo-requiring commands fail consistently" {
    # All these commands should fail with the same error
    for cmd in "status" "branch" "log" "diff" "add ." "commit -m test" "checkout -b test"; do
        run dolt $cmd
        [ "$status" -ne 0 ]
        [[ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]] || false
    done
}

# =============================================================================
# Tests with --use-db or --host flags bypass repo requirement
# These flags indicate a manual database/server connection, so the local
# directory check should be bypassed.
# =============================================================================

@test "requires-repo: --host and --use-db flags bypass repo requirement for log" {
    start_multi_db_server child_db

    # With --host and --use-db, log should work from parent directory
    run dolt --host 127.0.0.1 --port $PORT --no-tls --use-db child_db log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Initial commit" ]] || false
}

@test "requires-repo: --host and --use-db flags bypass repo requirement for branch" {
    start_multi_db_server child_db

    run dolt --host 127.0.0.1 --port $PORT --no-tls --use-db child_db branch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "main" ]] || false
}

@test "requires-repo: --host and --use-db flags bypass repo requirement for diff" {
    start_multi_db_server child_db

    run dolt --host 127.0.0.1 --port $PORT --no-tls --use-db child_db diff
    [ "$status" -eq 0 ]
}
