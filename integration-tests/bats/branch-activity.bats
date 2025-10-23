#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash
load $BATS_TEST_DIRNAME/helper/query-server-common.bash

setup() {
    setup_no_dolt_init
    
    mkdir repo1
    cd repo1
    dolt init
    dolt sql -q "CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR(50));"
    dolt sql -q "INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob');"
    dolt commit -Am "Initial commit"
    dolt branch feature1
    dolt branch feature2
    dolt branch feature3
    
    cd ../
    start_sql_server
}

teardown() {
    stop_sql_server 1
    teardown_common
}

# Helper function to start an idle dolt sql connection on a specific branch
start_idle_connection() {
    local branch=$1
    [ -n "$branch" ] || fail "Expected non-empty string, got empty"

    # Do nothing connection. These will be killed by the server shutting down.
    dolt --use-db "repo1/$branch" sql -q "SELECT SLEEP(60)" &
    
}

@test "branch-activity: last_read set for connections" {
    cd repo1

    # Start idle connections on different branches to simulate active clients. Don't include main, as it is used
    # by the query of the table and should be included there.
    start_idle_connection "feature1"
    start_idle_connection "feature2"

    # Wait a moment for connections to establish
    sleep 1
    
    # Now test that branch activity table shows the activity
    run dolt sql -q "SELECT branch FROM dolt_branch_activity where last_read IS NOT NULL"
    [ $status -eq 0 ]
    [[ "$output" =~ "main" ]] || false
    [[ "$output" =~ "feature1" ]] || false
    [[ "$output" =~ "feature2" ]] || false
    [[ ! "$output" =~ "feature3" ]] || false
}


@test "branch-activity: active session counts" {
    cd repo1

    # Start idle connections on different branches to simulate active clients
    start_idle_connection "main"
    start_idle_connection "feature1"
    start_idle_connection "feature2"
    start_idle_connection "feature2"
    start_idle_connection "feature2" # 3 active sessions on this branch.

    # Wait a moment for connections to establish
    sleep 1

    run dolt sql -r csv -q "SELECT branch,active_sessions FROM dolt_branch_activity where last_read IS NOT NULL"
    [ $status -eq 0 ]
    [[ "$output" =~ "main,2" ]] || false     # main has 1 idle + 1 from this query
    [[ "$output" =~ "feature1,1" ]] || false
    [[ "$output" =~ "feature2,3" ]] || false
}

@test "branch-activity: empty commit updates last_write" {
    cd repo1

    # verify no last_write initially
    run dolt sql -q "SELECT branch FROM dolt_branch_activity where last_write IS NOT NULL"
    [[ ! "$output" =~ "main" ]] || false

    dolt commit -m "empty commit" --allow-empty

    run dolt sql -q "SELECT branch FROM dolt_branch_activity where last_write IS NOT NULL"
    [[ "$output" =~ "main" ]] || false
}

@test "branch-activity: data-changing commit updates last_write" {
    cd repo1

    # verify no last_write initially
    run dolt sql -q "SELECT branch FROM dolt_branch_activity where last_write IS NOT NULL"
    [[ ! "$output" =~ "main" ]] || false

    dolt sql -q "INSERT INTO test VALUES (3, 'Charlie');"

    run dolt sql -q "SELECT branch FROM dolt_branch_activity where last_write IS NOT NULL"
    [[ "$output" =~ "main" ]] || false
}
