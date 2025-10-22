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
    
    cd ../
    start_sql_server
}

teardown() {
    cleanup_idle_connections

    stop_sql_server 1 && sleep 0.5
    teardown_common
}

# Helper function to start an idle dolt sql connection on a specific branch
start_idle_connection() {
    local branch=$1
    [ -n "$branch" ] || fail "Expected non-empty string, got empty"

    # Do nothing connection to keep the branch activ
    dolt --use-db "repo1/$branch" sql -q "SELECT SLEEP(60)" &
    local pid=$!
    
    # Store the PID for cleanup
    echo $pid >> $BATS_TMPDIR/idle_connections_$$
}

# Helper function to cleanup idle connections
cleanup_idle_connections() {
    if [ -f $BATS_TMPDIR/idle_connections_$$ ]; then
        while read pid; do
            kill $pid 2>/dev/null || true
        done < $BATS_TMPDIR/idle_connections_$$
    fi
}

@test "branch-activity: multi-client branch activity tracking" {
    cd repo1

    # Start idle connections on different branches to simulate active clients
    start_idle_connection "main"
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
}