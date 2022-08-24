#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash
load $BATS_TEST_DIRNAME/helper/query-server-common.bash

make_repo() {
  mkdir "$1"
  cd "$1"
  dolt init
  cd ..
}

setup() {
    skiponwindows "tests are flaky on Windows"
    setup_no_dolt_init
    mkdir $BATS_TMPDIR/sql-server-test$$
    nativevar DOLT_ROOT_PATH $BATS_TMPDIR/sql-server-test$$ /p
    dolt config --global --add user.email "test@test.com"
    dolt config --global --add user.name "test"
    make_repo repo1
    make_repo repo2
}

teardown() {
    stop_sql_server
    teardown_common
}

@test "sql-server-sigterm: one" {
    start_sql_server
    run ls repo1/.dolt
    [[ "$output" =~ "sql-server.lock" ]] || false
    run ls repo2/.dolt
    [[ "$output" =~ "sql-server.lock" ]] || false

    kill -9 $SERVER_PID

    run ls repo1/.dolt
    [[ "$output" =~ "sql-server.lock" ]] || false
    run ls repo2/.dolt
    [[ "$output" =~ "sql-server.lock" ]] || false
}

@test "sql-server-sigterm: two" {
    start_sql_server
    server_query repo1 1 dolt "" "SELECT 1" "1\n1"
    stop_sql_server
}

@test "sql-server-sigterm: three" {
    # Try adding fake pid numbers. Could happen via debugger or something
    echo "423423" > repo1/.dolt/sql-server.lock
    echo "4123423" > repo2/.dolt/sql-server.lock

    start_sql_server
    server_query repo1 1 dolt "" "SELECT 1" "1\n1"
    stop_sql_server
}

@test "sql-server-sigterm: four" {
    # Add malicious text to lockfile and expect to fail
    echo "iamamaliciousactor" > repo1/.dolt/sql-server.lock

    run start_sql_server
    [[ "$output" =~ "database locked by another sql-server; either clone the database to run a second server" ]] || false
    [ "$status" -eq 1 ]
}
