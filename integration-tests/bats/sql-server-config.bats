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
    setup_no_dolt_init
    make_repo repo1
    make_repo repo2
}

teardown() {
    stop_sql_server
    teardown_common
}

@test "sql-server: persist global variable before server startup" {
    cd repo1
    echo '{"server.max_connections":"1000"}' > .dolt/config.json
    start_sql_server repo1

    server_query repo1 1 "select @@GLOBAL.max_connections" "@@GLOBAL.max_connections\n1000"

}

@test "sql-server: persist invalid global variable name before server startup" {
    cd repo1
    echo '{"server.unknown":"1000"}' > .dolt/config.json
    run start_sql_server repo1
    [ "$status" -eq 1 ]
    [[ ! "$output" =~ "panic" ]]
    [[ "$output" =~ "Unknown system variable 'unknown'" ]]
}

@test "sql-server: persist invalid global variable value before server startup" {
    cd repo1
    echo '{"server.max_connections":"string"}' > .dolt/config.json
    run start_sql_server repo1
    [ "$status" -eq 1 ]
    [[ ! "$output" =~ "panic" ]]
    [[ "$output" =~ "strconv.ParseInt: parsing \"string\": invalid syntax" ]]
}

@test "sql-server: persist global variable during server session" {
    cd repo1
    start_sql_server repo1

    insert_query repo1 1 "SET @@PERSIST.max_connections = 1000"
    server_query repo1 1 "select @@GLOBAL.max_connections" "@@GLOBAL.max_connections\n1000"

    run cat .dolt/config.json
    [ "$status" -eq 0 ]
    [[ "$output" =~ "\"server.max_connections\":\"1000\"" ]]
}

@test "sql-server: persist only global variable during server session" {
    cd repo1
    start_sql_server repo1

    insert_query repo1 1 "SET PERSIST max_connections = 1000"
    insert_query repo1 1 "SET PERSIST_ONLY max_connections = 7777"
    server_query repo1 1 "select @@GLOBAL.max_connections" "@@GLOBAL.max_connections\n1000"

    run cat .dolt/config.json
    [ "$status" -eq 0 ]
    [[ "$output" =~ "\"server.max_connections\":\"7777\"" ]]
}

@test "sql-server: persist invalid global variable name during server session" {
    cd repo1
    start_sql_server repo1
    run insert_query repo1 1 "SET @@PERSIST.unknown = 1000"
    [ "$status" -eq 1 ]
    [[ ! "$output" =~ "panic" ]]
    [[ "$output" =~ "Unknown system variable 'unknown'" ]]
}

@test "sql-server: persist invalid global variable value during server session" {
    cd repo1
    start_sql_server repo1
    run insert_query repo1 1 "SET @@PERSIST.max_connections = 'string'"
    [ "$status" -eq 1 ]
    [[ ! "$output" =~ "panic" ]]
    [[ "$output" =~ "Variable 'max_connections' can't be set to the value of 'string'" ]]
}

@test "sql-server: reset persisted variable" {
    skip "TODO: parser support for RESET PERSIST"
    cd repo1
    start_sql_server repo1

    insert_query repo1 1 "SET @@PERSIST.max_connections = 1000"
    insert_query repo1 1 "RESET @@PERSIST.max_connections"

    run cat .dolt/config.json
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "\"server.max_connections\":\"1000\"" ]]
}

@test "sql-server: reset all persisted variables" {
    skip "TODO: parser support for RESET PERSIST"
    cd repo1
    start_sql_server repo1

    insert_query repo1 1 "SET @@PERSIST.max_connections = 1000"
    insert_query repo1 1 "SET @@PERSIST.auto_increment_increment = 1000"
    insert_query repo1 1 "RESET PERSIST"

    run cat .dolt/config.json
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "\"server.max_connections\":\"1000\"" ]]
}
