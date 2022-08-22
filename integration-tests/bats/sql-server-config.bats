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
    make_repo repo1
    make_repo repo2
}

teardown() {
    stop_sql_server
    teardown_common
}

@test "sql-server-config: persist global variable before server startup" {
    cd repo1
    echo '{"sqlserver.global.max_connections":"1000"}' > .dolt/config.json
    start_sql_server repo1

    server_query repo1 1 dolt "" "select @@GLOBAL.max_connections" "@@GLOBAL.max_connections\n1000"
}

@test "sql-server-config: invalid persisted global variable name throws warning on server startup, but does not crash" {
    cd repo1
    echo '{"sqlserver.global.unknown":"1000"}' > .dolt/config.json
    start_sql_server repo1
}

@test "sql-server-config: invalid persisted global variable value throws warning on server startup, but does not crash" {
    cd repo1
    echo '{"server.max_connections":"string"}' > .dolt/config.json
    start_sql_server repo1
}

@test "sql-server-config: persisted global variable in server" {
    cd repo1
    start_sql_server repo1

    server_query repo1 1 dolt "" "SET @@PERSIST.max_connections = 1000"
    server_query repo1 1 dolt "" "select @@GLOBAL.max_connections" "@@GLOBAL.max_connections\n1000"

    run dolt config --local --list
    [ "$status" -eq 0 ]
    [[ "$output" =~ "sqlserver.global.max_connections = 1000" ]] || false
}

@test "sql-server-config: dolt_replicate_heads is global variable" {
    cd repo1
    start_sql_server repo1

    server_query repo1 1 dolt "" "SET @@GLOBAL.dolt_replicate_heads = main"
    server_query repo1 1 dolt "" "select @@GLOBAL.dolt_replicate_heads" "@@GLOBAL.dolt_replicate_heads\nmain"
    server_query repo1 1 dolt "" "select @@SESSION.dolt_replicate_heads" "@@SESSION.dolt_replicate_heads\nmain"
    server_query repo1 1 dolt "" "select @@dolt_replicate_heads" "@@SESSION.dolt_replicate_heads\nmain"
}

@test "sql-server-config: dolt_replicate_all_heads is global variable" {
    cd repo1
    start_sql_server repo1

    server_query repo1 1 dolt "" "SET @@GLOBAL.dolt_replicate_all_heads = 1"
    server_query repo1 1 dolt "" "select @@GLOBAL.dolt_replicate_all_heads" "@@GLOBAL.dolt_replicate_all_heads\n1"
    server_query repo1 1 dolt "" "select @@SESSION.dolt_replicate_all_heads" "@@SESSION.dolt_replicate_all_heads\n1"
    server_query repo1 1 dolt "" "select @@dolt_replicate_all_heads" "@@SESSION.dolt_replicate_all_heads\n1"
}

@test "sql-server-config: dolt_transaction_commit is global variable" {
    cd repo1
    start_sql_server repo1

    server_query repo1 1 dolt "" "SET @@GLOBAL.dolt_transaction_commit = 1"
    server_query repo1 1 dolt "" "select @@GLOBAL.dolt_transaction_commit" "@@GLOBAL.dolt_transaction_commit\n1"
    server_query repo1 1dolt ""  "select @@SESSION.dolt_transaction_commit" "@@SESSION.dolt_transaction_commit\n1"
    server_query repo1 1 dolt "" "select @@dolt_transaction_commit" "@@SESSION.dolt_transaction_commit\n1"

    # only 1 commit
    commits=$(dolt log --oneline | wc -l)
    [ $commits -eq 1 ]

    # create a table
    server_query repo1 1 dolt "" "create table tmp (i int)"

    # now there are two commits
    commits=$(dolt log --oneline | wc -l)
    [ $commits -eq 2 ]
}

@test "sql-server-config: persist only global variable during server session" {
    cd repo1
    start_sql_server repo1

    server_query repo1 1 dolt "" "SET PERSIST max_connections = 1000"
    server_query repo1 1 dolt "" "SET PERSIST_ONLY max_connections = 7777"
    server_query repo1 1 dolt "" "select @@GLOBAL.max_connections" "@@GLOBAL.max_connections\n1000"

    run dolt config --local --list
    [ "$status" -eq 0 ]
    [[ "$output" =~ "sqlserver.global.max_connections = 7777" ]] || false
}

@test "sql-server-config: persist invalid global variable name during server session" {
    cd repo1
    start_sql_server repo1
    run server_query repo1 1 dolt "" "SET @@PERSIST.unknown = 1000" "" 1
    [[ "$output" =~ "Unknown system variable" ]] || false
}

@test "sql-server-config: persist invalid global variable value during server session" {
    cd repo1
    start_sql_server repo1
    run server_query repo1 1 dolt "" "SET @@PERSIST.max_connections = 'string'" "" 1
    [[ "$output" =~ "Variable 'max_connections' can't be set to the value of 'string'" ]] || false
}

@test "sql-server-config: reset persisted variable" {
    skip "TODO: parser support for RESET PERSIST"
    cd repo1
    start_sql_server repo1

    server_query repo1 1 dolt "" "SET @@PERSIST.max_connections = 1000"
    server_query repo1 1 dolt "" "RESET @@PERSIST.max_connections"

    run dolt config --local --list
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "sqlserver.global.max_connections = 1000" ]] || false
}

@test "sql-server-config: reset all persisted variables" {
    skip "TODO: parser support for RESET PERSIST"
    cd repo1
    start_sql_server repo1

    server_query repo1 1 dolt "" "SET @@PERSIST.max_connections = 1000"
    server_query repo1 1 dolt "" "SET @@PERSIST.auto_increment_increment = 1000"
    server_query repo1 1 dolt "" "RESET PERSIST"

    run dolt config --local --list
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "sqlserver.global.max_connections = 1000" ]] || false
    [[ ! "$output" =~ "sqlserver.global.auto_increment_increment = 1000" ]] || false
}

@test "sql-server-config: set max_connections with yaml config" {
    cd repo1
    DEFAULT_DB="repo1"
    let PORT="$$ % (65536-1024) + 1024"
    echo "
log_level: debug

user:
  name: dolt

listener:
  host: 0.0.0.0
  port: $PORT
  max_connections: 999

behavior:
  read_only: false
  autocommit: true" > server.yaml
    dolt sql-server --config server.yaml &
    SERVER_PID=$!
    wait_for_connection $PORT 5000

    server_query repo1 1 dolt "" "select @@GLOBAL.max_connections" "@@GLOBAL.max_connections\n999"
}

@test "sql-server-config: set max_connections with yaml config with persistence ignore" {
    cd repo1
    DEFAULT_DB="repo1"
    let PORT="$$ % (65536-1024) + 1024"
    echo "
log_level: debug

user:
  name: dolt

listener:
  host: 0.0.0.0
  port: $PORT
  max_connections: 999

behavior:
  read_only: false
  autocommit: true
  persistence_behavior: ignore" > server.yaml

    dolt sql-server --config server.yaml --max-connections 333 &
    SERVER_PID=$!
    wait_for_connection $PORT 5000

    server_query repo1 1 dolt "" "select @@GLOBAL.max_connections" "@@GLOBAL.max_connections\n999"
}

@test "sql-server-config: persistence behavior set to load" {
    cd repo1
    start_sql_server_with_args --host 0.0.0.0 --user dolt --persistence-behavior load repo1

    server_query repo1 1 dolt "" "select @@GLOBAL.max_connections" "@@GLOBAL.max_connections\n151"
}

@test "sql-server-config: persistence behavior set to ignore" {
    cd repo1
    start_sql_server_with_args --host 0.0.0.0 --user dolt --persistence-behavior ignore repo1

    server_query repo1 1 dolt "" "select @@GLOBAL.max_connections" "@@GLOBAL.max_connections\n100"
}

@test "sql-server-config: persisted global variable defined on the command line" {
    cd repo1
    start_sql_server_with_args --host 0.0.0.0 --user dolt --max-connections 555 repo1

    server_query repo1 1 dolt "" "select @@GLOBAL.max_connections" "@@GLOBAL.max_connections\n555"
}

@test "sql-server-config: persist global variable before server startup with persistence behavior with ignore" {
    cd repo1
    echo '{"sqlserver.global.max_connections":"1000"}' > .dolt/config.json
    start_sql_server_with_args --host 0.0.0.0 --user dolt --persistence-behavior ignore repo1

    server_query repo1 1 dolt "" "select @@GLOBAL.max_connections" "@@GLOBAL.max_connections\n100"
}

@test "sql-server-config: persisted global variable defined on the command line with persistence ignored" {
    cd repo1
    start_sql_server_with_args --host 0.0.0.0 --user dolt --max-connections 555 --persistence-behavior ignore repo1

    server_query repo1 1 dolt "" "select @@GLOBAL.max_connections" "@@GLOBAL.max_connections\n555"
}
