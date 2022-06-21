#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    mkdir $BATS_TMPDIR/dolt-repo-$$
    cd $BATS_TMPDIR/dolt-repo-$$
    setup_common
}

teardown() {
    teardown_common
    rm -rf "$BATS_TMPDIR/config-test$$"
}

@test "sql-config: query persisted variable with cli engine" {
    echo '{"sqlserver.global.max_connections":"1000"}' > .dolt/config.json
    run dolt sql -q "SELECT @@GLOBAL.max_connections" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "${lines[0]}" =~ "@@GLOBAL.max_connections" ]] || false
    [[ "${lines[1]}" =~ "1000" ]] || false
}

@test "sql-config: set persist global variable with cli engine" {
    dolt sql -q "SET PERSIST max_connections = 1000"
    run dolt config --local --list
    [ "$status" -eq 0 ]
    [[ "$output" =~ "sqlserver.global.max_connections = 1000" ]] || false
}

@test "sql-config: set persist multiple global variables with cli engine" {
    dolt sql -q "SET PERSIST max_connections = 1000"
    dolt sql -q "SET PERSIST auto_increment_increment = 2"
    run dolt config --local --list
    [ "$status" -eq 0 ]
    [[ "$output" =~ "sqlserver.global.auto_increment_increment = 2" ]] || false
    [[ "$output" =~ "sqlserver.global.max_connections = 1000" ]] || false
}

@test "sql-config: persist only global variable with cli engine" {
    dolt sql -q "SET PERSIST_ONLY max_connections = 1000"
    run dolt config --local --list
    [ "$status" -eq 0 ]
    [[ "$output" =~ "sqlserver.global.max_connections = 1000" ]] || false
}

@test "sql-config: remove persisted variable with cli engine" {
    skip "TODO parser support for RESET PERSIST"

    dolt sql -q "SET PERSIST_ONLY max_connections = 1000"
    dolt sql -q "RESET PERSIST max_connections"

    run dolt config --local --list
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "sqlserver.global.max_connections = 1000" ]] || false
}

@test "sql-config: remove all persisted variables with cli engine" {
    skip "TODO parser support for RESET PERSIST"

    dolt sql -q "SET PERSIST_ONLY max_connections = 1000"
    dolt sql -q "SET PERSIST_ONLY auto_increment_increment = 2"
    dolt sql -q "RESET PERSIST"

    run dolt config --local --list
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "sqlserver.global.max_connections = 1000" ]] || false
    [[ ! "$output" =~ "sqlserver.global.auto_increment_increment = 2" ]] || false
}

@test "sql-config: persist dolt specific global variable" {
    mkdir repo1
    cd repo1
    dolt init
    dolt sql -q "SET PERSIST_ONLY repo1_head = 1000"
    run dolt config --local --list
    [ "$status" -eq 0 ]
    [[ "$output" =~ "sqlserver.global.repo1_head = 1000" ]] || false
}

@test "sql-config: persist invalid variable name" {
    run dolt sql -q "SET PERSIST unknown = 1000"
    [ "$status" -eq 1 ]
    [[ ! "$output" =~ "panic" ]] || false
    [[ "$output" =~ "Unknown system variable 'unknown'" ]] || false
}

@test "sql-config: persist invalid variable type" {
    run dolt sql -q "SET PERSIST max_connections = string"
    [ "$status" -eq 1 ]
    [[ ! "$output" =~ "panic" ]] || false
    [[ "$output" =~ "Variable 'max_connections' can't be set to the value of 'string'" ]] || false
}

@test "sql-config: invalid persisted system variable name errors on cli sql command" {
    echo '{"sqlserver.global.unknown":"1000"}' > .dolt/config.json
    run dolt sql -q "SELECT @@GLOBAL.unknown" -r csv
    [ "$status" -eq 1 ]
    [[ ! "$output" =~ "panic" ]]
    [[ "$output" =~ "Unknown system variable 'unknown'" ]] || false
}

@test "sql-config: invalid persisted system variable type errors on cli sql command" {
    echo '{"sqlserver.global.max_connections":"string"}' > .dolt/config.json
    run dolt sql -q "SELECT @@GLOBAL.max_connections" -r csv
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "panic" ]]
    [[ "$output" =~ "failed to load persisted global variables: key: 'max_connections'" ]] || false
    [[ "$output" =~ "invalid syntax" ]] || false
    [[ "$output" =~ "151" ]]
}
