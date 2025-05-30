#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}

# Tests the basic functionality of the dolt_update_column_tag stored procedure.
#
# Note that we use BATS to test this, since reading column tags is not supported
# via a SQL interface, only from the `dolt schema tags` command currently,
# otherwise we'd prefer enginetests in go.
@test "sql-update-column-tag: update column tag" {
    dolt sql -q "create table t1 (pk int primary key, c1 int);"

    run dolt schema tags
    [ "$status" -eq 0 ]
    [[ "$output" =~ "t1" ]] || false
    [[ "$output" =~ "pk" ]] || false
    [[ "$output" =~ "c1" ]] || false
    [[ ! "$output" =~ " t1    | pk     | 42 " ]] || false
    [[ ! "$output" =~ " t1    | c1     | 420 " ]] || false

    dolt sql -q "call dolt_update_column_tag('t1', 'pk', 42);"
    dolt sql -q "call dolt_update_column_tag('t1', 'c1', 420);"

    run dolt schema tags
    [ "$status" -eq 0 ]
    [[ "$output" =~ " t1    | pk     | 42 " ]] || false
    [[ "$output" =~ " t1    | c1     | 420 " ]] || false
}

# Tests error cases for the dolt_update_column_tag stored procedure.
@test "sql-update-column-tag: error cases" {
    dolt sql -q "create table t1 (pk int primary key, c1 int);"

    # invalid arg count
    run dolt sql -q "call dolt_update_column_tag();"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "incorrect number of arguments" ]] || false

    run dolt sql -q "call dolt_update_column_tag('t1', 'pk');"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "incorrect number of arguments" ]] || false

    run dolt sql -q "call dolt_update_column_tag('t1', 'pk', 42, 'zzz');"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Expected at most 3" ]] || false

    # invalid table
    run dolt sql -q "call dolt_update_column_tag('doesnotexist', 'pk', 42);"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "does not exist" ]] || false

    # invalid column
    run dolt sql -q "call dolt_update_column_tag('t1', 'doesnotexist', 42);"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "does not exist" ]] || false

    # invalid tag
    run dolt sql -q "call dolt_update_column_tag('t1', 'pk', 'not an integer');"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "failed to parse tag" ]] || false
}