#! /usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}

get_staged_tables() {
    dolt status | awk '
        match($0, /new table:\ */) { print substr($0, RSTART+RLENGTH) }
        /Untracked tables:/ { exit }
        /Tables with conflicting dolt_ignore patterns:/ { exit }
    '
}

get_working_tables() {
    dolt status | awk '
        BEGIN { working = 0 }
        (working == 1) && match($0, /new table:\ */) { print substr($0, RSTART+RLENGTH) }
        /Untracked tables:/ { working = 1 }
        /Tables with conflicting dolt_ignore patterns:/ { working = 0 }
    '
}

@test "add: add dot" {

    dolt sql -q "create table testtable (pk int PRIMARY KEY)"

    staged=$(get_staged_tables)
    working=$(get_working_tables)

    [[ ! -z $(echo "$working" | grep "testtable") ]] || false
    [[ -z $(echo "$staged" | grep "testtable") ]] || false

    dolt add .

    staged=$(get_staged_tables)
    working=$(get_working_tables)

    [[ ! -z $(echo "$staged" | grep "testtable") ]] || false
    [[ -z $(echo "$working" | grep "testtable") ]] || false
}

@test "add: add by name." {

    dolt sql -q "create table addedTable (pk int PRIMARY KEY)"
    dolt sql -q "create table notAddedTable (pk int PRIMARY KEY)"

    staged=$(get_staged_tables)
    working=$(get_working_tables)

    [[ ! -z $(echo "$working" | grep "addedTable") ]] || false
    [[ -z $(echo "$staged" | grep "addedTable") ]] || false
    [[ ! -z $(echo "$working" | grep "notAddedTable") ]] || false
    [[ -z $(echo "$staged" | grep "notAddedTable") ]] || false

    dolt add addedTable

    staged=$(get_staged_tables)
    working=$(get_working_tables)

    [[ ! -z $(echo "$staged" | grep "addedTable") ]] || false
    [[ -z $(echo "$working" | grep "addedTable") ]] || false
    [[ ! -z $(echo "$working" | grep "notAddedTable") ]] || false
    [[ -z $(echo "$staged" | grep "notAddedTable") ]] || false
}