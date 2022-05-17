#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    skip_nbf_dolt_1

    dolt sql <<SQL
CREATE TABLE test (
    pk int primary key
);
SQL

    dolt commit -a -m "Add a table"
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "sql-clean: DOLT_CLEAN clears unstaged tables" {
    # call proc
    dolt sql -q "create table test2 (pk int primary key)"

    run dolt sql -q "call dolt_clean()"
    [ $status -eq 0 ]

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false

    # call dproc
    dolt sql -q "create table test2 (pk int primary key)"
    run dolt sql -q "call dclean('--dry-run')"
    [ $status -eq 0 ]

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "new table:      test2" ]] || false

    dolt sql -q "call dclean('test2')"

    # dolt cli
    dolt sql -q "create table test2 (pk int primary key)"
    dolt sql -q "create table test3 (pk int primary key)"
    dolt clean test3
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "new table:      test2" ]] || false
    [[ ! "$output" =~ "new table:      test3" ]] || false

    # don't touch staged root
    dolt add test2
    dolt clean
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "new table:      test2" ]] || false
}

@test "sql-clean: DOLT_CLEAN unknown table name" {
    dolt sql -q "create table test2 (pk int primary key)"

    run dolt sql -q "call dclean('unknown')"
    [ $status -eq 1 ]
    [[ "$output" =~ "table not found: 'unknown'" ]] || false

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "new table:      test2" ]] || false

    run dolt clean unknown
    [ $status -eq 1 ]
    [[ "$output" =~ "table not found: 'unknown'" ]] || false

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "new table:      test2" ]] || false
}

