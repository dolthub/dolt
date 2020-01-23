#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    dolt table import -c -s `batshelper capital-letter-column-names.json` test `batshelper capital-letter-column-names.csv`
}

teardown() {
    teardown_common
}

@test "capital letter col names. dolt table select with a where clause" {
    run dolt sql -q "select * from test where Aaa = 2"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "BBB" ]] || false
}

@test "capital letter col names. dolt schema show" {
    run dolt schema show
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Aaa" ]] || false
    [[ "$output" =~ "Bbb" ]] || false
    [[ "$output" =~ "Ccc" ]] || false
}

@test "capital letter col names. sql select" {
    run dolt sql -q "select Bbb from test where Aaa=2"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "bbb" ]] || false
    [[ "$output" =~ "Bbb" ]] || false
    [[ ! "$output" =~ "Aaa" ]] || false
    [[ ! "$output" =~ "aaa" ]] || false
}

@test "capital letter col names. dolt table export" {
    run dolt table export test export.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully exported data" ]] || false
    run cat export.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "bbb" ]] || false
    [[ "$output" =~ "Bbb" ]] || false
    [[ "$output" =~ "Aaa" ]] || false
    [[ "$output" =~ "aaa" ]] || false
}

@test "capital letter col names. dolt table copy" {
    run dolt table cp test test2
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt sql -q "select * from test2"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "bbb" ]] || false
    [[ "$output" =~ "Bbb" ]] || false
    [[ "$output" =~ "Aaa" ]] || false
    [[ "$output" =~ "aaa" ]] || false
}
