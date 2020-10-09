#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}

@test "dolt gc smoke test" {
    dolt sql <<SQL
CREATE TABLE test (pk int PRIMARY KEY);
INSERT INTO test VALUES
    (1),(2),(3),(4),(5);
SQL
    run dolt sql -q 'select count(*) from test' -r csv
    [ "$status" -eq "0" ]
     [[ "$output" =~ "5" ]] || false

    run dolt gc
    [ "$status" -eq "0" ]
    run dolt status
    [ "$status" -eq "0" ]

    dolt sql <<SQL
CREATE TABLE test2 (pk int PRIMARY KEY);
INSERT INTO test2 VALUES
    (1),(2),(3),(4),(5);
SQL

    run dolt sql -q 'select count(*) from test' -r csv
    [ "$status" -eq "0" ]
     [[ "$output" =~ "5" ]] || false
    run dolt sql -q 'select count(*) from test2' -r csv
    [ "$status" -eq "0" ]
     [[ "$output" =~ "5" ]] || false

    run dolt gc
    [ "$status" -eq "0" ]
    run dolt status
    [ "$status" -eq "0" ]
}