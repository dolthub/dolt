#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}

@test "dolt gc smoke test" {
    run dolt gc
    [ "$status" -eq "0" ]

    dolt sql <<SQL
CREATE TABLE test (pk int PRIMARY KEY);
INSERT INTO test VALUES (1),(2),(3),(4),(5);
SQL

    run dolt gc
    [ "$status" -eq "0" ]
}