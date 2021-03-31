#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}

@test "json: JSON hidden behind feature flag" {
    run dolt sql <<SQL
    CREATE TABLE js (
        pk int PRIMARY KEY,
        js json
    );
SQL
    [ $status -ne 0 ]

    run dolt --json sql <<SQL
    CREATE TABLE js (
        pk int PRIMARY KEY,
        js json
    );
SQL
    [ $status -eq 0 ]
}
