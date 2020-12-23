#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common

    dolt sql <<SQL
CREATE TABLE test (
    pk int primary key
);
SQL
}

teardown() {
    teardown_common
}

@test "status properly works with working and staged tables" {
    run dolt sql -q "select * from dolt_status"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false
    [[ "$output" =~ "false" ]] || false
    [[ "$output" =~ "new table" ]] || false

    dolt add .

    # Confirm table is now marked as staged
    run dolt sql -q "select * from dolt_status"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false
    [[ "$output" =~ "true" ]] || false
    [[ "$output" =~ "new table" ]] || false

}