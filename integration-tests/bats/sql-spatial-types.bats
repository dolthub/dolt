#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "sql-spatial-types: can't make spatial types without enabling feature flag" {
    run dolt sql -q "create table point_tbl (p point)"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "cannot be made" ]] || false
}

@test "sql-spatial-types: can make spatial types with flag" {
    DOLT_ENABLE_SPATIAL_TYPES=true run dolt sql -q "create table point_tbl (p point)"
    [ "$status" -eq 0 ]
    [[ "$output" = "" ]] || false
}