#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "spatial indexes disabled" {
    run dolt sql -q "create table t (p point srid 0 not null, spatial index(p))"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "only the following types of index constraints are supported" ]] || false
}

@test "spatial indexes enabled" {
    DOLT_ENABLE_SPATIAL_INDEX=1 run dolt sql -q "create table t (p point srid 0 not null, spatial index(p))"
    [ "$status" -eq 0 ]
}