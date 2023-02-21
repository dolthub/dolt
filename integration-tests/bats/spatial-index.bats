#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "spatial-index: spatial indexes disabled" {
    skip_nbf_not_dolt
    run dolt sql -q "create table t (p point srid 0 not null, spatial index(p))"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "only the following types of index constraints are supported" ]] || false
}

@test "spatial-index: spatial indexes enabled" {
    skip_nbf_not_dolt
    DOLT_ENABLE_SPATIAL_INDEX=1 run dolt sql -q "create table t (p point srid 0 not null, spatial index(p))"
    [ "$status" -eq 0 ]
}

@test "spatial-index: not supported in old format" {
    rm -rf .dolt
    dolt init --old-format
    DOLT_ENABLE_SPATIAL_INDEX=1 run dolt sql -q "create table t (p point srid 0 not null, spatial index(p))"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "spatial indexes are only supported in storage format" ]] || false
}