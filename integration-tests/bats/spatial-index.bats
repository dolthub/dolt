#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "spatial-index: spatial indexes enabled" {
    skip_nbf_not_dolt
    run dolt sql -q "create table t (p point srid 0 not null, spatial index(p))"
    [ "$status" -eq 0 ]
}

@test "spatial-index: not supported in old format" {
    rm -rf .dolt
    dolt init --old-format
    run dolt sql -q "create table t (p point srid 0 not null, spatial index(p))"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "spatial indexes are only supported in storage format" ]] || false
}