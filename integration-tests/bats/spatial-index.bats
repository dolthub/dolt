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
    run dolt sql -q "create table t (p point srid 0 not null, spatial index(p))"
    [ "$status" -eq 0 ]
}
