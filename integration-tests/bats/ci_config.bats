#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "ci: ci config tables exist on initialized database" {
    run dolt sql -q "select * from dolt_ci_workflows;"
    [ "$status" -eq 0 ]
}
