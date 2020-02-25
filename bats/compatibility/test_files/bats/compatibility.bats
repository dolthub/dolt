#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}

@test "smoke test" {
    run dolt status
    [ "$status" -eq 0 ]

    run dolt branch
    [ "$status" -eq 0 ]
}