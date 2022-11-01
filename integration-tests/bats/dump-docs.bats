#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "dump-docs: works" {
  run dolt dump-docs
  [ "$status" -eq 0 ]
  [[ ! "$output" =~ "error: Failed to dump docs" ]] || false
}