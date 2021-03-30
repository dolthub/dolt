#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

# Used for misc. tests that don't really fix anywhere else.

setup() {
   setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "misc: dolt reset --hard with more than one additional arg throws an error " {
    run dolt reset --hard HEAD HEAD2
    [ "$status" -eq 1 ]
}