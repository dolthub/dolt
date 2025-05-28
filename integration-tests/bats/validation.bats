#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
     teardown_common
}

# Validation is a set of tests that validate various things about dolt
# that have nothing to do with product functionality directly.

@test "validation: no test symbols in binary" {
    run grep_for_testify
    [ "$output" = "" ]
}

grep_for_testify() {
    strings `type -p dolt` | grep testify
}
