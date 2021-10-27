#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
     teardown_common
}

@test "deleted-branches: can checkout existing branch after checked out branch is deleted" {
    dolt branch -c main to_keep
    dolt sql -q 'delete from dolt_branches where name = "main"'
    dolt checkout to_keep
}
