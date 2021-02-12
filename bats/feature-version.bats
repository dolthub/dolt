#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
     rm -rf "$BATS_TMPDIR/dolt-repo-$$"
}

@test "set feature version with CLI flag" {
    dolt --feature-version 19 sql -q "CREATE TABLE test (pk int PRIMARY KEY)"
    run dolt --feature-version 19 version --feature
    [[ "$output" =~ "feature version: 19" ]] || false
}
