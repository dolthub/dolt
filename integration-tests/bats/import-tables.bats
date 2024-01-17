#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "import-tables: error if no operation is provided" {
    run dolt table import t test.csv

    [ "$status" -eq 1 ]
    [[ "$output" =~ "Must specify exactly one of -c, -u, -a, or -r." ]] || false
}

@test "import-tables: error if multiple operations are provided" {
    run dolt table import -c -u -r t test.csv
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Must specify exactly one of -c, -u, -a, or -r." ]] || false
}