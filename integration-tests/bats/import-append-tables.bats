#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "import-append-tables: " {
    dolt sql -q "CREATE TABLE t (pk int primary key, col1 int);"
    dolt import -a <<CSV
pk, col1
1, 1
CSV
    run dolt import -a <<CSV
pk, col1
1, 2
CSV

    [ "$status" -eq 1 ]
    [[ "$output" =~ "An error occurred while moving data" ]] || false
    # [[ "$output" =~ "cause: error: row [1,1] would be overwritten by [1,2]" ]] || false
}
