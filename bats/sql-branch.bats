#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common

    dolt sql <<SQL
CREATE TABLE test (
    pk int primary key
);
CREATE TABLE test2 (
    pk int primary key
);
INSERT INTO test VALUES (0),(1),(2);
SQL
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "active_branch() func" {
    run dolt sql -q 'select active_branch()' -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ "active_branch()" ]] || false
    [[ "$output" =~ "master" ]] || false
}

@test "active_branch() func on feature branch" {
    run dolt branch tmp_br
    run dolt checkout tmp_br
    run dolt sql -q 'select active_branch()' -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ "active_branch()" ]] || false
    [[ "$output" =~ "tmp_br" ]] || false
}
