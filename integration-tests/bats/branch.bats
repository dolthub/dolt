#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "branch: branch --datasets lists all datasets" {
    dolt branch other
    dolt commit --allow-empty -m "empty"
    dolt tag mytag head
    dolt sql -q "create table t (c0 int)"

    run dolt branch --datasets
    [[ "$status" -eq 0 ]] || false
    [[ "$output" =~ "refs/heads/main" ]] || false
    [[ "$output" =~ "refs/heads/other" ]] || false
    [[ "$output" =~ "refs/internal/create" ]] || false
    [[ "$output" =~ "refs/tags/mytag" ]] || false
    [[ "$output" =~ "workingSets/heads/main" ]] || false
    [[ "$output" =~ "workingSets/heads/other" ]] || false
}

@test "branch: deleting a branch deletes its working set" {
    dolt checkout -b to_delete

    run dolt branch --datasets
    [[ "$output" =~ "workingSets/heads/main" ]] || false
    [[ "$output" =~ "workingSets/heads/to_delete" ]] || false

    dolt checkout main
    dolt branch -d -f to_delete

    run dolt branch --datasets
    [[ "$show_tables" -eq 0 ]] || false
    [[ ! "$output" =~ "to_delete" ]] || false
}

@test "branch: moving current working branch takes its working set" {
    dolt sql -q 'create table test (id int primary key);'
    dolt branch -m main new_main
    run dolt sql -q 'show tables'
    [[ "$output" =~ "test" ]] || false
}
