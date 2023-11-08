#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

teardown() {
    assert_feature_version
    teardown_common
}

# Asserts that when DOLT_DISABLE_REFLOG is set, the dolt_reflog() table
# function returns an empty result set with no error.
@test "reflog: disabled with DOLT_DISABLE_REFLOG" {
    export DOLT_DISABLE_REFLOG=true
    setup_common
    dolt sql -q "create table t (i int primary key, j int);"
    dolt sql -q "insert into t values (1, 1), (2, 2), (3, 3)";
    dolt commit -Am "initial commit"
    dolt commit --allow-empty -m "test commit 1"

    run dolt sql -q "select * from dolt_reflog();"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 0 ]
}

# Sanity check for the most basic case of querying the Dolt reflog
@test "reflog: enabled by default" {
    setup_common
    dolt sql -q "create table t (i int primary key, j int);"
    dolt sql -q "insert into t values (1, 1), (2, 2), (3, 3)";
    dolt commit -Am "initial commit"

    run dolt sql -q "select * from dolt_reflog();"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]
    [[ "$output"  =~ "initial commit" ]] || false
    [[ "$output"  =~ "Initialize data repository" ]] || false
}

# Asserts that when DOLT_REFLOG_RECORD_LIMIT has been set, the reflog only contains the
# most recent entries and is limited by the env var's value.
@test "reflog: set DOLT_REFLOG_RECORD_LIMIT" {
    export DOLT_REFLOG_RECORD_LIMIT=2
    setup_common
    dolt sql -q "create table t (i int primary key, j int);"
    dolt sql -q "insert into t values (1, 1), (2, 2), (3, 3)";
    dolt commit -Am "initial commit"
    dolt commit --allow-empty -m "test commit 1"
    dolt commit --allow-empty -m "test commit 2"

    # Only the most recent two ref changes should appear in the log
    run dolt sql -q "select * from dolt_reflog();"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test commit 1" ]] || false
    [[ "$output" =~ "test commit 2" ]] || false
    [[ ! "$output" =~ "initial commit" ]] || false
    [[ ! "$output" =~ "Initialize data repository" ]] || false
}
