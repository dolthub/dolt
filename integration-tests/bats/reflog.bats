#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

teardown() {
    assert_feature_version
    teardown_common
}

@test "reflog: disabled with DOLT_DISABLE_REFLOG" {
    export DOLT_DISABLE_REFLOG=true
    setup_common
    dolt sql -q "create table t (i int primary key, j int);"
    dolt sql -q "insert into t values (1, 1), (2, 2), (3, 3)";
    dolt commit -Am "initial commit"

    run dolt sql -q "select * from dolt_reflog();"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 0 ]
}

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

@test "reflog: set DOLT_REFLOG_RECORD_LIMIT" {
    export DOLT_REFLOG_RECORD_LIMIT=2
    setup_common
    dolt sql -q "create table t (i int primary key, j int);"
    dolt sql -q "insert into t values (1, 1), (2, 2), (3, 3)";
    dolt commit -Am "initial commit"
    dolt commit --allow-empty -m "test commit"

    run dolt sql -q "select * from dolt_reflog();"
    [ "$status" -eq 0 ]
    [[ "$output"  =~ "exceeded reflog record limit" ]] || false
    [[ "$output"  =~ "Initialize data repository" ]] || false
}