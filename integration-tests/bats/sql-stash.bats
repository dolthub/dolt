#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "sql-stash: push does not affect stash" {
    TESTDIRS=$(pwd)/testdirs
    mkdir -p $TESTDIRS/{rem1,repo1}

    cd $TESTDIRS/repo1
    dolt init
    dolt remote add origin file://../rem1
    dolt remote add test-remote file://../rem1
    dolt push origin main
    dolt sql -q "create table t1 (a int primary key, b int)"
    dolt add .
    dolt sql -q "call dolt_stash('push', 'stash1');"
    dolt push origin main

    cd $TESTDIRS
    dolt clone file://rem1 repo2
    cd repo2
    run dolt sql -q "select * from dolt_stashes"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 0 ]
}