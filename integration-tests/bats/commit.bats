#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "commit: -ALL (-A) adds all tables including new ones to the staged set." {
    dolt sql -q "CREATE table t (pk int primary key);"
    dolt sql -q "INSERT INTO t VALUES (1);"
    dolt add t
    dolt commit -m "add table t"
    dolt reset --hard
    run dolt sql -q "SELECT * from t;"
    [ $status -eq 0 ]
    [[ "$output" =~ "| 1" ]] || false

    dolt sql -q "DELETE from t where pk = 1;"
    dolt commit -ALL -m "update table t"
    dolt reset --hard
    run dolt sql -q "SELECT * from t;"
    [ $status -eq 0 ]
    [[ ! "$output" =~ "| 1" ]] || false

    dolt sql -q "DROP TABLE t;"
    dolt commit -Am "drop table t;"
    dolt reset --hard
    run dolt ls
    [ $status -eq 0 ]
    [[ "$output" =~ "No tables in working set" ]] || false

    dolt sql -q "CREATE table t2 (pk int primary key);"
    dolt commit -Am "add table t2"
    dolt reset --hard
    run dolt ls
    [ $status -eq 0 ]
    [[ "$output" =~ "t2" ]] || false
}

@test "commit: failed to open commit editor." {
    export EDITOR="foo"
    export DOLT_TEST_FORCE_OPEN_EDITOR="1"
    dolt sql -q "CREATE table t (pk int primary key);"
    dolt sql -q "INSERT INTO t VALUES (1);"
    dolt add t
    run dolt commit
    [ $status -eq 1 ]
    [[ "$output" =~ "Failed to open commit editor" ]] || false
}