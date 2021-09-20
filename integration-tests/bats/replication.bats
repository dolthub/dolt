#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    TMPDIRS=$(pwd)/tmpdirs
    mkdir -p $TMPDIRS/{bac1,repo1}

    # repo1 -> rem1 -> repo2
    cd $TMPDIRS/repo1
    dolt init
    dolt branch feature
    cd $TMPDIRS
}

teardown() {
    teardown_common
    rm -rf $TMPDIRS
    cd $BATS_TMPDIR
}

@test "replication: default no replication" {
    cd repo1
    dolt sql -q "create table t1 (a int primary key)"
    dolt commit -am "cm"

    [ ! -d "../bac1/.dolt" ] || false
}

@test "replication: push on commit" {
    DOLT_BACKUP_REMOTE=file://../bac1
    cd repo1
    dolt sql -q "create table t1 (a int primary key)"
    dolt commit -am "cm"

    run noms ds ../bac1
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]
    [[ "$output" =~ "refs/heads/master" ]] || false
}

@test "replication: no tags" {
    DOLT_BACKUP_REMOTE=file://../bac1
    cd repo1
    dolt tag

    [ ! -d "../bac1/.dolt" ] || false
}

