#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    TMPDIRS=$(pwd)/tmpdirs
    mkdir -p $TMPDIRS/{bac1,rem1,repo1}

    # repo1 -> bac1 -> repo2
    cd $TMPDIRS/repo1
    dolt init
    dolt branch feature
    dolt remote add backup1 file://../bac1
    dolt remote add remote1 file://../rem1
    dolt push remote1 main
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

@test "replication: no push on cli commit" {

    cd repo1
    dolt config --local --add sqlserver.global.DOLT_REPLICATE_TO_REMOTE backup1
    dolt sql -q "create table t1 (a int primary key)"
    dolt commit -am "cm"

    cd ..
    run dolt clone file://./bac1 repo2
    [ "$status" -eq 1 ]
}

@test "replication: push on cli engine commit" {
    cd repo1
    dolt config --local --add sqlserver.global.DOLT_REPLICATE_TO_REMOTE backup1
    dolt sql -q "create table t1 (a int primary key)"
    dolt sql -q "select dolt_commit('-am', 'cm')"

    cd ..
    dolt clone file://./bac1 repo2
    cd repo2
    run dolt ls
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "t1" ]] || false
}

@test "replication: no tags" {
    cd repo1
    dolt config --local --add sqlserver.global.DOLT_REPLICATE_TO_REMOTE backup1
    dolt tag

    [ ! -d "../bac1/.dolt" ] || false
}

@test "replication: pull on read" {
    dolt clone file://./rem1 repo2
    cd repo2
    dolt sql -q "create table t1 (a int primary key)"
    dolt commit -am "new commit"
    dolt push origin main

    cd ../repo1
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]
    [[ ! "$output" =~ "t1" ]] || false

    dolt config --local --add sqlserver.global.DOLT_READ_REPLICA_REMOTE remote1
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "t1" ]] || false
}

@test "replication: replicate on branch table update" {
    cd repo1
    dolt config --local --add sqlserver.global.DOLT_REPLICATE_TO_REMOTE backup1
    dolt sql -q "create table t1 (a int primary key)"
    dolt sql -q "UPDATE dolt_branches SET hash = COMMIT('--author', '{user_name} <{email_address}>','-m', 'cm') WHERE name = 'main' AND hash = @@repo1_head"

    cd ..
    dolt clone file://./bac1 repo2
    cd repo2
    run dolt ls
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "t1" ]] || false
}
