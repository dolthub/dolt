#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common

    TMPDIRS=$(pwd)/tmpdirs
    mkdir -p $TMPDIRS/{rem1,repo1}

    # repo1 -> rem1 -> repo2
    cd $TMPDIRS/repo1
    dolt init
    dolt remote add origin file://../rem1
    dolt remote add test-remote file://../rem1
    dolt push origin main

    cd $TMPDIRS
    dolt clone file://rem1 repo2
    cd $TMPDIRS/repo2
    dolt log
    dolt remote add test-remote file://../rem1

    # table and comits only present on repo1, rem1 at start
    cd $TMPDIRS/repo1
    dolt sql -q "create table t1 (a int primary key, b int)"
    dolt add .
    dolt commit -am "First commit"
    dolt sql -q "insert into t1 values (0,0)"
    dolt commit -am "Second commit"
    cd $TMPDIRS
}

teardown() {
    teardown_common
    rm -rf $TMPDIRS
    cd $BATS_TMPDIR
}

@test "sql-push: dolt_push --force flag" {
    cd repo2
    dolt sql -q "create table t2 (a int)"
    dolt add .
    dolt commit -am "commit to override"
    dolt push origin main

    cd ../repo1
    run dolt sql -q "CALL dolt_push('origin', 'main')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Updates were rejected because the tip of your current branch is behind" ]] || false

    dolt sql -q "CALL dolt_push('--force', 'origin', 'main')"
}

@test "sql-push: push to unknown remote on CALL" {
    cd repo1
    run dolt sql -q "CALL dolt_push('unknown', 'main')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "fatal: 'unknown' does not appear to be a dolt repository" ]] || false
}

@test "sql-push: push unknown branch on CALL" {
    cd repo1
    run dolt sql -q "CALL dolt_push('origin', 'unknown')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "refspec not found: 'unknown'" ]] || false
}

@test "sql-push: not specifying a branch throws an error on CALL" {
    cd repo1
    run dolt sql -q "CALL dolt_push('-u', 'origin')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "fatal: The current branch main has no upstream branch." ]] || false
}

@test "sql-push: pushing empty branch does not panic on CALL" {
    cd repo1
    run dolt sql -q "CALL dolt_push('origin', '')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "invalid ref spec: ''" ]] || false
}

@test "sql-push: up to date push returns message" {
    cd repo1
    dolt sql -q "call dolt_push('origin', 'main')"
    run dolt sql -q "call dolt_push('origin', 'main')"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Everything up-to-date" ]] || false
}
