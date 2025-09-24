#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

basic_conflict() {
    dolt sql -q "create table t (i int primary key, t text)"
    dolt add .
    dolt commit -am "init commit"
    dolt checkout -b other
    dolt sql -q "insert into t values (1,'other')"
    dolt commit -am "other commit"
    dolt checkout main
    dolt sql -q "insert into t values (1,'main')"
    dolt commit -am "main commit"
}

two_conflicts() {
    dolt sql -q "create table t (i int primary key, t text)"
    dolt sql -q "create table t2 (i int primary key, t text)"
    dolt add .
    dolt commit -am "init commit"
    dolt checkout -b other
    dolt sql -q "insert into t values (1,'other')"
    dolt sql -q "insert into t2 values (1,'other2')"
    dolt commit -am "other commit"
    dolt checkout main
    dolt sql -q "insert into t values (1,'main')"
    dolt sql -q "insert into t2 values (1,'main2')"
    dolt commit -am "main commit"
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "conflicts-resolve: call with no arguments, errors" {
    run dolt conflicts resolve
    [ $status -eq 1 ]
    [[ $output =~ "--ours or --theirs must be supplied" ]] || false
}

@test "conflicts-resolve: call without specifying table, errors" {
    run dolt conflicts resolve --theirs
    [ $status -eq 1 ]
    [[ $output =~ "specify at least one table to resolve conflicts" ]] || false
}

@test "conflicts-resolve: call with non-existent table, errors" {
    run dolt conflicts resolve --ours notexists
    [ $status -eq 1 ]
    [[ $output =~ "table not found" ]] || false
}

@test "conflicts-resolve: no conflicts, no changes" {
    basic_conflict

    dolt checkout main
    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ $output =~ "main" ]] || false

    dolt checkout other
    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ $output =~ "other" ]] || false

    dolt checkout main
    run dolt conflicts resolve --ours t
    [ $status -eq 0 ]
    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ $output =~ "main" ]] || false

    run dolt conflicts resolve --theirs t
    [ $status -eq 0 ]
    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ $output =~ "main" ]] || false

    dolt checkout other
    run dolt conflicts resolve --ours t
    [ $status -eq 0 ]
    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ $output =~ "other" ]] || false

    run dolt conflicts resolve --theirs t
    [ $status -eq 0 ]
    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ $output =~ "other" ]] || false
}

@test "conflicts-resolve: merge other into main, resolve with ours" {
    basic_conflict

    dolt checkout main
    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ $output =~ "main" ]] || false

    run dolt merge other
    [ $status -eq 1 ]
    [[ $output =~ "Automatic merge failed" ]] || false

    run dolt conflicts resolve --ours .
    [ $status -eq 0 ]
    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ $output =~ "main" ]] || false
}

@test "conflicts-resolve: merge other into main, resolve with ours, specify table" {
    basic_conflict

    dolt checkout main
    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ $output =~ "main" ]] || false

    run dolt merge other
    [ $status -eq 1 ]
    [[ $output =~ "Automatic merge failed" ]] || false

    run dolt conflicts resolve --ours t
    [ $status -eq 0 ]
    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ $output =~ "main" ]] || false
}

@test "conflicts-resolve: merge other into main, resolve with theirs" {
    basic_conflict

    dolt checkout main
    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ $output =~ "main" ]] || false

    run dolt merge other
    [ $status -eq 1 ]
    [[ $output =~ "Automatic merge failed" ]] || false

    run dolt conflicts resolve --theirs .
    [ $status -eq 0 ]
    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ $output =~ "other" ]] || false
}

@test "conflicts-resolve: merge other into main, resolve with theirs, specify table" {
    basic_conflict

    dolt checkout main
    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ $output =~ "main" ]] || false

    run dolt merge other
    [ $status -eq 1 ]
    [[ $output =~ "Automatic merge failed" ]] || false

    run dolt conflicts resolve --theirs t
    [ $status -eq 0 ]
    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ $output =~ "other" ]] || false
}

@test "conflicts-resolve: two conflicted tables, resolve theirs all" {
    two_conflicts

    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ $output =~ "main" ]] || false

    run dolt sql -q "select * from t2"
    [ $status -eq 0 ]
    [[ $output =~ "main2" ]] || false

    run dolt merge other
    [ $status -eq 1 ]
    [[ $output =~ "Automatic merge failed" ]] || false

    run dolt conflicts resolve --theirs .
    [ $status -eq 0 ]

    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ $output =~ "other" ]] || false
    run dolt sql -q "select * from t2"
    [ $status -eq 0 ]
    [[ $output =~ "other2" ]] || false
}

@test "conflicts-resolve: two conflicted tables, resolve theirs one table, ours other table" {
    two_conflicts

    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ $output =~ "main" ]] || false

    run dolt sql -q "select * from t2"
    [ $status -eq 0 ]
    [[ $output =~ "main2" ]] || false

    run dolt merge other
    [ $status -eq 1 ]
    [[ $output =~ "Automatic merge failed" ]] || false

    run dolt conflicts resolve --theirs t
    [ $status -eq 0 ]
    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ $output =~ "other" ]] || false

    run dolt conflicts resolve --ours t2
    run dolt sql -q "select * from t2"
    [ $status -eq 0 ]
    [[ $output =~ "main2" ]] || false
}

@test "conflicts-resolve: merge main into other, resolve with ours" {
    basic_conflict

    dolt checkout other
    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ $output =~ "other" ]] || false

    run dolt merge main
    [ $status -eq 1 ]
    [[ $output =~ "Automatic merge failed" ]] || false

    run dolt conflicts resolve --ours .
    [ $status -eq 0 ]
    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ $output =~ "other" ]] || false
}

@test "conflicts-resolve: merge main into other, resolve with ours, specify table" {
    basic_conflict

    dolt checkout other
    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ $output =~ "other" ]] || false

    run dolt merge main
    [ $status -eq 1 ]
    [[ $output =~ "Automatic merge failed" ]] || false

    run dolt conflicts resolve --ours t
    [ $status -eq 0 ]
    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ $output =~ "other" ]] || false
}

@test "conflicts-resolve: merge main into other, resolve with theirs" {
    basic_conflict

    dolt checkout other
    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ $output =~ "other" ]] || false

    run dolt merge main
    [ $status -eq 1 ]
    [[ $output =~ "Automatic merge failed" ]] || false

    run dolt conflicts resolve --theirs .
    [ $status -eq 0 ]
    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ $output =~ "main" ]] || false
}

@test "conflicts-resolve: merge main into other, resolve with theirs, specify table" {
    basic_conflict

    dolt checkout other
    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ $output =~ "other" ]] || false

    run dolt merge main
    [ $status -eq 1 ]
    [[ $output =~ "Automatic merge failed" ]] || false

    run dolt conflicts resolve --theirs t
    [ $status -eq 0 ]
    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ $output =~ "main" ]] || false
}
