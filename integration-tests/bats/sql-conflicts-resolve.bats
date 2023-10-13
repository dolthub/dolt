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

teardown() {
    assert_feature_version
    teardown_common
}

@test "sql-conflicts-resolve: call with no arguments, errors" {
    run dolt sql -q "call dolt_conflicts_resolve()"
    [ $status -eq 1 ]
    [[ $output =~ "--ours or --theirs must be supplied" ]] || false
}

@test "sql-conflicts-resolve: call without specifying table, errors" {
    run dolt sql -q "call dolt_conflicts_resolve('--theirs')"
    [ $status -eq 1 ]
    [[ $output =~ "specify at least one table to resolve conflicts" ]] || false
}

@test "sql-conflicts-resolve: call with non-existent table, errors" {
    run dolt sql -q "call dolt_conflicts_resolve('--ours', 'notexists')"
    [ $status -eq 1 ]
    [[ $output =~ "table not found" ]] || false
}

@test "sql-conflicts-resolve: no conflicts, no changes" {
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
    run dolt sql -q "CALL dolt_conflicts_resolve('--ours', 't')"
    [ $status -eq 0 ]
    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ $output =~ "main" ]] || false

    run dolt sql -q "CALL dolt_conflicts_resolve('--theirs', 't')"
    [ $status -eq 0 ]
    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ $output =~ "main" ]] || false

    dolt checkout other
    run dolt sql -q "CALL dolt_conflicts_resolve('--ours', 't')"
    [ $status -eq 0 ]
    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ $output =~ "other" ]] || false

    run dolt sql -q "CALL dolt_conflicts_resolve('--theirs', 't')"
    [ $status -eq 0 ]
    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ $output =~ "other" ]] || false
}

@test "sql-conflicts-resolve: merge other into main, resolve with ours" {
    basic_conflict

    dolt checkout main
    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ $output =~ "main" ]] || false

    run dolt merge other
    [ $status -eq 1 ]
    [[ $output =~ "Automatic merge failed" ]] || false

    run dolt sql -q "CALL dolt_conflicts_resolve('--ours', 't')"
    [ $status -eq 0 ]
    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ $output =~ "main" ]] || false
}

@test "sql-conflicts-resolve: merge other into main, resolve with theirs" {
    basic_conflict

    dolt checkout main
    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ $output =~ "main" ]] || false

    run dolt merge other
    [ $status -eq 1 ]
    [[ $output =~ "Automatic merge failed" ]] || false

    run dolt sql -q "CALL dolt_conflicts_resolve('--theirs', 't')"
    [ $status -eq 0 ]
    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ $output =~ "other" ]] || false
}

@test "sql-conflicts-resolve: merge main into other, resolve with ours" {
    basic_conflict

    dolt checkout other
    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ $output =~ "other" ]] || false

    run dolt merge main
    [ $status -eq 1 ]
    [[ $output =~ "Automatic merge failed" ]] || false

    run dolt sql -q "CALL dolt_conflicts_resolve('--ours', 't')"
    [ $status -eq 0 ]
    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ $output =~ "other" ]] || false
}

@test "sql-conflicts-resolve: merge main into other, resolve with theirs" {
    basic_conflict

    dolt checkout other
    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ $output =~ "other" ]] || false

    run dolt merge main
    [ $status -eq 1 ]
    [[ $output =~ "Automatic merge failed" ]] || false

    run dolt sql -q "CALL dolt_conflicts_resolve('--theirs', 't')"
    [ $status -eq 0 ]
    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ $output =~ "main" ]] || false
}