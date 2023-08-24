#!/usr/bin/env bats

load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    dolt sql <<SQL
create database db1;
use db1;
create table t (pk int primary key);
insert into t values (1);
call dolt_commit('-Am', 'added table t');
call dolt_branch('b1');
call dolt_branch('b2');
SQL
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "global-args: can specify default branch with --use-db" {
    dolt --use-db db1 sql -q "insert into t values (2)"
    dolt --use-db db1 commit -Am "added row to t"

    run dolt --use-db db1 sql -q "select * from t"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1" ]] || false
    [[ "$output" =~ "2" ]] || false
    mainOutput=$output

    run dolt --use-db db1/main sql -q "select * from t"
    [ "$status" -eq 0 ]
    [[ "$mainOutput" == "$output" ]] || false
}

@test "global-args: can specify non-default branch with --use-db" {
    dolt sql <<SQL
use db1;
call dolt_checkout('b1');
insert into t values (2);
call dolt_commit('-Am', 'modified b1');
SQL

    run dolt --use-db db1 sql -q "select * from t"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1" ]] || false
    [[ ! "$output" =~ "2" ]] || false

    run dolt --use-db db1/b1 sql -q "select * from t"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1" ]] || false
    [[ "$output" =~ "2" ]] || false
}

@test "global-args: can specify multiple non-default branches with --use-db" {
    dolt sql <<SQL
use db1;
call dolt_checkout('b1');
insert into t values (2);
call dolt_commit('-Am', 'modified b1');
call dolt_checkout('b2');
insert into t values (3);
call dolt_commit('-Am', 'modified b2');
SQL

    run dolt --use-db db1/b1 sql -q "select * from t"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1" ]] || false
    [[ "$output" =~ "2" ]] || false
    [[ ! "$output" =~ "3" ]] || false

    run dolt --use-db db1/b2 sql -q "select * from t"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1" ]] || false
    [[ ! "$output" =~ "2" ]] || false
    [[ "$output" =~ "3" ]] || false
}

@test "global-args: can specify multiple non-default branches with --branch" {
    dolt sql <<SQL
use db1;
call dolt_checkout('b1');
insert into t values (2);
call dolt_commit('-Am', 'modified b1');
call dolt_checkout('b2');
insert into t values (3);
call dolt_commit('-Am', 'modified b2');
SQL

    run dolt --use-db db1 --branch b1 sql -q "select * from t"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1" ]] || false
    [[ "$output" =~ "2" ]] || false
    [[ ! "$output" =~ "3" ]] || false

    run dolt --use-db db1 --branch b2 sql -q "select * from t"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1" ]] || false
    [[ ! "$output" =~ "2" ]] || false
    [[ "$output" =~ "3" ]] || false
}

@test "global-args: can not specify a branch twice with ambiguous name"  {
    dolt sql <<SQL
use db1;
call dolt_checkout('b1');
insert into t values (2);
call dolt_commit('-Am', 'modified b1');
call dolt_checkout('b2');
insert into t values (3);
call dolt_commit('-Am', 'modified b2');
SQL

    run dolt --use-db=db1/b1 --branch b2 sql -q "select * from t"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Ambiguous branch name: b1 or b2" ]] || false

    run dolt --use-db db1 --branch b2 sql -q "select * from t"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1" ]] || false
    [[ ! "$output" =~ "2" ]] || false
    [[ "$output" =~ "3" ]] || false
}
