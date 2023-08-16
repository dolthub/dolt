#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    dolt sql -q "create table t (i int primary key, j int);"
    dolt sql -q "insert into t values (1, 1), (2, 2), (3, 3)";
    dolt sql -q "create table tt (i int primary key, j int);"
    dolt sql -q "insert into tt values (1, 1), (2, 2), (3, 3)";
    dolt add .
    dolt commit -m "initial commit"
    dolt branch other
    dolt sql -q "update t set j = 10 where i = 2"
    dolt sql -q "delete from t where i = 3;"
    dolt sql -q "insert into t values (4, 4);"
    dolt add .
    dolt commit -m "changes"
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "query-diff: no args" {
    run dolt query-diff
    [ "$status" -eq 1 ]
    [ "${#lines[@]}" -eq 1 ]
    [[ "${lines[0]}"  =~ "please provide exactly two queries" ]] || false
}

@test "query-diff: too many args" {
    run dolt query-diff "select * from t;" "select * from t;" "select * from t;"
    [ "$status" -eq 1 ]
    [ "${#lines[@]}" -eq 1 ]
    [[ "${lines[0]}"  =~ "please provide exactly two queries" ]] || false
}

@test "query-diff: no changes" {
    run dolt query-diff "select * from t;" "select * from t;"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
    [[ "${lines[0]}"  =~ "diff --dolt a/select * from t; b/select * from t;" ]] || false
    [[ "${lines[1]}"  =~ "--- a/select * from t;" ]] || false
    [[ "${lines[2]}"  =~ "+++ b/select * from t;" ]] || false
}

@test "query-diff: basic case" {
    run dolt query-diff "select * from t as of other;" "select * from t as of head;"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 11 ]
    [[ "${lines[0]}"  =~ "diff --dolt a/select * from t as of other; b/select * from t as of head;" ]] || false
    [[ "${lines[1]}"  =~ "--- a/select * from t as of other;" ]] || false
    [[ "${lines[2]}"  =~ "+++ b/select * from t as of head;" ]] || false
    [[ "${lines[3]}"  =~ "+---+---+----+" ]] || false
    [[ "${lines[4]}"  =~ "|   | i | j  |" ]] || false
    [[ "${lines[5]}"  =~ "+---+---+----+" ]] || false
    [[ "${lines[6]}"  =~ "| < | 2 | 2  |" ]] || false
    [[ "${lines[7]}"  =~ "| > | 2 | 10 |" ]] || false
    [[ "${lines[8]}"  =~ "| - | 3 | 3  |" ]] || false
    [[ "${lines[9]}"  =~ "| + | 4 | 4  |" ]] || false
    [[ "${lines[10]}" =~ "+---+---+----+" ]] || false
}


@test "query-diff: other table" {
    run dolt query-diff "select * from t;" "select * from tt;"

    [[ "${lines[0]}"  =~ "diff --dolt a/select * from t; b/select * from tt;" ]] || false
    [[ "${lines[1]}"  =~ "--- a/select * from t" ]] || false
    [[ "${lines[2]}"  =~ "+++ b/select * from tt" ]] || false
    [[ "${lines[3]}"  =~ "+---+------+------+------+------+" ]] || false
    [[ "${lines[4]}"  =~ "|   | i    | j    | i    | j    |" ]] || false
    [[ "${lines[5]}"  =~ "+---+------+------+------+------+" ]] || false
    [[ "${lines[6]}"  =~ "| - | 1    | 1    | NULL | NULL |" ]] || false
    [[ "${lines[7]}"  =~ "| - | 2    | 10   | NULL | NULL |" ]] || false
    [[ "${lines[8]}"  =~ "| - | 4    | 4    | NULL | NULL |" ]] || false
    [[ "${lines[9]}"  =~ "| + | NULL | NULL | 1    | 1    |" ]] || false
    [[ "${lines[10]}" =~ "| + | NULL | NULL | 2    | 2    |" ]] || false
    [[ "${lines[11]}" =~ "| + | NULL | NULL | 3    | 3    |" ]] || false
    [[ "${lines[12]}" =~ "+---+------+------+------+------+" ]] || false
}