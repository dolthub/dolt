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
    [ "${#lines[@]}" -eq 0 ]
}

@test "query-diff: basic case" {
    run dolt query-diff "select * from t as of other;" "select * from t as of head;"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 7 ]
    [[ "${lines[0]}"  =~ "+--------+--------+------+------+-----------+" ]] || false
    [[ "${lines[1]}"  =~ "| from_i | from_j | to_i | to_j | diff_type |" ]] || false
    [[ "${lines[2]}"  =~ "+--------+--------+------+------+-----------+" ]] || false
    [[ "${lines[3]}"  =~ "| 2      | 2      | 2    | 10   | modified  |" ]] || false
    [[ "${lines[4]}"  =~ "| 3      | 3      | NULL | NULL | deleted   |" ]] || false
    [[ "${lines[5]}"  =~ "| NULL   | NULL   | 4    | 4    | added     |" ]] || false
    [[ "${lines[6]}"  =~ "+--------+--------+------+------+-----------+" ]] || false
}


@test "query-diff: other table" {
    run dolt query-diff "select * from t;" "select * from tt;"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 10 ]
    [[ "${lines[0]}" =~ "+--------+--------+------+------+-----------+" ]] || false
    [[ "${lines[1]}" =~ "| from_i | from_j | to_i | to_j | diff_type |" ]] || false
    [[ "${lines[2]}" =~ "+--------+--------+------+------+-----------+" ]] || false
    [[ "${lines[3]}" =~ "| 1      | 1      | NULL | NULL | deleted   |" ]] || false
    [[ "${lines[4]}" =~ "| 2      | 10     | NULL | NULL | deleted   |" ]] || false
    [[ "${lines[5]}" =~ "| 4      | 4      | NULL | NULL | deleted   |" ]] || false
    [[ "${lines[6]}" =~ "| NULL   | NULL   | 1    | 1    | added     |" ]] || false
    [[ "${lines[7]}" =~ "| NULL   | NULL   | 2    | 2    | added     |" ]] || false
    [[ "${lines[8]}" =~ "| NULL   | NULL   | 3    | 3    | added     |" ]] || false
    [[ "${lines[9]}" =~ "+--------+--------+------+------+-----------+" ]] || false
}