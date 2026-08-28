#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}

@test "prepare: select statements" {
    dolt sql -q "create table t (i int, j int);"
    dolt sql -q "insert into t values (1, 1), (2, 2), (3, 3);"

    run dolt sql -q "prepare s from 'select * from t order by i'; execute s;"
    [ $status -eq 0 ]
    [[ "${lines[0]}" = "+---+---+" ]] || false
    [[ "${lines[1]}" = "| i | j |" ]] || false
    [[ "${lines[2]}" = "+---+---+" ]] || false
    [[ "${lines[3]}" = "| 1 | 1 |" ]] || false
    [[ "${lines[4]}" = "| 2 | 2 |" ]] || false
    [[ "${lines[5]}" = "| 3 | 3 |" ]] || false
    [[ "${lines[6]}" = "+---+---+" ]] || false

    run dolt sql -q "set @x = 2; set @y = 3; prepare s from 'select i, j + ? from t where i = ? order by i'; execute s using @x, @y;"
    [ $status -eq 0 ]
    [[ "${lines[0]}" = "+---+-------+" ]] || false
    [[ "${lines[1]}" = "| i | j + ? |" ]] || false
    [[ "${lines[2]}" = "+---+-------+" ]] || false
    [[ "${lines[3]}" = "| 3 | 5     |" ]] || false
    [[ "${lines[4]}" = "+---+-------+" ]] || false

    run dolt sql -q "set @stmt = 'select count(*) from t where i >= 2'; prepare s from @stmt; execute s;"
    [ $status -eq 0 ]
    [[ "${lines[0]}" = "+----------+" ]] || false
    [[ "${lines[1]}" = "| count(*) |" ]] || false
    [[ "${lines[2]}" = "+----------+" ]] || false
    [[ "${lines[3]}" = "| 2        |" ]] || false
    [[ "${lines[4]}" = "+----------+" ]] || false
}

@test "prepare: insert statements" {
    dolt sql -q "create table t (i int, j int);"
    run dolt sql -q "prepare s from 'insert into t values (1, 1)'; execute s; select * from t order by i;"
    [ $status -eq 0 ]
    [[ "${lines[0]}" = "+---+---+" ]] || false
    [[ "${lines[1]}" = "| i | j |" ]] || false
    [[ "${lines[2]}" = "+---+---+" ]] || false
    [[ "${lines[3]}" = "| 1 | 1 |" ]] || false
    [[ "${lines[4]}" = "+---+---+" ]] || false

    run dolt sql -q "set @x = 123; set @y = 456; prepare s from 'insert into t values (?, ?)'; execute s using @x, @y; select * from t order by i;"
    [ $status -eq 0 ]
    [[ "${lines[0]}" = "+-----+-----+" ]] || false
    [[ "${lines[1]}" = "| i   | j   |" ]] || false
    [[ "${lines[2]}" = "+-----+-----+" ]] || false
    [[ "${lines[3]}" = "| 1   | 1   |" ]] || false
    [[ "${lines[4]}" = "| 123 | 456 |" ]] || false
    [[ "${lines[5]}" = "+-----+-----+" ]] || false

    run dolt sql -q "set @stmt = 'insert into t values (111, 999)'; prepare s from @stmt; execute s; select * from t order by i;"
    [ $status -eq 0 ]
    [[ "${lines[0]}" = "+-----+-----+" ]] || false
    [[ "${lines[1]}" = "| i   | j   |" ]] || false
    [[ "${lines[2]}" = "+-----+-----+" ]] || false
    [[ "${lines[3]}" = "| 1   | 1   |" ]] || false
    [[ "${lines[4]}" = "| 111 | 999 |" ]] || false
    [[ "${lines[5]}" = "| 123 | 456 |" ]] || false
    [[ "${lines[6]}" = "+-----+-----+" ]] || false
}

@test "prepare: update statements" {
    dolt sql -q "create table t (i int, j int);"
    dolt sql -q "insert into t values (1, 1), (2, 2), (3, 3);"

    run dolt sql -q "prepare s from 'update t set j = 100 where i = 1'; execute s; select * from t order by i;"
    [ $status -eq 0 ]
    [[ "${lines[0]}" = "+---+-----+" ]] || false
    [[ "${lines[1]}" = "| i | j   |" ]] || false
    [[ "${lines[2]}" = "+---+-----+" ]] || false
    [[ "${lines[3]}" = "| 1 | 100 |" ]] || false
    [[ "${lines[4]}" = "| 2 | 2   |" ]] || false
    [[ "${lines[5]}" = "| 3 | 3   |" ]] || false
    [[ "${lines[6]}" = "+---+-----+" ]] || false

    run dolt sql -q "set @x = 200; set @y = 2; prepare s from 'update t set j = ? where i = ?'; execute s using @x, @y; select * from t order by i;"
    [ $status -eq 0 ]
    [[ "${lines[0]}" = "+---+-----+" ]] || false
    [[ "${lines[1]}" = "| i | j   |" ]] || false
    [[ "${lines[2]}" = "+---+-----+" ]] || false
    [[ "${lines[3]}" = "| 1 | 100 |" ]] || false
    [[ "${lines[4]}" = "| 2 | 200 |" ]] || false
    [[ "${lines[5]}" = "| 3 | 3   |" ]] || false
    [[ "${lines[6]}" = "+---+-----+" ]] || false


    run dolt sql -q "set @stmt = 'update t set j = 300 where i = 3'; prepare s from @stmt; execute s; select * from t order by i;"
    [ $status -eq 0 ]
    [[ "${lines[0]}" = "+---+-----+" ]] || false
    [[ "${lines[1]}" = "| i | j   |" ]] || false
    [[ "${lines[2]}" = "+---+-----+" ]] || false
    [[ "${lines[3]}" = "| 1 | 100 |" ]] || false
    [[ "${lines[4]}" = "| 2 | 200 |" ]] || false
    [[ "${lines[5]}" = "| 3 | 300 |" ]] || false
    [[ "${lines[6]}" = "+---+-----+" ]] || false
}


@test "prepare: delete statements" {
    dolt sql -q "create table t (i int, j int);"
    dolt sql -q "insert into t values (1, 1), (2, 2), (3, 3);"

    run dolt sql -q "prepare s from 'delete from t where i = 1'; execute s; select * from t order by i;"
    [ $status -eq 0 ]
    [[ "${lines[0]}" = "+---+---+" ]] || false
    [[ "${lines[1]}" = "| i | j |" ]] || false
    [[ "${lines[2]}" = "+---+---+" ]] || false
    [[ "${lines[3]}" = "| 2 | 2 |" ]] || false
    [[ "${lines[4]}" = "| 3 | 3 |" ]] || false
    [[ "${lines[5]}" = "+---+---+" ]] || false

    run dolt sql -q "set @x = 2; prepare s from 'delete from t where i = ?'; execute s using @x; select * from t order by i;"
    [ $status -eq 0 ]
    [[ "${lines[0]}" = "+---+---+" ]] || false
    [[ "${lines[1]}" = "| i | j |" ]] || false
    [[ "${lines[2]}" = "+---+---+" ]] || false
    [[ "${lines[3]}" = "| 3 | 3 |" ]] || false
    [[ "${lines[4]}" = "+---+---+" ]] || false


    run dolt sql -q "set @stmt = 'delete from t where i = 3'; prepare s from @stmt; execute s; select * from t order by i;"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 0 ]
    [[ "${lines[0]}" = "" ]] || false
}