#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common

    dolt sql <<SQL
CREATE TABLE test (
    pk int,
    c1 int,
    c2 varchar(20),
    primary key (pk)
);
SQL
    dolt sql <<SQL
CREATE TABLE quiz (
    pk int,
    c1 int,
    c2 varchar(20),
    primary key (pk)
);
SQL
}

teardown() {
    teardown_common
}

@test "dolt query_diff" {
    dolt sql -q 'insert into test values (0,0,"0"), (1,1,"1")'
    dolt add .
    dolt commit -m rows
    dolt sql -q 'update test set c1 = 9 where pk = 0'
    dolt sql -q 'delete from test where pk=1'
    dolt sql -q 'insert into test values (2,2,"2")'
    dolt query_diff 'select * from test'
    run dolt query_diff 'select * from test'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "|     | pk | c1 | c2 |" ]]
    [[ "$output" =~ "|  <  | 0  | 0  | 0  |" ]]
    [[ "$output" =~ "|  >  | 0  | 9  | 0  |" ]]
    [[ "$output" =~ "|  -  | 1  | 1  | 1  |" ]]
    [[ "$output" =~ "|  +  | 2  | 2  | 2  |" ]]
}

@test "dolt query_diff join query" {
    dolt sql -q 'insert into test values (0,0,"0"), (1,1,"1")'
    dolt sql -q 'insert into quiz values (0,0,"0"), (1,1,"1")'
    dolt add .
    dolt commit -m rows
    dolt sql -q 'update quiz set c2 = "1" where pk = 0'
    run dolt query_diff 'select test.pk, test.c1, test.c2, quiz.pk, quiz.c1 from test join quiz on test.c2 = quiz.c2 order by test.pk, quiz.pk'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "|     | pk | c1 | c2 | pk | c1 |" ]]
    [[ "$output" =~ "|  -  | 0  | 0  | 0  | 0  | 0  |" ]]
    [[ "$output" =~ "|  +  | 1  | 1  | 1  | 0  | 0  |" ]]
}

@test "dolt query_diff a view" {
    dolt sql -q 'insert into test values (0,0,"0"), (1,1,"1"), (3,3,"3")'
    dolt sql -q 'create view positive as select * from test where pk > 0'
    dolt add .
    dolt commit -m rows
    dolt sql -q 'delete from test where pk=1'
    dolt sql -q 'insert into test values (2,2,"2")'
    run dolt query_diff 'select * from positive order by pk'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "|     | pk | c1 | c2 |" ]]
    [[ "$output" =~ "|  -  | 1  | 1  | 1  |" ]]
    [[ "$output" =~ "|  +  | 2  | 2  | 2  |" ]]
}

@test "dolt query_diff query error" {
    dolt add .
    dolt commit -m 'added tables'
    run dolt query_diff head^ head 'select * from test order by pk'
    [ "$status" -ne 0 ]
    [[ "$output" =~ "error executing query on from root" ]]
    run dolt query_diff head head^ 'select * from test order by pk'
    [ "$status" -ne 0 ]
    [[ "$output" =~ "error executing query on to root" ]]
}
