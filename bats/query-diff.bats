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

@test "dolt diff -q" {
    dolt sql -q 'insert into test values (0,0,"0"), (1,1,"1")'
    dolt add .
    dolt commit -m rows
    dolt sql -q 'update test set c1 = 9 where pk = 0'
    dolt sql -q 'delete from test where pk=1'
    dolt sql -q 'insert into test values (2,2,"2")'
    dolt diff -q 'select * from test'
    run dolt diff -q 'select * from test'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "|     | pk | c1 | c2 |" ]]
    [[ "$output" =~ "|  <  | 0  | 0  | 0  |" ]]
    [[ "$output" =~ "|  >  | 0  | 9  | 0  |" ]]
    [[ "$output" =~ "|  -  | 1  | 1  | 1  |" ]]
    [[ "$output" =~ "|  +  | 2  | 2  | 2  |" ]]
}

@test "dolt diff -q join query" {
    dolt sql -q 'insert into test values (0,0,"0"), (1,1,"1")'
    dolt sql -q 'insert into quiz values (0,0,"0"), (1,1,"1")'
    dolt add .
    dolt commit -m rows
    dolt sql -q 'update quiz set c2 = "1" where pk = 0'
    run dolt diff -q 'select test.pk, test.c1, test.c2, quiz.pk, quiz.c1 from test join quiz on test.c2 = quiz.c2 order by test.pk, quiz.pk'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "|     | pk | c1 | c2 | pk | c1 |" ]]
    [[ "$output" =~ "|  -  | 0  | 0  | 0  | 0  | 0  |" ]]
    [[ "$output" =~ "|  +  | 1  | 1  | 1  | 0  | 0  |" ]]
}

@test "dolt diff -q a view" {
    dolt sql -q 'insert into test values (0,0,"0"), (1,1,"1"), (3,3,"3")'
    dolt sql -q 'create view positive as select * from test where pk > 0'
    dolt add .
    dolt commit -m rows
    dolt sql -q 'delete from test where pk=1'
    dolt sql -q 'insert into test values (2,2,"2")'
    run dolt diff -q 'select * from positive order by pk'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "|     | pk | c1 | c2 |" ]]
    [[ "$output" =~ "|  -  | 1  | 1  | 1  |" ]]
    [[ "$output" =~ "|  +  | 2  | 2  | 2  |" ]]
}
