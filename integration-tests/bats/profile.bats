#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash
load $BATS_TEST_DIRNAME/helper/query-server-common.bash

make_repo() {
  mkdir "$1"
  cd "$1"
  dolt init
  dolt sql -q "create table $1_tbl (id int)"
  dolt sql <<SQL
CREATE TABLE table1 (pk int PRIMARY KEY);
CREATE TABLE table2 (pk int PRIMARY KEY);
INSERT INTO dolt_ignore VALUES ('generated_*', 1);
SQL
  dolt add -A && dolt commit -m "tables table1, table2"
  dolt sql <<SQL
INSERT INTO  table1 VALUES (1),(2),(3);
INSERT INTO  table2 VALUES (1),(2),(3);
CREATE TABLE table3 (pk int PRIMARY KEY);
CREATE TABLE generated_foo (pk int PRIMARY KEY);
SQL
  dolt add table1
  # Note that we leave the table in a dirty state, which is useful to several tests, and harmless to others. For
  # some, you need to ensure the repo is clean, and you should run `dolt reset --hard` at the beginning of the test.
  cd ..
}

setup() {
    setup_no_dolt_init
    make_repo defaultDB
    make_repo altDB

    dolt config --global --add profile '{"defaultTest": {"use-db":"defaultDB"}}'
    unset DOLT_CLI_PASSWORD
    unset DOLT_SILENCE_USER_REQ_FOR_TESTING
}

teardown() {
    stop_sql_server 1
    teardown_common
}

@test "profile: --profile exists and isn't empty" {
    cd defaultDB
    dolt sql -q "create table test (pk int primary key)"
    dolt sql -q "insert into test values (999)"
    dolt add test
    dolt commit -m "insert initial value into test"
    cd ..

    run dolt --profile defaultTest sql -q "select * from test"
    [ "$status" -eq 0 ] || false
    [[ "$output" =~ "999" ]] || false
}

@test "profile: --profile doesn't exist" {
    run dolt --profile nonExistentProfile sql -q "select * from altDB_tbl"
    [ "$status" -eq 1 ] || false
    [[ "$output" =~ "Failure to parse arguments: profile nonExistentProfile not found" ]] || false
}
