#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    dolt sql <<SQL
CREATE TABLE test1 (
  pk BIGINT NOT NULL,
  c1 BIGINT,
  PRIMARY KEY (pk)
);
INSERT INTO test1 VALUES(1,1);
SQL
    dolt sql <<SQL
CREATE TABLE test2 (
  pk BIGINT NOT NULL,
  c1 BIGINT,
  PRIMARY KEY (pk)
);
INSERT INTO test2 VALUES(2,2);
SQL
}

teardown() {
    teardown_common
}

@test "cp table" {
    run dolt table cp test1 test_new
    [ "$status" -eq 0 ]
    run dolt sql -q 'show tables';
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test1" ]] || false
    [[ "$output" =~ "test_new" ]] || false
    run dolt sql -q 'select * from test_new' -r csv;
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,1" ]] || false
    [ "${#lines[@]}" -eq 2 ]
}

@test "mv table" {
    run dolt table mv test1 test_new
    [ "$status" -eq 0 ]
    run dolt sql -q 'show tables';
    [ "$status" -eq 0 ]
    ! [[ "$output" =~ "test1" ]] || false
    [[ "$output" =~ "test_new" ]] || false
    run dolt sql -q 'select * from test_new' -r csv;
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,1" ]] || false
    [ "${#lines[@]}" -eq 2 ]
}

@test "cp table with the existing name" {
    run dolt table cp test1 test2
    [ "$status" -ne 0 ]
    [[ "$output" =~ "already exists" ]] || false
}

@test "mv table with the existing name" {
    run dolt table mv test1 test2
    [ "$status" -ne 0 ]
    [[ "$output" =~ "already exists" ]] || false
}

@test "cp nonexistent table" {
    run dolt table cp not_found test2
    [ "$status" -ne 0 ]
    [[ "$output" =~ "not found" ]] || false
}

@test "mv non_existent table" {
    run dolt table mv not_found test2
    [ "$status" -ne 0 ]
    [[ "$output" =~ "not found" ]] || false
}

@test "cp table with invalid name" {
    run dolt table cp test1 123
    [ "$status" -eq 1 ]
    [[ "$output" =~ "not a valid table name" ]] || false
    run dolt table cp test1 dolt_docs
    [ "$status" -eq 1 ]
    [[ "$output" =~ "not a valid table name" ]] || false
    [[ "$output" =~ "reserved" ]] || false
    run dolt table cp test1 dolt_query_catalog
    [ "$status" -eq 1 ]
    [[ "$output" =~ "not a valid table name" ]] || false
    [[ "$output" =~ "reserved" ]] || false
    run dolt table cp test1 dolt_reserved
    [ "$status" -eq 1 ]
    [[ "$output" =~ "not a valid table name" ]] || false
    [[ "$output" =~ "reserved" ]] || false
}

@test "mv table with invalid name" {
    run dolt table mv test1 123
    [ "$status" -eq 1 ]
    [[ "$output" =~ "not a valid table name" ]] || false
    run dolt table mv test1 dolt_docs
    [ "$status" -eq 1 ]
    [[ "$output" =~ "not a valid table name" ]] || false
    [[ "$output" =~ "reserved" ]] || false
    run dolt table mv test1 dolt_query_catalog
    [ "$status" -eq 1 ]
    [[ "$output" =~ "not a valid table name" ]] || false
    [[ "$output" =~ "reserved" ]] || false
    run dolt table mv test1 dolt_reserved
    [ "$status" -eq 1 ]
    [[ "$output" =~ "not a valid table name" ]] || false
    [[ "$output" =~ "reserved" ]] || false
}
