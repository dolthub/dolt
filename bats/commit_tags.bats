#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common

    dolt sql <<SQL
CREATE TABLE test (
    pk int primary key
);
INSERT INTO test VALUES (0),(1),(2);
SQL
    dolt add .
    dolt commit -m "created table test"
    dolt sql <<SQL
DELETE FROM test WHERE pk = 0;
INSERT INTO test VALUES (3);
SQL
    dolt add .
    dolt commit -m "made changes"
}

teardown() {
    teardown_common
}

@test "create a tag with a explicit ref" {
    run dolt tag v1.0 HEAD^
    [ $status -eq 0 ]
    run dolt tag
    [ $status -eq 0 ]
    [ "$output" = "v1.0" ]
}

@test "create a tag with implicit head ref" {
    run dolt tag v1.0
    [ $status -eq 0 ]
    run dolt tag
    [ $status -eq 0 ]
    [ "$output" = "v1.0" ]
}

@test "delete a tag" {
    dolt tag v1.0
    dolt tag -d v1.0
    run dolt tag
    [ $status -eq 0 ]
    [ "$output" = "" ]
}

@test "checkout a tag" {
    dolt branch comp HEAD^
    dolt tag v1.0 HEAD^
    skip "need to implelement detached head first"
    run dolt checkout v1.0
    [ $status -eq 0 ]
    run dolt diff comp
    [ $status -eq 0 ]
    [ "$output" = "" ]
}

@test "commit onto checked out tag" {
    dolt tag v1.0 HEAD^
    skip "need to implement detached head first"
    dolt checkout v1.0
    run dolt sql -q "insert into test values (8),(9)"
    [ $status -eq 0 ]
    dolt add -A
    run dolt commit -m "msg"
    [ $status -eq 0 ]
}

@test "use a tag as ref for diff" {
    dolt tag v1.0 HEAD^
    run dolt diff v1.0
    [ $status -eq 0 ]
    [[ "$output" =~ "-  | 0" ]]
    [[ "$output" =~ "+  | 3" ]]
}

@use "use a tag as a ref for merge" {
    dolt tag v1.0 HEAD
    dolt checkout -b other HEAD^
    dolt sql -q "insert into test values (8),(9)"
    run dolt merge v1.0
    [ $status -eq 0 ]
    run dolt sql -q "select * from test"
    [ $status -eq 0 ]
    [[ "$output" =~ "1" ]]
    [[ "$output" =~ "2" ]]
    [[ "$output" =~ "3" ]]
    [[ "$output" =~ "8" ]]
    [[ "$output" =~ "9" ]]
}

@test "push/pull tags to/from a remote" {

}
