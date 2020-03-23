#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    dolt sql <<SQL
CREATE TABLE test1 (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  PRIMARY KEY (pk)
);
SQL
    dolt sql <<SQL
CREATE TABLE test2 (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  PRIMARY KEY (pk)
);
SQL

    dolt add .
    dolt commit -m "added tables"
}

teardown() {
    teardown_common
}

@test "3way merge doesn't stomp working changes" {
    dolt checkout -b merge_branch
    dolt SQL -q "INSERT INTO test1 values (0,1,2)"
    dolt add test1
    dolt commit -m "add pk 0 to test1"

    dolt checkout master
    dolt SQL -q "INSERT INTO test1 values (1,2,3)"
    dolt add test1
    dolt commit -m "add pk 1 to test1"

    dolt SQL -q "INSERT INTO test2 values (0,1,2)"
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test2" ]] || false
    [[ ! "$output" =~ "test1" ]] || false

    run dolt merge merge_branch
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "Fast-forward" ]] || false

    run dolt status
    echo -e "\n\noutput: " $output "\n\n"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test2" ]] || false
    [[ "$output" =~ "test1" ]] || false
}

@test "ff merge doesn't stomp working changes" {
    dolt checkout -b merge_branch
    dolt SQL -q "INSERT INTO test1 values (0,1,2)"
    dolt add test1
    dolt commit -m "modify test1"

    dolt checkout master
    dolt SQL -q "INSERT INTO test2 values (0,1,2)"
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test2" ]] || false
    [[ ! "$output" =~ "test1" ]] || false

    run dolt merge merge_branch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Fast-forward" ]] || false

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test2" ]] || false
    [[ ! "$output" =~ "test1" ]] || false
}

@test "3way merge rejected when working changes touch same tables" {
    dolt checkout -b merge_branch
    dolt SQL -q "INSERT INTO test1 values (0,1,2)"
    dolt add test1
    dolt commit -m "add pk 0 to test1"

    dolt checkout master
    dolt SQL -q "INSERT INTO test2 values (0,1,2)"
    dolt add test2
    dolt commit -m "add pk 0 to test2"

    dolt SQL -q "INSERT INTO test1 values (1,2,3)"
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test1" ]] || false
    [[ ! "$output" =~ "test2" ]] || false

    run dolt merge merge_branch
    [ "$status" -eq 1 ]
}

@test "ff merge rejected when working changes touch same tables" {
    dolt checkout -b merge_branch
    dolt SQL -q "INSERT INTO test1 values (0,1,2)"
    dolt add test1
    dolt commit -m "modify test1"

    dolt checkout master
    dolt ls
    dolt SQL -q "INSERT INTO test1 values (1,2,3)"
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test1" ]] || false
    [[ ! "$output" =~ "test2" ]] || false

    run dolt merge merge_branch
    [ "$status" -eq 1 ]
}
