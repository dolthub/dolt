#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    dolt sql <<SQL
CREATE TABLE test1 (
  pk int NOT NULL,
  c1 int,
  c2 int,
  PRIMARY KEY (pk)
);
CREATE TABLE test2 (
  pk int NOT NULL,
  c1 int,
  c2 int,
  PRIMARY KEY (pk)
);
SQL

    dolt add .
    dolt commit -m "added tables"
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "merge: 3way merge doesn't stomp working changes" {
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

    # make sure all the commits make it into the log
    dolt add .
    dolt commit -m "squash merge"

    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "add pk 0 to test1" ]] || false
    [[ "$output" =~ "add pk 1 to test1" ]] || false
}

@test "merge: --abort restores working changes" {
    dolt branch other

    dolt sql -q "INSERT INTO test1 VALUES (0,10,10),(1,11,11);"
    dolt commit -am "added rows to test1 on master"

    dolt checkout other
    dolt sql -q "INSERT INTO test1 VALUES (0,20,20),(1,21,21);"
    dolt commit -am "added rows to test1 on other"

    dolt checkout master
    # dirty the working set with changes to test2
    dolt sql -q "INSERT INTO test2 VALUES (9,9,9);"

    dolt merge other
    dolt merge --abort

    # per Git, working set changes to test2 should remain
    dolt sql -q "SELECT * FROM test2" -r csv
    run dolt sql -q "SELECT * FROM test2" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "9,9,9" ]] || false
}

@test "merge: squash merge" {
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

    run dolt merge --squash merge_branch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Squash" ]] || false
    [[ ! "$output" =~ "Fast-forward" ]] || false

@test "merge: unique index conflict" {
    dolt sql <<SQL
CREATE TABLE test (
    pk int PRIMARY KEY,
    c0 int,
    UNIQUE KEY(c0)
);
INSERT INTO test VALUES (0,0);
SQL
    dolt add -A && dolt commit -am "setup"

    dolt checkout -b other
    dolt sql -q "INSERT INTO test VALUES (2,19);"
    dolt commit -am "added row"

    dolt checkout master
    dolt sql -q "INSERT INTO test VALUES (1,19);"
    dolt commit -am "added row"

    skip "merge fails on unique index violation, should log conflict"
    dolt merge other
}

@test "merge: compound unique index conflict" {
    dolt sql <<SQL
CREATE TABLE test (
    pk int PRIMARY KEY,
    c0 int,
    c1 int,
    UNIQUE KEY(c0,c1)
);
INSERT INTO test VALUES (0, 0,  0);
INSERT INTO test VALUES (1, 11, 2);
INSERT INTO test VALUES (2, 1,  22);
SQL
    dolt add -A && dolt commit -am "setup"

    dolt checkout -b other
    dolt sql -q "UPDATE test SET c0 = 1 where c0 = 11"
    dolt commit -am "added row"

    dolt checkout master
    dolt sql -q "UPDATE test SET c1 = 2 where c1 = 22"
    dolt commit -am "added row"

    skip "merge fails on unique index violation, should log conflict"
    dolt merge other
}
