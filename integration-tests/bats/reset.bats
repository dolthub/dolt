#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

setup_ancestor() {
    dolt sql <<SQL
CREATE TABLE test1 (
  pk int NOT NULL,
  c1 int,
  c2 int,
  PRIMARY KEY (pk)
);
INSERT INTO test1 values (0,1,1);
SQL

    dolt add .
    dolt commit -m "added tables"
}

merge_without_conflicts() {
    setup_ancestor

    dolt checkout -b merge_branch
    dolt SQL -q "UPDATE test1 set c1 = 2;"
    dolt add test1
    dolt commit -m "update pk 0 = 2,1 to test1"

    dolt checkout main
    dolt SQL -q "UPDATE test1 set c2 = 2;"
    dolt add test1
    dolt commit -m "update pk 0 = 1,2 to test1"

    run dolt merge merge_branch
}

merge_with_conflicts() {
    setup_ancestor

    dolt checkout -b merge_branch
    dolt SQL -q "UPDATE test1 set c1 = 2, c2 = 2;"
    dolt add test1
    dolt commit -m "update pk 0 = 2,2 to test1"

    dolt checkout main
    dolt SQL -q "UPDATE test1 set c2 = 3, c2 = 3;"
    dolt add test1
    dolt commit -m "update pk 0 = 3,3 to test1"

    run dolt merge merge_branch
}

@test "reset: dolt reset --hard should clear an uncommitted merge state" {
    merge_without_conflicts

    run dolt reset --hard
    [ $status -eq 0 ]

    run dolt status
    [[ "$output" =~ "nothing to commit, working tree clean" ]]

    run dolt merge --abort
    [[ "$output" =~ "fatal: There is no merge to abort" ]]
}

@test "reset: dolt reset --hard should clear a conflicted merge state" {
    merge_with_conflicts

    run dolt reset --hard
    [ $status -eq 0 ]

    run dolt status
    [[ "$output" =~ "nothing to commit, working tree clean" ]]

    run dolt merge --abort
    [[ "$output" =~ "fatal: There is no merge to abort" ]]
}