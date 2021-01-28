#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common

    dolt sql <<SQL
CREATE TABLE test (
  pk int NOT NULL PRIMARY KEY,
  c0 int
);
INSERT INTO test VALUES
    (0,0),(1,1),(2,2);
CREATE TABLE to_drop (
    pk int PRIMARY KEY
);
SQL
    dolt add -A
    dolt commit -m "added table test"
}

teardown() {
    teardown_common
}

@test "dolt create and list workspace" {
    run dolt workspace new-workspace
    [ "$status" -eq 0 ]
    [[ "$output" =~ "successfully created workspace: new-workspace" ]] || false

     run dolt workspace another-workspace
    [ "$status" -eq 0 ]
    [[ "$output" =~ "successfully created workspace: another-workspace" ]] || false

    run dolt workspace
    [ "$status" -eq 0 ]
    [[ "$output" =~ "new-workspace" ]] || false
    [[ "$output" =~ "another-workspace" ]] || false

    run dolt workspace -d new-workspace
    [ "$status" -eq 0 ]

    run dolt workspace
    [ "$status" -eq 0 ]
    [[ "$output" =~ "another-workspace" ]] || false
}