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
    dolt sql <<SQL
DELETE FROM test WHERE pk = 0;
INSERT INTO test VALUES (3);
SQL
    dolt add .
}

teardown() {
    teardown_common
}

@test "DOLT_COMMIT with a message and author" {
    run dolt sql -q "SELECT DOLT_COMMIT('-m', 'Commit1', '--author', 'John Doe <john@doe.com>')"
    [ $status -eq 0 ]
    run dolt log
    [ $status -eq 0 ]
    [[ "$output" =~ "Commit1" ]] || false
    regex='John Doe <john@doe.com>'
    [[ "$output" =~ "$regex" ]] || false
}

@test "DOLT_COMMIT without a message throws error" {
    run dolt sql -q "SELECT DOLT_COMMIT()"
    [ $status -eq 1 ]
    run dolt log
    [ $status -eq 0 ]
    regex='Initialize'
    [[ "$output" =~ "$regex" ]] || false
}

@test "DOLT_COMMIT with just a message reads session parameters" {
    run dolt sql -q "SELECT DOLT_COMMIT('-m', 'Commit1')"
    [ $status -eq 0 ]
    run dolt log
    [ $status -eq 0 ]
    [[ "$output" =~ "Commit1" ]] || false
    regex='Bats Tests <bats@email.fake>'
    [[ "$output" =~ "$regex" ]] || false
}