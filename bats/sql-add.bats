#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common

    dolt sql <<SQL
CREATE TABLE test (
    pk int primary key
);
CREATE TABLE test2 (
    pk int primary key
);
INSERT INTO test VALUES (0),(1),(2);
SQL
}

teardown() {
    teardown_common
}

@test "DOLT_ADD all flag works" {
    run dolt sql -q "SELECT DOLT_ADD('-A')"
    run dolt sql -q "SELECT DOLT_COMMIT('-m', 'Commit1')"

    # Check that everything was added
    run dolt diff
    [ "$status" -eq 0 ]
    [ "$output" = "" ]

    run dolt log
    [ $status -eq 0 ]
    [[ "$output" =~ "Commit1" ]] || false
    regex='Bats Tests <bats@email.fake>'
    [[ "$output" =~ "$regex" ]] || false
}

@test "DOLT_ADD all w/ . works" {
    run dolt sql -q "SELECT DOLT_ADD('.')"
    run dolt sql -q "SELECT DOLT_COMMIT('-m', 'Commit1')"

    # Check that everything was added
    run dolt diff
    [ "$status" -eq 0 ]
    [ "$output" = "" ]

    run dolt log
    [ $status -eq 0 ]
    [[ "$output" =~ "Commit1" ]] || false
    regex='Bats Tests <bats@email.fake>'
    [[ "$output" =~ "$regex" ]] || false
}

@test "DOLT_ADD can take in one table" {
    run dolt sql -q "SELECT DOLT_ADD('test')"
    run dolt sql -q "SELECT DOLT_COMMIT('-m', 'Commit1')"

    # Check that just test was added and not test2.
    run dolt status
    [ "$status" -eq 0 ]
    regex='test2'
    [[ "$output" =~ "$regex" ]] || false

    run dolt log
    [ $status -eq 0 ]
    [[ "$output" =~ "Commit1" ]] || false
    regex='Bats Tests <bats@email.fake>'
    [[ "$output" =~ "$regex" ]] || false
}


@test "DOLT_ADD can take in multiple tables" {
    run dolt sql -q "SELECT DOLT_ADD('test', 'test2')"
    run dolt sql -q "SELECT DOLT_COMMIT('-m', 'Commit1')"

    # Check that both test and test2 are added.
    run dolt diff
    [ "$status" -eq 0 ]
    [ "$output" = "" ]

    run dolt log
    [ $status -eq 0 ]
    [[ "$output" =~ "Commit1" ]] || false
    regex='Bats Tests <bats@email.fake>'
    [[ "$output" =~ "$regex" ]] || false
}

@test "Check that Dolt add works with docs" {
     echo readme-text > README.md
     run ls
     [[ "$output" =~ "README.md" ]] || false

     run dolt sql -q "SELECT DOLT_ADD('README.md')"
     [ "$status" -eq 0 ]

     # Check that the README was added as a new doc.
     run dolt status
     [ "$status" -eq 0 ]
     regex='new doc'
     [[ "$output" =~ "$regex" ]] || false
}