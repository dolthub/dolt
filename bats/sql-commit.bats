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
    dolt sql <<SQL
DELETE FROM test WHERE pk = 0;
INSERT INTO test VALUES (3);
SQL
}

teardown() {
    teardown_common
}

@test "DOLT_COMMIT without a message throws error" {
    run dolt sql -q "SELECT DOLT_ADD('.')"
    [ $status -eq 0 ]

    run dolt sql -q "SELECT DOLT_COMMIT()"
    [ $status -eq 1 ]
    run dolt log
    [ $status -eq 0 ]
    regex='Initialize'
    [[ "$output" =~ "$regex" ]] || false
}

@test "DOLT_COMMIT with just a message reads session parameters" {
    run dolt sql -q "SELECT DOLT_ADD('.')"
    [ $status -eq 0 ]

    run dolt sql -q "SELECT DOLT_COMMIT('-m', 'Commit1')"
    [ $status -eq 0 ]
    run dolt log
    [ $status -eq 0 ]
    [[ "$output" =~ "Commit1" ]] || false
    regex='Bats Tests <bats@email.fake>'
    [[ "$output" =~ "$regex" ]] || false
}

@test "DOLT_COMMIT with the all flag performs properly" {
    run dolt sql -q "SELECT DOLT_COMMIT('-a', '-m', 'Commit1')"

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

@test "DOLT_COMMIT with all flag, message and author" {
    run dolt sql -q "SELECT DOLT_COMMIT('-a', '-m', 'Commit1', '--author', 'John Doe <john@doe.com>')"
    [ $status -eq 0 ]
    DCOMMIT=$output

    # Check that everything was added
    run dolt diff
    [ "$status" -eq 0 ]
    [ "$output" = "" ]

    run dolt log
    [ $status -eq 0 ]
    [[ "$output" =~ "Commit1" ]] || false
    regex='John Doe <john@doe.com>'
    [[ "$output" =~ "$regex" ]] || false

    # Check that dolt_log has the same hash as the output of DOLT_COMMIT
    run dolt sql -q "SELECT commit_hash from dolt_log LIMIT 1"
    [ $status -eq 0 ]
    [[ "$output" =~ "$DCOMMIT" ]] || false

    run dolt sql -q "SELECT * from dolt_commits ORDER BY Date DESC;"
    [ $status -eq 0 ]
    [[ "$output" =~ "Commit1" ]] || false
}

@test "DOLT_COMMIT works with --author without config variables set" {
    dolt config --global --unset user.name
    dolt config --global --unset user.email

    run dolt sql -q "SELECT DOLT_ADD('.')"

    run dolt sql -q "SELECT DOLT_COMMIT('-m', 'Commit1', '--author', 'John Doe <john@doe.com>')"
    [ "$status" -eq 0 ]
    DCOMMIT=$output

    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Commit1" ]] || false
    regex='John Doe <john@doe.com>'
    [[ "$output" =~ "$regex" ]] || false

    run dolt sql -q "SELECT * from dolt_log"
    [ $status -eq 0 ]
    [[ "$output" =~ "Commit1" ]] || false

    # Check that dolt_log has the same hash as the output of DOLT_COMMIT
    run dolt sql -q "SELECT commit_hash from dolt_log LIMIT 1"
    [ $status -eq 0 ]
    [[ "$output" =~ "$DCOMMIT" ]] || false

    run dolt sql -q "SELECT * from dolt_commits ORDER BY Date DESC;"
    [ $status -eq 0 ]
    [[ "$output" =~ "Commit1" ]] || false
}