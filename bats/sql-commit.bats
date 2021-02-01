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

@test "DOLT_COMMIT updates session variables and keeps dolt log fresh" {
    run dolt sql << SQL
SELECT DOLT_COMMIT('-a', '-m', 'Commit1');
SELECT * FROM dolt_log;
SQL

    [ $status -eq 0 ]
    [[ "$output" =~ "Commit1" ]] || false
}

@test "DOLT_COMMIT updates session variables and keeps dolt_diff fresh" {
    run dolt sql << SQL
SELECT DOLT_COMMIT('-a', '-m', 'Commit1');
SELECT count(*) FROM dolt_diff_test WHERE to_commit = hashof('head');
SQL

    [ $status -eq 0 ]
    # Represents that the diff table marks a change from the recent commit.
    [[ "$output" =~ "1" ]] || false
}

@test "DOLT_COMMIT updates session variables" {
    head_variable=@@dolt_repo_$$_head
    run dolt sql << SQL
SELECT DOLT_COMMIT('-a', '-m', 'Commit1');
SELECT $head_variable = HASHOF('head');
SQL

    [ $status -eq 0 ]
    [[ "$output" =~ "true" ]] || false
}

@test "DOLT_COMMIT with unstaged tables correctly gets new head root but does not overwrite working" {
    head_variable=@@dolt_repo_$$_head

    run dolt sql << SQL
CREATE TABLE test2 (
    pk int primary key
);
SELECT DOLT_ADD('test');
SELECT DOLT_COMMIT('-m', 'Commit1');
SELECT $head_variable = HASHOF('head');
SQL

    [ $status -eq 0 ]
    [[ "$output" =~ "true" ]] || false

    run dolt sql -r csv -q "select * from dolt_status;"
    [ $status -eq 0 ]
    [[ "$output" =~ 'test2,false,new table' ]] || false
}