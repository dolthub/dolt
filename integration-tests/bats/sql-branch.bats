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
    assert_feature_version
    teardown_common
}

@test "sql-branch: CALL DOLT_BRANCH works" {
    run dolt branch
    [[ ! "$output" =~ "new_branch" ]] || false

    run dolt sql -q "CALL DOLT_BRANCH('new-branch')"
    [ $status -eq 0 ]

    # should create new branch and should not checkout the new branch
    run dolt status
    [ $status -eq 0 ]
    [[ "$output" =~ "main" ]] || false

    run dolt branch
    [ $status -eq 0 ]
    [[ "$output" =~ "new-branch" ]] || false
}

@test "sql-branch: CALL DBRANCH works" {
    run dolt branch
    [[ ! "$output" =~ "new_branch" ]] || false

    run dolt sql -q "CALL dbranch('new-branch')"
    [ $status -eq 0 ]

    # should create new branch and should not checkout the new branch
    run dolt status
    [ $status -eq 0 ]
    [[ "$output" =~ "main" ]] || false

    run dolt branch
    [ $status -eq 0 ]
    [[ "$output" =~ "new-branch" ]] || false
}

@test "sql-branch: CALL DOLT_BRANCH error cases" {
    # branches that already exist
    dolt branch existing_branch
    run dolt sql -q "CALL DOLT_BRANCH('existing_branch')"
    [ $status -eq 1 ]
    [[ "$output" =~ "fatal: A branch named 'existing_branch' already exists." ]] || false

    # empty branch
    run dolt sql -q "CALL DOLT_BRANCH('')"
    [ $status -eq 1 ]
    [[ "$output" =~ "error: cannot branch empty string" ]] || false
}

@test "sql-branch: CALL DOLT_BRANCH -c copies not current branch and stays on current branch" {
    dolt add . && dolt commit -m "0, 1, and 2 in test table"
    run dolt status
    [[ "$output" =~ "main" ]] || false

    dolt checkout -b original
    dolt sql -q "insert into test values (4);"
    dolt add .
    dolt commit -m "add 4 in original"

    dolt checkout main

    # Current branch should be still main with test table without entry 4
    run dolt sql << SQL
CALL DOLT_BRANCH('-c', 'original', 'copy');
SELECT * FROM test WHERE pk > 3;
SQL
    [ $status -eq 0 ]
    [[ ! "$output" =~ "4" ]] || false

    run dolt status
    [ $status -eq 0 ]
    [[ "$output" =~ "main" ]] || false

    run dolt checkout copy
    [ $status -eq 0 ]

    run dolt sql -q "SELECT * FROM test WHERE pk > 3;"
    [[ "$output" =~ "4" ]] || false
}

@test "sql-branch: CALL DOLT_BRANCH -c throws error on error cases" {
    run dolt status
    [[ "$output" =~ "main" ]] || false

    # branch copying from is empty
    run dolt sql -q "CALL DOLT_BRANCH('-c','','copy')"
    [ $status -eq 1 ]
    [[ "$output" =~ "error: cannot branch empty string" ]] || false

    # branch copying to is empty
    run dolt sql -q "CALL DOLT_BRANCH('-c','main','')"
    [ $status -eq 1 ]
    [[ "$output" =~ "error: cannot branch empty string" ]] || false

    dolt branch 'existing_branch'
    run dolt branch
    [[ "$output" =~ "main" ]] || false
    [[ "$output" =~ "existing_branch" ]] || false
    [[ ! "$output" =~ "original" ]] || false

    # branch copying from that don't exist
    run dolt sql -q "CALL DOLT_BRANCH('-c', 'original', 'copy');"
    [ $status -eq 1 ]
    [[ "$output" =~ "fatal: A branch named 'original' not found" ]] || false

    # branch copying to that exists
    run dolt sql -q "CALL DOLT_BRANCH('-c', 'main', 'existing_branch');"
    [ $status -eq 1 ]
    [[ "$output" =~ "fatal: A branch named 'existing_branch' already exists." ]] || false
}

@test "sql-branch: CALL DOLT_BRANCH works as insert into dolt_branches table" {
    dolt add . && dolt commit -m "1, 2, and 3 in test table"

    run dolt sql -q "SELECT hash FROM dolt_branches WHERE name='main';"
    [ $status -eq 0 ]
    mainhash=$output

    dolt sql -q "CALL DOLT_BRANCH('feature-branch');"
    run dolt sql -q "SELECT hash FROM dolt_branches WHERE name='feature-branch';"
    [ $status -eq 0 ]
    [ "$output" = "$mainhash" ]
}

@test "sql-branch: CALL DOLT_BRANCH to rename and delete" {
    dolt add . && dolt commit -m "1, 2, and 3 in test table"
    dolt branch new_branch

    run dolt sql -q "CALL DOLT_BRANCH('-m', 'new_branch', 'changed');"
    [ $status -eq 0 ]

    run dolt sql -q "CALL DOLT_BRANCH('-d', 'changed');"
    [ $status -eq 0 ]

    dolt branch branch_with_unpushed_commit
    dolt checkout branch_with_unpushed_commit
    dolt commit --allow-empty -am 'empty commit'
    dolt checkout main

    run dolt sql -q "CALL DOLT_BRANCH('-d', 'branch_with_unpushed_commit');"
    [ $status -eq 1 ]
    [[ "$output" =~ "not fully merged" ]] || false

    run dolt sql -q "CALL DOLT_BRANCH('-D', 'branch_with_unpushed_commit');"
    [ $status -eq 0 ]
}

@test "sql-branch: CALL DOLT_BRANCH -d error cases" {
    dolt add . && dolt commit -m "1, 2, and 3 in test table"
    dolt branch new_branch

    # Attempting to delete the db's default branch results in an error
    run dolt sql -q "CALL DOLT_BRANCH('-D', 'main');"
    [ $status -eq 1 ]
    [[ "$output" =~ "Cannot delete checked out branch 'main'" ]] || false
}

@test "sql-branch: CALL DOLT_BRANCH -d -r to remove remote branch" {
    mkdir -p remotes/origin
    dolt remote add origin file://./remotes/origin
    dolt sql -q "create table t1 (id int primary key);"
    dolt commit -Am "initial commit"
    dolt sql -q "CALL DOLT_BRANCH('b1')"

    dolt push --set-upstream origin b1

    run dolt branch -r
    [ "$status" -eq 0 ]
    [[ "$output" =~ "origin/b1" ]] || false

    dolt sql -q "CALL DOLT_BRANCH('-D', '-r', 'origin/b1')"

    run dolt branch -r
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "origin/b1" ]] || false
}

@test "sql-branch: CALL DOLT_BRANCH -m on session active branch (dolt sql)" {
    dolt branch other
    run dolt sql -q "call dolt_checkout('other'); call dolt_branch('-m', 'other', 'newOther'); select active_branch();"
    [ $status -eq 0 ]
    [[ "$output" =~ "newOther" ]] || false
    run dolt branch
    [ $status -eq 0 ]
    [[ "$output" =~ "newOther" ]] || false
    [[ "$output" =~ "main" ]] || false
    [[ ! "$output" =~ "other" ]] || false
}