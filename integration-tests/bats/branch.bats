#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "branch: branch --datasets lists all datasets" {
    dolt branch other
    dolt commit --allow-empty -m "empty"
    dolt tag mytag head
    dolt sql -q "create table t (c0 int)"

    run dolt branch --datasets
    [[ "$status" -eq 0 ]] || false
    [[ "$output" =~ "refs/heads/main" ]] || false
    [[ "$output" =~ "refs/heads/other" ]] || false
    [[ "$output" =~ "refs/internal/create" ]] || false
    [[ "$output" =~ "refs/tags/mytag" ]] || false
    [[ "$output" =~ "workingSets/heads/main" ]] || false
    [[ "$output" =~ "workingSets/heads/other" ]] || false
}

@test "branch: deleting a branch deletes its working set" {
    dolt checkout -b to_delete

    run dolt branch --datasets
    [[ "$output" =~ "workingSets/heads/main" ]] || false
    [[ "$output" =~ "workingSets/heads/to_delete" ]] || false

    dolt checkout main
    dolt branch -d -f to_delete

    run dolt branch --datasets
    [[ "$show_tables" -eq 0 ]] || false
    [[ ! "$output" =~ "to_delete" ]] || false
}

@test "branch: moving current working branch takes its working set" {
    dolt sql -q 'create table test (id int primary key);'
    dolt branch -m main new_main
    run dolt sql -q 'show tables'
    [[ "$output" =~ "test" ]] || false
}

@test "branch: deleting an unmerged branch with a remote" {
    mkdir -p remotes/origin
    dolt remote add origin file://./remotes/origin
    dolt sql -q "create table t1 (id int primary key);"
    dolt commit -Am "initial commit"
    dolt branch b1
    dolt branch b2
    dolt branch b3
    
    dolt push --set-upstream origin b1
    dolt push --set-upstream origin b2
    dolt push --set-upstream origin b3

    # b1 is one commit ahead of the remote
    dolt checkout b1
    dolt sql -q "create table t2 (id int primary key);"
    dolt commit -Am "new table"

    # b2 is even with the remote

    # b3 is one commit behind the remote
    dolt checkout b3
    dolt sql -q "create table t2 (id int primary key);"
    dolt commit -Am "new table"
    dolt push origin b3
    dolt reset --hard HEAD~
    
    dolt checkout main
    run dolt branch -d b1
    [ "$status" -ne 0 ]
    [[ "$output" =~ "branch 'b1' is not fully merged" ]] || false
    [[ "$output" =~ "run 'dolt branch -D b1'" ]] || false

    dolt branch -D b1
    dolt branch -d b2
    dolt branch -d b3

    run dolt branch
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "b1" ]] || false
    [[ ! "$output" =~ "b2" ]] || false
    [[ ! "$output" =~ "b3" ]] || false
}

@test "branch: deleting an unmerged branch with no remote" {
    dolt sql -q "create table t1 (id int primary key);"
    dolt commit -Am "commit 1"
    dolt sql -q "create table t2 (id int primary key);"
    dolt commit -Am "commit 2"
    dolt branch b1
    dolt branch b2
    dolt branch b3
    
    # b1 is one commit ahead of main
    dolt checkout b1
    dolt sql -q "create table t3 (id int primary key);"
    dolt commit -Am "new table"
    # two additional copies
    dolt branch b1-1
    dolt branch b1-2

    # b2 is even with main

    # b3 is one commit behind main
    dolt checkout b3
    dolt reset --hard HEAD~
    
    dolt checkout main
    run dolt branch -d b1
    [ "$status" -ne 0 ]
    [[ "$output" =~ "branch 'b1' is not fully merged" ]] || false
    [[ "$output" =~ "run 'dolt branch -D b1'" ]] || false

    dolt branch -D b1

    dolt checkout b1-1
    # this works because it's even with the checked out branch (but not with main)
    dolt branch -d b1-2

    dolt checkout main
    dolt branch -D b1-1
    dolt branch -d b2
    dolt branch -d b3

    run dolt branch
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "b1" ]] || false
    [[ ! "$output" =~ "b2" ]] || false
    [[ ! "$output" =~ "b3" ]] || false
}

