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
    run dolt branch --show-current
    [[ "$output" =~ "new_main" ]] || false
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

@test "branch: attempting to delete the currently checked out branch results in an error" {
    run dolt branch -D main
    [ "$status" -ne 0 ]
    [[ "$output" =~ "Cannot delete checked out branch 'main'" ]] || false
}

@test "branch: supplying multiple directives results in an error" {
    run dolt branch -m -c main main2
    [ "$status" -ne 0 ]
    [[ "$output" =~ "Must specify exactly one of --move/-m, --copy/-c, --delete/-d, -D, --show-current, or --list." ]] || false
}

@test "branch: -a can only be supplied when listing branches" {
    dolt branch -a

    dolt branch -a --list main

    dolt branch test

    run dolt branch -a -d test
    [ "$status" -ne 0 ]
    [[ "$output" =~ "--all/-a can only be supplied when listing branches, not when deleting branches" ]] || false

    run dolt branch -a -c copy
    [ "$status" -ne 0 ]
    [[ "$output" =~ "--all/-a can only be supplied when listing branches, not when copying branches" ]] || false

    run dolt branch -a -m main new
    [ "$status" -ne 0 ]
    [[ "$output" =~ "--all/-a can only be supplied when listing branches, not when moving branches" ]] || false

    run dolt branch -a new
    [ "$status" -ne 0 ]
    [[ "$output" =~ "--all/-a can only be supplied when listing branches, not when creating branches" ]] || false
}

@test "branch: -v can only be supplied when listing branches" {
    dolt branch -v

    dolt branch -v --list main

    dolt branch test

    run dolt branch -v -d test
    [ "$status" -ne 0 ]
    [[ "$output" =~ "--verbose/-v can only be supplied when listing branches, not when deleting branches" ]] || false

    run dolt branch -v -c copy
    [ "$status" -ne 0 ]
    [[ "$output" =~ "--verbose/-v can only be supplied when listing branches, not when copying branches" ]] || false

    run dolt branch -v -m main new
    [ "$status" -ne 0 ]
    [[ "$output" =~ "--verbose/-v can only be supplied when listing branches, not when moving branches" ]] || false

    run dolt branch -v new
    [ "$status" -ne 0 ]
    [[ "$output" =~ "--verbose/-v can only be supplied when listing branches, not when creating branches" ]] || false
}

@test "branch: -r can only be supplied when listing or deleting branches" {
    dolt branch -r

    dolt branch -r --list main

    dolt branch test

    run dolt branch -r -c copy
    [ "$status" -ne 0 ]
    [[ "$output" =~ "--remote/-r can only be supplied when listing or deleting branches, not when copying branches" ]] || false

    run dolt branch -r -m main new
    [ "$status" -ne 0 ]
    [[ "$output" =~ "--remote/-r can only be supplied when listing or deleting branches, not when moving branches" ]] || false

    run dolt branch -r new
    [ "$status" -ne 0 ]
    [[ "$output" =~ "--remote/-r can only be supplied when listing or deleting branches, not when creating branches" ]] || false
}

@test "branch: -- escapes arg parsing" {
    # use -- to turn off arg parsing for the remaining arguments and treat
    # them all as position arguments
    dolt branch -- -b

    # verify that the '-b' branch was created successfully
    run dolt sql -r csv -q "select count(*) from dolt_branches where name='-b';"
    [ $status -eq 0 ]
    [[ $output =~ "1" ]] || false

    # verify that we can use -- to delete the -b branch
    dolt branch -d -f  -- -b
    run dolt sql -r csv -q "select count(*) from dolt_branches where name='-b';"
    [ $status -eq 0 ]
    [[ $output =~ "0" ]] || false
}

@test "branch: print nothing on successfull create" {
    run dolt branch newbranch1 HEAD
    [ $status -eq "0" ]
    [[ $output == "" ]] || false

    # Get the current commit - bare.
    run dolt merge-base HEAD HEAD
    [ $status -eq "0" ]
    hash="$output"
   
    run dolt branch newbranch2 $hash
    [ $status -eq "0" ]
    [[ $output == "" ]] || false
}

@test "branch: don't allow branch creation with HEAD or a commit id as a name" {
    # Get the current commit - bare.
    run dolt merge-base HEAD HEAD
    [ $status -eq "0" ]
    hash="$output"

    run dolt branch HEAD $hash
    [ $status -eq "1" ]
    [[ "$output" == "HEAD is an invalid branch name" ]] || false

    run dolt branch $hash HEAD
    [ $status -eq "1" ]
    [[ "$output" =~ "is an invalid branch name" ]] || false
    [[ ! "$output" =~ "HEAD" ]] || false

    dolt branch altBranch HEAD

    run dolt branch -m altBranch HEAD
    [ $status -eq "1" ]
    [[ "$output" == "HEAD is an invalid branch name" ]] || false
    run dolt branch -m altBranch $hash
    [ $status -eq "1" ]
    [[ "$output" =~ "is an invalid branch name" ]] || false

    run dolt branch -c altBranch HEAD
    [ $status -eq "1" ]
    [[ "$output" == "HEAD is an invalid branch name" ]] || false
    run dolt branch -c altBranch $hash
    [ $status -eq "1" ]
    [[ "$output" =~ "is an invalid branch name" ]] || false

    dolt checkout altBranch

    run dolt branch -m HEAD
    [ $status -eq "1" ]
    [[ "$output" == "HEAD is an invalid branch name" ]] || false
    run dolt branch -m $hash
    [ $status -eq "1" ]
    [[ "$output" =~ "is an invalid branch name" ]] || false

    run dolt branch -c HEAD
    [ $status -eq "1" ]
    [[ "$output" == "HEAD is an invalid branch name" ]] || false
    run dolt branch -c $hash
    [ $status -eq "1" ]
    [[ "$output" =~ "is an invalid branch name" ]] || false
}
