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
    dolt branch to_delete

    run dolt --branch to_delete branch --datasets
    [[ "$output" =~ "workingSets/heads/main" ]] || false
    [[ "$output" =~ "workingSets/heads/to_delete" ]] || false

    dolt branch -d -f to_delete

    run dolt branch --datasets
    [[ "$show_tables" -eq 0 ]] || false
    [[ ! "$output" =~ "to_delete" ]] || false
}

@test "branch: moving current working branch takes its working set" {
    if [ "$SQL_ENGINE" = "remote-engine" ]; then
          skip "moves main branch which is not allowed with remote server"
    fi
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
    dolt --branch b1 sql -q "create table t2 (id int primary key);"
    dolt --branch b1 commit -Am "new table"

    # b2 is even with the remote

    # b3 is one commit behind the remote
    dolt --branch b3 sql -q "create table t2 (id int primary key);"
    dolt --branch b3 commit -Am "new table"
    dolt --branch b3 push origin b3
    dolt --branch b3 reset --hard HEAD~

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
    dolt --branch b1 sql -q "create table t3 (id int primary key);"
    dolt --branch b1 commit -Am "new table"
    # two additional copies
    dolt --branch b1 branch b1-1
    dolt --branch b1 branch b1-2

    # b2 is even with main

    # b3 is one commit behind main
    dolt --branch b3 reset --hard HEAD~

    run dolt branch -d b1
    [ "$status" -ne 0 ]
    [[ "$output" =~ "branch 'b1' is not fully merged" ]] || false
    [[ "$output" =~ "run 'dolt branch -D b1'" ]] || false

    dolt branch -D b1

    # this works because it's even with the checked out branch (but not with main)
    dolt --branch b1-1 branch -d b1-2

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
    if [ "$SQL_ENGINE" = "remote-engine" ]; then
          skip "deletes main branch which is not allowed within  remote server"
    fi
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

@test "branch: print nothing on successful create" {
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

    run dolt --branch altBranch branch -m HEAD
    [ $status -eq "1" ]
    [[ "$output" == "HEAD is an invalid branch name" ]] || false
    run dolt --branch altBranch branch -m $hash
    [ $status -eq "1" ]
    [[ "$output" =~ "is an invalid branch name" ]] || false

    run dolt --branch altBranch branch -c HEAD
    [ $status -eq "1" ]
    [[ "$output" == "HEAD is an invalid branch name" ]] || false
    run dolt --branch altBranch branch -c $hash
    [ $status -eq "1" ]
    [[ "$output" =~ "is an invalid branch name" ]] || false
}

@test "branch: renaming default branch should update init.defaultbranch config" {
    if [ "$SQL_ENGINE" = "remote-engine" ]; then
          skip "renames main branch which is not allowed with remote server"
    fi
    # Set up initial default branch config
    dolt config --local --add init.defaultbranch main
    
    # Verify initial configuration
    run dolt config --local --get init.defaultbranch
    [ $status -eq 0 ]
    [[ "$output" =~ "main" ]] || false
    
    # Rename the default branch using SQL function
    dolt sql -q "CALL DOLT_BRANCH('-m', 'main', 'altmain')"
    
    # Verify the branch was renamed
    run dolt branch --show-current
    [ $status -eq 0 ]
    [[ "$output" =~ "altmain" ]] || false
    
    # The init.defaultbranch config should be updated when the default branch is renamed
    run dolt config --local --get init.defaultbranch
    [ $status -eq 0 ]
    [[ "$output" =~ "altmain" ]] || false
}

@test "branch: renaming non-default branch should not affect init.defaultbranch config" {
    # Set up initial default branch config
    dolt config --local --add init.defaultbranch main
    
    # Create a non-default branch
    dolt sql -q "CALL DOLT_BRANCH('feature')"
    
    # Verify initial configuration
    run dolt config --local --get init.defaultbranch
    [ $status -eq 0 ]
    [[ "$output" =~ "main" ]] || false
    
    # Rename the non-default branch using SQL function
    dolt sql -q "CALL DOLT_BRANCH('-m', 'feature', 'newfeature')"
    
    # Verify the branch was renamed
    run dolt branch
    [ $status -eq 0 ]
    [[ "$output" =~ "newfeature" ]] || false
    
    # Verify the old branch name is gone
    run dolt sql -r csv -q "select count(*) from dolt_branches where name='feature';"
    [ $status -eq 0 ]
    [[ "$output" =~ "0" ]] || false
    
    # The init.defaultbranch config should remain unchanged when a non-default branch is renamed
    run dolt config --local --get init.defaultbranch
    [ $status -eq 0 ]
    [[ "$output" =~ "main" ]] || false
}

@test "branch: dolt branch set upstream flag sets upstream" {
    mkdir remote
    mkdir repo1

    cd repo1
    dolt init
    dolt remote add origin file://../remote
    dolt push --set-upstream origin main

    run dolt branch testUpstream --set-upstream-to origin/main
    [ $status -eq 0 ]
    [[ "$output" =~ "branch 'testUpstream' set up to track 'origin/main'" ]] || false

    run dolt sql -q "select remote, branch from dolt_branches where name = 'testUpstream'" -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ "origin,main" ]] || false
}

@test "branch: can change upstream of existing branch with --set-upstream-to" {
    mkdir remote
    mkdir repo1

    cd repo1
    dolt init
    dolt remote add origin file://../remote
    dolt push --set-upstream origin main
    dolt branch br1 --set-upstream-to origin/main
    dolt branch other
    dolt --branch other push --set-upstream origin other

    run dolt sql -q "select remote, branch from dolt_branches where name = 'br1'" -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ "origin,main" ]] || false

    dolt branch br1 --set-upstream-to origin/other

    run dolt sql -q "select remote, branch from dolt_branches where name = 'br1'" -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ "origin,other" ]] || false
}

@test "branch: can change upstream of existing branch with --set-upstream-to and current branch is assumed" {
    mkdir remote
    mkdir repo1

    cd repo1
    dolt init
    dolt remote add origin file://../remote
    dolt push --set-upstream origin main
    dolt branch other
    dolt --branch other push --set-upstream origin other

    dolt branch --set-upstream-to origin/other

    run dolt sql -q "select remote, branch from dolt_branches where name = 'main'" -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ "origin,other" ]] || false
}

@test "branch: cannot set upstream of branches with invalid remote" {
    mkdir remote
    mkdir repo1

    cd repo1
    dolt init
    dolt remote add origin file://../remote
    run dolt branch br1 --track origin/invalid
    [ "$status" -eq 1 ]
    [[ "$output" =~ "error: branch not found: 'origin/invalid'" ]] || false

    run dolt branch main --set-upstream-to origin/invalid
    [ "$status" -eq 1 ]
    [[ "$output" =~ "error: branch not found: 'origin/invalid'" ]] || false
}

@test "branch: cannot use both --track and --set-upstream-to" {
    mkdir remote
    mkdir repo1

    cd repo1
    dolt init
    dolt remote add origin file://../remote
    dolt push --set-upstream origin main

    run dolt branch br1 --set-upstream-to origin/main --track origin/main
    [ $status -eq 1 ]
    [[ "$output" =~ "error: --set-upstream-to and --track are mutually exclusive options" ]] || false
}

@test "branch: --track sets upstream" {
    mkdir remote
    mkdir repo1

    cd repo1
    dolt init
    dolt remote add origin file://../remote
    dolt push --set-upstream origin main

    run dolt branch br1 --track origin/main
    [ $status -eq 0 ]
    [[ "$output" =~ "branch 'br1' set up to track 'origin/main'" ]] || false
}

@test "branch: can specify local branch with --track" {
    run dolt branch br1 --track main
    [ $status -eq 0 ]
    [[ "$output" =~ "branch 'br1' set up to track 'main'" ]] || false
}

@test "branch: --track presumes current branch without argument" {
    run dolt branch br1 --track
    [ $status -eq 0 ]
    [[ "$output" =~ "branch 'br1' set up to track 'main'" ]] || false
}

@test "branch: --set-upstream-to works with starting point" {
    mkdir remote
    mkdir repo1

    cd repo1
    dolt init
    dolt remote add origin file://../remote
    dolt sql -q "create table t (i int)"
    dolt commit -Am "Created a table"
    dolt push --set-upstream origin main

    # get the second to last commit hash
    hash=`dolt sql -q "select commit_hash from dolt_log where message = 'Initialize data repository'" -r csv | sed -n '2p'`
    dolt branch br1 --set-upstream-to origin/main "$hash"

    run dolt --branch br1 ls
    [ $status -eq 0 ]
    [[ "$output" =~ "No tables in working set" ]] || false
}

@test "branch: --set-upstream-to and --track presume HEAD starting point" {
    mkdir remote
    mkdir repo1

    cd repo1
    dolt init
    dolt remote add origin file://../remote

    dolt branch br1
    dolt --branch br1 commit --allow-empty -m "A new commit"
    dolt --branch br1 push --set-upstream origin main

    dolt branch setUpstream --set-upstream-to origin/main
    run dolt sql -q "select latest_commit_message from dolt_branches where name = 'setUpstream'" -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ "Initialize data repository" ]] || false

    dolt branch trackUpstream --track origin/main
    run dolt sql -q "select latest_commit_message from dolt_branches where name = 'trackUpstream'" -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ "Initialize data repository" ]] || false
}

@test "branch: --set-upstream-to and --track can set HEAD starting point" {
    mkdir remote
    mkdir repo1

    cd repo1
    dolt init
    dolt remote add origin file://../remote
    dolt branch br1
    dolt --branch br1 commit --allow-empty -m "A new commit"
    dolt --branch br1 push --set-upstream origin main

    dolt branch setUpstream --set-upstream-to origin/main HEAD
    run dolt sql -q "select latest_commit_message from dolt_branches where name = 'setUpstream'" -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ "Initialize data repository" ]] || false
    ! [[ "$output" =~ "A new commit" ]] || false
}

@test "branch: --set-upstream-to and --track cannot set branch as its own upstream" {
    run dolt branch --set-upstream-to main
    [ $status -eq 1 ]
    [[ "$output" =~ "not setting 'main' as its own upstream" ]] || false

    run dolt branch br1 --track br1
    [ $status -eq 1 ]
    [[ "$output" =~ "branch not found: 'br1'" ]] || false
}

@test "branch: --set-upstream-to and --track cannot set relative commit as upstream" {
    dolt commit --allow-empty -m "Empty commit 1"

    run dolt branch br1 --track HEAD~1
    [ "$status" -eq 1 ]
    [[ "$output" =~ "branch not found: 'HEAD~1'" ]] || false

    run dolt branch br1 --set-upstream-to HEAD~1
    [ "$status" -eq 1 ]
    [[ "$output" =~ "branch not found: 'HEAD~1'" ]] || false
}