#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common

    TMPDIRS=$(pwd)/tmpdirs
    mkdir -p $TMPDIRS/{rem1,repo1}

    # repo1 -> rem1 -> repo2
    cd $TMPDIRS/repo1
    dolt init
    dolt remote add origin file://../rem1
    dolt remote add test-remote file://../rem1
    dolt push origin main

    cd $TMPDIRS
    dolt clone file://rem1 repo2
    cd $TMPDIRS/repo2
    dolt log
    dolt remote add test-remote file://../rem1

    # table and comits only present on repo1, rem1 at start
    cd $TMPDIRS/repo1
    dolt sql -q "create table t1 (a int primary key, b int)"
    dolt add .
    dolt commit -am "First commit"
    dolt sql -q "insert into t1 values (0,0)"
    dolt commit -am "Second commit"
    cd $TMPDIRS
}

teardown() {
    teardown_common
    rm -rf $TMPDIRS
    cd $BATS_TMPDIR
}

@test "push: push origin" {
    cd repo1
    dolt push origin main

    cd ../repo2
    dolt pull origin
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "push: push custom remote" {
    cd repo1
    dolt push test-remote main

    cd ../repo2
    dolt pull origin
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "push: push infers correct remote" {
    cd repo1
    dolt push main    # should push to origin

    cd ../repo2
    dolt pull origin
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "push: push active branch" {
    skip "upstream state lost between sessions"
    cd repo1
    dolt push origin

    cd ../repo2
    dolt pull origin
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "push: push feature branch" {
    cd repo1
    dolt checkout -b feature
    dolt push origin feature

    cd ../repo2
    dolt fetch origin feature
    dolt checkout feature
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "push: can set upstream branch with different name" {
    cd repo1
    dolt checkout -b other
    dolt push -u origin other:remote-other

    run dolt branch -r
    [ "$status" -eq 0 ]
    [[ "$output" =~ "remote-other" ]] || false
}

@test "push: push aborts if nothing specified, upstream has different name" {
    cd repo1
    dolt checkout -b other
    dolt push -u origin other:remote-other

    run dolt push
    [ "$status" -eq 1 ]
    [[ "$output" =~ "the upstream branch of your current branch does not match the name of your current branch" ]] || false
}

@test "push: push --set-upstream persists" {
    cd repo1
    dolt checkout -b other
    run dolt push
    [ "$status" -eq 1 ]
    [[ "$output" =~ "The current branch other has no upstream branch." ]] || false

    dolt push -u origin other
    # upstream should be set still
    run dolt push
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Everything up-to-date" ]] || false
    [[ ! "$output" =~ "The current branch main has no upstream branch." ]] || false
}

@test "push: push without --set-upstream persists when push.autoSetupRemote is set to true" {
    cd repo1
    dolt checkout -b other
    run dolt push
    [ "$status" -eq 1 ]
    [[ "$output" =~ "The current branch other has no upstream branch." ]] || false

    dolt config --local --add push.autoSetUpRemote true
    dolt push
    # upstream should be set still
    run dolt push
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Everything up-to-date" ]] || false
    [[ ! "$output" =~ "The current branch main has no upstream branch." ]] || false
}

@test "push: push without --set-upstream persists when push.autoSetupRemote is set to all capital TRUE" {
    cd repo1
    dolt checkout -b other
    run dolt push
    [ "$status" -eq 1 ]
    [[ "$output" =~ "The current branch other has no upstream branch." ]] || false

    dolt config --local --add push.autoSetUpRemote TRUE
    dolt push
    # upstream should be set still
    run dolt push
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Everything up-to-date" ]] || false
    [[ ! "$output" =~ "The current branch main has no upstream branch." ]] || false
}

@test "push: push --force flag" {
    cd repo2
    dolt sql -q "create table t2 (a int)"
    dolt add .
    dolt commit -am "commit to override"
    dolt push origin main

    cd ../repo1
    run dolt push origin main
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Updates were rejected because the tip of your current branch is behind" ]] || false

    dolt push --force origin main
}

@test "push: push to unknown remote" {
    cd repo1
    run dolt push unknkown main
    [ "$status" -eq 1 ]
    [[ "$output" =~ "unknown remote: 'unknkown'" ]] || false
}

@test "push: push unknown branch" {
    cd repo1
    run dolt push origin unknown
    [ "$status" -eq 1 ]
    [[ "$output" =~ "refspec not found: 'unknown'" ]] || false
}

@test "push: not specifying a branch throws an error" {
    cd repo1
    run dolt push -u origin
    [ "$status" -eq 1 ]
    [[ "$output" =~ "--set-upstream requires <remote> and <refspec> params" ]] || false
}

@test "push: pushing empty branch does not panic" {
    cd repo1
    run dolt push origin ''
    [ "$status" -eq 1 ]
    [[ "$output" =~ "invalid ref spec: ''" ]] || false
}
