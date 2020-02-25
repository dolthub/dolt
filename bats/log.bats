#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}

@test "dolt log on initialized repo" {
    run dolt log
    [ "$status" -eq "0" ]
    [[ "$output" =~ "Initialize data repository" ]] || false
}

@test "dolt log with -n specified" {
    dolt sql -q "create table test (pk int, c1 int, primary key(pk))"
    dolt add test
    dolt commit -m "first commit"
    run dolt log
    [ $status -eq 0 ]
    [[ "$output" =~ "first commit" ]] || false
    [[ "$output" =~ "Initialize data repository" ]] || false
    run dolt log -n 1
    [ $status -eq 0 ]
    [[ "$output" =~ "first commit" ]] || false
    [[ ! "$output" =~ "Initialize data repository" ]] || false
}

@test "dolt log on fast-forward merge commits" {
    dolt sql -q	"create table test (pk int, c1 int, primary key(pk))"
    dolt add test
    dolt commit -m "Commit1"
    dolt checkout -b test-branch
    dolt sql -q "insert into test values (0,0)"
    dolt add test
    dolt commit -m "Commit2"
    dolt checkout master
    dolt merge test-branch
    run dolt log
    [ $status -eq 0 ]
    [[ "$output" =~ "Commit1" ]] || false
    [[ "$output" =~ "Commit2" ]] || false
    [[ "$output" =~ "Initialize data repository" ]] || false
    [[ ! "$output" =~ "Merge:" ]] || false
}

@test "dolt log properly orders merge commits" {
    dolt sql -q "create table test (pk int, c1 int, primary key(pk))"
    dolt add test
    dolt commit -m "Commit1"
    dolt checkout -b test-branch
    dolt sql -q "insert into test values (0,0)"
    dolt add test
    dolt commit -m "Commit2"
    dolt checkout master
    dolt sql -q "insert into test values (1,1)"
    dolt add test
    dolt commit -m "Commit3"
    dolt merge test-branch
    run dolt log
    [ $status -eq 0 ]
    [[ "$output" =~ "Commit1" ]] || false
    [[ "$output" =~ "Commit3" ]] || false
    [[ "$output" =~ "Initialize data repository" ]] || false
    [[ ! "$output" =~ "Merge:" ]] || false
    [[ ! "$output" =~ "Commit2" ]]
    dolt add test
    dolt commit -m "MergeCommit"
    run dolt log
    [ $status -eq 0 ]
    regex='Merge:.*MergeCommit.*Commit3.*Commit2.*Commit1.*Initialize data repository'
    [[ "$output" =~ $regex ]] || false
    run dolt log -n 5
    regex='Merge:.*MergeCommit.*Commit3.*Commit2.*Commit1.*Initialize data repository'
    [[ "$output" =~ $regex ]] || false
    run dolt log -n 4
    regex='Merge:.*MergeCommit.*Commit3.*Commit2.*Commit1'
    [[ "$output" =~ $regex ]] || false
    run dolt log -n 3
    regex='Merge:.*MergeCommit.*Commit3.*Commit2'
    [[ "$output" =~ $regex ]] || false
    run dolt log -n 2
    regex='Merge:.*MergeCommit.*Commit3'
    [[ "$output" =~ $regex ]] || false
    run dolt log -n 1
    regex='Merge:.*MergeCommit.*'
    [[ "$output" =~ $regex ]] || false
}
