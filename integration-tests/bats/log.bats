#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "log: on initialized repo" {
    run dolt log
    [ "$status" -eq "0" ]
    [[ "$output" =~ "Initialize data repository" ]] || false
}

@test "log: with -n specified" {
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

@test "log: on fast-forward merge commits" {
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

@test "log: properly orders merge commits" {
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
    [[ ! "$output" =~ "Commit2" ]] || false
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

@test "log: Properly throws an error when neither a valid commit hash nor a valid table are passed" {
    run dolt log notvalid
    [ "$status" -eq "1" ]
    [[ "$output" =~ "error: table notvalid does not exist" ]] || false
}

@test "log: Log on a table has basic functionality" {
    dolt sql -q "create table test (pk int PRIMARY KEY)"
    dolt commit -am "first commit"

    run dolt log test
    [ $status -eq 0 ]
    [[ "$output" =~ "first commit" ]] || false
    [[ ! "$output" =~ "Initialize data repository" ]] || false

    dolt sql -q "INSERT INTO test VALUES (1)"
    dolt commit -am "second commit"

    run dolt log test
    [ $status -eq 0 ]
    [[ "$output" =~ "second commit" ]] || false
    [[ "$output" =~ "first commit" ]] || false
    [[ ! "$output" =~ "Initialize data repository" ]] || false

    dolt sql -q "create table test2 (pk int PRIMARY KEY)"
    dolt commit -am "third commit"

    # Validate we only look at the right commits
    run dolt log test
    [ $status -eq 0 ]
    [[ "$output" =~ "second commit" ]] || false
    [[ "$output" =~ "first commit" ]] || false
    [[ ! "$output" =~ "Initialize data repository" ]] || false
    [[ ! "$output" =~ "third commit" ]] || false
}

@test "log: Log on a table works with -n" {
    dolt sql -q "create table test (pk int PRIMARY KEY)"
    dolt commit -am "first commit"

    run dolt log -n 1 test
    [ $status -eq 0 ]
    [[ "$output" =~ "first commit" ]] || false

    dolt sql -q "INSERT INTO test VALUES (1)"
    dolt commit -am "second commit"

    run dolt log -n 2 test
    [ $status -eq 0 ]
    [[ "$output" =~ "second commit" ]] || false
    [[ "$output" =~ "first commit" ]] || false

    dolt sql -q "create table test2 (pk int PRIMARY KEY)"
    dolt commit -am "third commit"

    dolt sql -q "insert into test2 values (4)"
    dolt commit -am "fourth commit"

    # Validate we only look at the right commits
    run dolt log test -n 1
    [ $status -eq 0 ]
    [[ "$output" =~ "second commit" ]] || false
    [[ ! "$output" =~ "first commit" ]] || false
    [[ ! "$output" =~ "Initialize data repository" ]] || false
    [[ ! "$output" =~ "third commit" ]] || false
    [[ ! "$output" =~ "fourth commit" ]] || false

    run dolt log test -n 100
    [ $status -eq 0 ]
    [[ "$output" =~ "second commit" ]] || false
    [[ "$output" =~ "first commit" ]] || false
    [[ ! "$output" =~ "Initialize data repository" ]] || false
    [[ ! "$output" =~ "third commit" ]] || false
    [[ ! "$output" =~ "fourth commit" ]] || false
}

@test "log: Log on a table works with merge commits" {
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

    run dolt log test
    [ $status -eq 0 ]
    [[ "$output" =~ "Commit1" ]] || false
    [[ "$output" =~ "Commit3" ]] || false
    [[ ! "$output" =~ "Initialize data repository" ]] || false
    [[ ! "$output" =~ "Merge:" ]] || false
    [[ ! "$output" =~ "Commit2" ]]

    dolt add test
    dolt commit -m "MergeCommit"

    run dolt log test
    [ $status -eq 0 ]
    [[ "$output" =~ "MergeCommit" ]] || false
    [[ "$output" =~ "Merge:" ]] || false
    [[ "$output" =~ "Commit1" ]] || false
    [[ "$output" =~ "Commit3" ]] || false
    [[ "$output" =~ "Commit2" ]] || false
    [[ ! "$output" =~ "Initialize data repository" ]] || false
}

@test "log: dolt log with ref and table" {
    dolt sql -q "create table test (pk int, c1 int, primary key(pk))"
    dolt add test
    dolt commit -m "Commit1"
    dolt checkout -b test-branch
    dolt sql -q "insert into test values (0,0)"
    dolt add test
    dolt commit -m "Commit2"
    dolt checkout master

    run dolt log test-branch test
    [ $status -eq 0 ]
    [[ "$output" =~ "Commit2" ]] || false
    [[ "$output" =~ "Commit1" ]] || false
}

@test "log: dolt log with table deleted between revisions" {
    dolt sql -q "create table test (pk int, c1 int, primary key(pk))"
    dolt add test
    dolt commit -m "Commit1"

    dolt sql -q "DROP TABLE test"
    dolt add test
    dolt commit -m "Commit 2"

    run dolt log test
    [ $status -eq 1 ]
    [[ "$output" =~ "error: table test does not exist" ]] || false

    dolt sql -q "create table test (pk int, c1 int, primary key(pk))"
    dolt add test
    dolt commit -m "Commit3"

    run dolt log test
    [[ "$output" =~ "Commit3" ]] || false
    [[ "$output" =~ "Commit1" ]] || false
    ! [[ "$output" =~ "Commit2" ]] || false
}

@test "log: --merges, --parents, --min-parents option" {
    dolt sql -q "create table test (pk int, c1 int, primary key(pk))"
    dolt add -A
    dolt commit -m "Created table"
    dolt checkout -b branch1
    dolt sql -q "insert into test values (0,0)"
    dolt add -A
    dolt commit -m "Inserted 0,0"
    dolt checkout master
    dolt checkout -b branch2
    dolt sql -q "insert into test values (1,1)"
    dolt add -A
    dolt commit -m "Inserted 1,1"

    dolt checkout master
    # Should be fast-forward 
    dolt merge branch1
    # An actual merge
    dolt merge branch2
    dolt commit -m "Merged branch2"

    # Only shows merge commits
    run dolt log --merges
    [ $status -eq 0 ]
    [[ "$output" =~ "Merged" ]] || false
    [[ ! "$output" =~ "0,0" ]] || false

    # Only shows merge commits but use --min-parents 2
    run dolt log --min-parents 2
    [ $status -eq 0 ]
    [[ "$output" =~ "Merged" ]] || false
    [[ ! "$output" =~ "0,0" ]] || false

    # Show everything but the first commit
    run dolt log --min-parents 1
    [ $status -eq 0 ]
    [[ "$output" =~ "0,0" ]] || false
    [[ ! "$output" =~ "Initialize data repository" ]] || false
    
    # each commit gets its parents in the log
    run dolt log --parents
    [ $status -eq 0 ]
    regex='commit .* .*\n'
    [[ "$output" =~ $regex ]] || false
}
