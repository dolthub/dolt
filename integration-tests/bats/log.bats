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
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Initialize data repository" ]] || false
}

@test "log: basic log" {
    dolt sql -q "create table testtable (pk int PRIMARY KEY)"
    dolt add .
    dolt commit -m "commit 1"
    dolt commit	--allow-empty -m "commit 2"
    dolt commit	--allow-empty -m "commit 3"
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "commit 1" ]] || false
    [[ "$output" =~ "commit 2" ]] || false
    [[ "$output" =~ "commit 3" ]] || false
}

@test "log: log respects branches" {
    if [ "$SQL_ENGINE" = "remote-engine" ]; then
      skip "needs checkout which is unsupported for remote-engine"
    fi

    dolt branch branch1
    dolt commit --allow-empty -m "commit 1 MAIN"
    dolt commit	--allow-empty -m "commit 2 MAIN"
    dolt commit	--allow-empty -m "commit 3 MAIN"
    run dolt log
    [ $status -eq 0 ]
    [[ "$output" =~ "MAIN" ]] || false
    [[ ! "$output" =~ "BRANCH1" ]] || false
    dolt checkout branch1
    dolt commit	--allow-empty -m "commit 1 BRANCH1"
    dolt commit --allow-empty -m "commit 2 BRANCH1"
    dolt commit --allow-empty -m "commit 3 BRANCH1"
    run	dolt log
    [ $status -eq 0 ]
    [[ ! "$output" =~ "MAIN" ]] || false
    [[ "$output" =~ "BRANCH1" ]] || false
    dolt checkout main
    run	dolt log
    [ $status -eq 0 ]
    [[ "$output" =~ "MAIN" ]] || false
    [[ ! "$output" =~ "BRANCH1" ]] || false
}

@test "log: two and three dot log" {
    if [ "$SQL_ENGINE" = "remote-engine" ]; then
      skip "needs checkout which is unsupported for remote-engine"
    fi

    dolt sql -q "create table testtable (pk int PRIMARY KEY)"
    dolt add .
    dolt commit -m "commit 1 MAIN"
    dolt commit	--allow-empty -m "commit 2 MAIN"
    dolt checkout -b branchA
    dolt commit	--allow-empty -m "commit 1 BRANCHA"
    dolt commit --allow-empty -m "commit 2 BRANCHA"
    dolt checkout -b branchB
    dolt commit --allow-empty -m "commit 1 BRANCHB"
    dolt checkout branchA
    dolt commit --allow-empty -m "commit 3 BRANCHA"

    run dolt log branchA
    [ $status -eq 0 ]
    [[ "$output" =~ "MAIN" ]] || false
    [[ "$output" =~ "BRANCHA" ]] || false
    run dolt log main 
    [ $status -eq 0 ]
    [[ "$output" =~ "MAIN" ]] || false
    [[ ! "$output" =~ "BRANCHA" ]] || false
    dolt checkout main
    dolt commit	--allow-empty -m "commit 3 AFTER"
    
    # # # # # # # # # # # # # # # # # # # # # # #
    #                                           #
    #                         1B (branchB)      #
    #                        /                  #
    #                  1A - 2A - 3A (branchA)   #
    #                 /                         #
    # (init) - 1M - 2M - 3M (main)              #
    #                                           #
    # # # # # # # # # # # # # # # # # # # # # # # 
    
    # Valid two dot
    run dolt log main..branchA
    [ $status -eq 0 ]
    [[ ! "$output" =~ "MAIN" ]] || false
    [[ ! "$output" =~ "AFTER" ]] || false
    [[ "$output" =~ "BRANCHA" ]] || false
    run dolt log ^main branchA
    [ $status -eq 0 ]
    [[ ! "$output" =~ "MAIN" ]] || false
    [[ ! "$output" =~ "AFTER" ]] || false
    [[ "$output" =~ "BRANCHA" ]] || false
    run dolt log branchA ^main
    [ $status -eq 0 ]
    [[ ! "$output" =~ "MAIN" ]] || false
    [[ ! "$output" =~ "AFTER" ]] || false
    [[ "$output" =~ "BRANCHA" ]] || false
    run dolt log branchA --not main
    [ $status -eq 0 ]
    [[ ! "$output" =~ "MAIN" ]] || false
    [[ ! "$output" =~ "AFTER" ]] || false
    [[ "$output" =~ "BRANCHA" ]] || false
    run dolt log branchA..main
    [ $status -eq 0 ]
    [[ ! "$output" =~ "MAIN" ]] || false
    [[ "$output" =~ "AFTER" ]] || false
    [[ ! "$output" =~ "BRANCHA" ]] || false
    run dolt log main..main
    [ $status -eq 0 ]
    
    run dolt log main^..branchA
    [ $status -eq 0 ]
    [[ ! "$output" =~ "MAIN" ]] || false
    [[ ! "$output" =~ "AFTER" ]] || false
    [[ "$output" =~ "BRANCHA" ]] || false
    run dolt log ^main^ branchA
    [ $status -eq 0 ]
    [[ ! "$output" =~ "MAIN" ]] || false
    [[ ! "$output" =~ "AFTER" ]] || false
    [[ "$output" =~ "BRANCHA" ]] || false
    run dolt log branchA --not main^
    [ $status -eq 0 ]
    [[ ! "$output" =~ "MAIN" ]] || false
    [[ ! "$output" =~ "AFTER" ]] || false
    [[ "$output" =~ "BRANCHA" ]] || false
    run dolt log ^main ^branchA
    [ $status -eq 0 ]

    # Valid three dot
    run dolt log main...branchA
    [ $status -eq 0 ]
    [[ ! "$output" =~ "MAIN" ]] || false
    [[ "$output" =~ "AFTER" ]] || false
    [[ "$output" =~ "BRANCHA" ]] || false
    run dolt log branchA...main
    [ $status -eq 0 ]
    [[ ! "$output" =~ "MAIN" ]] || false
    [[ "$output" =~ "AFTER" ]] || false
    [[ "$output" =~ "BRANCHA" ]] || false
    run dolt log main branchA --not $(dolt merge-base main branchA)
    [ $status -eq 0 ]
    [[ ! "$output" =~ "MAIN" ]] || false
    [[ "$output" =~ "AFTER" ]] || false
    [[ "$output" =~ "BRANCHA" ]] || false
    run dolt log branchB...branchA
    [ $status -eq 0 ]
    [[ ! "$output" =~ "MAIN" ]] || false
    [[ ! "$output" =~ "AFTER" ]] || false
    [[ "$output" =~ "BRANCHA" ]] || false
    [[ "$output" =~ "BRANCHB" ]] || false

    # Multiple refs
    run dolt log branchB branchA
    [ $status -eq 0 ]
    [[ "$output" =~ "MAIN" ]] || false
    [[ ! "$output" =~ "AFTER" ]] || false
    [[ "$output" =~ "BRANCHA" ]] || false
    [[ "$output" =~ "BRANCHB" ]] || false
    run dolt log main branchA
    [ $status -eq 0 ]
    [[ "$output" =~ "MAIN" ]] || false
    [[ "$output" =~ "AFTER" ]] || false
    [[ "$output" =~ "BRANCHA" ]] || false
    [[ ! "$output" =~ "BRANCHB" ]] || false
    run dolt log main branchB branchA
    [ $status -eq 0 ]
    [[ "$output" =~ "MAIN" ]] || false
    [[ "$output" =~ "AFTER" ]] || false
    [[ "$output" =~ "BRANCHA" ]] || false
    [[ "$output" =~ "BRANCHB" ]] || false
    run dolt log branchB main ^branchA
    [ $status -eq 0 ]
    [[ ! "$output" =~ "MAIN" ]] || false
    [[ "$output" =~ "AFTER" ]] || false
    [[ ! "$output" =~ "BRANCHA" ]] || false
    [[ "$output" =~ "BRANCHB" ]] || false
    run dolt log branchB main --not branchA
    [ $status -eq 0 ]
    [[ ! "$output" =~ "MAIN" ]] || false
    [[ "$output" =~ "AFTER" ]] || false
    [[ ! "$output" =~ "BRANCHA" ]] || false
    [[ "$output" =~ "BRANCHB" ]] || false
    run dolt log branchB main --not branchA --oneline
    [ $status -eq 0 ]
    [[ ! "$output" =~ "MAIN" ]] || false
    [[ "$output" =~ "AFTER" ]] || false
    [[ ! "$output" =~ "BRANCHA" ]] || false
    [[ "$output" =~ "BRANCHB" ]] || false
    run dolt log branchB main ^branchA ^main
    [ $status -eq 0 ]
    [[ ! "$output" =~ "MAIN" ]] || false
    [[ ! "$output" =~ "AFTER" ]] || false
    [[ ! "$output" =~ "BRANCHA" ]] || false
    [[ "$output" =~ "BRANCHB" ]] || false
    run dolt log branchB main --not branchA main
    [ $status -eq 0 ]
    [[ ! "$output" =~ "MAIN" ]] || false
    [[ ! "$output" =~ "AFTER" ]] || false
    [[ ! "$output" =~ "BRANCHA" ]] || false
    [[ "$output" =~ "BRANCHB" ]] || false
    run dolt log branchB main --not branchA main --oneline
    [ $status -eq 0 ]
    [[ ! "$output" =~ "MAIN" ]] || false
    [[ ! "$output" =~ "AFTER" ]] || false
    [[ ! "$output" =~ "BRANCHA" ]] || false
    [[ "$output" =~ "BRANCHB" ]] || false

    # Invalid
    run dolt log main..branchA main
    [ $status -eq 1 ]
     run dolt log main main..branchA
    [ $status -eq 1 ]
    run dolt log main...branchA main
    [ $status -eq 1 ]
     run dolt log main main...branchA
    [ $status -eq 1 ]
    run dolt log testtable ^main
    [ $status -eq 1 ]
    run dolt log main..branchA --not main
    [ $status -eq 1 ]
    run dolt log main..branchA --not ^main
    [ $status -eq 1 ]
    run dolt log main...branchA --not main
    [ $status -eq 1 ]
    run dolt log main...branchA --not ^main
    [ $status -eq 1 ]
}

@test "log: branch name and table name are the same" {
    if [ "$SQL_ENGINE" = "remote-engine" ]; then
      skip "needs checkout which is unsupported for remote-engine"
    fi

    dolt commit --allow-empty -m "commit 1 MAIN"
    dolt commit	--allow-empty -m "commit 2 MAIN"
    dolt checkout -b myname
    dolt sql -q "create table myname (pk int PRIMARY KEY)"
    dolt add .
    dolt commit -m "commit 1 BRANCH1"
    dolt commit --allow-empty -m "commit 2 BRANCH1"
    dolt commit --allow-empty -m "commit 3 BRANCH1"
    dolt checkout main
    dolt sql -q "create table main (pk int PRIMARY KEY)"
    dolt add .
    dolt commit -m "commit 3 MAIN"
    dolt checkout -b newBranch
    dolt commit --allow-empty -m "commit 2 BRANCH2"
    dolt checkout myname

    # Should default to branch name if one argument provided
    run dolt log myname
    [ $status -eq 0 ]
    [[ "$output" =~ "MAIN" ]] || false
    [[ "$output" =~ "BRANCH1" ]] || false

    # Should default to first argument as branch name, second argument as table name (if table exists) if two arguments provided
    run dolt log myname myname
    [ $status -eq 0 ]
    [[ ! "$output" =~ "MAIN" ]] || false
    [[ "$output" =~ "BRANCH1" ]] || false

    # Table main does not exist
    run dolt log main -- main
    [ $status -eq 0 ]

    # Table main exists on different branch
    run dolt log main main
    [ $status -eq 0 ]
    [[ "$output" =~ "MAIN" ]] || false
    [[ ! "$output" =~ "BRANCH1" ]] || false

    run dolt log newBranch newBranch
    [ $status -eq 1 ]
    [[ "$output" =~ "error: table newBranch does not exist" ]] || false
}

@test "log: branch with multiple tables" {
    if [ "$SQL_ENGINE" = "remote-engine" ]; then
      skip "needs checkout which is unsupported for remote-engine"
    fi

    dolt sql -q "create table test (pk int PRIMARY KEY)"
    dolt add .
    dolt commit -m "created table test"
    dolt sql -q "create table test2 (pk int PRIMARY KEY)"
    dolt add .
    dolt commit -m "created table test2"
    dolt checkout -b b1
    dolt sql -q "insert into test values (0)"
    dolt add .
    dolt commit -m "inserted 0 into test"
    dolt sql -q "create table test3 (pk int PRIMARY KEY)"
    dolt add .
    dolt commit -m "created table test3"
    dolt checkout -b b2
    dolt sql -q "insert into test2 values (1)"
    dolt add .
    dolt commit -m "inserted 1 into test2"
    dolt sql -q "create table test4 (pk int PRIMARY KEY)"
    dolt add .
    dolt commit -m "created table test4"

    run dolt log b1 test
    [ $status -eq 0 ]
    [[ "$output" =~ "created table test" ]] || false
    [[ "$output" =~ "inserted 0 into test" ]] || false
    [[ ! "$output" =~ "created table test2" ]] || false
    [[ ! "$output" =~ "created table test3" ]] || false
    [[ ! "$output" =~ "created table test4" ]] || false

    run dolt log b1 test test2
    [ $status -eq 0 ]
    [[ "$output" =~ "created table test" ]] || false
    [[ "$output" =~ "created table test2" ]] || false
    [[ "$output" =~ "inserted 0 into test" ]] || false
    [[ ! "$output" =~ "created table test3" ]] || false
    [[ ! "$output" =~ "created table test4" ]] || false

    run dolt log b2 b1 test test2
    [ $status -eq 0 ]
    [[ "$output" =~ "created table test" ]] || false
    [[ "$output" =~ "created table test2" ]] || false
    [[ "$output" =~ "inserted 0 into test" ]] || false
    [[ "$output" =~ "inserted 1 into test2" ]] || false
    [[ ! "$output" =~ "created table test3" ]] || false
    [[ ! "$output" =~ "created table test4" ]] || false

    run dolt log b2 test b1 test2
    [ $status -eq 1 ]
    [[ "$output" =~ "error: table b1 does not exist" ]] || false
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
    if [ "$SQL_ENGINE" = "remote-engine" ]; then
      skip "needs checkout which is unsupported for remote-engine"
    fi

    dolt sql -q	"create table test (pk int, c1 int, primary key(pk))"
    dolt add test
    dolt commit -m "Commit1"
    dolt checkout -b test-branch
    dolt sql -q "insert into test values (0,0)"
    dolt add test
    dolt commit -m "Commit2"
    dolt checkout main
    dolt merge test-branch
    run dolt log
    [ $status -eq 0 ]
    [[ "$output" =~ "Commit1" ]] || false
    [[ "$output" =~ "Commit2" ]] || false
    [[ "$output" =~ "Initialize data repository" ]] || false
    [[ ! "$output" =~ "Merge:" ]] || false
}

@test "log: properly orders merge commits" {
    if [ "$SQL_ENGINE" = "remote-engine" ]; then
      skip "needs checkout which is unsupported for remote-engine"
    fi

    dolt sql -q "create table test (pk int, c1 int, primary key(pk))"
    dolt add test
    dolt commit -m "Commit1"
    dolt checkout -b test-branch
    dolt sql -q "insert into test values (0,0)"
    dolt add test
    dolt commit -m "Commit2"
    dolt checkout main
    dolt sql -q "insert into test values (1,1)"
    dolt add test
    dolt commit -m "Commit3"
    dolt merge test-branch --no-commit
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
    [ "$status" -eq 1 ]
    [[ "$output" =~ "error: table notvalid does not exist" ]] || false
}

@test "log: Log on a table has basic functionality" {
    dolt sql -q "create table test (pk int PRIMARY KEY)"
    dolt add .
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
    dolt add .
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
    dolt add .
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
    dolt add .
    dolt commit -am "third commit"

    dolt sql -q "insert into test2 values (4)"
    dolt commit -am "fourth commit"

    # Validate we only look at the right commits
    run dolt log -n 1 test
    [ $status -eq 0 ]
    [[ "$output" =~ "second commit" ]] || false
    [[ ! "$output" =~ "first commit" ]] || false
    [[ ! "$output" =~ "Initialize data repository" ]] || false
    [[ ! "$output" =~ "third commit" ]] || false
    [[ ! "$output" =~ "fourth commit" ]] || false

    run dolt log -n 100 test
    [ $status -eq 0 ]
    [[ "$output" =~ "second commit" ]] || false
    [[ "$output" =~ "first commit" ]] || false
    [[ ! "$output" =~ "Initialize data repository" ]] || false
    [[ ! "$output" =~ "third commit" ]] || false
    [[ ! "$output" =~ "fourth commit" ]] || false
}

@test "log: Log on a table works with merge commits" {
    if [ "$SQL_ENGINE" = "remote-engine" ]; then
      skip "needs checkout which is unsupported for remote-engine"
    fi

    dolt sql -q "create table test (pk int, c1 int, primary key(pk))"
    dolt add test
    dolt commit -m "Commit1"
    dolt checkout -b test-branch
    dolt sql -q "insert into test values (0,0)"
    dolt add test
    dolt commit -m "Commit2"
    dolt checkout main
    dolt sql -q "insert into test values (1,1)"
    dolt add test
    dolt commit -m "Commit3"
    dolt merge test-branch --no-commit

    run dolt log test
    [ $status -eq 0 ]
    [[ "$output" =~ "Commit1" ]] || false
    [[ "$output" =~ "Commit3" ]] || false
    [[ ! "$output" =~ "Initialize data repository" ]] || false
    [[ ! "$output" =~ "Merge:" ]] || false
    [[ ! "$output" =~ "Commit2" ]] || false

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
    if [ "$SQL_ENGINE" = "remote-engine" ]; then
      skip "needs checkout which is unsupported for remote-engine"
    fi

    dolt sql -q "create table test (pk int, c1 int, primary key(pk))"
    dolt add test
    dolt commit -m "Commit1"
    dolt checkout -b test-branch
    dolt sql -q "insert into test values (0,0)"
    dolt add test
    dolt commit -m "Commit2"
    dolt checkout main

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

    run dolt log -- test
    [ $status -eq 0 ]

    dolt sql -q "create table test (pk int, c1 int, primary key(pk))"
    dolt add test
    dolt commit -m "Commit3"

    run dolt log test
    [[ "$output" =~ "Commit3" ]] || false
    [[ "$output" =~ "Commit1" ]] || false
    ! [[ "$output" =~ "Commit2" ]] || false
}

@test "log: --merges, --parents, --min-parents option" {
    if [ "$SQL_ENGINE" = "remote-engine" ]; then
      skip "needs checkout which is unsupported for remote-engine"
    fi

    dolt sql -q "create table test (pk int, c1 int, primary key(pk))"
    dolt add -A
    dolt commit -m "Created table"
    dolt checkout -b branch1
    dolt sql -q "insert into test values (0,0)"
    dolt add -A
    dolt commit -m "Inserted 0,0"
    dolt checkout main
    dolt checkout -b branch2
    dolt sql -q "insert into test values (1,1)"
    dolt add -A
    dolt commit -m "Inserted 1,1"

    dolt checkout main
    # Should be fast-forward 
    dolt merge branch1
    # An actual merge
    dolt merge branch2 -m "Merged branch2"

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

@test "log: --oneline only shows commit message in one line" {
    dolt commit --allow-empty -m "a message 1"
    dolt commit --allow-empty -m "a message 2"
    run dolt log --oneline
    [[ ! "$output" =~ "Author" ]] || false
    [[ ! "$output" =~ "Date" ]] || false
    [[ ! "$output" =~ "commit" ]] || false
    res=$(dolt log --oneline | wc -l)
    [ "$res" -eq 3 ] # don't forget initial commit
    dolt commit --allow-empty -m "a message 3"
    res=$(dolt log --oneline | wc -l)
    [ "$res" -eq 4 ] # exactly 1 line is added
}

@test "log: --decorate=short shows trimmed branches and tags" {
    dolt tag tag_v0
    run dolt log --decorate=short
    [[ "$output" =~ "commit" ]] || false
    [[ "$output" =~ "Author" ]] || false
    [[ "$output" =~ "Date" ]] || false
    [[ "$output" =~ "main" ]] || false
    [[ "$output" =~ "tag: tag_v0" ]] || false
    [[ ! "$output" =~ "/refs/heads/" ]] || false
    [[ ! "$output" =~ "/refs/tags/" ]] || false
}

@test "log: --decorate=full shows full branches and tags" {
    dolt tag tag_v0
    run dolt log --decorate=full
    [[ "$output" =~ "commit" ]] || false
    [[ "$output" =~ "Author" ]] || false
    [[ "$output" =~ "Date" ]] || false
    [[ "$output" =~ "refs/heads/main" ]] || false
    [[ "$output" =~ "tag: refs/tags/tag_v0" ]] || false
}

@test "log: --decorate=no doesn't show branches or tags" {
    dolt tag tag_v0
    run dolt log --decorate=no
    [[ "$output" =~ "commit" ]] || false
    [[ "$output" =~ "Author" ]] || false
    [[ "$output" =~ "Date" ]] || false
    [[ ! "$output" =~ "main" ]] || false
    [[ ! "$output" =~ "tag_v0" ]] || false
}

@test "log: decorate and oneline work together" {
    dolt commit --allow-empty -m "a message 1"
    dolt commit --allow-empty -m "a message 2"
    run dolt log --oneline --decorate=full
    [[ ! "$output" =~ "commit" ]] || false
    [[ ! "$output" =~ "Author" ]] || false
    [[ ! "$output" =~ "Date" ]] || false
    [[ "$output" =~ "refs/heads/main" ]] || false
    res=$(dolt log --oneline --decorate=full | wc -l)
    [ "$res" -eq 3 ] # don't forget initial commit
    dolt commit --allow-empty -m "a message 3"
    res=$(dolt log --oneline | wc -l)
    [ "$res" -eq 4 ] # exactly 1 line is added
}

@test "log: --decorate=notanoption throws error" {
    run dolt log --decorate=notanoption
    [ "$status" -eq 1 ]
    [[ "$output" =~ "invalid --decorate option" ]] || false
}

# bats test_tags=no_lambda
@test "log: check pager" {
    skiponwindows "Need to install expect and make this script work on windows."
    dolt commit --allow-empty -m "commit 1"
    dolt commit	--allow-empty -m "commit 2"
    dolt commit	--allow-empty -m "commit 3"
    dolt commit --allow-empty -m "commit 4"
    dolt commit	--allow-empty -m "commit 5"
    dolt commit	--allow-empty -m "commit 6"
    dolt commit --allow-empty -m "commit 7"
    dolt commit	--allow-empty -m "commit 8"
    dolt commit	--allow-empty -m "commit 9"
    dolt commit --allow-empty -m "commit 10"
    dolt commit	--allow-empty -m "commit 11"
    dolt commit	--allow-empty -m "commit 12"
    dolt commit	--allow-empty -m "commit 13"
    dolt commit --allow-empty -m "commit 14"
    dolt commit	--allow-empty -m "commit 15"
    dolt commit	--allow-empty -m "commit 16"

    run expect $BATS_TEST_DIRNAME/log.expect
    [ "$status" -eq 0 ]
}

@test "log: string formatting characters are escaped" {
    run dolt commit --allow-empty -m "% should be escaped"
    [[ "$output" =~ "% should be escaped" ]] || false
}

@test "log: identify HEAD" {
    dolt commit --allow-empty -m "commit 1"
    dolt tag commit1
    dolt commit --allow-empty -m "commit 2"
    dolt tag commit2
    run dolt log commit1
    [[ ! "$output" =~ "HEAD" ]] || false
    run dolt log commit2
    [[ "$output" =~ "HEAD" ]] || false
}

@test "log: --stat shows diffstat" {
    dolt sql -q "create table test (pk int primary key, c int)"
    dolt commit -Am "create table test"
    run dolt log --stat head -n=1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test added" ]] || false

    dolt sql -q "insert into test values (1,1)"
    dolt commit -Am "insert into test"
    run dolt log --stat head -n=1
    [ "$status" -eq 0 ]
    out=$(remove_color_codes "$output") # remove special characters for color
    [[ "$out" =~ " test | 1 +" ]] || false
    [[ "$out" =~ " 1 tables changed, 1 rows added(+), 0 rows modified(*), 0 rows deleted(-)" ]] || false

    dolt sql -q "update test set c = 2 where pk = 1"
    dolt commit -Am "update test"
    run dolt log --stat head -n=1
    [ "$status" -eq 0 ]
    out=$(remove_color_codes "$output") # remove special characters for color
    [[ "$out" =~ " test | 1 *" ]] || false
    [[ "$out" =~ " 1 tables changed, 0 rows added(+), 1 rows modified(*), 0 rows deleted(-)" ]] || false

    dolt sql -q "delete from test where pk = 1"
    dolt commit -Am "delete from test"
    run dolt log --stat head -n=1
    [ "$status" -eq 0 ]
    out=$(remove_color_codes "$output") # remove special characters for color
    [[ "$out" =~ " test | 1 -" ]] || false
    [[ "$out" =~ " 1 tables changed, 0 rows added(+), 0 rows modified(*), 1 rows deleted(-)" ]] || false

    dolt sql -q "drop table test"
    dolt commit -Am "drop table test"
    run dolt log --stat head -n=1
    [ "$status" -eq 0 ]
    [[ "$output" =~ " test deleted" ]] || false
}

@test "log: --stat works with --oneline" {
    dolt sql -q "create table test (pk int primary key, c int)"
    dolt commit -Am "create table test"
    dolt sql -q "insert into test values (1,1)"
    dolt commit -Am "insert into test"

    run dolt log --stat --oneline
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]
    l1=$(remove_color_codes "${lines[1]}") # remove special characters for color
    [[ "$l1" =~ " test | 1 +" ]] || false
    l2=$(remove_color_codes "${lines[2]}") # remove special characters for color
    [[ "$output" =~ " 1 tables changed, 1 rows added(+), 0 rows modified(*), 0 rows deleted(-)" ]] || false
    l3=$(remove_color_codes "${lines[4]}") # remove special characters for color
    [[ "$l3" =~ " test added" ]] || false
}

@test "log: --stat doesn't print diffstat for merge commits" {
    if [ "$SQL_ENGINE" = "remote-engine" ]; then
      skip "needs checkout which is unsupported for remote-engine"
    fi

    dolt sql -q "create table test (pk int primary key, c int)"
    dolt commit -Am "create table test"
    dolt branch branch1
    dolt sql -q "insert into test values (1,1)"
    dolt commit -Am "insert into test"
    dolt checkout branch1
    dolt sql -q "insert into test values (2,2)"
    dolt commit -Am "insert into test"
    dolt merge main -m "merge main"

    run dolt log --stat head -n=1
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "test" ]] || false
    [[ "$output" =~ "merge main" ]] || false
    [ "${#lines[@]}" -eq 5 ]
}

remove_color_codes() {
    echo "$1" | sed -E 's/\x1b\[[0-9;]*m//g'
}

@test "log: --graph: basic graph log" {
    dolt sql -q "create table testtable (pk int PRIMARY KEY)"
    dolt add .
    dolt commit -m "commit 1"
   
    # Run the dolt log --graph command
    run dolt log --graph
    [ "$status" -eq 0 ]
    
    # Check the output with patterns
    [[ "${lines[0]}" =~ \* ]] || false
    [[ $(remove_color_codes "${lines[0]}") =~ "* commit " ]] || false                          # * commit xxx
    [[ $(remove_color_codes "${lines[1]}") =~ "| Author:" ]] || false                          # | Author: 
    [[ $(remove_color_codes "${lines[2]}") =~ "| Date:" ]] || false                            # | Date: 
    [[ $(remove_color_codes "${lines[3]}") =~ "|" ]] || false                                  # | 
    [[ $(remove_color_codes "${lines[4]}") =~ "commit 1" ]] || false                           # |    commit 1 
    [[ $(remove_color_codes "${lines[5]}") =~ "|" ]] || false                                  # | 
    [[ $(remove_color_codes "${lines[6]}") =~ "* commit " ]] || false                          # * commit xxx
    [[ $(remove_color_codes "${lines[7]}") =~ "Author:" ]] || false                            #   Author: 
    [[ $(remove_color_codes "${lines[8]}") =~ "Date:" ]] || false                              #   Date: 
    [[ $(remove_color_codes "${lines[9]}") =~ "Initialize data repository" ]] || false         #      Initialize data repository
}


@test "log: --graph: graph with merges" {
    if [ "$SQL_ENGINE" = "remote-engine" ]; then
      skip "needs checkout which is unsupported for remote-engine"
    fi

    dolt sql -q "create table testtable (pk int PRIMARY KEY)"
    dolt add .
    dolt commit -m "commit 1 MAIN"
    dolt checkout -b branchA
    dolt commit --allow-empty -m "commit 1 BRANCHA"
    dolt checkout main
    dolt commit --allow-empty -m "commit 2 MAIN"
    dolt merge branchA -m "Merge branchA into main"

    run dolt log --graph  
    [ "$status" -eq 0 ]

    # Check the output with patterns
    [[ "${lines[0]}" =~ \* ]] || false
    [[ $(remove_color_codes "${lines[0]}") =~ "*   commit " ]] || false                        # *   commit xxx
    [[ $(remove_color_codes "${lines[1]}") =~ "|\  Merge:" ]] || false                         # |\  Merge:
    [[ $(remove_color_codes "${lines[2]}") =~ "| | Author:" ]] || false                        # | | Author: 
    [[ $(remove_color_codes "${lines[3]}") =~ "| | Date:" ]] || false                          # | | Date: 
    [[ $(remove_color_codes "${lines[4]}") =~ "| |" ]] || false                                # | | 
    [[ $(remove_color_codes "${lines[5]}") =~ "Merge branchA into main" ]] || false            # | |    Merge branchA into main 
    [[ $(remove_color_codes "${lines[6]}") =~ "| |" ]] || false                                # | | 
    [[ $(remove_color_codes "${lines[7]}") =~ "* | commit " ]] || false                        # * | commit xxx
    [[ $(remove_color_codes "${lines[8]}") =~ "| | Author:" ]] || false                        # | | Author: 
    [[ $(remove_color_codes "${lines[9]}") =~ "| | Date:" ]] || false                          # | | Date: 
    [[ $(remove_color_codes "${lines[10]}") =~ "| |" ]] || false                               # | | 
    [[ $(remove_color_codes "${lines[11]}") =~ "commit 2 MAIN" ]] || false                     # | |    commit 2 MAIN
    [[ $(remove_color_codes "${lines[12]}") =~ "| |" ]] || false                               # | | 
    [[ $(remove_color_codes "${lines[13]}") =~ "| * commit " ]] || false                       # | * commit xxx
    [[ $(remove_color_codes "${lines[14]}") =~ "| | Author:" ]] || false                       # | | Author: 
    [[ $(remove_color_codes "${lines[15]}") =~ "| | Date:" ]] || false                         # | | Date: 
    [[ $(remove_color_codes "${lines[16]}") =~ "| |" ]] || false                               # | |  
    [[ $(remove_color_codes "${lines[17]}") =~ "commit 1 BRANCHA" ]] || false                  # | |    commit 1 BRANCHA
    [[ $(remove_color_codes "${lines[18]}") =~ "|/" ]] || false                                # |/ 
    [[ $(remove_color_codes "${lines[19]}") =~ "* commit" ]] || false                          # *  commit xxx
    [[ $(remove_color_codes "${lines[20]}") =~ "| Author:" ]] || false                         # |  Author: 
    [[ $(remove_color_codes "${lines[21]}") =~ "| Date:" ]] || false                           # |  Date: 
    [[ $(remove_color_codes "${lines[22]}") =~ "|" ]] || false                                 # |  
    [[ $(remove_color_codes "${lines[23]}") =~ "commit 1 MAIN" ]] || false                     # |   commit 1 MAIN
    [[ $(remove_color_codes "${lines[24]}") =~ "|" ]] || false                                 # | 
    [[ $(remove_color_codes "${lines[25]}") =~ "* commit" ]] || false                          # * commit
    [[ $(remove_color_codes "${lines[26]}") =~ "Author:" ]] || false                           #   Author:
    [[ $(remove_color_codes "${lines[27]}") =~ "Date:" ]] || false                             #   Date:
    [[ $(remove_color_codes "${lines[28]}") =~ "Initialize data repository" ]] || false        #     Initialize data repository
}