#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

basic_conflict() {
    dolt sql -q "create table t (i int primary key, t text)"
    dolt add .
    dolt commit -am "init commit"
    dolt checkout -b other
    dolt sql -q "insert into t values (1,'other')"
    dolt commit -am "other commit"
    dolt checkout main
    dolt sql -q "insert into t values (1,'main')"
    dolt commit -am "main commit"
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "sql-conflicts-resolve: call with no arguments, errors" {
    run dolt sql -q "call dolt_conflicts_resolve()"
    [ $status -eq 1 ]
    [[ $output =~ "--ours or --theirs must be supplied" ]] || false
}

@test "sql-conflicts-resolve: call without specifying table, errors" {
    run dolt sql -q "call dolt_conflicts_resolve('--theirs')"
    [ $status -eq 1 ]
    [[ $output =~ "specify at least one table to resolve conflicts" ]] || false
}

@test "sql-conflicts-resolve: call with non-existent table, errors" {
    run dolt sql -q "call dolt_conflicts_resolve('--ours', 'notexists')"
    [ $status -eq 1 ]
    [[ $output =~ "table not found" ]] || false
}

@test "sql-conflicts-resolve: no conflicts, no changes" {
    basic_conflict

    dolt checkout main
    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ $output =~ "main" ]] || false

    dolt checkout other
    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ $output =~ "other" ]] || false

    dolt checkout main
    run dolt sql -q "CALL dolt_conflicts_resolve('--ours', 't')"
    [ $status -eq 0 ]
    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ $output =~ "main" ]] || false

    run dolt sql -q "CALL dolt_conflicts_resolve('--theirs', 't')"
    [ $status -eq 0 ]
    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ $output =~ "main" ]] || false

    dolt checkout other
    run dolt sql -q "CALL dolt_conflicts_resolve('--ours', 't')"
    [ $status -eq 0 ]
    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ $output =~ "other" ]] || false

    run dolt sql -q "CALL dolt_conflicts_resolve('--theirs', 't')"
    [ $status -eq 0 ]
    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ $output =~ "other" ]] || false
}

@test "sql-conflicts-resolve: merge other into main, resolve with ours" {
    basic_conflict

    dolt checkout main
    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ $output =~ "main" ]] || false

    run dolt merge other
    [[ $output =~ "Automatic merge failed" ]] || false

    run dolt sql -q "CALL dolt_conflicts_resolve('--ours', 't')"
    [ $status -eq 0 ]
    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ $output =~ "main" ]] || false
}

@test "sql-conflicts-resolve: merge other into main, resolve with theirs" {
    basic_conflict

    dolt checkout main
    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ $output =~ "main" ]] || false

    run dolt merge other
    [[ $output =~ "Automatic merge failed" ]] || false

    run dolt sql -q "CALL dolt_conflicts_resolve('--theirs', 't')"
    [ $status -eq 0 ]
    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ $output =~ "other" ]] || false
}

@test "sql-conflicts-resolve: merge main into other, resolve with ours" {
    basic_conflict

    dolt checkout other
    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ $output =~ "other" ]] || false

    run dolt merge main
    [[ $output =~ "Automatic merge failed" ]] || false

    run dolt sql -q "CALL dolt_conflicts_resolve('--ours', 't')"
    [ $status -eq 0 ]
    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ $output =~ "other" ]] || false
}

@test "sql-conflicts-resolve: merge main into other, resolve with theirs" {
    basic_conflict

    dolt checkout other
    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ $output =~ "other" ]] || false

    run dolt merge main
    [[ $output =~ "Automatic merge failed" ]] || false

    run dolt sql -q "CALL dolt_conflicts_resolve('--theirs', 't')"
    [ $status -eq 0 ]
    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ $output =~ "main" ]] || false
}

@test "sql-conflicts-resolve: two branches, one deletes rows, one modifies those same rows. merge. conflict" {
    dolt sql -q 'CREATE TABLE foo (`pk` INT PRIMARY KEY, `col:1` INT);'
    dolt sql -q "INSERT INTO foo VALUES (1, 1), (2, 1), (3, 1), (4, 1), (5, 1);"
    dolt add foo
    dolt commit -m 'initial commit.'

    dolt checkout -b deleter
    dolt sql -q 'delete from foo'
    dolt add foo
    dolt commit -m 'delete commit.'

    dolt checkout -b modifier main
    dolt sql -q 'update foo set `col:1` = `col:1` + 1 where pk in (1, 3, 5);'
    dolt add foo
    dolt commit -m 'modify commit.'

    dolt checkout -b merge-into-modified modifier
    run dolt merge deleter -m "merge"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "CONFLICT" ]] || false
    dolt merge --abort

    # Accept theirs deletes all rows.
    dolt checkout main
    dolt branch -d -f merge-into-modified
    dolt checkout -b merge-into-modified modifier
    dolt merge deleter -m "merge"

    dolt sql -q "call dolt_conflicts_resolve('--theirs', 'foo')"
    run dolt sql -q 'select count(*) from foo'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| 0        |" ]] || false
    dolt merge --abort
    dolt reset --hard

    # Accept ours deletes two rows.
    dolt checkout main
    dolt branch -d -f merge-into-modified
    dolt checkout -b merge-into-modified modifier
    dolt merge deleter -m "merge"
    dolt sql -q "call dolt_conflicts_resolve('--ours', 'foo')"
    run dolt sql -q 'select count(*) from foo'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| 3        |" ]] || false
    dolt merge --abort
    dolt reset --hard

    dolt checkout -b merge-into-deleter deleter
    run dolt merge modifier -m "merge"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "CONFLICT" ]] || false
    dolt merge --abort

    # Accept ours deletes all rows.
    dolt checkout main
    dolt branch -d -f merge-into-deleter
    dolt checkout -b merge-into-deleter deleter
    dolt merge modifier -m "merge"
    dolt sql -q "call dolt_conflicts_resolve('--ours', 'foo')"
    run dolt sql -q 'select count(*) from foo'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| 0        |" ]] || false
    dolt merge --abort
    dolt reset --hard

    # Accept theirs adds modified.
    dolt checkout main
    dolt branch -d -f merge-into-deleter
    dolt checkout -b merge-into-deleter deleter
    dolt merge modifier -m "merge"
    dolt sql -q "call dolt_conflicts_resolve('--theirs', 'foo')"
    run dolt sql -q 'select count(*) from foo'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| 3        |" ]] || false
    dolt merge --abort
    dolt reset --hard
}

@test "sql-conflicts-resolve: conflicts table properly cleared on dolt conflicts resolve" {
    dolt sql -q "create table test(pk int, c1 int, primary key(pk))"

    run dolt conflicts cat test
    [ $status -eq 0 ]
    [ "$output" = "" ]
    ! [[ "$output" =~ "pk" ]] || false

    dolt add .
    dolt commit -m "created table"
    dolt branch branch1
    dolt sql -q "insert into test values (0,0)"
    dolt add .
    dolt commit -m "inserted 0,0"
    dolt checkout branch1
    dolt sql -q "insert into test values (0,1)"
    dolt add .
    dolt commit -m "inserted 0,1"
    dolt checkout main
    dolt merge branch1 -m "merge"
    run dolt sql -q "call dolt_conflicts_resolve('--ours', 'test')"
    [ $status -eq 0 ]

    run dolt conflicts cat test
    [ $status -eq 0 ]
    [ "$output" = "" ]
    ! [[ "$output" =~ "pk" ]] || false

    run dolt sql -q "update test set c1=1"
    [ $status -eq 0 ]
    ! [[ "$output" =~ "unresolved conflicts from the merge" ]] || false

    dolt add .
    dolt commit -m "Committing active merge"

    run dolt conflicts cat test
    [ $status -eq 0 ]
    [ "$output" = "" ]
    ! [[ "$output" =~ "pk" ]] || false
}