#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    TMPDIRS=$(pwd)/tmpdirs
    mkdir -p $TMPDIRS/{rem1,tmp1}

    # tmp1 -> rem1 -> tmp2
    cd $TMPDIRS/tmp1
    dolt init
    dolt remote add origin file://../rem1
    dolt remote add test-remote file://../rem1
    dolt push origin master

    cd $TMPDIRS
    dolt clone file://rem1 tmp2
    cd $TMPDIRS/tmp2
    dolt log
    dolt remote add test-remote file://../rem1

    # table and comits only present on tmp1, rem1 at start
    cd $TMPDIRS/tmp1
    dolt sql -q "create table t1 (a int primary key, b int)"
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

@test "sql-push: dolt_push origin" {
    cd tmp1
    dolt sql -q "select dolt_push('origin', 'master')"

    cd ../tmp2
    dolt pull origin
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-push: dolt_push custom remote" {
    cd tmp1
    dolt sql -q "select dolt_push('test-remote', 'master')"

    cd ../tmp2
    dolt pull origin
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-push: dolt_push active branch" {
    skip "upstream state lost between sessions"
    cd tmp1
    dolt sql -q "select dolt_push('origin')"

    cd ../tmp2
    dolt pull origin
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-push: dolt_push feature branch" {
    cd tmp1
    dolt checkout -b feature
    dolt sql -q "select dolt_push('origin', 'feature')"

    cd ../tmp2
    dolt fetch origin feature
    dolt checkout feature
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-push: dolt_push --set-upstream transient outside of session" {
    cd tmp1
    dolt sql -q "select dolt_push('-u', 'origin', 'master')"

    cd ../tmp2
    dolt pull origin
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false

    cd ../tmp1
    # TODO persist branch config?
    run dolt sql -q "select dolt_push()"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "the current branch has no upstream branch" ]] || false
}

@test "sql-push: dolt_push --force flag" {
    cd tmp2
    dolt sql -q "create table t2 (a int)"
    dolt commit -am "commit to override"
    dolt push origin master

    cd ../tmp1
    run dolt sql -q "select dolt_push('origin', 'master')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "the tip of your current branch is behind its remote counterpart" ]] || false


    dolt sql -q "select dolt_push('--force', 'origin', 'master')"
}

@test "sql-push: push to unknown remote" {
    cd tmp1
    run dolt sql -q "select dolt_push('unknown', 'master')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "unknown remote: 'unknown'" ]] || false
}

@test "sql-push: push unknown branch" {
    cd tmp1
    run dolt sql -q "select dolt_push('origin', 'unknown')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "refspec not found: 'unknown'" ]] || false
}

@test "sql-push: not specifying a branch throws an error" {
    cd tmp1
    run dolt sql -q "select dolt_push('-u', 'origin')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "invalid set-upstream arguments" ]] || false
}

@test "sql-push: pushing empty branch does not panic" {
    cd tmp1
    run dolt sql -q "select dolt_push('origin', '')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "invalid ref spec: ''" ]] || false
}

