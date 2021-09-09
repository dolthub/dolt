#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    TMPDIRS=$(pwd)/tmpdirs
    mkdir -p $TMPDIRS/{rem1,tmp1}

    # tmp1 -> rem1 -> tmp2
    cd $TMPDIRS/tmp1
    dolt init
    dolt branch feature
    dolt remote add origin file://../rem1
    dolt remote add test-remote file://../rem1
    dolt push origin master

    cd $TMPDIRS
    dolt clone file://rem1 tmp2
    cd $TMPDIRS/tmp2
    dolt log
    dolt branch feature
    dolt remote add test-remote file://../rem1

    cd $TMPDIRS/tmp1
    dolt sql -q "create table t1 (a int primary key, b int)"
    dolt commit -am "First commit"
    dolt sql -q "insert into t1 values (0,0)"
    dolt commit -am "Second commit"
    dolt push origin master
    cd $TMPDIRS
}

teardown() {
    teardown_common
    rm -rf $TMPDIRS
    cd $BATS_TMPDIR
}

@test "sql-pull: dolt_pull master" {
    cd tmp2
    dolt sql -q "select dolt_pull('origin')"
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-pull: dolt_pull custom remote" {
    cd tmp2
    dolt sql -q "select dolt_pull('test-remote')"
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-pull: dolt_pull default origin" {
    cd tmp2
    dolt remote remove test-remote
    dolt sql -q "select dolt_pull()"
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-pull: dolt_pull default custom remote" {
    cd tmp2
    dolt remote remove origin
    dolt sql -q "select dolt_pull()"
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-pull: dolt_pull up to date does not error" {
    cd tmp2
    dolt sql -q "select dolt_pull('origin')"
    dolt sql -q "select dolt_pull('origin')"
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-pull: dolt_pull unknown remote fails" {
    cd tmp2
    run dolt sql -q "select dolt_pull('unknown')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "unknown remote" ]] || false
    [[ ! "$output" =~ "panic" ]] || false
}
@test "sql-pull: dolt_pull unknown feature branch fails" {
    cd tmp2
    dolt checkout feature
    run dolt sql -q "select dolt_pull('origin')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "branch not found" ]] || false
    [[ ! "$output" =~ "panic" ]] || false
}

@test "sql-pull: dolt_pull feature branch" {
    cd tmp1
    dolt checkout feature
    dolt merge master
    dolt push origin feature

    cd ../tmp2
    dolt checkout feature
    dolt sql -q "select dolt_pull('origin')"
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-pull: dolt_pull force" {
    skip "todo: support dolt pull --force (cli too)"
    cd tmp2
    dolt sql -q "create table t2 (a int)"
    dolt commit -am "2.0 commit"
    dolt push origin master

    cd ../tmp1
    dolt sql -q "create table t2 (a int primary key)"
    dolt sql -q "create table t3 (a int primary key)"
    dolt commit -am "2.1 commit"
    dolt push -f origin master

    cd ../tmp2
    run dolt sql -q "select dolt_pull('origin')"
    [ "$status" -eq 1 ]
    [[ ! "$output" =~ "panic" ]] || false
    [[ "$output" =~ "fetch failed; dataset head is not ancestor of commit" ]] || false

    dolt sql -q "select dolt_pull('-f', 'origin')"

    run dolt log -n 1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "2.1 commit" ]] || false

    run dolt sql -q "show tables" -r csv
    [ "${#lines[@]}" -eq 4 ]
    [[ "$output" =~ "t3" ]] || false
}

@test "sql-pull: dolt_pull squash" {
    skip "todo: support dolt pull --squash (cli too)"
    cd tmp2
    dolt sql -q "select dolt_pull('--squash', 'origin')"
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-pull: dolt_pull --noff flag" {
    cd tmp2
    dolt sql -q "select dolt_pull('--no-ff', 'origin')"
    dolt status
    run dolt log -n 1
    [ "$status" -eq 0 ]
    # TODO change the default message name
    [[ "$output" =~ "automatic SQL merge" ]] || false

    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-pull: empty remote name does not panic" {
    cd tmp2
    dolt sql -q "select dolt_pull('')"
}

@test "sql-pull: dolt_pull dirty working set fails" {
    cd tmp2
    dolt sql -q "create table t2 (a int)"
    run dolt sql -q "select dolt_pull('origin')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "cannot merge with uncommitted changes" ]] || false
}

@test "sql-pull: dolt_pull tag" {
    cd tmp1
    dolt tag v1
    dolt push origin v1
    dolt tag

    cd ../tmp2
    dolt sql -q "select dolt_pull('origin')"
    run dolt tag
    [ "$status" -eq 0 ]
    [[ "$output" =~ "v1" ]] || false
}

@test "sql-pull: dolt_pull tags only for resolved commits" {
    cd tmp1
    dolt tag v1 head
    dolt tag v2 head^
    dolt push origin v1
    dolt push origin v2

    dolt checkout feature
    dolt sql -q "create table t2 (a int)"
    dolt commit -am "feature commit"
    dolt tag v3
    dolt push origin v3

    cd ../tmp2
    dolt sql -q "select dolt_pull('origin')"
    run dolt tag
    [ "$status" -eq 0 ]
    [[ "$output" =~ "v1" ]] || false
    [[ "$output" =~ "v2" ]] || false
    [[ ! "$output" =~ "v3" ]] || false
}

