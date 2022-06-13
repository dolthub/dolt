#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    skip_nbf_dolt_1

    TMPDIRS=$(pwd)/tmpdirs
    mkdir -p $TMPDIRS/{rem1,repo1}

    # repo1 -> rem1 -> repo2
    cd $TMPDIRS/repo1
    dolt init
    dolt branch feature
    dolt remote add origin file://../rem1
    dolt remote add test-remote file://../rem1
    dolt push origin main

    cd $TMPDIRS
    dolt clone file://rem1 repo2
    cd $TMPDIRS/repo2
    dolt log
    dolt branch feature
    dolt remote add test-remote file://../rem1

    # table and comits only present on repo1, rem1 at start
    cd $TMPDIRS/repo1
    dolt sql -q "create table t1 (a int primary key, b int)"
    dolt commit -am "First commit"
    dolt sql -q "insert into t1 values (0,0)"
    dolt commit -am "Second commit"
    dolt push origin main
    cd $TMPDIRS
}

teardown() {
    teardown_common
    rm -rf $TMPDIRS
    cd $BATS_TMPDIR
}

@test "sql-pull: dolt_pull main" {
    cd repo2
    dolt sql -q "select dolt_pull('origin')"
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-pull: CALL dolt_pull main" {
    cd repo2
    dolt sql -q "CALL dolt_pull('origin')"
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-pull: CALL dpull main" {
    cd repo2
    dolt sql -q "CALL dpull('origin')"
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-pull: dolt_pull custom remote" {
    cd repo2
    dolt sql -q "select dolt_pull('test-remote')"
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-pull: CALL dolt_pull custom remote" {
    cd repo2
    dolt sql -q "CALL dolt_pull('test-remote')"
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-pull: dolt_pull default origin" {
    cd repo2
    dolt remote remove test-remote
    dolt sql -q "select dolt_pull()"
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-pull: CALL dolt_pull default origin" {
    cd repo2
    dolt remote remove test-remote
    dolt sql -q "CALL dolt_pull()"
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-pull: dolt_pull default custom remote" {
    cd repo2
    dolt remote remove origin
    dolt sql -q "select dolt_pull()"
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-pull: CALL dolt_pull default custom remote" {
    cd repo2
    dolt remote remove origin
    dolt sql -q "CALL dolt_pull()"
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-pull: dolt_pull up to date does not error" {
    cd repo2
    dolt sql -q "select dolt_pull('origin')"
    dolt sql -q "select dolt_pull('origin')"
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-pull: CALL dolt_pull up to date does not error" {
    cd repo2
    dolt sql -q "CALL dolt_pull('origin')"
    dolt sql -q "CALL dolt_pull('origin')"
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-pull: dolt_pull unknown remote fails" {
    cd repo2
    run dolt sql -q "select dolt_pull('unknown')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "unknown remote" ]] || false
    [[ ! "$output" =~ "panic" ]] || false
}

@test "sql-pull: CALL dolt_pull unknown remote fails" {
    cd repo2
    run dolt sql -q "CALL dolt_pull('unknown')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "unknown remote" ]] || false
    [[ ! "$output" =~ "panic" ]] || false
}

@test "sql-pull: dolt_pull unknown feature branch fails" {
    cd repo2
    dolt checkout feature
    run dolt sql -q "select dolt_pull('origin')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "branch not found" ]] || false
    [[ ! "$output" =~ "panic" ]] || false
}

@test "sql-pull: CALL dolt_pull unknown feature branch fails" {
    cd repo2
    dolt checkout feature
    run dolt sql -q "CALL dolt_pull('origin')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "branch not found" ]] || false
    [[ ! "$output" =~ "panic" ]] || false
}

@test "sql-pull: dolt_pull feature branch" {
    cd repo1
    dolt checkout feature
    dolt merge main
    dolt push origin feature

    cd ../repo2
    dolt checkout feature
    dolt sql -q "select dolt_pull('origin')"
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-pull: CALL dolt_pull feature branch" {
    cd repo1
    dolt checkout feature
    dolt merge main
    dolt push origin feature

    cd ../repo2
    dolt checkout feature
    dolt sql -q "CALL dolt_pull('origin')"
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-pull: dolt_pull force" {
    skip "todo: support dolt pull --force (cli too)"
    cd repo2
    dolt sql -q "create table t2 (a int)"
    dolt commit -am "2.0 commit"
    dolt push origin main

    cd ../repo1
    dolt sql -q "create table t2 (a int primary key)"
    dolt sql -q "create table t3 (a int primary key)"
    dolt commit -am "2.1 commit"
    dolt push -f origin main

    cd ../repo2
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

@test "sql-pull: CALL dolt_pull force" {
    skip "todo: support dolt pull --force (cli too)"
    cd repo2
    dolt sql -q "create table t2 (a int)"
    dolt commit -am "2.0 commit"
    dolt push origin main

    cd ../repo1
    dolt sql -q "create table t2 (a int primary key)"
    dolt sql -q "create table t3 (a int primary key)"
    dolt commit -am "2.1 commit"
    dolt push -f origin main

    cd ../repo2
    run dolt sql -q "CALL dolt_pull('origin')"
    [ "$status" -eq 1 ]
    [[ ! "$output" =~ "panic" ]] || false
    [[ "$output" =~ "fetch failed; dataset head is not ancestor of commit" ]] || false

    dolt sql -q "CALL dolt_pull('-f', 'origin')"

    run dolt log -n 1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "2.1 commit" ]] || false

    run dolt sql -q "show tables" -r csv
    [ "${#lines[@]}" -eq 4 ]
    [[ "$output" =~ "t3" ]] || false
}

@test "sql-pull: dolt_pull squash" {
    skip "todo: support dolt pull --squash (cli too)"
    cd repo2
    dolt sql -q "select dolt_pull('--squash', 'origin')"
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-pull: CALL dolt_pull squash" {
    skip "todo: support dolt pull --squash (cli too)"
    cd repo2
    dolt sql -q "CALL dolt_pull('--squash', 'origin')"
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-pull: dolt_pull --noff flag" {
    cd repo2
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

@test "sql-pull: CALL dolt_pull --noff flag" {
    cd repo2
    dolt sql -q "CALL dolt_pull('--no-ff', 'origin')"
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
    cd repo2
    dolt sql -q "select dolt_pull('')"
}

@test "sql-pull: empty remote name does not panic on CALL" {
    cd repo2
    dolt sql -q "CALL dolt_pull('')"
}

@test "sql-pull: dolt_pull dirty working set fails" {
    cd repo2
    dolt sql -q "create table t2 (a int)"
    run dolt sql -q "select dolt_pull('origin')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "cannot merge with uncommitted changes" ]] || false
}

@test "sql-pull: CALL dolt_pull dirty working set fails" {
    cd repo2
    dolt sql -q "create table t2 (a int)"
    run dolt sql -q "CALL dolt_pull('origin')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "cannot merge with uncommitted changes" ]] || false
}

@test "sql-pull: dolt_pull tag" {
    cd repo1
    dolt tag v1
    dolt push origin v1
    dolt tag

    cd ../repo2
    dolt sql -q "select dolt_pull('origin')"
    run dolt tag
    [ "$status" -eq 0 ]
    [[ "$output" =~ "v1" ]] || false
}

@test "sql-pull: CALL dolt_pull tag" {
    cd repo1
    dolt tag v1
    dolt push origin v1
    dolt tag

    cd ../repo2
    dolt sql -q "CALL dolt_pull('origin')"
    run dolt tag
    [ "$status" -eq 0 ]
    [[ "$output" =~ "v1" ]] || false
}

@test "sql-pull: dolt_pull tags only for resolved commits" {
    cd repo1
    dolt tag v1 head
    dolt tag v2 head^
    dolt push origin v1
    dolt push origin v2

    dolt checkout feature
    dolt sql -q "create table t2 (a int)"
    dolt commit -am "feature commit"
    dolt tag v3
    dolt push origin v3

    cd ../repo2
    dolt sql -q "select dolt_pull('origin')"
    run dolt tag
    [ "$status" -eq 0 ]
    [[ "$output" =~ "v1" ]] || false
    [[ "$output" =~ "v2" ]] || false
    [[ ! "$output" =~ "v3" ]] || false
}

@test "sql-pull: CALL dolt_pull tags only for resolved commits" {
    cd repo1
    dolt tag v1 head
    dolt tag v2 head^
    dolt push origin v1
    dolt push origin v2

    dolt checkout feature
    dolt sql -q "create table t2 (a int)"
    dolt commit -am "feature commit"
    dolt tag v3
    dolt push origin v3

    cd ../repo2
    dolt sql -q "CALL dolt_pull('origin')"
    run dolt tag
    [ "$status" -eq 0 ]
    [[ "$output" =~ "v1" ]] || false
    [[ "$output" =~ "v2" ]] || false
    [[ ! "$output" =~ "v3" ]] || false
}
