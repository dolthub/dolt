#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    TMPDIRS=$(pwd)/tmpdirs
    mkdir -p $TMPDIRS/{rem1,bac1,repo1}

    skip_nbf_dolt_1

    # repo1 -> rem1 -> repo2
    cd $TMPDIRS/repo1
    dolt init
    dolt tag v1
    dolt sql -q "create table t1 (a int)"
    dolt commit -am "cm"
    dolt branch feature
    dolt remote add origin file://../rem1
    dolt push origin main
    cd $TMPDIRS
}

teardown() {
    teardown_common
    rm -rf $TMPDIRS
    cd $BATS_TMPDIR
}

@test "backup: add named backup" {
    skip_nbf_dolt_1
    cd repo1
    dolt backup add bac1 file://../bac1
    run dolt backup -v
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]
    [[ "$output" =~ "bac1" ]] || false
}

@test "backup: remove named backup" {
    skip_nbf_dolt_1
    cd repo1
    dolt backup add bac1 file://../bac1
    run dolt backup -v
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]
    [[ "$output" =~ "bac1" ]] || false

    dolt backup remove bac1

    run dolt backup -v
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 0 ]
    [[ ! "$output" =~ "bac1" ]] || false
}

@test "backup: rm named backup" {
    skip_nbf_dolt_1
    cd repo1
    dolt backup add bac1 file://../bac1
    run dolt backup -v
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]
    [[ "$output" =~ "bac1" ]] || false

    dolt backup rm bac1

    run dolt backup -v
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 0 ]
    [[ ! "$output" =~ "bac1" ]] || false
}

@test "backup: removing a backup with the same name as a remote does not impact remote tracking refs" {
    skip_nbf_dolt_1
    cd repo1
    dolt backup add origin file://../bac1
    dolt backup remove origin

    run dolt sql -q "show tables as of hashof('origin/main')" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "${lines[0]}" =~ "Table" ]] || false
    [[ "${lines[1]}" =~ "t1" ]] || false
}

@test "backup: sync master to backup" {
    skip_nbf_dolt_1
    cd repo1
    dolt backup add bac1 file://../bac1
    dolt backup sync bac1

    cd ..
    run dolt backup restore file://./bac1 repo2
    cd repo2
    run dolt ls
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "t1" ]] || false
}

@test "backup: sync feature to backup" {
    skip_nbf_dolt_1
    cd repo1
    dolt backup add bac1 file://../bac1
    dolt backup sync bac1

    cd ..
    dolt backup restore file://./bac1 repo2
    cd repo2
    run dolt branch
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "* main" ]] || false
    [[ "$output" =~ "feature" ]] || false
}

@test "backup: sync tag to backup" {
    skip_nbf_dolt_1
    cd repo1
    dolt backup add bac1 file://../bac1
    dolt backup sync bac1

    cd ..
    dolt backup restore file://./bac1 repo2
    cd repo2
    run dolt tag
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]
    [[ "$output" =~ "v1" ]] || false
}

@test "backup: sync remote ref to backup" {
    skip_nbf_dolt_1
    cd repo1
    dolt backup add bac1 file://../bac1
    dolt backup sync bac1

    cd ..
    dolt backup restore file://./bac1 repo2
    cd repo2
    run dolt sql -q "show tables as of hashof('origin/main')" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "t1" ]] || false
}

@test "backup: sync working set to backup" {
    skip_nbf_dolt_1
    cd repo1
    dolt sql -q "create table t2 (a int)"
    dolt add t2
    dolt backup add bac1 file://../bac1
    dolt backup sync bac1

    cd ..
    dolt backup restore file://./bac1 repo2
    cd repo2
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "t2" ]] || false
}

@test "backup: no origin on restore" {
    skip_nbf_dolt_1
    cd repo1
    dolt backup add bac1 file://../bac1
    dolt backup sync bac1

    cd ..
    run dolt backup restore file://./bac1 repo2
    cd repo2
    run dolt remote -v
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 0 ]
    [[ ! "$output" =~ "origin" ]] || false
}

@test "backup: backup already up to date" {
    skip_nbf_dolt_1
    cd repo1
    dolt backup add bac1 file://../bac1
    dolt backup sync bac1
    dolt backup sync bac1
}

@test "backup: no backup exists" {
    skip_nbf_dolt_1
    cd repo1
    run dolt backup sync bac1
    [ "$status" -eq 1 ]
    [[ ! "$output" =~ "panic" ]] || false
    [[ "$output" =~ "unknown backup: 'bac1'" ]] || false
}

@test "backup: cannot override another client's backup" {
    skip_nbf_dolt_1
    skip "todo implement backup lock file"
    cd repo1
    dolt backup add bac1 file://../bac1
    dolt backup sync bac1

    cd .. && mkdir repo2 && cd repo2
    dolt init
    dolt sql -q "create table s1 (a int)"
    dolt commit -am "cm"

    dolt backup add bac1 file://../bac1
    dolt backup sync bac1
    [ "$status" -eq 1 ]
    [[ ! "$output" =~ "panic" ]] || false
    [[ "$output" =~ "unknown backup: 'bac1'" ]] || false
}

@test "backup: cannot clone a backup" {
    skip_nbf_dolt_1
    skip "todo implement backup lock file"

    cd repo1
    dolt backup add bac1 file://../bac1
    dolt backup sync bac1

    cd ..
    dolt clone file://./bac1 repo2
    [ "$status" -eq 1 ]
    [[ ! "$output" =~ "panic" ]] || false
    [[ "$output" =~ "cannot clone backup" ]] || false
}

@test "backup: cannot add backup with address of existing remote" {
    skip_nbf_dolt_1
    cd repo1
    dolt remote add rem1 file://../bac1
    run dolt backup add bac1 file://../bac1
    [ "$status" -eq 1 ]
    [[ ! "$output" =~ "panic" ]] || false
    [[ "$output" =~ "address conflict with a remote: 'rem1'" ]] || false
}

@test "backup: cannot add backup with address of existing backup" {
    skip_nbf_dolt_1
    cd repo1
    dolt backup add bac1 file://../bac1
    run dolt backup add bac2 file://../bac1
    [ "$status" -eq 1 ]
    [[ ! "$output" =~ "panic" ]] || false
    [[ "$output" =~ "address conflict with a remote: 'bac1'" ]] || false
}

@test "backup: cannot add remote with address of existing backup" {
    skip_nbf_dolt_1
    cd repo1
    dolt backup add bac1 file://../bac1
    run dolt remote add rem1 file://../bac1
    [ "$status" -eq 1 ]
    [[ ! "$output" =~ "panic" ]] || false
    [[ "$output" =~ "address conflict with a remote: 'bac1'" ]] || false
}
