#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    TMPDIRS=$(pwd)/tmpdirs
    mkdir -p $TMPDIRS/{rem1,bac1,repo1}

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
    cd repo1
    dolt backup add bac1 file://../bac1
    run dolt backup -v
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]
    [[ "$output" =~ "bac1" ]] || false
}

@test "backup: remove named backup" {
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

@test "backup: sync master to backup" {
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
    cd repo1
    dolt backup add bac1 file://../bac1
    dolt backup sync bac1

    cd ..
    dolt backup restore file://./bac1 repo2
    cd repo2
    noms ds .dolt/noms
    run dolt sql -q "show tables as of hashof('origin/main')" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "t1" ]] || false
}

@test "backup: sync working set to backup" {
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
    cd repo1
    dolt backup add bac1 file://../bac1
    dolt backup sync bac1
    run dolt backup sync bac1
    [ "$status" -eq 1 ]
    [[ ! "$output" =~ "panic" ]] || false
    [[ "$output" =~ "backup already up to date" ]] || false
}
