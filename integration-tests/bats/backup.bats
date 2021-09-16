#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    TMPDIRS=$(pwd)/tmpdirs
    mkdir -p $TMPDIRS/{rem1,bac1,repo1}

    # repo1 -> rem1 -> repo2
    cd $TMPDIRS/repo1
    dolt init
    dolt branch feature
    dolt tag v1
    dolt backup add bac1 file://../bac1
    cd $TMPDIRS
}

teardown() {
    teardown_common
    rm -rf $TMPDIRS
    cd $BATS_TMPDIR
}

@test "backup: push to backup" {
    cd repo1
    dolt remote add origin file://../rem1
    dolt backup push bac1

    run noms ds ../bac1
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 4 ]
    [[ "$output" =~ "v1" ]] || false
    [[ "$output" =~ "refs/heads/feature" ]] || false
    [[ "$output" =~ "refs/heads/master" ]] || false
    [[ "$output" =~ "refs/internal/create" ]] || false
    [[ "$output" =~ "refs/tags/v1" ]] || false
}


@test "backup: clone from backup" {
    cd repo1
    dolt remote add origin file://../rem1
    dolt backup push bac1

    cd ..
    dolt clone file://./bac1 repo2
    cd repo2

    run dolt tag
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]
    [[ "$output" =~ "v1" ]] || false

    dolt branch
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "master" ]] || false
    [[ "$output" =~ "feature" ]] || false
    [ "$status" -eq 0 ]
}
