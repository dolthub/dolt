#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}

@test "ls empty creds" {
    run dolt creds ls
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 0 ]
}

@test "ls new cred" {
    run dolt creds new
    [ "$status" -eq 0 ]
    run dolt creds ls
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]
    [[ "${lines[0]}" =~ ^\ \  ]] || false
}

@test "ls -v new creds" {
    run dolt creds new
    [ "$status" -eq 0 ]
    run dolt creds new
    [ "$status" -eq 0 ]
    run dolt creds ls -v
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 4 ]
    [[ "${lines[0]}" =~ public\ key ]] || false
    [[ "${lines[0]}" =~ key\ id ]] || false
}

@test "rm removes a cred" {
    run dolt creds new
    [ "$status" -eq 0 ]
    run dolt creds ls
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]
    words=( ${lines[0]} )
    run dolt creds rm ${words[0]}
    [ "$status" -eq 0 ]
    run dolt creds ls
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 0 ]
}
