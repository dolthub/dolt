#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}

@test "dolt migrate --push & dolt migrate --pull" {
    pushd "$TEST_REPO"

    run dolt migrate --pull
    [ "$status" -ne "0" ]
    [[ "$output" =~ "Local repo must be migrated before pulling, run 'dolt migrate'" ]] || false

    run dolt migrate --push
    [ "$status" -ne "0" ]
    [[ "$output" =~ "Local repo must be migrated before pushing, run 'dolt migrate'" ]] || false

    run dolt migrate
    [ "$status" -eq "0" ]
    [[ "$output" =~ "Migrating repository to the latest format" ]] || false

    run dolt migrate --pull
    [ "$status" -ne "0" ]
    [[ "$output" =~ "Remote origin has not been migrated" ]] || false
    [[ "$output" =~ "Run 'dolt migrate --push origin' to push migration" ]] || false

    run dolt migrate --push
    [ "$status" -eq "0" ]

    popd
    pushd "$TEST_REPO-clone"

    run dolt migrate --pull
    [ "$status" -ne "0" ]
    [[ "$output" =~ "Local repo must be migrated before pulling, run 'dolt migrate'" ]] || false

    run dolt migrate --push
    [ "$status" -ne "0" ]
    [[ "$output" =~ "Local repo must be migrated before pushing, run 'dolt migrate'" ]] || false

    run dolt migrate
    [ "$status" -eq "0" ]
    [[ "$output" =~ "Migrating repository to the latest format" ]] || false

    run dolt migrate --push
    [ "$status" -ne "0" ]
    [[ "$output" =~ "Remote origin has been migrated" ]] || false
    [[ "$output" =~ "Run 'dolt migrate --pull' to update refs" ]] || false

    run dolt migrate --pull
    [ "$status" -eq "0" ]

    popd
}
