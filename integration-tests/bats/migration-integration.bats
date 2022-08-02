#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    skip_nbf_dolt_1
    skip_nbf_dolt_dev

    setup_common
    TARGET_NBF="__DOLT_1__"
}

teardown() {
    teardown_common
}

@test "migration-integration: first-hour-db" {
    dolt clone dolthub/first-hour-db
    cd first-hour-db

    dolt tag -v
    run dolt tag -v
    [ "$status" -eq 0 ]
    [[ "$output" =~ "r9jv07tf9un3fm1fg72v7ad9er89oeo7" ]] || false
    [[ ! "$output" =~ "popqo96mjvhsaumd3rbba9m56f1oij7h" ]] || false

    dolt migrate
    [[ $(cat ./.dolt/noms/manifest | cut -f 2 -d :) = "$TARGET_NBF" ]] || false

    dolt tag -v
    run dolt tag -v
    [ "$status" -eq 0 ]
    [[ "$output" =~ "popqo96mjvhsaumd3rbba9m56f1oij7h" ]] || false
    [[ ! "$output" =~ "r9jv07tf9un3fm1fg72v7ad9er89oeo7" ]] || false
}

@test "migration-integration: us-jails" {
    dolt clone dolthub/us-jails
    cd us-jails

    dolt tag -v
    run dolt tag -v
    [ "$status" -eq 0 ]
    [[ "$output" =~ "u8s83gapv7ghnbmrtpm8q5es0dbl7lpd" ]] || false
    [[ ! "$output" =~ "k0hgumfrd2i891h1nh172cfutih5n6ea" ]] || false

    dolt migrate
    [[ $(cat ./.dolt/noms/manifest | cut -f 2 -d :) = "$TARGET_NBF" ]] || false

    dolt tag -v
    run dolt tag -v
    [ "$status" -eq 0 ]
    [[ "$output" =~ "k0hgumfrd2i891h1nh172cfutih5n6ea" ]] || false
    [[ ! "$output" =~ "u8s83gapv7ghnbmrtpm8q5es0dbl7lpd" ]] || false
}
