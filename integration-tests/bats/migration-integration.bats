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

    run dolt tag -v
    [ "$status" -eq 0 ]
    [[ "$output" =~ "r9jv07tf9un3fm1fg72v7ad9er89oeo7" ]] || false
    [[ ! "$output" =~ "ovpnp265d9cubjeo9qf0ts10piq7c70d" ]] || false

    dolt migrate
    [[ $(cat ./.dolt/noms/manifest | cut -f 2 -d :) = "$TARGET_NBF" ]] || false

    run dolt tag -v
    [ "$status" -eq 0 ]
    [[ "$output" =~ "ovpnp265d9cubjeo9qf0ts10piq7c70d" ]] || false
    [[ ! "$output" =~ "r9jv07tf9un3fm1fg72v7ad9er89oeo7" ]] || false
}

@test "migration-integration: us-jails" {
    dolt clone dolthub/us-jails
    cd us-jails

    run dolt tag -v
    [ "$status" -eq 0 ]
    [[ "$output" =~ "u8s83gapv7ghnbmrtpm8q5es0dbl7lpd" ]] || false
    [[ ! "$output" =~ "t25l8d0u3tp1tul8o9ttf8k3t5a24n4q" ]] || false

    dolt migrate
    [[ $(cat ./.dolt/noms/manifest | cut -f 2 -d :) = "$TARGET_NBF" ]] || false

    run dolt tag -v
    [ "$status" -eq 0 ]
    [[ "$output" =~ "t25l8d0u3tp1tul8o9ttf8k3t5a24n4q" ]] || false
    [[ ! "$output" =~ "u8s83gapv7ghnbmrtpm8q5es0dbl7lpd" ]] || false
}
