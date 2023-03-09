#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    skip_nbf_dolt

    setup_common
    TARGET_NBF="__DOLT__"
}

teardown() {
    teardown_common
}

@test "migration-integration: first-hour-db" {
    dolt clone https://doltremoteapi.dolthub.com/dolthub/first-hour-db-migration-int
    cd first-hour-db-migration-int

    dolt tag -v
    run dolt tag -v
    [ "$status" -eq 0 ]
    [[ "$output" =~ "r9jv07tf9un3fm1fg72v7ad9er89oeo7" ]] || false
    [[ ! "$output" =~ "d0q6hb3vcq1oe178usc6rd28db1cnh26" ]] || false

    dolt migrate
    [[ $(cat ./.dolt/noms/manifest | cut -f 2 -d :) = "$TARGET_NBF" ]] || false

    dolt tag -v
    run dolt tag -v
    [ "$status" -eq 0 ]
    [[ "$output" =~ "d0q6hb3vcq1oe178usc6rd28db1cnh26" ]] || false
    [[ ! "$output" =~ "r9jv07tf9un3fm1fg72v7ad9er89oeo7" ]] || false

    # validate TEXT migration
    run dolt sql -q "select film_id, title from film order by film_id limit 1" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,ACADEMY DINOSAUR" ]] || false
}

@test "migration-integration: first-hour-db after garbage collection" {
    dolt clone https://doltremoteapi.dolthub.com/dolthub/first-hour-db-migration-int
    cd first-hour-db-migration-int
    dolt gc

    dolt tag -v
    run dolt tag -v
    [ "$status" -eq 0 ]
    [[ "$output" =~ "r9jv07tf9un3fm1fg72v7ad9er89oeo7" ]] || false
    [[ ! "$output" =~ "d0q6hb3vcq1oe178usc6rd28db1cnh26" ]] || false

    dolt migrate
    [[ $(cat ./.dolt/noms/manifest | cut -f 2 -d :) = "$TARGET_NBF" ]] || false

    dolt tag -v
    run dolt tag -v
    [ "$status" -eq 0 ]
    [[ "$output" =~ "d0q6hb3vcq1oe178usc6rd28db1cnh26" ]] || false
    [[ ! "$output" =~ "r9jv07tf9un3fm1fg72v7ad9er89oeo7" ]] || false

    # validate TEXT migration
    run dolt sql -q "select film_id, title from film order by film_id limit 1" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,ACADEMY DINOSAUR" ]] || false
}

@test "migration-integration: us-jails" {
    dolt clone https://doltremoteapi.dolthub.com/dolthub/us-jails-migration-integration
    cd us-jails-migration-integration

    dolt tag -v
    run dolt tag -v
    [ "$status" -eq 0 ]
    [[ "$output" =~ "u8s83gapv7ghnbmrtpm8q5es0dbl7lpd" ]] || false
    [[ ! "$output" =~ "nm1ubfu85p4c082bhertltrhkkeffaqg" ]] || false

    dolt migrate
    [[ $(cat ./.dolt/noms/manifest | cut -f 2 -d :) = "$TARGET_NBF" ]] || false

    dolt tag -v
    run dolt tag -v
    [ "$status" -eq 0 ]
    [[ "$output" =~ "nm1ubfu85p4c082bhertltrhkkeffaqg" ]] || false
    [[ ! "$output" =~ "u8s83gapv7ghnbmrtpm8q5es0dbl7lpd" ]] || false
}
