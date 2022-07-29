#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}

@test "integration: first-hour-db" {
    dolt clone dolthub/first-hour-db
    cd first-hour-db
    dolt migrate
}

@test "integration: us-jails" {
    dolt clone dolthub/us-jails
    cd us-jails
    dolt migrate
}

@test "integration: us-schools" {
    dolt clone dolthub/us-schools
    cd us-schools
    dolt migrate
}

@test "integration: SHAQ" {
    dolt clone dolthub/SHAQ
    cd SHAQ
    dolt migrate
}
