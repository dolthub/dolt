#! /usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_no_dolt_init
}

teardown() {
    teardown_common
}

@test "fsck" {
    mkdir ".dolt"
    cp -R "$BATS_TEST_DIRNAME/corrupt_dbs/bad_commit/" .dolt/

    dolt status

    run dolt fsck

    [ "$status" -eq 1 ]
    [[ "$output" =~ "rlmgv0komq0oj7qu4osdo759vs4c5pvg read with incorrect checksum: gpphmuvegiedtjtbfku4ru8jalfdk21u" ]]
}