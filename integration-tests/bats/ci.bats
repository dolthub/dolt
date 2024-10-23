#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "ci: init should create dolt ci workflow tables" {
    dolt ci init
    run dolt ci init
    [ "$status" -eq 0 ]

    run dolt log -n 1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully created Dolt CI tables" ]] || false

    run dolt sql -q "select * from dolt_ci_workflows;"
    [ "$status" -eq 0 ]

    run dolt sql -q "select * from dolt_ci_workflow_events;"
    [ "$status" -eq 0 ]

    run dolt sql -q "select * from dolt_ci_workflow_event_triggers;"
    [ "$status" -eq 0 ]
}

