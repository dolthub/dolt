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
    run dolt ci init
    [ "$status" -eq 0 ]

    run dolt sql -q "select * from dolt_ci_workflows;"
    [ "$status" -eq 0 ]
}

@test "ci: dolt_ci_workflows should not allow users to alter the rows or schema of dolt ci workflow tables directly" {
    run dolt ci init
    [ "$status" -eq 0 ]

    run dolt sql -q "show create table dolt_ci_workflows;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "name" ]] || false

    run dolt sql -q "insert into dolt_ci_workflows (name, created_at, updated_at) values ('workflow_1', current_timestamp, current_timestamp);"
    [ "$status" -eq 1 ]

    run dolt sql -q "alter table dolt_ci_workflows add column test_col int;"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "table dolt_ci_workflows cannot be altered" ]] || false

    run dolt sql -q "alter table dolt_ci_workflows drop column name;"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "table dolt_ci_workflows cannot be altered" ]] || false
}
