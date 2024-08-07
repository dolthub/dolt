#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "ci: dolt_ci_workflows table should exist on initialized database" {
    run dolt sql -q "select * from dolt_ci_workflows;"
    [ "$status" -eq 0 ]
}

@test "ci: dolt_ci_workflows should allow user inserts and updates" {
    run dolt sql -q "insert into dolt_ci_workflows (id, name, created_at, updated_at) values (uuid(), 'workflow_1', current_timestamp, current_timestamp);"
    [ "$status" -eq 0 ]

    dolt sql -q "insert into dolt_ci_workflows (id, name, created_at, updated_at) values (uuid(), 'workflow_1', current_timestamp, current_timestamp);"
    run dolt sql -q "select * from dolt_ci_workflows;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "workflow_1" ]] || false

    run dolt add dolt_ci_workflows
    [ "$status" -eq 0 ]

    run dolt commit -m 'add dolt_ci_workflow entry'
    [ "$status" -eq 0 ]

    run dolt sql -q "select * from dolt_ci_workflows;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "workflow_1" ]] || false

    run dolt sql -q "update dolt_ci_workflows set name = 'workflow_2' where name = 'workflow_1'"
    [ "$status" -eq 0 ]

    run dolt add dolt_ci_workflows
    [ "$status" -eq 0 ]

    run dolt commit -m 'update dolt_ci_workflow entry'
    [ "$status" -eq 0 ]

    run dolt sql -q "select * from dolt_ci_workflows;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "workflow_2" ]] || false
}

@test "ci: dolt_ci_workflows should not allow users to alter the schema" {
    run dolt sql -q "show create table dolt_ci_workflows;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "name" ]] || false

    run dolt sql -q "alter table dolt_ci_workflows add column test_col int;"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "table dolt_ci_workflows cannot be altered" ]] || false

    run dolt sql -q "alter table dolt_ci_workflows drop column name;"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "table dolt_ci_workflows cannot be altered" ]] || false
}
