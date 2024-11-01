#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

skip_remote_engine() {
    if [ "$SQL_ENGINE" = "remote-engine" ]; then
     skip "session ctx in shell is not the same as session in server"
    fi
}

@test "ci: init should create dolt ci workflow tables" {
    skip_remote_engine

    run dolt ci init
    [ "$status" -eq 0 ]

    run dolt log -n 1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully initialized Dolt CI" ]] || false

    run dolt sql -q "select * from dolt_ci_workflows;"
    [ "$status" -eq 0 ]

    run dolt sql -q "select * from dolt_ci_workflow_events;"
    [ "$status" -eq 0 ]

    run dolt sql -q "select * from dolt_ci_workflow_event_triggers;"
    [ "$status" -eq 0 ]

    run dolt sql -q "select * from dolt_ci_workflow_event_trigger_branches;"
    [ "$status" -eq 0 ]

    run dolt sql -q "select * from dolt_ci_workflow_event_trigger_activities;"
    [ "$status" -eq 0 ]

    run dolt sql -q "select * from dolt_ci_workflow_jobs;"
    [ "$status" -eq 0 ]

    run dolt sql -q "select * from dolt_ci_workflow_steps;"
    [ "$status" -eq 0 ]

    run dolt sql -q "select * from dolt_ci_workflow_saved_query_steps;"
    [ "$status" -eq 0 ]

    run dolt sql -q "select * from dolt_ci_workflow_saved_query_step_expected_row_column_results;"
    [ "$status" -eq 0 ]
}

@test "ci: destroy should destroy dolt ci workflow tables" {
    skip_remote_engine

    run dolt ci init
    [ "$status" -eq 0 ]

    run dolt log -n 1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully initialized Dolt CI" ]] || false

    run dolt sql -q "insert into dolt_ci_workflows values('workflow_1', now(), now());"
    [ "$status" -eq 0 ]

    run dolt sql -q "insert into dolt_ci_workflow_events values(uuid(), 'workflow_1', 1);"
    [ "$status" -eq 0 ]

    run dolt ci destroy
    [ "$status" -eq 0 ]

    run dolt log -n 1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully destroyed Dolt CI" ]] || false

    run dolt sql -q "select * from dolt_ci_workflows;"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "table not found: dolt_ci_workflows" ]] || false
}

@test "ci: should allow users to alter data in workflow tables" {
    skip_remote_engine

    run dolt ci init
    [ "$status" -eq 0 ]

    run dolt sql -q "show create table dolt_ci_workflows;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "name" ]] || false

    run dolt sql -q "insert into dolt_ci_workflows (name, created_at, updated_at) values ('workflow_1', current_timestamp, current_timestamp);"
    [ "$status" -eq 0 ]

    run dolt sql -q "select * from dolt_ci_workflows;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "workflow_1" ]] || false
}
