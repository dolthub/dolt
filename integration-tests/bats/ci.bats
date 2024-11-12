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

    dolt ci init

    run dolt log -n 1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully initialized Dolt CI" ]] || false

    dolt sql -q "select * from dolt_ci_workflows;"
    dolt sql -q "select * from dolt_ci_workflow_events;"
    dolt sql -q "select * from dolt_ci_workflow_event_triggers;"
    dolt sql -q "select * from dolt_ci_workflow_event_trigger_branches;"
    dolt sql -q "select * from dolt_ci_workflow_event_trigger_activities;"
    dolt sql -q "select * from dolt_ci_workflow_jobs;"
    dolt sql -q "select * from dolt_ci_workflow_steps;"
    dolt sql -q "select * from dolt_ci_workflow_saved_query_steps;"
    dolt sql -q "select * from dolt_ci_workflow_saved_query_step_expected_row_column_results;"
}

@test "ci: destroy should destroy dolt ci workflow tables" {
    skip_remote_engine

    dolt ci init

    run dolt log -n 1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully initialized Dolt CI" ]] || false

    dolt sql -q "insert into dolt_ci_workflows values('workflow_1', now(), now());"
    dolt sql -q "insert into dolt_ci_workflow_events values(uuid(), 'workflow_1', 1);"
    dolt ci destroy

    run dolt log -n 1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully destroyed Dolt CI" ]] || false

    run dolt sql -q "select * from dolt_ci_workflows;"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "table not found: dolt_ci_workflows" ]] || false
}

@test "ci: should allow users to alter data in workflow tables" {
    skip_remote_engine

    dolt ci init

    run dolt sql -q "show create table dolt_ci_workflows;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "name" ]] || false

    dolt sql -q "insert into dolt_ci_workflows (name, created_at, updated_at) values ('workflow_1', current_timestamp, current_timestamp);"

    run dolt sql -q "select * from dolt_ci_workflows;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "workflow_1" ]] || false
}

@test "ci: init should error if database has already initialized ci" {
    skip_remote_engine

    dolt ci init
    run dolt ci init
    [ "$status" -eq 1 ]
    [[ "$output" =~ "dolt ci has already been initialized" ]] || false
}

@test "ci: user cannot manually create ci tables in dolt_ci namespace" {
    skip_remote_engine

    run dolt sql -q "create table dolt_ci_workflows(pk int primary key);"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Invalid table name" ]] || false
}

@test "ci: workflow tables do not appear in show tables output" {
    skip_remote_engine
}

@test "ci: workflow tables do not appear in dolt ls" {
    skip_remote_engine
}

@test "ci: init command should only commit changes relevant to the ci tables" {
    skip_remote_engine

    dolt sql -q "create table t1(pk int primary key);"
    dolt ci init
    dolt status
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Untracked tables:" ]] || false
    [[ "$output" =~ "  (use \"dolt add <table>\" to include in what will be committed)" ]] || false
    [[ "$output" =~ "	new table:        t1" ]] || false
}

@test "ci: destroy command should only commit changes relevant to the ci tables" {
    skip_remote_engine

    dolt sql -q "create table t1(pk int primary key);"
    dolt ci init
    dolt ci destroy
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Untracked tables:" ]] || false
    [[ "$output" =~ "  (use \"dolt add <table>\" to include in what will be committed)" ]] || false
    [[ "$output" =~ "	new table:        t1" ]] || false
}

@test "ci: import command will import a valid workflow.yaml file" {
    skip_remote_engine
}

@test "ci: import command will error on an invalid workflow.yaml file" {
    skip_remote_engine
}

@test "ci: import command will update existing workflow" {
    skip_remote_engine
}

@test "ci: export exports a workflow to a yaml file" {
    skip_remote_engine
}

@test "ci: ls lists existing workflows" {
    skip_remote_engine
}
