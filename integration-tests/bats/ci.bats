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

get_commit_hash() {
  local logline=$(dolt log -n "$1")
  echo ${logline:12:32}
}

@test "ci: init should create dolt ci workflow tables" {
    dolt ci init

    run dolt log -n 1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully initialized Dolt CI" ]] || false

    dolt sql -q "select * from dolt_ci_workflows;"
    dolt sql -q "select * from dolt_ci_workflow_events;"
    dolt sql -q "select * from dolt_ci_workflow_event_triggers;"
    dolt sql -q "select * from dolt_ci_workflow_event_trigger_branches;"
    dolt sql -q "select * from dolt_ci_workflow_jobs;"
    dolt sql -q "select * from dolt_ci_workflow_steps;"
    dolt sql -q "select * from dolt_ci_workflow_saved_query_steps;"
    dolt sql -q "select * from dolt_ci_workflow_saved_query_step_expected_row_column_results;"
}

@test "ci: destroy should destroy dolt ci workflow tables" {
    dolt ci init

    run dolt log -n 1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully initialized Dolt CI" ]] || false

    dolt sql -q "insert into dolt_ci_workflows values('workflow_1', now(), now());"
    dolt sql -q "insert into dolt_ci_workflow_events values(uuid(), 1, 'workflow_1');"
    dolt ci destroy

    run dolt log -n 1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully destroyed Dolt CI" ]] || false

    run dolt sql -q "select * from dolt_ci_workflows;"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "table not found: dolt_ci_workflows" ]] || false
}

@test "ci: init should error if database has already initialized ci" {
    dolt ci init
    run dolt ci init
    [ "$status" -eq 1 ]
    [[ "$output" =~ "dolt ci has already been initialized" ]] || false
}

@test "ci: user cannot manually create ci tables in dolt_ci namespace" {
    run dolt sql -q "create table dolt_ci_workflows(pk int primary key);"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Invalid table name" ]] || false

    dolt sql -q "create table workflows(pk int primary key);"
    run dolt sql -q "rename table workflows to dolt_ci_workflows;"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Invalid table name" ]] || false
}

@test "ci: workflow tables do not appear in show tables output" {
    dolt ci init
    run dolt sql -q "show tables;"
    [ "$status" -eq 0 ]
    [[ ${output} != *"dolt_ci"* ]] || false
}

@test "ci: workflow tables do not appear in dolt ls" {
    dolt ci init
    run dolt ls
    [ "$status" -eq 0 ]
    [[ ${output} != *"dolt_ci"* ]] || false
}

@test "ci: workflow tables do appear in diffs" {
    first=$(get_commit_hash 1)

    dolt ci init
    last=$(get_commit_hash 1)
    run dolt diff "$first" "$last" --system
    [ "$status" -eq 0 ]
    [[ ${output} == *"dolt_ci"* ]] || false
}

@test "ci: init command should only commit changes relevant to the ci tables" {
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
    cat > workflow.yaml <<EOF
name: my_workflow
on:
  push:
    branches:
      - master
jobs:
  - name: validate tables
    steps:
      - name: assert expected tables exist
        saved_query_name: show tables
        expected_rows: "== 2"
EOF
    dolt ci init
    dolt ci import ./workflow.yaml
    run dolt ci ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "my_workflow" ]] || false
}

@test "ci: import command will error on an invalid workflow.yaml file" {
    cat > workflow.yaml <<EOF
name: my_workflow
on:
  push:
    branches:
      - master
jobs:
name: validate tables
    steps:
      - name: assert expected tables exist
        saved_query_name: show tables
        expected_rows: "== 2"
EOF
    dolt ci init
    run dolt ci import ./workflow.yaml
    [ "$status" -eq 1 ]
}

@test "ci: import command will update existing workflow" {
    cat > workflow_1.yaml <<EOF
name: workflow_1
on:
  push:
    branches:
      - master
jobs:
  - name: validate tables
    steps:
      - name: assert expected tables exist
        saved_query_name: show tables
        expected_rows: "== 2"
EOF
    dolt ci init
    dolt ci import ./workflow_1.yaml
    original=$(get_commit_hash 1)
    cat > workflow_1_updated.yaml <<EOF
name: workflow_1
on:
  push:
    branches:
      - master
jobs:
  - name: validate tables (new)
    steps:
      - name: assert expected tables exist (new)
        saved_query_name: show tables
        expected_rows: "!= 2"
EOF
    dolt ci import ./workflow_1_updated.yaml
    updated=$(get_commit_hash 1)
    run dolt diff "$original" "$updated" --system
    [ "$status" -eq 0 ]
    [[ ${output} == *"(new)"* ]] || false
    [[ ${output} == *"dolt_ci_workflow_steps"* ]] || false
    [[ ${output} == *"expected_row_count_comparison_type"* ]] || false
    [[ ${output} == *"dolt_ci_workflow_saved_query_step_expected_row_column_results"* ]] || false
}

@test "ci: import command will not update existing workflow if there are not changes detected" {
    cat > workflow.yaml <<EOF
name: my first DoltHub workflow
on:
  push:
    branches:
      - master
jobs:
  - name: validate tables
    steps:
      - name: assert expected tables exist
        saved_query_name: show tables
        expected_rows: "== 2"
      - name: assert table option_chain exists
        saved_query_name: option_chain exists
      - name: assert table volatility_history
        saved_query_name: volatility_history exists
  - name: validate schema
    steps:
      - name: assert 13 option_chain columns exist
        saved_query_name: check option_chain column length
        expected_columns: "<= 13"
      - name: assert call_put column exist
        saved_query_name: check option_chain.call_put exists
        expected_columns: "== 1"
      - name: assert 16 volatility_history columns exist
        saved_query_name: check volatility_history column length
        expected_columns: ">= 16"
      - name: assert act_symbol column exist
        saved_query_name: check volatility_history.act_symbol exists
        expected_columns: "== 1"
  - name: check data
    steps:
      - name: assert option_chain table has data
        saved_query_name: check option_chain data
        expected_rows: "> 0"
      - name: assert volatility_history table has data
        saved_query_name: check volatility_history data
        expected_rows: "> 0"
EOF

    dolt ci init
    dolt ci import ./workflow.yaml
    run dolt ci import ./workflow.yaml
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Dolt CI Workflow 'my first DoltHub workflow' up to date." ]] || false
}

@test "ci: ls lists existing workflows" {
    cat > workflow.yaml <<EOF
name: my_workflow
on:
  push:
    branches:
      - master
jobs:
  - name: validate tables
    steps:
      - name: assert expected tables exist
        saved_query_name: show tables
        expected_rows: "== 2"
EOF
    dolt ci init
    dolt ci import ./workflow.yaml
    run dolt ci ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "my_workflow" ]] || false
}

@test "ci: export exports a workflow to a yaml file" {
    cat > workflow.yaml <<EOF
name: my_workflow
on:
  push:
    branches:
      - master
jobs:
  - name: validate tables
    steps:
      - name: assert expected tables exist
        saved_query_name: show tables
        expected_rows: "== 2"
EOF
    dolt ci init
    dolt ci import ./workflow.yaml
    run dolt ci export "my_workflow"
    [ "$status" -eq 0 ]
    cat my_workflow.yaml
    run cat my_workflow.yaml
    [ "$status" -eq 0 ]
    [[ ${output} == *"name"* ]] || false
    [[ ${output} == *"push:"* ]] || false
    [[ ${output} == *"branches:"* ]] || false
    [[ ${output} == *"jobs:"* ]] || false
    [[ ${output} == *"steps:"* ]] || false
}

@test "ci: export errors on invalid workflow" {
    dolt ci init
    run dolt ci export invalid
    [ "$status" -eq 1 ]
    [[ "$output" =~ "workflow not found" ]] || false
}

@test "ci: remove deletes a workflow" {
    cat > workflow_1.yaml <<EOF
name: workflow_1
on:
  push:
    branches:
      - master
jobs:
  - name: validate tables
    steps:
      - name: assert expected tables exist
        saved_query_name: show tables
        expected_rows: "== 2"
EOF
    cat > workflow_2.yaml <<EOF
name: workflow_2
on:
  push:
    branches:
      - master
jobs:
  - name: validate tables
    steps:
      - name: assert expected tables exist
        saved_query_name: show tables
        expected_rows: "== 2"
EOF
    dolt ci init
    dolt ci import ./workflow_1.yaml
    dolt ci import ./workflow_2.yaml
    run dolt ci ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "workflow_1" ]] || false
    [[ "$output" =~ "workflow_2" ]] || false
    run dolt ci remove "workflow_1"
    [ "$status" -eq 0 ]
    run dolt ci ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "workflow_2" ]] || false
}

@test "ci: remove errors on invalid workflow" {
    dolt ci init
    run dolt ci remove invalid
    [ "$status" -eq 1 ]
    [[ "$output" =~ "workflow not found" ]] || false
}

@test "ci: dolt ci view shows ci" {
    cat > workflow.yaml <<EOF
name: workflow
on:
  push:
    branches:
      - master
jobs:
  - name: validate tables
    steps:
      - name: assert expected tables exist
        saved_query_name: get tables
        expected_rows: "== 2"
EOF

    dolt ci init
    dolt sql --save "get tables" -q "show tables"
    dolt ci import ./workflow.yaml

    run dolt ci view "workflow"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "name: \"workflow\"" ]] || false
    [[ "$output" =~ "saved_query_name: \"get tables\"" ]] || false
    [[ "$output" =~ "saved_query_statement: \"show tables\"" ]] || false
}

@test "ci: dolt ci view errors on invalid workflow" {
    dolt ci init
    run dolt ci view "invalid"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "workflow not found" ]] || false
}

@test "ci: dolt ci view labels undefined saved queries" {
    cat > workflow_1.yaml <<EOF
name: workflow_1
on:
  push:
    branches:
      - master
jobs:
  - name: validate tables
    steps:
      - name: assert expected tables exist
        saved_query_name: show tables
        expected_rows: "== 2"
EOF

    dolt ci init
    dolt ci import ./workflow_1.yaml
    run dolt ci view "workflow_1"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "saved_query_statement: \"saved query not found\"" ]] || false
}

@test "ci: can use --job with dolt ci view to filter workflow" {
    cat > workflow_1.yaml <<EOF
name: workflow_1
on:
  push:
    branches:
      - master
jobs:
  - name: validate tables
    steps:
      - name: assert expected tables exist
        saved_query_name: get tables
        expected_rows: "== 2"
EOF

    dolt ci init
    dolt ci import ./workflow_1.yaml
    run dolt ci view "workflow_1" --job "validate tables"
    [ "$status" -eq 0 ]
    ! [[ "$output" =~ "workflow_1" ]] || false
    ! [[ "$output" =~ "master" ]] || false
    [[ "$output" =~ "name: \"validate tables\"" ]] || false
    [[ "$output" =~ "saved_query_name: \"get tables\"" ]] || false
}

@test "ci: view with --job option errors on bad job name" {
    cat > workflow_1.yaml <<EOF
name: workflow_1
on:
  push:
    branches:
      - master
jobs:
  - name: validate tables
    steps:
      - name: assert expected tables exist
        saved_query_name: get tables
        expected_rows: "== 2"
EOF

    dolt ci init
    dolt ci import ./workflow_1.yaml
    run dolt ci view "workflow_1" --job "invalid job"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "cannot find job with name: invalid job" ]] || false
}