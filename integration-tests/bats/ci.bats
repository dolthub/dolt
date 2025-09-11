#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
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

@test "ci: import command will error on invalid branches" {
    cat > workflow.yaml <<EOF
name: my_workflow
on:
  push:
    branches:
      - ..invalid
jobs:
  - name: test job
    steps:
      - name: test step
        saved_query_name: test query
EOF
    cat > workflow2.yaml <<EOF
name: workflow
on:
  push:
    branches:
      - "*"
jobs:
  - name: test job
    steps:
      - name: test step
        saved_query_name: test query
EOF

    dolt ci init
    run dolt ci import workflow.yaml
    [ "$status" -eq 1 ]
    [[ "$output" =~ "invalid branch name: ..invalid" ]] || false
    run dolt ci import workflow2.yaml
    [ "$status" -eq 1 ]
    [[ "$output" =~ "invalid branch name: *" ]] || false
}

@test "ci: import supports DoltTest steps (tests wildcard and specific)" {
    cat > workflow.yaml <<EOF
name: wf_dolt_test_only
on:
  push: {}
jobs:
  - name: run tests
    steps:
      - name: run all tests (wildcard groups)
        dolt_test_groups:
          - "*"
      - name: run specific tests
        dolt_test_tests:
          - test_a
          - test_b
EOF

    dolt ci init
    run dolt ci import ./workflow.yaml
    [ "$status" -eq 0 ]
    run dolt ci ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "wf_dolt_test_only" ]] || false

    # Verify DoltTest rows inserted
    run dolt sql -q "select group_name from dolt_ci_workflow_dolt_test_step_groups where group_name='*';"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "*" ]] || false

    dolt sql -q "select test_name from dolt_ci_workflow_dolt_test_step_tests where test_name in ('test_a','test_b');"
    run dolt sql -q "select test_name from dolt_ci_workflow_dolt_test_step_tests where test_name in ('test_a','test_b');"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test_a" ]] || false
    [[ "$output" =~ "test_b" ]] || false
}

@test "ci: import supports mixed SavedQuery and DoltTest steps" {
    cat > workflow.yaml <<EOF
name: wf_mixed
on:
  push: {}
jobs:
  - name: validate and test
    steps:
      - name: ensure tables listed
        saved_query_name: get tables
      - name: run groups a and b
        dolt_test_groups:
          - group_a
          - group_b
EOF

    dolt ci init
    run dolt ci import ./workflow.yaml
    [ "$status" -eq 0 ]
    run dolt ci ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "wf_mixed" ]] || false

    # Verify saved query step and dolt test group rows exist
    run dolt sql -q "select saved_query_name from dolt_ci_workflow_saved_query_steps where saved_query_name='get tables';"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "get tables" ]] || false

    run dolt sql -q "select group_name from dolt_ci_workflow_dolt_test_step_groups where group_name in ('group_a','group_b');"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "group_a" ]] || false
    [[ "$output" =~ "group_b" ]] || false
}

@test "ci: import supports DoltTest steps with both groups and tests" {
    cat > workflow.yaml <<EOF
name: wf_dolt_test_groups_and_tests
on:
  push: {}
jobs:
  - name: run selected tests in groups
    steps:
      - name: selected tests in selected groups
        dolt_test_groups:
          - g1
          - g2
        dolt_test_tests:
          - t1
EOF

    dolt ci init
    run dolt ci import ./workflow.yaml
    [ "$status" -eq 0 ]
    run dolt ci ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "wf_dolt_test_groups_and_tests" ]] || false

    run dolt sql -q "select group_name from dolt_ci_workflow_dolt_test_step_groups where group_name in ('g1','g2');"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "g1" ]] || false
    [[ "$output" =~ "g2" ]] || false

    run dolt sql -q "select test_name from dolt_ci_workflow_dolt_test_step_tests where test_name='t1';"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "t1" ]] || false
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

@test "ci: export includes DoltTest steps (groups and tests)" {
    cat > workflow.yaml <<EOF
name: wf_export_dolt_test
on:
  push: {}
jobs:
  - name: run tests
    steps:
      - name: run all group a
        dolt_test_groups:
          - group_a
      - name: run tests t1 and t2
        dolt_test_tests:
          - t1
          - t2
EOF

    dolt ci init
    dolt ci import ./workflow.yaml

    run dolt ci export "wf_export_dolt_test"
    [ "$status" -eq 0 ]
    run cat wf_export_dolt_test.yaml
    [ "$status" -eq 0 ]
    # Ensure the YAML contains the DoltTest steps and fields
    [[ "$output" =~ "name: \"wf_export_dolt_test\"" ]] || false
    [[ "$output" =~ "dolt_test_groups:" ]] || false
    [[ "$output" =~ "- \"group_a\"" ]] || false
    [[ "$output" =~ "dolt_test_tests:" ]] || false
    [[ "$output" =~ "- \"t1\"" ]] || false
    [[ "$output" =~ "- \"t2\"" ]] || false
}

@test "ci: export includes mixed SavedQuery and DoltTest steps" {
    cat > workflow.yaml <<EOF
name: wf_export_mixed
on:
  push: {}
jobs:
  - name: validate and test
    steps:
      - name: ensure tables listed
        saved_query_name: get tables
      - name: run group b
        dolt_test_groups:
          - group_b
EOF

    dolt ci init
    dolt ci import ./workflow.yaml

    run dolt ci export "wf_export_mixed"
    [ "$status" -eq 0 ]
    run cat wf_export_mixed.yaml
    [ "$status" -eq 0 ]
    [[ "$output" =~ "name: \"wf_export_mixed\"" ]] || false
    [[ "$output" =~ "saved_query_name: \"get tables\"" ]] || false
    [[ "$output" =~ "dolt_test_groups:" ]] || false
    [[ "$output" =~ "- \"group_b\"" ]] || false
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

@test "ci: run with expected rows" {
    cat > workflow.yaml <<EOF
name: workflow
on:
  push: {}
jobs:
  - name: verify initial commits
    steps:
      - name: "verify initial commits"
        saved_query_name: check dolt commit
        expected_rows: "== 3"
EOF
    dolt ci init
    dolt ci import ./workflow.yaml
    dolt sql --save "check dolt commit" -q "select * from dolt_commits;"
    run dolt ci run "workflow"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Running workflow: workflow" ]] || false
    [[ "$output" =~ "Running job: verify initial commits" ]] || false
    [[ "$output" =~ "Step: verify initial commits - PASS" ]] || false
}

@test "ci: ci run with expected columns" {
    cat > workflow.yaml <<EOF
name: workflow
on:
  push: {}
jobs:
  - name: verify dolt commit
    steps:
      - name: "verify dolt commit"
        saved_query_name: check dolt commit
        expected_columns: "== 5"
EOF
    dolt ci init
    dolt ci import ./workflow.yaml
    dolt sql --save "check dolt commit" -q "select * from dolt_commits;"
    run dolt ci run "workflow"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Running workflow: workflow" ]] || false
    [[ "$output" =~ "Running job: verify dolt commit" ]] || false
    [[ "$output" =~ "Step: verify dolt commit - PASS" ]] || false
}

@test "ci: each assertion type can be used" {
    cat > workflow.yaml <<EOF
name: workflow
on:
  push: {}
jobs:
  - name: check comparisons
    steps:
      - name: equals comp
        saved_query_name: main
        expected_columns: "== 5"
      - name: not equals comp
        saved_query_name: main
        expected_columns: "!= 1"
      - name: greater than comp
        saved_query_name: main
        expected_columns: "> 4"
      - name: greater or equal than comp
        saved_query_name: main
        expected_columns: ">= 5"
      - name: less than comp
        saved_query_name: main
        expected_columns: "< 6"
      - name: less or equal than comp
        saved_query_name: main
        expected_columns: "<= 5"
EOF
    dolt ci init
    dolt ci import ./workflow.yaml
    dolt sql --save "main" -q "select * from dolt_commits;"
    run dolt ci run "workflow"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Running workflow: workflow" ]] || false
    [[ "$output" =~ "Step: equals comp - PASS" ]] || false
    [[ "$output" =~ "Step: not equals comp - PASS" ]] || false
    [[ "$output" =~ "Step: greater than comp - PASS" ]] || false
    [[ "$output" =~ "Step: greater or equal than comp - PASS" ]] || false
    [[ "$output" =~ "Step: less than comp - PASS" ]] || false
    [[ "$output" =~ "Step: less or equal than comp - PASS" ]] || false
}

@test "ci: saved queries fail with ci run" {
    cat > workflow.yaml <<EOF
name: workflow
on:
  push: {}
jobs:
  - name: "bad query assertions"
    steps:
      - name: expect rows
        saved_query_name: main
        expected_rows: "== 2"
      - name: expect columns
        saved_query_name: main
        expected_columns: "< 5"
EOF
    dolt ci init
    dolt ci import ./workflow.yaml
    dolt sql --save "main" -q "select * from dolt_commits;"
    run dolt ci run "workflow"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Running workflow: workflow" ]] || false
    [[ "$output" =~ "Step: expect rows - FAIL" ]] || false
    [[ "$output" =~ "Ran query: select * from dolt_commits;" ]] || false
    [[ "$output" =~ "Assertion failed: expected row count 2, got 3" ]] || false
    [[ "$output" =~ "Step: expect columns - FAIL" ]] || false
    [[ "$output" =~ "Ran query: select * from dolt_commits;" ]] || false
    [[ "$output" =~ "Assertion failed: expected column count less than 5, got 5" ]] || false
}

@test "ci: ci run fails on bad query" {
    cat > workflow.yaml <<EOF
name: workflow
on:
  push: {}
jobs:
  - name: "bad saved queries"
    steps:
      - name: should fail, bad table name
        saved_query_name: invalid table
EOF
    dolt ci init
    dolt ci import ./workflow.yaml
    dolt sql -q "create table invalid (i int);"
    dolt sql --save "invalid table" -q "select * from invalid;"
    dolt sql -q "drop table invalid;"
    run dolt ci run "workflow"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Running workflow: workflow" ]] || false
    [[ "$output" =~ "Step: should fail, bad table name - FAIL" ]] || false
    [[ "$output" =~ "Ran query: select * from invalid" ]] || false
    [[ "$output" =~ "Query error" ]] || false
    [[ "$output" =~ "table not found: invalid" ]] || false
}

@test "ci: ci run fails on invalid workflow name" {
    dolt ci init
    run dolt ci run "invalid"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "workflow not found" ]] || false
    run dolt ci run
     [ "$status" -eq 1 ]
     [[ "$output" =~ "must specify workflow name" ]] || false
}

@test "ci: ci run fails on invalid query name" {
    cat > workflow.yaml <<EOF
name: workflow
on:
  push: {}
jobs:
  - name: "bad saved queries"
    steps:
      - name: should fail, bad query name
        saved_query_name: invalid query
EOF
    dolt ci init
    dolt ci import workflow.yaml
    run dolt ci run "workflow"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Running workflow: workflow" ]] || false
    [[ "$output" =~ "Step: should fail, bad query name - FAIL" ]] || false
    [[ "$output" =~ "Could not find saved query: invalid query" ]] || false
}
