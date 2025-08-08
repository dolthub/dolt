#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "sql-ci: run with expected rows" {
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
    run dolt sql -q "select * from dolt_ci_run('workflow')" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "verify initial commits,verify initial commits,\"\",PASS" ]] || false
}

@test "sql-ci: ci run with expected columns" {
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
    run dolt sql -q "select * from dolt_ci_run('workflow')" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "verify dolt commit,verify dolt commit,\"\",PASS" ]] || false
}

@test "sql-ci: each assertion type can be used" {
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
    run dolt sql -q "select * from dolt_ci_run('workflow')" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "check comparisons,equals comp,\"\",PASS" ]] || false
    [[ "$output" =~ "check comparisons,not equals comp,\"\",PASS" ]] || false
    [[ "$output" =~ "check comparisons,greater than comp,\"\",PASS" ]] || false
    [[ "$output" =~ "check comparisons,greater or equal than comp,\"\",PASS" ]] || false
    [[ "$output" =~ "check comparisons,less than comp,\"\",PASS" ]] || false
    [[ "$output" =~ "check comparisons,less or equal than comp,\"\",PASS" ]] || false
}

@test "sql-ci: saved queries fail with ci run" {
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
    run dolt sql -q "select * from dolt_ci_run('workflow')" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "bad query assertions,expect rows,select * from dolt_commits;,FAIL,\"expected row count 2, got 3\"" ]] || false
    [[ "$output" =~ "bad query assertions,expect columns,select * from dolt_commits;,FAIL,\"expected column count less than 5, got 5\"" ]] || false
}

@test "sql-ci: ci run fails on bad query" {
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
    run dolt sql -q "select * from dolt_ci_run('workflow')" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "bad saved queries,\"should fail, bad table name\",select * from invalid;,FAIL,query error: table not found: invalid" ]] || false
}

@test "sql-ci: ci run fails on invalid workflow name" {
    dolt ci init
    run dolt sql -q "select * from dolt_ci_run('invalid')"
    [[ "$output" =~ "could not find workflow with name: invalid" ]] || false
}

@test "sql-ci: ci run fails on invalid query name" {
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
    run dolt sql -q "select * from dolt_ci_run('workflow')" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "bad saved queries,\"should fail, bad query name\",\"\",FAIL,saved query does not exist: invalid query" ]] || false
}
