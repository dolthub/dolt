#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    dolt sql -q "create table table_one (pk int PRIMARY KEY)"
    dolt commit -Am "create table table_one"
    dolt sql -q "create table table_two (pk int PRIMARY KEY)"
    dolt commit -Am "create table table_two"
}

teardown() {
    teardown_common
}

@test "ls: ls works" {
    run dolt ls
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
    [[ "$output" =~ "Tables in working set:" ]] || false
    [[ "$output" =~ "table_one" ]] || false
    [[ "$output" =~ "table_two" ]] || false
}

@test "ls: ls includes unstaged table" {
    dolt sql -q "create table table_three (pk int PRIMARY KEY)"
    run dolt ls
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 4 ]
    [[ "$output" =~ "Tables in working set:" ]] || false
    [[ "$output" =~ "table_one" ]] || false
    [[ "$output" =~ "table_two" ]] || false
    [[ "$output" =~ "table_three" ]] || false
}

@test "ls: ls includes staged table" {
    dolt sql -q "create table table_three (pk int PRIMARY KEY)"
    dolt add .
    run dolt ls
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 4 ]
    [[ "$output" =~ "Tables in working set:" ]] || false
    [[ "$output" =~ "table_one" ]] || false
    [[ "$output" =~ "table_two" ]] || false
    [[ "$output" =~ "table_three" ]] || false
}

@test "ls: --verbose shows row count" {
    dolt sql -q "insert into table_one values (1), (2), (3)"

    run dolt ls --verbose
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Tables in working set:" ]] || false
    [[ "$output" =~ "table_one" ]] || false
    [[ "$output" =~ "3 rows" ]] || false
    [[ "$output" =~ "table_two" ]] || false
    [[ "$output" =~ "0 rows" ]] || false
}

@test "ls: --system shows system tables" {
    run dolt ls --system
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 23 ]
    [[ "$output" =~ "System tables:" ]] || false
    [[ "$output" =~ "dolt_status" ]] || false
    [[ "$output" =~ "dolt_commits" ]] || false
    [[ "$output" =~ "dolt_commit_ancestors" ]] || false
    [[ "$output" =~ "dolt_constraint_violations" ]] || false
    [[ "$output" =~ "dolt_log" ]] || false
    [[ "$output" =~ "dolt_conflicts" ]] || false
    [[ "$output" =~ "dolt_remotes" ]] || false
    [[ "$output" =~ "dolt_branches" ]] || false
    [[ "$output" =~ "dolt_remote_branches" ]] || false
    [[ "$output" =~ "dolt_help" ]] || false
    [[ "$output" =~ "dolt_constraint_violations_table_one" ]] || false
    [[ "$output" =~ "dolt_history_table_one" ]] || false
    [[ "$output" =~ "dolt_conflicts_table_one" ]] || false
    [[ "$output" =~ "dolt_diff_table_one" ]] || false
    [[ "$output" =~ "dolt_commit_diff_table_one" ]] || false
    [[ "$output" =~ "dolt_constraint_violations_table_two" ]] || false
    [[ "$output" =~ "dolt_history_table_two" ]] || false
    [[ "$output" =~ "dolt_conflicts_table_two" ]] || false
    [[ "$output" =~ "dolt_diff_table_two" ]] || false
    [[ "$output" =~ "dolt_commit_diff_table_two" ]] || false
    [[ "$output" =~ "dolt_workspace_table_one" ]] || false
    [[ "$output" =~ "dolt_workspace_table_two" ]] || false
}

@test "ls: --all shows tables in working set and system tables" {
    run dolt ls --all
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Tables in working set:" ]] || false
    [[ "$output" =~ "table_one" ]] || false
    [[ "$output" =~ "table_two" ]] || false
    [[ "$output" =~ "System tables:" ]] || false
    [[ "$output" =~ "dolt_status" ]] || false
    [[ "$output" =~ "dolt_commits" ]] || false
    [[ "$output" =~ "dolt_commit_ancestors" ]] || false
    [[ "$output" =~ "dolt_constraint_violations" ]] || false
    [[ "$output" =~ "dolt_log" ]] || false
    [[ "$output" =~ "dolt_conflicts" ]] || false
    [[ "$output" =~ "dolt_remotes" ]] || false
    [[ "$output" =~ "dolt_branches" ]] || false
    [[ "$output" =~ "dolt_remote_branches" ]] || false
    [[ "$output" =~ "dolt_constraint_violations_table_one" ]] || false
    [[ "$output" =~ "dolt_history_table_one" ]] || false
    [[ "$output" =~ "dolt_conflicts_table_one" ]] || false
    [[ "$output" =~ "dolt_diff_table_one" ]] || false
    [[ "$output" =~ "dolt_commit_diff_table_one" ]] || false
    [[ "$output" =~ "dolt_constraint_violations_table_two" ]] || false
    [[ "$output" =~ "dolt_history_table_two" ]] || false
    [[ "$output" =~ "dolt_conflicts_table_two" ]] || false
    [[ "$output" =~ "dolt_diff_table_two" ]] || false
    [[ "$output" =~ "dolt_commit_diff_table_two" ]] || false
}

@test "ls: --all and --verbose shows row count for tables in working set" {
    dolt sql -q "insert into table_one values (1), (2), (3)"

    run dolt ls --all --verbose
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Tables in working set:" ]] || false
    [[ "$output" =~ "table_one" ]] || false
    [[ "$output" =~ "3 rows" ]] || false
    [[ "$output" =~ "table_two" ]] || false
    [[ "$output" =~ "0 rows" ]] || false
    [[ "$output" =~ "System tables:" ]] || false
    [[ "$output" =~ "dolt_status" ]] || false
    [[ "$output" =~ "dolt_commits" ]] || false
    [[ "$output" =~ "dolt_commit_ancestors" ]] || false
    [[ "$output" =~ "dolt_constraint_violations" ]] || false
    [[ "$output" =~ "dolt_log" ]] || false
    [[ "$output" =~ "dolt_conflicts" ]] || false
    [[ "$output" =~ "dolt_remotes" ]] || false
    [[ "$output" =~ "dolt_branches" ]] || false
    [[ "$output" =~ "dolt_remote_branches" ]] || false
    [[ "$output" =~ "dolt_constraint_violations_table_one" ]] || false
    [[ "$output" =~ "dolt_history_table_one" ]] || false
    [[ "$output" =~ "dolt_conflicts_table_one" ]] || false
    [[ "$output" =~ "dolt_diff_table_one" ]] || false
    [[ "$output" =~ "dolt_commit_diff_table_one" ]] || false
    [[ "$output" =~ "dolt_constraint_violations_table_two" ]] || false
    [[ "$output" =~ "dolt_history_table_two" ]] || false
    [[ "$output" =~ "dolt_conflicts_table_two" ]] || false
    [[ "$output" =~ "dolt_diff_table_two" ]] || false
    [[ "$output" =~ "dolt_commit_diff_table_two" ]] || false
}

@test "ls: --system and --all are mutually exclusive" {
    run dolt ls --system --all
    [ "$status" -eq 1 ]
    [[ "$output" =~ "--system and --all are mutually exclusive" ]] || false
}

@test "ls: ls with head" {
    run dolt ls HEAD
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Tables in " ]] || false    # Tables in [hash]:
    [[ "$output" =~ "table_one" ]] || false
    [[ "$output" =~ "table_two" ]] || false

    run dolt ls HEAD~1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Tables in " ]] || false    # Tables in [hash]:
    [[ "$output" =~ "table_one" ]] || false
    ! [[ "$output" =~ "table_two" ]] || false
}

@test "ls: ls with branch" {
    dolt checkout -b branch1
    dolt sql -q "create table table_three (pk int primary key)"
    dolt commit -Am "create table table_three"
    dolt checkout main

    run dolt ls branch1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Tables in " ]] || false    # Tables in [hash]:
    [[ "$output" =~ "table_one" ]] || false
    [[ "$output" =~ "table_two" ]] || false
    [[ "$output" =~ "table_three" ]] || false
}

@test "ls: no tables in working set" {
    dolt sql -q "drop table table_one"
    dolt sql -q "drop table table_two"

    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "No tables in working set" ]] || false
}

@test "ls: too many arguments" {
    run dolt ls HEAD HEAD~1
    [ "$status" -eq 1 ]
    [[ "$output" =~ "error: ls has too many positional arguments. Expected at most 1, found 2: HEAD, HEAD~1" ]] || false
}
