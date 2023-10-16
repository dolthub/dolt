#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    dolt sql -q "create table t1 (pk int PRIMARY KEY)"
    dolt commit -Am "create table t1"
    dolt sql -q "create table t2 (pk int PRIMARY KEY)"
    dolt commit -Am "create table t2"
}

teardown() {
    teardown_common
}

@test "ls: ls works" {
    run dolt ls
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
    [[ "$output" =~ "Tables in working set:" ]] || false
    [[ "$output" =~ "t1" ]] || false
    [[ "$output" =~ "t2" ]] || false
}

@test "ls: ls includes unstaged table" {
    dolt sql -q "create table t3 (pk int PRIMARY KEY)"
    run dolt ls
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 4 ]
    [[ "$output" =~ "Tables in working set:" ]] || false
    [[ "$output" =~ "t1" ]] || false
    [[ "$output" =~ "t2" ]] || false
    [[ "$output" =~ "t3" ]] || false
}

@test "ls: ls includes staged table" {
    dolt sql -q "create table t3 (pk int PRIMARY KEY)"
    dolt add .
    run dolt ls
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 4 ]
    [[ "$output" =~ "Tables in working set:" ]] || false
    [[ "$output" =~ "t1" ]] || false
    [[ "$output" =~ "t2" ]] || false
    [[ "$output" =~ "t3" ]] || false
}

@test "ls: --verbose shows row count" {
    dolt sql -q "insert into t1 values (1), (2), (3)"

    run dolt ls --verbose
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Tables in working set:" ]] || false
    [[ "$output" =~ "t1" ]] || false
    [[ "$output" =~ "3 rows" ]] || false
    [[ "$output" =~ "t2" ]] || false
    [[ "$output" =~ "0 rows" ]] || false
}

@test "ls: --system shows system tables" {
    run dolt ls --system
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 20 ]
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
    [[ "$output" =~ "dolt_constraint_violations_t1" ]] || false
    [[ "$output" =~ "dolt_history_t1" ]] || false
    [[ "$output" =~ "dolt_conflicts_t1" ]] || false
    [[ "$output" =~ "dolt_diff_t1" ]] || false
    [[ "$output" =~ "dolt_commit_diff_t1" ]] || false
    [[ "$output" =~ "dolt_constraint_violations_t2" ]] || false
    [[ "$output" =~ "dolt_history_t2" ]] || false
    [[ "$output" =~ "dolt_conflicts_t2" ]] || false
    [[ "$output" =~ "dolt_diff_t2" ]] || false
    [[ "$output" =~ "dolt_commit_diff_t2" ]] || false
}

@test "ls: --verbose doesn't show row count for system tables" {
    run dolt ls --system --verbose
    [ "$status" -eq 0 ]
    ! [[ "$output" =~ "rows" ]] || false
}

@test "ls: --all shows tables in working set and system tables" {
    run dolt ls --all
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Tables in working set:" ]] || false
    [[ "$output" =~ "t1" ]] || false
    [[ "$output" =~ "t2" ]] || false
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
    [[ "$output" =~ "dolt_constraint_violations_t1" ]] || false
    [[ "$output" =~ "dolt_history_t1" ]] || false
    [[ "$output" =~ "dolt_conflicts_t1" ]] || false
    [[ "$output" =~ "dolt_diff_t1" ]] || false
    [[ "$output" =~ "dolt_commit_diff_t1" ]] || false
    [[ "$output" =~ "dolt_constraint_violations_t2" ]] || false
    [[ "$output" =~ "dolt_history_t2" ]] || false
    [[ "$output" =~ "dolt_conflicts_t2" ]] || false
    [[ "$output" =~ "dolt_diff_t2" ]] || false
    [[ "$output" =~ "dolt_commit_diff_t2" ]] || false
}

@test "ls: --all and --verbose shows row count for tables in working set" {
    dolt sql -q "insert into t1 values (1), (2), (3)"

    run dolt ls --all --verbose
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Tables in working set:" ]] || false
    [[ "$output" =~ "t1" ]] || false
    [[ "$output" =~ "3 rows" ]] || false
    [[ "$output" =~ "t2" ]] || false
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
    [[ "$output" =~ "dolt_constraint_violations_t1" ]] || false
    [[ "$output" =~ "dolt_history_t1" ]] || false
    [[ "$output" =~ "dolt_conflicts_t1" ]] || false
    [[ "$output" =~ "dolt_diff_t1" ]] || false
    [[ "$output" =~ "dolt_commit_diff_t1" ]] || false
    [[ "$output" =~ "dolt_constraint_violations_t2" ]] || false
    [[ "$output" =~ "dolt_history_t2" ]] || false
    [[ "$output" =~ "dolt_conflicts_t2" ]] || false
    [[ "$output" =~ "dolt_diff_t2" ]] || false
    [[ "$output" =~ "dolt_commit_diff_t2" ]] || false
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
    [[ "$output" =~ "t1" ]] || false
    [[ "$output" =~ "t2" ]] || false

    run dolt ls HEAD~1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Tables in " ]] || false    # Tables in [hash]:
    [[ "$output" =~ "t1" ]] || false
    ! [[ "$output" =~ "t2" ]] || false
}

@test "ls: ls with branch" {
    dolt checkout -b branch1
    dolt sql -q "create table t3 (pk int primary key)"
    dolt commit -Am "create table t3"
    dolt checkout main

    run dolt ls branch1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Tables in " ]] || false    # Tables in [hash]:
    [[ "$output" =~ "t1" ]] || false
    [[ "$output" =~ "t2" ]] || false
    [[ "$output" =~ "t3" ]] || false
}

@test "ls: too many arguments" {
    run dolt ls HEAD HEAD~1
    [ "$status" -eq 1 ]
    [[ "$output" =~ "error: ls has too many positional arguments. Expected at most 1, found 2: HEAD, HEAD~1" ]] || false
}
