#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

# Asserts that when DOLT_DISABLE_REFLOG is set, dolt reflog returns nothing with no error.
@test "reflog: disabled with DOLT_DISABLE_REFLOG" {
    export DOLT_DISABLE_REFLOG=true
    dolt sql -q "create table t (i int primary key, j int);"
    dolt sql -q "insert into t values (1, 1), (2, 2), (3, 3)";
    dolt commit -Am "initial commit"
    dolt commit --allow-empty -m "test commit 1"

    run dolt reflog
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 0 ]
}

# Asserts that when DOLT_REFLOG_RECORD_LIMIT has been set, the reflog only contains the
# most recent entries and is limited by the env var's value.
@test "reflog: set DOLT_REFLOG_RECORD_LIMIT" {
    export DOLT_REFLOG_RECORD_LIMIT=2
    dolt sql -q "create table t (i int primary key, j int);"
    dolt sql -q "insert into t values (1, 1), (2, 2), (3, 3)";
    dolt commit -Am "initial commit"
    dolt commit --allow-empty -m "test commit 1"
    dolt commit --allow-empty -m "test commit 2"

    # Only the most recent two ref changes should appear in the log
    run dolt reflog
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test commit 1" ]] || false
    [[ "$output" =~ "test commit 2" ]] || false
    [[ ! "$output" =~ "initial commit" ]] || false
    [[ ! "$output" =~ "Initialize data repository" ]] || false
}

@test "reflog: simple reflog" {
    dolt sql -q "create table t (i int primary key, j int);"
    dolt sql -q "insert into t values (1, 1), (2, 2), (3, 3)";
    dolt commit -Am "initial commit"

    run dolt reflog
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    out=$(echo "$output" | sed -E 's/\x1b\[[0-9;]*m//g') # remove special characters for color
    [[ "$out" =~ "(HEAD -> main) HEAD@{0}: initial commit" ]] || false
    [[ "$out" =~ "HEAD@{1}: Initialize data repository" ]] || false
}

@test "reflog: reflog with ref given" {
    dolt sql <<SQL
create table t1(pk int primary key);
call dolt_commit('-Am', 'creating table t1');

insert into t1 values(1);
call dolt_commit('-Am', 'inserting row 1');
call dolt_tag('tag1');

call dolt_checkout('-b', 'branch1');
insert into t1 values(2);
call dolt_commit('-Am', 'inserting row 2');

insert into t1 values(3);
call dolt_commit('-Am', 'inserting row 3');
call dolt_tag('-d', 'tag1');
call dolt_tag('tag1');
SQL

    run dolt reflog refs/heads/main
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
    out=$(echo "$output" | sed -E 's/\x1b\[[0-9;]*m//g') # remove special characters for color
    [[ "$out" =~ "(HEAD -> main) HEAD@{0}: inserting row 1" ]] || false
    [[ "$out" =~ "HEAD@{1}: creating table t1" ]] || false
    [[ "$out" =~ "HEAD@{2}: Initialize data repository" ]] || false

    # ref is case-insensitive
    run dolt reflog rEFs/heAdS/MAIN
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
    out=$(echo "$output" | sed -E 's/\x1b\[[0-9;]*m//g') # remove special characters for color
    [[ "$out" =~ "(HEAD -> main) HEAD@{0}: inserting row 1" ]] || false
    [[ "$out" =~ "HEAD@{1}: creating table t1" ]] || false
    [[ "$out" =~ "HEAD@{2}: Initialize data repository" ]] || false

    run dolt reflog main
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
    out=$(echo "$output" | sed -E 's/\x1b\[[0-9;]*m//g') # remove special characters for color
    [[ "$out" =~ "(HEAD -> main) HEAD@{0}: inserting row 1" ]] || false
    [[ "$out" =~ "HEAD@{1}: creating table t1" ]] || false
    [[ "$out" =~ "HEAD@{2}: Initialize data repository" ]] || false

    # ref is case-insensitive
    run dolt reflog MaIn
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
    out=$(echo "$output" | sed -E 's/\x1b\[[0-9;]*m//g') # remove special characters for color
    [[ "$out" =~ "(HEAD -> main) HEAD@{0}: inserting row 1" ]] || false
    [[ "$out" =~ "HEAD@{1}: creating table t1" ]] || false
    [[ "$out" =~ "HEAD@{2}: Initialize data repository" ]] || false

    run dolt reflog refs/heads/branch1
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
    out=$(echo "$output" | sed -E 's/\x1b\[[0-9;]*m//g') # remove special characters for color
    [[ "$out" =~ "(branch1) HEAD@{0}: inserting row 3" ]] || false
    [[ "$out" =~ "HEAD@{1}: inserting row 2" ]] || false
    [[ "$out" =~ "HEAD@{2}: inserting row 1" ]] || false

    run dolt reflog branch1
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
    out=$(echo "$output" | sed -E 's/\x1b\[[0-9;]*m//g') # remove special characters for color
    [[ "$out" =~ "(branch1) HEAD@{0}: inserting row 3" ]] || false
    [[ "$out" =~ "HEAD@{1}: inserting row 2" ]] || false
    [[ "$out" =~ "HEAD@{2}: inserting row 1" ]] || false

    run dolt reflog refs/tags/tag1
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    out=$(echo "$output" | sed -E 's/\x1b\[[0-9;]*m//g') # remove special characters for color
    [[ "$out" =~ "(tag: tag1) HEAD@{0}: inserting row 3" ]] || false
    [[ "$out" =~ "HEAD@{1}: inserting row 1" ]] || false

    # ref is case-insensitive
    run dolt reflog Refs/tAGs/TaG1
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    out=$(echo "$output" | sed -E 's/\x1b\[[0-9;]*m//g') # remove special characters for color
    [[ "$out" =~ "(tag: tag1) HEAD@{0}: inserting row 3" ]] || false
    [[ "$out" =~ "HEAD@{1}: inserting row 1" ]] || false

    run dolt reflog tag1
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    out=$(echo "$output" | sed -E 's/\x1b\[[0-9;]*m//g') # remove special characters for color
    [[ "$out" =~ "(tag: tag1) HEAD@{0}: inserting row 3" ]] || false
    [[ "$out" =~ "HEAD@{1}: inserting row 1" ]] || false

    # ref is case-insensitive
    run dolt reflog TAg1
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    out=$(echo "$output" | sed -E 's/\x1b\[[0-9;]*m//g') # remove special characters for color
    [[ "$out" =~ "(tag: tag1) HEAD@{0}: inserting row 3" ]] || false
    [[ "$out" =~ "HEAD@{1}: inserting row 1" ]] || false

    dolt branch -D branch1

    run dolt reflog branch1
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
    out=$(echo "$output" | sed -E 's/\x1b\[[0-9;]*m//g') # remove special characters for color
    [[ "$out" =~ "(branch1) HEAD@{0}: inserting row 3" ]] || false
    [[ "$out" =~ "HEAD@{1}: inserting row 2" ]] || false
    [[ "$out" =~ "HEAD@{2}: inserting row 1" ]] || false

    dolt tag -d tag1
    run dolt reflog tag1
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    out=$(echo "$output" | sed -E 's/\x1b\[[0-9;]*m//g') # remove special characters for color
    [[ "$out" =~ "(tag: tag1) HEAD@{0}: inserting row 3" ]] || false
    [[ "$out" =~ "HEAD@{1}: inserting row 1" ]] || false
}

@test "reflog: garbage collection with no newgen data" {
    run dolt reflog
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]
    out=$(echo "$output" | sed -E 's/\x1b\[[0-9;]*m//g') # remove special characters for color
    [[ "$out" =~ "(HEAD -> main) HEAD@{0}: Initialize data repository" ]] || false

    dolt gc

    run dolt reflog
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 0 ]
}

@test "reflog: garbage collection with newgen data" {
    dolt sql <<SQL
create table t1(pk int primary key);
call dolt_commit('-Am', 'creating table t1');
insert into t1 values(1);
call dolt_commit('-Am', 'inserting row 1');
call dolt_tag('tag1');
insert into t1 values(2);
call dolt_commit('-Am', 'inserting row 2');
SQL

    run dolt reflog main
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 4 ]
    out=$(echo "$output" | sed -E 's/\x1b\[[0-9;]*m//g') # remove special characters for color
    [[ "$out" =~ "(HEAD -> main) HEAD@{0}: inserting row 2" ]] || false
    [[ "$out" =~ "HEAD@{1}: inserting row 1" ]] || false
    [[ "$out" =~ "HEAD@{2}: creating table t1" ]] || false
    [[ "$out" =~ "HEAD@{3}: Initialize data repository" ]] || false

    dolt gc

    run dolt reflog main
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 0 ]
}

@test "reflog: too many arguments given" {
    run dolt reflog foo bar
    [ "$status" -eq 1 ]
    [[ "$output" =~ "error: reflog has too many positional arguments" ]]
}

@test "reflog: unknown ref returns nothing" {
    dolt sql -q "create table t (i int primary key, j int);"
    dolt sql -q "insert into t values (1, 1), (2, 2), (3, 3)";
    dolt commit -Am "initial commit"

    run dolt reflog foo
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 0 ]
}
