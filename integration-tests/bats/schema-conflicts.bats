#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}

@test "schema-conflicts: dolt_schema_conflicts smoke test" {
    run dolt sql -q "select * from dolt_schema_conflicts" -r csv
    [[ "$output" =~ "table,our_schema,their_schema,description" ]]
}

setup_schema_conflict() {
    dolt sql -q "create table t (pk int primary key, c0 int);"
    dolt commit -Am "new table t"
    dolt branch other
    dolt sql -q "alter table t modify c0 varchar(20)"
    dolt commit -am "alter table t on branch main"
    dolt checkout other
    dolt sql -q "alter table t modify c0 datetime"
    dolt commit -am "alter table t on branch other"
    dolt checkout main
}

@test "schema-conflicts: sql merge, query schema conflicts" {
    setup_schema_conflict

    dolt sql -q "call dolt_merge('other')"

    run dolt sql -q "select * from dolt_schema_conflicts" -r vertical
    [ "$status" -eq 0 ]
    [[ "$output" =~ "table: t" ]]
    [[ "$output" =~ "our_schema: create table t (pk int primary key, c0 varchar(20))" ]]
    [[ "$output" =~ "their_schema: create table t (pk int primary key, c0 datetime)" ]]
    [[ "$output" =~ "description: " ]]
}

@test "schema-conflicts: cli merge, query schema conflicts" {
    setup_schema_conflict

    run dolt merge other
    [ "$status" -ne 0 ]

    run dolt sql -q "select * from dolt_schema_conflicts" -r vertical
    [ "$status" -eq 0 ]
    [[ "$output" =~ "table: t" ]]
    [[ "$output" =~ "our_schema: create table t (pk int primary key, c0 varchar(20))" ]]
    [[ "$output" =~ "their_schema: create table t (pk int primary key, c0 datetime)" ]]
    [[ "$output" =~ "description: " ]]
}

@test "schema-conflicts: resolve schema conflict with 'ours'" {
    setup_schema_conflict

    skip "todo"
    run dolt merge other
    [ "$status" -ne 0 ]

    dolt sql -q "call dolt_conflict_resolve('--ours', 't')"
    run dolt schema show t
    [ "$status" -eq 0 ]
    [[ "$output" =~ "varchar(20)" ]]
}

@test "schema-conflicts: resolve schema conflict with 'theirs'" {
    setup_schema_conflict

    skip "todo"
    run dolt merge other
    [ "$status" -ne 0 ]

    dolt sql -q "call dolt_conflict_resolve('--theirs', 't')"
    run dolt schema show t
    [ "$status" -eq 0 ]
    [[ "$output" =~ "datetime" ]]
}
