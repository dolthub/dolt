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
    [[ "$output" =~ "table_name,base_schema,our_schema,their_schema,description" ]] || false
}

setup_schema_conflict() {
    dolt sql -q "create table t (pk int primary key, c0 int);"
    dolt commit -Am "new table t"
    dolt branch other
    dolt sql -q "alter table t modify c0 varchar(20)"
    dolt commit -am "alter table t on branch main"
    dolt checkout other
    dolt sql -q "alter table t modify c0 datetime(6)"
    dolt commit -am "alter table t on branch other"
    dolt checkout main
}

@test "schema-conflicts: sql merge, query schema conflicts" {
    setup_schema_conflict

    dolt sql -q "set @@dolt_force_transaction_commit=1; call dolt_merge('other')"

    run dolt sql -q "select our_schema from dolt_schema_conflicts" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "varchar(20)," ]] || false
    run dolt sql -q "select their_schema from dolt_schema_conflicts" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "datetime(6)," ]] || false
}

@test "schema-conflicts: cli merge, query schema conflicts" {
    setup_schema_conflict

    dolt merge other

    run dolt sql -q "select our_schema from dolt_schema_conflicts" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "varchar(20)," ]] || false
    run dolt sql -q "select their_schema from dolt_schema_conflicts" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "datetime(6)," ]] || false
}

@test "schema-conflicts: resolve schema conflict with 'ours' via SQL" {
    skip "auto conflict resolution for schema merges is blocked until https://github.com/dolthub/dolt/issues/6616 is fixed"

    setup_schema_conflict

    dolt merge other

    dolt sql -q "call dolt_conflicts_resolve('--ours', 't')"
    run dolt schema show t
    [ "$status" -eq 0 ]
    [[ "$output" =~ "varchar(20)" ]] || false
}

@test "schema-conflicts: resolve schema conflict with 'theirs' via SQL" {
    skip "auto conflict resolution for schema merges is blocked until https://github.com/dolthub/dolt/issues/6616 is fixed"

    setup_schema_conflict

    dolt merge other

    dolt sql -q "call dolt_conflicts_resolve('--theirs', 't')"
    run dolt schema show t
    [ "$status" -eq 0 ]
    [[ "$output" =~ "datetime" ]] || false
}

@test "schema-conflicts: resolve schema conflict with 'ours' via CLI" {
    skip "auto conflict resolution for schema merges is blocked until https://github.com/dolthub/dolt/issues/6616 is fixed"

    setup_schema_conflict

    dolt merge other

    dolt conflicts resolve --ours t
    run dolt schema show t
    [ "$status" -eq 0 ]
    [[ "$output" =~ "varchar(20)" ]] || false
}

@test "schema-conflicts: resolve schema conflict with 'theirs' via CLI" {
    skip "auto conflict resolution for schema merges is blocked until https://github.com/dolthub/dolt/issues/6616 is fixed"

    setup_schema_conflict

    dolt merge other

    dolt conflicts resolve --theirs t
    run dolt schema show t
    [ "$status" -eq 0 ]
    [[ "$output" =~ "datetime" ]] || false
}
