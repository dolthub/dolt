#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    skip_nbf_dolt_1

    dolt sql <<SQL
CREATE TABLE test1 (
  pk BIGINT NOT NULL,
  c1 BIGINT,
  c2 BIGINT,
  c3 BIGINT,
  c4 BIGINT,
  c5 BIGINT,
  PRIMARY KEY (pk)
);
SQL

    dolt sql <<SQL
CREATE TABLE test2 (
  \`pk\` BIGINT NOT NULL,
  \`int\` INT,
  \`string\` TEXT,
  \`boolean\` BOOLEAN,
  \`float\` DOUBLE,
  \`uint\` BIGINT UNSIGNED,
  PRIMARY KEY (pk)
);
SQL

    # save a query to ensure we have a system table to ignore
    dolt sql -q "show tables" --save "BATS query"
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "schema-export: export one table to file" {
    run dolt schema export test1 export.schema
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    [ -f export.schema ]
    run cat export.schema
    [ "$status" -eq 0 ]
    [[ "$output" =~ "CREATE TABLE \`test1\`" ]] || false
    [[ ! "$output" =~ "working" ]] || false
    [[ ! "$output" =~ "dolt_" ]] || false

    # ensure system tables are present
    run dolt sql -q 'select name from dolt_query_catalog'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "BATS query" ]] || false
}

@test "schema-export: export all tables to file" {
    run dolt schema export export.schema
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    [ -f export.schema ]
    run cat export.schema
    [ "$status" -eq 0 ]
    [[ "$output" =~ "CREATE TABLE \`test1\`" ]] || false
    [[ "$output" =~ "CREATE TABLE \`test2\`" ]] || false
    [[ ! "$output" =~ "working" ]] || false
    [[ ! "$output" =~ "dolt_" ]] || false
}

@test "schema-export: export one table to std out" {
    run dolt schema export test1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "CREATE TABLE \`test1\`" ]] || false
    [[ ! "$output" =~ "working" ]] || false
    [[ ! "$output" =~ "dolt_" ]] || false
}

@test "schema-export: export all tables to std out" {
    run dolt schema export
    [ "$status" -eq 0 ]
    [[ "$output" =~ "CREATE TABLE \`test1\`" ]] || false
    [[ "$output" =~ "CREATE TABLE \`test2\`" ]] || false
    [[ ! "$output" =~ "working" ]] || false
    [[ ! "$output" =~ "dolt_" ]] || false
}
