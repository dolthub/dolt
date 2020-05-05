#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common

    dolt sql <<SQL
CREATE TABLE test1 (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL

    dolt sql <<SQL
CREATE TABLE test2 (
  \`pk\` BIGINT NOT NULL COMMENT 'tag:6',
  \`int\` INT COMMENT 'tag:7',
  \`string\` TEXT COMMENT 'tag:8',
  \`boolean\` BOOLEAN COMMENT 'tag:9',
  \`float\` DOUBLE COMMENT 'tag:10',
  \`uint\` BIGINT UNSIGNED COMMENT 'tag:11',
  PRIMARY KEY (pk)
);
SQL
}

teardown() {
    teardown_common
}

@test "dolt schema export, new specification" {
    dolt schema export test1 export.schema
    run dolt schema export test1 export.schema
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    [ -f export.schema ]
    cat export.schema
    run cat export.schema
    [ "$status" -eq 0 ]
    [[ "$output" =~ "CREATE" ]] || false
    [[ "$output" =~ "test1" ]] || false
    [[ ! "$output" =~ "working" ]] || false
}

@test "dolt schema export, no file" {
    run dolt schema export test1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test1" ]] || false 
}

@test "dolt schema export --all" {
    run dolt schema export --all export.schema
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    [ -f export.schema ]
    dolt sql -q "show tables" --save "test query"
    run dolt schema export --all
    [ "$status" -eq 0 ] 
    [[ "$output" =~ "CREATE" ]] || false
    [[ "$output" =~ "test1" ]] || false
    [[ ! "$output" =~ "working" ]] || false
    [[ ! "$output" =~ "dolt_" ]] || false
}
