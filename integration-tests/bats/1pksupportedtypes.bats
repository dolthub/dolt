#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    dolt sql <<SQL
CREATE TABLE test (
  \`pk\` BIGINT NOT NULL COMMENT 'tag:0',
  \`int\` BIGINT COMMENT 'tag:1',
  \`string\` LONGTEXT COMMENT 'tag:2',
  \`boolean\` BOOLEAN COMMENT 'tag:3',
  \`float\` DOUBLE COMMENT 'tag:4',
  \`uint\` BIGINT UNSIGNED COMMENT 'tag:5',
  \`uuid\` CHAR(36) CHARACTER SET ascii COLLATE ascii_bin COMMENT 'tag:6',
  PRIMARY KEY (pk)
);
SQL
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "1pksupportedtypes: dolt table put-row with all types then examine table" {
    run dolt sql -q "insert into test values (0, 1, 'foo', true, 1.11111111111111, 346, '123e4567-e89b-12d3-a456-426655440000')"
    [ "$status" -eq 0 ]
    run dolt sql -q "select * from test"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
}

@test "1pksupportedtypes: boolean 1,0,true,false inserts and examine table" {
    run dolt sql -q "insert into test values (0, 1, 'foo', 1, 1.11111111111111, 346, '123e4567-e89b-12d3-a456-426655440000')"
    [ "$status" -eq 0 ]
    run dolt sql -q "select * from test"
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "boolean" ]] || false
    [[ "${lines[3]}" =~ "1" ]] || false
    run dolt sql -q "replace into test values (0, 1, 'foo', 0, 1.11111111111111, 346, '123e4567-e89b-12d3-a456-426655440000')"
    [ "$status" -eq 0 ]
    run dolt sql -q "select * from test"
    [[ "${lines[3]}" =~ "0" ]] || false
    run dolt sql -q "replace into test values (0, 1, 'foo', true, 1.11111111111111, 346, '123e4567-e89b-12d3-a456-426655440000')"
    [ "$status" -eq 0 ]
    run dolt sql -q "select * from test"
    [[ "${lines[3]}" =~ "1" ]] || false
    run dolt sql -q "replace into test values (0, 1, 'foo', false, 1.11111111111111, 346, '123e4567-e89b-12d3-a456-426655440000')"
    [ "$status" -eq 0 ]
    run dolt sql -q "select * from test"
    [[ "${lines[3]}" =~ "0" ]] || false
}

@test "1pksupportedtypes: attempt to insert some schema violations" {
    run dolt sql -q "insert into test values (0, 1, 'foo', true, 1.11111111111111, -346, '123e4567-e89b-12d3-a456-426655440000')"
    [ "$status" -eq 1 ]
    run dolt sql -q "insert into test values (0, 1, 'foo', 'foo', 1.11111111111111, 346, 'not_a_uuid')"
    [ "$status" -eq 1 ]
}
