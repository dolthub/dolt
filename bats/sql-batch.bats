#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
}

teardown() {
    teardown_common
}

@test "dolt sql -b and -batch are a valid commands" {
    run dolt sql -b -q "insert into test values (0,0,0,0,0,0)"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Rows inserted: 1" ]] || false
    skip "Batch does not properly insert a newline after output"
    NEWLINE_REGEX = "Rows deleted: 0\n"
    [[ "$output" =~ $NEWLINE_REGEX  ]] || false
    dolt sql -batch -q "insert into test values (1,0,0,0,0,0)"
    run dolt sql -b -q "select * from test" 
    [ "$status" -eq 0 ]
    [[ "$output" =~ " 0 " ]] || false
    [[ "$output" =~ " 1 " ]] || false
    [[ "$output" =~ " Rows inserted " ]] || false
}

@test "Piped SQL files interpreted in batch mode" {
    run dolt sql <<SQL
insert into test values (0,0,0,0,0,0);
insert into test values (1,0,0,0,0,0);
SQL
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Rows inserted: 2" ]] || false
}

@test "Line number and bad query displayed on error in batch sql" {
    run dolt sql <<SQL
insert into test values (0,0,0,0,0,0);
insert into test values (1,0,0,0,0,0);
insert into test values poop;
SQL
    [ "$status" -ne 0 ]
    [[ "$output" =~ "Error processing batch" ]] || false
    skip "No line number and query on error"
    [[ "$output" =~ " 3 " ]] || false
    [[ "$output" =~ "insert into test values poop;" ]] || false
}
