#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    dolt sql <<SQL
CREATE TABLE test (
  pk1 BIGINT NOT NULL COMMENT 'tag:0',
  pk2 BIGINT NOT NULL COMMENT 'tag:1',
  c1 BIGINT COMMENT 'tag:2',
  c2 BIGINT COMMENT 'tag:3',
  c3 BIGINT COMMENT 'tag:4',
  c4 BIGINT COMMENT 'tag:5',
  c5 BIGINT COMMENT 'tag:6',
  PRIMARY KEY (pk1,pk2)
);
SQL
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "2pk5cols-ints: empty table" {
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "test" ]] || false
    run dolt sql -q "select * from test"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 0 ]

    run dolt diff
    [ "$status" -eq 0 ]
    [ "${lines[0]}" = "diff --dolt a/test b/test" ]
    [ "${lines[1]}" = "added table" ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Untracked tables" ]] || false
    [[ "$output" =~ "new table:" ]] || false
}

@test "2pk5cols-ints: add a row to a two primary table using dolt table put-row" {
    dolt add test
    dolt commit -m "added test table"
    run dolt sql -q "insert into test values (0, 0, 1, 2, 3, 4, 5)"
    [ "$status" -eq 0 ]
    run dolt diff
    [ "$status" -eq 0 ]
    [[ "$output" =~ \+[[:space:]]+\|[[:space:]]+0[[:space:]]+\|[[:space:]]+0 ]] || false
}

@test "2pk5cols-ints: add a row where one of the primary keys is different, not both" {
    dolt sql -q "insert into test values (0, 0, 1, 2, 3, 4, 5)"
    run dolt sql -q "insert into test values (0, 1, 1, 2, 3, 4, 10)"
    [ "$status" -eq 0 ]
    run dolt sql -q "select * from test"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]
    [[ "$output" =~ \|[[:space:]]+5 ]] || false
    [[ "$output" =~ \|[[:space:]]+10 ]] || false
}

@test "2pk5cols-ints: overwrite a row with two primary keys" {
    dolt sql -q "insert into test values (0, 0, 1, 2, 3, 4, 5)"
    run dolt sql -q "replace into test values (0, 0, 1, 2, 3, 4, 10)"
    [ "$status" -eq 0 ]
    run dolt sql -q "select * from test"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ ! "$output" =~ \|[[:space:]]+5 ]] || false
    [[ "$output" =~ \|[[:space:]]+10 ]] || false
}

@test "2pk5cols-ints: interact with a multiple primary key table with sql" {
    run dolt sql -q "insert into test (pk1,pk2,c1,c2,c3,c4,c5) values (0,0,6,6,6,6,6)"
    [ "$status" -eq 0 ]
    [[ "$output" = "" ]] || false
    run dolt sql -q "select * from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "c5" ]] || false
    [[ "$output" =~ "6" ]] || false
    run dolt sql -q "insert into test (pk1,pk2,c1,c2,c3,c4,c5) values (0,1,7,7,7,7,7),(1,0,8,8,8,8,8)"
    [ "$status" -eq 0 ]
    [[ "$output" = "" ]] || false
    run dolt sql -q "select * from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "c5" ]] || false
    [[ "$output" =~ "7" ]] || false
    [[ "$output" =~ "8" ]] || false
    run dolt sql -q "select * from test where pk1=1"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "c5" ]] || false
    [[ "$output" =~ "8" ]] || false
    [[ ! "$output" =~ "6" ]] || false
    run dolt sql -q "insert into test (pk1,pk2,c1,c2,c3,c4,c5) values (0,1,7,7,7,7,7)"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "duplicate primary key" ]] || false
    run dolt sql -q "insert into test (pk1,c1,c2,c3,c4,c5) values (0,6,6,6,6,6)"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Field 'pk2' doesn't have a default value" ]] || false
    run dolt sql -q "insert into test (c1,c2,c3,c4,c5) values (6,6,6,6,6)"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Field 'pk1' doesn't have a default value" ]] || false
}
