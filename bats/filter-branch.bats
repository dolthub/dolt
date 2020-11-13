#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common

    dolt sql <<SQL
CREATE TABLE test (
  pk int NOT NULL PRIMARY KEY,
  c0 int
);
INSERT INTO test VALUES
    (0,0),(1,1),(2,2);
SQL
}

teardown() {
    teardown_common
}

@test "dolt filter-branch smoke-test" {
    dolt add -A && dolt commit -m "added table test"

    dolt sql -q "INSERT INTO test VALUES (7,7),(8,8),(9,9);"
    dolt add -A && dolt commit -m "added more rows"

    dolt filter-branch "delete from test where pk > 1;"
    run dolt sql -q "select count(*) from test" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "2" ]] || false
}