#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}

@test "table export sql datetime" {
    skip "dates should be quoted"
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT PRIMARY KEY,
  v DATETIME
);
SQL
    dolt sql -q "INSERT INTO test VALUES (1, '2020-04-08 11:11:11'), (2, '2020-04-08 12:12:12')"
    dolt table export test test.sql
    run cat test.sql
    [[ "$output" =~ 'INSERT INTO `test` (`pk`,`v`) VALUES (1,"2020-04-08 11:11:11");' ]] || false
    [[ "$output" =~ 'INSERT INTO `test` (`pk`,`v`) VALUES (2,"2020-04-08 12:12:12");' ]] || false
}
