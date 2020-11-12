#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common

    dolt sql <<SQL
CREATE TABLE test (
  pk int NOT NULL PRIMARY KEY AUTO_INCREMENT,
  c0 int
);
SQL
}

teardown() {
    teardown_common
}

@test "dolt filter-branch smoke-test" {
    dolt filter-branch
}