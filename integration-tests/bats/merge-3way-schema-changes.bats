#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}


@test "merge-3way-schema-changes: add a NOT NULL column with default value on a branch" {
    dolt sql -q "create table t (pk int primary key);"
    dolt commit -Am "ancestor"

    dolt checkout -b right
    dolt sql -q "insert into t values (1);"
    dolt sql -q "alter table t add column col1 int not null default 0;"
    dolt commit -am "right"

    dolt checkout main
    dolt sql -q "insert into t values (2);"
    dolt commit -am "left"

    dolt merge right

    run dolt sql -q "select * from t" -r csv
    log_status_eq 0
    [[ "$output" =~ "1,0" ]] || false
    [[ "$output" =~ "2,0" ]] || false
}