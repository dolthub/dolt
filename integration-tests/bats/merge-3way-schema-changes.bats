#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    skip_nbf_not_dolt
}

teardown() {
    teardown_common
}


@test "merge-3way-schema-changes: blocked merge can be fixed by making the schema identical" {
    dolt sql -q "create table t (pk int primary key);"
    dolt commit -Am "ancestor"

    dolt checkout -b right
    dolt sql -q "insert into t values (1);"
    dolt sql -q "alter table t add column col1 int not null default 0;"
    dolt commit -am "right"

    dolt checkout main
    dolt sql -q "insert into t values (2);"
    dolt commit -am "left"

    run dolt merge right
    [ $status -ne 0 ]
    [[ $output =~ "table t can't be automatically merged." ]]

    run dolt diff main right --schema -r sql
    [ $status -eq 0 ]
    [[ $output =~ 'ALTER TABLE `t` ADD `col1` int NOT NULL DEFAULT 0;' ]]

    dolt sql -q 'ALTER TABLE `t` ADD `col1` int NOT NULL DEFAULT 0;'
    dolt commit -am "fix merge"
    dolt merge right

    run dolt sql -r csv -q "select * from t;"
    [[ $output =~ "pk,col1" ]]
    [[ $output =~ "1,0" ]]
    [[ $output =~ "2,0" ]]
}