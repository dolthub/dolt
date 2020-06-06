#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common

    dolt sql <<SQL
CREATE TABLE test (
    pk int,
    c1 int,
    c2 varchar(20),
    primary key (pk),
);
SQL
}

teardown() {
    teardown_common
}

@test "dolt query_diff" {
    dolt sql -q 'insert into test values (0,0,"0")'
    dolt sql -q 'insert into test values (1,1,"1")'
    dolt add .
    dolt commit -m rows
    dolt sql -q 'delete from test where pk=1'
    dolt sql -q 'insert into test values (2,2,"2")'
    run dolt query_diff 'select * from test order by pk'
    [ "$status" = 0 ]
    [[ "$output" =~ "1" ]] || false
    [[ "$output" =~ "2" ]] || false
    [[ ! "$output" =~ "0" ]] || false
}
