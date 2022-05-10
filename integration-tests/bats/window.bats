#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "window: no diff table window buffer sharing" {
    skip_nbf_dolt_1
    dolt sql -q "create table a (x int primary key, y varchar(1))"
    dolt add .
    dolt commit -m table
    dolt sql -q "insert into a values (1, 'a'), (0, 'b'), (2, 'c')"
    dolt add .
    dolt commit -m row
    run dolt sql -q "select to_x, first_value(to_y) over (partition by to_x), row_number() over (partition by diff_type order by to_x desc) from dolt_diff_a" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "0,b,3" ]] || false
    [[ "${lines[2]}" =~ "1,a,2" ]] || false
    [[ "${lines[3]}" =~ "2,c,1" ]] || false
}
