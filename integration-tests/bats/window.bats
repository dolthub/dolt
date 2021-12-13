#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common

    dolt sql <<SQL
CREATE TABLE a (
  x BIGINT NOT NULL COMMENT 'tag:0',
  y varchar(1) COMMENT 'tag:1',
  PRIMARY KEY (x)
);
SQL
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "window: clean working set" {
    dolt add .
    dolt commit -m table
    dolt sql -q "insert into a values (1, 'a'), (0, 'b'), (2, 'c')"
    dolt add .
    dolt commit -m row
    run dolt sql -q "select to_x, first_value(to_y) over (partition by to_x), row_number() over (partition by diff_type) from dolt_diff_a" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "0,b,1" ]] || false
    [[ "${lines[2]}" =~ "1,a,2" ]] || false
    [[ "${lines[3]}" =~ "2,c,3" ]] || false
}
