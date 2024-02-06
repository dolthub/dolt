#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    dolt sql <<SQL
create table t (i int, j int, k int);
insert into t (i, j, k) values (1, 2, 3), (4, 5, 6), (7, 8, 9);
SQL
    #TODO: change this to work for non-windows
    CURR_DIR=$(pwd)
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "sql-export: basic outfile" {
    run dolt sql -q "select * from t order by i, j, k into outfile '$CURR_DIR/outfile.out';"
    [ "$status" -eq 0 ]
    [ -f outfile.out ]

    run cat outfile.out
    [ "$status" -eq 0 ]
    [[ "${#lines[@]}" = "3" ]] || false
    [[ "${lines[0]}" =~ "1	2	3" ]] || false
    [[ "${lines[1]}" =~ "4	5	6" ]] || false
    [[ "${lines[2]}" =~ "7	8	9" ]] || false
}