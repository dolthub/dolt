#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    skip_nbf_dolt_1

    dolt sql -q "create table t1 (a bigint primary key, b bigint)"
    dolt sql -q "insert into t1 values (0,0), (1,1)"
    dolt commit -am "Init"
    dolt sql -q "drop table t1"
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "replace: same table gives empty diff" {
    dolt sql -q "create table t1 (a bigint primary key, b bigint)"
    run dolt diff -r=sql main
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "DELETE FROM \`t1\` WHERE (\`a\`=0);" ]] || false
    [[ "$output" =~ "DELETE FROM \`t1\` WHERE (\`a\`=1);" ]] || false

    dolt sql -q "insert into t1 values (0,0), (1,1)"
    dolt add .
    run dolt diff -r=sql main
    [ "$status" -eq 0 ]
    [[ "$output" = "" ]] || false
}

@test "replace: different name generates new tags" {
    dolt sql -q "create table t2 (a bigint primary key, b bigint)"
    run dolt diff -r=sql main
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]
    [[ "$output" =~ "DROP TABLE \`t1\`" ]] || false
    [[ "$output" =~ "CREATE TABLE \`t2\`" ]] || false
}
