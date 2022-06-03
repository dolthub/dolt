#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    skip_nbf_dolt_1

    dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 varchar(10))"
    dolt add -A
    dolt commit -m "Created table"
    dolt checkout -b branch1
    dolt sql -q "INSERT INTO test VALUES (1, 'a')"
    dolt add -A
    dolt commit -m "Inserted 1"
    dolt sql -q "INSERT INTO test VALUES (2, 'b')"
    dolt add -A
    dolt commit -m "Inserted 2"
    dolt sql -q "INSERT INTO test VALUES (3, 'c')"
    dolt add -A
    dolt commit -m "Inserted 3"
    dolt checkout main
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "cherry-pick: branch name cherry picks the latest commit" {
    dolt cherry-pick branch1~1
    dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "1" ]
}