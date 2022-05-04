#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}

@test "conflicts should show using the union-schema (new schema on right)" {
    dolt sql -q "CREATE TABLE t (a INT PRIMARY KEY, b INT);"
    dolt commit -am "base"

    dolt checkout -b right
    dolt sql <<SQL
ALTER TABLE t ADD c INT;
INSERT INTO t VALUES (1, 1, 1);
SQL
    dolt commit -am "right"

    dolt checkout main
    dolt sql -q "INSERT INTO t values (1, 1);"
    dolt commit -am "left"

    dolt merge right

    run dolt conflicts cat .
    [[ "$output" =~ "| a" ]]
    [[ "$output" =~ "| b" ]]
    [[ "$output" =~ "| c" ]]
}

@test "conflicts should show using the union-schema (new schema on left)" {
    dolt sql -q "CREATE TABLE t (a INT PRIMARY KEY, b INT);"
    dolt commit -am "base"

    dolt checkout -b right
    dolt sql -q "INSERT INTO t values (1, 1);"
    dolt commit -am "right"

    dolt checkout main
    dolt sql <<SQL
ALTER TABLE t ADD c INT;
INSERT INTO t VALUES (1, 1, 1);
SQL
    dolt commit -am "left"
    dolt merge right

    run dolt conflicts cat .
    [[ "$output" =~ "| a" ]]
    [[ "$output" =~ "| b" ]]
    [[ "$output" =~ "| c" ]]
}
