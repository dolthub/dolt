#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "sql-show: show table status on auto-increment table" {
    dolt sql -q "CREATE TABLE test(pk int NOT NULL AUTO_INCREMENT, c1 int, PRIMARY KEY (pk))"

    run dolt sql -q "show table status;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false

    dolt sql -q "INSERT INTO test (c1) VALUES (0)"
    run dolt sql -q "show table status;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false
}

@test "sql-show: show table status has null auto-increment column when table does not auto-increment" {
    dolt sql -q "CREATE TABLE test(pk int NOT NULL, c1 int, PRIMARY KEY (pk))"

    run dolt sql -q "show table status where \`Auto_increment\`=1;"
    [ "$status" -eq 0 ]
    ! [[ "$output" = "test" ]] || false

    dolt sql -q "INSERT INTO test VALUES (1, 0)"
    run dolt sql -q "show table status where \`Auto_increment\`=2;"
    [ "$status" -eq 0 ]
    ! [[ "$output" =~ "test" ]] || false
}


@test "sql-show: show table status has number of rows correct" {
    dolt sql -q "CREATE TABLE test(pk int NOT NULL AUTO_INCREMENT, c1 int, PRIMARY KEY (pk))"

    run dolt sql -q "show table status where Rows=0"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false

    dolt sql -q "INSERT INTO test (c1) VALUES (0)"
    run dolt sql -q "show table status where Rows=1;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false
}

@test "sql-show: show table status shows a data length > 0" {
    dolt sql -q "CREATE TABLE test(pk int NOT NULL AUTO_INCREMENT, c1 int, PRIMARY KEY (pk))"

    run dolt sql -q "show table status where \`Data_length\`=0"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false

    dolt sql -q "INSERT INTO test (c1) VALUES (0),(1)"
    run dolt sql -q "show table status where \`Data_length\`>0"
    [ "$status" -eq 0 ]

    # Looking for 32 bytes (2 cols * 2 * rows * 8 int bytes)
    [[ "$output" =~ "32" ]] || false
}