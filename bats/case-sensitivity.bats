#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common

    cat <<SQL > capital-letter-column-names-schema.sql
CREATE TABLE test (
    Aaa INT NOT NULL,
    Bbb VARCHAR(20),
    Ccc VARCHAR(20),
    PRIMARY KEY (Aaa)
);
SQL
    cat <<DELIM > capital-letter-column-names.csv
Aaa,Bbb,Ccc
1,aaa,AAA
2,bbb,BBB
DELIM

    dolt table import -c -s capital-letter-column-names-schema.sql test capital-letter-column-names.csv
}

teardown() {
    teardown_common
}

@test "capital letter col names. sql select with a where clause" {
    run dolt sql -q "select * from test where Aaa = 2"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "BBB" ]] || false
}

@test "capital letter col names. dolt schema show" {
    run dolt schema show
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Aaa" ]] || false
    [[ "$output" =~ "Bbb" ]] || false
    [[ "$output" =~ "Ccc" ]] || false
}

@test "capital letter col names. sql select" {
    run dolt sql -q "select Bbb from test where Aaa=2"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "bbb" ]] || false
    [[ "$output" =~ "Bbb" ]] || false
    [[ ! "$output" =~ "Aaa" ]] || false
    [[ ! "$output" =~ "aaa" ]] || false
}

@test "capital letter col names. dolt table export" {
    run dolt table export test export.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully exported data" ]] || false
    run cat export.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "bbb" ]] || false
    [[ "$output" =~ "Bbb" ]] || false
    [[ "$output" =~ "Aaa" ]] || false
    [[ "$output" =~ "aaa" ]] || false
}

@test "capital letter col names. dolt table copy" {
    run dolt table cp test test2
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt sql -q "select * from test2"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "bbb" ]] || false
    [[ "$output" =~ "Bbb" ]] || false
    [[ "$output" =~ "Aaa" ]] || false
    [[ "$output" =~ "aaa" ]] || false
}

@test "capital letter column names. select with an as" {
    run dolt sql -q "select Aaa as AAA from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "AAA" ]] || false
}

@test "select table case insensitive, table and column" {
    run dolt sql -r csv -q "select aaa as AAA from TEST order by 1 limit 1"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "AAA" ]] || false
    run dolt sql -r csv -q "select Test.aaa as AAA from TEST order by 1 limit 1"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "AAA" ]] || false
    [[ "$output" =~ "1" ]] || false
}

@test "select table alias case insensitive" {
    run dolt sql -r csv -q "select t.aaA as AaA from TEST T order by 1 limit 1"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "AaA" ]] || false
    [[ "$output" =~ "1" ]] || false
}

@test "select reserved word, preserve case" {
    run dolt sql -q "select 1 as DaTe"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "DaTe" ]] || false
}

@test "select column with different case" {
    run dolt sql -q "select AaA from Test order by 1 limit 1"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1" ]] || false
    skip "Column names come back as the table declares them, not as written in the query"
    [[ "$output" =~ "AaA" ]] || false
}
