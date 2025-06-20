#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "import-tables: error if no operation is provided" {
    run dolt table import t test.csv

    [ "$status" -eq 1 ]
    [[ "$output" =~ "Must specify exactly one of -c, -u, -a, or -r." ]] || false
}

@test "import-tables: error if multiple operations are provided" {
    run dolt table import -c -u -r t test.csv
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Must specify exactly one of -c, -u, -a, or -r." ]] || false
}

@test "import-tables: import tables where field names need to be escaped" {
    dolt table import -c t `batshelper escaped-characters.csv`

    run dolt sql -q "show create table t;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ '`--Key1` int' ]] || false
    [[ "$output" =~ '`"Key2` int' ]] || false
    [[ "$output" =~ '`'"'"'Key3` int' ]] || false
    [[ "$output" =~ '```Key4` int' ]] || false
    [[ "$output" =~ '`/Key5` int' ]] || false

    run dolt sql -q "select * from t;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ '| --Key1 | "Key2 | '"'"'Key3 | `Key4 | /Key5 |' ]] || false
    [[ "$output" =~ '| 0      | 1     | 2     | 3     | 4     |' ]] || false
}

@test "import-tables: import tables where primary key names need to be escaped" {
    dolt table import -c -pk "--Key1" t `batshelper escaped-characters.csv`
    dolt table import -r -pk '"Key2' t `batshelper escaped-characters.csv`
    dolt table import -r -pk "'Key3" t `batshelper escaped-characters.csv`
    dolt table import -r -pk '`Key4' t `batshelper escaped-characters.csv`
    dolt table import -r -pk "/Key5" t `batshelper escaped-characters.csv`
}

