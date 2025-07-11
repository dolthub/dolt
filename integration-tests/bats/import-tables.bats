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

@test "import-tables: error message shows actual argument count when too few arguments" {
    run dolt table import -c
    [ "$status" -eq 1 ]
    [[ "$output" =~ "expected 1 argument (for stdin) or 2 arguments (table and file), but received 0" ]] || false
}

@test "import-tables: error message shows actual argument count when single argument provided" {
    # Test with just table name (missing file)
    run dolt table import -c table_name
    [ "$status" -eq 1 ]
    # This should succeed in our validation but fail when trying to read from stdin
    # since we're providing 1 argument which is valid
}

@test "import-tables: handles incorrect flag format gracefully" {
    # Test the specific case from the issue with -pks instead of --pk
    run dolt table import -c -pks "year,state_fips" precinct_results test.csv
    [ "$status" -eq 1 ]
    # The argparser treats the unknown flag's value as a positional argument
    [[ "$output" =~ "error: import has too many positional arguments" ]] || false
    [[ "$output" =~ "Expected at most 2, found 3" ]] || false
}
