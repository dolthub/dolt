#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "import-append-tables: disallow overwriting row during append" {
    dolt sql -q "CREATE TABLE t (pk int primary key, col1 int);"
    dolt table import -a t <<CSV
pk, col1
1, 1
CSV
    run dolt table import -a t <<CSV
pk, col1
1, 2
CSV

    [ "$status" -eq 1 ]
    [[ "$output" =~ "An error occurred while moving data" ]] || false
    [[ "$output" =~ "row [1,1] would be overwritten by [1,2]" ]] || false

    run dolt sql -q "select * from t"
    echo "$output"
    [ "$status" -eq 0 ]
    [[   "$output" =~ "| 1  | 1    |" ]] || false
    [[ ! "$output" =~ "| 1  | 2    |" ]] || false
    [ "${#lines[@]}" -eq 5 ]
}

@test "import-append-tables: disallow multiple keys with different values during append" {
    dolt sql -q "CREATE TABLE t (pk int primary key, col1 int);"
    run dolt table import -a t <<CSV
pk, col1
1, 1
1, 2
CSV

    echo "$output"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "An error occurred while moving data" ]] || false
    [[ "$output" =~ "row [1,1] would be overwritten by [1,2]" ]] || false

    run dolt sql -q "select * from t"
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "| 1  | 1    |" ]] || false
    [[ ! "$output" =~ "| 1  | 2    |" ]] || false
    [ "${#lines[@]}" -eq 0 ]
}

@test "import-append-tables: ignore rows that would have no effect on import" {
    dolt sql -q "CREATE TABLE t (pk int primary key, col1 int);"
    dolt table import -a t <<CSV
pk, col1
1, 1
CSV
    run dolt table import -a t --continue <<CSV
pk, col1
1, 1
2, 3
CSV

    [ "$status" -eq 0 ]
    [[ "$output" =~ "The following rows were skipped:" ]] || false
    [[ "$output" =~ "[1,1]" ]] || false
    [[ "$output" =~ "Rows Processed: 1, Additions: 1, Modifications: 0, Had No Effect: 0" ]] || false
    [[ "$output" =~ "Lines skipped: 1" ]] || false

    run dolt sql -q "select * from t"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| 1  | 1    |" ]] || false
    [[ "$output" =~ "| 2  | 3    |" ]] || false
    [ "${#lines[@]}" -eq 6 ]
}

@test "import-append-tables: reject rows in source that would modify rows in destination, but continue if --continue is supplied" {
    dolt sql -q "CREATE TABLE t (pk int primary key, col1 int);"
    dolt table import -a t <<CSV
pk, col1
1, 1
CSV
    run dolt table import -a t --continue <<CSV
pk, col1
1, 2
2, 3
CSV

    [ "$status" -eq 0 ]
    [[ "$output" =~ "The following rows were skipped:" ]] || false
    [[ "$output" =~ "[1,2]" ]] || false
    [[ "$output" =~ "Rows Processed: 1, Additions: 1, Modifications: 0, Had No Effect: 0" ]] || false
    [[ "$output" =~ "Lines skipped: 1" ]] || false

    run dolt sql -q "select * from t"
    [ "$status" -eq 0 ]
    [[   "$output" =~ "| 1  | 1    |" ]] || false
    [[   "$output" =~ "| 2  | 3    |" ]] || false
    [[ ! "$output" =~ "| 1  | 2    |" ]] || false
    [ "${#lines[@]}" -eq 6 ]
}