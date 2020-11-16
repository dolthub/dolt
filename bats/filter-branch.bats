#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common

    dolt sql <<SQL
CREATE TABLE test (
  pk int NOT NULL PRIMARY KEY,
  c0 int
);
INSERT INTO test VALUES
    (0,0),(1,1),(2,2);
SQL
    dolt add -A
    dolt commit -m "added table test"
}

teardown() {
    teardown_common
}

@test "dolt filter-branch smoke-test" {
    dolt sql -q "INSERT INTO test VALUES (7,7),(8,8),(9,9);"
    dolt add -A && dolt commit -m "added more rows"

    dolt filter-branch "delete FROM test WHERE pk > 1;"
    run dolt sql -q "SELECT count(*) FROM test" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "2" ]] || false

    dolt sql -q "SELECT max(pk), max(c0) FROM dolt_history_test;" -r csv
    run dolt sql -q "SELECT max(pk), max(c0) FROM dolt_history_test;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,1" ]] || false
}

@test "filter multiple branches" {
    dolt branch other

    dolt sql -q "INSERT INTO test VALUES (7,7),(8,8),(9,9);"
    dolt add -A && dolt commit -m "added more rows"

    dolt checkout other
    dolt sql -q "INSERT INTO test VALUES (4,4),(5,5),(6,6);"
    dolt add -A && dolt commit -m "added more rows"

    dolt checkout master
    dolt filter-branch --all "delete FROM test WHERE pk > 4;"

    run dolt sql -q "SELECT pk,c0 FROM dolt_history_test ORDER BY pk" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "0,0" ]] || false
    [[ "$output" =~ "0,0" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "2,2" ]] || false

    dolt checkout other
    run dolt sql -q "SELECT pk,c0 FROM dolt_history_test ORDER BY pk" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "0,0" ]] || false
    [[ "$output" =~ "0,0" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "4,4" ]] || false
}

@test "filter branch with missing table" {
    dolt sql -q "DROP TABLE test;"
    dolt add -A && dolt commit -m "dropped test"

    # filter-branch warns about missing table but doesn't error
    run dolt filter-branch "delete FROM test WHERE pk > 1;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "table not found: test" ]] || false

    run dolt sql -q "SELECT count(*) FROM test AS OF 'HEAD~1';" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "2" ]] || false
}

@test "filter branch forks history" {
    dolt branch other

    dolt sql -q "INSERT INTO test VALUES (7,7),(8,8),(9,9);"
    dolt add -A && dolt commit -m "added more rows"

    dolt filter-branch "delete FROM test WHERE pk > 1;"

    dolt checkout other
    run dolt sql -q "SELECT * FROM test WHERE pk > 1" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "2,2" ]] || false
}