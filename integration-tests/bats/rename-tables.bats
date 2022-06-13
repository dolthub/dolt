#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "rename-tables: rename a table with sql" {
    dolt sql -q "CREATE TABLE test (pk int PRIMARY KEY);"
    run dolt sql -q "ALTER TABLE test RENAME quiz;"
    [ "$status" -eq 0 ]
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "quiz" ]] || false
    [[ ! "$output" =~ "test" ]] || false
    run dolt sql -q "CREATE TABLE test (pk int primary key);"
    [ "$status" -eq 0 ]
    run dolt sql -q "CREATE TABLE quiz (pk int primary key);"
    [ "$status" -ne 0 ]
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "quiz" ]] || false
    [[ "$output" =~ "test" ]] || false
}

# see 'cp-and-mv.bats' for renaming a table with `dolt mv`

@test "rename-tables: diff a renamed table" {
    skip_nbf_dolt_1
    dolt sql <<SQL
CREATE TABLE test (pk int PRIMARY KEY);
SQL
    dolt add test
    dolt commit -m 'added table test'
    run dolt sql -q 'alter table test rename to quiz'
    [ "$status" -eq 0 ]
    run dolt diff
    [ "$status" -eq 0 ]
    [[ "${lines[0]}" =~ "diff --dolt a/test b/quiz" ]] || false
    [[ "${lines[1]}" =~ "--- a/test @" ]] || false
    [[ "${lines[2]}" =~ "+++ b/quiz @" ]] || false
}

@test "rename-tables: sql diff a renamed table" {
    skip_nbf_dolt_1
    dolt sql <<SQL
CREATE TABLE test (pk int PRIMARY KEY);
SQL
    dolt add test
    dolt commit -m 'added table test'
    run dolt sql -q 'alter table test rename to quiz'
    [ "$status" -eq 0 ]
    dolt diff -r sql
    run dolt diff -r sql
    [ "$status" -eq 0 ]
    [[ "$output" =~ "RENAME TABLE \`test\` TO \`quiz\`" ]] || false
}

@test "rename-tables: merge a renamed table" {
    dolt sql <<SQL
CREATE TABLE test (pk int PRIMARY KEY);
INSERT INTO test VALUES (0),(1);
SQL
    dolt add -A && dolt commit -m "created table test"
    dolt checkout -b other
    dolt sql -q "INSERT INTO test VALUES (8);"
    dolt add -A && dolt commit -m "inserted some values on branch other"
    dolt checkout main
    dolt sql <<SQL
RENAME TABLE test TO quiz;
INSERT INTO quiz VALUES (9);
SQL
    dolt add -A && dolt commit -m "renamed test to quiz, added values"
    skip "merge works on matching table names currently, panics on renames"
    run dolt merge other
    [ "$status" -eq 0 ]
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "quiz" ]] || false
    [[ ! "$output" =~ "test" ]] || false
    run dolt sql -q "SELECT * FROM quiz;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "0" ]] || false
    [[ "$output" =~ "1" ]] || false
    [[ "$output" =~ "8" ]] || false
    [[ "$output" =~ "9" ]] || false
}

@test "rename-tables: Reusing a table name should NOT fail" {
    dolt sql -q "CREATE TABLE one (pk int PRIMARY KEY);"
    dolt sql -q "CREATE TABLE two (pk int PRIMARY KEY);"
    dolt add -A && dolt commit -m "create test tables"
    dolt sql -q "DROP TABLE one;"
    dolt add -A && dolt commit -m "dropped table one"
    dolt sql -q "DROP TABLE two;"
    dolt sql -q "CREATE TABLE three (pk int PRIMARY KEY);"
    dolt sql -q "DROP TABLE three;"

    run dolt sql -q "CREATE TABLE one (pk int PRIMARY KEY);"
    [ $status -eq 0 ]
    run dolt sql -q "CREATE TABLE two (pk int PRIMARY KEY);"
    [ $status -eq 0 ]
    run dolt sql -q "CREATE TABLE three (pk int PRIMARY KEY);"
    [ $status -eq 0 ]
    run dolt ls
    [[ "$output" =~ "one" ]] || false
    [[ "$output" =~ "two" ]] || false
    [[ "$output" =~ "three" ]] || false
    run dolt add -A
    [ $status -eq 0 ]
    run dolt commit -m "commit recreated tables"
    [ $status -eq 0 ]
}
