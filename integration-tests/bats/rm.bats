#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "rm: dolt rm without args" {
    run dolt rm
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Nothing specified, nothing removed. Which tables should I remove?" ]] || false
}

@test "rm: simple dolt rm" {
    dolt sql -q "CREATE TABLE test (i int)"
    dolt commit -A -m "created table"

    run dolt rm test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "rm 'test'" ]] || false

    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "No tables in working set" ]] || false
}

@test "rm: dolt rm nonexistent table" {
    run dolt rm nonexistent
    [ "$status" -eq 1 ]
    [[ "$output" =~ "error: the table(s) nonexistent do not exist" ]] || false
}

@test "rm: dolt rm multiple tables" {
    dolt sql -q "CREATE TABLE test1 (i int)"
    dolt sql -q "CREATE TABLE test2 (i int)"
    dolt commit -A -m "created tables"

    run dolt rm test1 test2
    [ "$status" -eq 0 ]
    [[ "$output" =~ "rm 'test1'" ]] || false
    [[ "$output" =~ "rm 'test2'" ]] || false

    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "No tables in working set" ]] || false
}

@test "rm: dolt rm multiple tables, some nonexistent" {
    dolt sql -q "CREATE TABLE test1 (i int)"
    dolt commit -A -m "created table"

    run dolt rm test1 test2 test3
    [ "$status" -eq 1 ]
    [[ "$output" =~ "error: the table(s) test2, test3 do not exist" ]] || false
}

@test "rm: dolt rm staged table" {
    dolt sql -q "CREATE TABLE test (i int)"
    dolt add test

    run dolt rm test
    [ "$status" -eq 1 ]
    [[ "$output" =~ "error: the table(s) test have changes saved in the index. Use --cached or commit." ]] || false
}

@test "rm: dolt rm unstaged table" {
    dolt sql -q "CREATE TABLE test (i int)"

    run dolt rm test
    [ "$status" -eq 1 ]
    [[ "$output" =~ "error: the table(s) test have unstaged changes." ]] || false
}

@test "rm: dolt rm with cached option" {
    dolt sql -q "CREATE TABLE test (i int)"
    dolt commit -A -m "created table"

    run dolt rm --cached test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "removed 'test' from tracking" ]] || false

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "deleted:" ]] || false
    [[ "$output" =~ "test" ]] || false
    [[ "$output" =~ "new table:" ]] || false
}

@test "rm: dolt rm errors on foreign key constrained table" {
    dolt sql -q "CREATE TABLE parent (pk int primary key, p1 int)"
    dolt sql -q "CREATE TABLE child (pk int primary key, c1 int, FOREIGN KEY (c1) REFERENCES parent (pk))"
    dolt commit -A -m "created tables"

    run dolt rm parent
    [ "$status" -eq 1 ]
    [[ "$output" =~ "unable to remove \`parent\` since it is referenced from table \`child\`" ]] || false
}