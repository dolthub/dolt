#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "rm: dolt rm without tables" {
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

@test "rm: dolt rm staged table with unstaged changes" {
    dolt sql -q "CREATE TABLE test (i int)"
    dolt add test
    dolt sql -q "INSERT INTO test VALUES (1)"

    run dolt rm test
    [ "$status" -eq 1 ]
    [[ "$output" =~ "error: the table(s) test have unstaged changes." ]] || false
}

@test "rm: dolt rm committed table with unstaged changes" {
    dolt sql -q "CREATE TABLE test (i int)"
    dolt commit -A -m "created table"
    dolt sql -q "INSERT INTO test VALUES (1)"

    run dolt rm test
    [ "$status" -eq 1 ]
    [[ "$output" =~ "error: the table(s) test have unstaged changes." ]] || false
}

@test "rm: dolt rm with cached option" {
    dolt sql -q "CREATE TABLE test (i int)"
    dolt commit -A -m "created table"

    run dolt rm --cached test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "rm 'test'" ]] || false

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "deleted:" ]] || false
    [[ "$output" =~ "test" ]] || false
    [[ "$output" =~ "new table:" ]] || false
}

@test "rm: dolt rm staged table with cached option" {
    dolt sql -q "CREATE TABLE test (i int)"
    dolt add test

    run dolt rm --cached test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "rm 'test'" ]] || false

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Untracked tables:" ]] || false
    [[ "$output" =~ "new table:" ]] || false
    [[ "$output" =~ "test" ]] || false
}

@test "rm: dolt rm staged and unstaged with cached option" {
    dolt sql -q "CREATE TABLE committed (i int)"
    dolt commit -A -m "created table"
    dolt sql -q "CREATE TABLE staged (i int)"
    dolt add staged

    run dolt rm --cached committed staged
    [ "$status" -eq 0 ]
    [[ "$output" =~ "rm 'committed'" ]] || false
    [[ "$output" =~ "rm 'staged'" ]] || false

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "deleted:" ]] || false
    [[ "$output" =~ "committed" ]] || false
    [[ "$output" =~ "new table:" ]] || false
    [[ "$output" =~ "staged" ]] || false
}

@test "rm: dolt rm errors on foreign key constrained table" {
    dolt sql -q "CREATE TABLE parent (pk int primary key, p1 int)"
    dolt sql -q "CREATE TABLE child (pk int primary key, c1 int, FOREIGN KEY (c1) REFERENCES parent (pk))"
    dolt commit -A -m "created tables"

    run dolt rm parent
    [ "$status" -eq 1 ]
    [[ "$output" =~ "unable to remove \`parent\` since it is referenced from table \`child\`" ]] || false
}

@test "rm: dolt rm errors on staged foreign key constrained table with cached option" {
    dolt sql -q "CREATE TABLE parent (pk int primary key, p1 int)"
    dolt commit -A -m "created tables"
    dolt sql -q "CREATE TABLE child (pk int primary key, c1 int, FOREIGN KEY (c1) REFERENCES parent (pk))"
    dolt add child

    run dolt rm --cached parent
    [ "$status" -eq 1 ]
    [[ "$output" =~ "unable to remove \`parent\` since it is referenced from table \`child\`" ]] || false
}

@test "rm: dolt rm errors on unstaged foreign key constrained table with cached option" {
    dolt sql -q "CREATE TABLE parent (pk int primary key, p1 int)"
    dolt commit -A -m "created tables"
    dolt sql -q "CREATE TABLE child (pk int primary key, c1 int, FOREIGN KEY (c1) REFERENCES parent (pk))"

    run dolt rm --cached parent
    [ "$status" -eq 1 ]
    [[ "$output" =~ "unable to remove \`parent\` since it is referenced from table \`child\`" ]] || false
}

@test "rm: dolt rm nonexistent table" {
    run dolt rm nonexistent
    echo "$output"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "error: the table(s) nonexistent do not exist" ]] || false
}