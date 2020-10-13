#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    cp -Rpf $REPO_DIR bats_repo
    cd bats_repo
}

teardown() {
    teardown_common
    cd ..
    rm -rf bats_repo
}

@test "dolt version" {
    # this will fail for older dolt versions but BATS will swallow the error
    run dolt migrate

    run dolt version
    [ "$status" -eq 0 ]
    regex='dolt version [0-9]+.[0-9]+.[0-9]+'
    [[ "$output" =~ $regex ]] || false
}

@test "dolt status" {
    skip "These compatibility tests fail now due to a backwards incompatibility with the dolt_docs table. Before v0.16.0 dolt_docs used tags 0 and 1, and these values were hard coded in the logic that syncs the docs table with the file system."
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch master" ]] || false
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false
}

@test "dolt ls" {
    # this will fail for older dolt versions but BATS will swallow the error
    run dolt migrate

    run dolt ls
    [ "$status" -eq 0 ]
    [[ "${lines[0]}" =~ "Tables in working set:" ]] || false
}

@test "dolt branch" {
    # this will fail for older dolt versions but BATS will swallow the error
    run dolt migrate

    run dolt branch
    [ "$status" -eq 0 ]
}

@test "dolt diff" {
    skip "These compatibility tests fail now due to a backwards incompatibility with the dolt_docs table. Before v0.16.0 dolt_docs used tags 0 and 1, and these values were hard coded in the logic that syncs the docs table with the file system."
    run dolt diff
    [ "$status" -eq 0 ]
}

@test "dolt schema show on branch init" {
    # this will fail for older dolt versions but BATS will swallow the error
    run dolt migrate

    dolt checkout init
    run dolt schema show abc
    [ "$status" -eq 0 ]
    output=`echo $output | tr '[:upper:]' '[:lower:]'` # lowercase the output
    [[ "${output}" =~ "abc @ working" ]] || false
    [[ "${output}" =~ "create table \`abc\` (" ]] || false
    [[ "${output}" =~ "\`pk\` bigint not null" ]] || false
    [[ "${output}" =~ "\`a\` longtext" ]] || false
    [[ "${output}" =~ "\`b\` double" ]] || false
    [[ "${output}" =~ "\`w\` bigint" ]] || false
    [[ "${output}" =~ "\`x\` bigint" ]] || false
    [[ "${output}" =~ "primary key (\`pk\`)" ]] || false
}

@test "dolt sql 'select * from abc' on branch init" {
    # this will fail for older dolt versions but BATS will swallow the error
    run dolt migrate

    dolt checkout init
    run dolt sql -q 'select * from abc;'
    [ "$status" -eq 0 ]


    [[ "${lines[1]}" =~ "| pk | a    | b   | w | x |" ]] || false
    [[ "${lines[2]}" =~ "+----+------+-----+---+---+" ]] || false
    [[ "${lines[3]}" =~ "| 0  | asdf | 1.1 | 0 | 0 |" ]] || false
    [[ "${lines[4]}" =~ "| 1  | asdf | 1.1 | 0 | 0 |" ]] || false
    [[ "${lines[5]}" =~ "| 2  | asdf | 1.1 | 0 | 0 |" ]] || false
}

@test "dolt schema show on branch master" {
    # this will fail for older dolt versions but BATS will swallow the error
    run dolt migrate

    run dolt schema show abc
    [ "$status" -eq 0 ]
    output=`echo $output | tr '[:upper:]' '[:lower:]'` # lowercase the output
    [[ "${output}" =~ "abc @ working" ]] || false
    [[ "${output}" =~ "create table \`abc\` (" ]] || false
    [[ "${output}" =~ "\`pk\` bigint not null" ]] || false
    [[ "${output}" =~ "\`a\` longtext" ]] || false
    [[ "${output}" =~ "\`b\` double" ]] || false
    [[ "${output}" =~ "\`x\` bigint" ]] || false
    [[ "${output}" =~ "\`y\` bigint" ]] || false
    [[ "${output}" =~ "primary key (\`pk\`)" ]] || false
}


@test "dolt sql 'select * from abc' on branch master" {
    # this will fail for older dolt versions but BATS will swallow the error
    run dolt migrate

    run dolt sql -q 'select * from abc;'
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "| pk | a    | b   | x | y      |" ]] || false
    [[ "${lines[2]}" =~ "+----+------+-----+---+--------+" ]] || false
    [[ "${lines[3]}" =~ "| 0  | asdf | 1.1 | 0 | <NULL> |" ]] || false
    [[ "${lines[4]}" =~ "| 2  | asdf | 1.1 | 0 | <NULL> |" ]] || false
    [[ "${lines[5]}" =~ "| 3  | data | 1.1 | 0 | <NULL> |" ]] || false
}

@test "dolt schema show on branch other" {
    # this will fail for older dolt versions but BATS will swallow the error
    run dolt migrate

    dolt checkout other
    run dolt schema show abc
    [ "$status" -eq 0 ]
    output=`echo $output | tr '[:upper:]' '[:lower:]'` # lowercase the output
    [[ "${output}" =~ "abc @ working" ]] || false
    [[ "${output}" =~ "create table \`abc\` (" ]] || false
    [[ "${output}" =~ "\`pk\` bigint not null" ]] || false
    [[ "${output}" =~ "\`a\` longtext" ]] || false
    [[ "${output}" =~ "\`b\` double" ]] || false
    [[ "${output}" =~ "\`w\` bigint" ]] || false
    [[ "${output}" =~ "\`z\` bigint" ]] || false
    [[ "${output}" =~ "primary key (\`pk\`)" ]] || false
}

@test "dolt sql 'select * from abc' on branch other" {
    # this will fail for older dolt versions but BATS will swallow the error
    run dolt migrate

    dolt checkout other
    run dolt sql -q 'select * from abc;'
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "| pk | a    | b   | w | z      |" ]] || false
    [[ "${lines[2]}" =~ "+----+------+-----+---+--------+" ]] || false
    [[ "${lines[3]}" =~ "| 0  | asdf | 1.1 | 0 | <NULL> |" ]] || false
    [[ "${lines[4]}" =~ "| 1  | asdf | 1.1 | 0 | <NULL> |" ]] || false
    [[ "${lines[5]}" =~ "| 4  | data | 1.1 | 0 | <NULL> |" ]] || false

    dolt checkout master
}

@test "dolt table import" {
    # this will fail for older dolt versions but BATS will swallow the error
    run dolt migrate

    run dolt table import -c abc2 abc.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Import completed successfully." ]] || false

    dolt sql -q 'drop table abc2'
}


@test "dolt migrate no-data" {
    # this will fail for older dolt versions but BATS will swallow the error
    run dolt migrate

    dolt checkout no-data
    run dolt sql -q 'show tables;'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+-------+" ]] || false
    [[ "$output" =~ "| Table |" ]] || false
    [[ "$output" =~ "+-------+" ]] || false
    [[ "$output" =~ "+-------+" ]] || false
}

@test "dolt_schemas" {
    # this will fail for older dolt versions but BATS will swallow the error
    run dolt migrate

    run dolt sql -q "select * from dolt_schemas"
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "| type | name  | fragment             |" ]] || false
    [[ "${lines[2]}" =~ "+------+-------+----------------------+" ]] || false
    [[ "${lines[3]}" =~ "| view | view1 | SELECT 2+2 FROM dual |" ]] || false
    run dolt sql -q 'select * from view1'
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "| 2 + 2 |" ]] || false
    [[ "${lines[2]}" =~ "+-------+" ]] || false
    [[ "${lines[3]}" =~ "| 4     |" ]] || false
}