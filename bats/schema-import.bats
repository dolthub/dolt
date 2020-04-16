#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}

@test "schema import create" {
    run dolt schema import -c --pks=pk test `batshelper 1pk5col-ints.csv`
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Created table successfully." ]] || false
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false
    run dolt schema show
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 10 ]
    [[ "${lines[0]}" =~ "test" ]] || false
    [[ "$output" =~ "\`pk\` BIGINT" ]] || false
    [[ "$output" =~ "\`c1\` BIGINT" ]] || false
    [[ "$output" =~ "\`c2\` BIGINT" ]] || false
    [[ "$output" =~ "\`c3\` BIGINT" ]] || false
    [[ "$output" =~ "\`c4\` BIGINT" ]] || false
    [[ "$output" =~ "\`c5\` BIGINT" ]] || false
    [[ "$output" =~ "PRIMARY KEY (\`pk\`)" ]] || false
}

@test "schema import dry run" {
    run dolt schema import --dry-run -c --pks=pk test `batshelper 1pk5col-ints.csv`
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 9 ]
    [[ "${lines[0]}" =~ "test" ]] || false
    [[ "$output" =~ "\`pk\` BIGINT" ]] || false
    [[ "$output" =~ "\`c1\` BIGINT" ]] || false
    [[ "$output" =~ "\`c2\` BIGINT" ]] || false
    [[ "$output" =~ "\`c3\` BIGINT" ]] || false
    [[ "$output" =~ "\`c4\` BIGINT" ]] || false
    [[ "$output" =~ "\`c5\` BIGINT" ]] || false

    run dolt ls
    [ "$status" -eq 0 ]
    ! [[ "$output" =~ "test" ]] || false
}

@test "schema import with a bunch of types" {
    run dolt schema import --dry-run -c --pks=pk test `batshelper 1pksupportedtypes.csv`
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 10 ]
    [[ "${lines[0]}" =~ "test" ]] || false
    [[ "$output" =~ "\`pk\` BIGINT" ]] || false
    [[ "$output" =~ "\`int\` BIGINT" ]] || false
    [[ "$output" =~ "\`string\` LONGTEXT" ]] || false
    [[ "$output" =~ "\`boolean\` BIT(1)" ]] || false
    [[ "$output" =~ "\`float\` DOUBLE" ]] || false
    [[ "$output" =~ "\`uint\` BIGINT" ]] || false
    [[ "$output" =~ "\`uuid\` CHAR(36) CHARACTER SET ascii COLLATE ascii_bin" ]] || false
}

@test "schema import with an empty csv" {
    run dolt schema import --dry-run -c --pks=pk test `batshelper bad.csv`
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Header line is empty" ]] || false
}

@test "schema import replace" {
    dolt schema import -c --pks=pk test `batshelper 1pk5col-ints.csv`
    run dolt schema import -r --pks=pk test `batshelper 1pksupportedtypes.csv`
    [ "$status" -eq 0 ]
    run dolt schema show
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 11 ]
    [[ "${lines[0]}" =~ "test" ]] || false
    [[ "$output" =~ "\`pk\` BIGINT" ]] || false
    [[ "$output" =~ "\`int\` BIGINT" ]] || false
    [[ "$output" =~ "\`string\` LONGTEXT" ]] || false
    [[ "$output" =~ "\`boolean\` BIT(1)" ]] || false
    [[ "$output" =~ "\`float\` DOUBLE" ]] || false
    [[ "$output" =~ "\`uint\` BIGINT" ]] || false
    [[ "$output" =~ "\`uuid\` CHAR(36) CHARACTER SET ascii COLLATE ascii_bin" ]] || false
}

@test "schema import with invalid names" {
    run dolt schema import -c --pks=pk 123 `batshelper 1pk5col-ints.csv`
    [ "$status" -eq 1 ]
    [[ "$output" =~ "not a valid table name" ]] || false
    run dolt schema import -c --pks=pk dolt_docs `batshelper 1pk5col-ints.csv`
    [ "$status" -eq 1 ]
    [[ "$output" =~ "not a valid table name" ]] || false
    [[ "$output" =~ "reserved" ]] || false
    run dolt schema import -c --pks=pk dolt_query_catalog `batshelper 1pk5col-ints.csv`
    [ "$status" -eq 1 ]
    [[ "$output" =~ "not a valid table name" ]] || false
    [[ "$output" =~ "reserved" ]] || false
    run dolt schema import -c --pks=pk dolt_reserved `batshelper 1pk5col-ints.csv`
    [ "$status" -eq 1 ]
    [[ "$output" =~ "not a valid table name" ]] || false
    [[ "$output" =~ "reserved" ]] || false
}

@test "schema import with multiple primary keys" {
    run dolt schema import -c --pks=pk1,pk2 test `batshelper 2pk5col-ints.csv`
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Created table successfully." ]] || false
    run dolt schema show
    [ "${#lines[@]}" -eq 11 ]
    [[ "${lines[0]}" =~ "test" ]] || false
    [[ "$output" =~ "\`pk1\` BIGINT" ]] || false
    [[ "$output" =~ "\`pk2\` BIGINT" ]] || false
    [[ "$output" =~ "\`c1\` BIGINT" ]] || false
    [[ "$output" =~ "\`c2\` BIGINT" ]] || false
    [[ "$output" =~ "\`c3\` BIGINT" ]] || false
    [[ "$output" =~ "\`c4\` BIGINT" ]] || false
    [[ "$output" =~ "\`c5\` BIGINT" ]] || false
    [[ "$output" =~ "PRIMARY KEY (\`pk1\`,\`pk2\`)" ]] || false
}

@test "schema import missing values in CSV rows" {
    run dolt schema import -c --pks=pk test `batshelper empty-strings-null-values.csv`
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 7 ]
    [[ "${lines[0]}" =~ "test" ]] || false
    [[ "$output" =~ "\`pk\` LONGTEXT" ]] || false
    [[ "$output" =~ "\`headerOne\` LONGTEXT" ]] || false
    [[ "$output" =~ "\`headerTwo\` BIGINT" ]] || false
}

@test "schema import --keep-types" {
    run dolt schema import -c --keep-types --pks=pk test `batshelper 1pk5col-ints.csv`
    [ "$status" -eq 1 ]
    [[ "$output" =~ "parameter keep-types not supported for create operations" ]] || false
    dolt schema import -c --pks=pk test `batshelper 1pk5col-ints.csv`
    run dolt schema import -r --keep-types --pks=pk test `batshelper 1pk5col-strings.csv`
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 11 ]
    [[ "${lines[0]}" =~ "test" ]] || false
    [[ "$output" =~ "\`pk\` BIGINT" ]] || false
    [[ "$output" =~ "\`c1\` BIGINT" ]] || false
    [[ "$output" =~ "\`c2\` BIGINT" ]] || false
    [[ "$output" =~ "\`c3\` BIGINT" ]] || false
    [[ "$output" =~ "\`c4\` BIGINT" ]] || false
    [[ "$output" =~ "\`c5\` BIGINT" ]] || false
    [[ "$output" =~ "\`c6\` LONGTEXT" ]] || false
    [[ "$output" =~ "PRIMARY KEY (\`pk\`)" ]] || false
}

@test "schema import with strings in csv" {
    # This CSV has quoted integers for the primary key ie "0","foo",... and
    # "1","bar",...
    run dolt schema import -r --keep-types --pks=pk test `batshelper 1pk5col-strings.csv`
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 11 ]
    [[ "${lines[0]}" =~ "test" ]] || false
    [[ "$output" =~ "\`pk\` BIGINT" ]] || false
    [[ "$output" =~ "\`c1\` LONGTEXT" ]] || false
    [[ "$output" =~ "\`c2\` LONGTEXT" ]] || false
    [[ "$output" =~ "\`c3\` LONGTEXT" ]] || false
    [[ "$output" =~ "\`c4\` LONGTEXT" ]] || false
    [[ "$output" =~ "\`c5\` LONGTEXT" ]] || false
    [[ "$output" =~ "\`c6\` LONGTEXT" ]] || false
    [[ "$output" =~ "PRIMARY KEY (\`pk\`)" ]] || false
}

@test "schema import supports dates andf times" {
    run dolt schema import -c --pks=pk test `batshelper 1pk-datetime.csv`
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]
    skip "schema import does not support datetime"
    [[ "$output" =~ "DATETIME" ]] || false;
}

@test "schema import of two tables" {
    dolt schema imporT -c --pks=pk test1 `batshelper 1pksupportedt\
ypes.csv`
    skip "Guaranteed tag collision right now"
    dolt schema import -c --pks=pk test2 `batshelper 1pk-datetime.csv`
    
}
