#!/usr/bin/env bats

setup() {
    load $BATS_TEST_DIRNAME/helper/common.bash
    export PATH=$PATH:~/go/bin
    export NOMS_VERSION_NEXT=1
    cd $BATS_TMPDIR
    mkdir "dolt-repo-$$"
    cd "dolt-repo-$$"
    dolt init
}

teardown() {
    rm -rf "$BATS_TMPDIR/dolt-repo-$$"
}

@test "update table using csv" {
    run dolt table create -s `batshelper 1pk5col-ints.schema` test
    [ "$status" -eq 0 ]
    run dolt table import -u test `batshelper 1pk5col-ints.csv`
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Rows Processed: 2, Additions: 2, Modifications: 0, Had No Effect: 0" ]] || false
    [[ "$output" =~ "Import completed successfully." ]] || false
}

@test "update table using schema with csv" {
    run dolt table create -s `batshelper 1pk5col-ints.schema` test
    [ "$status" -eq 0 ]
    run dolt table import -u -s `batshelper 1pk5col-ints.schema` test `batshelper 1pk5col-ints.csv`
    [ "$status" -eq 1 ]
    [[ "$output" =~ "fatal: schema is not supported for update or replace operations" ]] || false
}

@test "update table using csv with newlines" {
    skip "We currently fail on CSV imports with newlines"
    run dolt table create -s `batshelper 1pk5col-strings.schema` test
    [ "$status" -eq 0 ]
    run dolt table import -u test `batshelper 1pk5col-strings-newlines.csv`
    [ "$status" -eq 0 ]
}

@test "update table using json" {
    run dolt table create -s `batshelper employees-sch.json` employees
    [ "$status" -eq 0 ]
    run dolt table import -u employees `batshelper employees-tbl.json`
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Rows Processed: 3, Additions: 3, Modifications: 0, Had No Effect: 0" ]] || false
    [[ "$output" =~ "Import completed successfully." ]] || false
}

@test "update table using wrong json" {
    run dolt table create -s `batshelper employees-sch-wrong.json` employees
    [ "$status" -eq 0 ]
    run dolt table import -u employees `batshelper employees-tbl.json`
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Rows Processed: 0, Additions: 0, Modifications: 0, Had No Effect: 0" ]] || false
}

@test "update table using schema with json" {
    run dolt table create -s `batshelper employees-sch-wrong.json` employees
    [ "$status" -eq 0 ]
    run dolt table import -u -s `batshelper employees-sch.json` employees `batshelper employees-tbl.json`
    [ "$status" -eq 1 ]
    [[ "$output" =~ "fatal: schema is not supported for update or replace operations" ]] || false
}

@test "update table with existing imported data with different schema" {
  run dolt table import -c -s `batshelper employees-sch.json` employees `batshelper employees-tbl.json`
  [ "$status" -eq 0 ]
  [[ "$output" =~ "Import completed successfully." ]] || false
  run dolt table import -u employees `batshelper employees-tbl-schema-wrong.json`
  [ "$status" -eq 0 ]
  [[ "$output" =~ "Rows Processed: 0, Additions: 0, Modifications: 0, Had No Effect: 0" ]] || false
}

@test "update table with json when table does not exist" {
    run dolt table import -u employees `batshelper employees-tbl.json`
    [ "$status" -eq 1 ]
    [[ "$output" =~ "The following table could not be found:" ]] || false
}

@test "replace table with a json with columns in different order" {
    run dolt table create -s `batshelper employees-sch.json` employees
    [ "$status" -eq 0 ]
    run dolt table import -u employees `batshelper employees-tbl-schema-unordered.json`
    echo "$output"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Rows Processed: 3, Additions: 3, Modifications: 0, Had No Effect: 0" ]] || false
    [[ "$output" =~ "Import completed successfully." ]] || false
}

@test "replace table with a csv with columns in different order" {
    run dolt table create -s `batshelper employees-sch.json` employees
    [ "$status" -eq 0 ]
    run dolt table import -u employees `batshelper employees-tbl-schema-unordered.csv`
    echo "$output"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Rows Processed: 3, Additions: 3, Modifications: 0, Had No Effect: 0" ]] || false
    [[ "$output" =~ "Import completed successfully." ]] || false
}
