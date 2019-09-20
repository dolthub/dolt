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

@test "replace table using csv" {
    run dolt table create -s `batshelper 1pk5col-ints.schema` test
    [ "$status" -eq 0 ]
    run dolt table import -r test `batshelper 1pk5col-ints.csv`
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Rows Processed: 2, Additions: 2, Modifications: 0, Had No Effect: 0" ]] || false
    [[ "$output" =~ "Import completed successfully." ]] || false
}

@test "replace table using csv with wrong schema" {
    run dolt table create -s `batshelper 1pk5col-ints.schema` test
    [ "$status" -eq 0 ]
    run dolt table import -r test `batshelper 2pk5col-ints.csv`
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Error replacing table" ]] || false
    [[ "$output" =~ "cause: Schema from file does not match schema from existing table." ]] || false
}

@test "replace table using psv" {
    run dolt table create -s `batshelper 1pk5col-ints.schema` test
    [ "$status" -eq 0 ]
    run dolt table import -r test `batshelper 1pk5col-ints.psv`
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Rows Processed: 2, Additions: 2, Modifications: 0, Had No Effect: 0" ]] || false
    [[ "$output" =~ "Import completed successfully." ]] || false
}

@test "replace table using psv with wrong schema" {
    run dolt table create -s `batshelper 2pk5col-ints.schema` test
    [ "$status" -eq 0 ]
    run dolt table import -r test `batshelper 1pk5col-ints.psv`
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Error replacing table" ]] || false
    [[ "$output" =~ "cause: Schema from file does not match schema from existing table." ]] || false
}

@test "replace table using schema with csv" {
    run dolt table create -s `batshelper 1pk5col-ints.schema` test
    [ "$status" -eq 0 ]
    run dolt table import -r -s `batshelper 1pk5col-ints.schema` test `batshelper 1pk5col-ints.csv`
    [ "$status" -eq 1 ]
    [[ "$output" =~ "schema is not supported for update or replace operations" ]] || false
}

@test "replace table using json" {
    run dolt table create -s `batshelper employees-sch.json` employees
    [ "$status" -eq 0 ]
    run dolt table import -r employees `batshelper employees-tbl.json`
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Rows Processed: 3, Additions: 3, Modifications: 0, Had No Effect: 0" ]] || false
    [[ "$output" =~ "Import completed successfully." ]] || false
}

@test "replace table using json with wrong schema" {
    run dolt table create -s `batshelper employees-sch-wrong.json` employees
    [ "$status" -eq 0 ]
    run dolt table import -r employees `batshelper employees-tbl.json`
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Error replacing table" ]] || false
    [[ "$output" =~ "cause: Schema from file does not match schema from existing table." ]] || false
}

@test "replace table using schema with json" {
    run dolt table create -s `batshelper employees-sch-wrong.json` employees
    [ "$status" -eq 0 ]
    run dolt table import -r -s `batshelper employees-sch.json` employees `batshelper employees-tbl.json`
    [ "$status" -eq 1 ]
    [[ "$output" =~ "fatal: schema is not supported for update or replace operations" ]] || false
}

@test "replace table with json when table does not exist" {
    run dolt table import -r employees `batshelper employees-tbl.json`
    [ "$status" -eq 1 ]
    [[ "$output" =~ "The following table could not be found:" ]] || false
}

@test "replace table with existing data using json" {
    run dolt table import -c -s `batshelper employees-sch.json` employees `batshelper employees-tbl.json`
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Import completed successfully." ]] || false
    run dolt table import -r employees `batshelper employees-tbl-new.json`
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Rows Processed: 4, Additions: 4, Modifications: 0, Had No Effect: 0" ]] || false
    [[ "$output" =~ "Import completed successfully." ]] || false
}

@test "replace table with existing data with different schema" {
    run dolt table import -c -s `batshelper employees-sch.json` employees `batshelper employees-tbl.json`
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Import completed successfully." ]] || false
    run dolt table import -r employees `batshelper employees-tbl-schema-wrong.json`
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Error replacing table" ]] || false
    [[ "$output" =~ "cause: Schema from file does not match schema from existing table." ]] || false
}

@test "replace table with bad json" {
    run dolt table create -s `batshelper employees-sch.json` employees
    [ "$status" -eq 0 ]
    run dolt table import -r employees `batshelper employees-tbl-bad.json`
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Error creating reader" ]] || false
    [[ "$output" =~ "employees-tbl-bad.json to" ]] || false
}

@test "replace table using xlsx file" {
    run dolt table create -s `batshelper employees-sch-2.json` employees
    [ "$status" -eq 0 ]
    run dolt table import -r employees `batshelper employees.xlsx`
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Rows Processed: 3, Additions: 3, Modifications: 0, Had No Effect: 0" ]] || false
    [[ "$output" =~ "Import completed successfully." ]] || false
}

@test "replace table using xlsx file with wrong schema" {
    run dolt table create -s `batshelper employees-sch.json` employees
    [ "$status" -eq 0 ]
    run dolt table import -r employees `batshelper employees.xlsx`
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Error replacing table" ]] || false
    [[ "$output" =~ "cause: Schema from file does not match schema from existing table." ]] || false
}

@test "replace table with 2 primary keys with a csv with one primary key" {
    run dolt table import -c --pk=pk1,pk2 test `batshelper 2pk5col-ints.csv`
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Import completed successfully." ]] || false
    run dolt table import -r test `batshelper 1pk5col-ints.csv`
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Error replacing table" ]] || false
    [[ "$output" =~ "cause: Schema from file does not match schema from existing table." ]] || false
}

@test "replace table with 2 primary keys with a csv with 2 primary keys" {
    run dolt table import -c --pk=pk1,pk2 test `batshelper 2pk5col-ints.csv`
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Import completed successfully." ]] || false
    run dolt table import -r test `batshelper 2pk5col-ints.csv`
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Rows Processed: 4, Additions: 4, Modifications: 0, Had No Effect: 0" ]] || false
    [[ "$output" =~ "Import completed successfully." ]] || false
}