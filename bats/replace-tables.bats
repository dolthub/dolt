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

@test "replace table using schema with csv" {
    run dolt table create -s `batshelper 1pk5col-ints.schema` test
    [ "$status" -eq 0 ]
    run dolt table import -u -s `batshelper 1pk5col-ints.schema` test `batshelper 1pk5col-ints.csv`
    [ "$status" -eq 1 ]
    [[ "$output" =~ "schema is not supported for update or replace operations" ]] || false
}