#!/usr/bin/env bats

setup() {
    export PATH=$PATH:~/go/bin
    export NOMS_VERSION_NEXT=1
    cd $BATS_TMPDIR
    mkdir dolt-repo
    cd dolt-repo
    dolt init
}

teardown() {
    rm -rf $BATS_TMPDIR/dolt-repo
}

@test "create a table with a schema file and examine repo" {
    run dolt table create -s=$BATS_TEST_DIRNAME/1pk5col.schema test
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
}

@test "import data from csv and create the table" {
    run dolt table import -c --pk=pk test $BATS_TEST_DIRNAME/1pk5col.csv
        [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt table select test
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
}

@test "import data from psv and create the table" {
    run dolt table import -c --pk=pk test $BATS_TEST_DIRNAME/1pk5col.psv
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt table select test
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
}

