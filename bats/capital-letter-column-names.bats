#!/usr/bin/env bats

setup() {
    export PATH=$PATH:~/go/bin
    export NOMS_VERSION_NEXT=1
    cd $BATS_TMPDIR
    mkdir "dolt-repo-$$"
    cd "dolt-repo-$$"
    dolt init
    dolt table import -c -s $BATS_TEST_DIRNAME/helper/capital-letter-column-names.schema test $BATS_TEST_DIRNAME/helper/capital-letter-column-names.csv
}

teardown() {
    rm -rf "$BATS_TMPDIR/dolt-repo-$$"
}

@test "capital letter col names. put a row in a table using dolt table put-row" {
    dolt table put-row test Aaa:3 Bbb:ccc Ccc:CCC
    run dolt table select test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "CCC" ]] || false
}

@test "capital letter col names. remove a row from a table with dolt table rm-row" {
    dolt table rm-row test Aaa 2
    run dolt table select test
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "BBB" ]] || false
}

@test "capital letter col names. dolt table select with a where clause" {
    run dolt table select test --where Aaa=2
    [ "$status" -eq 0 ]
    [[ "$output" =~ "BBB" ]] || false
}

@test "capital letter col names. dolt schema" {
    run dolt schema
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Aaa" ]] || false
    [[ "$output" =~ "Bbb" ]] || false
    [[ "$output" =~ "Ccc" ]] || false
}

@test "capital letter col names. sql select" {
    run dolt sql -q "select Bbb from test where Aaa=2"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "bbb" ]] || false
    [[ "$output" =~ "Bbb" ]] || false
    [[ ! "$output" =~ "Aaa" ]] || false
    [[ ! "$output" =~ "aaa" ]] || false
}

@test "capital letter col names. dolt table export" {
    run dolt table export test export.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully exported data" ]] || false
    run cat export.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "bbb" ]] || false
    [[ "$output" =~ "Bbb" ]] || false
    [[ "$output" =~ "Aaa" ]] || false
    [[ "$output" =~ "aaa" ]] || false
}

@test "capital letter col names. dolt table copy" {
    run dolt table cp test test-copy
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt table select test-copy
    [ "$status" -eq 0 ]
    [[ "$output" =~ "bbb" ]] || false
    [[ "$output" =~ "Bbb" ]] || false
    [[ "$output" =~ "Aaa" ]] || false
    [[ "$output" =~ "aaa" ]] || false
}
