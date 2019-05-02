#!/usr/bin/env bats

setup() {
    export PATH=$PATH:~/go/bin
    export NOMS_VERSION_NEXT=1
    cd $BATS_TMPDIR
    mkdir "dolt-repo-$$"
    cd "dolt-repo-$$"
    dolt init
    dolt table import -c -pk=Aaa -s $BATS_TEST_DIRNAME/helper/capital-letter-column-names.schema test $BATS_TEST_DIRNAME/helper/capital-letter-column-names.csv
}

teardown() {
    rm -rf "$BATS_TMPDIR/dolt-repo-$$"
}

@test "put a row in a table" {
    run dolt table put-row test Aaa:3 Bbb:ccc Ccc:CCC
    skip "Brian hates capital letters and this is proof"
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
}