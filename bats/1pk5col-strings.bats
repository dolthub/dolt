#!/usr/bin/env bats

setup() {
    export PATH=$PATH:~/go/bin
    export NOMS_VERSION_NEXT=1
    cd $BATS_TMPDIR
    mkdir dolt-repo
    cd dolt-repo
    dolt init
    dolt table create -s=$BATS_TEST_DIRNAME/helper/1pk5col-strings.schema test
}

teardown() {
    rm -rf $BATS_TMPDIR/dolt-repo
}

@test "export a table with a string with commas to csv" {
    run dolt table put-row test pk:tim c1:is c2:super c3:duper c4:rad c5:"a,b,c,d,e"
    [ "$status" -eq 0 ]
    [ "$output" = "Successfully put row." ]
    run dolt table export test export.csv
    [ "$status" -eq 0 ]
    [ "$output" = "Successfully exported data." ]
    skip "dolt doesn't quote strings with the comma delimiter in them"
    grep -E \"a,b,c,d,e\" export.csv
}
