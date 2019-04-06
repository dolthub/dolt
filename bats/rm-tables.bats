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

@test "rm a staged but uncommitted table" {
    dolt table create -s=$BATS_TEST_DIRNAME/helper/1pk5col-ints.schema test
    run dolt add test
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt table rm test
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt add test
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt status
    [ "${lines[0]}" = "On branch master" ]
    [ "${lines[1]}" = "nothing to commit, working tree clean" ]
}
