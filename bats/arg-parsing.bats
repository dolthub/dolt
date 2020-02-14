#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    mkdir $BATS_TMPDIR/config-test$$
    nativevar DOLT_ROOT_PATH $BATS_TMPDIR/config-test$$ /p
    cd $BATS_TMPDIR/dolt-repo-$$
}

teardown() {
    teardown_common
    rm -rf "$BATS_TMPDIR/config-test$$"
}

@test "dolt supports Nix style argument parsing" {
    dolt checkout -b this-should-work
    run dolt branch
    [ $status -eq 0 ]
    [[ "$output" =~ "this-should-work" ]] || false
    dolt checkout master
    dolt branch -d this-should-work

    dolt checkout -b "this-should-work"
    run dolt branch
    [ $status -eq 0 ]
    [[ "$output" =~ "this-should-work" ]] || false
    dolt checkout master
    dolt branch -d "this-should-work"

    dolt checkout --b "this-should-work"
    run dolt branch
    [ $status -eq 0 ]
    [[ "$output" =~ "this-should-work" ]] || false
    dolt checkout master
    dolt branch --d "this-should-work"

    skip "Need spaces after single dash arguments"
    dolt checkout -bthis-should-work
    run dolt branch
    [ $status -eq 0 ]
    [[ "$output" =~ "this-should-work" ]] || false
    dolt checkout master
    dolt branch -dthis-should-work
}

@test "dolt supports chaining of modal arguments" {
    dolt sql -q "create table test(pk int, primary key (pk))"
    skip "Can't chain modal arguments"
    dolt table import -fc test `batshelper 1pk5col-ints.csv`
}
