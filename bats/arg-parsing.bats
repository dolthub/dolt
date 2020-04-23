#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
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

@test "dolt does not panic and returns error when empty string used with checkout" {
    run dolt checkout ""
    skip "Panics when attempting to checkout empty string" [[ "$output" =~ "error: cannot checkout empty string" ]] || false
    [ $status -eq 2 ]

    run dolt checkout -b ""
    [[ "$output" =~ "error: cannot checkout empty string" ]] || false
    [ $status -eq 2 ]
}
