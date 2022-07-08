#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "arg-parsing: dolt supports Nix style argument parsing" {
    dolt checkout -b some-branch
    run dolt branch
    [ $status -eq 0 ]
    [[ "$output" =~ "some-branch" ]] || false
    dolt checkout main
    dolt branch -d some-branch

    dolt checkout -b "some-branch"
    run dolt branch
    [ $status -eq 0 ]
    [[ "$output" =~ "some-branch" ]] || false
    dolt checkout main
    dolt branch -d "some-branch"

    dolt checkout --b "some-branch"
    run dolt branch
    [ $status -eq 0 ]
    [[ "$output" =~ "some-branch" ]] || false
    dolt checkout main
    dolt branch --d "some-branch"

    run dolt checkout -bsome-branch
    [ $status -eq 0 ]
    run dolt branch
    [ $status -eq 0 ]
    [[ "$output" =~ "some-branch" ]] || false
    dolt checkout main
    dolt branch -dsome-branch

    cat <<DELIM > ints.csv
pk,c1
0,0
DELIM
    dolt table import -cpk=pk some-branch ints.csv
}

@test "arg-parsing: dolt supports chaining of modal arguments" {
    dolt sql -q "create table test(pk int, primary key (pk))"
    dolt table import -fc -pk=pk test `batshelper 1pk5col-ints.csv`
}

@test "arg-parsing: dolt checkout with empty string returns error" {
    run dolt checkout ""
    [[ "$output" =~ "error: cannot checkout empty string" ]] || false
    [ $status -ne 0 ]

    run dolt checkout -b ""
    [[ "$output" =~ "error: cannot checkout empty string" ]] || false
    [ $status -ne 0 ]
}

@test "arg-parsing: dolt checkout on the same branch does not throw an error" {
     run dolt checkout main
     [ $status -eq 0 ]
     [[ "$output" =~ "Already on branch 'main'" ]] || false

     run dolt checkout main && dolt checkout main
     [ $status -eq 0 ]
     [[ "$output" =~ "Already on branch 'main'" ]] || false
}
