#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}

# Tests on an empty dolt repository
@test "dolt init on an already initialized repository" {
    run dolt init
    [ "$status" -ne 0 ]
    [ "$output" = "This directory has already been initialized." ]
}

@test "dolt status on a new repository" {
    run dolt status
    [ "$status" -eq 0 ]
    [ "${lines[0]}" = "On branch master" ]
    [ "${lines[1]}" = "Untracked files:" ]
    [ "${lines[2]}" = '  (use "dolt add <table|doc>" to include in what will be committed)' ]
    [[ "${lines[3]}" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    [[ "${lines[4]}" =~ ([[:space:]]*new doc:[[:space:]]*README.md) ]] || false
    run ls
    [ "${lines[0]}" = "LICENSE.md" ]
    [ "${lines[1]}" = "README.md" ]
    # L&R must be removed (or added and committed) for `nothing to commit` message to display
    run rm "LICENSE.md"
    run rm "README.md"
    run dolt status
    [ "$status" -eq 0 ]
    [ "${lines[0]}" = "On branch master" ]
    [ "${lines[1]}" = "nothing to commit, working tree clean" ]
}

@test "dolt ls in a new repository" {
    run dolt ls
    [ "$status" -eq 0 ]
    [ "$output" = "No tables in working set" ]
}

@test "dolt branch in a new repository" {
    run dolt branch
    [ "$status" -eq 0 ]
    # I can't seem to get this to match "* master" so I made a regex instead
    # [ "$output" = "* master" ]
    [[ "$output" =~ "* master" ]] || false
}

@test "dolt log in a new repository" {
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "commit " ]] || false
    [[ "$output" =~ "Initialize data repository" ]] || false
}

@test "dolt add . in new repository" {
    run dolt add .
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
}

@test "dolt reset in new repository" {
    run dolt reset
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
}

@test "dolt diff in new repository" {
    run dolt diff
    [ "$status" -eq 0 ]
    [[ "$output" =~ "diff --dolt a/LICENSE.md b/LICENSE.md" ]] || false
    [[ "$output" =~ "diff --dolt a/README.md b/README.md" ]] || false
    [[ "$output" =~ "added doc" ]] || false
}

@test "dolt commit with nothing added" {
    # L&R must be removed (or added and committed) in order to test `no changes added to commit` message
    rm "LICENSE.md"
    rm "README.md"
    run dolt commit -m "commit"
    [ "$status" -eq 1 ]
    [ "$output" = 'no changes added to commit (use "dolt add")' ]
}

@test "dolt commit --allow-empty with nothing added" {
    run dolt commit -m "distinctively-named commit" --allow-empty
    [ "$status" -eq 0 ]
    run dolt log
    [[ "$output" =~ "distinctively-named commit" ]] || false
}

@test "dolt sql in a new repository" {
   run dolt sql -q "select * from test"
   [ "$status" -eq 1 ]
   [[ "$output" = "table not found: test" ]] || false
}

@test "invalid sql in a new repository" {
    run dolt sql -q "foo bar"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Error parsing SQL" ]] || false
}

@test "dolt schema show in new repository" {
    run dolt schema show
    [ "$status" -eq 0 ]
    [[ "$output" =~ "No tables in working set" ]] || false
}

@test "dolt table select in new repository" {
    run dolt sql -q "select * from test"
    [ "$status" -ne 0 ]
}

@test "dolt table import in a new repository" {
    run dolt table import
    [ "$status" -ne 0 ]
    [[ "${lines[0]}" =~ "usage" ]] || false
}

@test "dolt table export in a new repository" {
    run dolt table export
    [ "$status" -ne 0 ]
    [[ "${lines[0]}" =~ "usage" ]] || false
}

@test "dolt table rm in a new repository" {
    run dolt table rm
    [ "$status" -ne 0 ]
    [[ "${lines[0]}" =~ "usage" ]] || false
}

@test "dolt table cp in a new repository" {
    run dolt table cp
    [ "$status" -ne 0 ]
    [[ "${lines[0]}" =~ "usage" ]] || false
}

@test "dolt checkout master on master" {
    run dolt checkout master
    [ "$status" -eq 1 ]
    [ "$output" = "Already on branch 'master'" ]
}

@test "dolt checkout non-existant branch" {
    run dolt checkout foo
    [ "$status" -ne 0 ]
    [ "$output" = "error: could not find foo" ]
}

@test "create and checkout a branch" {
    run dolt branch test
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt checkout test
    [ "$status" -eq 0 ]
    [ "$output" = "Switched to branch 'test'" ]
    run dolt branch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "* test" ]] || false
}

@test "create and checkout a branch with dolt checkout -b" {
    run dolt checkout -b test
    [ "$status" -eq 0 ]
    [ "$output" = "Switched to branch 'test'" ]
    run dolt branch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "* test" ]] || false
}

@test "delete a branch" {
    dolt branch test
    run dolt branch -d test
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt branch
    [[ ! "$output" =~ "test" ]] || false
}

@test "move a branch" {
    dolt branch foo
    run dolt branch -m foo bar
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt branch
    [[ ! "$output" =~ "foo" ]] || false
    [[ "$output" =~ "bar" ]] || false
}

@test "copy a branch" {
    dolt branch foo
    run dolt branch -c foo bar
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt branch
    [[ "$output" =~ "foo" ]] || false
    [[ "$output" =~ "bar" ]] || false
}

@test "branch names must support /" {
    run dolt branch tim/test-this-format-11
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
}

@test "branch names do not support ." {
    run dolt branch "dots.are.not.supported"
    [ "$status" -eq 1 ]
    [ "$output" = "fatal: 'dots.are.not.supported' is an invalid branch name." ]
}
