#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

# Tests on an empty dolt repository
@test "empty-repo: dolt init on an already initialized repository" {
    run dolt init
    [ "$status" -ne 0 ]
    [ "$output" = "This directory has already been initialized." ]
}

@test "empty-repo: dolt status on a new repository" {
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false
    [[ ! "$output" =~ "Untracked tables:" ]] || false
    [[ ! "$output" =~ "LICENSE.md" ]] || false
    [[ ! "$output" =~ "README.md" ]] || false
}

@test "empty-repo: dolt ls in a new repository" {
    run dolt ls
    [ "$status" -eq 0 ]
    [ "$output" = "No tables in working set" ]
}

@test "empty-repo: dolt branch in a new repository" {
    run dolt branch
    [ "$status" -eq 0 ]
    # I can't seem to get this to match "* main" so I made a regex instead
    # [ "$output" = "* main" ]
    [[ "$output" =~ "* main" ]] || false
}

@test "empty-repo: dolt log in a new repository" {
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "commit " ]] || false
    [[ "$output" =~ "Initialize data repository" ]] || false
}

@test "empty-repo: dolt add . in new repository" {
    run dolt add .
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
}

@test "empty-repo: dolt reset in new repository" {
    run dolt reset
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
}

@test "empty-repo: dolt diff in new repository" {
    run dolt diff
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
}

@test "empty-repo: dolt commit with nothing added" {
    run dolt commit -m "commit"
    [ "$status" -eq 1 ]
    [ "$output" = 'no changes added to commit (use "dolt add")' ]
}

@test "empty-repo: dolt commit --allow-empty with nothing added" {
    run dolt commit -m "distinctively-named commit" --allow-empty
    [ "$status" -eq 0 ]
    run dolt log
    [[ "$output" =~ "distinctively-named commit" ]] || false
}

@test "empty-repo: dolt sql in a new repository" {
   run dolt sql -q "select * from test"
   [ "$status" -eq 1 ]
   [[ "$output" =~ "table not found: test" ]] || false
}

@test "empty-repo: invalid sql in a new repository" {
    run dolt sql -q "foo bar"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Error parsing SQL" ]] || false
}

@test "empty-repo: dolt schema show in new repository" {
    run dolt schema show
    [ "$status" -eq 0 ]
    [[ "$output" =~ "No tables in working set" ]] || false
}

@test "empty-repo: dolt table select in new repository" {
    run dolt sql -q "select * from test"
    [ "$status" -ne 0 ]
}

@test "empty-repo: dolt table import in a new repository" {
    run dolt table import
    [ "$status" -ne 0 ]
    [[ "$output" =~ "usage" ]] || false
}

@test "empty-repo: dolt table export in a new repository" {
    run dolt table export
    [ "$status" -ne 0 ]
    [[ "${lines[0]}" =~ "usage" ]] || false
}

@test "empty-repo: dolt table rm in a new repository" {
    run dolt table rm
    [ "$status" -ne 0 ]
    [[ "${lines[0]}" =~ "usage" ]] || false
}

@test "empty-repo: dolt table cp in a new repository" {
    run dolt table cp
    [ "$status" -ne 0 ]
    [[ "${lines[0]}" =~ "usage" ]] || false
}

@test "empty-repo: dolt checkout main on main" {
    run dolt checkout main
    [ "$status" -eq 0 ]
    [ "$output" = "Already on branch 'main'" ]
}

@test "empty-repo: dolt checkout non-existent branch" {
    run dolt checkout foo
    [ "$status" -ne 0 ]
    echo "output: $output"
    [[ "$output" =~ "tablespec 'foo' did not match any table(s) known to dolt" ]] || false
}

@test "empty-repo: create and checkout a branch" {
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

@test "empty-repo: create and checkout a branch with dolt checkout -b" {
    run dolt checkout -b test
    [ "$status" -eq 0 ]
    [ "$output" = "Switched to branch 'test'" ]
    run dolt branch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "* test" ]] || false
}

@test "empty-repo: delete a branch" {
    dolt branch test
    run dolt branch -d test
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt branch
    [[ ! "$output" =~ "test" ]] || false
}

@test "empty-repo: move a branch" {
    dolt branch foo
    run dolt branch -m foo bar
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt branch
    [[ ! "$output" =~ "foo" ]] || false
    [[ "$output" =~ "bar" ]] || false
}

@test "empty-repo: copy a branch" {
    dolt branch foo
    run dolt branch -c foo bar
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt branch
    [[ "$output" =~ "foo" ]] || false
    [[ "$output" =~ "bar" ]] || false
}

@test "empty-repo: branch names must support /" {
    run dolt branch tim/test-this-format-11
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
}

@test "empty-repo: branch names support ." {
    dolt branch "dots.are.supported"

    run dolt branch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "dots.are.supported" ]] || false
}
