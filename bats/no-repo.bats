#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    mkdir no-dolt-dir-$$
	cd no-dolt-dir-$$
}

teardown() {
    teardown_common
    rm -rf $BATS_TMPDIR/no-dolt-dir-$$
}

@test "checking we have a dolt executable available" {
    command -v dolt
}

@test "invoking dolt with no arguments" {
    run dolt
    [ "$status" -eq 1 ]
    [ "${lines[0]}" = "Valid commands for dolt are" ]
    # Check help output for supported commands                                       
    [[ "$output" =~ "init -" ]] || false
    [[ "$output" =~ "status -" ]] || false
    [[ "$output" =~ "add -" ]] || false
    [[ "$output" =~ "reset -" ]] || false
    [[ "$output" =~ "commit -" ]] || false
    [[ "$output" =~ "sql -" ]] || false
    [[ "$output" =~ "log -" ]] || false
    [[ "$output" =~ "diff -" ]] || false
    [[ "$output" =~ "merge -" ]] || false
    [[ "$output" =~ "branch -" ]] || false
    [[ "$output" =~ "checkout -" ]] || false
    [[ "$output" =~ "remote -" ]] || false
    [[ "$output" =~ "push -" ]] || false
    [[ "$output" =~ "pull -" ]] || false
    [[ "$output" =~ "fetch -" ]] || false
    [[ "$output" =~ "clone -" ]] || false
    [[ "$output" =~ "creds -" ]] || false
    [[ "$output" =~ "login -" ]] || false
    [[ "$output" =~ "version -" ]] || false
    [[ "$output" =~ "config -" ]] || false
    [[ "$output" =~ "ls -" ]] || false
    [[ "$output" =~ "table -" ]] || false
    [[ "$output" =~ "conflicts -" ]] || false
}

@test "testing dolt version output" {
    run dolt version
    [ "$status" -eq 0 ]
    [[ "$output" =~ "dolt version " ]] || false
}


# Tests for dolt commands outside of a dolt repository                               
NOT_VALID_REPO_ERROR="The current directory is not a valid dolt repository."
@test "dolt status outside of a dolt repository" {
    run dolt status
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "dolt add outside of a dolt repository" {
    run dolt add
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "dolt reset outside of a dolt repository" {
    run dolt reset
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "dolt commit outside of a dolt repository" {
    run dolt commit
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "dolt log outside of a dolt repository" {
    run dolt log
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "dolt diff outside of a dolt repository" {
    run dolt diff
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "dolt merge outside of a dolt repository" {
    run dolt merge
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "dolt branch outside of a dolt repository" {
    run dolt branch
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "dolt sql outside of a dolt repository" {
    run dolt sql
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "dolt checkout outside of a dolt repository" {
    run dolt checkout
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "dolt remote outside of a dolt repository" {
    run dolt remote
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "dolt push outside of a dolt repository" {
    run dolt push
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "dolt pull outside of a dolt repository" {
    run dolt pull
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "dolt fetch outside of a dolt repository" {
    run dolt fetch
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "dolt ls outside of a dolt repository" {
    run dolt ls
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "dolt table outside of a dolt repository" {
    run dolt table
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "Valid commands for dolt table are" ]
    # Check help output for supported commands                                       
    [[ "$output" =~ "import -" ]] || false
    [[ "$output" =~ "export -" ]] || false
    [[ "$output" =~ "create -" ]] || false
    [[ "$output" =~ "rm -" ]] || false
    [[ "$output" =~ "mv -" ]] || false
    [[ "$output" =~ "cp -" ]] || false
    [[ "$output" =~ "select -" ]] || false
    [[ "$output" =~ "put-row -" ]] || false
    [[ "$output" =~ "rm-row -" ]] || false
}

@test "dolt table import outside of a dolt repository" {
    run dolt table import
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "dolt table create outside of a dolt repository" {
    run dolt table create
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "dolt table rm outside of a dolt repository" {
    run dolt table rm
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "dolt table mv outside of a dolt repository" {
    run dolt table mv
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "dolt table cp outside of a dolt repository" {
    run dolt table cp
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "dolt table select outside of a dolt repository" {
    run dolt table select
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "dolt schema show outside of a dolt repository" {
    run dolt schema show
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "dolt table put-row outside of a dolt repository" {
    run dolt table put-row
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "dolt table rm-row outside of a dolt repository" {
    run dolt table rm-row
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "dolt conflicts outside of a dolt repository" {
    run dolt conflicts
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "Valid commands for dolt conflicts are" ]
    # Check help output for supported commands                                       
    [[ "$output" =~ "cat -" ]] || false
    [[ "$output" =~ "resolve -" ]] || false
}

@test "initializing a dolt repository" {
    mkdir dolt-repo-$$-new
    cd dolt-repo-$$-new
    run dolt init
    [ "$status" -eq 0 ]
    [ "$output" = "Successfully initialized dolt data repository." ]
    [ -d .dolt ]
    [ -d .dolt/noms ]
    [ -f .dolt/config.json ]
    [ -f .dolt/repo_state.json ]
    [ -f README.md ]
    [ -f LICENSE.md ]
    rm -rf $BATS_TMPDIR/dolt-repo-$$-new
}
