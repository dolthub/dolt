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
    [[ "$output" =~ "init - Create an empty Dolt data repository." ]] || false
    [[ "$output" =~ "status - Show the working tree status." ]] || false
    [[ "$output" =~ "add - Add table changes to the list of staged table changes." ]] || false
    [[ "$output" =~ "reset - Remove table changes from the list of staged table changes." ]] || false
    [[ "$output" =~ "commit - Record changes to the repository." ]] || false
    [[ "$output" =~ "sql - Run a SQL query against tables in repository." ]] || false
    [[ "$output" =~ "sql-server - Start a MySQL-compatible server." ]] || false
    [[ "$output" =~ "log - Show commit logs." ]] || false
    [[ "$output" =~ "diff - Diff a table." ]] || false
    [[ "$output" =~ "blame - Show what revision and author last modified each row of a table." ]] || false
    [[ "$output" =~ "merge - Merge a branch." ]] || false
    [[ "$output" =~ "branch - Create, list, edit, delete branches." ]] || false
    [[ "$output" =~ "checkout - Checkout a branch or overwrite a table from HEAD." ]] || false
    [[ "$output" =~ "remote - Manage set of tracked repositories." ]] || false
    [[ "$output" =~ "push - Push to a dolt remote." ]] || false
    [[ "$output" =~ "pull - Fetch from a dolt remote data repository and merge." ]] || false
    [[ "$output" =~ "fetch - Update the database from a remote data repository." ]] || false
    [[ "$output" =~ "clone - Clone from a remote data repository." ]] || false
    [[ "$output" =~ "creds - Commands for managing credentials." ]] || false
    [[ "$output" =~ "login - Login to a dolt remote host." ]] || false
    [[ "$output" =~ "version - Displays the current Dolt cli version." ]] || false
    [[ "$output" =~ "config - Dolt configuration." ]] || false
    [[ "$output" =~ "ls - List tables in the working set." ]] || false
    [[ "$output" =~ "schema - Commands for showing and importing table schemas." ]] || false
    [[ "$output" =~ "table - Commands for copying, renaming, deleting, and exporting tables." ]] || false
    [[ "$output" =~ "conflicts - Commands for viewing and resolving merge conflicts." ]] || false
    [[ "$output" =~ "migrate - Executes a repository migration to update to the latest format." ]] || false
}

@test "testing dolt version output" {
    run dolt version
    [ "$status" -eq 0 ]
    regex='dolt version [0-9]+.[0-9]+.[0-9]+'
    [[ "$output" =~ $regex ]] || false
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
    [[ "$output" =~ "rm -" ]] || false
    [[ "$output" =~ "mv -" ]] || false
    [[ "$output" =~ "cp -" ]] || false
}

@test "dolt table import outside of a dolt repository" {
    run dolt table import
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

@test "dolt schema show outside of a dolt repository" {
    run dolt schema show
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

@test "dolt migrate outside of a dolt repository" {
    run dolt migrate
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "initializing a dolt repository" {
    mkdir dolt-repo-$$-new
    cd dolt-repo-$$-new
    run dolt init
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully initialized dolt data repository." ]] || false
    [ -d .dolt ]
    [ -d .dolt/noms ]
    [ -f .dolt/config.json ]
    [ -f .dolt/repo_state.json ]
    [ ! -f README.md ]
    [ ! -f LICENSE.md ]
    rm -rf $BATS_TMPDIR/dolt-repo-$$-new
}

@test "dolt init should not stomp existing LICENSE.md and README.md" {
    echo "greatest README ever" > README.md
    echo "greatest LICENSE ever" > LICENSE.md
    dolt init
    grep "greatest README ever" README.md
    grep "greatest LICENSE ever" LICENSE.md
}

@test "all versions of help work outside a repository" {
    dolt checkout --help
    dolt checkout -help
    run dolt checkout help
    [ "$status" -ne 0 ]
}
