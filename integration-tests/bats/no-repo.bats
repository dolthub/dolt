#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    mkdir no-dolt-dir-$$
    cd no-dolt-dir-$$
}

teardown() {
    cd ../
    teardown_common
    rm -rf $BATS_TMPDIR/no-dolt-dir-$$
}

@test "no-repo: checking we have a dolt executable available" {
    command -v dolt
}

@test "no-repo: invoking dolt with no arguments" {
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
    [[ "$output" =~ "tag - Create, list, delete tags" ]] || false
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
    [[ "$output" =~ "migrate - Executes a database migration to use the latest Dolt data format." ]] || false
    [[ "$output" =~ "gc - Cleans up unreferenced data from the repository." ]] || false
    [[ "$output" =~ "filter-branch - Edits the commit history using the provided query." ]] || false
    [[ "$output" =~ "merge-base - Find the common ancestor of two commits." ]] || false
}

@test "no-repo: dolt --help exits 0" {
    run dolt --help
    [ "$status" -eq 0 ]
    [ "${lines[0]}" = "Valid commands for dolt are" ]

    # Check help output for supported commands (spotcheck)
    [[ "$output" =~ "init - Create an empty Dolt data repository." ]] || false
    [[ "$output" =~ "status - Show the working tree status." ]] || false
}

@test "no-repo: check all commands for valid help text" {
    # pipe all commands to a file
    # cut -s suppresses the line if it doesn't contain the delim
    dolt | cut -f 1 -d " - " -s | sed "s/ //g" > all.txt

    # filter out commands without "-h"
    cat all.txt \
        | sed "s/creds//g"     \
        | sed "s/version//g"   \
        | sed "s/schema//g"    \
        | sed "s/table//g"     \
        | sed "s/conflicts//g" \
        > commands.txt

    cat commands.txt | while IFS= read -r cmd;
    do
        if [ -z "$cmd" ]; then
            continue
        fi

        run dolt "$cmd" -h
        [ "$status" -eq 0 ]
        [[ "$output" =~ "NAME" ]] || false
        [[ "$output" =~ "DESCRIPTION" ]] || false
    done
}

@test "no-repo: testing dolt version output" {
    run dolt version
    [ "$status" -eq 0 ]
    regex='dolt version [0-9]+.[0-9]+.[0-9]+'
    [[ "$output" =~ $regex ]] || false
}


# Tests for dolt commands outside of a dolt repository
NOT_VALID_REPO_ERROR="The current directory is not a valid dolt repository."
@test "no-repo: dolt status outside of a dolt repository" {
    run dolt status
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "no-repo: dolt add outside of a dolt repository" {
    run dolt add
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "no-repo: dolt reset outside of a dolt repository" {
    run dolt reset
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "no-repo: dolt commit outside of a dolt repository" {
    run dolt commit
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "no-repo: dolt log outside of a dolt repository" {
    run dolt log
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "no-repo: dolt diff outside of a dolt repository" {
    run dolt diff
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "no-repo: dolt merge outside of a dolt repository" {
    run dolt merge
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "no-repo: dolt branch outside of a dolt repository" {
    run dolt branch
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "no-repo: dolt sql outside of a dolt repository" {
    run dolt sql -q "show databases"
    [ "$status" -eq 0 ]
}

@test "no-repo: dolt sql statements with no databases" {
    run dolt sql -q "show tables"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "no database selected" ]] || false

    run dolt sql -q "create table a (a int primary key)"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "no database selected" ]] || false

    run dolt sql -q "show triggers"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "no database selected" ]] || false
}

@test "no-repo: dolt checkout outside of a dolt repository" {
    run dolt checkout
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "no-repo: dolt remote outside of a dolt repository" {
    run dolt remote
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "no-repo: dolt push outside of a dolt repository" {
    run dolt push
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "no-repo: dolt pull outside of a dolt repository" {
    run dolt pull
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "no-repo: dolt fetch outside of a dolt repository" {
    run dolt fetch
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "no-repo: dolt ls outside of a dolt repository" {
    run dolt ls
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "no-repo: dolt table outside of a dolt repository" {
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

@test "no-repo: dolt table import outside of a dolt repository" {
    run dolt table import
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "no-repo: dolt table rm outside of a dolt repository" {
    run dolt table rm
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "no-repo: dolt table mv outside of a dolt repository" {
    run dolt table mv
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "no-repo: dolt table cp outside of a dolt repository" {
    run dolt table cp
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "no-repo: dolt schema show outside of a dolt repository" {
    run dolt schema show
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "no-repo: dolt conflicts outside of a dolt repository" {
    run dolt conflicts
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "Valid commands for dolt conflicts are" ]
    # Check help output for supported commands
    [[ "$output" =~ "cat -" ]] || false
    [[ "$output" =~ "resolve -" ]] || false
}

@test "no-repo: dolt migrate outside of a dolt repository" {
    run dolt migrate
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "no-repo: initializing a dolt repository" {
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
    cd ../
    rm -rf $BATS_TMPDIR/dolt-repo-$$-new
}

@test "no-repo: dolt init should not stomp existing LICENSE.md and README.md" {
    echo "greatest README ever" > README.md
    echo "greatest LICENSE ever" > LICENSE.md
    dolt init
    grep "greatest README ever" README.md
    grep "greatest LICENSE ever" LICENSE.md
}

@test "no-repo: all versions of help work outside a repository" {
    dolt checkout --help
    run dolt checkout -help
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "{{.LessThan}}" ]] || false
    run dolt checkout help
    [ "$status" -ne 0 ]
}

@test "no-repo: don't panic if invalid HOME" {
    DOLT_ROOT_PATH=
    HOME=/this/is/garbage
    run dolt
    [ "$status" -eq 1 ]
    [[ ! "$output" =~ "panic" ]]
    [[ "$output" =~ "Failed to load the HOME directory" ]]
}

@test "no-repo: init with new storage version" {
    DOLT_FORMAT_FEATURE_FLAG=true dolt init
    run cat .dolt/noms/manifest
    [[ "$output" =~ "__DOLT_1__" ]]
    [[ ! "$output" =~ "__LD_1__" ]]
}

@test "no-repo: dolt login exits when receiving SIGINT" {
    dolt login & # run this in the background
    PID=$! # capture background PID
    sleep 1 # Wait a sec
    kill -SIGINT $PID # Send SIGINT (CTRL + C) to PID
    sleep 1 # Wait another sec
    run grep -q 'dolt' <(ps) # Ensure no process named dolt is running
    [ "$output" == "" ]
    run kill -9 $PID # Kill process if it doesn't pass
}
