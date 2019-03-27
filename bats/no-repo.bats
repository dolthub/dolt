#!/usr/bin/env bats                                                                  
setup() {
    export PATH=$PATH:~/go/bin
    export NOMS_VERSION_NEXT=1

}

@test "checking we have a dolt executable available" {
    command -v dolt
}

@test "invoking dolt with no arguments" {
    run dolt
    [ "$status" -eq 1 ]
    [ "${lines[0]}" = "Valid commands for dolt are" ]
    # Check help output for supported commands                                       
    [[ "$output" =~ "init -" ]]
    [[ "$output" =~ "status -" ]]
    [[ "$output" =~ "add -" ]]
    [[ "$output" =~ "reset -" ]]
    [[ "$output" =~ "commit -" ]]
    [[ "$output" =~ "sql -" ]]
    [[ "$output" =~ "log -" ]]
    [[ "$output" =~ "diff -" ]]
    [[ "$output" =~ "merge -" ]]
    [[ "$output" =~ "branch -" ]]
    [[ "$output" =~ "checkout -" ]]
    [[ "$output" =~ "remote -" ]]
    [[ "$output" =~ "push -" ]]
    [[ "$output" =~ "pull -" ]]
    [[ "$output" =~ "fetch -" ]]
    [[ "$output" =~ "clone -" ]]
    [[ "$output" =~ "creds -" ]]
    [[ "$output" =~ "login -" ]]
    [[ "$output" =~ "version -" ]]
    [[ "$output" =~ "config -" ]]
    [[ "$output" =~ "ls -" ]]
    [[ "$output" =~ "table -" ]]
    [[ "$output" =~ "conflicts -" ]]
}

@test "testing dolt version output" {
    run dolt version
    [ "$status" -eq 0 ]
    [[ "$output" =~ "dolt version " ]]
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
    [[ "$output" =~ "import -" ]]
    [[ "$output" =~ "export -" ]]
    [[ "$output" =~ "create -" ]]
    [[ "$output" =~ "rm -" ]]
    [[ "$output" =~ "mv -" ]]
    [[ "$output" =~ "cp -" ]]
    [[ "$output" =~ "select -" ]]
    [[ "$output" =~ "schema -" ]]
    [[ "$output" =~ "put-row -" ]]
    [[ "$output" =~ "rm-row -" ]]
}

@test "dolt conflicts outside of a dolt repository" {
    run dolt conflicts
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "Valid commands for dolt conflicts are" ]
    # Check help output for supported commands                                       
    [[ "$output" =~ "cat -" ]]
    [[ "$output" =~ "resolve -" ]]
}

@test "initializing a dolt repository" {
    cd $BATS_TMPDIR
    mkdir dolt-repo
    cd dolt-repo
    run dolt init
    [ "$status" -eq 0 ]
    [ "$output" = "Successfully initialized dolt data repository." ]
    [ -d .dolt ]
    [ -d .dolt/noms ]
    [ -f .dolt/config.json ]
    [ -f .dolt/repo_state.json ]
    rm -rf $BATS_TMPDIR/dolt-repo
}