#!/usr/bin/env bats

setup() {
    export PATH=$PATH:~/go/bin
    export NOMS_VERSION_NEXT=1
    cd $BATS_TMPDIR
    mkdir dolt-repo
    cd dolt-repo
}

teardown() {
    rm -rf $BATS_TMPDIR/dolt-repo
}

@test "checking we have a dolt executable available" {
    command -v dolt
}

@test "invoking dolt with no arguments" {
    run dolt
    [ "$status" -eq 1 ]
    [ "${lines[0]}" = "Valid commands for dolt are" ]
}

@test "initializing a dolt repository" {
    run dolt init
    [ "$status" -eq 0 ]
    [ "$output" = "Successfully initialized dolt data repository." ]
    [ -d .dolt ]
    [ -d .dolt/noms ]
    [ -f .dolt/config.json ] 
    [ -f .dolt/repo_state.json ]
}

@test "dolt status on a new repository" {
    dolt init
    run dolt status
    [ "$status" -eq 0 ]
    [ "${lines[0]}" = "On branch master" ]
    [ "${lines[1]}" = "nothing to commit, working tree clean" ]
}

@test "dolt ls in a new repository" {
    dolt init
    run dolt ls
    [ "$status" -eq 0 ]
    [ "$output" = "Tables in working set:" ]
}

@test "dolt branch in a new repository" {
    dolt init 
    run dolt branch
    [ "$status" -eq 0 ]
    # I can't seem to get this to match "* master" so I made a regex instead
    # [ "$output" = "* master" ] 
    [[ "$output" =~ "* master" ]]
}