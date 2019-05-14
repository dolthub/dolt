#!/usr/bin/env bats

setup() {
    export PATH=$PATH:~/go/bin
    export NOMS_VERSION_NEXT=1
    cd $BATS_TMPDIR
    mkdir "dolt-repo-$$"
    cd "dolt-repo-$$"
    dolt init
}

teardown() {
    rm -rf "$BATS_TMPDIR/dolt-repo-$$"
}

@test "start a sql shell and exit using exit" {
    run bash -c "echo exit | dolt sql" 
    [ $status -eq 0 ]
    [[ "$output" =~ "# Welcome to the DoltSQL shell." ]] || false
    [[ "$output" =~ "Bye" ]] || false
}

@test "start a sql shell and exit using quit" {
    run bash -c "echo quit | dolt sql"
    [ $status -eq 0 ]
    [[ "$output" =~ "# Welcome to the DoltSQL shell." ]] || false
    [[ "$output" =~ "Bye" ]] || false
}