#!/usr/bin/env bats

setup() {
    export PATH=$PATH:~/go/bin
    export NOMS_VERSION_NEXT=1
    cd $BATS_TMPDIR
    mkdir remotes-$$
    remotesrv --dir "$BATS_TMPDIR/remotes-$$" &>/dev/null 3>&- &
    mkdir "dolt-repo-$$"
    cd "dolt-repo-$$"
    dolt init
}

teardown() {
    rm -rf "$BATS_TMPDIR/dolt-repo-$$"
    pkill -9 remotesrv
    rm -rf "$BATS_TMPDIR/remotes-$$"
}

@test "dolt remotes server is running" {
    pgrep remotesrv
}

@test "add a remote using dolt remote" {
    run dolt remote add test-remote localhost:50051/test-org/test-repo --insecure
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt remote -v 
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test-remote" ]] || false
}

@test "push and pull from a remote" {
    dolt remote add test-remote localhost:50051/test-org/test-repo --insecure
    run dolt push test-remote master
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    [ -d "$BATS_TMPDIR/remotes-$$/test-org/test-repo" ]
    run dolt pull test-remote
    [ "$status" -eq 0 ]
    skip "Should say Already up to date not fast forward"
    [[ "$output" = "up to date" ]] || false
}

@test "rename a remote" {
    dolt remote add test-remote localhost:50051/test-org/test-repo --insecure
    run dolt remote rename test-remote renamed-remote
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt remote -v
    [ "$status" -eq 0 ]
    [[ "$output" =~ "renamed-remote" ]] || false
    [[ ! "$output" =~ "test-remote" ]] || false
}

@test "remove a remote" {
    dolt remote add test-remote localhost:50051/test-org/test-repo --insecure
    run dolt remote remove test-remote
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt remote -v
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "test-remote" ]] || false
}