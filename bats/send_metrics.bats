#!/usr/bin/env bats

setup() {
    load $BATS_TEST_DIRNAME/helper/common.bash
    export PATH=$PATH:~/go/bin
    export NOMS_VERSION_NEXT=1
    dolt config --global --unset metrics.disabled
    cd $BATS_TMPDIR
    mkdir "dolt-repo-$$"
    cd "dolt-repo-$$"
    dolt init
}

teardown() {
    rm -rf "$BATS_TMPDIR/dolt-repo-$$"
}

# How these tests work --
# The initial dolt command will generate an events file
# and will kick off a separate process that attempts to send the
# events in this file to the grpc events server.
# This attempt will fail, since there is no server running, which will allow
# the events file to persist.
# Next, we will run the "send-metrics" command with the "--output" flag
# which will parse the events file and send it's info to stdout.
# Because we've implemented a locking system, our "send-metrics" call should
# fail with a status code of 2 if try to do work on the events directory while
# a separate process is already working on it

# Create events data then flush data to stdout non-concurrently
@test "create events data then flush data to stdout non-concurrently" {
    run dolt ls
    [ "$status" -eq 0 ]
    run sleep 1
    run dolt send-metrics --output
    [ "$status" -eq 0 ]
}

# Create events data then flush data to stdout concurrently
@test "create events data then flush data to stdout concurrently" {
    run dolt ls
    [ "$status" -eq 0 ]
    run dolt send-metrics --output
    [ "$status" -eq 2 ]
    run dolt send-metrics --output
    [ "$status" -eq 2 ]
}