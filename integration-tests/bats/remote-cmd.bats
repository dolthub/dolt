#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

# This test suite is narrow in focus for the `dolt remote` command
# 
# The remote command is used in many tests for set up, but we want
# to verify that it's sql migration works without pulling in a lot
# of changes to tests that would be cumbersome.

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "remote-cmd: perform add" {
    dolt remote add origin http://customhost/org/db

    run dolt remote
    [ "$status" -eq 0 ]
    [[ "$output" =~ "origin" ]] || false
    [[ ! "$output" =~ "customhost" ]] || false

    run dolt remote -v
    [ "$status" -eq 0 ]
    [[ "$output" =~ "origin http://customhost/org/db" ]] || false
}

@test "remote-cmd: perform re-add" {
    dolt remote add origin http://customhost/org/db

    run dolt remote add origin http://otherhost/org/db
    [ "$status" -eq 1 ]
    [[ "$output" =~ "remote already exists" ]] || false
}

@test "remote-cmd: perform remove" {
    dolt remote add origin http://customhost/org/db
    dolt remote add other http://otherhost/org/db

    dolt remote remove origin

    run dolt remote -v
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "origin" ]] || false
    [[ ! "$output" =~ "customhost" ]] || false
    [[ "$output" =~ "other" ]] || false
    [[ "$output" =~ "otherhost" ]] || false
}

@test "remote-cmd: remove non-existent" {
    run dolt remote remove origin
    [ "$status" -eq 1 ]
    [[ "$output" =~ "unknown remote: 'origin'" ]] || false
}

# TODO - expand aws/gcp/oci testing.
@test "remote-cmd: aws params" {
    if [ "$SQL_ENGINE" = "remote-engine" ]; then
       # Verify that we get the right errors when there is a server running.
        run dolt remote add --aws-region us-west origin aws://customhost/org/db
        [ "$status" -eq 1 ]
        [[ "$output" =~ "Stop server and re-run" ]] || false
    else
        dolt remote add --aws-region us-west origin aws://customhost/org/db
        run dolt remote -v
        [ "$status" -eq 0 ]
        [[ "$output" =~ "origin aws://customhost/org/db {\"aws-region\": \"us-west\"}" ]] || false

        run dolt remote add --aws-region us-west other http://customhost/org/db
        [ "$status" -eq 1 ]
        [[ "$output" =~ "only valid for aws remotes" ]] || false
    fi
}

@test "remote-cmd: remove origin and verify tracking is gone" {
    mkdir remote_repo
    mkdir initter
    cd initter
    dolt init
    dolt remote add origin file://../remote_repo
    dolt push origin main
    cd ../
    rm -rf initter
 
    dolt clone file://remote_repo cloned_repo
    cd cloned_repo
    
    # Verify we are tracking origin
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Your branch is up to date with 'origin/main'" ]] || false
    
    # Remove the remote
    dolt remote remove origin
    
    # Verify that the current branch is not tracking origin (because it doesn't exist)
    run dolt status
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "origin" ]] || false
    [[ ! "$output" =~ "Your branch is up to date with 'origin/main'" ]] || false
}
