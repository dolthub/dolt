#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}

@test "conjoin: --all flag and storage file IDs are mutually exclusive" {
    run dolt admin conjoin --all somevalidhash1234567890abcdef12345678
    [ "$status" -eq 1 ]
    [[ "$output" =~ "--all flag and storage file IDs are mutually exclusive" ]] || false
}

@test "conjoin: must specify either --all flag or storage file IDs" {
    run dolt admin conjoin
    [ "$status" -eq 1 ]
    [[ "$output" =~ "must specify either --all flag or storage file IDs" ]] || false
}

@test "conjoin: invalid storage file ID format" {
    run dolt admin conjoin invalidhash
    [ "$status" -eq 1 ]
    [[ "$output" =~ "invalid storage file ID: invalidhash" ]] || false
}

@test "conjoin: --all flag works (not yet implemented)" {
    run dolt admin conjoin --all
    [ "$status" -eq 1 ]
    [[ "$output" =~ "conjoin command not yet implemented" ]] || false
}

@test "conjoin: valid storage file ID works (not yet implemented)" {
    run dolt admin conjoin abcdef1234567890abcdef1234567890
    [ "$status" -eq 1 ]
    [[ "$output" =~ "conjoin command not yet implemented" ]] || false
}