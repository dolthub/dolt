#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash
load $BATS_TEST_DIRNAME/helper/data-generation.bash

setup() {
    setup_common
    
    # Create initial table for testing using efficient data generation
    create_test_table
    dolt sql -q "$(insert_statement)"
}

teardown() {
    teardown_common
}

# Helper function to count storage files in oldgen (32 char hash names)
count_oldgen_files() {
    find .dolt/noms/oldgen -type f -name "????????????????????????????????" 2>/dev/null | grep -v LOCK | grep -v manifest | wc -l | tr -d ' '
}

# Helper function to get storage file IDs from oldgen
get_oldgen_storage_ids() {
    find .dolt/noms/oldgen -type f -name "????????????????????????????????" 2>/dev/null | grep -v LOCK | grep -v manifest | xargs -I {} basename {}
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

@test "conjoin: get storage file IDs and test conjoin with specific IDs" {
    dolt sql -q "$(mutations_and_gc_statement)"
    dolt sql -q "$(mutations_and_gc_statement)"
    dolt sql -q "$(mutations_and_gc_statement)"
    
    # Get storage file IDs from oldgen
    storage_ids=($(get_oldgen_storage_ids))

    [ "${#storage_ids[@]}" -eq 3 ]
    
    # Select first 2 IDs
    id1="${storage_ids[0]}"
    id2="${storage_ids[1]}"
    
    # Verify we got valid IDs for debugging
    [ -n "$id1" ] || (echo "ERROR: id1 is empty" && false)
    [ -n "$id2" ] || (echo "ERROR: id2 is empty" && false)
    
    # Verify the IDs look correct (32 characters, valid hash format)
    [ "${#id1}" -eq 32 ]
    [ "${#id2}" -eq 32 ]
    
    # Test conjoin with the specific IDs (expected to fail with "not yet implemented")
    run dolt admin conjoin "$id1" "$id2"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "conjoin command not yet implemented" ]] || false
}

