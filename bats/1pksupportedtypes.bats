#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    dolt table create -s=`batshelper 1pksupportedtypes.schema` test
}

teardown() {
    teardown_common
}

@test "dolt table put-row with all types then examine table" {
    run dolt table put-row test pk:0 int:1 string:foo boolean:true float:1.11111111111111 uint:346 uuid:123e4567-e89b-12d3-a456-426655440000
    [ "$status" -eq 0 ] 
    [ "$output" = "Successfully put row." ]
    run dolt table select test
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
}

@test "boolean 1,0,true,false inserts and examine table" {
    run dolt table put-row test pk:0 int:1 string:foo boolean:1 float:1.11111111111111 uint:346 uuid:123e4567-e89b-12d3-a456-426655440000
    [ "$status" -eq 0 ]
    [ "$output" = "Successfully put row." ]
    run dolt table select test boolean
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "boolean" ]] || false
    # Can't get the [ "${lines[1]}" = "true" ] to return true. Going regex instead
    [[ "${lines[3]}" =~ "true" ]] || false
    run dolt table put-row test pk:0 int:1 string:foo boolean:0 float:1.11111111111111 uint:346 uuid:123e4567-e89b-12d3-a456-426655440000
    [ "$status" -eq 0 ]
    [ "$output" = "Successfully put row." ]
    run dolt table select test boolean
    [[ "${lines[3]}" =~ "false" ]] || false
    run dolt table put-row test pk:0 int:1 string:foo boolean:true float:1.11111111111111 uint:346 uuid:123e4567-e89b-12d3-a456-426655440000
    [ "$status" -eq 0 ]
    [ "$output" = "Successfully put row." ]
    run dolt table select test boolean
    [[ "${lines[3]}" =~ "true" ]] || false
    run dolt table put-row test pk:0 int:1 string:foo boolean:false float:1.11111111111111 uint:346 uuid:123e4567-e89b-12d3-a456-426655440000
    [ "$status" -eq 0 ]
    [ "$output" = "Successfully put row." ]
    run dolt table select test boolean
    [[ "${lines[3]}" =~ "false" ]] || false
}

@test "attempt to insert some schema violations" {
    run dolt table put-row test pk:0 int:1 string:foo boolean:foo float:1.11111111111111 uint:346 uuid:123e4567-e89b-12d3-a456-426655440000
    [ "$status" -eq 1 ]
    [ "${lines[0]}" = "inserted row does not match schema" ]
    run dolt table put-row test pk:0 int:1 string:foo boolean:true float:1.11111111111111 uint:346 uuid:not_a_uuid
    [ "$status" -eq 1 ]
    [ "${lines[0]}" = "inserted row does not match schema" ]
}

@test "attempt to insert some schema violations 2" {
    skip "need strict checking option for imports and put-row.  currently 1.1 is coerced into the value 1"
    run dolt table put-row test pk:0 int:1.1 string:foo boolean:1 float:1.11111111111111 uint:346 uuid:123e4567-e89b-12d3-a456-426655440000
    [ "$status" -eq 1 ]
    [ "${lines[0]}" = "inserted row does not match schema" ]
}