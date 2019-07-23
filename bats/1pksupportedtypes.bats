#!/usr/bin/env bats

setup() {
    export PATH=$PATH:~/go/bin
    export NOMS_VERSION_NEXT=1
    cd $BATS_TMPDIR
    mkdir "dolt-repo-$$"
    cd "dolt-repo-$$"
    load $BATS_TEST_DIRNAME/helper/common.bash
    dolt init
    dolt table create -s=`batshelper 1pksupportedtypes.schema` test
}

teardown() {
    rm -rf "$BATS_TMPDIR/dolt-repo-$$"
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
    run dolt table put-row test pk:0 int:1.1 string:foo boolean:1 float:1.11111111111111 uint:346 uuid:123e4567-e89b-12d3-a456-426655440000
    [ "$status" -eq 1 ]
    [ "${lines[0]}" = "inserted row does not match schema" ]
    run dolt table put-row test pk:0 int:1.1 string:foo boolean:foo float:1.11111111111111 uint:346 uuid:123e4567-e89b-12d3-a456-426655440000
    [ "$status" -eq 1 ]
    [ "${lines[0]}" = "inserted row does not match schema" ]
run dolt table put-row test pk:0 int:1.1 string:foo boolean:foo float:1.11111111111111 uint:346 uuid:123e4567-e89b-12d3-a456-426655440000
    [ "$status" -eq 1 ]
    [ "${lines[0]}" = "inserted row does not match schema" ]
run dolt table put-row test pk:0 int:1.1 string:foo boolean:1 float:1.11111111111111 uint:-346 uuid:123e4567-e89b-12d3-a456-426655440000
    [ "$status" -eq 1 ]
    [ "${lines[0]}" = "inserted row does not match schema" ]
run dolt table put-row test pk:0 int:1.1 string:foo boolean:1 float:1.11111111111111 uint:346 uuid:123e467-e89b-12d3-a456-426655440000
    [ "$status" -eq 1 ]
    [ "${lines[0]}" = "inserted row does not match schema" ]
}