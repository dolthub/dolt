#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}

@test "dolt status and ls to view docs on dolt init" {
    run ls
    [ "${lines[0]}" = "LICENSE.md" ]
    [ "${lines[1]}" = "README.md" ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch master" ]]
    [[ "$output" =~ "Untracked files" ]]
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*README.md) ]] || false
    run cat LICENSE.md
    [ "$output" = "This is a repository level LICENSE. Either edit it, add it, and commit it, or remove the file." ]
    run cat README.md
    [ "$output" = "This is a repository level README. Either edit it, add it, and commit it, or remove the file." ]
}

@test "dolt add . and dolt commit dolt docs" {
    run dolt status
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*README.md) ]] || false
    echo testing123 > LICENSE.md
    run cat LICENSE.md
    [ "$output" = "testing123" ]
    run dolt add .
    [ "$status" -eq 0 ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch master" ]]
    [[ "$output" =~ "Changes to be committed:" ]]
    [[ "${lines[3]}" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    [[ "${lines[4]}" =~ ([[:space:]]*new doc:[[:space:]]*README.md) ]] || false
    run dolt commit -m "adding license and readme"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "adding license and readme" ]] || false
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch master" ]]
    [[ "$output" =~ "nothing to commit, working tree clean" ]]
}

@test "dolt add . and dolt commit dolt docs with another table" {
    run dolt status
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*README.md) ]] || false
    run dolt add .
    [ "$status" -eq 0 ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch master" ]]
    [[ "$output" =~ "Changes to be committed:" ]]
    [[ "${lines[3]}" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    [[ "${lines[4]}" =~ ([[:space:]]*new doc:[[:space:]]*README.md) ]] || false
    run dolt table create -s `batshelper 1pk5col-ints.schema` test
    [ "$status" -eq 0 ]
    run dolt add test
    [ "$status" -eq 0 ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "${lines[3]}" =~ ([[:space:]]*new table:[[:space:]]*test) ]] || false
    [[ "${lines[4]}" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    [[ "${lines[5]}" =~ ([[:space:]]*new doc:[[:space:]]*README.md) ]] || false
    run dolt commit -m "adding license and readme, and test table"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "adding license and readme, and test table" ]] || false
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch master" ]]
    [[ "$output" =~ "nothing to commit, working tree clean" ]]
}

@test "dolt add LICENSE.md stages license" {
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch master" ]]
    [[ "$output" =~ "Untracked files" ]]
    [[ "${lines[3]}" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    [[ "${lines[4]}" =~ ([[:space:]]*new doc:[[:space:]]*README.md) ]] || false
    run dolt add LICENSE.md
    [ "$status" -eq 0 ] || false
    run dolt status
    [ "$status" -eq 0 ]
    [[ "${lines[0]}" =~ "On branch master" ]] || false
    [[ "${lines[1]}" =~ "Changes to be committed:" ]] || false
    [[ "${lines[3]}" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    [ "${lines[4]}" = "Untracked files:" ]
    [[ "${lines[6]}" =~ ([[:space:]]*new doc:[[:space:]]*README.md) ]] || false
    run dolt commit -m "license commit"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "license commit" ]] || false
}

@test "dolt add README.md stages readme" {
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch master" ]] || false
    [[ "$output" =~ "Untracked files" ]] || false
    [[ "${lines[3]}" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    [[ "${lines[4]}" =~ ([[:space:]]*new doc:[[:space:]]*README.md) ]] || false
    run dolt add README.md
    [ "$status" -eq 0 ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch master" ]] || false
    [[ "${lines[1]}" =~ "Changes to be committed:" ]] || false
    [[ "${lines[3]}" =~ ([[:space:]]*new doc:[[:space:]]*README.md) ]] || false
    [ "${lines[4]}" = "Untracked files:" ]
    [[ "${lines[6]}" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    run dolt commit -m "readme commit"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "readme commit" ]] || false
}
# @test "dolt add doesn't add files that are not LICENSE.md or README.md" {
    
# }

# @test "dolt reset should remove docs from staging area" {
    
# }

# @test "dolt reset --hard should set doc values to head commit doc values" {
    
# }

#  @test "dolt ls should not show dolt_docs table" {

#  }

# @test "dolt table * does not allow operations on dolt_docs" {

# }

# @test "dolt sql does not allow queries or edits to dolt_docs" {

# }

# @test "dolt diff shows diffs between working root and file system docs" {

# }