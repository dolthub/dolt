#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_no_dolt_init
    mkdir $BATS_TMPDIR/config-test$$
    nativevar DOLT_ROOT_PATH $BATS_TMPDIR/config-test$$ /p
    cd $BATS_TMPDIR/dolt-repo-$$
}

teardown() {
    teardown_common
    rm -rf "$BATS_TMPDIR/config-test$$"
}

@test "make sure no dolt configuration for simulated fresh user" {
    run dolt config --list
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
}

@test "try to initialize a repository with no configuration" {
    run dolt init
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Could not determine user.name" ]] || false
}

@test "set a global config variable" {
    run dolt config --global --add test test
    [ "$status" -eq 0 ]
    # Need to make this a regex because of the coloring
    [[ "$output" =~ "Config successfully updated" ]] || false
    [ -f `nativepath ~/.dolt/config_global.json` ]
    run dolt config --list
    [ "$status" -eq 0 ]
    [ "$output" = "test = test" ]
    run dolt config --get test
    [ "$status" -eq 0 ]
    [ "$output" = "test" ]
    run dolt config --global --add test
    [ "$status" -eq 1 ]
    [[ "$output" =~ "wrong number of arguments" ]] || false
    run dolt config --global --add
    skip "dolt config --global --add with no name value pair currently succeeds"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "wrong number of arguments" ]] || false
}

@test "delete a config variable" {
    dolt config --global --add test test
    run dolt config --global --unset test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Config successfully updated" ]] || false
    run dolt config --list
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt config --get test
    [ "$status" -eq 1 ]
    [ "$output" = "" ]
}

@test "set and delete multiple config variables" {
    dolt config --global --add test1 test1
    dolt config --global --add test2 test2
    dolt config --global --add test3 test3
    run dolt config --list
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
    run dolt config --global --unset test1 test2 test3
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Config successfully updated" ]]
    run dolt config --list
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
}

@test "set a user and email and init a repo" {
    dolt config --global --add user.name "bats tester"
    run dolt init
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Could not determine user.email" ]] || false
    dolt config --global --add user.email "bats-tester@liquidata.co"
    run dolt init
    [ "$status" -eq 0 ]
    [ "$output" = "Successfully initialized dolt data repository." ]
}

@test "set a local config variable" {
    dolt config --global --add user.name "bats tester"
    dolt config --global --add user.email "bats-tester@liquidata.co"
    dolt init
    run dolt config --local --add testlocal testlocal
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Config successfully updated" ]] || false
    [ -f .dolt/config.json ]
    run dolt config --list
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
    [[ "$output" =~ "testlocal = testlocal" ]] || false
    run dolt config --get testlocal
    [ "$status" -eq 0 ]
    [ "$output" = "testlocal" ]
}

@test "override a global config variable with a local config variable" {
    dolt config --global --add user.name "bats tester"
    dolt config --global --add user.email "bats-tester@liquidata.co"
    dolt init
    dolt config --global --add test global
    dolt config --local --add test local
    run dolt config --local --get test
    [ "$status" -eq 0 ]
    [ "$output" = "local" ]
    run dolt config --list
    [ "$status" -eq 0 ]
    skip "list option in config does not respect local overrides"
    [[ "$output" =~ "test = local" ]] || false
    [[ ! "$output" =~ "test = global" ]] || false
}
