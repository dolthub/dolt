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

@test "config: COMMIT correctly errors when user.name or user.email is unset." {
    dolt config --global --add user.name "bats tester"
    dolt config --global --add user.email "joshn@doe.com"

    dolt init
    dolt sql -q "
    CREATE TABLE test (
       pk int primary key
    )"

    dolt config --global --unset user.name
    dolt config --global --unset user.email

    run dolt sql -q "SET @@dolt_repo_$$_head = COMMIT('-a', '-m', 'updated stuff')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Aborting commit due to empty committer name. Is your config set" ]] || false

    dolt config --global --add user.name "bats tester"
    run dolt sql -q "SET @@dolt_repo_$$_head = COMMIT('-a', '-m', 'updated stuff')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Aborting commit due to empty committer email. Is your config set" ]] || false
}

@test "config: Set default init branch" {
    dolt config --global --add user.name "bats tester"
    dolt config --global --add user.email "joshn@doe.com"

    dolt config --global --add init.default_branch "main"
    run dolt config --list
    [ "$status" -eq 0 ]
    [[ "$output" =~ "init.default_branch = main" ]]

    dolt init
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "on branch main" ]]
    run dolt branch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "*main" ]]

    # cleanup
    dolt config --global --unset init.default_branch
}