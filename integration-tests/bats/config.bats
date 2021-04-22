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

@test "config: make sure no dolt configuration for simulated fresh user" {
    run dolt config --list
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
}

@test "config: try to initialize a repository with no configuration" {
    run dolt init
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Could not determine user.name" ]] || false
}

@test "config: set a global config variable" {
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
    [ "$status" -eq 1 ]
    [[ "$output" =~ "wrong number of arguments" ]] || false
}

@test "config: delete a config variable" {
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

@test "config: set and delete multiple config variables" {
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

@test "config: set a user and email and init a repo" {
    dolt config --global --add user.name "bats tester"
    run dolt init
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Could not determine user.email" ]] || false
    dolt config --global --add user.email "bats-tester@liquidata.co"
    run dolt init
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully initialized dolt data repository." ]] || false
}

@test "config: set a local config variable" {
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

@test "config: override a global config variable with a local config variable" {
    dolt config --global --add user.name "bats tester"
    dolt config --global --add user.email "bats-tester@liquidata.co"
    dolt init
    dolt config --global --add test global
    dolt config --local --add test local
    run dolt config --local --get test
    [ "$status" -eq 0 ]
    [ "$output" = "local" ]
    # will list both global and local values in list output
    run dolt config --list
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test = local" ]] || false
    [[ "$output" =~ "test = global" ]] || false
    # will get the local value explicitly
    run dolt config --get --local test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "local" ]] || false
    [[ ! "$output" =~ "global" ]] || false
    # will get the global value explicitly
    run dolt config --get --global test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "global" ]] || false
    [[ ! "$output" =~ "local" ]] || false
    # will get the local value implicitly
    run dolt config --get test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "local" ]] || false
    [[ ! "$output" =~ "global" ]] || false
}

@test "config: Commit to repo w/ ---author and without config vars sets" {
    dolt config --global --add user.name "bats tester"
    dolt config --global --add user.email "joshn@doe.com"

    dolt init
    dolt sql -q  "
    CREATE TABLE test (
      pk BIGINT NOT NULL COMMENT 'tag:0',
      c1 BIGINT COMMENT 'tag:1',
      c2 BIGINT COMMENT 'tag:2',
      c3 BIGINT COMMENT 'tag:3',
      c4 BIGINT COMMENT 'tag:4',
      c5 BIGINT COMMENT 'tag:5',
      PRIMARY KEY (pk)
    );"

    dolt config --global --unset user.name
    dolt config --global --unset user.email

    run dolt config --list
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 0 ]

    dolt add .
    run dolt commit --author="John Doe <john@doe.com>" -m="Commit1"
    [ "$status" -eq 0 ]

    run dolt log
    [ "$status" -eq 0 ]
    regex='John Doe <john@doe.com>'
    [[ "$output" =~ "$regex" ]] || false
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