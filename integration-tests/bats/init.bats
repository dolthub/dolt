#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_no_dolt_init
    stash_current_dolt_user
}

teardown() {
    restore_stashed_dolt_user
    assert_feature_version
    teardown_common
}

@test "init: implicit global configuration" {
  set_dolt_user "baz", "baz@bash.com"

  run dolt init
  [ "$status" -eq 0 ]

  run dolt config --local --get user.name
  [ "$status" -eq 1 ]

  run dolt config --local --get user.email
  [ "$status" -eq 1 ]

  assert_valid_repository
}

@test "init: explicit local configuration for name" {
  set_dolt_user "baz", "baz@bash.com"

  run dolt init --name foo
  [ "$status" -eq 0 ]

  run dolt config --local --get user.name
  [ "$status" -eq 0 ]
  [[ "$output" =~ "foo" ]] || false

  run dolt config --local --get user.email
  [ "$status" -eq 1 ]

  assert_valid_repository
}

@test "init: explicit local configuration for email" {
  set_dolt_user "baz", "baz@bash.com"

  run dolt init --email foo@bar.com
  [ "$status" -eq 0 ]

  run dolt config --local --get user.name
  [ "$status" -eq 1 ]

  run dolt config --local --get user.email
  [ "$status" -eq 0 ]
  [[ "$output" =~ "foo@bar.com" ]] || false

  assert_valid_repository
}

@test "init: explicit local configuration for config file" {
  set_dolt_user "baz", "baz@bash.com"

  mkdir .dolt

  run dolt config --add user.name foo
  [ "$status" -eq 0 ]
  run dolt config --add user.email foo@bar.com
  [ "$status" -eq 0 ]

  run dolt config --local --get user.name
  [ "$status" -eq 0 ]
  [[ "$output" =~ "foo" ]] || false

  run dolt config --local --get user.email
  [ "$status" -eq 0 ]
  [[ "$output" =~ "foo@bar.com" ]] || false

  run dolt init
  [ "$status" -eq 0 ]

  assert_valid_repository
}

@test "init: explicit local configuration for name and email" {
  set_dolt_user "baz", "baz@bash.com"

  run dolt init --name foo --email foo@bar.com
  [ "$status" -eq 0 ]

  run dolt config --local --get user.name
  [ "$status" -eq 0 ]
  [[ "$output" =~ "foo" ]] || false

  run dolt config --local --get user.email
  [ "$status" -eq 0 ]
  [[ "$output" =~ "foo@bar.com" ]] || false

  assert_valid_repository
}

@test "init: explicit local configuration for name and email with no global config" {
  unset_dolt_user

  run dolt init --name foo --email foo@bar.com
  [ "$status" -eq 0 ]

  run dolt config --local --get user.name
  [ "$status" -eq 0 ]
  [[ "$output" =~ "foo" ]] || false

  run dolt config --local --get user.email
  [ "$status" -eq 0 ]
  [[ "$output" =~ "foo@bar.com" ]] || false

  assert_valid_repository
}

@test "init: no explicit or implicit configuration for name and email" {
  unset_dolt_user

  run dolt init
  [ "$status" -eq 1 ]
  [[ "$output" =~ "Author identity unknown" ]] || false
}

@test "init: implicit default initial branch" {
  set_dolt_user "baz", "baz@bash.com"

  run dolt init
  [ "$status" -eq 0 ]

  run dolt branch --show-current
  [ "$status" -eq 0 ]
  [[ "$output" =~ "main" ]] || false

  assert_valid_repository
}

@test "init: implicit global initial branch" {
  set_dolt_user "baz", "baz@bash.com"

  run dolt config --global -add init.defaultbranch globalInitialBranch

  run dolt init
  [ "$status" -eq 0 ]

  run dolt branch --show-current
  [ "$status" -eq 0 ]
  [[ "$output" =~ "globalInitialBranch" ]] || false

  assert_valid_repository
}

@test "init: explicit initial branch" {
  set_dolt_user "baz", "baz@bash.com"

  run dolt init -b initialBranch
  [ "$status" -eq 0 ]

  run dolt branch --show-current
  [ "$status" -eq 0 ]
  [[ "$output" =~ "initialBranch" ]] || false

  assert_valid_repository
}

@test "init: running init in existing Dolt directory fails" {
  set_dolt_user "baz", "baz@bash.com"

  run dolt init
  [ "$status" -eq 0 ]

  run dolt init
  [ "$status" -eq 1 ]
}

@test "init: running init with invalid argument or option fails" {
  set_dolt_user "baz", "baz@bash.com"

  run dolt init invalid
  [ "$status" -eq 1 ]
  [[ "$output" =~ "does not take positional arguments, but found 1" ]] || false

  run dolt init --invalid
  [ "$status" -eq 1 ]
  [[ "$output" =~ "error: unknown option" ]] || false
}

@test "init: running init with the new format, creates a new format database" {
    set_dolt_user "baz", "baz@bash.com"

    run dolt init --new-format
    [ $status -eq 0 ]

    run dolt init
    [ "$status" -eq 1 ]

    run cut -d ":" -f 2 .dolt/noms/manifest
    [ "$output" = "__DOLT__" ]
}

@test "init: initing a new database displays the correct version" {
    set_dolt_user "baz", "baz@bash.com"

    run dolt init --new-format
    [ $status -eq 0 ]

    run dolt version -v
    [ $status -eq 0 ]
    [[ $output =~ "database storage format: NEW ( __DOLT__ )" ]] || false

    run dolt sql -q "SELECT dolt_storage_format();"
    [[ $output =~ "NEW ( __DOLT__ )" ]] || false
}

@test "init: run init with --new-format, CREATE DATABASE through sql-server running in new-format repo should create a new format database" {
    set_dolt_user "baz", "baz@bash.com"

    run dolt init --new-format
    [ $status -eq 0 ]

    run dolt version --verbose
    [ "$status" -eq 0 ]
    [[ ! $output =~ "OLD ( __LD_1__ )" ]] || false
    [[ $output =~ "NEW ( __DOLT__ )" ]] || false

    dolt sql -q "create database test"
    run ls
    [[ $output =~ "test" ]] || false

    cd test
    run dolt version --verbose
    [ "$status" -eq 0 ]
    [[ ! $output =~ "OLD ( __LD_1__ )" ]] || false
    [[ $output =~ "NEW ( __DOLT__ )" ]] || false
}

@test "init: create a database when current working directory does not have a database yet" {
    set_dolt_user "baz", "baz@bash.com"

    # Default format is NEW (__DOLT__) when DOLT_DEFAULT_BIN_FORMAT is undefined
    if [ "$DOLT_DEFAULT_BIN_FORMAT" = "" ]
    then
        orig_bin_format="__DOLT__"
    else
        orig_bin_format=$DOLT_DEFAULT_BIN_FORMAT
    fi

    mkdir new_format && cd new_format
    run dolt init --new-format
    [ $status -eq 0 ]

    run dolt version --verbose
    [ "$status" -eq 0 ]
    [[ ! $output =~ "OLD ( __LD_1__ )" ]] || false
    [[ $output =~ "NEW ( __DOLT__ )" ]] || false

    cd ..
    run dolt version --verbose
    [ "$status" -eq 0 ]
    ! [[ $output =~ "no valid database in this directory" ]] || false

    dolt sql -q "create database test"
    run ls
    [[ $output =~ "test" ]] || false

    cd test
    run dolt version --verbose
    [ "$status" -eq 0 ]
    [[ "$output" =~ "__DOLT__" ]] || false
}

@test "init: create a db when there is an empty .dolt dir works" {
    set_dolt_user "baz", "baz@bash.com"

    mkdir dbdir
    cd dbdir
    mkdir .dolt

    dolt init
}

@test "init: Fail when there is anything in the .dolt dir" {
    set_dolt_user "baz", "baz@bash.com"

    mkdir -p dbdir/.dolt
    cd dbdir
    touch .dolt/not_config.json
    run dolt init
    [ "$status" -eq 1 ]
    [[ "$output" =~ ".dolt directory already exists" ]] || false
}

@test "init: fun flag produces an initial commit with the right hash" {
    set_dolt_user "baz", "baz@bash.com"	
    dolt init --fun
    run dolt log
    [ $status -eq 0 ]
    [[ $output =~ "commit dolt" ]] || [[ $output =~ "commit do1t" ]] || [[ $output =~ "commit d0lt" ]] || [[ $output =~ "commit d01t" ]] || false
}

@test "init: initialize a non-working directory with --data-dir" {
    baseDir=$(pwd)
    mkdir not_a_repo
    mkdir repo_dir
    cd not_a_repo
    dolt --data-dir $baseDir/repo_dir init

    # Assert that the current directory has NOT been initialized
    run dolt status
    [ $status -eq 1 ]
    [[ $output =~ "The current directory is not a valid dolt repository" ]] || false
    [ ! -d "$baseDir/not_a_repo/.dolt" ]

    # Assert that ../repo_dir HAS been initialized
    cd $baseDir/repo_dir
    run dolt status
    [ $status -eq 0 ]
    [[ $output =~ "On branch main" ]] || false
    [ -d "$baseDir/repo_dir/.dolt" ]
}

assert_valid_repository () {
  run dolt log
  [ "$status" -eq 0 ]
  [[ "$output" =~ "Initialize data repository" ]] || false
}
