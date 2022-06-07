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

assert_valid_repository () {
  run dolt log
  [ "$status" -eq 0 ]
  [[ "$output" =~ "Initialize data repository" ]] || false
}
