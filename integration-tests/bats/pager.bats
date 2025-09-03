#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
  setup_common

  for i in {1..20}; do
    dolt commit --allow-empty -m "commit $i abc"
  done
}

teardown() {
  teardown_common
}

export NO_COLOR=1

# bats test_tags=no_lambda
@test "pager: dolt log with pager" {
  skiponwindows "Need to install expect and make this script work on windows."
  # We use `expect` because we need TTY simulation to get the pager to kick in
  export DOLT_PAGER="tail -n 2"
  run expect $BATS_TEST_DIRNAME/pager.expect
  [ "$status" -eq 0 ]
  [[ ! "$output" =~ "commit 2 abc" ]] || false
  [[ "$output" =~ "commit 1 abc" ]] || false
  [[ "$output" =~ "Initialize data repository" ]] || false
  [ "${#lines[@]}" -eq 2 ]

  export DOLT_PAGER="head -n 3"
  run expect $BATS_TEST_DIRNAME/pager.expect
  [ "$status" -eq 0 ]
  [[ "$output" =~ "commit 20 abc" ]] || false
  [[ "$output" =~ "commit 19 abc" ]] || false
  [[ "$output" =~ "commit 18 abc" ]] || false
  [[ ! "$output" =~ "commit 17 abc" ]] || false
  [ "${#lines[@]}" -eq 3 ]
}

# bats test_tags=no_lambda
@test "pager: gracefully exit when pager doesn't exist" {
  skiponwindows "Need to install expect and make this script work on windows."
  export DOLT_PAGER="foobar"

  run expect $BATS_TEST_DIRNAME/pager.expect
  [ "$status" -eq 0 ]
  [[ "$output" =~ "warning: specified pager 'foobar' not found, falling back to less or more" ]] || false
}
