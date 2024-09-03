#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

init_gpg() {
  # check if gpg is installed properly
  run which gpg
  [ "$status" -eq 0 ]

  # check for existence of public.gpg and private.gpg
  run gpg --list-keys
  if [[ "$output" =~ "573DA8C6366D04E35CDB1A44E09A0B208F666373" ]]; then
    echo "key exists"
  else
    echo "importing $BATS_TEST_DIRNAME/private.pgp"
    run gpg --import "$BATS_TEST_DIRNAME/private.pgp"
    [ "$status" -eq 0 ]
  fi
}

@test "signing a dolt commit" {
  init_gpg
  run dolt sql -q "CREATE TABLE t (pk INT primary key);"
  [ "$status" -eq 0 ]

  run dolt add .
  [ "$status" -eq 0 ]

  run dolt commit -m "initial commit"
  [ "$status" -eq 0 ]

  run dolt sql -q "INSERT INTO t VALUES (1);"
  [ "$status" -eq 0 ]

  run dolt add .
  [ "$status" -eq 0 ]

  run dolt commit -S "573DA8C6366D04E35CDB1A44E09A0B208F666373" -m "signed commit"
  echo $output
  [ "$status" -eq 0 ]

  run dolt log --show-signature
  echo $output
  [ "$status" -eq 0 ]
  [[ "$output" =~ 'gpg: Good signature from "Test User <test@dolthub.com>"' ]] || false
}