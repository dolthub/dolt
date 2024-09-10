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
  # run it once to create the directory
  gpg --list-keys

  # check for existence of public.gpg and private.gpg
  run gpg --list-keys
  echo gpg --list-keys: \"$output\"
  if [[ "$output" =~ "573DA8C6366D04E35CDB1A44E09A0B208F666373" ]]; then
    echo "key exists"
  else
    echo "importing $BATS_TEST_DIRNAME/private.pgp"
    run gpg --import "$BATS_TEST_DIRNAME/private.pgp"
    echo gpg --import $BATS_TEST_DIRNAME/private.pgp: \"$output\"
    [ "$status" -eq 0 ]
  fi
}

@test "signed: dolt commit with key specified on command line" {
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

@test "signed: dolt commit with key specified in config" {
  init_gpg
  run dolt config --global --add sqlserver.global.signingkey "573DA8C6366D04E35CDB1A44E09A0B208F666373"
  [ "$status" -eq 0 ]

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

  run dolt commit -S -m "signed commit"
  [ "$status" -eq 0 ]

  run dolt log --show-signature
  [ "$status" -eq 0 ]
  [[ "$output" =~ 'gpg: Good signature from "Test User <test@dolthub.com>"' ]] || false
}

@test "signed: signing by default using the config" {
  init_gpg
  run dolt config --global --add sqlserver.global.signingkey "573DA8C6366D04E35CDB1A44E09A0B208F666373"
  [ "$status" -eq 0 ]

  run dolt config --global --add sqlserver.global.gpgsign true
  [ "$status" -eq 0 ]

  run dolt sql -q "CREATE TABLE t (pk INT primary key);"
  [ "$status" -eq 0 ]

  run dolt add .
  [ "$status" -eq 0 ]

  run dolt commit -m "initial commit"
  echo $output
  [ "$status" -eq 0 ]

  run dolt sql -q "INSERT INTO t VALUES (1);"
  [ "$status" -eq 0 ]

  run dolt add .
  [ "$status" -eq 0 ]

  run dolt commit -m "signed commit without being specified on the command line"
  [ "$status" -eq 0 ]

  run dolt log --show-signature
  [ "$status" -eq 0 ]
  [[ "$output" =~ 'gpg: Good signature from "Test User <test@dolthub.com>"' ]] || false
}

@test "signed: using stored procedure" {
  init_gpg
  run dolt config --global --add sqlserver.global.signingkey "573DA8C6366D04E35CDB1A44E09A0B208F666373"
  [ "$status" -eq 0 ]

  run dolt config --global --add sqlserver.global.gpgsign true
  [ "$status" -eq 0 ]

  run dolt sql -q "CREATE TABLE t (pk INT primary key);"
  [ "$status" -eq 0 ]

  run dolt add .
  [ "$status" -eq 0 ]

  run dolt commit -m "initial commit"
  echo $output
  [ "$status" -eq 0 ]

  run dolt sql -q "INSERT INTO t VALUES (1);"
  [ "$status" -eq 0 ]

  run dolt add .
  [ "$status" -eq 0 ]

  run dolt sql -q "CALL dolt_commit('-m', 'signed commit');"
  [ "$status" -eq 0 ]

  run dolt log --show-signature
  [ "$status" -eq 0 ]
  [[ "$output" =~ 'gpg: Good signature from "Test User <test@dolthub.com>"' ]] || false
}