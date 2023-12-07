#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
  setup_no_dolt_init
}

teardown() {
  stop_sql_server
  assert_feature_version
  teardown_common
}

@test "clone-drop: clone a database and then drop it" {
  mkdir repo
  cd repo
  dolt init
  dolt remote add pushed 'file://../pushed'
  dolt push pushed main:main
  dolt sql -q 'call dolt_clone("file://../pushed", "cloned"); drop database cloned;'
}

@test "clone-drop: sql-server: clone a database and then drop it" {
  mkdir repo
  cd repo
  dolt init
  dolt remote add pushed 'file://../pushed'
  dolt push pushed main:main
  start_sql_server
  dolt sql -q 'call dolt_clone("file://../pushed", "cloned"); drop database cloned;'
}
