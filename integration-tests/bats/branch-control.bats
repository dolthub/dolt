#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "branch-contriol: fresh database. branch control tables exist" {
      run dolt sql -r csv -q "select * from dolt_branch_control"
      [ $status -eq 0 ]
      [ ${lines[0]} = "branch,user,host,permissions" ]
      [ ${lines[1]} = "%,root,localhost,admin" ]
      [ ${lines[2]} = "%,%,%,write" ]

      run dolt sql -q "select * from dolt_branch_namespace_control"
      [ $status -eq 0 ]
      skip "This returns nothing. I think it should return an emopty table ie. names of columns but no data"
}


