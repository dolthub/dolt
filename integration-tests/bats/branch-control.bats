#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash
load $BATS_TEST_DIRNAME/helper/query-server-common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
    stop_sql_server
}

@test "branch-control: fresh database. branch control tables exist" {
      run dolt sql -r csv -q "select * from dolt_branch_control"
      [ $status -eq 0 ]
      [ ${lines[0]} = "branch,user,host,permissions" ]
      [ ${lines[1]} = "%,root,localhost,admin" ]
      [ ${lines[2]} = "%,%,%,write" ]

      run dolt sql -q "select * from dolt_branch_namespace_control"
      [ $status -eq 0 ]
      skip "This returns nothing. I think it should return an emopty table ie. names of columns but no data"
}

@test "branch-control: fresh database. branch control tables exist through server interface" {
    start_sql_server

    # Should I be able to see all users if I only have write perms?
    server_query "dolt_repo_$$" 1 dolt "" "select * from dolt_branch_control" "branch,user,host,permissions\n%,dolt,0.0.0.0,{'admin'}\n%,%,%,{'write'}"

    server_query "dolt_repo_$$" 1 dolt "" "select * from dolt_branch_namespace_control" ""
}

@test "branch-control: modify dolt_branch_control from dolt sql then make sure changes are reflected" {
    dolt sql -q "create user test"
    dolt sql -q "grant all on *.* to test"
    dolt sql -q "delete from dolt_branch_control where user='%'"
    dolt sql -q "insert into dolt_branch_control values ('test', 'test', '%', 'write')"

    run dolt sql -r csv -q "select * from dolt_branch_control"
    [ $status -eq 0 ]
    [ ${lines[0]} = "branch,user,host,permissions" ]
    [ ${lines[1]} = "%,root,localhost,admin" ]
    [ ${lines[2]} = "test,test,%,write" ]

    start_sql_server
    server_query "dolt_repo_$$" 1 test "" "select * from dolt_branch_control" "branch,user,host,permissions\n%,dolt,0.0.0.0,{'admin'}\ntest,test,%,{'write'}"

    
}
