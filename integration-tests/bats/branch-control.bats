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

setup_test_user() {
    dolt sql -q "create user test"
    dolt sql -q "grant all on *.* to test"
    dolt sql -q "delete from dolt_branch_control where user='%'"
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

    server_query "dolt_repo_$$" 1 dolt "" "select * from dolt_branch_control" "branch,user,host,permissions\n%,dolt,0.0.0.0,{'admin'}\n%,%,%,{'write'}"

    server_query "dolt_repo_$$" 1 dolt "" "select * from dolt_branch_namespace_control" ""
}

@test "branch-control: modify dolt_branch_control from dolt sql then make sure changes are reflected" {
    setup_test_user
    dolt sql -q "insert into dolt_branch_control values ('test-branch', 'test', '%', 'write')"

    run dolt sql -r csv -q "select * from dolt_branch_control"
    [ $status -eq 0 ]
    [ ${lines[0]} = "branch,user,host,permissions" ]
    [ ${lines[1]} = "%,root,localhost,admin" ]
    [ ${lines[2]} = "test-branch,test,%,write" ]

    # Is it weird that the dolt_branch_control can see the dolt user when
    # logged in as test?
    start_sql_server
    server_query "dolt_repo_$$" 1 test "" "select * from dolt_branch_control" "branch,user,host,permissions\n%,dolt,0.0.0.0,{'admin'}\ntest-branch,test,%,{'write'}"
    
}

@test "branch-control: default user root works as expected" {
    # I can't figure out how to get a dolt sql-server started as root.
    # So, I'm copying the pattern from sql-privs.bats and starting it
    # manually.
    PORT=$( definePORT )
    dolt sql-server --host 0.0.0.0 --port=$PORT &
    SERVER_PID=$! # will get killed by teardown_common
    sleep 5 # not using python wait so this works on windows

    skip "This does not return branch permissions for the root user even though I'm connected as root"
    server_query "dolt_repo_$$" 1 root "" "select * from dolt_branch_control" "branch,user,host,permissions\n%,root,localhost,{'admin'}\n%,%,%,{'write'}"

    skip "this delete query fails even though I have permissions"
    run server_query "dolt_repo_$$" 1 root "" "delete from dolt_branch_control where user='%'" "" 1
    ! [[ $output =~ "cannot delete the row" ]] || false

    skip "the query above does not delete the row so this fails"
    server_query "dolt_repo_$$" 1 root "" "select * from dolt_branch_control" "branch,user,host,permissions\n%,root,localhost,{'admin'}"
}

@test "branch-control: test basic branch write permissions" {
    setup_test_user

    dolt sql -q "insert into dolt_branch_control values ('test-branch', 'test', '%', 'write')"
    dolt branch test-branch
    
    start_sql_server

    run server_query "dolt_repo_$$" 1 test "" "create table t (c1 int)" "" 1
    [[ $output =~ "does not have the correct permissions" ]] || false

    server_query "dolt_repo_$$" 1 test "" "call dolt_checkout('test-branch'); create table t (c1 int)" 

    # I should also have branch permissions on branches I create
    server_query "dolt_repo_$$" 1 test "" "call dolt_checkout('-b', 'test-branch-2'); create table t (c1 int)"

    # Now back to main. Still locked out.
    run server_query "dolt_repo_$$" 1 test "" "create table t (c1 int)" "" 1
    [[ $output =~ "does not have the correct permissions" ]] || false
}

@test "branch-control: test admin permissions" {
    setup_test_user

    dolt sql -q "create user test2"
    dolt sql -q "grant all on *.* to test2"

    dolt sql -q "insert into dolt_branch_control values ('test-branch', 'test','%', 'admin')"
    dolt branch test-branch

    start_sql_server
    
    # Admin has no write permission to branch not an admin on
    run server_query "dolt_repo_$$" 1 test "" "create table t (c1 int)" "" 1
    [[ $output =~ "does not have the correct permissions" ]] || false
    
    # Admin can write
    server_query "dolt_repo_$$" 1 test "" "call dolt_checkout('test-branch'); create table t (c1 int)"

    server_query "dolt_repo_$$" 1 test "" "insert into dolt_branch_control values ('test-branch', 'test2', '%', 'write')"
    server_query "dolt_repo_$$" 1 test "" "select * from dolt_branch_control" "branch,user,host,permissions\n%,dolt,0.0.0.0,{'admin'}\ntest-branch,test,%,{'admin'}\ntest-branch,test2,%,{'write'}"
    server_query "dolt_repo_$$" 1 test2 "" "select * from dolt_branch_control" "branch,user,host,permissions\n%,dolt,0.0.0.0,{'admin'}\ntest-branch,test,%,{'admin'}\ntest-branch,test2,%,{'write'}"

    # test2 now has write permissions on test-branch
    server_query "dolt_repo_$$" 1 test2 "" "call dolt_checkout('test-branch'); insert into t values(0)"
    
    server_query "dolt_repo_$$" 1 test "" "delete from dolt_branch_control where user='test2'"
    server_query "dolt_repo_$$" 1 test "" "select * from dolt_branch_control" "branch,user,host,permissions\n%,dolt,0.0.0.0,{'admin'}\ntest-branch,test,%,{'admin'}"
}
