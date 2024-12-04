#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash
load $BATS_TEST_DIRNAME/helper/query-server-common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    stop_sql_server
    teardown_common
}

setup_test_user() {
    dolt sql -q "create user test identified by ''"
    dolt sql -q "grant all on *.* to test"
    dolt sql -q "delete from dolt_branch_control where user='%'"
}

@test "branch-control: fresh database. branch control tables exist" {
      run dolt sql -r csv -q "select * from dolt_branch_control"
      [ $status -eq 0 ]
      [ ${lines[0]} = "database,branch,user,host,permissions" ]
      [ ${lines[1]} = "%,%,%,%,write" ]

      dolt sql -q "select * from dolt_branch_namespace_control"

      run dolt sql -q "describe dolt_branch_control"
      [ $status -eq 0 ]
      [[ $output =~ "database" ]] || false
      [[ $output =~ "branch" ]] || false
      [[ $output =~ "user" ]] || false
      [[ $output =~ "host" ]] || false
      [[ $output =~ "permissions" ]] || false

      run dolt sql -q "describe dolt_branch_namespace_control"
      [ $status -eq 0 ]
      [[ $output =~ "database" ]] || false
      [[ $output =~ "branch" ]] || false
      [[ $output =~ "user" ]] || false
      [[ $output =~ "host" ]] || false
}

@test "branch-control: fresh database. branch control tables exist through server interface" {
    start_sql_server

    run dolt sql --result-format csv -q "select * from dolt_branch_control"
    [ $status -eq 0 ]
    [ ${lines[0]} = "database,branch,user,host,permissions" ]
    [ ${lines[1]} = "%,%,%,%,write" ]

    dolt sql -q "select * from dolt_branch_namespace_control"
}

@test "branch-control: modify dolt_branch_control from dolt sql then make sure changes are reflected" {
    setup_test_user
    dolt sql -q "insert into dolt_branch_control values ('test-db','test-branch', 'test', '%', 'write')"

    run dolt sql -r csv -q "select * from dolt_branch_control"
    [ $status -eq 0 ]
    [ ${lines[0]} = "database,branch,user,host,permissions" ]
    [ ${lines[1]} = "test-db,test-branch,test,%,write" ]

    start_sql_server
    run dolt sql --result-format csv -q "select * from dolt_branch_control"
    [ $status -eq 0 ]
    [ ${lines[0]} = "database,branch,user,host,permissions" ]
    [ ${lines[1]} = "test-db,test-branch,test,%,write" ]
}

@test "branch-control: default user root works as expected" {
    # I can't figure out how to get a dolt sql-server started as root.
    # So, I'm copying the pattern from sql-privs.bats and starting it
    # manually.
    PORT=$( definePORT )
    dolt sql-server --host 0.0.0.0 --port=$PORT &
    SERVER_PID=$! # will get killed by teardown_common
    sleep 5 # not using python wait so this works on windows

    run dolt sql --result-format csv -q "select * from dolt_branch_control"
    [ ${lines[0]} = "database,branch,user,host,permissions" ]
    [ ${lines[1]} = "%,%,%,%,write" ]

    dolt sql -q "delete from dolt_branch_control where user='%'"
     
    run dolt sql -q "select * from dolt_branch_control"
    [ $status -eq 0 ]
    [ "$output" == "" ]
}

@test "branch-control: test basic branch write permissions" {
    setup_test_user

    dolt sql -q "insert into dolt_branch_control values ('dolt-repo-$$', 'test-branch', 'test', '%', 'write')"
    dolt branch test-branch
    
    start_sql_server

    run dolt sql -q "create table t (c1 int)"
    [ $status -ne 0 ]
    [[ $output =~ "does not have the correct permissions" ]] || false

    dolt -u test -p '' sql -q "call dolt_checkout('test-branch'); create table t (c1 int)"

    dolt -u test -p '' sql -q "call dolt_checkout('test-branch'); call dolt_add('t'); call dolt_commit('-m', 'Testing commit');"

    # I should also have branch permissions on branches I create
    dolt sql -q "call dolt_checkout('-b', 'test-branch-2'); create table t (c1 int)"

    # Now back to main. Still locked out.
    run dolt sql -q "create table t (c1 int)"
    [ $status -ne 0 ]
    [[ $output =~ "does not have the correct permissions" ]] || false
}

@test "branch-control: test admin permissions" {
    setup_test_user

    dolt sql -q "create user test2 identified by ''"
    dolt sql -q "grant all on *.* to test2"

    dolt sql -q "insert into dolt_branch_control values ('dolt-repo-$$', 'test-branch', 'test', '%', 'admin')"
    dolt branch test-branch

    start_sql_server
    
    # Admin has no write permission to branch not an admin on
    run dolt -u test -p '' sql -q "create table t (c1 int)"
    [ $status -ne 0 ]
    [[ $output =~ "does not have the correct permissions" ]] || false
    
    # Admin can write
    dolt -u test -p '' sql -q "call dolt_checkout('test-branch'); create table t (c1 int)"

    # Admin can make other users
    dolt -u test -p '' sql -q "insert into dolt_branch_control values ('dolt-repo-$$', 'test-branch', 'test2', '%', 'write')"
    run dolt -u test -p '' sql --result-format csv -q "select * from dolt_branch_control"
    [ $status -eq 0 ]
    [ ${lines[0]} = "database,branch,user,host,permissions" ]
    [ ${lines[1]} = "dolt-repo-$$,test-branch,test,%,admin" ]
    [ ${lines[2]} = "dolt-repo-$$,test-branch,root,localhost,admin" ]
    [ ${lines[3]} = "dolt-repo-$$,test-branch,test2,%,write" ]

    # test2 can see all branch permissions
    run dolt -u test2 -p '' sql --result-format csv -q "select * from dolt_branch_control"
    [ $status -eq 0 ]
    [ ${lines[0]} = "database,branch,user,host,permissions" ]
    [ ${lines[1]} = "dolt-repo-$$,test-branch,test,%,admin" ]
    [ ${lines[2]} = "dolt-repo-$$,test-branch,root,localhost,admin" ]
    [ ${lines[3]} = "dolt-repo-$$,test-branch,test2,%,write" ]

    # test2 now has write permissions on test-branch
    dolt -u test2 -p '' sql -q "call dolt_checkout('test-branch'); insert into t values(0)"

    # Remove test2 permissions
    dolt -u test -p '' sql -q "delete from dolt_branch_control where user='test2'"

    run dolt -u test -p '' sql --result-format csv -q "select * from dolt_branch_control"
    [ $status -eq 0 ]
    [ ${lines[0]} = "database,branch,user,host,permissions" ]
    [ ${lines[1]} = "dolt-repo-$$,test-branch,test,%,admin" ]

    # test2 cannot write to branch
    run dolt -u test2 -p '' sql -q "call dolt_checkout('test-branch'); insert into t values(1)"
    [ $status -ne 0 ]
    [[ $output =~ "does not have the correct permissions" ]] || false
}

@test "branch-control: creating a branch grants admin permissions" {
    setup_test_user

    dolt sql -q "insert into dolt_branch_control values ('dolt-repo-$$', 'main', 'test', '%', 'write')"

    start_sql_server

    dolt -u test -p '' sql -q "call dolt_branch('test-branch')"

    run dolt -u test -p '' sql --result-format csv -q "select * from dolt_branch_control"
    [ $status -eq 0 ]
    [ ${lines[0]} = "database,branch,user,host,permissions" ]
    [ ${lines[1]} = "dolt-repo-$$,main,test,%,write" ]
    [ ${lines[2]} = "dolt-repo-$$,test-branch,test,%,admin" ]
}

@test "branch-control: test branch namespace control" {
    setup_test_user

    dolt sql -q "create user test2 identified by ''"
    dolt sql -q "grant all on *.* to test2"

    dolt sql -q "insert into dolt_branch_control values ('dolt-repo-$$', 'test-
branch', 'test', '%', 'admin')"
    dolt sql -q "insert into dolt_branch_namespace_control values ('dolt-repo-$$', 'test-%', 'test2', '%')"

    start_sql_server

    run dolt -u test -p '' sql --result-format csv -q "select * from dolt_branch_namespace_control"
    [ $status -eq 0 ]
    [ ${lines[0]} = "database,branch,user,host" ]
    [ ${lines[1]} = "dolt-repo-$$,test-%,test2,%" ]

    # test cannot create test-branch
    run dolt -u test -p '' sql -q "call dolt_branch('test-branch')"
    [ $status -ne 0 ]
    [[ $output =~ "cannot create a branch" ]] || false

    # test2 can create test-branch
    dolt -u test2 -p '' sql -q "call dolt_branch('test-branch')"
}

@test "branch-control: test longest match in branch namespace control" {
    setup_test_user

    dolt sql -q "create user test2 identified by ''"
    dolt sql -q "grant all on *.* to test2"

    dolt sql -q "insert into dolt_branch_namespace_control values ('dolt-repo-$$', 'test/%', 'test', '%')"
    dolt sql -q "insert into dolt_branch_namespace_control values ('dolt-repo-$$', 'test2/%', 'test2', '%')"

    start_sql_server

    # test can create a branch in its namespace but not in test2
    dolt -u test -p '' sql -q "call dolt_branch('test/branch1')"
    run dolt -u test -p '' sql -q "call dolt_branch('test2/branch1')"
    [ $status -ne 0 ]
    [[ $output =~ "cannot create a branch" ]] || false

    dolt -u test2 -p '' sql -q "call dolt_branch('test2/branch1')"
    run dolt -u test2 -p '' sql -q "call dolt_branch('test/branch1')"
    [ $status -ne 0 ]
    [[ $output =~ "cannot create a branch" ]] || false   
}

@test "branch-control: test longest match in branch access control" {
  setup_test_user
  dolt sql -q "create user admin identified by ''"
  dolt sql -q "grant all on *.* to admin"
  dolt sql -q "insert into dolt_branch_control values ('%', '%', 'admin', '%', 'admin')"

  dolt sql -q "insert into dolt_branch_control values ('dolt-repo-$$', 'test-branch', 'test', '%', 'read')"
  dolt sql -q "insert into dolt_branch_control values ('dolt-repo-$$', '%', 'test', '%', 'write')"
  dolt branch test-branch

  start_sql_server

  run dolt -u test -p '' sql -q "call dolt_checkout('test-branch'); create table t (c1 int)"
  [ $status -ne 0 ]
  [[ $output =~ "does not have the correct permissions" ]] || false

  dolt -u admin -p '' sql -q "delete from dolt_branch_control where branch = 'test-branch'"

  run dolt -u test -p '' sql -q "call dolt_checkout('test-branch'); create table t (c1 int)"
  [ $status -eq 0 ]
  [[ ! $output =~ "does not have the correct permissions" ]] || false
}

@test "branch-control: repeat deletion does not cause a nil panic" {
  dolt sql <<SQL
DELETE FROM dolt_branch_control;
INSERT INTO dolt_branch_control VALUES ("dolt","s1","ab","%","admin");
INSERT INTO dolt_branch_control VALUES ("dolt","s2","ab","%","admin");
INSERT INTO dolt_branch_control VALUES ("%","%","%","%","write");
DELETE FROM dolt_branch_control;
INSERT INTO dolt_branch_control VALUES ("dolt","s1","ab","%","admin");
INSERT INTO dolt_branch_control VALUES ("dolt","s2","ab","%","admin");
INSERT INTO dolt_branch_control VALUES ("%","%","%","%","write");
DELETE FROM dolt_branch_control;
INSERT INTO dolt_branch_control VALUES ("dolt","s1","ab","%","admin");
INSERT INTO dolt_branch_control VALUES ("dolt","s2","ab","%","admin");
INSERT INTO dolt_branch_control VALUES ("%","%","%","%","write");
DELETE FROM dolt_branch_control;
INSERT INTO dolt_branch_control VALUES ("dolt","s1","ab","%","admin");
INSERT INTO dolt_branch_control VALUES ("dolt","s2","ab","%","admin");
INSERT INTO dolt_branch_control VALUES ("%","%","%","%","write");
SQL
  run dolt sql -q "SELECT * FROM dolt_branch_control ORDER BY 1,2,3" -r=csv
  [ $status -eq 0 ]
  [ ${lines[0]} = "database,branch,user,host,permissions" ]
  [ ${lines[1]} = "%,%,%,%,write" ]
  [ ${lines[2]} = "dolt,s1,ab,%,admin" ]
  [ ${lines[3]} = "dolt,s2,ab,%,admin" ]

  # Related to the above issue, multiple deletions would report matches even when they should have all been deleted
  run dolt sql -q "DELETE FROM dolt_branch_control;"
  [ $status -eq 0 ]
  [[ $output =~ "3 rows affected" ]] || false
  run dolt sql -q "DELETE FROM dolt_branch_control;"
  [ $status -eq 0 ]
  [[ $output =~ "0 rows affected" ]] || false
  run dolt sql -q "DELETE FROM dolt_branch_control;"
  [ $status -eq 0 ]
  [[ $output =~ "0 rows affected" ]] || false
  run dolt sql -q "DELETE FROM dolt_branch_control;"
  [ $status -eq 0 ]
  [[ $output =~ "0 rows affected" ]] || false
}

@test "branch-control: Issue #8622 ttask" {
  # https://github.com/dolthub/dolt/issues/8622
  dolt sql <<SQL
CREATE DATABASE t;
CREATE USER IF NOT EXISTS 'tadmin'@'%' IDENTIFIED BY 't';
CREATE USER IF NOT EXISTS 'ttask'@'%' IDENTIFIED BY 't';
GRANT ALL ON t.* TO 'tadmin'@'%';
GRANT ALL ON t.* TO 'ttask'@'%';
INSERT INTO t.dolt_branch_namespace_control (database, branch, user, host) VALUES ('t', '%', '', '');
INSERT INTO t.dolt_branch_namespace_control (database, branch, user, host) VALUES ('t', 'task%', 'ttask', '%');
INSERT INTO t.dolt_branch_namespace_control (database, branch, user, host) VALUES ('t', '%', 'tadmin', '%');
SQL
  run dolt -u ttask -p t --use-db t sql -q "CALL DOLT_BRANCH('task_feature');"
  [ $status -eq 0 ]
  [[ ! $output =~ "cannot create" ]] || false
}

@test "branch-control: Issue #8622 tadmin" {
  # https://github.com/dolthub/dolt/issues/8622
  dolt sql <<SQL
CREATE DATABASE t;
CREATE USER IF NOT EXISTS 'tadmin'@'%' IDENTIFIED BY 't';
CREATE USER IF NOT EXISTS 'ttask'@'%' IDENTIFIED BY 't';
GRANT ALL ON t.* TO 'tadmin'@'%';
GRANT ALL ON t.* TO 'ttask'@'%';
INSERT INTO t.dolt_branch_namespace_control (database, branch, user, host) VALUES ('t', '%', '', '');
INSERT INTO t.dolt_branch_namespace_control (database, branch, user, host) VALUES ('t', 'task%', 'ttask', '%');
INSERT INTO t.dolt_branch_namespace_control (database, branch, user, host) VALUES ('t', '%', 'tadmin', '%');
SQL
  run dolt -u tadmin -p t --use-db t sql -q "CALL DOLT_BRANCH('task_feature');"
  [ $status -ne 0 ]
  [[ $output =~ "cannot create a branch" ]] || false
}

@test "branch-control: Issue #8623" {
  # https://github.com/dolthub/dolt/issues/8623
  dolt sql <<SQL
CREATE DATABASE t;
CREATE USER 'ttask'@'%' IDENTIFIED BY 't';
GRANT ALL ON t.* TO 'ttask'@'%';
SELECT * FROM t.dolt_branch_control;
INSERT INTO t.dolt_branch_control (database, branch, user, host, permissions) VALUES ('t', 'task%', 'ttask', '%', 'admin');
USE t;
CALL DOLT_BRANCH('task_feature');
SQL
  # First we'll ensure that DOLT_ADD works when connecting to the database and using DOLT_CHECKOUT first
  run dolt -u ttask -p t sql -r=json -q "USE t;CALL DOLT_CHECKOUT('task_feature');SELECT database(), active_branch();"
  [ $status -eq 0 ]
  [[ $output =~ '{"active_branch()":"task_feature","database()":"t"}' ]] || false
  run dolt -u ttask -p t sql -q "USE t;CALL DOLT_CHECKOUT('task_feature');CALL DOLT_ADD('-A');"
  [ $status -eq 0 ]
  [[ ! $output =~ "does not have the correct permissions" ]] || false
  # Next we'll ensure that DOLT_ADD works when connecting to the branch directly, without using DOLT_CHECKOUT
  run dolt -u ttask -p t sql -r=json -q "USE t/task_feature;SELECT database(), active_branch();"
  [ $status -eq 0 ]
  [[ $output =~ '{"active_branch()":"task_feature","database()":"t/task_feature"}' ]] || false
  run dolt -u ttask -p t sql -q "USE t/task_feature;CALL DOLT_ADD('-A');"
  [ $status -eq 0 ]
  [[ ! $output =~ "cannot create" ]] || false
}
