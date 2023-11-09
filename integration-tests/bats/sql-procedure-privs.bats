#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

# Suite of tests to validate restrictions on procedure privileges.

# working dir will be test_db, which has a single database: mydb
make_test_db_and_users() {
  rm -rf test_db
  mkdir test_db
  cd test_db

  mkdir mydb
  cd mydb
  dolt init
  cd ..

  ## All tests need a user, or two.
  dolt sql -q "CREATE USER neil@localhost IDENTIFIED BY 'pwd'"
  dolt sql -q "CREATE USER mike@localhost IDENTIFIED BY 'pwd'"
}

# working dir will be dolt_repo$$
delete_test_repo() {
    cd ..
    rm -rf test_db
}

setup() {
    setup_no_dolt_init
    make_test_db_and_users
}

teardown() {
    delete_test_repo
    teardown_common
}

@test "sql-procedure-privs: smoke test" {
    dolt sql -q "GRANT SELECT ON mydb.* TO neil@localhost"
    dolt sql -q "GRANT EXECUTE ON mydb.* TO mike@localhost"

    dolt -u neil -p pwd sql -q "select * from dolt_log()"

    run dolt -u neil -p pwd sql -q "call dolt_branch('br1')"
    [ $status -eq 1 ]
    [[ $output =~ "command denied to user 'neil'@'localhost" ]] || false

    dolt -u mike -p pwd sql -q "select * from dolt_log()"
    dolt -u mike -p pwd sql -q "call dolt_branch('br1')"

    run dolt -u mike -p pwd sql -q "call dolt_gc()"
    [ $status -eq 1 ]
    [[ $output =~ "command denied to user 'mike'@'localhost" ]] || false

    # Admin procedure privs must be granted explicitly
    dolt sql -q "GRANT EXECUTE ON PROCEDURE mydb.dolt_gc TO mike@localhost"
    dolt -u mike -p pwd sql -q "call dolt_gc()"
}

mike_blocked_check() {
    run dolt -u mike -p pwd sql -q "call $1"
    [ $status -eq 1 ]
    [[ $output =~ "command denied to user 'mike'@'localhost" ]] || false
}

@test "sql-procedure-privs: admin procedures all block" {
    # Execute privs on a DB does not grant admin procedure privs
    dolt sql -q "GRANT EXECUTE ON mydb.* TO mike@localhost"

    mike_blocked_check "dolt_backup('sync','foo')"
    mike_blocked_check "dolt_clone('file:///myDatabasesDir/database/.dolt/noms')"
    mike_blocked_check "dolt_fetch('origin')"
    mike_blocked_check "dolt_gc()"
    mike_blocked_check "dolt_pull('origin')"
    mike_blocked_check "dolt_purge_dropped_databases()"
    mike_blocked_check "dolt_remote('add','origin1','Dolthub/museum-collections')"
    mike_blocked_check "dolt_undrop('foo')"

    # Verify non-admin procedures are executable, not an exhaustive list tho.
    dolt -u mike -p pwd sql -q "call dolt_branch('br1')"
    dolt -u mike -p pwd sql -q "call dolt_checkout('br1')"
}

@test "sql-procedure-privs: direct execute privs are let through" {
    # Execute privs on a DB does not grant admin procedure privs
    dolt sql -q "GRANT EXECUTE ON PROCEDURE mydb.dolt_gc TO neil@localhost"
    dolt -u neil -p pwd sql -q "call dolt_gc()"

    run dolt -u neil -p pwd sql -q "call dolt_fetch('origin')"
    [ $status -eq 1 ]
    [[ $output =~ "command denied to user 'neil'@'localhost" ]] || false

    # since no db privs exist, this user should not be able to do non-admin procedures
    run dolt -u neil -p pwd sql -q "call dolt_branch('br1')"
    [ $status -eq 1 ]
    [[ $output =~ "command denied to user 'neil'@'localhost" ]] || false
}

@test "sql-procedure-privs: grant option works as explicit grant" {
  # Give Mike grant, but not execute perms. Verify he can grant to Neil, but can't execute.
  dolt sql -q "GRANT GRANT OPTION ON PROCEDURE mydb.dolt_gc TO mike@localhost"
  # Being able to grant access does not give you access
  mike_blocked_check "dolt_gc()"

  run dolt -u neil -p pwd sql -q "call dolt_gc()"
  [ $status -eq 1 ]
  [[ $output =~ "Access denied for user 'neil'@'localhost' to database 'mydb'" ]] || false

  dolt -u mike -p pwd sql -q "GRANT EXECUTE ON PROCEDURE mydb.dolt_gc TO neil@localhost"

  dolt -u neil -p pwd sql -q "call dolt_gc()"
}

@test "sql-procedure-privs: grant option at DB level does not give admin grant priviliges" {
  # Give Mike grant, but non-execute perms. Verify he can grant to Neil, but can't execute.
  dolt sql -q "GRANT GRANT OPTION ON mydb.* TO mike@localhost"
  # Being able to grant access does not give you access
  mike_blocked_check "dolt_gc()"

  run dolt -u mike -p pwd sql -q "GRANT EXECUTE ON PROCEDURE mydb.dolt_gc TO neil@localhost"
  [ $status -eq 1 ]
  [[ $output =~ "command denied to user 'mike'@'localhost" ]] || false
}

@test "sql-procedure-privs: grant option at DB level works for non-admin procedures" {
  dolt sql -q "GRANT GRANT OPTION ON mydb.* TO mike@localhost"
  mike_blocked_check "dolt_branch('br1')"

  run dolt -u neil -p pwd sql -q "call dolt_branch('br1')"
  [ $status -eq 1 ]
  [[ $output =~ "Access denied for user 'neil'@'localhost' to database 'mydb'" ]] || false

  dolt -u mike -p pwd sql -q "GRANT EXECUTE ON PROCEDURE mydb.dolt_branch TO neil@localhost"
  dolt -u neil -p pwd sql -q "call dolt_branch('br1')"
}

@test "sql-procedure-privs: non-dolt procedure execution" {
  dolt sql <<SQL
DELIMITER //
CREATE PROCEDURE user_proc()
BEGIN
    SELECT 'hello cruel world';
END//
SQL

  dolt sql -q "GRANT GRANT OPTION ON mydb.* TO mike@localhost"
  mike_blocked_check "user_proc()"

  run dolt -u neil -p pwd sql -q "call user_proc()"
  [ $status -eq 1 ]
  [[ $output =~ "Access denied for user 'neil'@'localhost' to database 'mydb'" ]] || false

  dolt -u mike -p pwd sql -q "GRANT EXECUTE ON PROCEDURE mydb.user_proc TO neil@localhost"
  dolt -u neil -p pwd sql -q "call user_proc()"
}

# TODO - Alter Routine Grants can be created and revoked (tested in enginetests), but we can't
# actually alter any routines in a meaningful way, so until we can there is nothing to test.
