#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
  setup_no_dolt_init
  dolt init

  # NOTE: Instead of running setup_common, we embed the same commands here so that we can set up our test data
  #       to work with with the remote-engine test variant. To test database directory names that contain a hyphen
  #       (which is converted to an underscore when accessed through a SQL interface), we need to create the
  #       directory on disk *before* the remote-engine sql-server starts.
  mkdir drop-me-2 && cd drop-me-2
  dolt init && cd ..
  setup_remote_server
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "undrop: undrop error messages" {
  # When called without any argument, dolt_undrop() returns an error
  # that includes the database names that can be undropped.
  run dolt sql -q "CALL dolt_undrop();"
  [ $status -eq 1 ]
  [[ $output =~ "no database name specified." ]] || false
  [[ $output =~ "there are no databases currently available to be undropped" ]] || false

  # When called without an invalid database name, dolt_undrop() returns
  # an error that includes the database names that can be undropped.
  run dolt sql -q "CALL dolt_undrop('doesnotexist')"
  [ $status -eq 1 ]
  [[ $output =~ "no database named 'doesnotexist' found to undrop" ]] || false
  [[ $output =~ "there are no databases currently available to be undropped" ]] || false

  # When called with multiple arguments, dolt_undrop() returns an error
  # explaining that only one argument may be specified.
  run dolt sql -q "CALL dolt_undrop('one', 'two', 'three')"
  [ $status -eq 1 ]
  [[ $output =~ "dolt_undrop called with too many arguments" ]] || false
  [[ $output =~ "dolt_undrop only accepts one argument - the name of the dropped database to restore" ]] || false
}

@test "undrop: purge error messages" {
  # Assert that specifying args when calling dolt_purge_dropped_databases() returns an error
  run dolt sql -q "call dolt_purge_dropped_databases('all', 'of', 'the', 'dbs');"
  [ $status -eq 1 ]
  [[ $output =~ "dolt_purge_dropped_databases does not take any arguments" ]] || false
}

@test "undrop: undrop root database" {
  # Create a new Dolt database directory to use as a root database
  # NOTE: We use hyphens here to test how db dirs are renamed.
  mkdir test-db-1 && cd test-db-1
  dolt init

  # Create some data and a commit in the database
  dolt sql << EOF
create table t1 (pk int primary key, c1 varchar(200));
insert into t1 values (1, "one");
call dolt_commit('-Am', 'creating table t1');
EOF
  run dolt sql -q "show databases;"
  [ $status -eq 0 ]
  [[ $output =~ "test_db_1" ]] || false

  # Drop the root database
  dolt sql -q "drop database test_db_1;"
  run dolt sql -q "show databases;"
  [ $status -eq 0 ]
  [[ ! $output =~ "test_db_1" ]] || false

  # Undrop the test_db_1 database
  # NOTE: After being undropped, the database is no longer the root database,
  #       but contained in a subdirectory like a non-root database.
  dolt sql -q "call dolt_undrop('test_db_1');"
  run dolt sql -q "show databases;"
  [ $status -eq 0 ]
  [[ $output =~ "test_db_1" ]] || false

  # Sanity check querying some data
  run dolt sql -r csv -q "select * from test_db_1.t1;"
  [ $status -eq 0 ]
  [[ $output =~ "1,one" ]] || false
}

# Asserts that a non-root database can be dropped and then restored with dolt_undrop(), even when
# the case of the database name given to dolt_undrop() doesn't match match the original case.
@test "undrop: undrop non-root database" {
  dolt sql << EOF
use drop_me_2;
create table t1 (pk int primary key, c1 varchar(200));
insert into t1 values (1, "one");
call dolt_commit('-Am', 'creating table t1');
EOF
  run dolt sql -q "show databases;"
  [ $status -eq 0 ]
  [[ $output =~ "drop_me_2" ]] || false

  dolt sql -q "drop database drop_me_2;"
  run dolt sql -q "show databases;"
  [ $status -eq 0 ]
  [[ ! $output =~ "drop_me_2" ]] || false

  # Call dolt_undrop() with non-matching case for the database name to
  # ensure dolt_undrop() works with case-insensitive database names.
  dolt sql -q "call dolt_undrop('DrOp_mE_2');"
  run dolt sql -q "show databases;"
  [ $status -eq 0 ]
  [[ $output =~ "drop_me_2" ]] || false

  run dolt sql -r csv -q "select * from drop_me_2.t1;"
  [ $status -eq 0 ]
  [[ $output =~ "1,one" ]] || false
}

# When a database is dropped, and then a new database is recreated
# with the same name and dropped, dolt_undrop will undrop the most
# recent database with that name.
@test "undrop: drop database, recreate, and drop again" {
  # Create a database named test123
  dolt sql << EOF
create database test123;
use test123;
create table t1 (pk int primary key, c1 varchar(100));
insert into t1 values (1, "one");
call dolt_commit('-Am', 'adding table t1 to test123 database');
EOF

  # Drop database test123 and make sure it's gone
  dolt sql -q "drop database test123;"
  run dolt sql -q "show databases;"
  [ $status -eq 0 ]
  [[ ! $output =~ "test123" ]] || false

  # Create a new database named test123
  dolt sql << EOF
create database test123;
use test123;
create table t2 (pk int primary key, c2 varchar(100));
insert into t2 values (100, "one hundy");
call dolt_commit('-Am', 'adding table t2 to new test123 database');
EOF

  # Drop the new test123 database and make sure it's gone
  dolt sql -q "drop database test123;"
  run dolt sql -q "show databases;"
  [ $status -eq 0 ]
  [[ ! $output =~ "test123" ]] || false

  # Undrop the database
  dolt sql -q "call dolt_undrop('test123');"
  run dolt sql -r csv -q "select * from test123.t2;"
  [ $status -eq 0 ]
  [[ $output =~ "100,one hundy" ]] || false
}

# Asserts that when there is already an existing database with the same name, a dropped database
# cannot be undropped.
# TODO: In the future, it might be useful to allow dolt_undrop() to rename the dropped database to
#       a new name, but for now, keep it simple and just disallow restoring in this case.
@test "undrop: undrop conflict" {
  dolt sql << EOF
create database dAtAbAsE1;
use dAtAbAsE1;
create table t1 (pk int primary key, c1 varchar(200));
insert into t1 values (1, "one");
call dolt_commit('-Am', 'creating table t1');
EOF
  run dolt sql -q "show databases;"
  [ $status -eq 0 ]
  [[ $output =~ "dAtAbAsE1" ]] || false

  # Drop dAtAbAsE1
  dolt sql -q "drop database dAtAbAsE1;"
  run dolt sql -q "show databases;"
  [ $status -eq 0 ]
  [[ ! $output =~ "dAtAbAsE1" ]] || false

  # Create a new database named dAtAbAsE1
  dolt sql << EOF
create database database1;
use database1;
create table t2 (pk int primary key, c1 varchar(200));
insert into t2 values (1000, "thousand");
call dolt_commit('-Am', 'creating table t2');
EOF
  run dolt sql -q "show databases;"
  [ $status -eq 0 ]
  [[ $output =~ "database1" ]] || false

  # Trying to undrop dAtAbAsE1 results in an error, since a database already exists
  run dolt sql -q "call dolt_undrop('dAtAbAsE1');"
  [ $status -eq 1 ]
  [[ $output =~ "unable to undrop database 'dAtAbAsE1'" ]] || false
  [[ $output =~ "another database already exists with the same case-insensitive name" ]] || false
}

@test "undrop: purging dropped databases" {
  # Create a database to keep and a database to purge
  dolt sql << EOF
create database keepme;
create database purgeme;
use purgeme;
create table t3 (pk int primary key, c1 varchar(200));
insert into t3 values (3, "three");
call dolt_commit('-Am', 'creating table t3');
EOF
  run dolt sql -q "show databases;"
  [ $status -eq 0 ]
  [[ $output =~ "purgeme" ]] || false
  [[ $output =~ "keepme" ]] || false

  # Assert that we can call dolt_purge_dropped_databases when there aren't any dropped dbs yet
  dolt sql -q "call dolt_purge_dropped_databases;"

  # Drop the purgeme database so we can purge it
  dolt sql -q "drop database purgeme;"
  run dolt sql -q "show databases;"
  [ $status -eq 0 ]
  [[ ! $output =~ "purgeme" ]] || false
  [[ $output =~ "keepme" ]] || false

  # Purge the purgeme database and make sure we can't undrop it
  dolt sql -q "call dolt_purge_dropped_databases;"
  run dolt sql -q "call dolt_undrop('purgeme');"
  [ $status -eq 1 ]
  [[ $output =~ "no database named 'purgeme' found to undrop" ]] || false
  [[ $output =~ "there are no databases currently available to be undropped" ]] || false

  # Double check that the keepme database is still present
  run dolt sql -q "show databases;"
  [ $status -eq 0 ]
  [[ ! $output =~ "purgeme" ]] || false
  [[ $output =~ "keepme" ]] || false
}
