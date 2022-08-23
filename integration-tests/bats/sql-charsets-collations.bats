#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash
load $BATS_TEST_DIRNAME/helper/query-server-common.bash

setup() {
    setup_common
}

teardown() {
    stop_sql_server
    teardown_common
}

@test "sql-charsets-collations: define charset and collation on a column" {
      dolt sql -q "create table german1 (c char(10) CHARACTER SET latin1 COLLATE latin1_german1_ci)"
      run dolt sql -q "show create table german1";
      [ $status -eq 0 ]
      [[ $output =~ "CHARACTER SET latin1" ]] || false
      [[ $output =~ "COLLATE latin1_german1_ci" ]] || false
}

@test "sql-charsets-collations: define charset and collation on a table" {
      dolt sql -q "create table german1 (c char(10)) CHARACTER SET latin1 COLLATE latin1_german1_ci"
      run dolt sql -q "show create table german1";
      [ $status -eq 0 ]
      skip "Defining charsets and collations on a table not supported"
      [[ $output =~ "CHARACTER SET latin1" ]] || false
      [[ $output =~ "COLLATE latin1_german1_ci" ]] || false
}

@test "sql-charsets-collations: define charset and collation on a database" {
    start_sql_server

    server_query "" 1 dolt "" "CREATE DATABASE test CHARACTER SET latin1 COLLATE latin1_swedish_ci;"
    skip "Defining charsets and collations on a database not supported"
    server_query "test" 1 dolt "" "use test; SELECT @@character_set_database" ";@@SESSION.character_set_database\nlatin1"
    server_query "test" 1 dolt "" "use test; SELECT @@character_set_database" ";@@SESSION.collation_database\nlatin1_swedish_ci"
}

@test "sql-charsets-collations: define and use a colation and charset" {
    dolt sql -q "create table german1 (c char(10) CHARACTER SET latin1 COLLATE latin1_german1_ci)"
    dolt sql -q "insert into german1 values ('Bar'), ('Bär')"
    run dolt sql -q "SELECT * FROM german1 WHERE c = 'Bär'"
    skip "This panics"
    [ $status -eq 0 ]
    [[ $output =~ 'Bar' ]] || false
    [[ $output =~ 'Bär' ]] || false

    dolt sql -q	"create table german2 (c char(10) CHARACTER SET latin1 COLLATE latin1_german2_ci)"
    dolt sql -q	"insert into german1 values ('Bar'), ('Bär')"
    run dolt sql -q "SELECT * FROM german1 WHERE c = 'Bär'"
    [ $status -eq 0 ]
    [[ ! $output =~ 'Bar' ]] || false
    [[ $output =~ 'Bär' ]] || false
}

@test "sql-charsets-collations: Modify a charset on a column" {
    dolt sql -q "create table german1 (c char(10) CHARACTER SET latin1 COLLATE latin1_german1_ci)"
    dolt sql -q	"insert into german1 values ('Bar'), ('Bär')"
    dolt sql -q "alter table german1 modify column c char(10) CHARACTER SET utf8mb4"
    run dolt sql -q "show create table german1"
    [ $status -eq 0 ]
    [[ $output =~ 'utf8mb4' ]] || false
    # Ask Daylon if this is the right collation
    [[ $output =~ 'utf8mb4_0900_ai_ci' ]] || false
    [[ ! $output =~ 'latin1_german1_ci' ]] || false
    [[ ! $output =~ 'latin1' ]] || false

    
}

@test "sql-charsets-collations: Modify a collation on a column" {
    dolt sql -q "create table german1 (c char(10) CHARACTER SET latin1 COLLATE latin1_german1_ci)"
    dolt sql -q	"insert into german1 values ('Bar'), ('Bär')"
    dolt sql -q "alter table german1 modify column c char(10) COLLATE latin1_german2_ci"
    run dolt sql -q "show create table german1"
    [ $status -eq 0 ]
    [[ $output =~ 'latin1_german2_ci' ]] || false
    [[ ! $output =~ 'latin1_german1_ci' ]] || false

    skip "Panics"
    run dolt sql -q "SELECT * FROM german1 WHERE c = 'Bär'"
    [ $status -eq 0 ]
    [[ ! $output =~ 'Bar' ]] || false
    [[ $output =~ 'Bär' ]] || false
}
