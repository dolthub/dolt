#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash
load $BATS_TEST_DIRNAME/helper/query-server-common.bash

make_repo() {
  mkdir "$1"
  cd "$1"
  dolt init
  cd ..
}

create_test_table() {
    dolt sql-client --host=0.0.0.0 --port=$PORT --user=dolt <<SQL
USE repo1;
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
}

show_tables() {
    dolt sql-client --host=0.0.0.0 --port=$PORT --user=dolt <<SQL
USE repo1;
SHOW TABLES;
SQL
}

setup() {
    setup_no_dolt_init
    make_repo repo1
}

teardown() {
    stop_sql_server
    teardown_common
}

@test "sql-client: test sql-client shows tables" {
    skiponwindows "Missing dependencies"
    cd repo1
    start_sql_server repo1
    cd ../

    # No tables at the start
    show_tables
    run show_tables
    [ "$status" -eq 0 ]
    [ "${lines[0]}" = '# Welcome to the Dolt MySQL client.' ]
    [ "${lines[1]}" = "# Statements must be terminated with ';'." ]
    [ "${lines[2]}" = '# "exit" or "quit" (or Ctrl-D) to exit.' ]

    create_test_table
    run show_tables
    [ "$status" -eq 0 ]
    [ "${lines[0]}" = '# Welcome to the Dolt MySQL client.' ]
    [ "${lines[1]}" = "# Statements must be terminated with ';'." ]
    [ "${lines[2]}" = '# "exit" or "quit" (or Ctrl-D) to exit.' ]
    [ "${lines[3]}" = '+-----------------+' ]
    [ "${lines[4]}" = '| Tables_in_repo1 |' ]
    [ "${lines[5]}" = '+-----------------+' ]
    [ "${lines[6]}" = '| test            |' ]
    [ "${lines[7]}" = '+-----------------+' ]
}

@test "sql-client: --user argument is required" {
    run dolt sql-client
    [ "$status" -eq 1 ]
    [[ "$output" =~  "--user or -u argument is required" ]] || false
}

@test "sql-client: multiple statments in --query" {
    cd repo1
    start_sql_server repo1

    dolt sql-client -u dolt -P $PORT --use-db repo1 -q "
    	 create table t(c int);
	 insert into t values (0),(1);
	 update t set c=2 where c=0;"
    run dolt sql-client -u dolt -P $PORT --use-db repo1 -q "select c from t"
    [ $status -eq 0 ]
    [[ $output =~ " 1 " ]] || false
    [[ $output =~ " 2 " ]] || false
    ! [[ $output =~ " 0 " ]] || false
}

@test "sql-client: no-auto-commit" {
    cd repo1
    start_sql_server repo1

    dolt sql-client -u dolt -P $PORT --use-db repo1 --no-auto-commit -q "CREATE TABLE one_pk (
        pk BIGINT NOT NULL,
        c1 BIGINT,
        c2 BIGINT,
        PRIMARY KEY (pk)
    )"
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "No tables in working set" ]] || false

    # Now issue a manual commit
    dolt sql-client -u dolt -P $PORT --use-db repo1 --no-auto-commit -q "CREATE TABLE one_pk (
        pk BIGINT NOT NULL,
        c1 BIGINT,
        c2 BIGINT,
        PRIMARY KEY (pk));
	COMMIT;"
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "one_pk" ]] || false
}

@test "sql-client: connect directly to a branch using --use-db" {
    cd repo1
    dolt branch branch1
    start_sql_server repo1

    dolt sql-client -u dolt -P $PORT --use-db repo1/branch1 -q "
         create table t(c int);
         insert into t values (0),(1);
         update t set c=2 where c=0;"
    run	dolt sql-client -u dolt -P $PORT --use-db repo1/branch1 -q "select c from t"
    [ $status -eq 0 ]
    [[ $output =~ " 1 " ]] || false
    [[ $output =~ " 2 " ]] || false
    ! [[ $output =~ " 0 " ]] || false

    run dolt sql-client -u dolt -P $PORT --use-db repo1 -q "select c from t"
    [ $status -ne 0 ]
    [[ $output =~ "not found" ]] || false
}

@test "sql-client: handle dashes for implicit database" {
    make_repo test-dashes
    cd test-dashes
    PORT=$( definePORT )
    dolt sql-server --user=root --port=$PORT &
    SERVER_PID=$! # will get killed by teardown_common
    sleep 5 # not using python wait so this works on windows

    run	dolt sql-client -u root -P $PORT -q "show databases"
    [ $status -eq 0 ]
    [[ $output =~ " test_dashes " ]] || false
}

@test "sql-client: prints accurate query timing" {
    skiponwindows "Missing dependencies"
    cd repo1
    start_sql_server repo1
    cd ../
    run dolt sql-client --host=0.0.0.0 --port=$PORT --user=dolt <<SQL
USE repo1;
SELECT SLEEP(2);
SQL
    [[ $output =~ "1 row in set (2".*" sec)" ]] || false
}
