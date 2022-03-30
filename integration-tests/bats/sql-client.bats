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
    dolt sql-client --host=0.0.0.0 --port=$PORT --user=dolt =<<SQL
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
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."
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
