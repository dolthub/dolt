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

show_users() {
    dolt sql-client --host=0.0.0.0 --port=$PORT --user=dolt <<SQL
SELECT user from mysql.user;
SQL
}

create_user() {
    dolt sql-client --host=0.0.0.0 --port=$PORT --user=dolt <<SQL
CREATE USER new_user;
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

@test "0-sql-client: test sql-client shows tables" {
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

@test "0-sql-client: no privs.json and no mysql.db, create mysql.db" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    cd repo1

    start_sql_server repo1
    cd ../

    run show_users
    [ "$status" -eq 0 ]
    [ "${lines[0]}" = '# Welcome to the Dolt MySQL client.' ]
    [ "${lines[1]}" = "# Statements must be terminated with ';'." ]
    [ "${lines[2]}" = '# "exit" or "quit" (or Ctrl-D) to exit.' ]
    [ "${lines[3]}" = '+------+' ]
    [ "${lines[4]}" = '| User |' ]
    [ "${lines[5]}" = '+------+' ]
    [ "${lines[6]}" = '| dolt |' ]
    [ "${lines[7]}" = '+------+' ]

    # check that mysql.db file exists, and privs.json doesn't
    cd repo1
    run ls
    [[ "$output" =~ "mysql.db" ]] || false
    ![[ "$output" =~ "privs.json" ]] || false

    # remove mysql.db and privs.json if they exist
    rm -f mysql.db
    rm -f privs.json
}

@test "0-sql-client: has privs.json and no mysql.db, read from privs.json and create mysql.db" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    cd repo1
    cp $BATS_TEST_DIRNAME/privs.json .

    start_sql_server repo1
    cd ../

    run show_users
    [ "$status" -eq 0 ]
    [ "${lines[0]}" = '# Welcome to the Dolt MySQL client.' ]
    [ "${lines[1]}" = "# Statements must be terminated with ';'." ]
    [ "${lines[2]}" = '# "exit" or "quit" (or Ctrl-D) to exit.' ]
    [ "${lines[3]}" = '+------------+' ]
    [ "${lines[4]}" = '| User       |' ]
    [ "${lines[5]}" = '+------------+' ]
    [ "${lines[6]}" = '| dolt       |' ]
    [ "${lines[7]}" = '| privs_user |' ]
    [ "${lines[8]}" = '+------------+' ]

    # make a new user, triggering persist
    run create_user
    [ "$status" -eq 0 ]

    # ensure changes did not save to privs.json
    cd repo1
    run cat privs.json
    ![[ "$output" =~ "new_user" ]]

    # check that mysql.db and privs.json exist
    run ls
    [[ "$output" =~ "mysql.db" ]] || false
    [[ "$output" =~ "privs.json" ]] || false

    # remove mysql.db and privs.json if they exist
    rm -f mysql.db
    rm -f privs.json
}