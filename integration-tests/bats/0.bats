#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash
load $BATS_TEST_DIRNAME/helper/query-server-common.bash

make_repo() {
  mkdir "$1"
  cd "$1"
  dolt init
  cd ..
}

show_users() {
    dolt sql-client --host=0.0.0.0 --port=$PORT --user=dolt <<SQL
SELECT user from mysql.user;
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

@test "0: no privs.json and no mysql.db, create mysql.db" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    cd repo1

    # remove mysql.db and privs.json if they exist
    rm -f mysql.db
    rm -f privs.json

    start_sql_server repo1
    cd ../

    # expect only dolt as user
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

    cd repo1

    # check that mysql.db file exists, and privs.json doesn't
    run ls
    [[ "$output" =~ "mysql.db" ]] || false
    ![[ "$output" =~ "privs.json" ]] || false

    # remove mysql.db and privs.json if they exist
    rm -f mysql.db
    rm -f privs.json
}