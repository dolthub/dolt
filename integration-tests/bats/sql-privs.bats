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
SELECT user from mysql.user order by user;
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

@test "sql-privs: no doltcfg directory, makes one" {
    skiponwindows "redirecting SQL to sql-client returns nothing after welcome messages"
    cd repo1
    rm -rf .doltcfg

    start_sql_server repo1

    # expect only dolt user
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

    # create user
    run create_user

    # expect only dolt user and new_user
    run show_users
    [ "$status" -eq 0 ]
    [ "${lines[0]}" = '# Welcome to the Dolt MySQL client.' ]
    [ "${lines[1]}" = "# Statements must be terminated with ';'." ]
    [ "${lines[2]}" = '# "exit" or "quit" (or Ctrl-D) to exit.' ]
    [ "${lines[3]}" = '+----------+' ]
    [ "${lines[4]}" = '| User     |' ]
    [ "${lines[5]}" = '+----------+' ]
    [ "${lines[6]}" = '| dolt     |' ]
    [ "${lines[7]}" = '| new_user |' ]
    [ "${lines[8]}" = '+----------+' ]

    # restart server
    stop_sql_server
    start_sql_server repo1

    # expect only dolt user and new_user
    run show_users
    [ "$status" -eq 0 ]
    [ "${lines[0]}" = '# Welcome to the Dolt MySQL client.' ]
    [ "${lines[1]}" = "# Statements must be terminated with ';'." ]
    [ "${lines[2]}" = '# "exit" or "quit" (or Ctrl-D) to exit.' ]
    [ "${lines[3]}" = '+----------+' ]
    [ "${lines[4]}" = '| User     |' ]
    [ "${lines[5]}" = '+----------+' ]
    [ "${lines[6]}" = '| dolt     |' ]
    [ "${lines[7]}" = '| new_user |' ]
    [ "${lines[8]}" = '+----------+' ]

    # remove config file
    rm -rf .doltcfg

    # leave the directory
    cd ..
}

@test "sql-privs: multiple doltcfg directories causes error" {
    skiponwindows "redirecting SQL to sql-client returns nothing after welcome messages"
    cd repo1

    rm -rf .doltcfg
    rm -rf inner_db

    mkdir .doltcfg

    mkdir inner_db
    cd inner_db
    mkdir .doltcfg
    dolt init

    run start_sql_server_with_args inner_db
    [ "$status" -eq 1 ]
    [[ "$output" =~ "multiple .doltcfg directories detected" ]] || false

    stop_sql_server

    # remove mysql.db if they exist
    cd ..
    rm -rf .doltcfg
    rm -rf inner_db
}

@test "sql-privs: specify doltcfg directory" {
    skiponwindows "redirecting SQL to sql-client returns nothing after welcome messages"
    cd repo1

    rm -rf .doltcfg
    rm -rf cfgdir

    mkdir cfgdir
    start_sql_server_with_args --doltcfg-dir cfgdir repo1

    # expect dolt user and new_user
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

    # create user
    run create_user
    [ "$status" -eq 0 ]

    # expect only dolt user and new_user
    run show_users
    [ "$status" -eq 0 ]
    [ "${lines[0]}" = '# Welcome to the Dolt MySQL client.' ]
    [ "${lines[1]}" = "# Statements must be terminated with ';'." ]
    [ "${lines[2]}" = '# "exit" or "quit" (or Ctrl-D) to exit.' ]
    [ "${lines[3]}" = '+----------+' ]
    [ "${lines[4]}" = '| User     |' ]
    [ "${lines[5]}" = '+----------+' ]
    [ "${lines[6]}" = '| dolt     |' ]
    [ "${lines[7]}" = '| new_user |' ]
    [ "${lines[8]}" = '+----------+' ]

    stop_sql_server
    start_sql_server_with_args --doltcfg-dir cfgdir repo1

    # expect only dolt user and new_user
    run show_users
    [ "$status" -eq 0 ]
    [ "${lines[0]}" = '# Welcome to the Dolt MySQL client.' ]
    [ "${lines[1]}" = "# Statements must be terminated with ';'." ]
    [ "${lines[2]}" = '# "exit" or "quit" (or Ctrl-D) to exit.' ]
    [ "${lines[3]}" = '+----------+' ]
    [ "${lines[4]}" = '| User     |' ]
    [ "${lines[5]}" = '+----------+' ]
    [ "${lines[6]}" = '| dolt     |' ]
    [ "${lines[7]}" = '| new_user |' ]
    [ "${lines[8]}" = '+----------+' ]

    rm -rf .doltcfg
    rm -rf cfgdir
    cd ..
}