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

@test "sql-privs: no privs.json and no mysql.db, create mysql.db" {
    skiponwindows "redirecting SQL to sql-client returns nothing after welcome messages"
    cd repo1

    # remove/replace mysql.db and privs.json if they exist
    rm -f mysql.db
    rm -f privs.json

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
    [ "$status" -eq 0 ]

    # expect dolt and new_user
    run show_users
    [ "${lines[0]}" = '# Welcome to the Dolt MySQL client.' ]
    [ "${lines[1]}" = "# Statements must be terminated with ';'." ]
    [ "${lines[2]}" = '# "exit" or "quit" (or Ctrl-D) to exit.' ]
    [ "${lines[3]}" = '+----------+' ]
    [ "${lines[4]}" = '| User     |' ]
    [ "${lines[5]}" = '+----------+' ]
    [ "${lines[6]}" = '| dolt     |' ]
    [ "${lines[7]}" = '| new_user |' ]
    [ "${lines[8]}" = '+----------+' ]

    # check that mysql.db file exists, and privs.json doesn't
    run ls
    [[ "$output" =~ "mysql.db" ]] || false
    ! [[ "$output" =~ "privs.json" ]] || false

    # restart server
    stop_sql_server
    start_sql_server repo1

    # check for new_user
    run show_users
    [ "${lines[0]}" = '# Welcome to the Dolt MySQL client.' ]
    [ "${lines[1]}" = "# Statements must be terminated with ';'." ]
    [ "${lines[2]}" = '# "exit" or "quit" (or Ctrl-D) to exit.' ]
    [ "${lines[3]}" = '+----------+' ]
    [ "${lines[4]}" = '| User     |' ]
    [ "${lines[5]}" = '+----------+' ]
    [ "${lines[6]}" = '| dolt     |' ]
    [ "${lines[7]}" = '| new_user |' ]
    [ "${lines[8]}" = '+----------+' ]

    # remove mysql.db and privs.json if they exist
    rm -f mysql.db
    rm -f privs.json

    # leave the directory
    cd ..
}

@test "sql-privs: has privs.json and no mysql.db, read from privs.json and create mysql.db" {
    skiponwindows "redirecting SQL to sql-client returns nothing after welcome messages"
    cd repo1

    # remove/replace mysql.db and privs.json if they exist
    rm -f mysql.db
    rm -f privs.json
    cp $BATS_TEST_DIRNAME/privs.json .

    start_sql_server repo1

    # expect dolt and privs_user
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

    # create user
    run create_user
    [ "$status" -eq 0 ]

    # expect dolt, privs_user, and new_user
    run show_users
    [ "${lines[0]}" = '# Welcome to the Dolt MySQL client.' ]
    [ "${lines[1]}" = "# Statements must be terminated with ';'." ]
    [ "${lines[2]}" = '# "exit" or "quit" (or Ctrl-D) to exit.' ]
    [ "${lines[3]}" = '+------------+' ]
    [ "${lines[4]}" = '| User       |' ]
    [ "${lines[5]}" = '+------------+' ]
    [ "${lines[6]}" = '| dolt       |' ]
    [ "${lines[7]}" = '| new_user   |' ]
    [ "${lines[8]}" = '| privs_user |' ]
    [ "${lines[9]}" = '+------------+' ]

    # new user didn't persist to privs.json
    run cat privs.json
    ! [[ "$output" =~ "new_user" ]] || false

    # check that mysql.db and privs.json exist
    run ls
    [[ "$output" =~ "mysql.db" ]] || false
    [[ "$output" =~ "privs.json" ]] || false

    # restart server
    stop_sql_server
    start_sql_server repo1

    # expect dolt, privs_user, and new_user
    run show_users
    [ "${lines[0]}" = '# Welcome to the Dolt MySQL client.' ]
    [ "${lines[1]}" = "# Statements must be terminated with ';'." ]
    [ "${lines[2]}" = '# "exit" or "quit" (or Ctrl-D) to exit.' ]
    [ "${lines[3]}" = '+------------+' ]
    [ "${lines[4]}" = '| User       |' ]
    [ "${lines[5]}" = '+------------+' ]
    [ "${lines[6]}" = '| dolt       |' ]
    [ "${lines[7]}" = '| new_user   |' ]
    [ "${lines[8]}" = '| privs_user |' ]
    [ "${lines[9]}" = '+------------+' ]

    # remove mysql.db and privs.json if they exist
    rm -f mysql.db
    rm -f privs.json

    # leave the directory
    cd ..
}

@test "sql-privs: no privs.json and has mysql.db, read from mysql.db" {
    skiponwindows "redirecting SQL to sql-client returns nothing after welcome messages"

    cd repo1

    # remove/replace mysql.db and privs.json if they exist
    rm -f mysql.db
    rm -f privs.json
    cp $BATS_TEST_DIRNAME/mysql.db .

    start_sql_server repo1

    # expect dolt and mysql_user
    run show_users
    [ "$status" -eq 0 ]
    [ "${lines[0]}" = '# Welcome to the Dolt MySQL client.' ]
    [ "${lines[1]}" = "# Statements must be terminated with ';'." ]
    [ "${lines[2]}" = '# "exit" or "quit" (or Ctrl-D) to exit.' ]
    [ "${lines[3]}" = '+------------+' ]
    [ "${lines[4]}" = '| User       |' ]
    [ "${lines[5]}" = '+------------+' ]
    [ "${lines[6]}" = '| dolt       |' ]
    [ "${lines[7]}" = '| mysql_user |' ]
    [ "${lines[8]}" = '+------------+' ]

    # create user
    run create_user
    [ "$status" -eq 0 ]

    # expect dolt, new_user, and mysql_user
    run show_users
    [ "${lines[0]}" = '# Welcome to the Dolt MySQL client.' ]
    [ "${lines[1]}" = "# Statements must be terminated with ';'." ]
    [ "${lines[2]}" = '# "exit" or "quit" (or Ctrl-D) to exit.' ]
    [ "${lines[3]}" = '+------------+' ]
    [ "${lines[4]}" = '| User       |' ]
    [ "${lines[5]}" = '+------------+' ]
    [ "${lines[6]}" = '| dolt       |' ]
    [ "${lines[7]}" = '| mysql_user |' ]
    [ "${lines[8]}" = '| new_user   |' ]
    [ "${lines[9]}" = '+------------+' ]

    # check that mysql.db exists, and privs.json doesn't
    run ls
    [[ "$output" =~ "mysql.db" ]] || false
    ! [[ "$output" =~ "privs.json" ]] || false

    # restart server
    stop_sql_server
    start_sql_server repo1

    # expect dolt, new_user, and mysql_user
    run show_users
    [ "${lines[0]}" = '# Welcome to the Dolt MySQL client.' ]
    [ "${lines[1]}" = "# Statements must be terminated with ';'." ]
    [ "${lines[2]}" = '# "exit" or "quit" (or Ctrl-D) to exit.' ]
    [ "${lines[3]}" = '+------------+' ]
    [ "${lines[4]}" = '| User       |' ]
    [ "${lines[5]}" = '+------------+' ]
    [ "${lines[6]}" = '| dolt       |' ]
    [ "${lines[7]}" = '| mysql_user |' ]
    [ "${lines[8]}" = '| new_user   |' ]
    [ "${lines[9]}" = '+------------+' ]

    # remove mysql.db and privs.json if they exist
    rm -f mysql.db
    rm -f privs.json

    # leave the directory
    cd ..
}

@test "sql-privs: has privs.json and has mysql.db, only reads from mysql.db" {
    skiponwindows "redirecting SQL to sql-client returns nothing after welcome messages"

    cd repo1

    # remove/replace mysql.db and privs.json if they exist
    rm -f mysql.db
    rm -f privs.json
    cp $BATS_TEST_DIRNAME/privs.json .
    cp $BATS_TEST_DIRNAME/mysql.db .

    start_sql_server repo1

    # expect dolt and mysql_user
    run show_users
    [ "$status" -eq 0 ]
    [ "${lines[0]}" = '# Welcome to the Dolt MySQL client.' ]
    [ "${lines[1]}" = "# Statements must be terminated with ';'." ]
    [ "${lines[2]}" = '# "exit" or "quit" (or Ctrl-D) to exit.' ]
    [ "${lines[3]}" = '+------------+' ]
    [ "${lines[4]}" = '| User       |' ]
    [ "${lines[5]}" = '+------------+' ]
    [ "${lines[6]}" = '| dolt       |' ]
    [ "${lines[7]}" = '| mysql_user |' ]
    [ "${lines[8]}" = '+------------+' ]

    # create user
    run create_user
    [ "$status" -eq 0 ]

    # expect dolt, new_user, and mysql_user
    run show_users
    [ "${lines[0]}" = '# Welcome to the Dolt MySQL client.' ]
    [ "${lines[1]}" = "# Statements must be terminated with ';'." ]
    [ "${lines[2]}" = '# "exit" or "quit" (or Ctrl-D) to exit.' ]
    [ "${lines[3]}" = '+------------+' ]
    [ "${lines[4]}" = '| User       |' ]
    [ "${lines[5]}" = '+------------+' ]
    [ "${lines[6]}" = '| dolt       |' ]
    [ "${lines[7]}" = '| mysql_user |' ]
    [ "${lines[8]}" = '| new_user   |' ]
    [ "${lines[9]}" = '+------------+' ]

    # new user didn't persist to privs.json
    run cat privs.json
    ! [[ "$output" =~ "new_user" ]] || false

    # check that mysql.db and privs.json exist
    run ls
    [[ "$output" =~ "mysql.db" ]] || false
    [[ "$output" =~ "privs.json" ]] || false

    # restart server
    stop_sql_server
    start_sql_server repo1

    # expect dolt, new_user, and mysql_user
    run show_users
    [ "${lines[0]}" = '# Welcome to the Dolt MySQL client.' ]
    [ "${lines[1]}" = "# Statements must be terminated with ';'." ]
    [ "${lines[2]}" = '# "exit" or "quit" (or Ctrl-D) to exit.' ]
    [ "${lines[3]}" = '+------------+' ]
    [ "${lines[4]}" = '| User       |' ]
    [ "${lines[5]}" = '+------------+' ]
    [ "${lines[6]}" = '| dolt       |' ]
    [ "${lines[7]}" = '| mysql_user |' ]
    [ "${lines[8]}" = '| new_user   |' ]
    [ "${lines[9]}" = '+------------+' ]

    # remove mysql.db and privs.json if they exist
    rm -f mysql.db
    rm -f privs.json

    # leave the directory
    cd ..
}