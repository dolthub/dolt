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

@test "sql-privs: default options" {
    skiponwindows "redirecting SQL to sql-client returns nothing after welcome messages"
    cd repo1
    rm -rf .doltcfg

    start_sql_server repo1

    # expect only dolt user
    run show_users
    [ "$status" -eq 0 ]
    [[ "$output" =~ "dolt" ]] || false
    ! [[ "$output" =~ "new_user" ]] || false

    # check for config directory
    run ls -a
    [[ "$output" =~ ".doltcfg" ]] || false

    # create user
    run create_user

    # expect dolt user and new_user
    run show_users
    [ "$status" -eq 0 ]
    [[ "$output" =~ "dolt" ]] || false
    [[ "$output" =~ "new_user" ]] || false

    # restart server
    stop_sql_server 1
    start_sql_server repo1

    # expect dolt user and new_user
    run show_users
    [ "$status" -eq 0 ]
    [[ "$output" =~ "dolt" ]] || false
    [[ "$output" =~ "new_user" ]] || false

    stop_sql_server 1

    # remove config file
    rm -rf .doltcfg
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

    run start_sql_server repo1
    [ "$status" -eq 1 ]
    [[ "$output" =~ "multiple .doltcfg directories detected" ]] || false

    # remove config directory and inner_db
    cd ..
    rm -rf .doltcfg
    rm -rf inner_db

    cd ..
}

@test "sql-privs: dolt sql -q" {
    # remove any previous config directories
    rm -rf .doltcfg

    # show users, expect just root user
    run dolt sql -q "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    ! [[ "$output" =~ "new_user" ]] || false

    # create new_user
    run dolt sql -q "create user new_user"
    [ "$status" -eq 0 ]

    # show users, expect root user and new_user
    run dolt sql -q "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    [[ "$output" =~ "new_user" ]] || false

    # check for config directory
    run ls -a
    [[ "$output" =~ ".doltcfg" ]] || false

    # check for no privileges.db file
    run ls .doltcfg
    [[ "$output" =~ "privileges.db" ]] || false

    # remove config directory just in case
    rm -rf .doltcfg
}

@test "sql: dolt sql -q --data-dir" {
    # remove config files
    rm -rf .doltcfg
    rm -rf db_dir

    # create data dir
    mkdir db_dir
    cd db_dir

    # create databases
    mkdir db1
    cd db1
    dolt init
    cd ..

    mkdir db2
    cd db2
    dolt init
    cd ..

    mkdir db3
    cd db3
    dolt init
    cd ..

    cd ..

    # show databases, expect all
    run dolt sql --data-dir=db_dir -q "show databases;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "db1" ]] || false
    [[ "$output" =~ "db2" ]] || false
    [[ "$output" =~ "db3" ]] || false

    # show users, expect just root user
    run dolt sql --data-dir=db_dir -q "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    ! [[ "$output" =~ "new_user" ]] || false

    # expect no .doltcfg in current directory
    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false

    # expect .doltcfg in $datadir
    run ls -a db_dir
    [[ "$output" =~ ".doltcfg" ]] || false

    # create new user
    run dolt sql --data-dir=db_dir -q "create user new_user"
    [ "$status" -eq 0 ]

    # show users, expect root user and new_user
    run dolt sql --data-dir=db_dir -q "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    [[ "$output" =~ "new_user" ]] || false

    # expect no privileges.db in current directory
    run ls
    ! [[ "$output" =~ "privileges.db" ]] || false

    # expect no privileges.db in $datadir directory
    run ls db_dir
    ! [[ "$output" =~ "privileges.db" ]] || false

    # expect privileges.db in $datadir/.doltcfg
    run ls db_dir/.doltcfg
    [[ "$output" =~ "privileges.db" ]] || false


    # test relative to $datadir
    cd db_dir

    # show databases, expect all
    run dolt sql -q "show databases;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "db1" ]] || false
    [[ "$output" =~ "db2" ]] || false
    [[ "$output" =~ "db3" ]] || false

    # expect to find same users when in $datadir
    run dolt sql -q "select user from mysql.user"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    [[ "$output" =~ "new_user" ]] || false

    cd ..

    # remove config files
    rm -rf .doltcfg
    rm -rf db_dir
}

@test "sql: dolt sql -q --doltcfg-dir" {
    # remove any previous config directories
    rm -rf .doltcfg
    rm -rf doltcfgdir

    # show users, expect just root user
    run dolt sql --doltcfg-dir=doltcfgdir -q "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    ! [[ "$output" =~ "new_user" ]] || false

    # expect only custom doltcfgdir
    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    [[ "$output" =~ "doltcfgdir" ]] || false

    # create new_user
    run dolt sql --doltcfg-dir=doltcfgdir -q "create user new_user"
    [ "$status" -eq 0 ]

    # show users, expect root user and new_user
    run dolt sql --doltcfg-dir=doltcfgdir -q "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    [[ "$output" =~ "new_user" ]] || false

    # expect privileges files in doltcfgdir
    run ls doltcfgdir
    [[ "$output" =~ "privileges.db" ]] || false

    # remove config directory just in case
    rm -rf .doltcfg
    rm -rf doltcfgdir
}

@test "sql: dolt sql -q --privilege-file" {
    # remove config files
    rm -rf .doltcfg
    rm -f privs.db

    # show users, expect just root user
    run dolt sql --privilege-file=privs.db -q "select user from mysql.user;"
    [[ "$output" =~ "root" ]] || false
    ! [[ "$output" =~ "new_user" ]] || false

    # expect default doltcfg directory
    run ls -a
    [[ "$output" =~ ".doltcfg" ]] || false

    # create new_user
    run dolt sql --privilege-file=privs.db -q "create user new_user"
    [ "$status" -eq 0 ]

    # show users, expect root user and new_user
    run dolt sql --privilege-file=privs.db -q "select user from mysql.user;"
    [[ "$output" =~ "root" ]] || false
    [[ "$output" =~ "new_user" ]] || false

    # expect custom privilege file current directory
    run ls
    [[ "$output" =~ "privs.db" ]] || false

    # expect to not see new_user when privs.db not specified
    run dolt sql -q "select user from mysql.user"
    [[ "$output" =~ "root" ]] || false
    ! [[ "$output" =~ "new_user" ]] || false

    # remove config files
    rm -rf .doltcfg
    rm -f privs.db
}

@test "sql: dolt sql -q --data-dir --doltcfg-dir" {
    # remove config files
    rm -rf .doltcfg
    rm -rf db_dir
    rm -rf doltcfgdir

    # create data dir
    mkdir db_dir
    cd db_dir

    # create databases
    mkdir db1
    cd db1
    dolt init
    cd ..

    mkdir db2
    cd db2
    dolt init
    cd ..

    mkdir db3
    cd db3
    dolt init
    cd ..

    cd ..

    # show databases, expect all
    run dolt sql --data-dir=db_dir --doltcfg-dir=doltcfgdir -q "show databases;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "db1" ]] || false
    [[ "$output" =~ "db2" ]] || false
    [[ "$output" =~ "db3" ]] || false

    # show users, expect just root user
    run dolt sql --data-dir=db_dir --doltcfg-dir=doltcfgdir -q "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    ! [[ "$output" =~ "new_user" ]] || false

    # expect custom doltcfg in current directory
    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    [[ "$output" =~ "doltcfgdir" ]] || false

    # expect no .doltcfg in $datadir
    run ls -a db_dir
    ! [[ "$output" =~ ".doltcfg" ]] || false

    # create new user
    run dolt sql --data-dir=db_dir --doltcfg-dir=doltcfgdir -q "create user new_user"
    [ "$status" -eq 0 ]

    # show users, expect root user and new_user
    run dolt sql --data-dir=db_dir --doltcfg-dir=doltcfgdir -q "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    [[ "$output" =~ "new_user" ]] || false

    # expect no privileges.db in current directory
    run ls
    ! [[ "$output" =~ "privileges.db" ]] || false

    # expect no privileges.db in $datadir directory
    run ls db_dir
    ! [[ "$output" =~ "privileges.db" ]] || false

    # expect privileges.db in $doltcfg directory
    run ls doltcfgdir
    [[ "$output" =~ "privileges.db" ]] || false

    # test relative to $datadir
    cd db_dir

    # show databases, expect all
    run dolt sql -q "show databases;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "db1" ]] || false
    [[ "$output" =~ "db2" ]] || false
    [[ "$output" =~ "db3" ]] || false

    # show users, expect root
    run dolt sql -q "select user from mysql.user"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    ! [[ "$output" =~ "new_user" ]] || false

    # show users, expect root and new_user
    run dolt sql --doltcfg-dir=../doltcfgdir -q "select user from mysql.user"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    [[ "$output" =~ "new_user" ]] || false

    cd ..

    # remove config files
    rm -rf .doltcfg
    rm -rf db_dir
    rm -rf doltcfgdir
}

@test "sql: dolt sql -q --data-dir --privilege-file" {
    # remove config files
    rm -rf .doltcfg
    rm -rf db_dir
    rm -rf privs.db

    # create data dir
    mkdir db_dir
    cd db_dir

    # create databases
    mkdir db1
    cd db1
    dolt init
    cd ..

    mkdir db2
    cd db2
    dolt init
    cd ..

    mkdir db3
    cd db3
    dolt init
    cd ..

    cd ..

    # show databases, expect all
    run dolt sql --data-dir=db_dir --privilege-file=privs.db -q "show databases;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "db1" ]] || false
    [[ "$output" =~ "db2" ]] || false
    [[ "$output" =~ "db3" ]] || false

    # show users, expect just root user
    run dolt sql --data-dir=db_dir --privilege-file=privs.db -q "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    ! [[ "$output" =~ "new_user" ]] || false

    # expect no .doltcfg in current directory
    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false

    # expect .doltcfg in $datadir
    run ls -a db_dir
    [[ "$output" =~ ".doltcfg" ]] || false

    # create new user
    run dolt sql --data-dir=db_dir --privilege-file=privs.db -q "create user new_user"
    [ "$status" -eq 0 ]

    # show users, expect root user and new_user
    run dolt sql --data-dir=db_dir --privilege-file=privs.db -q "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    [[ "$output" =~ "new_user" ]] || false

    # expect privs.db in current directory
    run ls
    [[ "$output" =~ "privs.db" ]] || false

    # expect no privileges.db in $datadir directory
    run ls db_dir
    ! [[ "$output" =~ "privs.db" ]] || false

    # expect no privs.db in $doltcfg directory
    run ls db_dir/.doltcfg
    ! [[ "$output" =~ "privs.db" ]] || false

    # test relative to $datadir
    cd db_dir

    # show databases, expect all
    run dolt sql -q "show databases;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "db1" ]] || false
    [[ "$output" =~ "db2" ]] || false
    [[ "$output" =~ "db3" ]] || false

    # show users, expect root
    run dolt sql -q "select user from mysql.user"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    ! [[ "$output" =~ "new_user" ]] || false

    # show users, expect root and new_user
    run dolt sql --privilege-file=../privs.db -q "select user from mysql.user"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    [[ "$output" =~ "new_user" ]] || false

    cd ..

    # remove config files
    rm -rf .doltcfg
    rm -rf db_dir
    rm -rf privs.db
}

@test "sql-privs: dolt sql -q --doltcfg-dir --privilege-file" {
    # remove any previous config directories
    rm -rf .doltcfg
    rm -rf doltcfgdir
    rm -rf privs.db

    # show users, expect just root user
    run dolt sql --doltcfg-dir=doltcfgdir --privilege-file=privs.db -q "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    ! [[ "$output" =~ "new_user" ]] || false

    # expect custom doltcfgdir
    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    [[ "$output" =~ "doltcfgdir" ]] || false

    # create new_user
    run dolt sql --doltcfg-dir=doltcfgdir --privilege-file=privs.db -q "create user new_user"
    [ "$status" -eq 0 ]

    # show users, expect root user and new_user
    run dolt sql --doltcfg-dir=doltcfgdir --privilege-file=privs.db -q "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    [[ "$output" =~ "new_user" ]] || false

    # expect privileges file
    run ls
    [[ "$output" =~ "privs.db" ]] || false

    # expect no privileges file in doltcfgdir
    run ls doltcfgdir
    ! [[ "$output" =~ "privileges.db" ]] || false
    ! [[ "$output" =~ "privs.db" ]] || false

    # remove config directory just in case
    rm -rf .doltcfg
    rm -rf doltcfgdir
    rm -rf privs.db
}

@test "sql: dolt sql -q --data-dir --doltcfg-dir --privileges-file" {
    # remove config files
    rm -rf .doltcfg
    rm -rf db_dir
    rm -rf doltcfgdir
    rm -rf privs.db

    # create data dir
    mkdir db_dir
    cd db_dir

    # create databases
    mkdir db1
    cd db1
    dolt init
    cd ..

    mkdir db2
    cd db2
    dolt init
    cd ..

    mkdir db3
    cd db3
    dolt init
    cd ..

    cd ..

    # show databases, expect all
    run dolt sql --data-dir=db_dir --doltcfg-dir=doltcfgdir --privilege-file=privs.db -q "show databases;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "db1" ]] || false
    [[ "$output" =~ "db2" ]] || false
    [[ "$output" =~ "db3" ]] || false

    # show users, expect just root user
    run dolt sql --data-dir=db_dir --doltcfg-dir=doltcfgdir --privilege-file=privs.db -q "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    ! [[ "$output" =~ "new_user" ]] || false

    # expect custom doltcfg in current directory
    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    [[ "$output" =~ "doltcfgdir" ]] || false

    # expect no .doltcfg in $datadir
    run ls -a db_dir
    ! [[ "$output" =~ ".doltcfg" ]] || false

    # create new user
    run dolt sql --data-dir=db_dir --doltcfg-dir=doltcfgdir --privilege-file=privs.db -q "create user new_user"
    [ "$status" -eq 0 ]

    # show users, expect root user and new_user
    run dolt sql --data-dir=db_dir --doltcfg-dir=doltcfgdir --privilege-file=privs.db -q "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    [[ "$output" =~ "new_user" ]] || false

    # expect privs.db in current directory
    run ls
    ! [[ "$output" =~ "privileges.db" ]] || false
    [[ "$output" =~ "privs.db" ]] || false

    # expect no privileges.db in $datadir directory
    run ls db_dir
    ! [[ "$output" =~ "privileges.db" ]] || false
    ! [[ "$output" =~ "privs.db" ]] || false

    # expect no privileges.db in $doltcfg directory
    run ls doltcfgdir
    ! [[ "$output" =~ "privileges.db" ]] || false
    ! [[ "$output" =~ "privs.db" ]] || false

    # test relative to $datadir
    cd db_dir

    # show databases, expect all
    run dolt sql -q "show databases;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "db1" ]] || false
    [[ "$output" =~ "db2" ]] || false
    [[ "$output" =~ "db3" ]] || false

    # show users, expect root
    run dolt sql -q "select user from mysql.user"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    ! [[ "$output" =~ "new_user" ]] || false

    # show users, expect root and new_user
    run dolt sql --doltcfg-dir=../doltcfgdir -q "select user from mysql.user"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    ! [[ "$output" =~ "new_user" ]] || false

    # show users, expect root and new_user
    run dolt sql --privilege-file=../privs.db -q "select user from mysql.user"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    [[ "$output" =~ "new_user" ]] || false

    cd ..

    # remove config files
    rm -rf .doltcfg
    rm -rf db_dir
    rm -rf doltcfgdir
    rm -rf privs.db
}

@test "sql: dolt sql -q create database and specify privilege file" {
    # remove existing directories
    rm -rf .doltcfg
    rm -rf inner_db
    rm -f privs.db

    run dolt sql --privilege-file=privs.db -q "create database inner_db;"
    [ "$status" -eq 0 ]

    run dolt sql --privilege-file=privs.db -q "create user new_user;"
    [ "$status" -eq 0 ]

    run dolt sql --privilege-file=privs.db -q "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    [[ "$output" =~ "new_user" ]] || false

    cd inner_db

    run dolt sql --privilege-file=../privs.db -q "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    [[ "$output" =~ "new_user" ]] || false

    cd ..

    # remove existing directories
    rm -rf .doltcfg
    rm -rf inner_db
    rm -f privs.db
}