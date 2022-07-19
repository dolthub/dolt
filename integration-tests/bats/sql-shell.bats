#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    dolt sql <<SQL
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

teardown() {
    assert_feature_version
    teardown_common
}

@test "sql-shell: run a query in sql shell" {
    skiponwindows "Works on Windows command prompt but not the WSL terminal used during bats"
    run bash -c "echo 'select * from test;' | dolt sql"
    [ $status -eq 0 ]
    [[ "$output" =~ "pk" ]] || false
}

@test "sql-shell: can't start shell with invalid user" {
    run dolt sql --user=notroot <<< "show databases"
    [ "$status" -eq 0 ]
    run dolt sql --user=toor <<< "show databases"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "user toor@localhost does not exist" ]] || false
}

@test "sql-shell: sql shell writes to disk after every iteration (autocommit)" {
    skiponwindows "Need to install expect and make this script work on windows."
    run $BATS_TEST_DIRNAME/sql-shell.expect
    echo "$output"

    # 2 tables are created. 1 from above and 1 in the expect file.
    [[ "$output" =~ "+---------------------" ]] || false
    [[ "$output" =~ "| Tables_in_dolt_repo_" ]] || false
    [[ "$output" =~ "+---------------------" ]] || false
    [[ "$output" =~ "| test                " ]] || false
    [[ "$output" =~ "| test_expect         " ]] || false
    [[ "$output" =~ "+---------------------" ]] || false
}

@test "sql-shell: default datadir, doltcfg, and privs" {
    # remove config files
    rm -rf .doltcfg

     # show users, expect just root user
    run dolt sql <<< "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    ! [[ "$output" =~ "new_user" ]] || false

    # check for config directory
    run ls -a
    [[ "$output" =~ ".doltcfg" ]] || false

    # check for no privileges.db file
    run ls .doltcfg
    ! [[ "$output" =~ "privileges.db" ]] || false

    # create new_user
    run dolt sql <<< "create user new_user"
    [ "$status" -eq 0 ]

    # show users, expect root user and new_user
    run dolt sql <<< "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    [[ "$output" =~ "new_user" ]] || false

    # check for config directory
    run ls -a
    [[ "$output" =~ ".doltcfg" ]] || false

    # check for no privileges.db file
    run ls .doltcfg
    [[ "$output" =~ "privileges.db" ]] || false

    # remove existing .doltcfg
    rm -rf .doltcfg
}

@test "sql-shell: specify data-dir" {
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
    run dolt sql --data-dir=db_dir <<< "show databases;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "db1" ]] || false
    [[ "$output" =~ "db2" ]] || false
    [[ "$output" =~ "db3" ]] || false

    # show users, expect just root user
    run dolt sql --data-dir=db_dir <<< "select user from mysql.user;"
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
    run dolt sql --data-dir=db_dir <<< "create user new_user"
    [ "$status" -eq 0 ]

    # show users, expect root user and new_user
    run dolt sql --data-dir=db_dir <<< "select user from mysql.user;"
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
    run dolt sql <<< "show databases;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "db1" ]] || false
    [[ "$output" =~ "db2" ]] || false
    [[ "$output" =~ "db3" ]] || false

    # expect to find same users when in $datadir
    run dolt sql <<< "select user from mysql.user"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    [[ "$output" =~ "new_user" ]] || false

    cd ..

    # remove config files
    rm -rf .doltcfg
    rm -rf db_dir
}

@test "sql-shell: specify doltcfg directory" {
    # remove any previous config directories
    rm -rf .doltcfg
    rm -rf doltcfgdir

    # show users, expect just root user
    run dolt sql --doltcfg-dir=doltcfgdir <<< "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    ! [[ "$output" =~ "new_user" ]] || false

    # expect only custom doltcfgdir
    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    [[ "$output" =~ "doltcfgdir" ]] || false

    # create new_user
    run dolt sql --doltcfg-dir=doltcfgdir <<< "create user new_user"
    [ "$status" -eq 0 ]

    # show users, expect root user and new_user
    run dolt sql --doltcfg-dir=doltcfgdir <<< "select user from mysql.user;"
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

@test "sql-shell: specify privilege file" {
    # remove config files
    rm -rf .doltcfg
    rm -f privs.db

    # show users, expect just root user
    run dolt sql --privilege-file=privs.db <<< "select user from mysql.user;"
    [[ "$output" =~ "root" ]] || false
    ! [[ "$output" =~ "new_user" ]] || false

    # expect default doltcfg directory
    run ls -a
    [[ "$output" =~ ".doltcfg" ]] || false

    # create new_user
    run dolt sql --privilege-file=privs.db <<< "create user new_user"
    [ "$status" -eq 0 ]

    # show users, expect root user and new_user
    run dolt sql --privilege-file=privs.db <<< "select user from mysql.user;"
    [[ "$output" =~ "root" ]] || false
    [[ "$output" =~ "new_user" ]] || false

    # expect custom privilege file current directory
    run ls
    [[ "$output" =~ "privs.db" ]] || false

    # expect to not see new_user when privs.db not specified
    run dolt sql <<< "select user from mysql.user"
    [[ "$output" =~ "root" ]] || false
    ! [[ "$output" =~ "new_user" ]] || false

    # remove config files
    rm -rf .doltcfg
    rm -f privs.db
}

@test "sql-shell: specify data-dir and doltcfg-dir" {
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
    run dolt sql --data-dir=db_dir --doltcfg-dir=doltcfgdir <<< "show databases;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "db1" ]] || false
    [[ "$output" =~ "db2" ]] || false
    [[ "$output" =~ "db3" ]] || false

    # show users, expect just root user
    run dolt sql --data-dir=db_dir --doltcfg-dir=doltcfgdir <<< "select user from mysql.user;"
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
    run dolt sql --data-dir=db_dir --doltcfg-dir=doltcfgdir <<< "create user new_user"
    [ "$status" -eq 0 ]

    # show users, expect root user and new_user
    run dolt sql --data-dir=db_dir --doltcfg-dir=doltcfgdir <<< "select user from mysql.user;"
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
    run dolt sql <<< "show databases;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "db1" ]] || false
    [[ "$output" =~ "db2" ]] || false
    [[ "$output" =~ "db3" ]] || false

    # show users, expect root
    run dolt sql <<< "select user from mysql.user"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    ! [[ "$output" =~ "new_user" ]] || false

    # show users, expect root and new_user
    run dolt sql --doltcfg-dir=../doltcfgdir <<< "select user from mysql.user"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    [[ "$output" =~ "new_user" ]] || false

    cd ..

    # remove config files
    rm -rf .doltcfg
    rm -rf db_dir
    rm -rf doltcfgdir
}

@test "sql-shell: specify data-dir and privilege-file" {
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
    run dolt sql --data-dir=db_dir --privilege-file=privs.db <<< "show databases;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "db1" ]] || false
    [[ "$output" =~ "db2" ]] || false
    [[ "$output" =~ "db3" ]] || false

    # show users, expect just root user
    run dolt sql --data-dir=db_dir --privilege-file=privs.db <<< "select user from mysql.user;"
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
    run dolt sql --data-dir=db_dir --privilege-file=privs.db <<< "create user new_user"
    [ "$status" -eq 0 ]

    # show users, expect root user and new_user
    run dolt sql --data-dir=db_dir --privilege-file=privs.db <<< "select user from mysql.user;"
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
    run dolt sql <<< "show databases;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "db1" ]] || false
    [[ "$output" =~ "db2" ]] || false
    [[ "$output" =~ "db3" ]] || false

    # show users, expect root
    run dolt sql <<< "select user from mysql.user"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    ! [[ "$output" =~ "new_user" ]] || false

    # show users, expect root and new_user
    run dolt sql --privilege-file=../privs.db <<< "select user from mysql.user"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    [[ "$output" =~ "new_user" ]] || false

    cd ..

    # remove config files
    rm -rf .doltcfg
    rm -rf db_dir
    rm -rf privs.db
}

@test "sql-shell: specify doltcfg-dir and privilege-file" {
    # remove any previous config directories
    rm -rf .doltcfg
    rm -rf doltcfgdir
    rm -rf privs.db

    # show users, expect just root user
    run dolt sql --doltcfg-dir=doltcfgdir --privilege-file=privs.db <<< "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    ! [[ "$output" =~ "new_user" ]] || false

    # expect custom doltcfgdir
    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    [[ "$output" =~ "doltcfgdir" ]] || false

    # create new_user
    run dolt sql --doltcfg-dir=doltcfgdir --privilege-file=privs.db <<< "create user new_user"
    [ "$status" -eq 0 ]

    # show users, expect root user and new_user
    run dolt sql --doltcfg-dir=doltcfgdir --privilege-file=privs.db <<< "select user from mysql.user;"
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

@test "sql-shell: specify data directory, cfg directory, and privilege file" {
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
    run dolt sql --data-dir=db_dir --doltcfg-dir=doltcfgdir --privilege-file=privs.db <<< "show databases;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "db1" ]] || false
    [[ "$output" =~ "db2" ]] || false
    [[ "$output" =~ "db3" ]] || false

    # show users, expect just root user
    run dolt sql --data-dir=db_dir --doltcfg-dir=doltcfgdir --privilege-file=privs.db <<< "select user from mysql.user;"
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
    run dolt sql --data-dir=db_dir --doltcfg-dir=doltcfgdir --privilege-file=privs.db <<< "create user new_user"
    [ "$status" -eq 0 ]

    # show users, expect root user and new_user
    run dolt sql --data-dir=db_dir --doltcfg-dir=doltcfgdir --privilege-file=privs.db <<< "select user from mysql.user;"
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
    run dolt sql <<< "show databases;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "db1" ]] || false
    [[ "$output" =~ "db2" ]] || false
    [[ "$output" =~ "db3" ]] || false

    # show users, expect root
    run dolt sql <<< "select user from mysql.user"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    ! [[ "$output" =~ "new_user" ]] || false

    # show users, expect root and new_user
    run dolt sql --doltcfg-dir=../doltcfgdir <<< "select user from mysql.user"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    ! [[ "$output" =~ "new_user" ]] || false

    # show users, expect root and new_user
    run dolt sql --privilege-file=../privs.db <<< "select user from mysql.user"
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


@test "sql-shell: .doltcfg in parent directory errors" {
    # remove existing directories
    rm -rf .doltcfg
    rm -rf inner_db

    mkdir .doltcfg
    mkdir inner_db
    cd inner_db
    mkdir .doltcfg

    run dolt sql <<< "show databases;"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "multiple .doltcfg directories detected" ]] || false

    # specifying datadir, resolves issue
    run dolt sql --data-dir=. <<< "show databases;"
    [ "$status" -eq 0 ]

    # remove existing directories
    rm -rf .doltcfg
    rm -rf inner_db
}

@test "sql-shell: specify data directory outside of dolt repo" {
    # remove files
    rm -rf datadir
    rm -rf .doltcfg
    rm -rf new_repo

    # initialize data directory and inner dbs
    mkdir datadir
    cd datadir

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

    # save data path
    DATADIR=$(pwd)

    cd ..

    # initialize new repo
    mkdir new_repo
    cd new_repo

    run dolt sql --data-dir=$DATADIR <<< "show databases"
    [ $status -eq 0 ]
    [[ $output =~ "db1" ]] || false
    [[ $output =~ "db2" ]] || false
    [[ $output =~ "db3" ]] || false

    run dolt sql --data-dir=$DATADIR <<< "create user new_user"
    [ $status -eq 0 ]

    run dolt sql --data-dir=$DATADIR <<< "use db1; select user from mysql.user"
    [ $status -eq 0 ]
    [[ $output =~ "new_user" ]] || false

    run dolt sql --data-dir=$DATADIR <<< "use db2; select user from mysql.user"
    [ $status -eq 0 ]
    [[ $output =~ "new_user" ]] || false

    run dolt sql --data-dir=$DATADIR <<< "use db3; select user from mysql.user"
    [ $status -eq 0 ]
    [[ $output =~ "new_user" ]] || false

    # check that correct files exist
    cd ..

    run ls -a
    [[ $output =~ "datadir" ]] || false
    [[ $output =~ "new_repo" ]] || false
    ! [[ $output =~ ".doltcfg" ]] || false

    run ls -a datadir
    [[ $output =~ ".doltcfg" ]] || false

    run ls -a datadir/.doltcfg
    [[ $output =~ "privileges.db" ]] || false

    # remove files
    rm -rf new_repo
    rm -rf datadir
}

@test "sql-shell: bad sql in sql shell should error" {
    run dolt sql <<< "This is bad sql"
    [ $status -eq 1 ]
    run dolt sql <<< "select * from test; This is bad sql; insert into test (pk) values (666); select * from test;"
    [ $status -eq 1 ]
    [[ ! "$output" =~ "666" ]] || false
}

@test "sql-shell: inline query with missing -q flag should error" {
    run dolt sql "SELECT * FROM test;"
    [ $status -eq 1 ]
    [[ "$output" =~ "Invalid Argument:" ]] || false
}

@test "sql-shell: validate string formatting" {
      dolt sql <<SQL
CREATE TABLE test2 (
  str varchar(256) NOT NULL,
  PRIMARY KEY (str)
);
SQL
  dolt add .
  dolt commit -m "created table"

  TESTSTR='0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ`~!@#$%^&*()){}[]/=?+|,.<>;:_-_%d%s%f'
  dolt sql -q "INSERT INTO test2 (str) VALUES ('$TESTSTR')"

  run dolt sql -q "SELECT * FROM test2"
  [ $status -eq 0 ]
  [[ "$output" =~ "$TESTSTR" ]] || false

  run dolt sql -q "SELECT * FROM test2" -r csv
  [ $status -eq 0 ]
  [[ "$output" =~ "$TESTSTR" ]] || false

  run dolt sql -q "SELECT * FROM test2" -r json
  [ $status -eq 0 ]
  [[ "$output" =~ "$TESTSTR" ]] || false

  dolt add .
  dolt commit -m "added data"

  run dolt diff HEAD^
  [ $status -eq 0 ]
  echo $output
  [[ "$output" =~ "$TESTSTR" ]] || false
}

@test "sql-shell: active branch after checkout" {
    run dolt sql <<< "select active_branch()"
    [ $status -eq 0 ]
    [[ "$output" =~ "active_branch()" ]] || false
    [[ "$output" =~ "main" ]] || false
    run dolt sql <<< "select dolt_checkout('-b', 'tmp_br') as co; select active_branch()"
    [ $status -eq 0 ]
    [[ "$output" =~ "active_branch()" ]] || false
    [[ "$output" =~ "tmp_br" ]] || false
}
