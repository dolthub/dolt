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

@test "sql-shell: --user option changes superuser" {
    # remove config
    rm -rf .doltcfg

    # default is root@localhost
    run dolt sql <<< "select user, host from mysql.user"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    ! [[ "$output" =~ "dolt" ]] || false
    [[ "$output" =~ "localhost" ]] || false

    # make it dolt@localhost
    run dolt --user=dolt sql <<< "select user, host from mysql.user"
    [ "$status" -eq 0 ]
    ! [[ "$output" =~ "root" ]] || false
    [[ "$output" =~ "dolt" ]] || false
    [[ "$output" =~ "localhost" ]] || false

    # remove config
    rm -rf .doltcfg
}

@test "sql-shell: use user without privileges, and no superuser created" {
    rm -rf .doltcfg

    # default user is root
    run dolt sql <<< "select user from mysql.user"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]]

    # create user
    run dolt sql <<< "create user new_user@'localhost'"
    [ "$status" -eq 0 ]

    run dolt sql --user=new_user <<< "select user from mysql.user"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Access denied for user" ]]

    rm -rf .doltcfg
}

@test "sql-shell: pipe query text to sql shell" {
    skiponwindows "Works on Windows command prompt but not the WSL terminal used during bats"
    run bash -c "echo 'show tables' | dolt sql"
    [ $status -eq 0 ]
    [[ "$output" =~ "test" ]] || false
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

@test "sql-shell: shell works after failing query" {
    skiponwindows "Need to install expect and make this script work on windows."
    $BATS_TEST_DIRNAME/sql-works-after-failing-query.expect
}

@test "sql-shell: delimiter" {
    skiponwindows "Need to install expect and make this script work on windows."
    mkdir doltsql
    cd doltsql
    dolt init

    run $BATS_TEST_DIRNAME/sql-delimiter.expect
    [ "$status" -eq "0" ]
    [[ ! "$output" =~ "Error" ]] || false
    [[ ! "$output" =~ "error" ]] || false

    run dolt sql -q "SELECT * FROM test ORDER BY 1" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "0,0" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false

    run dolt sql -q "SHOW TRIGGERS"
    [ "$status" -eq "0" ]
    [[ "$output" =~ "SET NEW.v1 = NEW.v1 * 11" ]] || false

    cd ..
    rm -rf doltsql
}

@test "sql-shell: use databases" {
    skiponwindows "Need to install expect and make this script work on windows."
    mkdir doltsql
    cd doltsql
    dolt init
    dolt sql -q "create database db1"
    dolt sql -q "create database db2"

    dolt branch test

    run expect $BATS_TEST_DIRNAME/sql-use.expect
    echo $output
    
    [ "$status" -eq "0" ]
    [[ ! "$output" =~ "Error" ]] || false
    [[ ! "$output" =~ "error" ]] || false

    cd ..
    rm -rf doltsql
}

@test "sql-shell: default datadir, doltcfg, and privs" {
    # remove config files
    rm -rf .doltcfg

    run dolt sql <<< "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    ! [[ "$output" =~ "new_user" ]] || false

    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false

    run ls .doltcfg
    ! [[ "$output" =~ "privileges.db" ]] || false

    run dolt sql <<< "create user new_user"
    [ "$status" -eq 0 ]

    run dolt sql <<< "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    [[ "$output" =~ "new_user" ]] || false

    run ls -a
    [[ "$output" =~ ".doltcfg" ]] || false

    run ls .doltcfg
    [[ "$output" =~ "privileges.db" ]] || false

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
    run dolt --data-dir=db_dir sql <<< "show databases;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "db1" ]] || false
    [[ "$output" =~ "db2" ]] || false
    [[ "$output" =~ "db3" ]] || false

    # show users, expect just root user
    run dolt --data-dir=db_dir sql <<< "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    ! [[ "$output" =~ "new_user" ]] || false

    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false

    run ls -a db_dir
    ! [[ "$output" =~ ".doltcfg" ]] || false

    # create new user
    run dolt --data-dir=db_dir sql <<< "create user new_user"
    [ "$status" -eq 0 ]

    # show users, expect root user and new_user
    run dolt --data-dir=db_dir sql <<< "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    [[ "$output" =~ "new_user" ]] || false

    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    ! [[ "$output" =~ "privileges.db" ]] || false

    run ls -a db_dir
    [[ "$output" =~ ".doltcfg" ]] || false
    ! [[ "$output" =~ "privileges.db" ]] || false

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
    run dolt --doltcfg-dir=doltcfgdir sql <<< "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    ! [[ "$output" =~ "new_user" ]] || false

    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    ! [[ "$output" =~ "doltcfgdir" ]] || false

    # create new_user
    run dolt --doltcfg-dir=doltcfgdir sql <<< "create user new_user"
    [ "$status" -eq 0 ]

    # show users, expect root user and new_user
    run dolt --doltcfg-dir=doltcfgdir sql <<< "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    [[ "$output" =~ "new_user" ]] || false

    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    [[ "$output" =~ "doltcfgdir" ]] || false

    run ls doltcfgdir
    [[ "$output" =~ "privileges.db" ]] || false

    # remove files
    rm -rf .doltcfg
    rm -rf doltcfgdir
}

@test "sql-shell: specify privilege file" {
    # remove config files
    rm -rf .doltcfg
    rm -f privs.db

    # show users, expect just root user
    run dolt --privilege-file=privs.db sql <<< "select user from mysql.user;"
    [[ "$output" =~ "root" ]] || false
    ! [[ "$output" =~ "new_user" ]] || false

    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    ! [[ "$output" =~ "privs.db" ]] || false

    # create new_user
    run dolt --privilege-file=privs.db sql <<< "create user new_user"
    [ "$status" -eq 0 ]

    # show users, expect root user and new_user
    run dolt --privilege-file=privs.db sql <<< "select user from mysql.user;"
    [[ "$output" =~ "root" ]] || false
    [[ "$output" =~ "new_user" ]] || false

    run ls -a
    [[ "$output" =~ ".doltcfg" ]] || false
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
    run dolt --data-dir=db_dir --doltcfg-dir=doltcfgdir sql <<< "show databases;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "db1" ]] || false
    [[ "$output" =~ "db2" ]] || false
    [[ "$output" =~ "db3" ]] || false

    # show users, expect just root user
    run dolt --data-dir=db_dir --doltcfg-dir=doltcfgdir sql <<< "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    ! [[ "$output" =~ "new_user" ]] || false

    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    ! [[ "$output" =~ "doltcfgdir" ]] || false

    run ls -a db_dir
    ! [[ "$output" =~ ".doltcfg" ]] || false

    # create new user
    run dolt --data-dir=db_dir --doltcfg-dir=doltcfgdir sql <<< "create user new_user"
    [ "$status" -eq 0 ]

    # show users, expect root user and new_user
    run dolt --data-dir=db_dir --doltcfg-dir=doltcfgdir sql <<< "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    [[ "$output" =~ "new_user" ]] || false

    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    [[ "$output" =~ "doltcfgdir" ]] || false
    ! [[ "$output" =~ "privileges.db" ]] || false

    run ls db_dir
    ! [[ "$output" =~ "privileges.db" ]] || false

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
    run dolt --doltcfg-dir=../doltcfgdir sql <<< "select user from mysql.user"
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
    run dolt --data-dir=db_dir --privilege-file=privs.db sql <<< "show databases;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "db1" ]] || false
    [[ "$output" =~ "db2" ]] || false
    [[ "$output" =~ "db3" ]] || false

    # show users, expect just root user
    run dolt --data-dir=db_dir --privilege-file=privs.db sql <<< "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    ! [[ "$output" =~ "new_user" ]] || false

    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false

    run ls -a db_dir
    ! [[ "$output" =~ ".doltcfg" ]] || false

    # create new user
    run dolt --data-dir=db_dir --privilege-file=privs.db sql <<< "create user new_user"
    [ "$status" -eq 0 ]

    # show users, expect root user and new_user
    run dolt --data-dir=db_dir --privilege-file=privs.db sql <<< "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    [[ "$output" =~ "new_user" ]] || false

    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    [[ "$output" =~ "privs.db" ]] || false

    run ls -a db_dir
    [[ "$output" =~ ".doltcfg" ]] || false
    ! [[ "$output" =~ "privs.db" ]] || false

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
    run dolt --privilege-file=../privs.db sql <<< "select user from mysql.user"
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
    run dolt --doltcfg-dir=doltcfgdir --privilege-file=privs.db sql <<< "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    ! [[ "$output" =~ "new_user" ]] || false

    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    ! [[ "$output" =~ "doltcfgdir" ]] || false

    # create new_user
    run dolt --doltcfg-dir=doltcfgdir --privilege-file=privs.db sql <<< "create user new_user"
    [ "$status" -eq 0 ]

    # show users, expect root user and new_user
    run dolt --doltcfg-dir=doltcfgdir --privilege-file=privs.db sql <<< "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    [[ "$output" =~ "new_user" ]] || false

    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    [[ "$output" =~ "doltcfgdir" ]] || false
    [[ "$output" =~ "privs.db" ]] || false

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
    run dolt --data-dir=db_dir --doltcfg-dir=doltcfgdir --privilege-file=privs.db sql <<< "show databases;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "db1" ]] || false
    [[ "$output" =~ "db2" ]] || false
    [[ "$output" =~ "db3" ]] || false

    # show users, expect just root user
    run dolt --data-dir=db_dir --doltcfg-dir=doltcfgdir --privilege-file=privs.db sql <<< "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    ! [[ "$output" =~ "new_user" ]] || false

    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    ! [[ "$output" =~ "doltcfgdir" ]] || false

    run ls -a db_dir
    ! [[ "$output" =~ ".doltcfg" ]] || false

    # create new user
    run dolt --data-dir=db_dir --doltcfg-dir=doltcfgdir --privilege-file=privs.db sql <<< "create user new_user"
    [ "$status" -eq 0 ]

    # show users, expect root user and new_user
    run dolt --data-dir=db_dir --doltcfg-dir=doltcfgdir --privilege-file=privs.db sql <<< "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    [[ "$output" =~ "new_user" ]] || false

    run ls -a
    ! [[ "$output" =~ "privileges.db" ]] || false
    [[ "$output" =~ "privs.db" ]] || false
    ! [[ "$output" =~ ".doltcfg" ]] || false
    [[ "$output" =~ "doltcfgdir" ]] || false

    run ls db_dir
    ! [[ "$output" =~ "privileges.db" ]] || false
    ! [[ "$output" =~ "privs.db" ]] || false

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
    run dolt --doltcfg-dir=../doltcfgdir sql <<< "select user from mysql.user"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    ! [[ "$output" =~ "new_user" ]] || false

    # show users, expect root and new_user
    run dolt --privilege-file=../privs.db sql <<< "select user from mysql.user"
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
    run dolt --data-dir=. sql <<< "show databases;"
    [ "$status" -eq 0 ]

    # remove existing directories
    rm -rf .doltcfg
    rm -rf inner_db
}

@test "sql-shell: .doltcfg defaults to parent directory" {
    # remove existing directories
    rm -rf .doltcfg
    rm -rf inner_db

    # create user in parent
    run dolt sql <<< "create user new_user"
    [ "$status" -eq 0 ]

    run dolt sql <<< "select user from mysql.user"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    [[ "$output" =~ "new_user" ]] || false

    # check that .doltcfg and privileges.db was created
    run ls -a
    [[ "$output" =~ ".doltcfg" ]] || false
    run ls .doltcfg
    [[ "$output" =~ "privileges.db" ]] || false

    mkdir inner_db
    cd inner_db
    run dolt sql <<< "select user from mysql.user"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    [[ "$output" =~ "new_user" ]] || false

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

    run dolt --data-dir=$DATADIR sql <<< "show databases"
    [ $status -eq 0 ]
    [[ $output =~ "db1" ]] || false
    [[ $output =~ "db2" ]] || false
    [[ $output =~ "db3" ]] || false

    run dolt --data-dir=$DATADIR sql <<< "create user new_user"
    [ $status -eq 0 ]

    run dolt --data-dir=$DATADIR sql <<< "use db1; select user from mysql.user"
    [ $status -eq 0 ]
    [[ $output =~ "new_user" ]] || false

    run dolt --data-dir=$DATADIR sql <<< "use db2; select user from mysql.user"
    [ $status -eq 0 ]
    [[ $output =~ "new_user" ]] || false

    run dolt --data-dir=$DATADIR sql <<< "use db3; select user from mysql.user"
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
    [[ "$output" =~ "does not take positional arguments, but found 1" ]] || false
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

  dolt sql -q "SELECT * FROM test2" -r json
  run dolt sql -q "SELECT * FROM test2" -r json
  [ $status -eq 0 ]
  # The golang json encoder escapes < and > and & for HTML compatibility
  JSON_TESTSTR='0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ`~!@#$%^\u0026*()){}[]/=?+|,.\u003c\u003e;:_-_%d%s%f'
  [[ "$output" =~ "$JSON_TESTSTR" ]] || false

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
    run dolt sql <<< "call dolt_checkout('-b', 'tmp_br'); select active_branch()"
    [ $status -eq 0 ]
    [[ "$output" =~ "active_branch()" ]] || false
    [[ "$output" =~ "tmp_br" ]] || false
}
