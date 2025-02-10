#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    export DOLT_DBNAME_REPLACE="true"
    dolt sql <<SQL
CREATE TABLE one_pk (
  pk BIGINT NOT NULL,
  c1 BIGINT,
  c2 BIGINT,
  c3 BIGINT,
  c4 BIGINT,
  c5 BIGINT,
  PRIMARY KEY (pk)
);
CREATE TABLE two_pk (
  pk1 BIGINT NOT NULL,
  pk2 BIGINT NOT NULL,
  c1 BIGINT,
  c2 BIGINT,
  c3 BIGINT,
  c4 BIGINT,
  c5 BIGINT,
  PRIMARY KEY (pk1,pk2)
);
CREATE TABLE has_datetimes (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  date_created DATETIME COMMENT 'tag:1',
  PRIMARY KEY (pk)
);
INSERT INTO one_pk (pk,c1,c2,c3,c4,c5) VALUES (0,0,0,0,0,0),(1,10,10,10,10,10),(2,20,20,20,20,20),(3,30,30,30,30,30);
INSERT INTO two_pk (pk1,pk2,c1,c2,c3,c4,c5) VALUES (0,0,0,0,0,0,0),(0,1,10,10,10,10,10),(1,0,20,20,20,20,20),(1,1,30,30,30,30,30);
INSERT INTO has_datetimes (pk, date_created) VALUES (0, '2020-02-17 00:00:00');
SQL
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "sql: --user don't create superuser if using an existing user" {
    rm -rf .doltcfg

    # default user is root
    run dolt sql -q "select user from mysql.user"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false

    # create user
    run dolt sql -q "create user new_user@'localhost'"
    [ "$status" -eq 0 ]

    run dolt --user=new_user sql -q "select user from mysql.user"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Access denied for user" ]] || false

    rm -rf .doltcfg
}

@test "sql: check configurations with all default options" {
    # remove any previous config directories
    rm -rf .doltcfg

    # show users, expect just root user
    run dolt sql -q "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    ! [[ "$output" =~ "new_user" ]] || false

    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false

    run ls .doltcfg
    ! [[ "$output" =~ "privileges.db" ]] || false

    # create new_user
    run dolt sql -q "create user new_user"
    [ "$status" -eq 0 ]

    # show users, expect root user and new_user
    run dolt sql -q "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    [[ "$output" =~ "new_user" ]] || false

    run ls -a
    [[ "$output" =~ ".doltcfg" ]] || false

    run ls .doltcfg
    [[ "$output" =~ "privileges.db" ]] || false

    rm -rf .doltcfg
}

@test "sql: check configurations specify data directory" {
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
    run dolt --data-dir=db_dir sql -q "show databases;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "db1" ]] || false
    [[ "$output" =~ "db2" ]] || false
    [[ "$output" =~ "db3" ]] || false

    # show users, expect just root user
    run dolt --data-dir=db_dir sql -q "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    ! [[ "$output" =~ "new_user" ]] || false

    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false

    run ls -a db_dir
    ! [[ "$output" =~ ".doltcfg" ]] || false

    # create new user
    run dolt --data-dir=db_dir sql -q "create user new_user"
    [ "$status" -eq 0 ]

    # show users, expect root user and new_user
    run dolt --data-dir=db_dir sql -q "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    [[ "$output" =~ "new_user" ]] || false

    run ls
    ! [[ "$output" =~ "privileges.db" ]] || false

    run ls -a db_dir
    [[ "$output" =~ ".doltcfg" ]] || false
    ! [[ "$output" =~ "privileges.db" ]] || false

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

@test "sql: check configurations specify doltcfg directory" {
    # remove any previous config directories
    rm -rf .doltcfg
    rm -rf doltcfgdir

    # show users, expect just root user
    run dolt --doltcfg-dir=doltcfgdir sql -q "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    ! [[ "$output" =~ "new_user" ]] || false

    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    ! [[ "$output" =~ "doltcfgdir" ]] || false

    # create new_user
    run dolt --doltcfg-dir=doltcfgdir sql -q "create user new_user"
    [ "$status" -eq 0 ]

    # show users, expect root user and new_user
    run dolt --doltcfg-dir=doltcfgdir sql -q "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    [[ "$output" =~ "new_user" ]] || false

    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    [[ "$output" =~ "doltcfgdir" ]] || false

    run ls doltcfgdir
    [[ "$output" =~ "privileges.db" ]] || false

    # remove config directory just in case
    rm -rf .doltcfg
    rm -rf doltcfgdir
}

@test "sql: check configurations specify privilege file" {
    # remove config files
    rm -rf .doltcfg
    rm -f privs.db

    # show users, expect just root user
    run dolt --privilege-file=privs.db sql -q "select user from mysql.user;"
    [[ "$output" =~ "root" ]] || false
    ! [[ "$output" =~ "new_user" ]] || false

    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false

    # create new_user
    run dolt --privilege-file=privs.db sql -q "create user new_user"
    [ "$status" -eq 0 ]

    # show users, expect root user and new_user
    run dolt --privilege-file=privs.db sql -q "select user from mysql.user;"
    [[ "$output" =~ "root" ]] || false
    [[ "$output" =~ "new_user" ]] || false

    run ls -a
    [[ "$output" =~ ".doltcfg" ]] || false
    [[ "$output" =~ "privs.db" ]] || false

    # expect to not see new_user when privs.db not specified
    run dolt sql -q "select user from mysql.user"
    [[ "$output" =~ "root" ]] || false
    ! [[ "$output" =~ "new_user" ]] || false

    # remove config files
    rm -rf .doltcfg
    rm -f privs.db
}

@test "sql: check configurations specify data directory and doltcfg directory" {
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
    run dolt --data-dir=db_dir --doltcfg-dir=doltcfgdir sql -q "show databases;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "db1" ]] || false
    [[ "$output" =~ "db2" ]] || false
    [[ "$output" =~ "db3" ]] || false

    # show users, expect just root user
    run dolt --data-dir=db_dir --doltcfg-dir=doltcfgdir sql -q "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    ! [[ "$output" =~ "new_user" ]] || false

    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    ! [[ "$output" =~ "doltcfgdir" ]] || false

    run ls -a db_dir
    ! [[ "$output" =~ ".doltcfg" ]] || false

    # create new user
    run dolt --data-dir=db_dir --doltcfg-dir=doltcfgdir sql -q "create user new_user"
    [ "$status" -eq 0 ]

    # show users, expect root user and new_user
    run dolt --data-dir=db_dir --doltcfg-dir=doltcfgdir sql -q "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    [[ "$output" =~ "new_user" ]] || false

    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    [[ "$output" =~ "doltcfgdir" ]] || false
    ! [[ "$output" =~ "privileges.db" ]] || false

    run ls db_dir
    ! [[ "$output" =~ ".doltcfg" ]] || false
    ! [[ "$output" =~ "privileges.db" ]] || false

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
    run dolt --doltcfg-dir=../doltcfgdir sql -q "select user from mysql.user"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    [[ "$output" =~ "new_user" ]] || false

    cd ..

    # remove config files
    rm -rf .doltcfg
    rm -rf db_dir
    rm -rf doltcfgdir
}

@test "sql: check configurations specify data directory and privilege file" {
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
    run dolt --data-dir=db_dir --privilege-file=privs.db sql -q "show databases;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "db1" ]] || false
    [[ "$output" =~ "db2" ]] || false
    [[ "$output" =~ "db3" ]] || false

    # show users, expect just root user
    run dolt --data-dir=db_dir --privilege-file=privs.db sql -q "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    ! [[ "$output" =~ "new_user" ]] || false

    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false

    run ls -a db_dir
    ! [[ "$output" =~ ".doltcfg" ]] || false

    # create new user
    run dolt --data-dir=db_dir --privilege-file=privs.db sql -q "create user new_user"
    [ "$status" -eq 0 ]

    # show users, expect root user and new_user
    run dolt --data-dir=db_dir --privilege-file=privs.db sql -q "select user from mysql.user;"
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
    run dolt --privilege-file=../privs.db sql -q "select user from mysql.user"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    [[ "$output" =~ "new_user" ]] || false

    cd ..

    # remove config files
    rm -rf .doltcfg
    rm -rf db_dir
    rm -rf privs.db
}

@test "sql: check configurations specify doltcfg directory and privilege file" {
    # remove any previous config directories
    rm -rf .doltcfg
    rm -rf doltcfgdir
    rm -rf privs.db

    # show users, expect just root user
    run dolt --doltcfg-dir=doltcfgdir --privilege-file=privs.db sql -q "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    ! [[ "$output" =~ "new_user" ]] || false

    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    ! [[ "$output" =~ "doltcfgdir" ]] || false

    # create new_user
    run dolt --doltcfg-dir=doltcfgdir --privilege-file=privs.db sql -q "create user new_user"
    [ "$status" -eq 0 ]

    # show users, expect root user and new_user
    run dolt --doltcfg-dir=doltcfgdir --privilege-file=privs.db sql -q "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    [[ "$output" =~ "new_user" ]] || false

    # expect privileges file
    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    [[ "$output" =~ "doltcfgdir" ]] || false
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

@test "sql: check configurations specify data directory, doltcfg directory, and privilege file" {
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
    run dolt --data-dir=db_dir --doltcfg-dir=doltcfgdir --privilege-file=privs.db sql -q "show databases;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "db1" ]] || false
    [[ "$output" =~ "db2" ]] || false
    [[ "$output" =~ "db3" ]] || false

    # show users, expect just root user
    run dolt --data-dir=db_dir --doltcfg-dir=doltcfgdir --privilege-file=privs.db sql -q "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    ! [[ "$output" =~ "new_user" ]] || false

    # expect custom doltcfg in current directory
    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    ! [[ "$output" =~ "doltcfgdir" ]] || false

    run ls -a db_dir
    ! [[ "$output" =~ ".doltcfg" ]] || false

    # create new user
    run dolt --data-dir=db_dir --doltcfg-dir=doltcfgdir --privilege-file=privs.db sql -q "create user new_user"
    [ "$status" -eq 0 ]

    # show users, expect root user and new_user
    run dolt --data-dir=db_dir --doltcfg-dir=doltcfgdir --privilege-file=privs.db sql -q "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    [[ "$output" =~ "new_user" ]] || false

    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    [[ "$output" =~ "doltcfgdir" ]] || false
    ! [[ "$output" =~ "privileges.db" ]] || false
    [[ "$output" =~ "privs.db" ]] || false

    run ls db_dir
    ! [[ "$output" =~ "privileges.db" ]] || false
    ! [[ "$output" =~ "privs.db" ]] || false

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
    run dolt --doltcfg-dir=../doltcfgdir sql -q "select user from mysql.user"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    ! [[ "$output" =~ "new_user" ]] || false

    # show users, expect root and new_user
    run dolt --privilege-file=../privs.db sql -q "select user from mysql.user"
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

    run dolt --privilege-file=privs.db sql -q "create database inner_db;"
    [ "$status" -eq 0 ]

    run dolt --privilege-file=privs.db sql -q "create user new_user;"
    [ "$status" -eq 0 ]

    run dolt --privilege-file=privs.db sql -q "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    [[ "$output" =~ "new_user" ]] || false

    cd inner_db

    run dolt --privilege-file=../privs.db sql -q "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    [[ "$output" =~ "new_user" ]] || false

    cd ..

    # remove existing directories
    rm -rf .doltcfg
    rm -rf inner_db
    rm -f privs.db
}

@test "sql: dolt sql -q .doltcfg in parent directory errors" {
    # remove existing directories
    rm -rf .doltcfg
    rm -rf inner_db

    mkdir .doltcfg
    mkdir inner_db
    cd inner_db
    mkdir .doltcfg

    run dolt sql -q "show databases;"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "multiple .doltcfg directories detected" ]] || false

    # specifying datadir, resolves issue
    run dolt --data-dir=. sql -q "show databases;"
    [ "$status" -eq 0 ]

    # remove existing directories
    rm -rf .doltcfg
    rm -rf inner_db
}

@test "sql: .doltcfg defaults to parent directory" {
    # remove existing directories
    rm -rf .doltcfg
    rm -rf inner_db

    # create user in parent
    run dolt sql -q "create user new_user"
    [ "$status" -eq 0 ]

    run dolt sql -q "select user from mysql.user"
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
    run dolt sql -q "select user from mysql.user"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    [[ "$output" =~ "new_user" ]] || false

    # remove existing directories
    rm -rf .doltcfg
    rm -rf inner_db
}

@test "sql: dolt sql -q specify data directory outside of dolt repo" {
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

    run dolt --data-dir=$DATADIR sql -q "show databases"
    [ $status -eq 0 ]
    [[ $output =~ "db1" ]] || false
    [[ $output =~ "db2" ]] || false
    [[ $output =~ "db3" ]] || false

    run dolt --data-dir=$DATADIR sql -q "create user new_user"
    [ $status -eq 0 ]

    run dolt --data-dir=$DATADIR sql -q "use db1; select user from mysql.user"
    [ $status -eq 0 ]
    [[ $output =~ "new_user" ]] || false

    run dolt --data-dir=$DATADIR sql -q "use db2; select user from mysql.user"
    [ $status -eq 0 ]
    [[ $output =~ "new_user" ]] || false

    run dolt --data-dir=$DATADIR sql -q "use db3; select user from mysql.user"
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

@test "sql: errors do not write incomplete rows" {
    dolt sql <<"SQL"
CREATE TABLE test (
    pk BIGINT PRIMARY KEY,
    v1 BIGINT,
    INDEX (v1)
);
INSERT INTO test VALUES (1,1), (4,4), (5,5);
SQL
    run dolt sql -q "INSERT INTO test VALUES (2,2), (3,3), (1,1);"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "duplicate" ]] || false
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "4,4" ]] || false
    [[ "$output" =~ "5,5" ]] || false
    [[ ! "$output" =~ "2,2" ]] || false
    [[ ! "$output" =~ "3,3" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
    run dolt sql -q "UPDATE test SET pk = pk + 1;"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "duplicate" ]] || false
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "4,4" ]] || false
    [[ "$output" =~ "5,5" ]] || false
    [[ ! "$output" =~ "2,2" ]] || false
    [[ ! "$output" =~ "3,3" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false

    dolt sql <<"SQL"
CREATE TABLE test2 (
    pk BIGINT PRIMARY KEY,
    CONSTRAINT fk_test FOREIGN KEY (pk) REFERENCES test (v1)
);
INSERT INTO test2 VALUES (4);
SQL
    run dolt sql -q "DELETE FROM test WHERE pk > 0;"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "violation" ]] || false
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "4,4" ]] || false
    [[ "$output" =~ "5,5" ]] || false
    [[ ! "$output" =~ "2,2" ]] || false
    [[ ! "$output" =~ "3,3" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
    run dolt sql -q "SELECT * FROM test2" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk" ]] || false
    [[ "$output" =~ "4" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "REPLACE INTO test VALUES (1,7), (4,8), (5,9);"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "violation" ]] || false
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "4,4" ]] || false
    [[ "$output" =~ "5,5" ]] || false
    [[ ! "$output" =~ "2,2" ]] || false
    [[ ! "$output" =~ "3,3" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
    run dolt sql -q "SELECT * FROM test2" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk" ]] || false
    [[ "$output" =~ "4" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
}

@test "sql: select from multiple tables" {
    run dolt sql -q "select pk,pk1,pk2 from one_pk,two_pk"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 20 ]
    run dolt sql -q "select pk,pk1,pk2 from one_pk,two_pk where one_pk.c1=two_pk.c1"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 8 ]
    run dolt sql -q "select pk,pk1,pk2,one_pk.c1 as foo,two_pk.c1 as bar from one_pk,two_pk where one_pk.c1=two_pk.c1"
    [ "$status" -eq 0 ]
    [[ "$output" =~ foo ]] || false
    [[ "$output" =~ bar ]] || false
    [ "${#lines[@]}" -eq 8 ]

    # check information_schema.STATISTICS table
    # TODO: caridnality here are all 0's as it's not supported yet
    run dolt sql -q "select * from information_schema.STATISTICS;" -r csv
    [[ "$output" =~ "has_datetimes,0,dolt_repo_$$,PRIMARY,1,pk,A,0,,,\"\",BTREE,\"\",\"\",YES," ]] || false
    [[ "$output" =~ "one_pk,0,dolt_repo_$$,PRIMARY,1,pk,A,0,,,\"\",BTREE,\"\",\"\",YES," ]] || false
    [[ "$output" =~ "two_pk,0,dolt_repo_$$,PRIMARY,1,pk1,A,0,,,\"\",BTREE,\"\",\"\",YES," ]] || false
    [[ "$output" =~ "two_pk,0,dolt_repo_$$,PRIMARY,2,pk2,A,0,,,\"\",BTREE,\"\",\"\",YES," ]] || false

    skip "ALTER VIEW is unsupported"
    # check cardinality on information_schema.STATISTICS table
    run dolt sql -q "select table_name, column_name, cardinality from information_schema.STATISTICS;" -r csv
    [[ "$output" =~ "has_datetimes,pk,1" ]] || false
    [[ "$output" =~ "one_pk,pk,4" ]] || false
    [[ "$output" =~ "two_pk,pk1,2" ]] || false
    [[ "$output" =~ "two_pk,pk2,4" ]] || false
}

@test "sql: AS OF queries" {
    dolt add .
    dolt commit -m "Initial main commit" --date "2020-03-01T12:00:00Z"

    main_commit=`dolt log | head -n1 | cut -d' ' -f2`
    dolt sql -q "update one_pk set c1 = c1 + 1"
    dolt sql -q "drop table two_pk"
    dolt checkout -b new_branch
    dolt add .
    dolt commit -m "Updated a table, dropped a table" --date "2020-03-01T13:00:00Z"
    new_commit=`dolt log | head -n1 | cut -d' ' -f2`

    run dolt sql -r csv -q "select pk,c1 from one_pk order by c1"
    [ $status -eq 0 ]
    [[ "$output" =~ "0,1" ]] || false
    [[ "$output" =~ "1,11" ]] || false
    [[ "$output" =~ "2,21" ]] || false
    [[ "$output" =~ "3,31" ]] || false

    run dolt sql -r csv -q "select pk,c1 from one_pk as of 'main' order by c1"
    [ $status -eq 0 ]
    [[ "$output" =~ "0,0" ]] || false
    [[ "$output" =~ "1,10" ]] || false
    [[ "$output" =~ "2,20" ]] || false
    [[ "$output" =~ "3,30" ]] || false

    run dolt sql -r csv -q "select pk,c1 from one_pk as of '$main_commit' order by c1"
    [ $status -eq 0 ]
    [[ "$output" =~ "0,0" ]] || false
    [[ "$output" =~ "1,10" ]] || false
    [[ "$output" =~ "2,20" ]] || false
    [[ "$output" =~ "3,30" ]] || false

    run dolt sql -r csv -q "select count(*) from two_pk as of 'main'"
    [ $status -eq 0 ]
    [[ "$output" =~ "4" ]] || false

    run dolt sql -r csv -q "select count(*) from two_pk as of '$main_commit'"
    [ $status -eq 0 ]
    [[ "$output" =~ "4" ]] || false

    run dolt sql -r csv -q "select pk,c1 from one_pk as of 'HEAD~' order by c1"
    [ $status -eq 0 ]
    [[ "$output" =~ "0,0" ]] || false
    [[ "$output" =~ "1,10" ]] || false
    [[ "$output" =~ "2,20" ]] || false
    [[ "$output" =~ "3,30" ]] || false

    run dolt sql -r csv -q "select pk,c1 from one_pk as of 'new_branch^' order by c1"
    [ $status -eq 0 ]
    [[ "$output" =~ "0,0" ]] || false
    [[ "$output" =~ "1,10" ]] || false
    [[ "$output" =~ "2,20" ]] || false
    [[ "$output" =~ "3,30" ]] || false

    dolt checkout main
    run dolt sql -r csv -q "select pk,c1 from one_pk as of 'new_branch' order by c1"
    [ $status -eq 0 ]
    [[ "$output" =~ "0,1" ]] || false
    [[ "$output" =~ "1,11" ]] || false
    [[ "$output" =~ "2,21" ]] || false
    [[ "$output" =~ "3,31" ]] || false

    run dolt sql -r csv -q "select pk,c1 from one_pk as of '$new_commit' order by c1"
    [ $status -eq 0 ]
    [[ "$output" =~ "0,1" ]] || false
    [[ "$output" =~ "1,11" ]] || false
    [[ "$output" =~ "2,21" ]] || false
    [[ "$output" =~ "3,31" ]] || false

    dolt checkout new_branch
    run dolt sql -r csv -q "select pk,c1 from one_pk as of CONVERT('2020-03-01 12:00:00', DATETIME) order by c1"
    [ $status -eq 0 ]
    [[ "$output" =~ "0,0" ]] || false
    [[ "$output" =~ "1,10" ]] || false
    [[ "$output" =~ "2,20" ]] || false
    [[ "$output" =~ "3,30" ]] || false

    run dolt sql -r csv -q "select pk,c1 from one_pk as of CONVERT('2020-03-01 12:15:00', DATETIME) order by c1"
    [ $status -eq 0 ]
    [[ "$output" =~ "0,0" ]] || false
    [[ "$output" =~ "1,10" ]] || false
    [[ "$output" =~ "2,20" ]] || false
    [[ "$output" =~ "3,30" ]] || false

    run dolt sql -r csv -q "select pk,c1 from one_pk as of CONVERT('2020-03-01 13:00:00', DATETIME) order by c1"
    [ $status -eq 0 ]
    [[ "$output" =~ "0,1" ]] || false
    [[ "$output" =~ "1,11" ]] || false
    [[ "$output" =~ "2,21" ]] || false
    [[ "$output" =~ "3,31" ]] || false

    run dolt sql -r csv -q "select pk,c1 from one_pk as of CONVERT('2020-03-01 13:15:00', DATETIME) order by c1"
    [ $status -eq 0 ]
    [[ "$output" =~ "0,1" ]] || false
    [[ "$output" =~ "1,11" ]] || false
    [[ "$output" =~ "2,21" ]] || false
    [[ "$output" =~ "3,31" ]] || false

    run dolt sql -r csv -q "select pk,c1 from one_pk as of CONVERT('2020-03-01 11:59:59', DATETIME) order by c1"
    [ $status -eq 1 ]
    [[ "$output" =~ "not found" ]] || false
}

@test "sql: output formats" {
    dolt sql <<SQL
    CREATE TABLE test (
    a int primary key,
    b float,
    c varchar(80),
    d datetime
);
SQL
    dolt sql <<SQL
    insert into test values (1, 1.5, "1", "2020-01-01");
    insert into test values (2, 2.5, "2", "2020-02-02");
    insert into test values (3, NULL, "3", "2020-03-03");
    insert into test values (4, 4.5, NULL, "2020-04-04");
    insert into test values (5, 5.5, "5", NULL);
SQL

    run dolt sql -r csv -q "select * from test order by a"
    [ $status -eq 0 ]
    [[ "$output" =~ "a,b,c,d" ]] || false
    [[ "$output" =~ '1,1.5,1,2020-01-01 00:00:00' ]] || false
    [[ "$output" =~ '2,2.5,2,2020-02-02 00:00:00' ]] || false
    [[ "$output" =~ '3,,3,2020-03-03 00:00:00' ]] || false
    [[ "$output" =~ '4,4.5,,2020-04-04 00:00:00' ]] || false
    [[ "$output" =~ '5,5.5,5,' ]] || false
    [ "${#lines[@]}" -eq 6 ]

    run dolt sql -r csv -q "select @@character_set_client"
    [ $status -eq 0 ]
    [[ "$output" =~ "utf8mb4" ]] || false

    run dolt sql -r json -q "select * from test order by a"
    [ $status -eq 0 ]
    [ "$output" == '{"rows": [{"a":1,"b":1.5,"c":"1","d":"2020-01-01 00:00:00"},{"a":2,"b":2.5,"c":"2","d":"2020-02-02 00:00:00"},{"a":3,"c":"3","d":"2020-03-03 00:00:00"},{"a":4,"b":4.5,"d":"2020-04-04 00:00:00"},{"a":5,"b":5.5,"c":"5"}]}' ]

    run dolt sql -r json -q "select @@character_set_client"
    [ $status -eq 0 ]
    [[ "$output" =~ "utf8mb4" ]] || false

    dolt sql -r parquet -q "select * from test order by a" > out.parquet
    run parquet cat out.parquet
    [ "$status" -eq 0 ]
    [[ "$output" =~ '{"a": 1, "b": 1.5, "c": "1", "d": 1577836800000000}' ]] || false
    [[ "$output" =~ '{"a": 2, "b": 2.5, "c": "2", "d": 1580601600000000}' ]] || false
    [[ "$output" =~ '{"a": 3, "b": null, "c": "3", "d": 1583193600000000}' ]] || false
    [[ "$output" =~ '{"a": 4, "b": 4.5, "c": null, "d": 1585958400000000}' ]] || false
    [[ "$output" =~ '{"a": 5, "b": 5.5, "c": "5", "d": null}' ]] || false
    [ "${#lines[@]}" -eq 5 ]

    run dolt sql -r parquet -q "select @@character_set_client"
    [ $status -eq 0 ]
    [[ "$output" =~ "utf8mb4" ]] || false
}

@test "sql: empty output exports properly" {
    dolt sql <<SQL
    CREATE TABLE test (
    a int primary key,
    b float,
    c varchar(80),
    d datetime
);
SQL

    run dolt sql -r json -q "select * from test order by a"
    [ $status -eq 0 ]
    [[ "$output" =~ "{}" ]] || false

    dolt sql -r parquet -q "select * from test order by a" > out.parquet
    run parquet cat out.parquet
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 0 ]
}

@test "sql: parquet file output separates output and status messages" {
    dolt sql <<SQL
    CREATE TABLE test (
    a int primary key,
    b float,
    c varchar(80),
    d datetime
);
    insert into test values (1, 1.5, "1", "2020-01-01");
SQL

    echo "select * from test;" > in.sql

    dolt sql -r parquet -f in.sql > out.parquet 2> out.txt

    run cat out.parquet
    [ $status -eq 0 ]
    [[ ! "$output" =~ "Processed" ]] || false

    run cat out.txt
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Processed" ]] || false
}

@test "sql: output for escaped longtext exports properly" {
 dolt sql <<SQL
    CREATE TABLE test (
    a int primary key,
    v LONGTEXT
);
SQL
dolt sql <<SQL
    insert into test values (1, "{""key"": ""value""}");
    insert into test values (2, """Hello""");
SQL

    run dolt sql -r json -q "select * from test order by a"
    [ $status -eq 0 ]
    [ "$output" == '{"rows": [{"a":1,"v":"{\"key\": \"value\"}"},{"a":2,"v":"\"Hello\""}]}' ]

    run dolt sql -r csv -q "select * from test order by a"
    [ $status -eq 0 ]
    [[ "$output" =~ "a,v" ]] || false
    [[ "$output" =~ '1,"{""key"": ""value""}"' ]] || false
    [[ "$output" =~ '2,"""Hello"""' ]] || false

    dolt sql -r parquet -q "select * from test order by a" > out.parquet
    run parquet cat out.parquet
    [ $status -eq 0 ]
    [[ "$output" =~ '{"a": 1, "v": "{\"key\": \"value\"}"}' ]] || false
    [[ "$output" =~ '{"a": 2, "v": "\"Hello\""}' ]] || false
}

@test "sql: ambiguous column name" {
    run dolt sql -q "select pk,pk1,pk2 from one_pk,two_pk where c1=0"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "ambiguous column name \"c1\", it's present in all these tables: [two_pk one_pk]" ]] || false
}

@test "sql: select with and and or clauses" {
    run dolt sql -q "select pk,pk1,pk2 from one_pk,two_pk where pk=0 and pk1=0 or pk2=1"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 13 ]
}

@test "sql: select the same column twice using column aliases" {
    run dolt sql -q "select pk,c1 as foo,c1 as bar from one_pk"
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "NULL" ]] || false
    [[ "$output" =~ "foo" ]] || false
    [[ "$output" =~ "bar" ]] || false
}

@test "sql: select same column twice using table aliases" {
    run dolt sql -q "select foo.pk,foo.c1,bar.c1 from one_pk as foo, one_pk as bar"
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "NULL" ]] || false
    [[ "$output" =~ "c1" ]] || false
}

@test "sql: select ambiguous column using table aliases" {
    run dolt sql -q "select pk,foo.c1,bar.c1 from one_pk as foo, one_pk as bar"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "ambiguous" ]] || false
}

@test "sql: basic inner join" {
    run dolt sql -q "select pk,pk1,pk2 from one_pk join two_pk on one_pk.c1=two_pk.c1 order by pk"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 8 ]
    first_join_output=$output
    run dolt sql -q "select pk,pk1,pk2 from two_pk join one_pk on one_pk.c1=two_pk.c1 order by pk"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 8 ]
    [ "$output" = "$first_join_output" ]
    run dolt sql -q "select pk,pk1,pk2 from one_pk join two_pk on one_pk.c1=two_pk.c1 where pk=1 order by pk"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    run dolt sql -q "select pk,pk1,pk2,one_pk.c1 as foo,two_pk.c1 as bar from one_pk join two_pk on one_pk.c1=two_pk.c1 order by pk"
    [ "$status" -eq 0 ]
    [[ "$output" =~ foo ]] || false
    [[ "$output" =~ bar ]] || false
    [ "${#lines[@]}" -eq 8 ]
    run dolt sql -q "select pk,pk1,pk2,one_pk.c1 as foo,two_pk.c1 as bar from one_pk join two_pk on one_pk.c1=two_pk.c1 where one_pk.c1=10 order by pk"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "$output" =~ "10" ]] || false
}

@test "sql: select two tables and join to one" {
    run dolt sql -q "select op.pk,pk1,pk2 from one_pk,two_pk join one_pk as op on op.pk=pk1"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 20 ]
}

@test "sql: non unique table alias" {
    run dolt sql -q "select pk from one_pk,one_pk"
    [ $status -eq 1 ]
}

@test "sql: is null and is not null statements" {
    dolt sql -q "insert into one_pk (pk,c1,c2) values (11,0,0)"
    run dolt sql -q "select pk from one_pk where c3 is null"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "$output" =~ "11" ]] || false
    run dolt sql -q "select pk from one_pk where c3 is not null"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 8 ]
    [[ ! "$output" =~ "11" ]] || false
}

@test "sql: addition and subtraction" {
    dolt sql -q "insert into one_pk (pk,c1,c2,c3,c4,c5) values (11,0,5,10,15,20)"
    run dolt sql -q "select pk from one_pk where c2-c1>=5"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "$output" =~ "11" ]] || false
    run dolt sql -q "select pk from one_pk where c3-c2-c1>=5"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "$output" =~ "11" ]] || false
    run dolt sql -q "select pk from one_pk where c2+c1<=5"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]
    [[ "$output" =~ "0" ]] || false
    [[ "$output" =~ "11" ]] || false
}

@test "sql: order by and limit" {
    run dolt sql -q "select * from one_pk order by pk limit 1"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "$output" =~ " 0 " ]] || false
    [[ ! "$output" =~ " 10 " ]] || false
    run dolt sql -q "select * from one_pk order by pk limit 0,1"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "$output" =~ " 0 " ]] || false
    [[ ! "$output" =~ " 10 " ]] || false
    run dolt sql -q "select * from one_pk order by pk limit 1,1"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "$output" =~ " 10 " ]] || false
    [[ ! "$output" =~ " 0 " ]] || false
    run dolt sql -q "select * from one_pk order by pk limit 1,0"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 0 ]
    [[ ! "$output" =~ " 0 " ]] || false
    run dolt sql -q "select * from one_pk order by pk desc limit 1"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "$output" =~ "30" ]] || false
    [[ ! "$output" =~ "10" ]] || false
    run dolt sql -q "select * from two_pk order by pk1, pk2 desc limit 1"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "$output" =~ "10" ]] || false
    run dolt sql -q "select pk,c2 from one_pk order by c1 limit 1"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "$output" =~ "0" ]] || false
    [[ ! "$output" =~ "10" ]] || false
    run dolt sql -q "select * from one_pk,two_pk order by pk1,pk2,pk limit 1"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "$output" =~ "0" ]] || false
    [[ ! "$output" =~ "10" ]] || false
    dolt sql -q "select * from one_pk join two_pk order by pk1,pk2,pk limit 1"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "$output" =~ "0" ]] || false
    [[ ! "$output" =~ "10" ]] || false
    run dolt sql -q "select * from one_pk order by limit 1"
    [ $status -eq 1 ]
    run dolt sql -q "select * from one_pk order by bad limit 1"
    [ $status -eq 1 ]
    [[ "$output" =~ "column \"bad\" could not be found" ]] || false
    run dolt sql -q "select * from one_pk order pk by limit"
    [ $status -eq 1 ]
}

@test "sql: limit less than zero" {
    run dolt sql -q "select * from one_pk order by pk limit -1"
    [ $status -eq 1 ]
    [[ "$output" =~ "syntax error" ]] || false
    run dolt sql -q "select * from one_pk order by pk limit -2"
    [ $status -eq 1 ]
    [[ "$output" =~ "syntax error" ]] || false
    run dolt sql -q "select * from one_pk order by pk limit -1,1"
    [ $status -eq 1 ]
    [[ "$output" =~ "syntax error" ]] || false
}

@test "sql: addition on both left and right sides of comparison operator" {
    dolt sql -q "insert into one_pk (pk,c1,c2,c3,c4,c5) values (11,5,5,10,15,20)"
    run dolt sql -q "select pk from one_pk where c2+c1<=5+5"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]
    [[ "$output" =~ 0 ]] || false
    [[ "$output" =~ 11 ]] || false
}

@test "sql: select with in list" {
    run dolt sql -q "select pk from one_pk where c1 in (10,20)"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]
    [[ "$output" =~ "1" ]] || false
    [[ "$output" =~ "2" ]] || false
    run dolt sql -q "select pk from one_pk where c1 in (11,21)"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 0 ]
    run dolt sql -q "select pk from one_pk where c1 not in (10,20)"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]
    [[ "$output" =~ "0" ]] || false
    [[ "$output" =~ "3" ]] || false
    run dolt sql -q "select pk from one_pk where c1 not in (10,20) and c1 in (30)"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "$output" =~ "3" ]] || false
}

@test "sql: parser does not support empty list" {
    run dolt sql -q "select pk from one_pk where c1 not in ()"
    [ $status -eq 1 ]
    [[ "$output" =~ "Error parsing SQL" ]] || false
}

@test "sql: addition in join statement" {
    run dolt sql -q "select * from one_pk join two_pk on pk1-pk>0 and pk2<1"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "$output" =~ "20" ]] || false
}

@test "sql: leave off table name in select" {
    dolt sql -q "insert into one_pk (pk,c1,c2) values (11,0,0)"
    run dolt sql -q "select pk where c3 is null"
    [ $status -eq 1 ]
    [[ "$output" =~ "column \"c3\" could not be found in any table in scope" ]] || false
}

@test "sql: show tables" {
    run dolt sql -q "show tables"
    [ $status -eq 0 ]
    echo ${#lines[@]}
    [ "${#lines[@]}" -eq 7 ]
    [[ "$output" =~ "one_pk" ]] || false
    [[ "$output" =~ "two_pk" ]] || false
    [[ "$output" =~ "has_datetimes" ]] || false
}

@test "sql: show tables AS OF" {
    dolt add .; dolt commit -m 'commit tables'
    dolt sql <<SQL
CREATE TABLE table_a(x int primary key);
CREATE TABLE table_b(x int primary key);
SQL
    dolt add .; dolt commit -m 'commit tables'

    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]
    [[ "$output" =~ table_a ]] || false

    run dolt sql -q "show tables AS OF 'HEAD~'" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 4 ]
    [[ ! "$output" =~ table_a ]] || false
}

@test "sql: USE branch" {
    dolt add .; dolt commit -m 'commit tables'
    dolt checkout -b feature-branch
    dolt checkout main

    dolt sql  <<SQL
USE \`dolt_repo_$$/feature-branch\`;
CREATE TABLE table_a(x int primary key);
CREATE TABLE table_b(x int primary key);
CALL DOLT_ADD('.');
call dolt_commit('-a', '-m', 'two new tables');
SQL

    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 4 ]
    [[ ! "$output" =~ table_a ]] || false

    dolt checkout feature-branch
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]
    [[ "$output" =~ table_a ]] || false
}

@test "sql: create new database" {
    dolt add .; dolt commit -m 'commit tables'
    dolt checkout -b feature-branch
    dolt checkout main

    dolt sql  <<SQL
CREATE DATABASE test1;
USE test1;
CREATE TABLE table_a(x int primary key);
CALL DOLT_ADD('.');
insert into table_a values (1), (2);
call dolt_commit('-a', '-m', 'created table_a');
SQL

    cd test1

    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ table_a ]] || false

    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "created table_a" ]] || false

    cd ../
    run dolt sql  <<SQL
use test1;
show tables;
SQL

    [ "$status" -eq 0 ]
    [[ "$output" =~ "table_a" ]] || false

    dolt sql -q "create database test2"
    [ -d "test2" ]

    # the current db should always be the one that the SQL command was
    # run in, not any nested dbs
    run dolt sql -q "select database()"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "dolt_repo" ]] || false

    run dolt sql -q "show databases"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "dolt_repo" ]] || false
    [[ "$output" =~ "test1" ]] || false
    [[ "$output" =~ "test2" ]] || false
    [[ "$output" =~ "information_schema" ]] || false

    touch existing_file
    mkdir existing_dir

    run dolt sql -q "create database existing_file"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "exists" ]] || false

    run dolt sql -q "create database existing_dir"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "exists" ]] || false
}

@test "sql: use database with multiple dbs" {
    dolt sql -q "create database db1"
    dolt sql -q "create database db2"

    dolt sql <<SQL
use db1;
create table t1 (a int primary key);
insert into t1 values (10);
use db2;
create table t2 (a int primary key);
insert into t2 values (20);
SQL

    cd db1
    run dolt sql -q "select * from t1"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "10" ]] || false

    cd ../db2
    run dolt sql -q "select * from t2"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "20" ]] || false
}

@test "sql: dolt_show_branch_databases" {
    mkdir new && cd new

    dolt sql <<SQL
create database db1;
create database db2;
use db1;
create table t1 (a int primary key);
call dolt_commit('-Am', 'new table');
call dolt_branch('b1');
call dolt_branch('b2');
use db2;
create table t2 (b int primary key);
call dolt_commit('-Am', 'new table');
call dolt_branch('b3');
call dolt_branch('b4');
SQL

    run dolt sql -r csv -q "show databases"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "db1" ]] || false
    [[ "$output" =~ "db2" ]] || false
    [[ ! "$output" =~ "/" ]] || false

    run dolt sql -r csv -q "set dolt_show_branch_databases = 1; show databases"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 11 ] # 2 base dbs, 3 branch dbs each, 2 mysql dbs, 1 header line
    [[ "$output" =~ "db1/b1" ]] || false
    [[ "$output" =~ "db1/b2" ]] || false
    [[ "$output" =~ "db1/main" ]] || false
    [[ "$output" =~ "db2/b3" ]] || false
    [[ "$output" =~ "db2/b4" ]] || false
    [[ "$output" =~ "db2/main" ]] || false

    run dolt sql -q "show databases"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "db1" ]] || false
    [[ "$output" =~ "db2" ]] || false
    [[ ! "$output" =~ "/" ]] || false

    dolt sql -q "set @@persist.dolt_show_branch_databases = 1"
    run dolt sql -r csv -q "show databases"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 11 ]

    # make sure we aren't double-counting revision dbs
    run dolt sql -r csv -q 'use `db1/main`; show databases'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Database changed" ]] || false
    [ "${#lines[@]}" -eq 12 ] # one line for above output, 11 dbs
}

@test "sql: run outside a dolt directory" {
    mkdir new && cd new

    mkdir decoy
    touch decoy/file.txt

    dolt sql  <<SQL
CREATE DATABASE test1;
USE test1;
CREATE TABLE table_a(x int primary key);
CALL DOLT_ADD('.');
insert into table_a values (1), (2);
call dolt_commit('-a', '-m', 'created table_a');
SQL

    cd test1

    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ table_a ]] || false

    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "created table_a" ]] || false

    cd ../

    run dolt sql -q "show databases"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test1" ]] || false
    [[ "$output" =~ "information_schema" ]] || false
    [[ ! "$output" =~ "decoy" ]] || false

    # There's a bug in the teardown logic that means we need to cd
    # into the repo directory before the test ends
    cd ../
}

@test "sql: drop database with branches in use" {
    skiponwindows "Dropping databases can fail on windows due to file in use errors, need to fix"

    mkdir new && cd new

    # this works fine, no attempt to use a dropped database
    dolt sql  <<SQL
CREATE DATABASE test1;
CREATE DATABASE test2;
USE test1;
CALL DOLT_CHECKOUT('-b', 'newBranch');
USE \`test1/newBranch\`;
USE test2;
DROP DATABASE test1;
SHOW TABLES;
SQL

    # this fails, we're using test1 after dropping it
    run dolt sql  <<SQL
CREATE DATABASE test1;
USE test1;
CALL DOLT_CHECKOUT('-b', 'newBranch');
USE \`TEST1/newBranch\`;
USE test2;
DROP DATABASE Test1;
SHOW TABLES;
USE \`test1/newBranch\`;
SQL

    [ $status -ne 0 ]
    [[ "$output" =~ "database not found: test1/newBranch" ]] || false

    cd ../
}

@test "sql: bad dolt db" {
    mkdir new && cd new

    mkdir -p decoy/.dolt/noms/oldgen
    mkdir -p decoy/.dolt/noms/temptf
    echo '{}' > decoy/config.json

    # Not doing this cd ../ results in the teardown method failing on
    # a skip, not sure why. It's not part of the actual test
    cd ../
    skip "This results in a panic right now"

    run dolt sql -q "show databases" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "information_schema" ]] || false
    [[ ! "$output" =~ "decoy" ]] || false
}

@test "sql: set head ref session var" {
    dolt add .; dolt commit -m 'commit tables'
    dolt checkout -b feature-branch
    dolt checkout main

    run dolt sql -q "select @@dolt_repo_$$_head_ref;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ 'refs/heads/main' ]] || false

    dolt sql  <<SQL
set @@dolt_repo_$$_head_ref = 'feature-branch';
CREATE TABLE test (x int primary key);
CALL DOLT_ADD('.');
call dolt_commit('-a', '-m', 'new table');
SQL

    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 4 ]
    [[ ! "$output" =~ test ]] || false

    dolt checkout feature-branch
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "$output" =~ test ]] || false

    dolt checkout main
    dolt sql  <<SQL
set @@dolt_repo_$$_head_ref = 'refs/heads/feature-branch';
insert into test values (1), (2), (3);
call dolt_commit('-a', '-m', 'inserted 3 values');
SQL

    dolt checkout feature-branch
    run dolt sql -q "select * from test" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 4 ]

    dolt checkout main

    run dolt sql  <<SQL
set @@dolt_repo_$$_head_ref = 'feature-branch';
select @@dolt_repo_$$_head_ref;
SQL

    [ "$status" -eq 0 ]
    [[ "$output" =~ 'refs/heads/feature-branch' ]] || false

    # switching to a branch that doesn't exist should be an error
    run dolt sql -q "set @@dolt_repo_$$_head_ref = 'does-not-exist';"
    [ "$status" -eq 1 ]
    # TODO: this error message could be improved
    [[ "$output" =~ "database not found: dolt_repo_$$/does-not-exist" ]] || false
}

@test "sql: branch qualified DB name in select" {
    dolt add .; dolt commit -m 'commit tables'
    dolt checkout -b feature-branch
    dolt checkout main

    dolt sql  <<SQL
USE \`dolt_repo_$$/feature-branch\`;
CREATE TABLE a1(x int primary key);
CALL DOLT_ADD('.');
insert into a1 values (1), (2), (3);
call dolt_commit('-a', '-m', 'new table');
SQL

    run dolt sql -q "select * from \`dolt_repo_$$/feature-branch\`.a1 order by x;" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 4 ]
}

@test "sql: branch qualified DB name in insert" {
    dolt add .; dolt commit -m 'commit tables'
    dolt checkout -b feature-branch
    dolt checkout main

    dolt sql  <<SQL
USE \`dolt_repo_$$/feature-branch\`;
CREATE TABLE a1(x int primary key);
CALL DOLT_ADD('.');
insert into a1 values (1), (2), (3);
call dolt_commit('-a', '-m', 'new table');
SQL

    run dolt sql -q "insert into \`dolt_repo_$$/feature-branch\`.a1 values (4);" -r csv
    [ "$status" -eq 0 ]

    run dolt sql -q "select * from \`dolt_repo_$$/feature-branch\`.a1 order by x;" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
}

@test "sql: commit hash qualified DB name in select" {
    dolt add .; dolt commit -m 'commit tables'
    dolt checkout -b feature-branch

    dolt sql  <<SQL
CREATE TABLE a1(x int primary key);
CALL DOLT_ADD('.');
insert into a1 values (1), (2), (3);
call dolt_commit('-a', '-m', 'new table');
insert into a1 values (4), (5), (6);
call dolt_commit('-a', '-m', 'more values');
SQL

    # get the second to last commit hash
    hash=`dolt log | grep commit | cut -d" " -f2 | tail -n+2 | head -n1`

    run dolt sql -q "select * from \`dolt_repo_$$/$hash\`.a1 order by x;" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 4 ]
    [[ ! "$output" =~ "5" ]] || false

    # same with USE syntax
    run dolt sql  -r csv <<SQL
    USE \`dolt_repo_$$/$hash\`;
    select * from a1;
SQL

    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 5 ] # First line is "database changed"
    [[ ! "$output" =~ "5" ]] || false
}

@test "sql: commit hash qualified DB name in delete" {
    dolt add .; dolt commit -m 'commit tables'
    dolt checkout -b feature-branch

    dolt sql  <<SQL
CREATE TABLE a1(x int primary key);
CALL DOLT_ADD('.');
insert into a1 values (1), (2), (3);
call dolt_commit('-a', '-m', 'new table');
insert into a1 values (4), (5), (6);
call dolt_commit('-a', '-m', 'more values');
SQL

    # get the second to last commit hash
    hash=`dolt log | grep commit | cut -d" " -f2 | tail -n+2 | head -n1`

    run dolt sql -q "delete from \`dolt_repo_$$/$hash\`.a1;" -r csv
    [ "$status" -eq 1 ]
    [[ "$output" =~ 'read-only' ]] || false

    # same with USE syntax
    run dolt sql  <<SQL
    USE \`dolt_repo_$$/$hash\`;
    delete from a1;
SQL

    [ "$status" -eq 1 ]
    [[ "$output" =~ 'read-only' ]] || false
}

@test "sql: commit hash qualified DB name in update" {
    dolt add .; dolt commit -m 'commit tables'
    dolt checkout -b feature-branch

    dolt sql  <<SQL
CREATE TABLE a1(x int primary key);
CALL DOLT_ADD('.');
insert into a1 values (1), (2), (3);
call dolt_commit('-a', '-m', 'new table');
insert into a1 values (4), (5), (6);
call dolt_commit('-a', '-m', 'more values');
SQL

    # get the second to last commit hash
    hash=`dolt log | grep commit | cut -d" " -f2 | tail -n+2 | head -n1`

    run dolt sql -q "update \`dolt_repo_$$/$hash\`.a1 set x = x*10" -r csv
    [ "$status" -eq 1 ]
    [[ "$output" =~ 'read-only' ]] || false

    # same with USE syntax
    run dolt sql  <<SQL
    USE \`dolt_repo_$$/$hash\`;
    update a1 set x = x*10;
SQL

    [ "$status" -eq 1 ]
    [[ "$output" =~ 'read-only' ]] || false
}

@test "sql: tag qualified DB name in select" {
    dolt add .; dolt commit -m 'commit tables'
    dolt checkout -b feature-branch

    dolt sql  <<SQL
USE \`dolt_repo_$$/feature-branch\`;
CREATE TABLE a1(x int primary key);
CALL DOLT_ADD('.');
insert into a1 values (1), (2), (3);
call dolt_commit('-a', '-m', 'new table');
SQL

    run dolt tag v1
    [ "$status" -eq 0 ]

    run dolt sql -q "select * from \`dolt_repo_$$/v1\`.a1 order by x;" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 4 ]
}

@test "sql: USE tag doesn't create duplicate commit DB name" {
    dolt add .; dolt commit -m 'commit tables'
    dolt checkout -b feature-branch

    # get the last commit hash
    hash=`dolt log | grep commit | cut -d" " -f2 | tail -n+1 | head -n1`

    dolt sql  <<SQL
USE \`dolt_repo_$$/$hash\`;
USE \`dolt_repo_$$/feature-branch\`;
CALL DOLT_TAG("v1");
USE \`dolt_repo_$$/v1\`;
CALL dolt_checkout('feature-branch');
SQL
}

@test "sql: USE fake hash throws error" {
    dolt add .; dolt commit -m 'commit tables'

    # get the last commit hash
    hash=`dolt log | grep commit | cut -d" " -f2 | tail -n+1 | head -n1`

    # no error for this hash
    dolt sql  <<SQL
USE \`dolt_repo_$$/$hash\`;
SQL

    # try with a fake hash
    hash='h4jks5lomp9u41r6902knn0pfr7lsgth'
    run dolt sql -q "use \`dolt_repo_$$/$hash\`"
    [ "$status" -ne 0 ]
    [[ "$output" =~ "database not found" ]] || false
}

@test "sql: tag qualified DB name in delete" {
    dolt add .; dolt commit -m 'commit tables'
    dolt checkout -b feature-branch

    dolt sql  <<SQL
CREATE TABLE a1(x int primary key);
CALL DOLT_ADD('.');
insert into a1 values (1), (2), (3);
call dolt_commit('-a', '-m', 'new table');
insert into a1 values (4), (5), (6);
call dolt_commit('-a', '-m', 'more values');
SQL

    run dolt tag v1
    [ "$status" -eq 0 ]

    run dolt sql -q "delete from \`dolt_repo_$$/v1\`.a1;" -r csv
    [ "$status" -eq 1 ]
    [[ "$output" =~ 'read-only' ]] || false

    # same with USE syntax
    run dolt sql  <<SQL
    USE \`dolt_repo_$$/v1\`;
    delete from a1;
SQL

    [ "$status" -eq 1 ]
    [[ "$output" =~ 'read-only' ]] || false
}

@test "sql: tag qualified DB name in update" {
    dolt add .; dolt commit -m 'commit tables'
    dolt checkout -b feature-branch

    dolt sql  <<SQL
CREATE TABLE a1(x int primary key);
CALL DOLT_ADD('.');
insert into a1 values (1), (2), (3);
call dolt_commit('-a', '-m', 'new table');
insert into a1 values (4), (5), (6);
call dolt_commit('-a', '-m', 'more values');
SQL

    run dolt tag v1
    [ "$status" -eq 0 ]

    run dolt sql -q "update \`dolt_repo_$$/v1\`.a1 set x = x*10" -r csv
    [ "$status" -eq 1 ]
    [[ "$output" =~ 'read-only' ]] || false

    # same with USE syntax
    run dolt sql  <<SQL
    USE \`dolt_repo_$$/v1\`;
    update a1 set x = x*10;
SQL

    [ "$status" -eq 1 ]
    [[ "$output" =~ 'read-only' ]] || false
}

@test "sql: describe" {
    run dolt sql -q "describe one_pk"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 10 ]
    [[ "$output" =~ "pk" ]] || false
    [[ "$output" =~ "c5" ]] || false
}

@test "sql: describe with information_schema correctly works" {
    run dolt sql -r csv -q "describe information_schema.columns"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 23 ]
}

@test "sql: describe bad table name" {
    run dolt sql -q "describe poop"
    [ $status -eq 1 ]
    [[ "$output" =~ "table not found: poop" ]] || false
}

@test "sql: alter table to add and delete a column" {
    run dolt sql -q "alter table one_pk add (c6 int)"
    [ $status -eq 0 ]
    run dolt sql -q "describe one_pk"
    [ $status -eq 0 ]
    [[ "$output" =~ "c6" ]] || false
    run dolt schema show one_pk
    [[ "$output" =~ "c6" ]] || false
    run dolt sql -q "alter table one_pk drop column c6"
    [ $status -eq 0 ]
    run dolt sql -q "describe one_pk"
    [ $status -eq 0 ]
    [[ ! "$output" =~ "c6" ]] || false
    run dolt schema show one_pk
    [[ ! "$output" =~ "c6" ]] || false
}

@test "sql: alter table to rename a column" {
    dolt sql -q "alter table one_pk add (c6 int)"
    run dolt sql -q "alter table one_pk rename column c6 to c7"
    [ $status -eq 0 ]
    run dolt sql -q "describe one_pk"
    [ $status -eq 0 ]
    [[ "$output" =~ "c7" ]] || false
    [[ ! "$output" =~ "c6" ]] || false
}

@test "sql: alter table change column to rename a column" {
    dolt sql -q "alter table one_pk add (c6 int)"
    dolt sql -q "alter table one_pk change column c6 c7 int"
    run dolt sql -q "describe one_pk"
    [ $status -eq 0 ]
    [[ "$output" =~ "c7" ]] || false
    [[ ! "$output" =~ "c6" ]] || false
}

@test "sql: alter table without parentheses" {
    run dolt sql -q "alter table one_pk add c6 int"
    [ $status -eq 0 ]
    run dolt sql -q "describe one_pk"
    [ $status -eq 0 ]
    [[ "$output" =~ "c6" ]] || false
}

@test "sql: alter table modify column with no actual change" {
    # this specifically tests a previous bug where we would get a name collision and fail
    dolt sql -q "alter table one_pk modify column c5 bigint"
    run dolt schema show one_pk
    [ $status -eq 0 ]
    [[ "$output" =~ '`pk` bigint NOT NULL' ]] || false
    [[ "$output" =~ '`c1` bigint' ]] || false
    [[ "$output" =~ '`c2` bigint' ]] || false
    [[ "$output" =~ '`c3` bigint' ]] || false
    [[ "$output" =~ '`c4` bigint' ]] || false
    [[ "$output" =~ '`c5` bigint' ]] || false
    [[ "$output" =~ 'PRIMARY KEY (`pk`)' ]] || false
}

@test "sql: alter table change column with no actual change" {
    # this specifically tests a previous bug where we would get a name collision and fail
    dolt sql -q "alter table one_pk change column c5 c5 bigint"
    run dolt schema show one_pk
    [ $status -eq 0 ]
    [[ "$output" =~ '`pk` bigint NOT NULL' ]] || false
    [[ "$output" =~ '`c1` bigint' ]] || false
    [[ "$output" =~ '`c2` bigint' ]] || false
    [[ "$output" =~ '`c3` bigint' ]] || false
    [[ "$output" =~ '`c4` bigint' ]] || false
    [[ "$output" =~ '`c5` bigint' ]] || false
    [[ "$output" =~ 'PRIMARY KEY (`pk`)' ]] || false
}

@test "sql: alter table modify column type success" {
    dolt sql <<SQL
CREATE TABLE t1(pk BIGINT PRIMARY KEY, v1 INT, INDEX(v1));
CREATE TABLE t2(pk BIGINT PRIMARY KEY, v1 VARCHAR(20), INDEX(v1));
CREATE TABLE t3(pk BIGINT PRIMARY KEY, v1 DATETIME, INDEX(v1));
INSERT INTO t1 VALUES (0,-1),(1,1);
INSERT INTO t2 VALUES (0,'hi'),(1,'bye');
INSERT INTO t3 VALUES (0,'1999-11-02 17:39:38'),(1,'2021-01-08 02:59:27');
ALTER TABLE t1 MODIFY COLUMN v1 BIGINT;
ALTER TABLE t2 MODIFY COLUMN v1 VARCHAR(2000);
ALTER TABLE t3 MODIFY COLUMN v1 TIMESTAMP;
SQL

    run dolt sql -q "SELECT * FROM t1 ORDER BY pk" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "0,-1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM t2 ORDER BY pk" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "0,hi" ]] || false
    [[ "$output" =~ "1,bye" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM t3 ORDER BY pk" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "0,1999-11-02 17:39:38" ]] || false
    [[ "$output" =~ "1,2021-01-08 02:59:27" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false

    dolt sql <<SQL
CREATE TABLE t4(pk int unsigned primary key, v1 INT, INDEX(v1));
insert into t4 values (1, 1);
ALTER TABLE t4 MODIFY COLUMN pk float;
SQL

    run dolt sql -q "SELECT * FROM t4 ORDER BY pk" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
}

@test "sql: alter table modify column type failure" {
    dolt sql <<SQL
CREATE TABLE t1(pk BIGINT PRIMARY KEY, v1 INT, INDEX(v1));
INSERT INTO t1 VALUES (0,-1),(1,1);
SQL
    run dolt sql -q "ALTER TABLE t1 MODIFY COLUMN v1 INT UNSIGNED"
    [ "$status" -eq "1" ]
}

@test "sql: alter table modify column type no data change" {

    # there was a bug on NULLs where it would register a change
    dolt sql <<SQL
CREATE TABLE t1(pk BIGINT PRIMARY KEY, v1 VARCHAR(64), INDEX(v1));
INSERT INTO t1 VALUES (0,NULL),(1,NULL);
SQL
    dolt add -A
    dolt commit -m "commit"
    dolt sql -q "ALTER TABLE t1 MODIFY COLUMN v1 VARCHAR(100) NULL"
    run dolt diff -d
    [ "$status" -eq "0" ]
    [[ ! "$output" =~ "|  <  |" ]] || false
    [[ ! "$output" =~ "|  >  |" ]] || false
}

@test "sql: drop table" {
    dolt sql -q "drop table one_pk"
    run dolt ls
    [[ ! "$output" =~ "one_pk" ]] || false
    run dolt sql -q "drop table poop"
    [ $status -eq 1 ]
    [[ "$output" =~ "table not found: poop" ]] || false
}

@test "sql: replace count" {
    skip "right now we always count a replace as a delete and insert when we shouldn't"
    dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v BIGINT);"
    run dolt sql -q "REPLACE INTO test VALUES (1, 1);"
    [ $status -eq 0 ]
    [[ "${lines[3]}" =~ " 1 " ]] || false
    run dolt sql -q "REPLACE INTO test VALUES (1, 2);"
    [ $status -eq 0 ]
    [[ "${lines[3]}" =~ " 2 " ]] || false
}

@test "sql: unix_timestamp function" {
    run dolt sql -q "SELECT UNIX_TIMESTAMP(NOW()) FROM dual;"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
}

@test "sql: select union all" {
    run dolt sql -r csv -q "SELECT 2+2 FROM dual UNION ALL SELECT 2+2 FROM dual UNION ALL SELECT 2+3 FROM dual;"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 4 ]
}

@test "sql: select union" {
    run dolt sql -r csv -q "SELECT 2+2 FROM dual UNION SELECT 2+2 FROM dual UNION SELECT 2+3 FROM dual;"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
    run dolt sql -r csv -q "SELECT 2+2 FROM dual UNION DISTINCT SELECT 2+2 FROM dual UNION SELECT 2+3 FROM dual;"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
    run dolt sql -r csv -q "(SELECT 2+2 FROM dual UNION DISTINCT SELECT 2+2 FROM dual) UNION SELECT 2+3 FROM dual;"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
    run dolt sql -r csv -q "SELECT 2+2 FROM dual UNION DISTINCT (SELECT 2+2 FROM dual UNION SELECT 2+3 FROM dual);"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
}

@test "sql: greatest/least with a timestamp" {
    run dolt sql -q "SELECT GREATEST(NOW(), DATE_ADD(NOW(), INTERVAL 2 DAY)) FROM dual;"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    run dolt sql -q "SELECT LEAST(NOW(), DATE_ADD(NOW(), INTERVAL 2 DAY)) FROM dual;"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
}

@test "sql: greatest with converted null" {
    run dolt sql -q "SELECT GREATEST(CAST(NOW() AS CHAR), CAST(NULL AS CHAR)) FROM dual;"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "${lines[3]}" =~ " NULL " ]] || false
}

@test "sql: date_format function" {
    dolt sql -q "select date_format(date_created, '%Y-%m-%d') from has_datetimes"
}

@test "sql: DATE_ADD and DATE_SUB in where clause" {
    run dolt sql -q "select * from has_datetimes where date_created > DATE_SUB('2020-02-18 00:00:00', INTERVAL 2 DAY)"
    [ $status -eq 0 ]
    [[ "$output" =~ "17 " ]] || false
    run dolt sql -q "select * from has_datetimes where date_created > DATE_ADD('2020-02-14 00:00:00', INTERVAL 2 DAY)"
    [ $status -eq 0 ]
    [[ "$output" =~ "17 " ]] || false
}

@test "sql: update a datetime column" {
    dolt sql -q "insert into has_datetimes (pk) values (1)"
    run dolt sql -q "update has_datetimes set date_created='2020-02-11 00:00:00' where pk=1"
    [ $status -eq 0 ]
    [[ ! "$output" =~ "Expected GetField expression" ]] || false
}

@test "sql: group by statements" {
    dolt sql -q "insert into one_pk (pk,c1,c2,c3,c4,c5) values (4,0,0,0,0,0),(5,0,0,0,0,0)"
    run dolt sql -q "select max(pk) from one_pk group by c1"
    [ $status -eq 0 ]
    [[ "$output" =~ " 5 " ]] || false
    [[ ! "$output" =~ " 4 " ]] || false
    run dolt sql -q "select max(pk), min(c2) from one_pk group by c1"
    [ $status -eq 0 ]
    [[ "$output" =~ " 5 " ]] || false
    [[ "$output" =~ " 0 " ]] || false
    [[ ! "$output" =~ " 4 " ]] || false
    run dolt sql -r csv -q "select max(pk),c2 from one_pk group by c1"
    [ $status -eq 0 ]
    [[ "$output" =~ "5,0" ]] || false
    [[ "$output" =~ "1,10" ]] || false
    [[ "$output" =~ "2,20" ]] || false
    [[ "$output" =~ "3,30" ]] || false
}

@test "sql: substr() and cast() functions" {
    run dolt sql -q "select substr(cast(date_created as char), 1, 4) from has_datetimes"
    [ $status -eq 0 ]
    [[ "$output" =~ " 2020 " ]] || false
    [[ ! "$output" =~ "17" ]] || false
}

@test "sql: divide by zero does not panic" {
    run dolt sql -q "select 1/0 from dual"
    [ $status -eq 0 ]
    echo $output
    [[ "$output" =~ "NULL" ]] || false
    [[ ! "$output" =~ "panic: " ]] || false
    run dolt sql -q "select 1.0/0.0 from dual"
    [ $status -eq 0 ]
    [[ "$output" =~ "NULL" ]] || false
    [[ ! "$output" =~ "panic: " ]] || false
    run dolt sql -q "select 1 div 0 from dual"
    [ $status -eq 0 ]
    [[ "$output" =~ "NULL" ]] || false
    [[ ! "$output" =~ "panic: " ]] || false
}

@test "sql: delete all rows in table" {
    run dolt sql <<SQL
DELETE FROM one_pk;
SELECT count(*) FROM one_pk;
SQL
    [ $status -eq 0 ]
    [[ "$output" =~ "0" ]] || false
}


@test "sql: batch delimiter" {
    dolt sql <<SQL
DELIMITER //
CREATE TABLE test (
  pk BIGINT PRIMARY KEY,
  v1 BIGINT,
  v2 BIGINT
)//
INSERT INTO test VALUES (1, 1, 1) //
DELIMITER $
INSERT INTO test VALUES (2, 2, 2)$ $
CREATE PROCEDURE p1(x BIGINT)
BEGIN
  IF x < 10 THEN
    SET x = 10;
  END IF;
  SELECT pk+x, v1+x, v2+x FROM test ORDER BY 1;
END$
DELIMITER ;
INSERT INTO test VALUES (3, 3, 3);
DELIMITER **********
INSERT INTO test VALUES (4, 4, 4)**********
DELIMITER &
INSERT INTO test VALUES (5, 5, 5)&
INSERT INTO test VALUES (6, 6, 6)
SQL
    run dolt sql -q "CALL p1(3)" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "(test.pk + x),(test.v1 + x),(test.v2 + x)" ]] || false
    [[ "$output" =~ "11,11,11" ]] || false
    [[ "$output" =~ "12,12,12" ]] || false
    [[ "$output" =~ "13,13,13" ]] || false
    [[ "$output" =~ "14,14,14" ]] || false
    [[ "$output" =~ "15,15,15" ]] || false
    [[ "$output" =~ "16,16,16" ]] || false
    [[ "${#lines[@]}" = "7" ]] || false

    run dolt sql -q "CALL p1(20)" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "(test.pk + x),(test.v1 + x),(test.v2 + x)" ]] || false
    [[ "$output" =~ "21,21,21" ]] || false
    [[ "$output" =~ "22,22,22" ]] || false
    [[ "$output" =~ "23,23,23" ]] || false
    [[ "$output" =~ "24,24,24" ]] || false
    [[ "$output" =~ "25,25,25" ]] || false
    [[ "$output" =~ "26,26,26" ]] || false
    [[ "${#lines[@]}" = "7" ]] || false

    dolt sql <<SQL
DELIMITER //
CREATE TABLE test2(
  pk BIGINT PRIMARY KEY,
  v1 VARCHAR(20)
)//
INSERT INTO test2 VALUES (1, '//'), (2, "//")//
SQL
    run dolt sql -q "SELECT * FROM test2" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,//" ]] || false
    [[ "$output" =~ "2,//" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "sql: insert on duplicate key inserts data by column" {
    dolt sql -q "CREATE TABLE test (col_a varchar(2) not null, col_b varchar(2), col_c varchar(2), primary key(col_a));"
    dolt add test
    dolt commit -m "created table"

    dolt sql -q "INSERT INTO test (col_a,col_b) VALUES('a', 'b');"
    dolt sql -q "INSERT INTO test (col_a,col_b,col_c) VALUES ('a','','b') ON DUPLICATE KEY UPDATE col_a = col_a, col_b = col_b, col_c = VALUES(col_c);"
    run dolt sql -r csv -q "SELECT * from test where col_a = 'a'"
    [ $status -eq 0 ]
    echo $output
    [[ "$output" =~ "a,b,b" ]] || false

    dolt sql -b -q "INSERT INTO test VALUES ('b','b','b');INSERT INTO test VALUES ('b', '1', '1') ON DUPLICATE KEY UPDATE col_b = '2', col_c='2';"
    run dolt sql -r csv -q "SELECT * from test where col_a = 'b'"
    [ $status -eq 0 ]
    [[ "$output" =~ "b,2,2" ]] || false

    dolt sql -q "INSERT INTO test VALUES ('c', 'c', 'c'), ('c', '1', '1') ON DUPLICATE KEY UPDATE col_b = '2', col_c='2'"
    run dolt sql -r csv -q "SELECT * from test where col_a = 'c'"
    [ $status -eq 0 ]
    [[ "$output" =~ "c,2,2" ]] || false

    dolt sql <<SQL
INSERT INTO test VALUES ('d','d','d');
DELETE FROM test WHERE col_a='d';
INSERT INTO test VALUES ('d', '1', '1') ON DUPLICATE KEY UPDATE col_b = '2', col_c='2';
SQL
    run dolt sql -r csv -q "SELECT * from test where col_a = 'd'"
    [ $status -eq 0 ]
    [[ "$output" =~ "d,1,1" ]] || false
}

@test "sql: duplicate key inserts on table with primary and secondary indexes" {
    dolt sql -q "CREATE TABLE test (pk int primary key, uk int unique key, i int);"
    dolt sql -q "INSERT INTO test VALUES(0,0,0);"
    run dolt sql -r csv -q "SELECT * from test"
    [ $status -eq 0 ]
    [[ "$output" =~ "0,0,0" ]] || false
    run dolt sql -q "INSERT INTO test (pk,uk) VALUES(1,0) on duplicate key update i = 99;"
    [ $status -eq 0 ]
    run dolt sql -r csv -q "SELECT * from test"
    [ $status -eq 0 ]
    [[ "$output" =~ "0,0,99" ]] || false
}

@test "sql: select with json output supports datetime" {
    run dolt sql -r json -q "select * from has_datetimes"
    [ $status -eq 0 ]
    [[ "$output" =~ "2020-02-17 00:00:00" ]] || false
}

@test "sql: dolt_version() func" {
    SQL=$(dolt sql -q 'select dolt_version() from dual;' -r csv | tail -n 1)
    CLI=$(dolt version | sed '1p;d' | cut -d " " -f 3)
    [ "$SQL" == "$CLI" ]
}

@test "sql: stored procedures creation check" {
    dolt sql -q "
DELIMITER //
CREATE PROCEDURE p1(s VARCHAR(200), N DOUBLE, m DOUBLE)
BEGIN
  SET s = '';
  IF n = m THEN SET s = 'equals';
  ELSE
    IF n > m THEN SET s = 'greater';
    ELSE SET s = 'less';
    END IF;
    SET s = CONCAT('is ', s, ' than');
  END IF;
  SET s = CONCAT(n, ' ', s, ' ', m, '.');
  SELECT s;
END;
//"
    run dolt sql -q "CALL p1('', 1, 1)" -r=csv
   [ "$status" -eq "0" ]
    [[ "$output" =~ "1 equals 1." ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "CALL p1('', 2, 1)" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "2 is greater than 1." ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "CALL p1('', 1, 2)" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "1 is less than 2." ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_procedures" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "name,create_stmt,created_at,modified_at" ]] || false
    # Just the beginning portion is good enough, we don't need to test the timestamps as they change
    [[ "$output" =~ 'p1,"CREATE PROCEDURE p1(s VARCHAR(200), N DOUBLE, m DOUBLE)' ]] || false
    [[ "${#lines[@]}" = "14" ]] || false
}

@test "sql: stored procedures show and delete" {
    dolt sql <<SQL
CREATE PROCEDURE p1() SELECT 5*5;
CREATE PROCEDURE p2() SELECT 6*6;
SQL
    # We're excluding timestamps in these statements
    # Initial look
    run dolt sql -b -q "SET @@show_external_procedures = 0;SELECT * FROM dolt_procedures" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "name,create_stmt,created_at,modified_at" ]] || false
    [[ "$output" =~ 'p1,CREATE PROCEDURE p1() SELECT 5*5' ]] || false
    [[ "$output" =~ 'p2,CREATE PROCEDURE p2() SELECT 6*6' ]] || false

    run dolt sql -b -q "SET @@show_external_procedures = 0;SHOW PROCEDURE STATUS" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "Db,Name,Type,Definer,Modified,Created,Security_type,Comment,character_set_client,collation_connection,Database Collation" ]] || false
    [[ "$output" =~ ',p1,PROCEDURE,' ]] || false
    [[ "$output" =~ ',p2,PROCEDURE,' ]] || false

    # Drop p2
    dolt sql -q "DROP PROCEDURE p2"
    run dolt sql -b -q "SET @@show_external_procedures = 0;SELECT * FROM dolt_procedures" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "name,create_stmt,created_at,modified_at" ]] || false
    [[ "$output" =~ 'p1,CREATE PROCEDURE p1() SELECT 5*5' ]] || false
    [[ ! "$output" =~ 'p2,CREATE PROCEDURE p2() SELECT 6*6' ]] || false

    run dolt sql -b -q "SET @@show_external_procedures = 0;SHOW PROCEDURE STATUS" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "Db,Name,Type,Definer,Modified,Created,Security_type,Comment,character_set_client,collation_connection,Database Collation" ]] || false
    [[ "$output" =~ ',p1,PROCEDURE,' ]] || false
    [[ ! "$output" =~ ',p2,PROCEDURE,' ]] || false

    # Drop p2 again and error
    run dolt sql -q "DROP PROCEDURE p2"
    [ "$status" -eq "1" ]
    [[ "$output" =~ '"p2" does not exist' ]] || false

    # Drop p1 using if exists
    dolt sql -q "DROP PROCEDURE IF EXISTS p1"
    run dolt sql -b -q "SET @@show_external_procedures = 0;SELECT * FROM dolt_procedures" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "name,create_stmt,created_at,modified_at" ]] || false
    [[ ! "$output" =~ 'p1,CREATE PROCEDURE p1() SELECT 5*5' ]] || false
    [[ ! "$output" =~ 'p2,CREATE PROCEDURE p2() SELECT 6*6' ]] || false

    run dolt sql -b -q "SET @@show_external_procedures = 0;SHOW PROCEDURE STATUS" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "Db,Name,Type,Definer,Modified,Created,Security_type,Comment,character_set_client,collation_connection,Database Collation" ]] || false
    [[ ! "$output" =~ ',p1,PROCEDURE,' ]] || false
    [[ ! "$output" =~ ',p2,PROCEDURE,' ]] || false
}

@test "sql: check info_schema routines and parameters tables for stored procedures" {
    dolt sql <<SQL
CREATE TABLE inventory (item_id int primary key, shelf_id int, items varchar(100));
CREATE PROCEDURE in_stock (IN p_id INT, OUT p_count INT) SELECT COUNT(*) FROM inventory WHERE shelf_id = p_id INTO p_count;
SQL

    # check information_schema.PARAMETERS table
    run dolt sql -q "select specific_name, ordinal_position, parameter_mode, parameter_name, data_type, dtd_identifier, routine_type from information_schema.PARAMETERS;" -r csv
    [[ "$output" =~ "in_stock,1,IN,p_id,int,int,PROCEDURE" ]] || false
    [[ "$output" =~ "in_stock,2,OUT,p_count,int,int,PROCEDURE" ]] || false

    # check information_schema.ROUTINES table
    run dolt sql -q "select specific_name, routine_name, routine_type, routine_body, routine_definition from information_schema.ROUTINES;" -r csv
    [[ "$output" =~ "in_stock,in_stock,PROCEDURE,SQL,SELECT COUNT(*) FROM inventory WHERE shelf_id = p_id INTO p_count" ]] || false

    # check information_schema.ROUTINES table
    run dolt sql -q "select specific_name, is_deterministic, sql_data_access, security_type, routine_comment, definer, character_set_client, collation_connection, database_collation from information_schema.ROUTINES;" -r csv
    [[ "$output" =~ "in_stock,NO,CONTAINS SQL,DEFINER,\"\",\"\",utf8mb4,utf8mb4_0900_bin,utf8mb4_0900_bin" ]] || false
}

@test "sql: active_branch() func" {
    run dolt sql -q 'select active_branch()' -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ "active_branch()" ]] || false
    [[ "$output" =~ "main" ]] || false
}

@test "sql: active_branch() func on feature branch" {
    run dolt branch tmp_br
    run dolt checkout tmp_br
    run dolt sql -q 'select active_branch()' -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ "active_branch()" ]] || false
    [[ "$output" =~ "tmp_br" ]] || false

    run dolt sql -q 'select name from dolt_branches where name = (select active_branch())' -r csv
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "tmp_br" ]] || false
}

@test "sql: sql select current_user returns mysql syntax" {
    run dolt sql -q "select current_user" -r csv
    [ "$status" -eq 0 ]
    [ "${lines[0]}" = "current_user" ]
}

@test "sql: autocommit = off" {
    dolt sql -q "create table t1 (a int);"
    dolt commit -Am 'clean working set'

    run dolt sql <<SQL
set autocommit = off;
insert into t1 values (3), (5);
select * from t1;
SQL

    [ "$status" -eq 0 ]
    [[ "$output" =~ "3" ]] || false
    [[ "$output" =~ "5" ]] || false

    # no changes committed
    run dolt sql -q "select count(*) from t1" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "0" ]] || false

    run dolt diff
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 0 ]

    dolt sql <<SQL
set autocommit = off;
insert into t1 values (3), (5);
commit;
SQL

    run dolt sql -q "select count(*) from t1" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "2" ]] || false

    run dolt diff
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -ne 0 ]
}

@test "sql: found_row works with update properly" {
    run dolt sql  <<SQL
set autocommit = off;
CREATE TABLE tbl(pk int primary key, v1 int);
INSERT INTO tbl VALUES (1,1), (2,1);
UPDATE tbl set v1 = 1 where v1 = 1;
SELECT FOUND_ROWS();
SQL

    [ "$status" -eq 0 ]
    [[ "$output" =~ "| FOUND_ROWS() |" ]] || false
    [[ "$output" =~ "| 2            |" ]] || false
}

@test "sql: found_row works with update properly in batch mode" {
    run dolt sql <<SQL
CREATE TABLE tbl(pk int primary key, v1 int);
INSERT INTO tbl VALUES (1,1), (2,1);
UPDATE tbl set v1 = 1 where v1 = 1;
SELECT FOUND_ROWS();
SQL

    [ "$status" -eq 0 ]
    [[ "$output" =~ "| FOUND_ROWS() |" ]] || false
    [[ "$output" =~ "| 2            |" ]] || false
}

@test "sql: empty byte is parsed" {
    dolt sql -q "create table mytable(pk int, val bit);"
    run dolt sql -q "INSERT INTO mytable values (1, b'');"
    [ "$status" -eq 0 ]

    run dolt sql -q "SELECT pk, convert(val, unsigned) from mytable"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1  | 0" ]] || false
}

@test "sql: dolt diff table correctly works with IN" {
    dolt sql -q "CREATE TABLE mytable(pk int primary key);"
    dolt add .
    dolt sql -q "INSERT INTO mytable VALUES (1), (2)"
    dolt commit -am "Commit 1"

    head_commit=$(get_head_commit)

    run dolt sql -q "SELECT COUNT(*) from dolt_diff_mytable where dolt_diff_mytable.to_commit IN ('$head_commit', '00200202')"
    echo $head_commit
    echo $output
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| COUNT(*) |" ]] || false
    [[ "$output" =~ "| 2        |" ]] || false

    dolt sql -q "INSERT INTO mytable VALUES (3)"
    dolt commit -am "Commit 2"

    head_commit2=$(get_head_commit)

    run dolt sql -q "SELECT COUNT(*) from dolt_diff_mytable where dolt_diff_mytable.to_commit IN ('$head_commit', '$head_commit2', 'fake-val')"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| COUNT(*) |" ]] || false
    [[ "$output" =~ "| 3        |" ]] || false
}

@test "sql: dolt diff table correctly works with NOT and/or IS NULL" {
    dolt sql -q "CREATE TABLE t(pk int primary key);"
    dolt add .
    dolt commit -m "new table t"
    dolt sql -q "INSERT INTO t VALUES (1), (2)"
    dolt commit -am "add 1, 2"

    run dolt sql -q "SELECT COUNT(*) from dolt_diff_t where from_pk is null"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "2" ]] || false

    dolt sql -q "UPDATE t SET pk = 3 WHERE pk = 2"
    dolt commit -am "add 3"

    run dolt sql -q "SELECT COUNT(*) from dolt_diff_t where from_pk is not null"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1" ]] || false
}

@test "sql: dolt diff table correctly works with datetime comparisons" {
    dolt sql -q "CREATE TABLE t(pk int primary key);"
    dolt add .
    dolt commit -m "new table t"
    dolt sql -q "INSERT INTO t VALUES (1), (2), (3)"
    dolt commit -am "add 1, 2, 3"

    # adds a row and removes a row
    dolt sql -q "UPDATE t SET pk = 4 WHERE pk = 2"

    run dolt sql -q "SELECT COUNT(*) from dolt_diff_t where to_commit_date is not null"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "3" ]] || false

    run dolt sql -q "SELECT COUNT(*) from dolt_diff_t where to_commit_date < UTC_TIMESTAMP(6)"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "3" ]] || false
}

@test "sql: dolt diff table respects qualified database" {
    dolt sql -q "CREATE DATABASE db01; CREATE DATABASE db02;"
    dolt sql -q "USE db01; CREATE TABLE t01(pk int primary key);"
    run dolt sql -q "USE db01; SELECT * FROM dolt_diff;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "t01" ]] || false

    run dolt sql -q "USE db02; SELECT * FROM dolt_diff;"
    [ "$status" -eq 0 ]
    ! [[ "$output" =~ "t01" ]] || false

    run dolt sql -q "USE db02; SELECT * FROM db01.dolt_diff;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "t01" ]] || false
}

@test "sql: sql print on order by returns the correct result" {
    dolt sql -q "CREATE TABLE mytable(pk int primary key);"
    dolt sql -q "INSERT INTO mytable VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),(20)"

    # This is a valid test since the batch side for reading is 10. If race conditions exist than this test will be flaky.
    run dolt sql -r csv -q "SELECT * FROM mytable ORDER BY pk"
    [ "${lines[1]}" = "1" ]
    [ "${lines[2]}" = "2" ]
    [ "${lines[3]}" = "3" ]
    [ "${lines[4]}" = "4" ]
    [ "${lines[5]}" = "5" ]
    [ "${lines[6]}" = "6" ]
    [ "${lines[7]}" = "7" ]
    [ "${lines[8]}" = "8" ]
    [ "${lines[9]}" = "9" ]
    [ "${lines[10]}" = "10" ]
    [ "${lines[11]}" = "11" ]
    [ "${lines[12]}" = "12" ]
    [ "${lines[13]}" = "13" ]
    [ "${lines[14]}" = "14" ]
    [ "${lines[15]}" = "15" ]
    [ "${lines[16]}" = "16" ]
    [ "${lines[17]}" = "17" ]
    [ "${lines[18]}" = "18" ]
    [ "${lines[19]}" = "19" ]
    [ "${lines[20]}" = "20" ]
}

@test "sql: simple update against join works" {
    dolt sql -q "CREATE TABLE test(pk int primary key, val int);"
    dolt sql -q "INSERT INTO test values (1,1)"

    dolt sql -q "CREATE TABLE test2(pk int primary key)"
    dolt sql -q "insert into test2 values (1)"

    run dolt sql -q "UPDATE test INNER JOIN test2 on test.pk = test2.pk set val = 2"
    [ "$status" -eq 0 ]

    run dolt sql -r csv -q "SELECT * FROM test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,2" ]] || false
}

# regression test for query errors involving partial and full index matches
# See https://github.com/dolthub/dolt/issues/1131
@test "sql: covering indexes" {
    dolt sql <<SQL
CREATE TABLE test4 (
  a int NOT NULL,
  b int NOT NULL,
  c int NOT NULL,
  d int NOT NULL,
  PRIMARY KEY (a,b,c,d),
  KEY t4bc (b,c)
);
CREATE TABLE test2 (
  a int NOT NULL,
  b int,
  c int,
  PRIMARY KEY (a),
  KEY t2bc (b,c)
);
insert into test4 values (1,2,3,4), (5,6,7,8);
insert into test2 values (1,2,3), (4,5,6);
SQL

    # non indexed lookup
    run dolt sql -r csv -q "select * from test4 where b = 6;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "5,6,7,8" ]] || false

    # lookup on all columns in primary index
    run dolt sql -r csv -q "select * from test4 where a = 5 and b = 6 and c = 7 and d = 8;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "5,6,7,8" ]] || false

    # lookup on all columns in secondary index
    run dolt sql -r csv -q "select * from test4 where a = 5 and b = 6 and c = 7 and d = 8;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "5,6,7,8" ]] || false

    # lookup on covering index not part of primary key
    run dolt sql -r csv -q "select * from test2 where b = 5 and c = 6;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "4,5,6" ]] || false
}

get_head_commit() {
    dolt log -n 1 | grep -m 1 commit | cut -c 13-44
}

@test "sql: sql -q query vertical format check" {
    run dolt sql -r vertical -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "*************************** 1. row ***************************" ]] || false
    [[ "$output" =~ "Tables_in_dolt_repo" ]] || false
    [[ "$output" =~ ": has_datetimes" ]] || false
    [[ "$output" =~ "*************************** 2. row ***************************" ]] || false
    [[ "$output" =~ ": one_pk" ]] || false
    [[ "$output" =~ "*************************** 3. row ***************************" ]] || false
    [[ "$output" =~ ": two_pk" ]] || false

    dolt sql <<SQL
INSERT INTO one_pk (pk,c1,c2,c3,c4,c5) VALUES (4,40,40,40,40,40),(5,50,50,50,50,50),(6,60,60,60,60,60),(7,70,70,70,70,70);
INSERT INTO one_pk (pk,c1,c2,c3,c4,c5) VALUES (8,80,80,80,80,80),(9,90,90,90,90,90),(10,100,100,100,100,100),(11,110,110,110,110,110);
INSERT INTO one_pk (pk,c1,c2,c3,c4,c5) VALUES (12,120,120,120,120,120),(13,130,130,130,130,130);
SQL

    run dolt sql -r vertical -q "SELECT pk AS primaryKey FROM one_pk"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "*************************** 14. row ***************************" ]] || false
}

# bats test_tags=no_lambda
@test "sql: vertical query format in sql shell" {
    skiponwindows "Need to install expect and make this script work on windows."

    expect $BATS_TEST_DIRNAME/sql-vertical-format.expect
}

@test "sql: --file param" {
    cat > script.sql <<SQL
    drop table if exists test;
    create table test (a int primary key, b int);
    insert into test values (1,1), (2,2);
SQL

    run dolt sql --file script.sql
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Processed 100.0% of the file" ]] || false

    run dolt sql -q "select * from test" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,1" ]] || false

    run dolt sql --batch --file script.sql
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Processed 100.0% of the file" ]] || false

    run dolt sql -q "select * from test" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,1" ]] || false

    run dolt sql --file not-exists.sql
    [ "$status" -eq 1 ]
}

@test "sql: server with no dbs yet should be able to describe dolt stored procedures" {
    # make directories outside of the existing init'ed dolt repos
    tempDir=$(mktemp -d)
    cd $tempDir
    mkdir repo1
    cd repo1

    # check that without a DB we get descriptive errors
    run dolt sql -q "SHOW CREATE PROCEDURE dolt_clone;"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "no database selected" ]] || false

    run dolt sql -q "SHOW CREATE PROCEDURE dolt_branch;"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "no database selected" ]] || false

    # initialize dolt
    dolt init

    # check that the DB "repo1" exists
    run dolt sql -q "SHOW DATABASES;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "repo1" ]] || false

    # check that the DB "repo1" is selected
    run dolt sql -q "SELECT DATABASE();"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "repo1" ]] || false

    # check that current DB can be used
    run dolt sql -q "SHOW CREATE PROCEDURE dolt_branch;"
    [ "$status" -eq 0 ]

    # check that the qualified DB name can be used
    run dolt sql -q "SHOW CREATE PROCEDURE repo1.dolt_branch;"
    [ "$status" -eq 0 ]

    # check that procedures can be queried from multiple DBs
    run dolt sql -q "CREATE DATABASE repo2;"
    [ "$status" -eq 0 ]
    run dolt sql -q "SHOW CREATE PROCEDURE repo2.dolt_branch;"
    [ "$status" -eq 0 ]
    run dolt sql -q "USE repo1; SHOW CREATE PROCEDURE dolt_branch;"
    [ "$status" -eq 0 ]
}

@test "sql: can insert datetime with golang time struct zero value 0001-01-01 00:00:00" {
    dolt sql -q 'CREATE TABLE dts (created_at datetime NOT NULL);'
    run dolt sql -q 'INSERT INTO dts (`created_at`) VALUES ("0001-01-01 00:00:00");'
    [ "$status" -eq 0 ]
}

@test "sql: multi statement query returns accurate timing" {
  dolt sql -q "CREATE TABLE t(a int);"
  dolt sql -q "INSERT INTO t VALUES (1);"
  dolt sql -q "CREATE TABLE t1(b int);"
  run dolt sql <<SQL
insert into t1 (SELECT * FROM t WHERE EXISTS(SELECT SLEEP(1) UNION SELECT 1));
insert into t1 (SELECT * FROM t WHERE EXISTS(SELECT SLEEP(2) UNION SELECT 1));
insert into t1 (SELECT * FROM t WHERE EXISTS(SELECT SLEEP(3) UNION SELECT 1));
SQL
[[ "$output" =~ "Query OK, 1 row affected (1".*" sec)" ]] || false
[[ "$output" =~ "Query OK, 1 row affected (2".*" sec)" ]] || false
[[ "$output" =~ "Query OK, 1 row affected (3".*" sec)" ]] || false
}


@test "sql: check --data-dir used from a completely different location and still resolve DB names" {
    # remove config files
    rm -rf .doltcfg
    rm -rf db_dir

    mkdir db_dir
    cd db_dir
    ROOT_DIR=$(pwd)

    # create an alternate database, without the table
    mkdir dba
    cd dba
    dolt init
    cd ..
    dolt sql -q "create table dba_tbl (id int)"

    mkdir dbb
    cd dbb
    dolt init
    dolt sql -q "create table dbb_tbl (id int)"

    # Ensure --data-dir flag is really used by changing the cwd.
    cd /tmp

    run dolt --data-dir="$ROOT_DIR/dbb" sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "dbb_tbl" ]] || false

    run dolt --data-dir="$ROOT_DIR/dba" sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "dba_tbl" ]] || false

    # Default to first DB alphabetically.
    run dolt --data-dir="$ROOT_DIR" sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "dba_tbl" ]] || false

    # --use-db arg can be used to be specific.
    run dolt --data-dir="$ROOT_DIR" --use-db=dbb sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "dbb_tbl" ]] || false

    # Redundant use of the flag is OK.
    run dolt --data-dir="$ROOT_DIR/dbb" --use-db=dbb sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "dbb_tbl" ]] || false

    # Use of the use-db flag when we have a different DB specified by data-dir should error.
    run dolt --data-dir="$ROOT_DIR/dbb" --use-db=dba sql -q "show tables"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "provided --use-db dba does not exist" ]] || false
}

@test "sql: USE information schema and mysql databases" {
    run dolt sql <<SQL
USE information_schema;
show tables;
SQL

    # spot check result
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Database changed" ]] || false
    [[ "$output" =~ "columns" ]] || false

    run dolt sql <<SQL
USE mysql;
show tables;
SQL

    # spot check result
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Database changed" ]] || false
    [[ "$output" =~ "role_edges" ]] || false

}

@test "sql: prevent LOAD_FILE() from accessing files outside of working directory" {
    echo "should not be able to read this" > ../dont_read.txt
    echo "should be able to read this" > ./do_read.txt

    run dolt sql -q "select load_file('../dont_read.txt')";
    [ "$status" -eq 0 ]
    [[ "$output" =~ "NULL" ]] || false
    [[ "$output" != "should not be able to read this" ]] || false

    run dolt sql -q "select load_file('./do_read.txt')";
    [ "$status" -eq 0 ]
    [[ "$output" =~ "should be able to read this" ]] || false
}

@test "sql: ignore an empty .dolt directory" {
    mkdir empty_dir
    cd empty_dir

    mkdir .dolt
    dolt sql -q "select 1"
}

@test "sql: handle importing files with bom headers" {
    dolt sql < $BATS_TEST_DIRNAME/helper/with_utf8_bom.sql
    dolt table rm t1
    dolt sql < $BATS_TEST_DIRNAME/helper/with_utf16le_bom.sql
    dolt table rm t1
    dolt sql < $BATS_TEST_DIRNAME/helper/with_utf16be_bom.sql
    dolt table rm t1
}
