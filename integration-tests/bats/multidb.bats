#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash
load $BATS_TEST_DIRNAME/helper/query-server-common.bash

setup() {
    setup_common
    TMPDIRS=$(pwd)/tmpdirs

    init_helper $TMPDIRS
    cd $TMPDIRS
}

init_helper() {
    TMPDIRS=$1
    mkdir -p "${TMPDIRS}/dbs1"
    for i in {1..2}; do
        mkdir "${TMPDIRS}/dbs1/repo${i}"
        cd "${TMPDIRS}/dbs1/repo${i}"
        dolt init
    done
}

teardown() {
    stop_sql_server
    teardown_common
    rm -rf $TMPDIRS
    cd $BATS_TMPDIR
}

@test "multidb: database default branches" {
    cd dbs1
    start_multi_db_server repo1
    dolt sql -q "create database new;
    	 use new;
    	 call dcheckout('-b', 'feat');
    	 create table t (x int);
    	 call dolt_add('.');
    	 call dcommit('-am', 'cm');
    	 set @@global.new_default_branch='feat'"
    dolt sql -q "use repo1"
}

@test "multidb: incompatible BIN FORMATs" {
    mkdir dbs1/repo4
    cd dbs1/repo4
    DOLT_DEFAULT_BIN_FORMAT=__DOLT__ dolt init
    cd ..

    dolt --help
    dolt sql -q "show tables"
}

make__LD_1__db() {
    mkdir $1
    cd $1
    DOLT_DEFAULT_BIN_FORMAT=__LD_1__ dolt init
    cd ..
}

make__DOLT__db() {
    mkdir $1
    cd $1
    DOLT_DEFAULT_BIN_FORMAT=__DOLT__ dolt init
    cd ..
}

@test "multidb: databases are hidden based on DOLT_DEFAULT_BIN_FORMAT where there is no database in ./.dolt" {
    cd dbs1

    make__DOLT__db new1
    make__LD_1__db old1
    make__LD_1__db old2

    orig_bin_format=$DOLT_DEFAULT_BIN_FORMAT

    export DOLT_DEFAULT_BIN_FORMAT=__LD_1__
    run dolt sql -q "SELECT 1;"
    [ $status -eq 0 ]
    [[ "$output" =~ "incompatible format for database 'new1'; expected '__LD_1__', found '__DOLT__'" ]] || false

    run dolt sql -q "SHOW DATABASES;"
    [ $status -eq 0 ]
    echo $output
    [[ "$output" =~ "| old1" ]] || false
    [[ "$output" =~ "| old2" ]] || false
    [[ ! "$output" =~ "| new1" ]] || false

    export DOLT_DEFAULT_BIN_FORMAT=__DOLT__
    run dolt sql -q "SELECT 1;"
    [ $status -eq 0 ]
    [[ "$output" =~ "incompatible format for database 'old1'; expected '__DOLT__', found '__LD_1__'" ]] || false
    [[ "$output" =~ "incompatible format for database 'old2'; expected '__DOLT__', found '__LD_1__'" ]] || false

    run dolt sql -q "SHOW DATABASES;"
    [ $status -eq 0 ]
    [[ ! "$output" =~ "| old1" ]] || false
    [[ ! "$output" =~ "| old2" ]] || false
    [[ "$output" =~ "| new1" ]] || false

    export DOLT_DEFAULT_BIN_FORMAT=$orig_bin_format
}

@test "multidb: additional databases are hidden based on the format of the database in ./.dolt" {
    rm -r dbs1
    mkdir test_db
    cd test_db

    make__DOLT__db new1
    make__LD_1__db old1

    export DOLT_DEFAULT_BIN_FORMAT="__DOLT__"
    dolt init
    run dolt sql -q "SHOW DATABASES;"
    [ $status -eq 0 ]
    [[ "$output" =~ "incompatible format for database 'old1'; expected '__DOLT__', found '__LD_1__'" ]] || false
    [[ "$output" =~ "| test_db" ]] || false
    [[ "$output" =~ "| new1" ]] || false
    [[ ! "$output" =~ "| old1" ]] || false

    rm -r .dolt
    export DOLT_DEFAULT_BIN_FORMAT="__LD_1__"
    dolt init
    run dolt sql -q "SHOW DATABASES;"
    [ $status -eq 0 ]
    [[ "$output" =~ "incompatible format for database 'new1'; expected '__LD_1__', found '__DOLT__'" ]] || false
    [[ "$output" =~ "| test_db" ]] || false
    [[ ! "$output" =~ "| new1" ]] || false
    [[ "$output" =~ "| old1" ]] || false
}

