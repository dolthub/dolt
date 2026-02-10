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
    	 call dolt_checkout('-b', 'feat');
    	 create table t (x int);
    	 call dolt_add('.');
    	 call dolt_commit('-am', 'cm');
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
