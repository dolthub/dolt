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
    multi_query repo1 1 "create database new; use new; call dcheckout('-b', 'feat'); create table t (x int); call dcommit('-am', 'cm'); set @@global.new_default_branch='feat'"
    server_query repo1 1 "use repo1"
}
