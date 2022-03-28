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
    for i in {1..3}; do
        mkdir "${TMPDIRS}/dbs1/repo${i}"
        cd "${TMPDIRS}/dbs1/repo${i}"
        dolt init
        mkdir -p "${TMPDIRS}/rem1/repo${i}"
        dolt remote add remote1 "file://../../rem1/repo${i}"
    done
}

clone_helper() {
    TMPDIRS=$1
    mkdir -p "${TMPDIRS}/dbs2"
    for i in {1..3}; do
        cd $TMPDIRS
        if [ -f "rem1/repo${i}/manifest" ]; then
            dolt clone "file://./rem1/repo${i}" "dbs2/repo${i}"
            cd "dbs2/repo${i}"
            dolt remote add remote1 "file://../../rem1/repo${i}"
        fi
    done
    cd $TMPDIRS
}

push_helper() {
    TMPDIRS=$1
    for i in {1..3}; do
        cd "${TMPDIRS}/dbs1/repo${i}"
        dolt push remote1 main
    done
    cd $TMPDIRS
}

teardown() {
    stop_sql_server
    teardown_common
    rm -rf $TMPDIRS
    cd $BATS_TMPDIR

    dolt config --list | awk '{ print $1 }' | grep sqlserver.global | xargs dolt config --global --unset
}

@test "replication-multidb: load global vars" {
    dolt config --global --add sqlserver.global.dolt_replicate_to_remote remote1
    cd dbs1/repo1
    dolt config --local --add sqlserver.global.dolt_replicate_to_remote unknown
    cd ../..
    run dolt sql --multi-db-dir=dbs1 -b -q "select @@GLOBAL.dolt_replicate_to_remote"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "remote1" ]] || false
}

@test "replication-multidb: push on sqlengine commit" {
    dolt config --global --add sqlserver.global.dolt_replicate_to_remote remote1
    dolt sql --multi-db-dir=dbs1 -b -q "use repo1; create table t1 (a int primary key)"
    dolt sql --multi-db-dir=dbs1 -b -q "use repo1; select dolt_commit('-am', 'cm')"

    clone_helper $TMPDIRS
    run dolt sql --multi-db-dir=dbs2 -b -q "use repo1; show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 4 ]
    [[ "$output" =~ "t1" ]] || false
}

@test "replication-multidb: pull on read" {
    push_helper $TMPDIRS
    dolt sql --multi-db-dir=dbs1 -b -q "use repo1; create table t1 (a int primary key)"
    dolt sql --multi-db-dir=dbs1 -b -q "use repo1; select dolt_commit('-am', 'cm')"

    clone_helper $TMPDIRS
    push_helper $TMPDIRS

    dolt config --global --add sqlserver.global.dolt_read_replica_remote remote1
    dolt config --global --add sqlserver.global.dolt_replicate_heads main
    run dolt sql --multi-db-dir=dbs2 -b -q "use repo1; show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 4 ]
    [[ "$output" =~ "t1" ]] || false
}

@test "replication-multidb: missing database config" {
    dolt config --global --add sqlserver.global.dolt_replicate_to_remote unknown
    run dolt sql --multi-db-dir=dbs1 -b -q "use repo1; create table t1 (a int primary key)"
    [ "$status" -eq 1 ]
    [[ ! "$output" =~ "panic" ]] || false
    [[ "$output" =~ "remote not found: 'unknown'" ]] || false
}

@test "replication-multidb: missing database config quiet warning" {
    dolt config --global --add sqlserver.global.dolt_replicate_to_remote unknown
    dolt config --global --add sqlserver.global.dolt_skip_replication_errors 1
    dolt sql --multi-db-dir=dbs1 -b -q "use repo1; create table t1 (a int primary key)"
}

@test "replication-multidb: sql-server push on commit" {
    dolt config --global --add sqlserver.global.dolt_replicate_to_remote remote1
    cd dbs1
    start_multi_db_server repo1
    cd ..

    server_query repo1 1 "create table t1 (a int primary key)"
    multi_query repo1 1 "select dolt_commit('-am', 'cm')"

    clone_helper $TMPDIRS
    run dolt sql --multi-db-dir=dbs2 -b -q "use repo1; show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 4 ]
    [[ "$output" =~ "t1" ]] || false
}

@test "replication-multidb: sql-server pull on read" {
    push_helper $TMPDIRS
    dolt sql --multi-db-dir=dbs1 -b -q "use repo1; create table t1 (a int primary key)"
    dolt sql --multi-db-dir=dbs1 -b -q "use repo1; select dolt_commit('-am', 'cm')"

    clone_helper $TMPDIRS
    push_helper $TMPDIRS

    dolt config --global --add sqlserver.global.dolt_read_replica_remote remote1
    dolt config --global --add sqlserver.global.dolt_replicate_heads main
    cd dbs1
    start_multi_db_server repo1
    server_query repo1 1 "show tables" "Tables_in_repo1\nt1"
}
