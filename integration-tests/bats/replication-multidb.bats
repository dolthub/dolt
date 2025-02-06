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

    if ! [ "$DOLT_DEFAULT_BIN_FORMAT" = "__DOLT__" ]; then
      dolt config --list | awk '{ print $1 }' | grep sqlserver.global | xargs -r dolt config --global --unset
    fi
}

@test "replication-multidb: load global vars" {
    dolt config --global --add sqlserver.global.dolt_replicate_to_remote remote1
    cd dbs1/repo1
    dolt config --local --add sqlserver.global.dolt_replicate_to_remote unknown
    cd ../..
    run dolt --data-dir=dbs1 sql -b -q "select @@GLOBAL.dolt_replicate_to_remote"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "remote1" ]] || false
}

@test "replication-multidb: push on sqlengine commit" {
    dolt config --global --add sqlserver.global.dolt_replicate_to_remote remote1
    dolt --data-dir= dbs1 sql -b -q "use repo1; create table t1 (a int primary key)"
    dolt --data-dir= dbs1 sql -b -q "use repo1; call dolt_add('.')"
    dolt --data-dir= dbs1 sql -b -q "use repo1; call dolt_commit('-am', 'cm')"
    dolt --data-dir= dbs1 sql -b -q "use repo2; create table t2 (a int primary key)"
    dolt --data-dir= dbs1 sql -b -q "use repo2; call dolt_add('.')"
    dolt --data-dir= dbs1 sql -b -q "use repo2; call dolt_commit('-am', 'cm')"
    dolt --data-dir= dbs1 sql -b -q "use repo3; create table t3 (a int primary key)"
    dolt --data-dir= dbs1 sql -b -q "use repo3; call dolt_add('.')"
    dolt --data-dir= dbs1 sql -b -q "use repo3; call dolt_commit('-am', 'cm')"
    
    clone_helper $TMPDIRS
    run dolt --data-dir=dbs2 sql -b -q "use repo1; show tables" -r csv

    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
    [[ "$output" =~ "t1" ]] || false
    [[ ! "$output" =~ "t2" ]] || false
    [[ ! "$output" =~ "t3" ]] || false

    run dolt --data-dir=dbs2 sql -b -q "use repo2; show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
    [[ "$output" =~ "t2" ]] || false
    [[ ! n"$output" =~ "t1" ]] || false
    [[ ! "$output" =~ "t3" ]] || faalse

    run dolt --data-dir=dbs2 sql -b -q "use repo3; show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
    [[ "$output" =~ "t3" ]] || false
    [[ ! "$output" =~ "t1" ]] || false
    [[ ! "$output" =~ "t2" ]] || false
}

@test "replication-multidb: push newly created database" {
    dolt config --global --add sqlserver.global.dolt_replicate_to_remote remote1
    dolt sql -q "set @@persist.dolt_replication_remote_url_template = 'file://$TMPDIRS/rem1/{database}'"

    dolt --data-dir=dbs1 sql <<SQL
create database newdb;
use newdb;
create table new_table (b int primary key);
call dolt_add('.');
call dolt_commit('-am', 'new table');
SQL

    mkdir -p "${TMPDIRS}/dbs2"
    cd $TMPDIRS
    dolt clone "file://./rem1/newdb" "dbs2/newdb"

    # this is a hack: we have to change our persisted global server
    # vars for the sql command to work on the replica
    # TODO: fix this mess
    dolt config --global --unset sqlserver.global.dolt_replicate_to_remote
    
    run dolt --data-dir=dbs2 sql -q "use newdb; show tables" -r csv
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
    [[ "$output" =~ "new_table" ]] || false
}

@test "replication-multidb: push newly cloned database" {
    pushd .

    mkdir -p "${TMPDIRS}/rem2"
    # push all the dbs to remote1
    for i in {1..3}; do
        cd "${TMPDIRS}/dbs1/repo${i}"
        dolt push remote1 main
        # also create a new remote2 for each DB but don't push to it
        dolt remote add remote2 "file://../../rem2/repo${i}"
    done

    popd

    dolt config --global --add sqlserver.global.dolt_replicate_to_remote remote2
    dolt sql -q "set @@persist.dolt_replication_remote_url_template = 'file://$TMPDIRS/rem2/{database}'"

    mkdir -p "${TMPDIRS}/dbs2"
    dolt --data-dir=dbs2 sql <<SQL
call dolt_clone('file://${TMPDIRS}/rem1/repo1', 'repo1');
use repo1;
create table new_table (b int primary key);
call dolt_commit('-Am', 'new table');
call dolt_clone('file://${TMPDIRS}/rem1/repo2', 'repo2');
SQL

    mkdir -p "${TMPDIRS}/dbs3"
    cd $TMPDIRS
    dolt clone "file://./rem2/repo1" "dbs3/repo1"
    dolt clone "file://./rem2/repo2" "dbs3/repo2"

    # this is a hack: we have to change our persisted global server
    # vars for the sql command to work on the replica
    # TODO: fix this mess
    dolt config --global --unset sqlserver.global.dolt_replicate_to_remote
    
    run dolt --data-dir=dbs3 sql -q "use repo1; show tables" -r csv
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
    [[ "$output" =~ "new_table" ]] || false

    run dolt --data-dir=dbs3 sql -q "use repo2; show tables" -r csv
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ ! "$output" =~ "new_table" ]] || false
}

@test "replication-multidb: push newly created database with no commits" {
    dolt config --global --add sqlserver.global.dolt_replicate_to_remote remote1
    dolt sql -q "set @@persist.dolt_replication_remote_url_template = 'file://$TMPDIRS/rem1/{database}'"

    dolt --data-dir=dbs1 sql -q "create database newdb;"

    mkdir -p "${TMPDIRS}/dbs2"
    cd $TMPDIRS
    dolt clone "file://./rem1/newdb" "dbs2/newdb"

    # this is a hack: we have to change our persisted global server
    # vars for the sql command to work on the replica TODO: fix this
    # mess
    dolt config --global --unset sqlserver.global.dolt_replicate_to_remote
    
    run dolt --data-dir=dbs2 sql -q "use newdb; show tables" -r csv
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
}

@test "replication-multidb: pull on read" {
    push_helper $TMPDIRS
    dolt --data-dir=dbs1 sql -b -q "use repo1; create table t1 (a int primary key)"
    dolt --data-dir=dbs1 sql -b -q "use repo1; call dolt_add('.')"
    dolt --data-dir=dbs1 sql -b -q "use repo1; call dolt_commit('-am', 'cm')"
    dolt --data-dir=dbs1 sql -b -q "use repo2; create table t2 (a int primary key)"
    dolt --data-dir=dbs1 sql -b -q "use repo2; call dolt_add('.')"
    dolt --data-dir=dbs1 sql -b -q "use repo2; call dolt_commit('-am', 'cm')"
    dolt --data-dir=dbs1 sql -b -q "use repo3; create table t3 (a int primary key)"
    dolt --data-dir=dbs1 sql -b -q "use repo3; call dolt_add('.')"
    dolt --data-dir=dbs1 sql -b -q "use repo3; call dolt_commit('-am', 'cm')"

    clone_helper $TMPDIRS
    push_helper $TMPDIRS

    dolt config --global --add sqlserver.global.dolt_read_replica_remote remote1
    dolt config --global --add sqlserver.global.dolt_replicate_heads main

    run dolt --data-dir=dbs2 sql -b -q "use repo1; show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
    [[ "$output" =~ "t1" ]] || false
    [[ ! "$output" =~ "t2" ]] || false
    [[ ! "$output" =~ "t3" ]] || false

    run dolt --data-dir=dbs2 sql -b -q "use repo2; show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
    [[ "$output" =~ "t2" ]] || false
    [[ ! n"$output" =~ "t1" ]] || false
    [[ ! "$output" =~ "t3" ]] || faalse

    run dolt --data-dir=dbs2 sql -b -q "use repo3; show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
    [[ "$output" =~ "t3" ]] || false
    [[ ! "$output" =~ "t1" ]] || false
    [[ ! "$output" =~ "t2" ]] || false
}

@test "replication-multidb: pull newly created database" {
    dolt config --global --add sqlserver.global.dolt_replicate_to_remote remote1
    dolt sql -q "set @@persist.dolt_replication_remote_url_template = 'file://$TMPDIRS/rem1/{database}'"

    dolt --data-dir=dbs1 sql <<SQL
create database newdb;
use newdb;
create table new_table (b int primary key);
call dolt_add('.');
call dolt_commit('-am', 'new table');
SQL

    cd $TMPDIRS
    mkdir -p "dbs2"

    # this is a hack: we have to change our persisted global server
    # vars for the sql command to work on the replica TODO: fix this
    # mess
    dolt config --global --unset sqlserver.global.dolt_replicate_to_remote
    dolt config --global --add sqlserver.global.dolt_read_replica_remote remote1
    dolt config --global --add sqlserver.global.dolt_replicate_all_heads 1

    [ ! -d "dbs2/newdb" ]
    
    run dolt --data-dir=dbs2 sql -q "use newdb; show tables" -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ "new_table" ]] || false

    [ -d "dbs2/newdb" ]

    run dolt --data-dir=dbs2 sql -q "use not_exist; show tables" -r csv
    [ $status -ne 0 ]
    [[ "$output" =~ "database not found" ]] || false
}

@test "replication-multidb: missing database config" {
    dolt config --global --add sqlserver.global.dolt_replicate_to_remote unknown
    run dolt --data-dir=dbs1 sql -b -q "use repo1; create table t1 (a int primary key)"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "remote not found: 'unknown'" ]] || false
    [[ "$output" =~ "replication disabled" ]] || false
}

@test "replication-multidb: missing database config quiet warning" {
    dolt config --global --add sqlserver.global.dolt_replicate_to_remote unknown
    dolt config --global --add sqlserver.global.dolt_skip_replication_errors 1
    run dolt --data-dir=dbs1 sql -b -q "use repo1; create table t1 (a int primary key)"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "remote not found: 'unknown'" ]] || false
    [[ "$output" =~ "replication disabled" ]] || false
}

@test "replication-multidb: sql-server push on commit" {
    dolt config --global --add sqlserver.global.dolt_replicate_to_remote remote1
    cd dbs1
    start_multi_db_server repo1
    cd ..

    dolt --use-db repo1 --port $PORT --host 0.0.0.0 --no-tls sql -q "create table t1 (a int primary key)"
    dolt --use-db repo1 --port $PORT --host 0.0.0.0 --no-tls sql -q "call dolt_add('.')"
    dolt --use-db repo1 --port $PORT --host 0.0.0.0 --no-tls sql -q "call dolt_commit('-am', 'cm')"
    dolt --use-db repo2 --port $PORT --host 0.0.0.0 --no-tls sql -q "create table t2 (a int primary key)"
    dolt --use-db repo2 --port $PORT --host 0.0.0.0 --no-tls sql -q "call dolt_add('.')"
    dolt --use-db repo2 --port $PORT --host 0.0.0.0 --no-tls sql -q "call dolt_commit('-am', 'cm')"
    dolt --use-db repo3 --port $PORT --host 0.0.0.0 --no-tls sql -q "create table t3 (a int primary key)"
    dolt --use-db repo3 --port $PORT --host 0.0.0.0 --no-tls sql -q "call dolt_add('.')"
    dolt --use-db repo3 --port $PORT --host 0.0.0.0 --no-tls sql -q "call dolt_commit('-am', 'cm')"

    clone_helper $TMPDIRS

    run dolt --data-dir=dbs2 sql -b -q "use repo1; show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
    [[ "$output" =~ "t1" ]] || false
    [[ ! "$output" =~ "t2" ]] || false
    [[ ! "$output" =~ "t3" ]] || false

    run dolt --data-dir=dbs2 sql -b -q "use repo2; show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
    [[ "$output" =~ "t2" ]] || false
    [[ ! n"$output" =~ "t1" ]] || false
    [[ ! "$output" =~ "t3" ]] || faalse

    run dolt --data-dir=dbs2 sql -b -q "use repo3; show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
    [[ "$output" =~ "t3" ]] || false
    [[ ! "$output" =~ "t1" ]] || false
    [[ ! "$output" =~ "t2" ]] || false
}

# Test that a Dolt sql-server automatically pulls new commits from a remote for new databases
# that have been automatically cloned.
@test "replication-multidb: sql-server pull on read for new databases" {
    # Create test data in repo1 and push it to remote1
    cd "${TMPDIRS}/dbs1/repo1"
    dolt sql -q "create table t1 (pk int primary key); insert into t1 values (42);"
    dolt commit -Am "creating table t1"
    dolt push remote1 main:main

    # Create test data in repo2 and push it to remote1
    cd "${TMPDIRS}/dbs1/repo2"
    dolt sql -q "create table t2 (pk int primary key); insert into t2 values (123);"
    dolt commit -Am "creating table t2"
    dolt push remote1 main:main

    # Start a read replica server that replicates from the remotes on disk
    mkdir "${TMPDIRS}/readReplicaServer"
    cd "${TMPDIRS}/readReplicaServer"
    dolt config --global --add sqlserver.global.dolt_read_replica_remote remote1
    dolt config --global --add sqlserver.global.dolt_replicate_heads main
    dolt sql -q "set @@persist.dolt_replication_remote_url_template = 'file://$TMPDIRS/rem1/{database}'"
    start_multi_db_server information_schema

    # Unset the global config so that the other databases don't pick up
    # these values when we use them.
    dolt config --global --unset sqlserver.global.dolt_read_replica_remote
    dolt config --global --unset sqlserver.global.dolt_replicate_heads

    # Assert that no databases are synced to the read replica server yet
    run dolt --port $PORT --host 0.0.0.0 --no-tls sql -q "show databases"
    [ $status -eq 0 ]
    [[ "$output" =~ "information_schema" ]] || false
    [[ ! "$output" =~ "repo1" ]] || false
    [[ ! "$output" =~ "repo2" ]] || false

    # Sync repo1 to the read replica server by use'ing it
    run dolt --port $PORT --host 0.0.0.0 --no-tls sql -q "use repo1; show databases;"
    [ $status -eq 0 ]
    [[ "$output" =~ "information_schema" ]] || false
    [[ "$output" =~ "repo1" ]] || false
    [[ ! "$output" =~ "repo2" ]] || false

    # Sync repo1 by using it
    run dolt --use-db repo1 --port $PORT --host 0.0.0.0 --no-tls sql -q "select * from t1;"
    [ $status -eq 0 ]
    [[ "$output" =~ "42" ]] || false

    # Push a new change to repo1
    cd "${TMPDIRS}/dbs1/repo1"
    dolt sql -q "insert into t1 values (43);"
    dolt commit -Am "Adding row 43"
    dolt push remote1 main:main

    # Verify that the changes from repo1 have been pulled
    run dolt --use-db repo1 --port $PORT --host 0.0.0.0 --no-tls sql -q "select * from t1;"
    [ $status -eq 0 ]
    [[ "$output" =~ "42" ]] || false
    [[ "$output" =~ "43" ]] || false
}

@test "replication-multidb: sql-server pull on read" {
    push_helper $TMPDIRS
    dolt --data-dir=dbs1 sql -b -q "use repo1; create table t1 (a int primary key)"
    dolt --data-dir=dbs1 sql -b -q "use repo1; call dolt_add('.')"
    dolt --data-dir=dbs1 sql -b -q "use repo1; call dolt_commit('-am', 'cm')"
    dolt --data-dir=dbs1 sql -b -q "use repo2; create table t2 (a int primary key)"
    dolt --data-dir=dbs1 sql -b -q "use repo2; call dolt_add('.')"
    dolt --data-dir=dbs1 sql -b -q "use repo2; call dolt_commit('-am', 'cm')"
    dolt --data-dir=dbs1 sql -b -q "use repo3; create table t3 (a int primary key)"
    dolt --data-dir=dbs1 sql -b -q "use repo3; call dolt_add('.')"
    dolt --data-dir=dbs1 sql -b -q "use repo3; call dolt_commit('-am', 'cm')"

    clone_helper $TMPDIRS
    push_helper $TMPDIRS

    dolt config --global --add sqlserver.global.dolt_read_replica_remote remote1
    dolt config --global --add sqlserver.global.dolt_replicate_heads main
    cd dbs1
    start_multi_db_server repo1
    
    run dolt --use-db repo1 sql -q "show tables"
    [ $status -eq 0 ]
    [[ "$output" =~ Tables_in_repo1 ]] || false
    [[ "$output" =~ t1 ]] || false
    
    run dolt --use-db repo2 sql -q "show tables"
    [ $status -eq 0 ]
    [[ "$output" =~ Tables_in_repo2 ]] || false
    [[ "$output" =~ t2 ]] || false
    
    run dolt --use-db repo3 sql -q "show tables"
    [ $status -eq 0 ]
    [[ "$output" =~ Tables_in_repo3 ]] || false
    [[ "$output" =~ t3 ]] || false
}

@test "replication-multidb: sql-server pull non-current head" {
    push_helper $TMPDIRS
    dolt --data-dir=dbs1 sql <<SQL
use repo1; 
create table t1 (a int primary key);
insert into t1 values (1);
call dolt_add('.');
call dolt_commit('-am', 'initial table');
call dolt_checkout('-b', 'b2');
update t1 set a = 2;
call dolt_commit('-am', 'new values');
SQL

    clone_helper $TMPDIRS
    push_helper $TMPDIRS

    dolt config --global --add sqlserver.global.dolt_read_replica_remote remote1
    dolt config --global --add sqlserver.global.dolt_replicate_heads main

    cd dbs1
    start_multi_db_server repo1
    
    run dolt --use-db repo1/b2 sql -q "select * from t1"
    [ $status -eq 0 ]
    [[ "$output" =~ 2 ]] || false
    [[ ! "$output" =~ 1 ]] || false

    run dolt --use-db repo1/b2 sql -q "select count(*) from dolt_status"
    [ $status -eq 0 ]
    [[ "$output" =~ 0 ]] || false

    stop_sql_server

    cd ..
    dolt --data-dir=dbs1 sql <<SQL
use repo1; 
call dolt_checkout('b2');
update t1 set a = 3;
call dolt_commit('-am', 'new values again');
SQL

    cd dbs1
    start_multi_db_server repo1
    
    run dolt --use-db repo1/b2 sql -q "select * from t1"
    [ $status -eq 0 ]
    [[ "$output" =~ 3 ]] || false

    run dolt --use-db repo1/b2 sql -q "select count(*) from dolt_status"
    [ $status -eq 0 ]
    [[ "$output" =~ 0 ]] || false
}
