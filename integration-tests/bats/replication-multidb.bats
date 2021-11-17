#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

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
    #dolt push remote1 main
}

teardown() {
    teardown_common
    rm -rf $TMPDIRS
    cd $BATS_TMPDIR
}

#@test "replication-multidb: load global vars" {
    #dolt config --global --add sqlserver.global.dolt_replicate_to_remote remote1
    #cd dbs1/repo1
    #dolt config --local --add sqlserver.global.dolt_replicate_to_remote unknown
    #cd ../..
    #run dolt sql --multi-db-dir=dbs1 -b -q "select @@GLOBAL.dolt_replicate_to_remote"
    #[ "$status" -eq 0 ]
    #[[ "$output" =~ "remote1" ]] || false
#}

#@test "replication-multidb: push on sqlengine commit" {
    #dolt config --global --add sqlserver.global.dolt_replicate_to_remote remote1
    #dolt sql --multi-db-dir=dbs1 -b -q "use repo1; create table t1 (a int primary key)"
    #dolt sql --multi-db-dir=dbs1 -b -q "use repo1; select dolt_commit('-am', 'cm')"

    #clone_helper $TMPDIRS
    #run dolt sql --multi-db-dir=dbs2 -b -q "use repo1; show tables" -r csv
    #[ "$status" -eq 0 ]
    #[ "${#lines[@]}" -eq 4 ]
    #[[ "$output" =~ "t1" ]] || false
#}

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

#@test "replication-multidb: push on branch table update" {
    #cd repo1
    #dolt config --local --add sqlserver.global.dolt_replicate_to_remote backup1
    #dolt sql -q "create table t1 (a int primary key)"
    #dolt sql -q "UPDATE dolt_branches SET hash = COMMIT('--author', '{user_name} <{email_address}>','-m', 'cm') WHERE name = 'main' AND hash = @@repo1_head"

    #cd ..
    #dolt clone file://./bac1 repo2
    #cd repo2
    #run dolt ls
    #[ "$status" -eq 0 ]
    #[ "${#lines[@]}" -eq 2 ]
    #[[ "$output" =~ "t1" ]] || false
#}

#@test "replication-multidb: pull non-main head" {
    #dolt clone file://./rem1 repo2
    #cd repo2
    #dolt checkout -b new_feature
    #dolt sql -q "create table t1 (a int)"
    #dolt commit -am "cm"
    #dolt push origin new_feature

    #cd ../repo1
    #dolt config --local --add sqlserver.global.dolt_replicate_heads new_feature
    #dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1
    #run dolt sql -q "show tables as of hashof('new_feature')" -r csv
    #[ "$status" -eq 0 ]
    #[ "${#lines[@]}" -eq 2 ]
    #[[ "${lines[0]}" =~ "Table" ]] || false
    #[[ "${lines[1]}" =~ "t1" ]] || false
#}

#@test "replication-multidb: pull multiple heads" {
    #dolt clone file://./rem1 repo2
    #cd repo2
    #dolt checkout -b new_feature
    #dolt sql -q "create table t1 (a int)"
    #dolt commit -am "cm"
    #dolt push origin new_feature
    #dolt checkout main
    #dolt sql -q "create table t2 (a int)"
    #dolt commit -am "cm"
    #dolt push origin main

    #cd ../repo1
    #dolt config --local --add sqlserver.global.dolt_replicate_heads main,new_feature
    #dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1

    #run dolt sql -q "show tables as of hashof('new_feature')" -r csv
    #[ "$status" -eq 0 ]
    #[ "${#lines[@]}" -eq 2 ]
    #[[ "${lines[0]}" =~ "Table" ]] || false
    #[[ "${lines[1]}" =~ "t1" ]] || false

    #run dolt sql -q "show tables as of hashof('main')" -r csv
    #[ "$status" -eq 0 ]
    #[ "${#lines[@]}" -eq 2 ]
    #[[ "${lines[0]}" =~ "Table" ]] || false
    #[[ "${lines[1]}" =~ "t2" ]] || false
#}

#@test "replication-multidb: pull all heads" {
    #dolt clone file://./rem1 repo2
    #cd repo2
    #dolt checkout -b new_feature
    #dolt sql -q "create table t1 (a int)"
    #dolt commit -am "cm"
    #dolt push origin new_feature

    #cd ../repo1
    #dolt config --local --add sqlserver.global.dolt_replicate_all_heads 1
    #dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1
    #run dolt sql -q "show tables as of hashof('new_feature')" -r csv
    #[ "$status" -eq 0 ]
    #[ "${#lines[@]}" -eq 2 ]
    #[[ "${lines[0]}" =~ "Table" ]] || false
    #[[ "${lines[1]}" =~ "t1" ]] || false
#}

