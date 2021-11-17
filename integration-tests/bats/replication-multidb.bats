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
        cd "${TMPDIRS}/dbs2/repo${i}"
    done
    #dolt push remote1 main
}

teardown() {
    teardown_common
    rm -rf $TMPDIRS
    cd $BATS_TMPDIR
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

#@test "replication-multidb: no push on cli commit" {

    #cd repo1
    #dolt config --local --add sqlserver.global.dolt_replicate_to_remote backup1
    #dolt sql -q "create table t1 (a int primary key)"
    #dolt commit -am "cm"

    #cd ..
    #run dolt clone file://./bac1 repo2
    #[ "$status" -eq 1 ]
#}

#@test "replication-multidb: push on cli engine commit" {
    #cd repo1
    #dolt config --local --add sqlserver.global.dolt_replicate_to_remote backup1
    #dolt sql -q "create table t1 (a int primary key)"
    #dolt sql -q "select dolt_commit('-am', 'cm')"

    #cd ..
    #dolt clone file://./bac1 repo2
    #cd repo2
    #run dolt ls
    #[ "$status" -eq 0 ]
    #[ "${#lines[@]}" -eq 2 ]
    #[[ "$output" =~ "t1" ]] || false
#}

#@test "replication-multidb: tag does not trigger replication-multidb" {
    #cd repo1
    #dolt config --local --add sqlserver.global.dolt_replicate_to_remote backup1
    #dolt tag

    #[ ! -d "../bac1/.dolt" ] || false
#}

#@test "replication-multidb: pull on read" {
    #dolt clone file://./rem1 repo2
    #cd repo2
    #dolt sql -q "create table t1 (a int primary key)"
    #dolt commit -am "new commit"
    #dolt push origin main

    #cd ../repo1
    #run dolt sql -q "show tables" -r csv
    #[ "$status" -eq 0 ]
    #[ "${#lines[@]}" -eq 1 ]
    #[[ ! "$output" =~ "t1" ]] || false

    #dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1
    #dolt config --local --add sqlserver.global.dolt_replicate_heads main
    #run dolt sql -q "show tables" -r csv
    #[ "$status" -eq 0 ]
    #[ "${#lines[@]}" -eq 2 ]
    #[[ "$output" =~ "t1" ]] || false
#}

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

#@test "replication-multidb: pull with unknown head" {
    #dolt clone file://./rem1 repo2
    #cd repo2
    #dolt branch new_feature
    #dolt push origin new_feature

    #cd ../repo1
    #dolt config --local --add sqlserver.global.dolt_replicate_heads main,unknown
    #dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1
    #run dolt sql -q "show tables"
    #[ "$status" -eq 1 ]
    #[[ ! "$output" =~ "panic" ]] || false
    #[[ "$output" =~ "replication-multidb failed: unable to find 'unknown' on 'remote1'; branch not found" ]] || false
#}

#@test "replication-multidb: pull multiple heads, one invalid branch name" {
    #dolt clone file://./rem1 repo2
    #cd repo2
    #dolt branch new_feature
    #dolt push origin new_feature

    #cd ../repo1
    #dolt config --local --add sqlserver.global.dolt_replicate_heads main,unknown
    #dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1
    #run dolt sql -q "show tables"
    #[ "$status" -eq 1 ]
    #[[ ! "$output" =~ "panic" ]] || false
    #[[ "$output" =~ "unable to find 'unknown' on 'remote1'; branch not found" ]] || false
#}

#@test "replication-multidb: pull with no head configuration fails" {
    #dolt clone file://./rem1 repo2
    #cd repo2
    #dolt branch new_feature
    #dolt push origin new_feature

    #cd ../repo1
    #dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1
    #run dolt sql -q "show tables"
    #[ "$status" -eq 1 ]
    #[[ ! "$output" =~ "panic" ]] || false
    #[[ "$output" =~ "invalid replicate heads setting: dolt_replicate_heads not set" ]] || false
#}

#@test "replication-multidb: replica pull conflicting head configurations" {
    #dolt clone file://./rem1 repo2
    #cd repo2
    #dolt branch new_feature
    #dolt push origin new_feature

    #cd ../repo1
    #dolt config --local --add sqlserver.global.dolt_replicate_heads main,unknown
    #dolt config --local --add sqlserver.global.dolt_replicate_all_heads 1
    #dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1
    #run dolt sql -q "show tables"
    #[ "$status" -eq 1 ]
    #[[ ! "$output" =~ "panic" ]] || false
    #[[ "$output" =~ "invalid replicate heads setting; cannot set both" ]] || false
#}


#@test "replication-multidb: replica pull multiple heads quiet warnings" {
    #dolt clone file://./rem1 repo2
    #cd repo2
    #dolt branch new_feature
    #dolt push origin new_feature

    #cd ../repo1
    #dolt config --local --add sqlserver.global.dolt_skip_replication-multidb_errors 1
    #dolt config --local --add sqlserver.global.dolt_replicate_heads unknown
    #dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1
    #run dolt sql -q "show tables"
    #[ "$status" -eq 0 ]
    #[[ ! "$output" =~ "panic" ]] || false
    #[[ "$output" =~ "replication-multidb failed: unable to find 'unknown' on 'remote1'; branch not found" ]] || false

    #run dolt checkout new_feature
    #[ "$status" -eq 1 ]
    #[[ ! "$output" =~ "panic" ]] || false
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

#@test "replication-multidb: pull all heads pulls tags" {
    #dolt clone file://./rem1 repo2
    #cd repo2
    #dolt checkout -b new_feature
    #dolt tag v1
    #dolt push origin new_feature
    #dolt push origin v1

    #cd ../repo1
    #dolt config --local --add sqlserver.global.dolt_replicate_all_heads 1
    #dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1
    #dolt sql -q "START TRANSACTION"
    #run dolt tag
    #[ "$status" -eq 0 ]
    #[ "${#lines[@]}" -eq 1 ]
    #[[ "$output" =~ "v1" ]] || false
#}

#@test "replication-multidb: push feature head" {
    #cd repo1
    #dolt config --local --add sqlserver.global.dolt_replicate_to_remote remote1
    #dolt checkout -b new_feature
    #dolt sql -q "create table t1 (a int primary key)"
    #dolt sql -q "select dolt_commit('-am', 'cm')"

    #cd ..
    #dolt clone file://./rem1 repo2
    #cd repo2
    #dolt fetch origin new_feature
#}

#@test "replication-multidb: push to unknown remote error" {
    #cd repo1
    #dolt config --local --add sqlserver.global.dolt_replicate_to_remote unknown
    #run dolt sql -q "create table t1 (a int primary key)"
    #[ "$status" -eq 1 ]
    #[[ ! "$output" =~ "panic" ]] || false
    #[[ "$output" =~ "failure loading hook; remote not found: 'unknown'" ]] || false
#}

#@test "replication-multidb: quiet push to unknown remote warnings" {
    #cd repo1
    #dolt config --local --add sqlserver.global.dolt_skip_replication-multidb_errors 1
    #dolt config --local --add sqlserver.global.dolt_replicate_to_remote unknown
    #run dolt sql -q "create table t1 (a int primary key)"
    #[ "$status" -eq 0 ]
    #[[ ! "$output" =~ "remote not found" ]] || false

    #run dolt sql -q "select dolt_commit('-am', 'cm')"
    #[ "$status" -eq 0 ]
    #[[ "$output" =~ "failure loading hook; remote not found: 'unknown'" ]] || false
    #[[ "$output" =~ "dolt_commit('-am', 'cm')" ]] || false
#}

#@test "replication-multidb: bad source doesn't error during non-transactional commands" {
    #cd repo1
    #dolt config --local --add sqlserver.global.dolt_read_replica_remote unknown
    #dolt config --local --add sqlserver.global.dolt_replicate_heads main

    #run dolt status
    #[ "$status" -eq 0 ]
    #[[ ! "$output" =~ "remote not found: 'unknown'" ]] || false
#}

#@test "replication-multidb: pull bad remote errors" {
    #cd repo1
    #dolt config --local --add sqlserver.global.dolt_read_replica_remote unknown
    #dolt config --local --add sqlserver.global.dolt_replicate_heads main

    #run dolt sql -q "show tables"
    #[ "$status" -eq 1 ]
    #[[ ! "$output" =~ "panic" ]]
    #[[ "$output" =~ "remote not found: 'unknown'" ]] || false
#}

#@test "replication-multidb: pull bad remote quiet warning" {
    #cd repo1
    #dolt config --local --add sqlserver.global.dolt_read_replica_remote unknown
    #dolt config --local --add sqlserver.global.dolt_replicate_heads main
    #dolt config --local --add sqlserver.global.dolt_skip_replication-multidb_errors 1

    #run dolt sql -q "show tables"
    #[ "$status" -eq 0 ]
    #[[ ! "$output" =~ "panic" ]]
    #[[ "$output" =~ "remote not found: 'unknown'" ]] || false
    #[[ "$output" =~ "dolt_replication-multidb_remote value is misconfigured" ]] || false
#}

#@test "replication-multidb: use database syntax fetches missing branch" {
    #dolt clone file://./rem1 repo2
    #cd repo2
    #dolt checkout -b feature-branch
    #dolt sql -q "create table t1 (a int primary key)"
    #dolt commit -am "new commit"
    #dolt push origin feature-branch

    #cd ../repo1
    #dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1
    #dolt config --local --add sqlserver.global.dolt_replicate_heads main
    #run dolt sql -b -q "USE \`repo1/feature-branch\`; show tables" -r csv
    #[ "$status" -eq 0 ]
    #[ "${#lines[@]}" -eq 4 ]
    #[[ "${lines[1]}" =~ "Table" ]] || false
    #[[ "${lines[2]}" =~ "t1" ]] || false
#}

#@test "replication-multidb: database autofetch doesn't change replication-multidb heads setting" {
    #dolt clone file://./rem1 repo2
    #cd repo2
    #dolt branch feature-branch
    #dolt push origin feature-branch

    #cd ../repo1
    #dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1
    #dolt config --local --add sqlserver.global.dolt_replicate_heads main
    #run dolt sql -q "use \`repo1/feature-branch\`"

    #cd ../repo2
    #dolt checkout feature-branch
    #dolt sql -q "create table t1 (a int primary key)"
    #dolt commit -am "new commit"
    #dolt push origin feature-branch

    #cd ../repo1
    #dolt sql -b -q "show tables" -r csv
    #[ "$status" -eq 0 ]
    #[[ ! "output" =~ "t1" ]] || false
#}
