#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    skip_nbf_dolt_1

    TMPDIRS=$(pwd)/tmpdirs
    mkdir -p $TMPDIRS/{bac1,rem1,repo1}

    # repo1 -> bac1 -> repo2
    cd $TMPDIRS/repo1
    dolt init
    dolt branch feature
    dolt remote add backup1 file://../bac1
    dolt remote add remote1 file://../rem1
    dolt push remote1 main
    cd $TMPDIRS
}

teardown() {
    teardown_common
    rm -rf $TMPDIRS
    cd $BATS_TMPDIR
}

@test "replication: default no replication" {
    cd repo1
    dolt sql -q "create table t1 (a int primary key)"
    dolt commit -am "cm"

    [ ! -d "../bac1/.dolt" ] || false
}

@test "replication: no push on cli commit" {

    cd repo1
    dolt config --local --add sqlserver.global.dolt_replicate_to_remote backup1
    dolt sql -q "create table t1 (a int primary key)"
    dolt commit -am "cm"

    cd ..
    run dolt clone file://./bac1 repo2
    [ "$status" -eq 1 ]
}

@test "replication: push on cli engine commit" {
    cd repo1
    dolt config --local --add sqlserver.global.dolt_replicate_to_remote backup1
    dolt sql -q "create table t1 (a int primary key)"
    dolt sql -q "select dolt_commit('-am', 'cm')"

    cd ..
    dolt clone file://./bac1 repo2
    cd repo2
    run dolt ls
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "t1" ]] || false
}

@test "replication: tag does not trigger replication" {
    cd repo1
    dolt config --local --add sqlserver.global.dolt_replicate_to_remote backup1
    dolt tag

    [ ! -d "../bac1/.dolt" ] || false
}

@test "replication: pull on read" {
    dolt clone file://./rem1 repo2
    cd repo2
    dolt sql -q "create table t1 (a int primary key)"
    dolt commit -am "new commit"
    dolt push origin main

    cd ../repo1
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]
    [[ ! "$output" =~ "t1" ]] || false

    dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1
    dolt config --local --add sqlserver.global.dolt_replicate_heads main
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "t1" ]] || false
}

@test "replication: push on branch table update" {
    cd repo1
    dolt config --local --add sqlserver.global.dolt_replicate_to_remote backup1
    dolt sql -q "create table t1 (a int primary key)"
    dolt add -A
    dolt sql -q "UPDATE dolt_branches SET hash = COMMIT('--author', '{user_name} <{email_address}>','-m', 'cm') WHERE name = 'main' AND hash = @@repo1_head"

    cd ..
    dolt clone file://./bac1 repo2
    cd repo2
    run dolt ls
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "t1" ]] || false
}

@test "replication: pull non-main head" {
    dolt clone file://./rem1 repo2
    cd repo2
    dolt checkout -b new_feature
    dolt sql -q "create table t1 (a int)"
    dolt commit -am "cm"
    dolt push origin new_feature

    cd ../repo1
    dolt config --local --add sqlserver.global.dolt_replicate_heads new_feature
    dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1
    run dolt sql -q "show tables as of hashof('new_feature')" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "${lines[0]}" =~ "Table" ]] || false
    [[ "${lines[1]}" =~ "t1" ]] || false
}

@test "replication: pull multiple heads" {
    dolt clone file://./rem1 repo2
    cd repo2
    dolt checkout -b new_feature
    dolt sql -q "create table t1 (a int)"
    dolt commit -am "cm"
    dolt push origin new_feature
    dolt checkout main
    dolt sql -q "create table t2 (a int)"
    dolt commit -am "cm"
    dolt push origin main

    cd ../repo1
    dolt config --local --add sqlserver.global.dolt_replicate_heads main,new_feature
    dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1

    run dolt sql -q "show tables as of hashof('new_feature')" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "${lines[0]}" =~ "Table" ]] || false
    [[ "${lines[1]}" =~ "t1" ]] || false

    run dolt sql -q "show tables as of hashof('main')" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "${lines[0]}" =~ "Table" ]] || false
    [[ "${lines[1]}" =~ "t2" ]] || false
}

@test "replication: pull with unknown head" {
    dolt clone file://./rem1 repo2
    cd repo2
    dolt branch new_feature
    dolt push origin new_feature

    cd ../repo1
    dolt config --local --add sqlserver.global.dolt_replicate_heads main,unknown
    dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1
    run dolt sql -q "show tables"
    [ "$status" -eq 1 ]
    [[ ! "$output" =~ "panic" ]] || false
    [[ "$output" =~ "replication failed: unable to find 'unknown' on 'remote1'; branch not found" ]] || false
}

@test "replication: pull multiple heads, one invalid branch name" {
    dolt clone file://./rem1 repo2
    cd repo2
    dolt branch new_feature
    dolt push origin new_feature

    cd ../repo1
    dolt config --local --add sqlserver.global.dolt_replicate_heads main,unknown
    dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1
    run dolt sql -q "show tables"
    [ "$status" -eq 1 ]
    [[ ! "$output" =~ "panic" ]] || false
    [[ "$output" =~ "unable to find 'unknown' on 'remote1'; branch not found" ]] || false
}

@test "replication: pull with no head configuration fails" {
    dolt clone file://./rem1 repo2
    cd repo2
    dolt branch new_feature
    dolt push origin new_feature

    cd ../repo1
    dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1
    run dolt sql -q "show tables"
    [ "$status" -eq 1 ]
    [[ ! "$output" =~ "panic" ]] || false
    [[ "$output" =~ "invalid replicate heads setting: dolt_replicate_heads not set" ]] || false
}

@test "replication: replica pull conflicting head configurations" {
    dolt clone file://./rem1 repo2
    cd repo2
    dolt branch new_feature
    dolt push origin new_feature

    cd ../repo1
    dolt config --local --add sqlserver.global.dolt_replicate_heads main,unknown
    dolt config --local --add sqlserver.global.dolt_replicate_all_heads 1
    dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1
    run dolt sql -q "show tables"
    [ "$status" -eq 1 ]
    [[ ! "$output" =~ "panic" ]] || false
    [[ "$output" =~ "invalid replicate heads setting; cannot set both" ]] || false
}


@test "replication: replica pull multiple heads quiet warnings" {
    dolt clone file://./rem1 repo2
    cd repo2
    dolt branch new_feature
    dolt push origin new_feature

    cd ../repo1
    dolt config --local --add sqlserver.global.dolt_skip_replication_errors 1
    dolt config --local --add sqlserver.global.dolt_replicate_heads unknown
    dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1
    run dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "panic" ]] || false
    [[ "$output" =~ "replication failed: unable to find 'unknown' on 'remote1'; branch not found" ]] || false

    run dolt checkout new_feature
    [ "$status" -eq 1 ]
    [[ ! "$output" =~ "panic" ]] || false
}

@test "replication: pull all heads from feat" {
    cd repo1
    dolt branch -c main feat
    dolt push remote1 feat
    cd ..
    # clone repo2 from repo1
    dolt clone file://./rem1 repo2
    cd repo2
    dolt checkout feat
    dolt checkout main
    # add tables to repo1
    cd ../repo1
    dolt checkout feat
    dolt sql -q "create table t1 (a int)"
    dolt sql -q "create table t2 (a int)"
    dolt commit -am "cm"
    # remote1 has tables
    dolt push remote1 feat
    # repo2 replication config
    cd ../repo2
    dolt config --local --add sqlserver.global.dolt_replicate_all_heads 1
    dolt config --local --add sqlserver.global.dolt_read_replica_remote origin
    # repo2 pulls on read
    run dolt sql -r csv -b <<SQL
call dolt_checkout('feat');
show tables;
SQL
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 4 ]
    [[ "${lines[0]}" =~ "Tables_in_repo2" ]] || false
    [[ "${lines[1]}" =~ "t1" ]] || false
    [[ "${lines[2]}" =~ "t2" ]] || false
}

@test "replication: pull all heads" {
    dolt clone file://./rem1 repo2
    cd repo2
    dolt checkout -b new_feature
    dolt sql -q "create table t1 (a int)"
    dolt commit -am "cm"
    dolt push origin new_feature

    cd ../repo1
    dolt config --local --add sqlserver.global.dolt_replicate_all_heads 1
    dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1
    run dolt sql -q "show tables as of hashof('new_feature')" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "${lines[0]}" =~ "Table" ]] || false
    [[ "${lines[1]}" =~ "t1" ]] || false
}

@test "replication: pull all heads pulls tags" {
    dolt clone file://./rem1 repo2
    cd repo2
    dolt checkout -b new_feature
    dolt tag v1
    dolt push origin new_feature
    dolt push origin v1

    cd ../repo1
    dolt config --local --add sqlserver.global.dolt_replicate_all_heads 1
    dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1
    dolt sql -q "START TRANSACTION"
    run dolt tag
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]
    [[ "$output" =~ "v1" ]] || false
}

@test "replication: push feature head" {
    cd repo1
    dolt config --local --add sqlserver.global.dolt_replicate_to_remote remote1
    dolt checkout -b new_feature
    dolt sql -q "create table t1 (a int primary key)"
    dolt sql -q "select dolt_commit('-am', 'cm')"

    cd ..
    dolt clone file://./rem1 repo2
    cd repo2
    dolt fetch origin new_feature
}

@test "replication: push to unknown remote error" {
    cd repo1
    dolt config --local --add sqlserver.global.dolt_replicate_to_remote unknown
    run dolt sql -q "create table t1 (a int primary key)"
    [ "$status" -eq 1 ]
    [[ ! "$output" =~ "panic" ]] || false
    [[ "$output" =~ "failure loading hook; remote not found: 'unknown'" ]] || false
}

@test "replication: quiet push to unknown remote warnings" {
    cd repo1
    dolt config --local --add sqlserver.global.dolt_skip_replication_errors 1
    dolt config --local --add sqlserver.global.dolt_replicate_to_remote unknown
    run dolt sql -q "create table t1 (a int primary key)"
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "remote not found" ]] || false

    run dolt sql -q "select dolt_commit('-am', 'cm')"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "failure loading hook; remote not found: 'unknown'" ]] || false
    [[ "$output" =~ "dolt_commit('-am', 'cm')" ]] || false
}

@test "replication: bad source doesn't error during non-transactional commands" {
    cd repo1
    dolt config --local --add sqlserver.global.dolt_read_replica_remote unknown
    dolt config --local --add sqlserver.global.dolt_replicate_heads main

    run dolt status
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "remote not found: 'unknown'" ]] || false
}

@test "replication: pull bad remote errors" {
    cd repo1
    dolt config --local --add sqlserver.global.dolt_read_replica_remote unknown
    dolt config --local --add sqlserver.global.dolt_replicate_heads main

    run dolt sql -q "show tables"
    [ "$status" -eq 1 ]
    [[ ! "$output" =~ "panic" ]]
    [[ "$output" =~ "remote not found: 'unknown'" ]] || false
}

@test "replication: pull bad remote quiet warning" {
    cd repo1
    dolt config --local --add sqlserver.global.dolt_read_replica_remote unknown
    dolt config --local --add sqlserver.global.dolt_replicate_heads main
    dolt config --local --add sqlserver.global.dolt_skip_replication_errors 1

    run dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "panic" ]]
    [[ "$output" =~ "remote not found: 'unknown'" ]] || false
    [[ "$output" =~ "dolt_replication_remote value is misconfigured" ]] || false
}

@test "replication: use database syntax fetches missing branch" {
    dolt clone file://./rem1 repo2
    cd repo2
    dolt checkout -b feature-branch
    dolt sql -q "create table t1 (a int primary key)"
    dolt commit -am "new commit"
    dolt push origin feature-branch

    cd ../repo1
    dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1
    dolt config --local --add sqlserver.global.dolt_replicate_heads main
    run dolt sql -b -q "USE \`repo1/feature-branch\`; show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 4 ]
    [[ "${lines[1]}" =~ "Table" ]] || false
    [[ "${lines[2]}" =~ "t1" ]] || false
}

@test "replication: database autofetch doesn't change replication heads setting" {
    dolt clone file://./rem1 repo2
    cd repo2
    dolt branch feature-branch
    dolt push origin feature-branch

    cd ../repo1
    dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1
    dolt config --local --add sqlserver.global.dolt_replicate_heads main
    run dolt sql -q "use \`repo1/feature-branch\`"

    cd ../repo2
    dolt checkout feature-branch
    dolt sql -q "create table t1 (a int primary key)"
    dolt commit -am "new commit"
    dolt push origin feature-branch

    cd ../repo1
    dolt sql -b -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [[ ! "output" =~ "t1" ]] || false
}

@test "replication: async push on cli engine commit" {
    cd repo1
    dolt config --local --add sqlserver.global.dolt_replicate_to_remote remote1
    dolt config --local --add sqlserver.global.dolt_async_replication 1
    dolt sql -q "create table t1 (a int primary key)"
    dolt sql -q "select dolt_commit('-am', 'cm')"
    sleep 5

    cd ..
    dolt clone file://./rem1 repo2
    cd repo2
    run dolt ls
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "t1" ]] || false
}

@test "replication: local clone" {
    run dolt clone file://./repo1/.dolt/noms repo2
    [ "$status" -eq 0 ]
    cd repo2
    run dolt ls
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]
}
