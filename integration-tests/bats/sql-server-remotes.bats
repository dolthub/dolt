#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash
load $BATS_TEST_DIRNAME/helper/query-server-common.bash

make_repo() {
  mkdir "$1"
  cd "$1"
  dolt init
  cd ..
}

setup() {
    setup_no_dolt_init
    make_repo repo1
    make_repo repo2
}

teardown() {
    stop_sql_server
    teardown_common
}

@test "sql-server: sql-push --set-remote within session" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    mkdir rem1
    cd repo1
    dolt remote add origin file://../rem1
    start_sql_server repo1

    dolt push origin main
    run server_query repo1 1 "select dolt_push() as p" "p\n0"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "the current branch has no upstream branch" ]] || false

    server_query repo1 1 "select dolt_push('--set-upstream', 'origin', 'main') as p" "p\n1"

    skip "In-memory branch doesn't track upstream correctly"
    server_query repo1 1 "select dolt_push() as p" "p\n1"
}

@test "sql-server: replicate to backup after sql-session commit" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    mkdir bac1
    cd repo1
    dolt remote add backup1 file://../bac1
    dolt config --local --add sqlserver.global.DOLT_REPLICATE_TO_REMOTE backup1
    start_sql_server repo1

    multi_query repo1 1 "
    CREATE TABLE test (
      pk int primary key
    );
    INSERT INTO test VALUES (0),(1),(2);
    SELECT DOLT_ADD('.');
    SELECT DOLT_COMMIT('-m', 'Step 1');"

    cd ..
    dolt clone file://./bac1 repo3
    cd repo3
    run dolt sql -q "select * from test" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[0]}" =~ "pk" ]]
    [[ "${lines[1]}" =~ "0" ]]
    [[ "${lines[2]}" =~ "1" ]]
    [[ "${lines[3]}" =~ "2" ]]
}

@test "sql-server: read-replica pulls new commits on read" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    mkdir remote1
    cd repo2
    dolt remote add remote1 file://../remote1
    dolt push -u remote1 main

    cd ..
    rm -rf repo1
    dolt clone file://./remote1 repo1
    cd repo1
    dolt remote add remote1 file://../remote1

    cd ../repo2
    dolt sql -q "create table test (a int)"
    dolt commit -am "new commit"
    dolt push -u remote1 main

    cd ../repo1
    dolt config --local --add sqlserver.global.DOLT_READ_REPLICA_REMOTE remote1
    start_sql_server repo1

    server_query repo1 1 "show tables" "Table\ntest"
}

@test "sql-server: replica remote not found error" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    cd repo1
    dolt config --local --add sqlserver.global.DOLT_READ_REPLICA_REMOTE remote1

    run dolt sql-server
    [ "$status" -eq 1 ]
    [[ ! "$output" =~ "panic" ]]
    [[ "$output" =~ "remote not found: 'remote1'" ]] || false
}

@test "sql-server: quiet replica warnings" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    cd repo1
    dolt config --local --add sqlserver.global.dolt_skip_replication_errors true
    dolt config --local --add sqlserver.global.DOLT_READ_REPLICA_REMOTE remote1
    start_sql_server repo1

    run server_query repo1 1 "show tables" "Table\n"
}

@test "sql-server: replication source remote not found error" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    cd repo1
    dolt config --local --add sqlserver.global.dolt_replicate_to_remote remote1

    run dolt sql-server
    [ "$status" -eq 1 ]
    [[ ! "$output" =~ "panic" ]]
    [[ "$output" =~ "remote not found: 'remote1'" ]] || false
}

@test "sql-server: quiet replication source warnings" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    cd repo1
    dolt config --local --add sqlserver.global.dolt_skip_replication_errors true
    dolt config --local --add sqlserver.global.dolt_replicate_to_remote remote1
    start_sql_server repo1

    server_query repo1 1 "show tables" "Table\n"
}
