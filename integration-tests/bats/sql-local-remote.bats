#! /usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash
load $BATS_TEST_DIRNAME/helper/query-server-common.bash

make_repo() {
  mkdir "$1"
  cd "$1"
  dolt init
  dolt sql -q "create table $1_tbl (id int)"
  cd ..
}

setup() {
    setup_no_dolt_init
    make_repo defaultDB
    make_repo altDB
}

teardown() {
    stop_sql_server 1
    teardown_common
}

@test "sql-local-remote: test switch between server/no server" {
    start_sql_server defaultDB

    run dolt --verbose-engine-setup --user dolt sql -q "show databases" 
    [ "$status" -eq 0 ] || false
    [[ "$output" =~ "starting remote mode" ]] || false
    [[ "$output" =~ "defaultDB" ]] || false
    [[ "$output" =~ "altDB" ]] || false

    stop_sql_server 1

    run dolt --verbose-engine-setup sql -q "show databases" 
    [ "$status" -eq 0 ] || false
    [[ "$output" =~ "starting local mode" ]] || false
    [[ "$output" =~ "defaultDB" ]] || false
    [[ "$output" =~ "altDB" ]] || false
}

@test "sql-local-remote: check --data-dir pointing to a server root can be used when in different directory." {
    start_sql_server altDb
    ROOT_DIR=$(pwd)

    cd /tmp

    run dolt --verbose-engine-setup --data-dir="$ROOT_DIR" --user dolt --use-db altDB sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "starting remote mode" ]] || false
    [[ "$output" =~ "altDB_tbl" ]] || false

    run dolt --verbose-engine-setup --data-dir="$ROOT_DIR" --user dolt --use-db defaultDB sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "starting remote mode" ]] || false
    [[ "$output" =~ "defaultDB_tbl" ]] || false

    run dolt --verbose-engine-setup --data-dir="$ROOT_DIR" --user dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "starting remote mode" ]] || false
    [[ "$output" =~ "altDB_tbl" ]] || false

    stop_sql_server 1

    run dolt --verbose-engine-setup --data-dir="$ROOT_DIR" --user dolt --use-db altDB sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "starting local mode" ]] || false
    [[ "$output" =~ "altDB_tbl" ]] || false

    run dolt --verbose-engine-setup --data-dir="$ROOT_DIR" --user dolt --use-db defaultDB sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "starting local mode" ]] || false
    [[ "$output" =~ "defaultDB_tbl" ]] || false

    run dolt --verbose-engine-setup --data-dir="$ROOT_DIR" --user dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "starting local mode" ]] || false
    [[ "$output" =~ "altDB_tbl" ]] || false
}


@test "sql-local-remote: check --data-dir pointing to a database root can be used when in different directory." {
    start_sql_server altDb
    ROOT_DIR=$(pwd)

    cd /tmp

    run dolt --verbose-engine-setup --data-dir="$ROOT_DIR/altDB" --user dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "starting remote mode" ]] || false
    [[ "$output" =~ "altDB_tbl" ]] || false

    run dolt --verbose-engine-setup --data-dir="$ROOT_DIR/altDB" --user dolt --use-db defaultDB sql -q "show tables"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "defaultDB does not exist" ]] || false

    stop_sql_server 1

    run dolt --verbose-engine-setup --data-dir="$ROOT_DIR/altDB" --user dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "starting local mode" ]] || false
    [[ "$output" =~ "altDB_tbl" ]] || false

    run dolt --verbose-engine-setup --data-dir="$ROOT_DIR/altDB" --user dolt --use-db defaultDB sql -q "show tables"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "defaultDB does not exist" ]] || false
}

