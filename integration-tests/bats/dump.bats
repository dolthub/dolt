#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common

    REPO_NAME="dolt_repo_$$"
    mkdir $REPO_NAME
    cd $REPO_NAME

    dolt sql -q "CREATE TABLE mysqldump_table(pk int)"
    dolt sql -q "INSERT INTO mysqldump_table VALUES (1);"
    dolt sql -q "CREATE TABLE warehouse(warehouse_id int primary key, warehouse_name longtext)"
    dolt sql -q "INSERT into warehouse VALUES (1, 'UPS'), (2, 'TV'), (3, 'Table');"

    let PORT="$$ % (65536-1024) + 1024"
    USER="dolt"
    dolt sql-server --host 0.0.0.0 --port=$PORT --user=$USER --loglevel=trace &
    SERVER_PID=$!
    # Give the server a chance to start
    sleep 1

    export MYSQL_PWD=""
}

teardown() {
    assert_feature_version
    teardown_common

    cd ..
    kill $SERVER_PID
    rm -rf $REPO_NAME
}

@test "dump: dolt dump SQL export" {
    run dolt dump
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully exported data." ]] || false
    [ -f dumps/doltdump.sql ]

    run grep INSERT dumps/doltdump.sql
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 4 ]

    run dolt dump
    [ "$status" -ne 0 ]
    [[ "$output" =~ "doltdump.sql already exists" ]] || false

    run dolt dump -f
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully exported data." ]] || false

}

@test "mysqldump works" {
    mkdir dumps
    run mysqldump $REPO_NAME -P $PORT -h 0.0.0.0 -u $USER > dumps/mysqldump.sql
    [ "$status" -eq 0 ]
    [ -f dumps/mysqldump.sql ]

    run dolt dump
    [ "$status" -eq 0 ]
    [ -f dumps/doltdump.sql ]

    cd dumps
    dolt init
    dolt branch dolt_branch

    dolt sql < mysqldump.sql
    dolt add .
    dolt commit --allow-empty -m "create tables from mysqldump"

    dolt checkout dolt_branch
    dolt sql < doltdump.sql
    dolt add .
    dolt commit --allow-empty -m "create tables from doltdump"

    dolt diff --summary main dolt_branch
    [ "$status" -eq 0 ]
    [[ "$output" = "" ]]
}

