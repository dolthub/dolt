#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash
load $BATS_TEST_DIRNAME/helper/query-server-common.bash

setup() {
    setup_common
    export PYTEST_DIR=`batshelper`
    export SQL_PORT=$(($ % (65536-1024) + 1024))
    dolt sql-server --port=$SQL_PORT &
    wait_for_connection 5000
}

teardown() {
    pkill dolt
    teardown_common
}

@test "run sql server and verify connection" {
    server_query "SHOW tables" 'Tables'
    server_query "CREATE TABLE one_pk (
                    pk BIGINT NOT NULL COMMENT 'tag:0',
                    c1 BIGINT COMMENT 'tag:1',
                    c2 BIGINT COMMENT 'tag:2',
                    PRIMARY KEY (pk)
                  )" ""
     insert_query "INSERT INTO one_pk (pk) values (0)" 1
     server_query "SELECT * from one_pk ORDER BY pk" "pk,c1,c2\n0,None,None"
     insert_query "INSERT INTO one_pk (pk,c1) values (1,1)" 1
     insert_query "INSERT INTO one_pk (pk,c1,c2) values (2,2,2),(3,3,3)" 2
     server_query "SELECT * from one_pk ORDER by pk" "pk,c1,c2\n0,None,None\n1,1,None\n2,2,2\n3,3,3"
     update_query "UPDATE one_pk SET c2=c1 WHERE c2 is NULL and c1 IS NOT NULL" 1
}