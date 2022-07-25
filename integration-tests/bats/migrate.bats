#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}

function checksum_table {
    QUERY="SELECT GROUP_CONCAT(column_name) FROM information_schema.columns WHERE table_name = '$1'"
    COLUMNS=$( dolt sql -q "$QUERY" -r csv | tail -n1 | sed 's/"//g' )
    dolt sql -q "SELECT CAST(SUM(CRC32(CONCAT($COLUMNS))) AS UNSIGNED) FROM $1" -r csv | tail -n1
}

@test "migrate: smoke test" {
    dolt sql <<SQL
CREATE TABLE test (pk int primary key, c0 int, c1 int);
INSERT INTO test VALUES (1,1,1),(2,2,2),(3,3,3);
CALL dcommit('-am', 'added table test');
SQL

    CHECKSUM=$(checksum_table test)
    run dolt migrate
    [ $status -eq 0 ]

    run checksum_table test
    [ $status -eq 0 ]
    [[ "$output" =~ "$CHECKSUM" ]] || false
}