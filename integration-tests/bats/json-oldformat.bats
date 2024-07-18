#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

# This BATS test attempts to read JSON documents that were written prior to v1.40

# This function was used to create the dolt repo used for this test. It is not run during testing.
create_repo() {
    dolt init
    dolt sql -q "CREATE TABLE jsonTable(pk int primary key, j json);"
    dolt sql -q 'INSERT INTO jsonTable( with recursive cte (pk, j) as ( select 0, JSON_OBJECT("K", "V") union all select pk+1, JSON_INSERT(j, CONCAT("$.k", pk), j) from cte where pk < 20 ) select * from cte );'
    dolt add .
    dolt commit -m "create json table"
}

setup() {
    cp -r $BATS_TEST_DIRNAME/json-oldformat-repo/ $BATS_TMPDIR/dolt-repo-$$
    cd $BATS_TMPDIR/dolt-repo-$$
}

@test "json-oldformat: verify queries" {
    run dolt sql -q "SELECT pk, JSON_EXTRACT(j, '$.k10.k5.k3.k2') FROM jsonTable WHERE pk = 12;"
    [[ "$output" =~ '{"K": "V", "k0": {"K": "V"}, "k1": {"K": "V", "k0": {"K": "V"}}}' ]] || false

    run dolt sql -q "SELECT pk, JSON_VALUE(j, '$.k8.k6.k4.k1') FROM jsonTable WHERE pk = 12;"
    [[ "$output" =~ '{"K": "V", "k0": {"K": "V"}}' ]] || false

    run dolt sql -q "SELECT pk, JSON_EXTRACT(JSON_INSERT(j, '$.k9.k6.k1.TESTKEY', 'TESTVALUE'), '$.k9.k6.k1') FROM jsonTable WHERE pk = 12;"
    [[ "$output" =~ '{"K": "V", "k0": {"K": "V"}, "TESTKEY": "TESTVALUE"}' ]] || false
}
