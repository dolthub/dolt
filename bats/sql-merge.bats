#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common

    dolt sql <<SQL
CREATE TABLE test (
    pk int primary key
);

INSERT INTO test VALUES (0),(1),(2);
SQL
}

teardown() {
    teardown_common
}

@test "DOLT_MERGE works with ff" {
        dolt sql << SQL
SELECT DOLT_COMMIT('-a', '-m', 'Step 1');
SELECT DOLT_CHECKOUT('-b', 'feature-branch');
INSERT INTO test VALUES (3);
SELECT DOLT_COMMIT('-a', '-m', 'this is a ff');
SELECT DOLT_CHECKOUT('master');
SQL
    run dolt sql -q "SELECT DOLT_MERGE('feature-branch');"
    [ $status -eq 0 ]

    run dolt log -n 1
    [ $status -eq 0 ]
    [[ "$output" =~ "this is a ff" ]] || false

    run dolt sql -q "SELECT COUNT(*) FROM dolt_log"
    [ $status -eq 0 ]
    [[ "$output" =~ "3" ]] || false
}

@test "DOLT_MERGE correctly returns head and working session variables." {
    dolt sql << SQL
SELECT DOLT_COMMIT('-a', '-m', 'Step 1');
SELECT DOLT_CHECKOUT('-b', 'feature-branch');
INSERT INTO test VALUES (3);
SELECT DOLT_COMMIT('-a', '-m', 'this is a ff');
SQL
    head_variable=@@dolt_repo_$$_head
    head_hash=$(get_head_commit)

    dolt sql << SQL
SELECT DOLT_CHECKOUT('master');
SELECT DOLT_MERGE('feature-branch');
SQL

    run dolt sql -q "SELECT $head_variable"
    [ $status -eq 0 ]
    [[ "$output" =~ $head_hash ]] || false
}

@test "DOLT_MERGE correctly merges branches with differing content in same table without conflicts" {
    dolt sql << SQL
SELECT DOLT_COMMIT('-a', '-m', 'Step 1');
SELECT DOLT_CHECKOUT('-b', 'feature-branch');
INSERT INTO test VALUES (3);
SELECT DOLT_COMMIT('-a', '-m', 'Insert 3');
SELECT DOLT_CHECKOUT('master');
INSERT INTO test VALUES (500000);
SELECT DOLT_COMMIT('-a', '-m', 'Insert 500000');
SELECT DOLT_MERGE('feature-branch');
SQL

    run dolt sql -q "SELECT * FROM test" -r csv
    echo $output
    [ $status -eq 0 ]
    [[ "$output" =~ "pk" ]] || false
    [[ "$output" =~ "0" ]] || false
    [[ "$output" =~ "1" ]] || false
    [[ "$output" =~ "2" ]] || false
    [[ "$output" =~ "3" ]] || false
    [[ "$output" =~ "500000" ]] || false


    run dolt log -n 1
    [ $status -eq 0 ]
    [[ "$output" =~ "Insert 500000" ]] || false

    run dolt sql -q "SELECT COUNT(*) FROM dolt_log"
    [ $status -eq 0 ]
    [[ "$output" =~ "3" ]] || false
}

@test "DOLT_MERGE works with no-ff" {
        run dolt sql << SQL
SELECT DOLT_COMMIT('-a', '-m', 'Step 1');
SELECT DOLT_CHECKOUT('-b', 'feature-branch');
INSERT INTO test VALUES (3);
SELECT DOLT_COMMIT('-a', '-m', 'update feature-branch');
SELECT DOLT_CHECKOUT('master');
SELECT DOLT_MERGE('feature-branch', '-no-ff', '-m', 'this is a no-ff');
SQL
    [ $status -eq 0 ]

    run dolt log -n 1
    [ $status -eq 0 ]
    [[ "$output" =~ "this is a no-ff" ]] || false

    run dolt sql -q "SELECT COUNT(*) FROM dolt_log"
    [ $status -eq 0 ]
    [[ "$output" =~ "4" ]] || false
}

@test "DOLT_MERGE no-ff changes head and working session variables." {
    dolt sql << SQL
SELECT DOLT_COMMIT('-a', '-m', 'Step 1');
SELECT DOLT_CHECKOUT('-b', 'feature-branch');
INSERT INTO test VALUES (3);
SELECT DOLT_COMMIT('-a', '-m', 'update feature-branch');
SELECT DOLT_CHECKOUT('master');
SQL
    head_variable=@@dolt_repo_$$_head
    head_hash=$(get_head_commit)
    working_variable=@@dolt_repo_$$_working
    working_hash=$(get_working_hash)

    run dolt sql -q "SELECT DOLT_MERGE('feature-branch', '-no-ff', '-m', 'this is a no-ff');"
    [ $status -eq 0 ]

    run dolt sql -q "SELECT $head_variable"
    [ $status -eq 0 ]
    [[ ! "$output" =~ $head_hash ]] || false

    run dolt sql -q "SELECT $working_variable"
    [ $status -eq 0 ]
    [[ ! "$output" =~ $working_hash ]] || false
}

get_head_commit() {
    dolt log -n 1 | grep -m 1 commit | cut -c 8-
}

get_working_hash() {
  dolt sql -q "select @@dolt_repo_$$_working" | sed -n 4p | sed -e 's/|//' -e 's/|//'  -e 's/ //'
}
