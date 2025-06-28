#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common

    dolt sql <<SQL
CREATE TABLE test (
    pk int primary key,
    v int
);

CREATE TABLE test_two_pk (
    pk1 int,
    pk2 int,
    v int,
    primary key (pk1, pk2)
);

CALL DOLT_ADD('.');
CALL DOLT_COMMIT('-m', 'create table');
CALL DOLT_BRANCH('create');

INSERT INTO test VALUES (10, 10), (11, 11), (20, 20), (21, 21), (30, 30), (31, 31);
INSERT INTO test_two_pk (pk1, pk2) VALUES (1, 10), (1, 11), (2, 20), (2, 21), (3, 30), (3, 31);
CALL DOLT_ADD('.');
CALL DOLT_COMMIT('-m', 'initial values');
CALL DOLT_BRANCH('initial');

INSERT INTO test VALUES (12, 12), (22, 22), (32, 32);
INSERT INTO test_two_pk (pk1, pk2) VALUES (1, 12), (2, 22), (3, 32);
DELETE FROM test WHERE pk = 1 OR pk = 20 OR pk = 30;
DELETE FROM test_two_pk WHERE pk2 = 10 OR pk2 = 20 OR pk2 = 30;
UPDATE test SET v = 0 WHERE pk = 11 OR pk = 21 OR pk = 31;
UPDATE test_two_pk SET v = 1 WHERE pk2 = 11 OR pk2 = 21 OR pk2 = 31;

CALL DOLT_ADD('.');
CALL DOLT_COMMIT('-m', 'update values');
CALL DOLT_BRANCH('update');
SQL
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "sql-commit-diff: DOLT_COMMIT_DIFF with a range on to_ key" {
    run dolt sql -q "select from_pk, to_pk, diff_type from dolt_commit_diff_test where from_commit = DOLT_HASHOF('initial') and to_commit = DOLT_HASHOF('update') and to_pk > 12 and to_pk < 30;"
    [ $status -eq 0 ]
    [[ "${#lines[@]}" = "6" ]] # 2 rows + 4 formatting lines
    [[ "$output" =~ "| NULL    | 22    | added     |" ]] || false
    [[ "$output" =~ "| 21      | 21    | modified  |" ]] || false
    run dolt sql -q "describe plan select * from dolt_commit_diff_test where from_commit = DOLT_HASHOF('create') and to_commit = DOLT_HASHOF('head') and to_pk > 0 and to_pk < 5;"
    [ $status -eq 0 ]
    [[ "$output" =~ "index: [dolt_commit_diff_test.to_commit,dolt_commit_diff_test.from_commit,dolt_commit_diff_test.to_pk]" ]] || false
    [[ "$output" =~ "(0, 5)" ]] || false

    run dolt sql -q "select from_pk1, from_pk2, to_pk1, to_pk2, diff_type from dolt_commit_diff_test_two_pk where from_commit = DOLT_HASHOF('initial') and to_commit = DOLT_HASHOF('update') and to_pk1 = 2;"
    [ $status -eq 0 ]
    [[ "${#lines[@]}" = "6" ]] # 2 rows + 4 formatting lines
    [[ "$output" =~ "| 2        | 21       | 2      | 21     | modified  |" ]] || false
    [[ "$output" =~ "| NULL     | NULL     | 2      | 22     | added     |" ]] || false
    run dolt sql -q "describe plan select * from dolt_commit_diff_test_two_pk where from_commit = DOLT_HASHOF('create') and to_commit = DOLT_HASHOF('head') and to_pk1 = 2;"
    [ $status -eq 0 ]
    [[ "$output" =~ "index: [dolt_commit_diff_test_two_pk.to_commit,dolt_commit_diff_test_two_pk.from_commit,dolt_commit_diff_test_two_pk.to_pk1,dolt_commit_diff_test_two_pk.to_pk2]" ]] || false
    [[ "$output" =~ "[2, 2], [NULL, ∞)" ]] || false
}

@test "sql-commit-diff: DOLT_COMMIT_DIFF with a range on from_ key" {
    run dolt sql -q "select from_pk, to_pk, diff_type from dolt_commit_diff_test where from_commit = DOLT_HASHOF('initial') and to_commit = DOLT_HASHOF('update') and from_pk > 12 and from_pk < 30;"
    [ $status -eq 0 ]
    [[ "${#lines[@]}" = "6" ]] # 2 rows + 4 formatting lines
    [[ "$output" =~ "| 20      | NULL  | removed   |" ]] || false
    [[ "$output" =~ "| 21      | 21    | modified  |" ]] || false
    run dolt sql -q "describe plan select * from dolt_commit_diff_test where from_commit = DOLT_HASHOF('create') and to_commit = DOLT_HASHOF('head') and from_pk > 0 and from_pk < 5;"
    [ $status -eq 0 ]
    [[ "$output" =~ "index: [dolt_commit_diff_test.to_commit,dolt_commit_diff_test.from_commit,dolt_commit_diff_test.from_pk]" ]] || false
    [[ "$output" =~ "(0, 5)" ]] || false

    run dolt sql -q "select from_pk1, from_pk2, to_pk1, to_pk2, diff_type from dolt_commit_diff_test_two_pk where from_commit = DOLT_HASHOF('initial') and to_commit = DOLT_HASHOF('update') and from_pk1 = 2;"
    [ $status -eq 0 ]
    echo "$output"
    [[ "${#lines[@]}" = "6" ]] # 2 rows + 4 formatting lines
    [[ "$output" =~ "| 2        | 21       | 2      | 21     | modified  |" ]] || false
    [[ "$output" =~ "| 2        | 20       | NULL   | NULL   | removed   |" ]] || false
    run dolt sql -q "describe plan select * from dolt_commit_diff_test_two_pk where from_commit = DOLT_HASHOF('create') and to_commit = DOLT_HASHOF('head') and from_pk1 = 2;"
    [ $status -eq 0 ]
    [[ "$output" =~ "index: [dolt_commit_diff_test_two_pk.to_commit,dolt_commit_diff_test_two_pk.from_commit,dolt_commit_diff_test_two_pk.from_pk1,dolt_commit_diff_test_two_pk.from_pk2]" ]] || false
    [[ "$output" =~ "[2, 2], [NULL, ∞)" ]] || false
}