#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    skip_nbf_dolt_1

    dolt sql <<SQL
CREATE TABLE test (
  pk int NOT NULL PRIMARY KEY,
  c0 int
);
INSERT INTO test VALUES
    (0,0),(1,1),(2,2);
CREATE TABLE to_drop (
    pk int PRIMARY KEY
);
SQL
    dolt add -A
    dolt commit -m "added table test"
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "filter-branch: smoke-test" {
    dolt sql -q "INSERT INTO test VALUES (7,7),(8,8),(9,9);"
    dolt add -A && dolt commit -m "added more rows"

    dolt filter-branch "DELETE FROM test WHERE pk > 1;"
    run dolt sql -q "SELECT count(*) FROM test" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "2" ]] || false

    run dolt sql -q "SELECT max(pk), max(c0) FROM dolt_history_test;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,1" ]] || false
}

@test "filter-branch: filter multiple branches" {
    dolt branch other

    dolt sql -q "INSERT INTO test VALUES (7,7),(8,8),(9,9);"
    dolt add -A && dolt commit -m "added more rows"

    dolt checkout other
    dolt sql -q "INSERT INTO test VALUES (4,4),(5,5),(6,6);"
    dolt add -A && dolt commit -m "added more rows"

    dolt checkout main
    dolt filter-branch --all "DELETE FROM test WHERE pk > 4;"

    run dolt sql -q "SELECT pk,c0 FROM dolt_history_test ORDER BY pk" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "0,0" ]] || false
    [[ "$output" =~ "0,0" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "2,2" ]] || false

    dolt checkout other
    run dolt sql -q "SELECT pk,c0 FROM dolt_history_test ORDER BY pk" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "0,0" ]] || false
    [[ "$output" =~ "0,0" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "4,4" ]] || false
}

@test "filter-branch: with missing table" {
    dolt sql -q "DROP TABLE test;"
    dolt add -A && dolt commit -m "dropped test"

    # filter-branch warns about missing table but doesn't error
    run dolt filter-branch "DELETE FROM test WHERE pk > 1;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "table not found: test" ]] || false

    run dolt sql -q "SELECT count(*) FROM test AS OF 'HEAD~1';" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "2" ]] || false
}

@test "filter-branch: forks history" {
    dolt branch other

    dolt sql -q "INSERT INTO test VALUES (7,7),(8,8),(9,9);"
    dolt add -A && dolt commit -m "added more rows"

    dolt filter-branch "DELETE FROM test WHERE pk > 1;"

    dolt checkout other
    run dolt sql -q "SELECT * FROM test WHERE pk > 1" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "2,2" ]] || false
}

@test "filter-branch: filter until commit" {
    dolt sql -q "INSERT INTO test VALUES (7,7)"
    dolt add -A && dolt commit -m "added (7,7)"
    dolt sql -q "INSERT INTO test VALUES (8,8)"
    dolt add -A && dolt commit -m "added (8,8)"
    dolt sql -q "INSERT INTO test VALUES (9,9)"
    dolt add -A && dolt commit -m "added (9,9)"

    dolt filter-branch "DELETE FROM test WHERE pk > 2;" HEAD~2

    run dolt sql -q "SELECT max(pk), max(c0) FROM test AS OF 'HEAD';" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "2,2" ]] || false
    run dolt sql -q "SELECT max(pk), max(c0) FROM test AS OF 'HEAD~1';" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "2,2" ]] || false
    run dolt sql -q "SELECT max(pk), max(c0) FROM test AS OF 'HEAD~2';" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "7,7" ]] || false
}

function setup_write_test {
    dolt sql -q "INSERT INTO test VALUES (4,4);"
    dolt add -A && dolt commit -m "4"

    dolt sql -q "INSERT INTO test VALUES (5,5);"
    dolt add -A && dolt commit -m "5"
}

@test "filter-branch: INSERT INTO" {
    setup_write_test

    dolt filter-branch "INSERT INTO test VALUES (9,9);"

    run dolt sql -q "SELECT pk,c0 FROM dolt_history_test ORDER BY pk DESC LIMIT 4;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "9,9" ]] || false
    [[ "$output" =~ "9,9" ]] || false
    [[ "$output" =~ "9,9" ]] || false
    [[ "$output" =~ "5,5" ]] || false
}

@test "filter-branch: UPDATE" {
    setup_write_test

    dolt filter-branch "UPDATE test SET c0 = 9 WHERE pk = 2;"

    run dolt sql -q "SELECT pk,c0 FROM dolt_history_test ORDER BY c0 DESC LIMIT 4;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "2,9" ]] || false
    [[ "$output" =~ "2,9" ]] || false
    [[ "$output" =~ "2,9" ]] || false
    [[ "$output" =~ "5,5" ]] || false
}

@test "filter-branch: ADD/DROP column" {
    setup_write_test

    dolt filter-branch "ALTER TABLE TEST ADD COLUMN c1 int;"

    for commit in HEAD HEAD~1 HEAD~2; do
        run dolt sql -q "SELECT * FROM test AS OF '$commit';" -r csv
        [ "$status" -eq 0 ]
        [[ "$output" =~ "pk,c0,c1" ]] || false
    done

    dolt filter-branch "ALTER TABLE TEST DROP COLUMN c0;"

    for commit in HEAD HEAD~1 HEAD~2; do
        run dolt sql -q "SELECT * FROM test AS OF '$commit';" -r csv
        [ "$status" -eq 0 ]
        [[ "$output" =~ "pk,c1" ]] || false
    done
}

@test "filter-branch: ADD/DROP table" {
    setup_write_test

    for commit in HEAD HEAD~1 HEAD~2; do
        run dolt sql -q "SHOW TABLES AS OF '$commit';" -r csv
        [ "$status" -eq 0 ]
        [[ ! "$output" =~ "added" ]] || false
    done

    dolt filter-branch "CREATE TABLE added (pk int PRIMARY KEY);"

    for commit in HEAD HEAD~1 HEAD~2; do
        run dolt sql -q "SHOW TABLES AS OF '$commit';" -r csv
        [ "$status" -eq 0 ]
        [[ "$output" =~ "added" ]] || false
    done


    for commit in HEAD HEAD~1 HEAD~2; do
        run dolt sql -q "SHOW TABLES AS OF '$commit';" -r csv
        [ "$status" -eq 0 ]
        [[ "$output" =~ "to_drop" ]] || false
    done

    dolt filter-branch "DROP TABLE to_drop;"

    for commit in HEAD HEAD~1 HEAD~2; do
        run dolt sql -q "SHOW TABLES AS OF '$commit';" -r csv
        [ "$status" -eq 0 ]
        [[ ! "$output" =~ "to_drop" ]] || false
    done
}

@test "filter-branch: error on conflict" {
    setup_write_test

    run dolt filter-branch "INSERT INTO test VALUES (1,2);"
    [ "$status" -ne 0 ]
    [[ ! "$output" =~ "panic" ]] || false

    run dolt filter-branch "REPLACE INTO test VALUES (1,2);"
    [ "$status" -eq 0 ]

    run dolt sql -q "SELECT pk,c0 FROM dolt_history_test WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,2" ]] || false
    [[ "$output" =~ "1,2" ]] || false
    [[ "$output" =~ "1,2" ]] || false
}

@test "filter-branch: error on incorrect schema" {
    setup_write_test

    dolt sql <<SQL
ALTER TABLE test ADD COLUMN c1 int;
INSERT INTO test VALUES (6,6,6);
SQL
    dolt add -A && dolt commit -m "added column c1"

    run dolt filter-branch "INSERT INTO test VALUES (9,9);"
    [ "$status" -ne 0 ]
    [[ ! "$output" =~ "panic" ]] || false

    run dolt filter-branch "INSERT INTO test (pk,c0) VALUES (9,9);"
    [ "$status" -eq 0 ]

    run dolt sql -q "SELECT pk,c0 FROM dolt_history_test WHERE pk=9;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "9,9" ]] || false
    [[ "$output" =~ "9,9" ]] || false
    [[ "$output" =~ "9,9" ]] || false
    [[ "$output" =~ "9,9" ]] || false
}
