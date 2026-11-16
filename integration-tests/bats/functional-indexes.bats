#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "functional-indexes: create index with multiple expressions mixed with a plain column" {
    dolt sql <<SQL
CREATE TABLE t (pk INT PRIMARY KEY, name VARCHAR(100), age INT, c1 INT, c2 INT);
INSERT INTO t VALUES (1, 'alice', 30, 1, 2), (2, 'bob', 40, 3, 4);
CREATE INDEX idx1 ON t ((UPPER(name)), age, (c1 + c2));
SQL

    run dolt sql -q "SELECT pk FROM t WHERE UPPER(name) = 'ALICE' AND age = 30 AND c1 + c2 = 3;" -r csv
    [ "$status" -eq "0" ]
    [[ "${lines[1]}" =~ "1" ]] || false

    run dolt sql -q "SELECT pk FROM t WHERE UPPER(name) = 'BOB' AND age = 40 AND c1 + c2 = 7;" -r csv
    [ "$status" -eq "0" ]
    [[ "${lines[1]}" =~ "2" ]] || false

    run dolt sql -q "EXPLAIN FORMAT=TREE SELECT pk FROM t WHERE UPPER(name) = 'ALICE' AND age = 30 AND c1 + c2 = 3;"
    [ "$status" -eq "0" ]
    [[ "$output" =~ "IndexedTableAccess(t)" ]] || false
    [[ "$output" =~ "!hidden!idx1!0!0" ]] || false
    [[ "$output" =~ "!hidden!idx1!2!0" ]] || false
}

@test "functional-indexes: hidden generated columns are not visible in schema output" {
    dolt sql <<SQL
CREATE TABLE t (pk INT PRIMARY KEY, c1 INT, c2 INT, c3 INT);
CREATE UNIQUE INDEX idx1 ON t ((c1 * 10), c2, (c3 * 10));
SQL

    run dolt schema show t
    [ "$status" -eq "0" ]
    [[ "$output" =~ "UNIQUE KEY \`idx1\` (((c1 * 10)),\`c2\`,((c3 * 10)))" ]] || false
    [[ ! "$output" =~ "!hidden!" ]] || false

    run dolt sql -q "SHOW CREATE TABLE t;"
    [ "$status" -eq "0" ]
    [[ ! "$output" =~ "!hidden!" ]] || false
}

@test "functional-indexes: UNIQUE constraint is enforced across all expressions" {
    dolt sql <<SQL
CREATE TABLE t (pk INT PRIMARY KEY, c1 INT, c2 INT, c3 INT);
CREATE UNIQUE INDEX idx1 ON t ((c1 * 10), c2, (c3 * 10));
INSERT INTO t VALUES (1, 1, 2, 3);
SQL

    run dolt sql -q "INSERT INTO t VALUES (2, 1, 2, 3);"
    [ "$status" -ne "0" ]
    [[ "$output" =~ "duplicate" ]] || false

    run dolt sql -q "INSERT INTO t VALUES (3, 1, 3, 3);"
    [ "$status" -eq "0" ]
}

@test "functional-indexes: DROP INDEX removes only its own hidden columns" {
    dolt sql <<SQL
CREATE TABLE t (pk INT PRIMARY KEY, c1 INT, c2 INT, c3 INT);
INSERT INTO t VALUES (1, 10, 20, 30);
CREATE INDEX idx2 ON t ((c2 * 100));
CREATE INDEX idx1 ON t ((c1 * 10), c2, (c3 * 10));
SQL

    run dolt sql -q "SHOW EXTENDED COLUMNS FROM t;"
    [ "$status" -eq "0" ]
    [[ "$output" =~ "!hidden!idx1!0!0" ]] || false
    [[ "$output" =~ "!hidden!idx1!2!0" ]] || false
    [[ "$output" =~ "!hidden!idx2!0!0" ]] || false

    run dolt sql -q "DROP INDEX idx1 ON t;"
    [ "$status" -eq "0" ]

    run dolt sql -q "SHOW EXTENDED COLUMNS FROM t;"
    [ "$status" -eq "0" ]
    [[ ! "$output" =~ "!hidden!idx1!0!0" ]] || false
    [[ ! "$output" =~ "!hidden!idx1!2!0" ]] || false
    [[ "$output" =~ "!hidden!idx2!0!0" ]] || false

    run dolt sql -q "DROP INDEX idx2 ON t;"
    [ "$status" -eq "0" ]

    run dolt sql -q "SHOW EXTENDED COLUMNS FROM t;"
    [ "$status" -eq "0" ]
    [[ ! "$output" =~ "!hidden!" ]] || false
}

@test "functional-indexes: DROP COLUMN blocked for columns referenced by an expression" {
    dolt sql <<SQL
CREATE TABLE t (pk INT PRIMARY KEY, c1 INT, c2 INT, c3 INT);
CREATE INDEX idx1 ON t ((c1 * 10), c2, (c3 * 10));
SQL

    run dolt sql -q "ALTER TABLE t DROP COLUMN c1;"
    [ "$status" -ne "0" ]
    [[ "$output" =~ "functional index dependency" ]] || false

    run dolt sql -q "ALTER TABLE t DROP COLUMN c3;"
    [ "$status" -ne "0" ]
    [[ "$output" =~ "functional index dependency" ]] || false
}

@test "functional-indexes: survives dolt commit and diff" {
    dolt sql <<SQL
CREATE TABLE t (pk INT PRIMARY KEY, name VARCHAR(100), age INT, c1 INT, c2 INT);
CREATE INDEX idx1 ON t ((UPPER(name)), age, (c1 + c2));
SQL
    dolt add .
    dolt commit -m "create table with multi-expression functional index"

    dolt sql -q "INSERT INTO t VALUES (1, 'alice', 30, 1, 2);"

    run dolt diff
    [ "$status" -eq "0" ]
    [[ "$output" =~ "alice" ]] || false

    dolt add .
    dolt commit -m "insert row"

    run dolt sql -q "SELECT pk FROM t WHERE UPPER(name) = 'ALICE' AND age = 30 AND c1 + c2 = 3;" -r csv
    [ "$status" -eq "0" ]
    [[ "${lines[1]}" =~ "1" ]] || false
}
