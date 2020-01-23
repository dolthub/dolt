#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}

@test "diff sql output reconciles INSERT query" {
    dolt checkout -b firstbranch
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt table import -u test `batshelper 1pk5col-ints.csv`
    dolt add test
    dolt commit -m "Added one initial row"

    dolt checkout -b newbranch
    dolt sql -q 'INSERT INTO test (pk, c1, c2, c3, c4, c5) VALUES (2, 11, 0, 0, 0, 0)'
    dolt add test
    dolt commit -m "Added three rows"

    # confirm a difference exists
    run dolt diff --sql newbranch firstbranch
    [ "$status" -eq 0 ]
    [[ "$output" != "" ]] || false

    dolt diff --sql newbranch firstbranch > query
    dolt checkout firstbranch
    dolt sql < query
    rm query
    dolt add test
    dolt commit -m "Reconciled with newbranch"

    # confirm that both branches have the same content
    run dolt diff --sql newbranch firstbranch
    [ "$status" -eq 0 ]
    [[ "$output" = "" ]] || false
}

@test "diff sql output reconciles UPDATE query" {
    dolt checkout -b firstbranch
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt table import -u test `batshelper 1pk5col-ints.csv`
    dolt add test
    dolt commit -m "Added one initial row"

    dolt checkout -b newbranch
    dolt sql -q 'UPDATE test SET c1=11, c5=6 WHERE pk=0'
    dolt add test
    dolt commit -m "modified first row"

    # confirm a difference exists
    run dolt diff --sql newbranch firstbranch
    [ "$status" -eq 0 ]
    [[ "$output" != "" ]] || false

    dolt diff --sql newbranch firstbranch > query
    dolt checkout firstbranch
    dolt sql < query
    rm query
    dolt add test
    dolt commit -m "Reconciled with newbranch"

    # confirm that both branches have the same content
    run dolt diff --sql newbranch firstbranch
    [ "$status" -eq 0 ]
    [[ "$output" = "" ]] || false
}

@test "diff sql output reconciles DELETE query" {
    dolt checkout -b firstbranch
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt table import -u test `batshelper 1pk5col-ints.csv`
    dolt add test
    dolt commit -m "Added one initial row"

    dolt checkout -b newbranch
    dolt sql -q 'DELETE FROM test WHERE pk=0'
    dolt add test
    dolt commit -m "deleted first row"

    # confirm a difference exists
    run dolt diff --sql newbranch firstbranch
    [ "$status" -eq 0 ]
    [[ "$output" != "" ]] || false

    dolt diff --sql newbranch firstbranch > query
    dolt checkout firstbranch
    dolt sql < query
    rm query
    dolt add test
    dolt commit -m "Reconciled with newbranch"

    # confirm that both branches have the same content
    run dolt diff --sql newbranch firstbranch
    [ "$status" -eq 0 ]
    [[ "$output" = "" ]] || false
}

@test "diff sql output reconciles change to PRIMARY KEY field in row " {
    dolt checkout -b firstbranch
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt table import -u test `batshelper 1pk5col-ints.csv`
    dolt add test
    dolt commit -m "Added one initial row"

    dolt checkout -b newbranch
    dolt sql -q 'UPDATE test SET pk=2 WHERE pk=1'
    dolt add test
    dolt commit -m "modified first row"

    # confirm a difference exists
    run dolt diff --sql newbranch firstbranch
    [ "$status" -eq 0 ]
    [[ "$output" != "" ]] || false

    dolt diff --sql newbranch firstbranch > query
    dolt checkout firstbranch
    dolt sql < query
    rm query
    dolt add test
    dolt commit -m "Reconciled with newbranch"

    # confirm that both branches have the same content
    run dolt diff --sql newbranch firstbranch
    [ "$status" -eq 0 ]
    [[ "$output" = "" ]] || false
}

@test "diff sql output reconciles column rename" {

    dolt checkout -b firstbranch
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt sql -q 'insert into test values (1,1,1,1,1,1)'
    dolt add .
    dolt commit -m "added row"

    dolt checkout -b newbranch
    dolt sql -q "alter table test rename column c1 to c0"
    dolt add .
    dolt commit -m "renamed column"

    # confirm a difference exists
    run dolt diff --sql newbranch firstbranch
    [ "$status" -eq 0 ]
    [[ "$output" != "" ]] || false

    dolt diff --sql newbranch firstbranch > query
    dolt checkout firstbranch
    dolt sql < query
    rm query
    dolt add test
    dolt commit -m "Reconciled with newbranch"

    # confirm that both branches have the same content
    run dolt diff --sql newbranch firstbranch
    [ "$status" -eq 0 ]
    [[ "$output" = "" ]] || false
}

@test "diff sql output reconciles DROP column query" {
    dolt checkout -b firstbranch
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt sql -q 'insert into test values (1,1,1,1,1,1)'
    dolt add .
    dolt commit -m "added row"

    dolt checkout -b newbranch
    dolt sql -q "alter table test drop column c1"
    dolt add .
    dolt commit -m "dropped column"

    # confirm a difference exists
    run dolt diff --sql newbranch firstbranch
    [ "$status" -eq 0 ]
    [[ "$output" != "" ]] || false

    dolt diff --sql newbranch firstbranch > query
    dolt checkout firstbranch
    dolt sql < query
    rm query
    dolt add test
    dolt commit -m "Reconciled with newbranch"

    # confirm that both branches have the same content
    run dolt diff --sql newbranch firstbranch
    [ "$status" -eq 0 ]
    [[ "$output" = "" ]] || false
}

@test "diff sql output reconciles ADD column query" {
    dolt checkout -b firstbranch
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt sql -q 'insert into test values (1,1,1,1,1,1)'
    dolt add .
    dolt commit -m "added row"

    dolt checkout -b newbranch
    dolt sql -q "alter table test add c0 bigint"
    dolt add .
    dolt commit -m "added column"

    # confirm a difference exists
    run dolt diff --sql newbranch firstbranch
    [ "$status" -eq 0 ]
    [[ "$output" != "" ]] || false

    dolt diff --sql newbranch firstbranch > query
    dolt checkout firstbranch
    dolt sql < query
    rm query
    dolt add test
    dolt commit -m "Reconciled with newbranch"

    # confirm that both branches have the same content
    run dolt diff --sql newbranch firstbranch
    [ "$status" -eq 0 ]
    [[ "$output" = "" ]] || false
}

@test "diff sql reconciles CREATE TABLE" {
    # The docs break this test on `dolt add test` below. It is fixed by removing docs, or changing that line below to `dolt add .`
    rm LICENSE.md
    rm README.md
    dolt checkout -b firstbranch
    dolt checkout -b newbranch
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt add .
    dolt commit -m "created new table"

    # confirm a difference exists
    run dolt diff --sql newbranch firstbranch
    [ "$status" -eq 0 ]
    [[ "$output" != "" ]] || false

    dolt diff --sql newbranch firstbranch > query
    dolt checkout firstbranch
    dolt sql < query
    rm query
    dolt add test
    dolt commit -m "Reconciled with newbranch"

    # confirm that both branches have the same content
    dolt diff --sql newbranch firstbranch
    run dolt diff --sql newbranch firstbranch
    [ "$status" -eq 0 ]
    [[ "$output" = "" ]] || false
}

@test "diff sql includes row INSERTSs to new tables after CREATE TABLE" {
    # The docs break this test on `dolt add test` below. It is fixed by removing docs, or changing that line below to `dolt add .`
    rm LICENSE.md
    rm README.md
    dolt checkout -b firstbranch
    dolt checkout -b newbranch
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt sql -q 'insert into test values (1,1,1,1,1,1)'
    dolt sql -q 'insert into test values (2,2,2,2,2,2)'
    dolt sql -q 'insert into test values (3,3,3,3,3,3)'
    dolt add .
    dolt commit -m "created new table"

    # confirm a difference exists
    run dolt diff --sql newbranch firstbranch
    [ "$status" -eq 0 ]
    [[ "$output" != "" ]] || false

    dolt diff --sql newbranch firstbranch > query
    dolt checkout firstbranch
    dolt sql < query
    rm query
    dolt add test
    dolt commit -m "Reconciled with newbranch"

    # confirm that both branches have the same content
    dolt diff --sql newbranch firstbranch
    run dolt diff --sql newbranch firstbranch
    [ "$status" -eq 0 ]
    [[ "$output" = "" ]] || false
}

@test "diff sql reconciles DROP TABLE" {
    dolt checkout -b firstbranch
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt sql -q 'insert into test values (1,1,1,1,1,1)'
    dolt add .
    dolt commit -m "setup table"

    dolt checkout -b newbranch
    dolt table rm test
    dolt add .
    dolt commit -m "removed table"

    # confirm a difference exists
    run dolt diff --sql newbranch firstbranch
    [ "$status" -eq 0 ]
    [[ "$output" != "" ]] || false

    dolt diff --sql newbranch firstbranch > query
    dolt checkout firstbranch
    dolt sql < query
    rm query
    dolt add test
    dolt commit -m "Reconciled with newbranch"

    # confirm that both branches have the same content
    run dolt diff --sql newbranch firstbranch
    [ "$status" -eq 0 ]
    [[ "$output" = "" ]] || false
}

@test "diff sql outputs RENAME TABLE if underlying data is unchanged" {
    dolt checkout -b firstbranch
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt add .
    dolt commit -m "created table"

    dolt checkout -b newbranch
    dolt table mv test newname
    dolt diff -q
    dolt add .
    dolt commit -m "renamed table"

    # confirm RENAME statement is being used
    dolt diff --sql newbranch firstbranch > output
    # grep will exit error if it doesn't match the pattern
    run grep RENAME output
    [ "$status" -eq 0 ]

    dolt diff --sql newbranch firstbranch > query
    dolt checkout firstbranch
    dolt sql < query
    rm query
    dolt add .
    dolt commit -m "Reconciled with newbranch"

    # confirm that both branches have the same content
    run dolt diff --sql newbranch firstbranch
    [ "$status" -eq 0 ]
    [[ "$output" = "" ]] || false
}

@test "diff sql reconciles RENAME TABLE with DROP+ADD if data is changed" {
    dolt checkout -b firstbranch
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt sql -q 'insert into test values (1,1,1,1,1,1)'
    dolt add .
    dolt commit -m "created table"

    dolt checkout -b newbranch
    dolt sql -q='RENAME TABLE test TO newname'
    dolt sql -q 'insert into newname values (2,1,1,1,1,1)'
    dolt add .
    dolt commit -m "renamed table and added data"

    # confirm a difference exists
    run dolt diff --sql newbranch firstbranch
    [ "$status" -eq 0 ]
    [[ "$output" != "" ]] || false

    dolt diff --sql newbranch firstbranch > query
    dolt checkout firstbranch
    dolt sql < query
    rm query
    dolt add .
    dolt commit -m "Reconciled with newbranch"

    # confirm that both branches have the same content
    run dolt diff --sql newbranch firstbranch
    [ "$status" -eq 0 ]
    [[ "$output" = "" ]] || false
}

@test "diff sql recreates tables with all types" {

    skip "This test fails due to type incompatibility between SQL and Noms"

    dolt checkout -b firstbranch
    dolt checkout -b newbranch
    dolt sql <<SQL
CREATE TABLE test (
  \`pk\` BIGINT NOT NULL COMMENT 'tag:0',
  \`int\` BIGINT COMMENT 'tag:1',
  \`string\` LONGTEXT COMMENT 'tag:2',
  \`boolean\` BOOLEAN COMMENT 'tag:3',
  \`float\` DOUBLE COMMENT 'tag:4',
  \`uint\` BIGINT UNSIGNED COMMENT 'tag:5',
  \`uuid\` CHAR(36) CHARACTER SET ascii COLLATE ascii_bin COMMENT 'tag:6',
  PRIMARY KEY (pk)
);
SQL
    # dolt table import -u test `batshelper 1pksupportedtypes.csv`
    dolt add .
    dolt commit -m "created new table"

    # confirm a difference exists
    run dolt diff --sql newbranch firstbranch
    [ "$status" -eq 0 ]
    [[ "$output" != "" ]] || false

    dolt diff --sql newbranch firstbranch > query
    dolt checkout firstbranch
    dolt sql < query
    rm query
    dolt add test
    dolt commit -m "Reconciled with newbranch"

    # confirm that both branches have the same content
    dolt diff --sql newbranch firstbranch
    run dolt diff --sql newbranch firstbranch
    [ "$status" -eq 0 ]
    [[ "$output" = "" ]] || false
}

@test "sql diff supports all types" {
    dolt checkout -b firstbranch
    dolt sql <<SQL
CREATE TABLE test (
  \`pk\` BIGINT NOT NULL COMMENT 'tag:0',
  \`int\` BIGINT COMMENT 'tag:1',
  \`string\` LONGTEXT COMMENT 'tag:2',
  \`boolean\` BOOLEAN COMMENT 'tag:3',
  \`float\` DOUBLE COMMENT 'tag:4',
  \`uint\` BIGINT UNSIGNED COMMENT 'tag:5',
  \`uuid\` CHAR(36) CHARACTER SET ascii COLLATE ascii_bin COMMENT 'tag:6',
  PRIMARY KEY (pk)
);
SQL
    dolt table import -u test `batshelper 1pksupportedtypes.csv`
    dolt add .
    dolt commit -m "create/init table test"

    # for each query file in helper/queries/1pksuppportedtypes/
    # run query on db, create sql diff patch, confirm they're equivalent
    dolt branch newbranch
    for query in delete add update create_table
    do
        dolt checkout newbranch
        dolt sql < $BATS_TEST_DIRNAME/helper/queries/1pksupportedtypes/$query.sql
        dolt add .
        dolt commit -m "applied $query query"

        # confirm a difference exists
        run dolt diff --sql newbranch firstbranch
        [ "$status" -eq 0 ]
        [[ "$output" != "" ]] || false

        dolt diff --sql newbranch firstbranch > patch.sql
        dolt checkout firstbranch
        dolt sql < patch.sql
        rm patch.sql
        dolt add .
        dolt commit -m "Reconciled with newbranch"

        # confirm that both branches have the same content
        run dolt diff --sql newbranch firstbranch
        [ "$status" -eq 0 ]
        [[ "$output" = "" ]] || false
    done
}

@test "sql diff supports multiple primary keys" {
    dolt checkout -b firstbranch
    dolt sql <<SQL
CREATE TABLE test (
  pk1 BIGINT NOT NULL COMMENT 'tag:0',
  pk2 BIGINT NOT NULL COMMENT 'tag:1',
  c1 BIGINT COMMENT 'tag:2',
  c2 BIGINT COMMENT 'tag:3',
  c3 BIGINT COMMENT 'tag:4',
  c4 BIGINT COMMENT 'tag:5',
  c5 BIGINT COMMENT 'tag:6',
  PRIMARY KEY (pk1,pk2)
);
SQL
    dolt table import -u test `batshelper 2pk5col-ints.csv`
    dolt add .
    dolt commit -m "create/init table test"

    # for each query file in helper/queries/2pk5col-ints/
    # run query on db, create sql diff patch, confirm they're equivalent
    dolt branch newbranch
    for query in delete add update single_pk_update all_pk_update create_table
    do
        dolt checkout newbranch
        dolt sql < $BATS_TEST_DIRNAME/helper/queries/2pk5col-ints/$query.sql
        dolt add .
        dolt diff --sql
        dolt commit -m "applied $query query "

        # confirm a difference exists

        run dolt diff --sql newbranch firstbranch
        [ "$status" -eq 0 ]
        [[ "$output" != "" ]] || false

        dolt diff --sql newbranch firstbranch > patch.sql
        dolt checkout firstbranch
        dolt sql < patch.sql
        rm patch.sql
        dolt add .
        dolt commit -m "Reconciled with newbranch"

        # confirm that both branches have the same content
        run dolt diff --sql newbranch firstbranch
        [ "$status" -eq 0 ]
        [[ "$output" = "" ]] || false
    done
}