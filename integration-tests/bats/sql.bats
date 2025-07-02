#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    export DOLT_DBNAME_REPLACE="true"
    dolt sql <<SQL
CREATE TABLE one_pk (
  pk BIGINT NOT NULL,
  c1 BIGINT,
  c2 BIGINT,
  c3 BIGINT,
  c4 BIGINT,
  c5 BIGINT,
  PRIMARY KEY (pk)
);
CREATE TABLE two_pk (
  pk1 BIGINT NOT NULL,
  pk2 BIGINT NOT NULL,
  c1 BIGINT,
  c2 BIGINT,
  c3 BIGINT,
  c4 BIGINT,
  c5 BIGINT,
  PRIMARY KEY (pk1,pk2)
);
CREATE TABLE has_datetimes (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  date_created DATETIME COMMENT 'tag:1',
  PRIMARY KEY (pk)
);
INSERT INTO one_pk (pk,c1,c2,c3,c4,c5) VALUES (0,0,0,0,0,0),(1,10,10,10,10,10),(2,20,20,20,20,20),(3,30,30,30,30,30);
INSERT INTO two_pk (pk1,pk2,c1,c2,c3,c4,c5) VALUES (0,0,0,0,0,0,0),(0,1,10,10,10,10,10),(1,0,20,20,20,20,20),(1,1,30,30,30,30,30);
INSERT INTO has_datetimes (pk, date_created) VALUES (0, '2020-02-17 00:00:00');
SQL
}

teardown() {
    assert_feature_version
    teardown_common
}

# bats test_tags=no_lambda
@test "sql: multi statement query returns accurate timing" {
  dolt sql -q "CREATE TABLE t(a int);"
  dolt sql -q "INSERT INTO t VALUES (1);"
  dolt sql -q "CREATE TABLE t1(b int);"
  run $BATS_TEST_DIRNAME/sql-shell-multi-stmt-timings.expect
  [[ "$output" =~ "Query OK, 1 row affected (1".*" sec)" ]] || false
  [[ "$output" =~ "Query OK, 1 row affected (2".*" sec)" ]] || false
  [[ "$output" =~ "Query OK, 1 row affected (3".*" sec)" ]] || false
}

@test "sql: check --data-dir used from a completely different location and still resolve DB names" {
    # remove config files
    rm -rf .doltcfg
    rm -rf db_dir

    mkdir db_dir
    cd db_dir
    ROOT_DIR=$(pwd)

    # create an alternate database, without the table
    mkdir dba
    cd dba
    dolt init
    cd ..
    dolt sql -q "create table dba_tbl (id int)"

    mkdir dbb
    cd dbb
    dolt init
    dolt sql -q "create table dbb_tbl (id int)"

    # Ensure --data-dir flag is really used by changing the cwd.
    cd /tmp

    run dolt --data-dir="$ROOT_DIR/dbb" sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "dbb_tbl" ]] || false

    run dolt --data-dir="$ROOT_DIR/dba" sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "dba_tbl" ]] || false

    # Default to first DB alphabetically.
    run dolt --data-dir="$ROOT_DIR" sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "dba_tbl" ]] || false

    # --use-db arg can be used to be specific.
    run dolt --data-dir="$ROOT_DIR" --use-db=dbb sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "dbb_tbl" ]] || false

    # Redundant use of the flag is OK.
    run dolt --data-dir="$ROOT_DIR/dbb" --use-db=dbb sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "dbb_tbl" ]] || false

    # Use of the use-db flag when we have a different DB specified by data-dir should error.
    run dolt --data-dir="$ROOT_DIR/dbb" --use-db=dba sql -q "show tables"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "provided --use-db dba does not exist" ]] || false
}

@test "sql: USE information schema and mysql databases" {
    run dolt sql <<SQL
USE information_schema;
show tables;
SQL

    # spot check result
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "Database changed" ]] || false
    [[ "$output" =~ "columns" ]] || false

    run dolt sql <<SQL
USE mysql;
show tables;
SQL

    # spot check result
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "Database changed" ]] || false
    [[ "$output" =~ "role_edges" ]] || false

}

@test "sql: prevent LOAD_FILE() from accessing files outside of working directory" {
    echo "should not be able to read this" > ../dont_read.txt
    echo "should be able to read this" > ./do_read.txt

    run dolt sql -q "select load_file('../dont_read.txt')";
    [ "$status" -eq 0 ]
    [[ "$output" =~ "NULL" ]] || false
    [[ "$output" != "should not be able to read this" ]] || false

    run dolt sql -q "select load_file('./do_read.txt')";
    [ "$status" -eq 0 ]
    [[ "$output" =~ "should be able to read this" ]] || false
}

@test "sql: ignore an empty .dolt directory" {
    mkdir empty_dir
    cd empty_dir

    mkdir .dolt
    dolt sql -q "select 1"
}

@test "sql: handle importing files with bom headers" {
    dolt sql < $BATS_TEST_DIRNAME/helper/with_utf8_bom.sql
    dolt table rm t1
    dolt sql < $BATS_TEST_DIRNAME/helper/with_utf16le_bom.sql
    dolt table rm t1
    dolt sql < $BATS_TEST_DIRNAME/helper/with_utf16be_bom.sql
    dolt table rm t1
}
