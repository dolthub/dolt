#!/usr/bin/env bats                                                             
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}

@test "sql-check-constraints: basic tests for check constraints" {
    dolt sql <<SQL
CREATE table t1 (
       a INTEGER PRIMARY KEY check (a > 3),
       b INTEGER check (b > a)
);
SQL

    dolt sql -q "insert into t1 values (5, 6)"

    run dolt sql -q "insert into t1 values (3, 4)"
    [ $status -eq 1 ]
    [[ "$output" =~ "constraint" ]] || false

    run dolt sql -q "insert into t1 values (4, 2)"
    [ $status -eq 1 ]
    [[ "$output" =~ "constraint" ]] || false

    dolt sql <<SQL
CREATE table t2 (
       a INTEGER PRIMARY KEY,
       b INTEGER
);
ALTER TABLE t2 ADD CONSTRAINT chk1 CHECK (a > 3);
ALTER TABLE t2 ADD CONSTRAINT chk2 CHECK (b > a);
SQL

    dolt sql -q "insert into t2 values (5, 6)"
    dolt sql -q "insert into t2 values (6, NULL)"

    run dolt sql -q "insert into t2 values (3, 4)"
    [ $status -eq 1 ]
    [[ "$output" =~ "constraint" ]] || false

    run dolt sql -q "insert into t2 values (4, 2)"
    [ $status -eq 1 ]
    [[ "$output" =~ "constraint" ]] || false

    dolt sql -q "ALTER TABLE t2 DROP CONSTRAINT chk1;"
    dolt sql -q "insert into t2 values (3, 4)"
    
    run dolt sql -q "insert into t2 values (4, 2)"
    [ $status -eq 1 ]
    [[ "$output" =~ "constraint" ]] || false

    dolt sql -q "ALTER TABLE t2 DROP CONSTRAINT chk2;"    
    dolt sql -q "insert into t2 values (4, 2)"

    # t1 should still have its constraints
    run dolt sql -q "insert into t1 values (4, 2)"
    [ $status -eq 1 ]
    [[ "$output" =~ "constraint" ]] || false
}

@test "sql-check-constraints: check constraints survive adding a new column" {
    dolt sql <<SQL
create table foo (
       pk int,
       c1 int,
       CHECK (c1 > 3),
       PRIMARY KEY (pk)
);
ALTER TABLE foo ADD COLUMN j int;
SQL
    run dolt schema show
    [ $status -eq 0 ]
    echo $output
    [[ "$output" =~ "CHECK" ]] || false
    [[ "$output" =~ "`c1` > 3" ]] || false

    # check information_schema.CHECK_CONSTRAINTS table
    run dolt sql -q "select constraint_catalog, constraint_name, check_clause from information_schema.CHECK_CONSTRAINTS;" -r csv
    [[ "$output" =~ "def,foo_chk_rvgogafi,(\`c1\` > 3)" ]] || false
}

@test "sql-check-constraints: check constraints survive renaming a column" {
    dolt sql <<SQL
create table foo (
       pk int,
       c1 int,
       j int,
       CHECK (c1 > 3),
       PRIMARY KEY (pk)
);
ALTER TABLE foo RENAME COLUMN j to j2;
SQL
    run dolt schema show
    [ $status -eq 0 ]
    echo $output
    [[ "$output" =~ "CHECK" ]] || false
    [[ "$output" =~ "`c1` > 3" ]] || false
}

@test "sql-check-constraints: check constraints survive modifying a column" {
    dolt sql <<SQL
create table foo (
       pk int,
       c1 int,
       j int,
       CHECK (c1 > 3),
       PRIMARY KEY (pk)
);
ALTER TABLE foo MODIFY COLUMN j int COMMENT 'j column';
SQL
    run dolt schema show
    [ $status -eq 0 ]
    echo $output
    [[ "$output" =~ "CHECK" ]] || false
    [[ "$output" =~ "`c1` > 3" ]] || false
}

@test "sql-check-constraints: check constraints survive dropping a column" {
    dolt sql <<SQL
create table foo (
       pk int,
       c1 int,
       j int,
       CHECK (c1 > 3),
       PRIMARY KEY (pk)
);
ALTER TABLE foo DROP COLUMN j;
SQL
    run dolt schema show
    [ $status -eq 0 ]
    echo $output
    [[ "$output" =~ "CHECK" ]] || false
    [[ "$output" =~ "`c1` > 3" ]] || false
}

@test "sql-check-constraints: check constraints survive adding a primary key" {
    dolt sql <<SQL
create table foo (
       pk int,
       c1 int,
       CHECK (c1 > 3)
);
ALTER TABLE foo ADD PRIMARY KEY(pk);
SQL
    run dolt schema show
    [ $status -eq 0 ]
    echo $output
    [[ "$output" =~ "CHECK" ]] || false
    [[ "$output" =~ "`c1` > 3" ]] || false
}

@test "sql-check-constraints: check constraints survive dropping a primary key" {
    dolt sql <<SQL
create table foo (
       pk int,
       c1 int,
       CHECK (c1 > 3),
       PRIMARY KEY (pk)
);
ALTER TABLE foo DROP PRIMARY KEY;
SQL
    run dolt schema show
    [ $status -eq 0 ]
    echo $output
    [[ "$output" =~ "CHECK" ]] || false
    [[ "$output" =~ "`c1` > 3" ]] || false
}

@test "sql-check-constraints: check constraints survive renaming a table" {
    dolt sql <<SQL
create table foo (
       pk int,
       c1 int,
       CHECK (c1 > 3),
       PRIMARY KEY (pk)
);
RENAME TABLE foo to foo2;
SQL
    run dolt schema show
    [ $status -eq 0 ]
    echo $output
    [[ "$output" =~ "CHECK" ]] || false
    [[ "$output" =~ "`c1` > 3" ]] || false
}

