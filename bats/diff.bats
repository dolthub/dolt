#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}

@test "diff summary comparing working table to last commit" {
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
    dolt sql -q "insert into test values (0, 0, 0, 0, 0, 0)"
    dolt sql -q "insert into test values (1, 1, 1, 1, 1, 1)"
    dolt add test
    dolt commit -m "table created"
    dolt sql -q "insert into test values (2, 11, 0, 0, 0, 0)"
    dolt sql -q "insert into test values (3, 11, 0, 0, 0, 0)"
    run dolt diff --summary
    [ "$status" -eq 0 ]
    [[ "$output" =~ "2 Rows Unmodified (100.00%)" ]] || false
    [[ "$output" =~ "2 Rows Added (100.00%)" ]] || false
    [[ "$output" =~ "0 Rows Deleted (0.00%)" ]] || false
    [[ "$output" =~ "0 Rows Modified (0.00%)" ]] || false
    [[ "$output" =~ "0 Cells Modified (0.00%)" ]] || false
    [[ "$output" =~ "(2 Entries vs 4 Entries)" ]] || false

    dolt add test
    dolt commit -m "added two rows"
    dolt sql -q "replace into test values (0, 11, 0, 0, 0, 6)"
    run dolt diff --summary
    [ "$status" -eq 0 ]
    [[ "$output" =~ "3 Rows Unmodified (75.00%)" ]] || false
    [[ "$output" =~ "0 Rows Added (0.00%)" ]] || false
    [[ "$output" =~ "0 Rows Deleted (0.00%)" ]] || false
    [[ "$output" =~ "1 Row Modified (25.00%)" ]] || false
    [[ "$output" =~ "2 Cells Modified (8.33%)" ]] || false
    [[ "$output" =~ "(4 Entries vs 4 Entries)" ]] || false

    dolt add test
    dolt commit -m "modified first row"
    dolt sql -q "delete from test where pk = 0"
    run dolt diff --summary
    [ "$status" -eq 0 ]
    [[ "$output" =~ "3 Rows Unmodified (75.00%)" ]] || false
    [[ "$output" =~ "0 Rows Added (0.00%)" ]] || false
    [[ "$output" =~ "1 Row Deleted (25.00%)" ]] || false
    [[ "$output" =~ "0 Rows Modified (0.00%)" ]] || false
    [[ "$output" =~ "0 Cells Modified (0.00%)" ]] || false
    [[ "$output" =~ "(4 Entries vs 3 Entries)" ]] || false
}

@test "diff summary comparing row with a deleted cell and an added cell" {
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
    dolt add test
    dolt commit -m "create table"
    dolt sql -q "insert into test values (0, 1, 2, 3, 4, 5)"
    dolt add test
    dolt commit -m "put row"
    dolt sql -q "replace into test (pk, c1, c3, c4, c5) values (0, 1, 3, 4, 5)"
    run dolt diff --summary
    [ "$status" -eq 0 ]
    [[ "$output" =~ "0 Rows Unmodified (0.00%)" ]] || false
    [[ "$output" =~ "0 Rows Added (0.00%)" ]] || false
    [[ "$output" =~ "0 Rows Deleted (0.00%)" ]] || false
    [[ "$output" =~ "1 Row Modified (100.00%)" ]] || false
    [[ "$output" =~ "1 Cell Modified (16.67%)" ]] || false
    [[ "$output" =~ "(1 Entry vs 1 Entry)" ]] || false
    dolt add test
    dolt commit -m "row modified"
    dolt sql -q "replace into test values (0, 1, 2, 3, 4, 5)"
    run dolt diff --summary
    [ "$status" -eq 0 ]
    [[ "$output" =~ "0 Rows Unmodified (0.00%)" ]] || false
    [[ "$output" =~ "0 Rows Added (0.00%)" ]] || false
    [[ "$output" =~ "0 Rows Deleted (0.00%)" ]] || false
    [[ "$output" =~ "1 Row Modified (100.00%)" ]] || false
    [[ "$output" =~ "1 Cell Modified (16.67%)" ]] || false
    [[ "$output" =~ "(1 Entry vs 1 Entry)" ]] || false
}

@test "diff summary comparing two branches" {
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
    dolt sql -q "insert into test values (0, 0, 0, 0, 0, 0)"
    dolt add test
    dolt commit -m "Added one row"
    dolt checkout -b newbranch
    dolt sql -q "insert into test values (1, 1, 1, 1, 1, 1)"
    dolt add test
    dolt commit -m "Added another row"
    run dolt diff --summary newbranch firstbranch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1 Row Unmodified (100.00%)" ]] || false
    [[ "$output" =~ "1 Row Added (100.00%)" ]] || false
    [[ "$output" =~ "0 Rows Deleted (0.00%)" ]] || false
    [[ "$output" =~ "0 Rows Modified (0.00%)" ]] || false
    [[ "$output" =~ "0 Cells Modified (0.00%)" ]] || false
    [[ "$output" =~ "(1 Entry vs 2 Entries)" ]] || false
}

@test "diff summary shows correct changes after schema change" {
    dolt table import -c -s=`batshelper employees-sch.sql` employees `batshelper employees-tbl.json`
    dolt add employees
    dolt commit -m "Added employees table with data"
    dolt sql -q "alter table employees add city longtext"
    dolt sql -q "insert into employees values (3, 'taylor', 'bantle', 'software engineer', '', '', 'Santa Monica')"
    run dolt diff --summary
    [ "$status" -eq 0 ]
    [[ "$output" =~ "3 Rows Unmodified (100.00%)" ]] || false
    [[ "$output" =~ "1 Row Added (33.33%)" ]] || false
    [[ "$output" =~ "0 Rows Deleted (0.00%)" ]] || false
    [[ "$output" =~ "0 Rows Modified (0.00%)" ]] || false
    [[ "$output" =~ "0 Cells Modified (0.00%)" ]] || false
    [[ "$output" =~ "(3 Entries vs 4 Entries)" ]] || false
    dolt sql -q "replace into employees values (0, 'tim', 'sehn', 'ceo', '2 years ago', '', 'Santa Monica')"
    run dolt diff --summary
    [ "$status" -eq 0 ]
    [[ "$output" =~ "2 Rows Unmodified (66.67%)" ]] || false
    [[ "$output" =~ "1 Row Added (33.33%)" ]] || false
    [[ "$output" =~ "0 Rows Deleted (0.00%)" ]] || false
    [[ "$output" =~ "1 Row Modified (33.33%)" ]] || false
    [[ "$output" =~ "2 Cells Modified (11.11%)" ]] || false
    [[ "$output" =~ "(3 Entries vs 4 Entries)" ]] || false
}

@test "diff summary gets summaries for all tables with changes" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  c1 BIGINT,
  c2 BIGINT,
  c3 BIGINT,
  c4 BIGINT,
  c5 BIGINT,
  PRIMARY KEY (pk)
);
SQL
    dolt sql -q "insert into test values (0, 0, 0, 0, 0, 0)"
    dolt sql -q "insert into test values (1, 1, 1, 1, 1, 1)"
    dolt sql <<SQL
CREATE TABLE employees (
  \`id\` LONGTEXT NOT NULL,
  \`first name\` LONGTEXT,
  \`last name\` LONGTEXT,
  \`title\` LONGTEXT,
  \`start date\` LONGTEXT,
  \`end date\` LONGTEXT,
  PRIMARY KEY (id)
);
SQL
    dolt sql -q "insert into employees values (0, 'tim', 'sehn', 'ceo', '', '')"
    dolt add test employees
    dolt commit -m "test tables created"
    dolt sql -q "insert into test values (2, 11, 0, 0, 0, 0)"
    dolt sql -q "insert into employees values (1, 'brian', 'hendriks', 'founder', '', '')"
    run dolt diff --summary
    [ "$status" -eq 0 ]
    [[ "$output" =~ "diff --dolt a/test b/test" ]] || false
    [[ "$output" =~ "--- a/test @" ]] || false
    [[ "$output" =~ "+++ b/test @" ]] || false
    [[ "$output" =~ "diff --dolt a/employees b/employees" ]] || false
    [[ "$output" =~ "--- a/employees @" ]] || false
    [[ "$output" =~ "+++ b/employees @" ]] || false
}

@test "diff with where clause" {
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
    dolt sql -q "insert into test values (0, 0, 0, 0, 0, 0)"
    dolt sql -q "insert into test values (1, 1, 1, 1, 1, 1)"
    dolt add test
    dolt commit -m "table created"
    dolt sql -q "insert into test values (2, 22, 0, 0, 0, 0)"
    dolt sql -q "insert into test values (3, 33, 0, 0, 0, 0)"
    run dolt diff --where "pk=2"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "22" ]] || false
    ! [[ "$output" =~ "33" ]] || false

    dolt add test
    dolt commit -m "added two rows"

    dolt checkout -b test1
    dolt sql -q "insert into test values (4, 44, 0, 0, 0, 0)"
    dolt add .
    dolt commit -m "committed to branch test1"

    dolt checkout master
    dolt checkout -b test2
    dolt sql -q "insert into test values (5, 55, 0, 0, 0, 0)"
    dolt add .
    dolt commit -m "committed to branch test2"

    dolt checkout master
    run dolt diff test1 test2
    [ "$status" -eq 0 ]
    [[ "$output" =~ "44" ]] || false
    [[ "$output" =~ "55" ]] || false

    run dolt diff test1 test2 --where "pk=4"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "44" ]] || false
    ! [[ "$output" =~ "55" ]] || false

    run dolt diff test1 test2 --where "to_pk=4"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "44" ]] || false
    ! [[ "$output" =~ "55" ]] || false

    run dolt diff test1 test2 --where "to_pk=5"
    [ "$status" -eq 0 ]
    ! [[ "$output" =~ "44" ]] || false
    ! [[ "$output" =~ "55" ]] || false

    run dolt diff test1 test2 --where "from_pk=5"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "55" ]] || false
    ! [[ "$output" =~ "44" ]] || false

    run dolt diff test1 test2 --where "from_pk=4"
    [ "$status" -eq 0 ]
    ! [[ "$output" =~ "44" ]] || false
    ! [[ "$output" =~ "55" ]] || false
}

@test "diff with where clause errors" {
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
    dolt sql -q "insert into test values (0, 0, 0, 0, 0, 0)"
    dolt sql -q "insert into test values (1, 1, 1, 1, 1, 1)"
    dolt add test
    dolt commit -m "table created"
    dolt sql -q "insert into test values (2, 22, 0, 0, 0, 0)"
    dolt sql -q "insert into test values (3, 33, 0, 0, 0, 0)"

    run dolt diff --where "poop=0"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "failed to parse where clause" ]] || false

    dolt add test
    dolt commit -m "added two rows"

    run dolt diff --where "poop=0"
    skip "Bad where clause not found because the argument parsing logic is only triggered on existance of a diff"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "failed to parse where clause" ]] || false
}

@test "diff --cached" {
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
    skip "diff --cached not supported"
    run dolt diff --cached
    [ $status -wq 0 ]
    [ $output -eq "" ]
    dolt add test
    run dolt diff --cached
    [ $status -wq 0 ]
    [[ $output =~ "added table" ]] || false  
    dolt commit -m "First commit"
    dolt sql -q "insert into test values (0, 0, 0, 0, 0, 0)"
    run dolt diff
    [ $status -eq 0 ]
    CORRECT_DIFF=$output
    dolt add test
    run dolt diff --cached
    [ $status -eq 0 ]
    [ $output -eq $CORRECT_DIFF ]
}
