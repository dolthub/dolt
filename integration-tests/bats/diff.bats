#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    skip_nbf_dolt_1

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
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "diff: clean working set" {
    dolt add .
    dolt commit -m table
    dolt sql -q 'insert into test values (0,0,0,0,0,0)'
    dolt add .
    dolt commit -m row
    run dolt diff
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt diff head
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    dolt diff head^
    run dolt diff head^
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+  | 0" ]] || false
    run dolt diff head^ head
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+  | 0" ]] || false
    run dolt diff head head^
    [ "$status" -eq 0 ]
    [[ "$output" =~ "-  | 0" ]] || false
}

@test "diff: dirty working set" {
    dolt add .
    dolt commit -m table
    dolt sql -q 'insert into test values (0,0,0,0,0,0)'
    run dolt diff
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+  | 0" ]] || false
    run dolt diff head
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+  | 0" ]] || false
    dolt add .
    run dolt diff
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt diff head
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+  | 0" ]] || false
}

@test "diff: data diff only" {
    dolt commit -am "First commit"

    dolt sql -q "insert into test (pk) values (10);"

    dolt diff
    run dolt diff
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "CREATE TABLE" ]]
    [[ "$output" =~ "|     | pk | c1   | c2   | c3   | c4   | c5   |" ]] || false
    [[ "$output" =~ "|  +  | 10 | NULL | NULL | NULL | NULL | NULL |" ]] || false
}

@test "diff: schema diff only" {
    dolt commit -am "First commit"

    dolt sql -q "alter table test drop column c1"

    dolt diff
    run dolt diff
    [ "$status" -eq 0 ]
    [[ "$output" =~ "CREATE TABLE" ]]

    # TODO: column ordering on the first line should respect original
    # schema order, seems to be putting non-common columns at end
    [[ "$output" =~ "|  <  | pk | c2 | c3 | c4 | c5 | c1 |" ]] || false
    [[ "$output" =~ "|  >  | pk | c2 | c3 | c4 | c5 |    |" ]] || false
}

@test "diff: with table args" {
    dolt sql -q 'create table other (pk int not null primary key)'
    dolt add .
    dolt commit -m tables
    dolt sql -q 'insert into test values (0,0,0,0,0,0)'
    dolt sql -q 'insert into other values (9)'
    run dolt diff test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+  | 0" ]] || false
    [[ ! "$output" =~ "+  | 9" ]] || false
    run dolt diff other
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+  | 9" ]] || false
    [[ ! "$output" =~ "+  | 0" ]] || false
    run dolt diff test other
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+  | 0" ]] || false
    [[ "$output" =~ "+  | 9" ]] || false
    dolt add .
    run dolt diff head test other
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+  | 0" ]] || false
    [[ "$output" =~ "+  | 9" ]] || false
    dolt commit -m rows
    run dolt diff head^ head test other
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+  | 0" ]] || false
    [[ "$output" =~ "+  | 9" ]] || false
    run dolt diff head^ head fake
    [ "$status" -ne 0 ]
    [[ "$output" =~ "table fake does not exist in either diff root" ]] || false
}

@test "diff: with table and branch of the same name" {
    dolt sql -q 'create table dolomite (pk int not null primary key)'
    dolt add .
    dolt commit -m tables
    dolt branch dolomite
    dolt sql -q 'insert into dolomite values (9)'
    dolt add .
    dolt commit -m 'intermediate commit'
    dolt sql -q 'insert into test values (0,0,0,0,0,0)'
    # branch/commit args get preference over tables
    run dolt diff dolomite
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+  | 9" ]] || false
    [[ "$output" =~ "+  | 0" ]] || false
    run dolt diff dolomite test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+  | 0" ]] || false
    [[ ! "$output" =~ "+  | 9" ]] || false
    run dolt diff dolomite head dolomite
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+  | 9" ]] || false
    [[ ! "$output" =~ "+  | 0" ]] || false
    run dolt diff head^ head dolomite
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+  | 9" ]] || false
    [[ ! "$output" =~ "+  | 0" ]] || false
    dolt branch -D dolomite
    dolt sql -q 'insert into dolomite values (8)'
    run dolt diff dolomite
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+  | 8" ]] || false
    [[ ! "$output" =~ "+  | 0" ]] || false
}

@test "diff: with index and foreign key changes" {
    dolt sql <<SQL
CREATE TABLE parent (
    pk bigint PRIMARY KEY,
    c1 bigint,
    c2 bigint,
    INDEX c1 (c1)
);
ALTER TABLE test ADD CONSTRAINT fk1 FOREIGN KEY (c1) REFERENCES parent(c1);
SQL
    dolt add -A
    dolt commit -m "added parent table, foreign key"
    dolt sql <<SQL
ALTER TABLE parent ADD INDEX c2 (c2);
ALTER TABLE test DROP FOREIGN KEY fk1;
ALTER TABLE parent DROP INDEX c1;
ALTER TABLE test ADD CONSTRAINT fk2 FOREIGN KEY (c2) REFERENCES parent(c2);
SQL
    
    dolt diff test
    run dolt diff test
    [ "$status" -eq 0 ]
    [[ "$output" =~ '-  CONSTRAINT `fk1` FOREIGN KEY (`c1`) REFERENCES `parent` (`c1`)' ]] || false
    [[ "$output" =~ '+  KEY `c2` (`c2`),' ]] || false
    [[ "$output" =~ '+  CONSTRAINT `fk2` FOREIGN KEY (`c2`) REFERENCES `parent` (`c2`)' ]] || false

    dolt diff parent
    run dolt diff parent
    [ "$status" -eq 0 ]
    [[ "$output" =~ '-  KEY `c1` (`c1`)' ]] || false
    [[ "$output" =~ '+  KEY `c2` (`c2`)' ]] || false
}

@test "diff: summary comparing working table to last commit" {
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

@test "diff: summary comparing row with a deleted cell and an added cell" {
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

@test "diff: summary comparing two branches" {
    dolt checkout -b firstbranch
    dolt sql -q "insert into test values (0, 0, 0, 0, 0, 0)"
    dolt add test
    dolt commit -m "Added one row"
    dolt checkout -b newbranch
    dolt sql -q "insert into test values (1, 1, 1, 1, 1, 1)"
    dolt add test
    dolt commit -m "Added another row"
    run dolt diff --summary firstbranch newbranch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1 Row Unmodified (100.00%)" ]] || false
    [[ "$output" =~ "1 Row Added (100.00%)" ]] || false
    [[ "$output" =~ "0 Rows Deleted (0.00%)" ]] || false
    [[ "$output" =~ "0 Rows Modified (0.00%)" ]] || false
    [[ "$output" =~ "0 Cells Modified (0.00%)" ]] || false
    [[ "$output" =~ "(1 Entry vs 2 Entries)" ]] || false
}

@test "diff: summary shows correct changes after schema change" {
    cat <<DELIM > employees.csv
"id","first name","last name","title","start date","end date"
0,tim,sehn,ceo,"",""
1,aaron,son,founder,"",""
2,brian,hendricks,founder,"",""
DELIM
    dolt table import -c -pk=id employees employees.csv
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

@test "diff: summary gets summaries for all tables with changes" {
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

@test "diff: with where clause" {
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

    dolt checkout main
    dolt checkout -b test2
    dolt sql -q "insert into test values (5, 55, 0, 0, 0, 0)"
    dolt add .
    dolt commit -m "committed to branch test2"

    dolt checkout main
    run dolt diff test1 test2
    [ "$status" -eq 0 ]
    [[ "$output" =~ "44" ]] || false
    [[ "$output" =~ "55" ]] || false

    run dolt diff test1 test2 --where "pk=4"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "44" ]] || false
    ! [[ "$output" =~ "55" ]] || false

    run dolt diff test1 test2 --where "from_pk=4"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "44" ]] || false
    ! [[ "$output" =~ "55" ]] || false

    run dolt diff test1 test2 --where "from_pk=5"
    [ "$status" -eq 0 ]
    ! [[ "$output" =~ "44" ]] || false
    ! [[ "$output" =~ "55" ]] || false

    run dolt diff test1 test2 --where "to_pk=5"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "55" ]] || false
    ! [[ "$output" =~ "44" ]] || false

    run dolt diff test1 test2 --where "to_pk=4"
    [ "$status" -eq 0 ]
    ! [[ "$output" =~ "44" ]] || false
    ! [[ "$output" =~ "55" ]] || false
}

@test "diff: with where clause errors" {
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

@test "diff: --cached" {
    run dolt diff --cached
    [ $status -eq 0 ]
    [ "$output" = "" ]
    dolt add test
    run dolt diff --cached
    [ $status -eq 0 ]
    [[ $output =~ "added table" ]] || false  
    dolt commit -m "First commit"
    dolt sql -q "insert into test values (0, 0, 0, 0, 0, 0)"
    run dolt diff
    [ $status -eq 0 ]
    CORRECT_DIFF=$output
    dolt add test
    run dolt diff --cached
    [ $status -eq 0 ]
    [ "$output" = "$CORRECT_DIFF" ]
    # Make sure it ignores changes to the working set that aren't staged
    dolt sql -q "create table test2 (pk int, c1 int, primary key(pk))"
    run dolt diff --cached
    [ $status -eq 0 ]
    [ "$output" = "$CORRECT_DIFF" ]
}

@test "diff: with invalid ref does not panic" {
    dolt add .
    dolt commit -m table
    dolt checkout -b test-branch
    dolt sql -q "insert into test values (0, 0, 0, 0, 0, 0)"
    dolt add test
    dolt commit -m "added row"
    FIRST_COMMIT=`dolt log | grep commit | cut -d " " -f 2 | tail -1`
    run dolt diff $FIRST_COMMIT test-branch
    [ $status -eq 0 ]
    [[ ! $output =~ "panic" ]]
    run dolt diff main@$FIRST_COMMIT test-branch
    [ $status -eq 1 ]
    [[ ! $output =~ "panic" ]]
    run dolt diff ref.with.period test-branch
    [ $status -eq 1 ]
    [[ ! $output =~ "panic" ]]
}

@test "diff: with foreign key and sql output" {
    dolt sql <<SQL
CREATE TABLE parent (
  id int PRIMARY KEY,
  pv1 int,
  pv2 int,
  INDEX v1 (pv1),
  INDEX v2 (pv2)
);
SQL
    dolt add -A
    dolt commit -m "hi"
    dolt sql <<SQL
CREATE TABLE child (
  id int primary key,
  cv1 int,
  cv2 int,
  CONSTRAINT fk_named FOREIGN KEY (cv1) REFERENCES parent(pv1)
);
SQL
    run dolt diff -s -r=sql main
    [ $status -eq 0 ]
    [[ $output =~ "CONSTRAINT \`fk_named\` FOREIGN KEY (\`cv1\`) REFERENCES \`parent\` (\`pv1\`)" ]] || false
}

@test "diff: table with same name on different branches with different primary key sets" {
    dolt branch another-branch
    dolt sql <<SQL
CREATE TABLE a (
  id int PRIMARY KEY,
  pv1 int,
  pv2 int
);
SQL
    dolt add -A
    dolt commit -m "hi"
    dolt checkout another-branch
    dolt sql <<SQL
CREATE TABLE a (
  id int,
  cv1 int,
  cv2 int,
  primary key (id, cv1)
);
SQL
    dolt add -A
    dolt commit -m "hello"
    run dolt diff main another-branch
    echo $output
    ! [[ "$output" =~ "panic" ]] || false
    [[ "$output" =~ "pv1" ]] || false
    [[ "$output" =~ "cv1" ]] || false
    [ $status -eq 0 ]
}

@test "diff: sql update queries only show changed columns" {
    dolt sql -q "create table t(pk int primary key, val1 int, val2 int)"
    dolt sql -q "INSERT INTO t VALUES (1, 1, 1)"

    dolt commit -am "cm1"

    dolt sql -q "UPDATE t SET val1=2 where pk=1"
    run dolt diff -r sql
    [ $status -eq 0 ]
    [[ "$output" = 'UPDATE `t` SET `val1`=2 WHERE (`pk`=1);' ]] || false

    dolt commit -am "cm2"

    dolt sql -q "UPDATE t SET val1=3, val2=4 where pk = 1"
    run dolt diff -r sql
    [ $status -eq 0 ]
    [[ "$output" = 'UPDATE `t` SET `val1`=3,`val2`=4 WHERE (`pk`=1);' ]] || false

    dolt commit -am "cm3"

    dolt sql -q "UPDATE t SET val1=3 where (pk=1);"
    run dolt diff -r sql
    [ $status -eq 0 ]
    [[ "$output" = '' ]] || false

    dolt sql -q "alter table t add val3 int"
    dolt commit -am "cm4"

    dolt sql -q "update t set val1=30,val3=4 where pk=1"
    run dolt diff -r sql
    [ $status -eq 0 ]
    [[ "$output" = 'UPDATE `t` SET `val1`=30,`val3`=4 WHERE (`pk`=1);' ]] || false
}

@test "diff: run through some keyless sql diffs" {
    dolt sql -q "create table t(pk int, val int)"
    dolt commit -am "cm1"

    dolt sql -q "INSERT INTO t values (1, 1)"
    run dolt diff -r sql
    [ $status -eq 0 ]
    [[ "$output" = 'INSERT INTO `t` (`pk`,`val`) VALUES (1,1);' ]] || false

    dolt commit -am "cm2"

    dolt sql -q "INSERT INTO t values (1, 1)"
    run dolt diff -r sql
    [ $status -eq 0 ]
    [[ "$output" = 'INSERT INTO `t` (`pk`,`val`) VALUES (1,1);' ]] || false

    dolt commit -am "cm3"

    dolt sql -q "UPDATE t SET val = 2 where pk = 1"
    run dolt diff -r sql
    [ $status -eq 0 ]
    [ "${lines[0]}" = 'DELETE FROM `t` WHERE (`pk`=1 AND `val`=1);' ]
    [ "${lines[1]}" = 'DELETE FROM `t` WHERE (`pk`=1 AND `val`=1);' ]
    [ "${lines[2]}" = 'INSERT INTO `t` (`pk`,`val`) VALUES (1,2);' ]
    [ "${lines[3]}" = 'INSERT INTO `t` (`pk`,`val`) VALUES (1,2);' ]

    dolt commit -am "cm4"

    dolt sql -q "DELETE FROM t WHERE val < 3"
    run dolt diff -r sql
    [ $status -eq 0 ]
    [ "${lines[0]}" = 'DELETE FROM `t` WHERE (`pk`=1 AND `val`=2);' ]
    [ "${lines[1]}" = 'DELETE FROM `t` WHERE (`pk`=1 AND `val`=2);' ]

    dolt commit -am "cm5"

    dolt sql -q "alter table t add primary key (pk)"
    run dolt diff -r sql
    [ $status -eq 0 ]
    [ "${lines[0]}" = 'ALTER TABLE `t` DROP PRIMARY KEY;' ]
    [ "${lines[1]}" = 'ALTER TABLE `t` ADD PRIMARY KEY (pk);' ]
    [ "${lines[2]}" = 'warning: skipping data diff due to primary key set change' ]

    dolt commit -am "cm6"

    dolt sql -q "alter table t add column pk2 int"
    dolt sql -q "alter table t drop primary key"
    dolt sql -q "alter table t add primary key (pk, val)"
    run dolt diff -r sql
    [ $status -eq 0 ]
    [ "${lines[0]}" = 'ALTER TABLE `t` ADD `pk2` INT;' ]
    [ "${lines[1]}" = 'ALTER TABLE `t` DROP PRIMARY KEY;' ]
    [ "${lines[2]}" = 'ALTER TABLE `t` ADD PRIMARY KEY (pk,val);' ]
    [ "${lines[3]}" = 'warning: skipping data diff due to primary key set change' ]
}

@test "diff: adding and removing primary key should leave not null constraint" {
    skip "TODO diff needs a better way to indicate constraint changes"
    dolt sql -q "create table t(pk int, val int)"
    dolt commit -am "creating table"

    dolt sql -q "alter table t add primary key (pk)"
    run dolt diff -r sql
    [ $status -eq 0 ]
    [ "${lines[0]}" = 'ALTER TABLE `t` DROP PRIMARY KEY;' ]
    [ "${lines[1]}" = 'ALTER TABLE `t` ADD PRIMARY KEY (pk);' ]
    [ "${lines[2]}" = 'warning: skipping data diff due to primary key set change' ]

    dolt sql -q "alter table t drop primary key"
    run dolt diff -r sql
    [ $status -eq 0 ]
    [ "${lines[0]}" = 'ALTER TABLE `t` RENAME COLUMN `pk` TO `pk`;' ]
}

@test "diff: created and dropped tables include schema and data changes in results" {
  dolt sql -q "create table a(pk int primary key)"
  dolt commit -am "creating table a"
  dolt sql -q "insert into a values (1), (2)"
  dolt commit -am "inserting data into table a"
  dolt sql -q "drop table a"
  dolt commit -am "dropping table a"

  run dolt diff HEAD~3 HEAD~2
  [[ $output =~ 'added table' ]] || false
  [[ $output =~ '+CREATE TABLE `a` (' ]] || false

  run dolt diff HEAD~3 HEAD~1
  [[ $output =~ 'added table' ]] || false
  [[ $output =~ '+CREATE TABLE `a` (' ]] || false
  [[ $output =~ "+  | 1 " ]] || false

  run dolt diff HEAD~1 HEAD
  [[ $output =~ 'deleted table' ]] || false
  [[ $output =~ '-CREATE TABLE `a` (' ]] || false
  [[ $output =~ "-  | 1 " ]] || false
}

@test "diff: large diff does not drop rows" {
    dolt sql -q "create table t(pk int primary key, val int)"

    VALUES=""
    for i in {1..1000}
    do
      if [ $i -eq 1 ]
      then
        VALUES="${VALUES}($i,$i)"
      else
        VALUES="${VALUES},($i,$i)"
      fi
    done

    dolt sql -q "INSERT INTO t values $VALUES"
    dolt commit -am "Add the initial rows"

    dolt sql -q "UPDATE t set val = val + 1 WHERE pk < 10000"
    dolt commit -am "make a bulk update creating a large diff"

    run dolt diff HEAD~1
    [ "${#lines[@]}" -eq 2007 ] # 2000 diffs + 6 for top rows before data + 1 for bottom row of table
}