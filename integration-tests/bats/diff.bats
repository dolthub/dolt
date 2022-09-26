#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common

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
    [[ "$output" =~ "+ | 0" ]] || false

    run dolt diff head^ head
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+ | 0" ]] || false

    run dolt diff head head^
    [ "$status" -eq 0 ]
    [[ "$output" =~ "- | 0" ]] || false
}

@test "diff: dirty working set" {
    dolt add .
    dolt commit -m table
    dolt sql -q 'insert into test values (0,0,0,0,0,0)'
    run dolt diff
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+ | 0" ]] || false
    run dolt diff head
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+ | 0" ]] || false
    dolt add .
    run dolt diff
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt diff head
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+ | 0" ]] || false
}

@test "diff: data and schema changes" {
    dolt sql <<SQL
drop table test;
create table test (pk int primary key, c1 int, c2 int);
insert into test values (1,2,3);
insert into test values (4,5,6);
SQL
    dolt add .
    dolt commit -am "First commit"

    dolt sql <<SQL
alter table test 
drop column c2,
add column c3 varchar(10);
insert into test values (7,8,9);
delete from test where pk = 1;
update test set c1 = 100 where pk = 4;
SQL

    dolt diff
    run dolt diff

    EXPECTED=$(cat <<'EOF'
 CREATE TABLE `test` (
   `pk` int NOT NULL,
   `c1` int,
-  `c2` int,
+  `c3` varchar(10),
   PRIMARY KEY (`pk`)
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;
+---+----+-----+------+------+
|   | pk | c1  | c2   | c3   |
+---+----+-----+------+------+
| - | 1  | 2   | 3    | NULL |
| < | 4  | 5   | 6    | NULL |
| > | 4  | 100 | NULL | NULL |
| + | 7  | 8   | NULL | 9    |
+---+----+-----+------+------+
EOF
)

    [ "$status" -eq 0 ]
    [[ "$output" =~ "$EXPECTED" ]] || false

    run dolt diff --data --schema
    [ "$status" -eq 0 ]
    [[ "$output" =~ "$EXPECTED" ]] || false
    
    run dolt diff --schema

    EXPECTED=$(cat <<'EOF'
 CREATE TABLE `test` (
   `pk` int NOT NULL,
   `c1` int,
-  `c2` int,
+  `c3` varchar(10),
   PRIMARY KEY (`pk`)
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;
EOF
)

    [[ "$output" =~ "$EXPECTED" ]] || false
    # Count the line numbers to make sure there are no data changes output
    [ "${#lines[@]}" -eq 10 ]
    
    run dolt diff --data
    EXPECTED=$(cat <<'EOF'
+---+----+-----+------+------+
|   | pk | c1  | c2   | c3   |
+---+----+-----+------+------+
| - | 1  | 2   | 3    | NULL |
| < | 4  | 5   | 6    | NULL |
| > | 4  | 100 | NULL | NULL |
| + | 7  | 8   | NULL | 9    |
+---+----+-----+------+------+
EOF
)

    [[ "$output" =~ "$EXPECTED" ]] || false
    # Count the line numbers to make sure there are no schema changes output
    [ "${#lines[@]}" -eq 11 ]
}

@test "diff: data diff only" {
    dolt add .
    dolt commit -am "First commit"

    dolt sql -q "insert into test (pk) values (10);"

    dolt diff
    run dolt diff
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "CREATE TABLE" ]] || false
    [[ "$output" =~ "|   | pk | c1   | c2   | c3   | c4   | c5   |" ]] || false
    [[ "$output" =~ "| + | 10 | NULL | NULL | NULL | NULL | NULL |" ]] || false
}

@test "diff: schema changes only" {
    dolt add .
    dolt commit -am "First commit"

    dolt sql <<SQL
alter table test 
drop column c3, 
add column c6 varchar(10) after c2, 
modify column c4 tinyint comment 'new comment'
SQL

    dolt diff
    run dolt diff
    [ "$status" -eq 0 ]
    
    EXPECTED=$(cat <<'EOF'
 CREATE TABLE `test` (
   `pk` bigint NOT NULL COMMENT 'tag:0',
   `c1` bigint COMMENT 'tag:1',
   `c2` bigint COMMENT 'tag:2',
-  `c3` bigint COMMENT 'tag:3',
-  `c4` bigint COMMENT 'tag:4',
+  `c6` varchar(10),
+  `c4` tinyint COMMENT 'new comment',
   `c5` bigint COMMENT 'tag:5',
   PRIMARY KEY (`pk`)
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;
EOF
)

    [[ "$output" =~ "$EXPECTED" ]] || false

    # We want to make sure there is no trailing table output, so count the lines of output
    # 3 lines of metadata plus 11 of schema diff
    [ "${#lines[@]}" -eq 14 ]
}

@test "diff: with table args" {
    dolt sql -q 'create table other (pk int not null primary key)'
    dolt add .
    dolt commit -m tables
    dolt sql -q 'insert into test values (0,0,0,0,0,0)'
    dolt sql -q 'insert into other values (9)'

    dolt diff test
    run dolt diff test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+ | 0" ]] || false
    [[ ! "$output" =~ "+ | 9" ]] || false

    run dolt diff other
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+ | 9" ]] || false
    [[ ! "$output" =~ "+ | 0" ]] || false

    run dolt diff test other
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+ | 0" ]] || false
    [[ "$output" =~ "+ | 9" ]] || false

    dolt add .
    run dolt diff head test other
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+ | 0" ]] || false
    [[ "$output" =~ "+ | 9" ]] || false

    dolt commit -m rows
    run dolt diff head^ head test other
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+ | 0" ]] || false
    [[ "$output" =~ "+ | 9" ]] || false

    run dolt diff head^ head fake
    [ "$status" -ne 0 ]
    [[ "$output" =~ "table fake does not exist in either revision" ]] || false
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
    [[ "$output" =~ "+ | 9" ]] || false
    [[ "$output" =~ "+ | 0" ]] || false
    run dolt diff dolomite test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+ | 0" ]] || false
    [[ ! "$output" =~ "+ | 9" ]] || false
    run dolt diff dolomite head dolomite
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+ | 9" ]] || false
    [[ ! "$output" =~ "+ | 0" ]] || false
    run dolt diff head^ head dolomite
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+ | 9" ]] || false
    [[ ! "$output" =~ "+ | 0" ]] || false
    dolt branch -D dolomite
    dolt sql -q 'insert into dolomite values (8)'
    run dolt diff dolomite
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+ | 8" ]] || false
    [[ ! "$output" =~ "+ | 0" ]] || false
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

    dolt diff --summary
    run dolt diff --summary
    [ "$status" -eq 0 ]
    [[ "$output" =~ "2 Rows Unmodified (100.00%)" ]] || false
    [[ "$output" =~ "2 Rows Added (100.00%)" ]] || false
    [[ "$output" =~ "0 Rows Deleted (0.00%)" ]] || false
    [[ "$output" =~ "0 Rows Modified (0.00%)" ]] || false
    [[ "$output" =~ "12 Cells Added (100.00%)" ]] || false
    [[ "$output" =~ "0 Cells Deleted (0.00%)" ]] || false
    [[ "$output" =~ "0 Cells Modified (0.00%)" ]] || false
    [[ "$output" =~ "(2 Row Entries vs 4 Row Entries)" ]] || false

    dolt add test
    dolt commit -m "added two rows"
    dolt sql -q "replace into test values (0, 11, 0, 0, 0, 6)"
    run dolt diff --summary
    [ "$status" -eq 0 ]
    [[ "$output" =~ "3 Rows Unmodified (75.00%)" ]] || false
    [[ "$output" =~ "0 Rows Added (0.00%)" ]] || false
    [[ "$output" =~ "0 Rows Deleted (0.00%)" ]] || false
    [[ "$output" =~ "1 Row Modified (25.00%)" ]] || false
    [[ "$output" =~ "0 Cells Added (0.00%)" ]] || false
    [[ "$output" =~ "0 Cells Deleted (0.00%)" ]] || false
    [[ "$output" =~ "2 Cells Modified (8.33%)" ]] || false
    [[ "$output" =~ "(4 Row Entries vs 4 Row Entries)" ]] || false

    dolt add test
    dolt commit -m "modified first row"
    dolt sql -q "delete from test where pk = 0"
    run dolt diff --summary
    [ "$status" -eq 0 ]
    [[ "$output" =~ "3 Rows Unmodified (75.00%)" ]] || false
    [[ "$output" =~ "0 Rows Added (0.00%)" ]] || false
    [[ "$output" =~ "1 Row Deleted (25.00%)" ]] || false
    [[ "$output" =~ "0 Rows Modified (0.00%)" ]] || false
    [[ "$output" =~ "0 Cells Added (0.00%)" ]] || false
    [[ "$output" =~ "6 Cells Deleted (25.00%)" ]] || false
    [[ "$output" =~ "0 Cells Modified (0.00%)" ]] || false
    [[ "$output" =~ "(4 Row Entries vs 3 Row Entries)" ]] || false
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
    [[ "$output" =~ "0 Cells Added (0.00%)" ]] || false
    [[ "$output" =~ "0 Cells Deleted (0.00%)" ]] || false
    [[ "$output" =~ "1 Cell Modified (16.67%)" ]] || false
    [[ "$output" =~ "(1 Row Entry vs 1 Row Entry)" ]] || false
    dolt add test
    dolt commit -m "row modified"
    dolt sql -q "replace into test values (0, 1, 2, 3, 4, 5)"
    run dolt diff --summary
    [ "$status" -eq 0 ]
    [[ "$output" =~ "0 Rows Unmodified (0.00%)" ]] || false
    [[ "$output" =~ "0 Rows Added (0.00%)" ]] || false
    [[ "$output" =~ "0 Rows Deleted (0.00%)" ]] || false
    [[ "$output" =~ "1 Row Modified (100.00%)" ]] || false
    [[ "$output" =~ "0 Cells Added (0.00%)" ]] || false
    [[ "$output" =~ "0 Cells Deleted (0.00%)" ]] || false
    [[ "$output" =~ "1 Cell Modified (16.67%)" ]] || false
    [[ "$output" =~ "(1 Row Entry vs 1 Row Entry)" ]] || false
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
    [[ "$output" =~ "6 Cells Added (100.00%)" ]] || false
    [[ "$output" =~ "0 Cells Deleted (0.00%)" ]] || false
    [[ "$output" =~ "0 Cells Modified (0.00%)" ]] || false
    [[ "$output" =~ "(1 Row Entry vs 2 Row Entries)" ]] || false
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

    dolt diff --summary
    run dolt diff --summary
    [ "$status" -eq 0 ]
    [[ "$output" =~ "3 Rows Unmodified (100.00%)" ]] || false
    [[ "$output" =~ "1 Row Added (33.33%)" ]] || false
    [[ "$output" =~ "0 Rows Deleted (0.00%)" ]] || false
    [[ "$output" =~ "0 Rows Modified (0.00%)" ]] || false
    [[ "$output" =~ "10 Cells Added (55.56%)" ]] || false
    [[ "$output" =~ "0 Cells Deleted (0.00%)" ]] || false
    [[ "$output" =~ "0 Cells Modified (0.00%)" ]] || false
    [[ "$output" =~ "(3 Row Entries vs 4 Row Entries)" ]] || false

    dolt sql -q "replace into employees values (0, 'tim', 'sehn', 'ceo', '2 years ago', '', 'Santa Monica')"
    
    dolt diff --summary
    run dolt diff --summary
    [ "$status" -eq 0 ]
    [[ "$output" =~ "2 Rows Unmodified (66.67%)" ]] || false
    [[ "$output" =~ "1 Row Added (33.33%)" ]] || false
    [[ "$output" =~ "0 Rows Deleted (0.00%)" ]] || false
    [[ "$output" =~ "1 Row Modified (33.33%)" ]] || false
    [[ "$output" =~ "10 Cells Added (55.56%)" ]] || false
    [[ "$output" =~ "0 Cells Deleted (0.00%)" ]] || false
    [[ "$output" =~ "2 Cells Modified (11.11%)" ]] || false
    [[ "$output" =~ "(3 Row Entries vs 4 Row Entries)" ]] || false
}

@test "diff: summary gets summaries for all tables with changes" {
    dolt sql -q "insert into test values (0, 0, 0, 0, 0, 0)"
    dolt sql -q "insert into test values (1, 1, 1, 1, 1, 1)"
    dolt sql <<SQL
CREATE TABLE employees (
  \`id\` varchar(20) NOT NULL,
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
    dolt sql -q "insert into test values (2, 222, 0, 0, 0, 0)"
    dolt sql -q "insert into test values (3, 333, 0, 0, 0, 0)"

    run dolt diff --where "to_pk=2"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "222" ]] || false
    [[ ! "$output" =~ "333" ]] || false

    run dolt diff --where "to_pk < 3"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "222" ]] || false
    ! [[ "$output" =~ "333" ]] || false
    
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

    run dolt diff test1 test2 --where "from_pk=4"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "44" ]] || false
    ! [[ "$output" =~ "55" ]] || false

    run dolt diff test1 test2 --where "from_pk=4 OR to_pk=5"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "44" ]] || false
    [[ "$output" =~ "55" ]] || false
    
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

    run dolt diff test1 test2 --where "pk=4"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Error running diff query" ]] || false
    [[ "$output" =~ "where pk=4" ]] || false
}

@test "diff: diff summary incorrect primary key set change regression test" {
    dolt sql -q "create table testdrop (col1 varchar(20), id int primary key, col2 varchar(20))"
    dolt add .
    dolt sql -q "insert into testdrop values ('test1', 1, 'test2')"
    dolt commit -am "Add testdrop table"

    dolt sql -q "alter table testdrop drop column col1"
    run dolt diff --summary
    [ $status -eq 0 ]
    [[ $output =~ "1 Row Modified (100.00%)" ]]
}

@test "diff: with where clause errors" {
    dolt sql -q "insert into test values (0, 0, 0, 0, 0, 0)"
    dolt sql -q "insert into test values (1, 1, 1, 1, 1, 1)"
    dolt add test
    dolt commit -m "table created"
    dolt sql -q "insert into test values (2, 22, 0, 0, 0, 0)"
    dolt sql -q "insert into test values (3, 33, 0, 0, 0, 0)"

    run dolt diff --where "some nonsense"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Failed to parse diff query. Invalid where clause?" ]] || false
    [[ "$output" =~ "where some nonsense" ]] || false

    run dolt diff --where "poop = 0"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Error running diff query" ]] || false
    [[ "$output" =~ "where poop = 0" ]] || false

    dolt add test
    dolt commit -m "added two rows"

    run dolt diff --where "poop = 0"
    skip "Empty diffs don't validate the where clause"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Error running diff query" ]] || false
    [[ "$output" =~ "where poop = 0" ]] || false
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
    dolt add .
    dolt sql -q "INSERT INTO t VALUES (1, 1, 1)"

    dolt commit -am "cm1"

    dolt sql -q "UPDATE t SET val1=2 where pk=1"
    run dolt diff -r sql
    [ $status -eq 0 ]
    [[ "$output" = 'UPDATE `t` SET `val1`=2 WHERE `pk`=1;' ]] || false

    dolt commit -am "cm2"

    dolt sql -q "UPDATE t SET val1=3, val2=4 where pk = 1"
    dolt diff -r sql
    run dolt diff -r sql
    [ $status -eq 0 ]
    [[ "$output" = 'UPDATE `t` SET `val1`=3,`val2`=4 WHERE `pk`=1;' ]] || false

    dolt commit -am "cm3"

    dolt sql -q "UPDATE t SET val1=3 where pk=1;"
    run dolt diff -r sql
    [ $status -eq 0 ]
    [[ "$output" = '' ]] || false

    dolt sql -q "alter table t add val3 int"
    dolt commit -am "cm4"

    dolt sql -q "update t set val1=30,val3=4 where pk=1"
    run dolt diff -r sql
    [ $status -eq 0 ]
    [[ "$output" = 'UPDATE `t` SET `val1`=30,`val3`=4 WHERE `pk`=1;' ]] || false
}

@test "diff: skinny flag only shows row changed without schema changes" {
    dolt sql -q "CREATE TABLE t(pk int primary key, val1 int, val2 int)"
    dolt add .
    dolt sql -q "INSERT INTO t VALUES (1, 1, 1)"
    dolt commit -am "cm1"

    run dolt diff --skinny --data HEAD~1
    [ $status -eq 0 ]
    [[ "$output" =~ 'pk' ]] || false
    [[ "$output" =~ 'val1' ]] || false
    [[ "$output" =~ 'val2' ]] || false

    dolt sql -q "UPDATE t SET val1=2 WHERE pk=1"
    dolt commit -am "cm2"

    dolt sql -q "UPDATE t SET val1=3 WHERE pk=1"
    dolt commit -am "cm3"

    run dolt diff --skinny HEAD~1
    [ $status -eq 0 ]
    [[ ! "$output" =~ 'val2' ]] || false
    [[ "$output" =~ 'pk' ]] || false
    [[ "$output" =~ 'val1' ]] || false
}

@test "diff: skinny flag only shows row changed when both schema (column added) and data is changed (row updated)" {
    dolt sql -q "create table t(pk int primary key, val1 int, val2 int)"
    dolt add .
    dolt sql -q "INSERT INTO t VALUES (1, 1, 1)"
    dolt sql -q "INSERT INTO t VALUES (2, 2, 2)"
    dolt commit -am "cm1"

    run dolt diff --skinny --data HEAD~1
    [ $status -eq 0 ]
    [[ "$output" =~ 'pk' ]] || false
    [[ "$output" =~ 'val1' ]] || false
    [[ "$output" =~ 'val2' ]] || false

    dolt sql -q "UPDATE t SET val1=3 WHERE pk=1"
    dolt sql -q "ALTER TABLE t ADD val3 int "
    dolt sql -q "UPDATE t SET val3=4 WHERE pk=1"
    dolt commit -am "cm2"

    run dolt diff --skinny --data HEAD~1
    [ $status -eq 0 ]
    [[ "$output" =~ 'pk' ]] || false
    [[ "$output" =~ 'val1' ]] || false
    [[ "$output" =~ 'val3' ]] || false
    [[ ! "$output" =~ 'val2' ]] || false
}

@test "diff: skinny flag only shows row changed when both schema (column dropped) and data is changed (row updated)" {
    dolt sql -q "create table t(pk int primary key, val1 int, s varchar(255))"
    dolt add .
    dolt sql -q "INSERT INTO t VALUES (1, 1, 'bla')"
    dolt sql -q "INSERT INTO t VALUES (2, 2, 'bla2')"
    dolt commit -am "cm1"

    run dolt diff --skinny --data HEAD~1
    [ $status -eq 0 ]
    [[ "$output" =~ 'pk' ]] || false
    [[ "$output" =~ 'val1' ]] || false
    [[ "$output" =~ 's' ]] || false

    dolt sql -q "ALTER TABLE t DROP COLUMN s"
    dolt sql -q "UPDATE t SET val1=3 WHERE pk=1"
    dolt sql -q "UPDATE t SET val1=4 WHERE pk=2"
    dolt commit -am "cm2"

    run dolt diff --skinny --data HEAD~1
    [ $status -eq 0 ]
    [[ "$output" =~ 'pk' ]] || false
    [[ "$output" =~ 'val1' ]] || false
    [[ "$output" =~ 's' ]] || false
}

@test "diff: skinny flag only shows row changed when data is changed (row deleted)" {
    dolt sql -q "create table t(pk int primary key, val1 int, val2 int)"
    dolt add .
    dolt sql -q "INSERT INTO t VALUES (1, 1, 1)"
    dolt sql -q "INSERT INTO t VALUES (2, 2, 2)"
    dolt commit -am "cm1"

    run dolt diff --skinny --data HEAD~1
    [ $status -eq 0 ]
    [[ "$output" =~ 'pk' ]] || false
    [[ "$output" =~ 'val1' ]] || false
    [[ "$output" =~ 'val2' ]] || false

    dolt sql -q "DELETE FROM t WHERE pk=1"
    dolt commit -am "cm2"

    run dolt diff --skinny --data HEAD~1
    [ $status -eq 0 ]
    [[ "$output" =~ 'pk' ]] || false
    [[ "$output" =~ 'val1' ]] || false
    [[ "$output" =~ 'val2' ]] || false
}

@test "diff: keyless sql diffs" {
    
    dolt sql -q "create table t(pk int, val int)"
    dolt add .
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
    dolt diff -r sql
    run dolt diff -r sql
    [ $status -eq 0 ]
    [[ "$output" =~ 'DELETE FROM `t` WHERE `pk`=1 AND `val`=1;' ]]
    [[ "$output" =~ 'DELETE FROM `t` WHERE `pk`=1 AND `val`=1;' ]]
    [[ "$output" =~ 'INSERT INTO `t` (`pk`,`val`) VALUES (1,2);' ]]
    [[ "$output" =~ 'INSERT INTO `t` (`pk`,`val`) VALUES (1,2);' ]]
    [ "${#lines[@]}" = "4" ]

    dolt commit -am "cm4"

    dolt sql -q "DELETE FROM t WHERE val < 3"
    run dolt diff -r sql
    [ $status -eq 0 ]
    [ "${lines[0]}" = 'DELETE FROM `t` WHERE `pk`=1 AND `val`=2;' ]
    [ "${lines[1]}" = 'DELETE FROM `t` WHERE `pk`=1 AND `val`=2;' ]

    dolt commit -am "cm5"

    dolt sql -q "alter table t add primary key (pk)"
    dolt diff -r sql
    run dolt diff -r sql
    [ $status -eq 0 ]
    [ "${lines[0]}" = 'ALTER TABLE `t` DROP PRIMARY KEY;' ]
    [ "${lines[1]}" = 'ALTER TABLE `t` ADD PRIMARY KEY (pk);' ]
    [ "${lines[2]}" = 'Primary key sets differ between revisions for table t, skipping data diff' ]

    dolt commit -am "cm6"

    dolt sql -q "alter table t add column pk2 int"
    dolt sql -q "alter table t drop primary key"
    dolt sql -q "alter table t add primary key (pk, val)"
    run dolt diff -r sql
    [ $status -eq 0 ]
    [ "${lines[0]}" = 'ALTER TABLE `t` ADD `pk2` int;' ]
    [ "${lines[1]}" = 'ALTER TABLE `t` DROP PRIMARY KEY;' ]
    [ "${lines[2]}" = 'ALTER TABLE `t` ADD PRIMARY KEY (pk,val);' ]
    [ "${lines[3]}" = 'Primary key sets differ between revisions for table t, skipping data diff' ]
}

@test "diff: adding and removing primary key" {
    dolt sql <<SQL
create table t(pk int, val int);
insert into t values (1,1);
SQL
    dolt add .
    dolt commit -am "creating table"

    dolt sql -q "alter table t add primary key (pk)"
    
    run dolt diff -r sql
    [ $status -eq 0 ]
    [ "${lines[0]}" = 'ALTER TABLE `t` DROP PRIMARY KEY;' ]
    [ "${lines[1]}" = 'ALTER TABLE `t` ADD PRIMARY KEY (pk);' ]
    [ "${lines[2]}" = 'Primary key sets differ between revisions for table t, skipping data diff' ]

    dolt diff
    run dolt diff
    [ $status -eq 0 ]
    [[ "$output" =~ '+  PRIMARY KEY (`pk`)' ]] || false
    [[ "$output" =~ 'Primary key sets differ between revisions for table t, skipping data diff' ]] || false
    

    dolt commit -am 'added primary key'

    dolt sql -q "alter table t drop primary key"

    dolt diff -r sql
    run dolt diff -r sql
    [ $status -eq 0 ]
    [ "${lines[0]}" = 'ALTER TABLE `t` DROP PRIMARY KEY;' ]
    [[ "$output" =~ 'Primary key sets differ between revisions for table t, skipping data diff' ]] || false

    dolt diff
    run dolt diff
    [ $status -eq 0 ]
    [[ "$output" =~ '-  PRIMARY KEY (`pk`)' ]] || false
    [[ "$output" =~ 'Primary key sets differ between revisions for table t, skipping data diff' ]] || false
}

@test "diff: created and dropped tables include schema and data changes in results" {
  dolt sql -q "create table a(pk int primary key)"
  dolt add .
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
  [[ $output =~ "+ | 1 " ]] || false

  dolt diff HEAD~1 HEAD
  run dolt diff HEAD~1 HEAD
  [[ $output =~ 'deleted table' ]] || false
  [[ $output =~ '-CREATE TABLE `a` (' ]] || false
  [[ $output =~ "- | 1 " ]] || false
}

@test "diff: large diff does not drop rows" {
    dolt sql -q "create table t(pk int primary key, val int)"
    dolt add .

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

@test "diff: works with spaces in column names" {
   dolt sql -q 'CREATE table t (pk int, `type of food` varchar(100));'
   dolt sql -q "INSERT INTO t VALUES (1, 'ramen');"
   run dolt diff
   [ $status -eq 0 ]
   [[ $output =~ '| + | 1  | ramen        |' ]] || false
}

@test "diff: with limit" {
    dolt sql <<SQL
CREATE TABLE test2 (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  PRIMARY KEY (pk)
);
SQL

    dolt add .
    dolt commit -m table

    dolt sql -q "insert into test values (0, 0, 0, 0, 0, 0)"
    dolt sql -q "insert into test values (1, 1, 1, 1, 1, 1)"
    dolt sql -q "insert into test2 values (0)"
    dolt sql -q "insert into test2 values (1)"
    dolt sql -q "insert into test2 values (2)"

    run dolt diff

    EXPECTED_TABLE1=$(cat <<'EOF'
+---+----+----+----+----+----+----+
|   | pk | c1 | c2 | c3 | c4 | c5 |
+---+----+----+----+----+----+----+
| + | 0  | 0  | 0  | 0  | 0  | 0  |
| + | 1  | 1  | 1  | 1  | 1  | 1  |
+---+----+----+----+----+----+----+
EOF
)
    EXPECTED_TABLE2=$(cat <<'EOF'
+---+----+
|   | pk |
+---+----+
| + | 0  |
| + | 1  |
| + | 2  |
+---+----+
EOF
)

    [[ "$output" =~ "$EXPECTED_TABLE1" ]] || false
    [[ "$output" =~ "$EXPECTED_TABLE2" ]] || false

    run dolt diff --limit 3
    [[ "$output" =~ "$EXPECTED_TABLE1" ]] || false
    [[ "$output" =~ "$EXPECTED_TABLE2" ]] || false

    run dolt diff --limit 100
    [[ "$output" =~ "$EXPECTED_TABLE1" ]] || false
    [[ "$output" =~ "$EXPECTED_TABLE2" ]] || false

    run dolt diff --limit 1

    EXPECTED_TABLE1=$(cat <<'EOF'
+---+----+----+----+----+----+----+
|   | pk | c1 | c2 | c3 | c4 | c5 |
+---+----+----+----+----+----+----+
| + | 0  | 0  | 0  | 0  | 0  | 0  |
+---+----+----+----+----+----+----+
EOF
)
    EXPECTED_TABLE2=$(cat <<'EOF'
+---+----+
|   | pk |
+---+----+
| + | 0  |
+---+----+
EOF
)

    [[ "$output" =~ "$EXPECTED_TABLE1" ]] || false
    [[ "$output" =~ "$EXPECTED_TABLE2" ]] || false

    run dolt diff --limit 2

    EXPECTED_TABLE1=$(cat <<'EOF'
+---+----+----+----+----+----+----+
|   | pk | c1 | c2 | c3 | c4 | c5 |
+---+----+----+----+----+----+----+
| + | 0  | 0  | 0  | 0  | 0  | 0  |
| + | 1  | 1  | 1  | 1  | 1  | 1  |
+---+----+----+----+----+----+----+
EOF
)
    EXPECTED_TABLE2=$(cat <<'EOF'
+---+----+
|   | pk |
+---+----+
| + | 0  |
| + | 1  |
+---+----+
EOF
)

    [[ "$output" =~ "$EXPECTED_TABLE1" ]] || false
    [[ "$output" =~ "$EXPECTED_TABLE2" ]] || false

    run dolt diff test2 --where "to_pk > 0" --limit 1

    EXPECTED_TABLE2=$(cat <<'EOF'
+---+----+
|   | pk |
+---+----+
| + | 1  |
+---+----+
EOF
)

    [[ "$output" =~ "$EXPECTED_TABLE2" ]] || false

    run dolt diff --limit 0

    [[ "$output" =~ "diff --dolt a/test b/test" ]] || false
    [[ "$output" =~ "--- a/test @" ]] || false
    [[ "$output" =~ "+++ b/test @" ]] || false
    [[ "$output" =~ "diff --dolt a/test2 b/test2" ]] || false
    [[ "$output" =~ "--- a/test2 @" ]] || false
    [[ "$output" =~ "+++ b/test2 @" ]] || false
    
    run dolt diff --limit
    [ "$status" -ne 0 ]
}

@test "diff: allowed across primary key renames" {
    dolt sql <<SQL
CREATE TABLE t1 (pk int PRIMARY KEY, col1 int);
INSERT INTO t1 VALUES (1, 1);
CREATE TABLE t2 (pk1a int, pk1b int, col1 int, PRIMARY KEY (pk1a, pk1b));
INSERT INTO t2 VALUES (1, 1, 1);
SELECT DOLT_ADD('.');
SQL
    dolt commit -am "initial"

    dolt sql <<SQL
ALTER TABLE t1 RENAME COLUMN pk to pk2;
UPDATE t1 set col1 = 100;
ALTER TABLE t2 RENAME COLUMN pk1a to pk2a;
ALTER TABLE t2 RENAME COLUMN pk1b to pk2b;
UPDATE t2 set col1 = 100;
SELECT DOLT_ADD('.');
SQL
    dolt commit -am 'rename primary key'

    run dolt diff HEAD~1 HEAD
    [ $status -eq 0 ]

    EXPECTED_TABLE=$(cat <<'EOF'
+---+------+------+------+
|   | pk   | col1 | pk2  |
+---+------+------+------+
| < | 1    | 1    | NULL |
| > | NULL | 100  | 1    |
+---+------+------+------+
EOF
)
    [[ "$output" =~ "$EXPECTED_TABLE" ]]

    EXPECTED_TABLE=$(cat <<'EOF'
+---+------+------+------+------+------+
|   | pk1a | pk1b | col1 | pk2a | pk2b |
+---+------+------+------+------+------+
| < | 1    | 1    | 1    | NULL | NULL |
| > | NULL | NULL | 100  | 1    | 1    |
+---+------+------+------+------+------+
EOF
)
    [[ "$output" =~ "$EXPECTED_TABLE" ]]
}

# This test was added to prevent short tuples from causing an empty diff.
@test "diff: add a column, then set and unset its value. Should not show a diff" {
    dolt sql -q "CREATE table t (pk int primary key);"
    dolt sql -q "Insert into t values (1), (2), (3);"
    dolt sql -q "alter table t add column col1 int;"
    dolt add .
    dolt commit -am "setup"

    # Turn a short tuple into a nominal one
    dolt sql -q "UPDATE t set col1 = 1 where pk = 1;"
    dolt sql -q "UPDATE t set col1 = null where pk = 1;"

    run dolt diff
    [ $status -eq 0 ]
    [[ ! "$output" =~ "| 1" ]] || false

    run dolt diff --summary
    [ $status -eq 0 ]
    [[ ! "$output" =~ "1 Row Modified" ]] || false
}
