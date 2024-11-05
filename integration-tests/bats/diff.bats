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

@test "diff: db collation diff" {
    dolt sql -q "create database colldb"
    cd colldb

    dolt sql -q "alter database colldb collate utf8mb4_spanish_ci"

    run dolt diff
    [ "$status" -eq 0 ]
    [[ "$output" =~ 'CREATE DATABASE `colldb`' ]] || false
    [[ "$output" =~ "40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_spanish_ci" ]] || false

    run dolt diff --data
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ 'CREATE DATABASE `colldb`' ]] || false
    [[ ! "$output" =~ "40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_spanish_ci" ]] || false

    run dolt diff --schema
    [ "$status" -eq 0 ]
    [[ "$output" =~ 'CREATE DATABASE `colldb`' ]] || false
    [[ "$output" =~ "40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_spanish_ci" ]] || false

    run dolt diff --summary
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 0 ]

    run dolt diff -r json
    EXPECTED=$(cat <<'EOF'
{"tables":[{"name":"__DATABASE__colldb","schema_diff":["ALTER DATABASE `colldb` COLLATE='utf8mb4_spanish_ci';"],"data_diff":[]}]}
EOF
)
    [ "$status" -eq 0 ]
    [[ "$output" =~ "$EXPECTED" ]] || false

    run dolt diff -r sql
    [ "$status" -eq 0 ]
    [[ "$output" =~ "ALTER DATABASE \`colldb\` COLLATE='utf8mb4_spanish_ci';" ]] || false
}

@test "diff: db collation diff regression test" {
    dolt sql -q "create database COLLDB"
    cd COLLDB

    dolt sql -q "alter database COLLDB collate utf8mb4_spanish_ci"
    # regression test for dolt diff failing when database name starts or ends with any of the characters in __DATABASE__

    run dolt diff -r sql
    [ "$status" -eq 0 ]
    [[ "$output" =~ "ALTER DATABASE \`COLLDB\` COLLATE='utf8mb4_spanish_ci';" ]] || false
}

@test "diff: row, line, in-place, context diff modes" {
    # We're not using the test table, so we might as well delete it
    dolt sql <<SQL
DROP TABLE test;
CREATE TABLE tbl (PK BIGINT PRIMARY KEY);
INSERT INTO tbl VALUES (1), (2), (3);
DELIMITER //
CREATE PROCEDURE modify1() BEGIN
DECLARE a INT DEFAULT 1;
SELECT a
  AS RESULT;
END//
CREATE PROCEDURE modify2() SELECT 42;//
CREATE PROCEDURE remove() BEGIN
SELECT 8;
END//
SQL
    dolt add -A
    dolt commit -m "First commit"
    dolt branch original

    dolt sql <<SQL
DELETE FROM tbl WHERE pk = 2;
INSERT INTO tbl VALUES (4);
DROP PROCEDURE modify1;
DROP PROCEDURE modify2;
DROP PROCEDURE remove;
DELIMITER //
CREATE PROCEDURE modify1() BEGIN
SELECT 2
  AS RESULTING
  FROM DUAL;
END//
CREATE PROCEDURE modify2() SELECT 43;//
CREATE PROCEDURE adding() BEGIN
SELECT 9;
END//
SQL
    dolt add -A
    dolt commit -m "Second commit"

    # Look at the row diff
    run dolt diff original --diff-mode=row
    [ "$status" -eq 0 ]
    # Verify that standard diffs are still working
    [[ "$output" =~ "|   | PK |" ]] || false
    [[ "$output" =~ "+---+----+" ]] || false
    [[ "$output" =~ "| - | 2  |" ]] || false
    [[ "$output" =~ "| + | 4  |" ]] || false
    # Check the overall stored procedure diff (excluding dates since they're variable)
    [[ "$output" =~ "+---+---------+--------------------------------------+" ]] || false
    [[ "$output" =~ "|   | name    | create_stmt                          |" ]] || false
    [[ "$output" =~ "+---+---------+--------------------------------------+" ]] || false
    [[ "$output" =~ "| + | adding  | CREATE PROCEDURE adding() BEGIN      |" ]] || false
    [[ "$output" =~ "|   |         | SELECT 9;                            |" ]] || false
    [[ "$output" =~ "|   |         | END                                  |" ]] || false
    [[ "$output" =~ "| < | modify1 | CREATE PROCEDURE modify1() BEGIN     |" ]] || false
    [[ "$output" =~ "|   |         | DECLARE a INT DEFAULT 1;             |" ]] || false
    [[ "$output" =~ "|   |         | SELECT a                             |" ]] || false
    [[ "$output" =~ "|   |         |   AS RESULT;                         |" ]] || false
    [[ "$output" =~ "|   |         | END                                  |" ]] || false
    [[ "$output" =~ "| > | modify1 | CREATE PROCEDURE modify1() BEGIN     |" ]] || false
    [[ "$output" =~ "|   |         | SELECT 2                             |" ]] || false
    [[ "$output" =~ "|   |         |   AS RESULTING                       |" ]] || false
    [[ "$output" =~ "|   |         |   FROM DUAL;                         |" ]] || false
    [[ "$output" =~ "|   |         | END                                  |" ]] || false
    [[ "$output" =~ "| < | modify2 | CREATE PROCEDURE modify2() SELECT 42 |" ]] || false
    [[ "$output" =~ "| > | modify2 | CREATE PROCEDURE modify2() SELECT 43 |" ]] || false
    [[ "$output" =~ "| - | remove  | CREATE PROCEDURE remove() BEGIN      |" ]] || false
    [[ "$output" =~ "|   |         | SELECT 8;                            |" ]] || false
    [[ "$output" =~ "|   |         | END                                  |" ]] || false
    [[ "$output" =~ "+---+---------+--------------------------------------+" ]] || false

    # Look at the line-by-line diff
    run dolt diff original --diff-mode=line
    [ "$status" -eq 0 ]
    # Verify that standard diffs are still working
    [[ "$output" =~ "|   | PK |" ]] || false
    [[ "$output" =~ "+---+----+" ]] || false
    [[ "$output" =~ "| - | 2  |" ]] || false
    [[ "$output" =~ "| + | 4  |" ]] || false
    # Check the overall stored procedure diff (excluding dates since they're variable)
    [[ "$output" =~ "+---+---------+---------------------------------------+" ]] || false
    [[ "$output" =~ "|   | name    | create_stmt                           |" ]] || false
    [[ "$output" =~ "+---+---------+---------------------------------------+" ]] || false
    [[ "$output" =~ "| + | adding  | CREATE PROCEDURE adding() BEGIN       |" ]] || false
    [[ "$output" =~ "|   |         | SELECT 9;                             |" ]] || false
    [[ "$output" =~ "|   |         | END                                   |" ]] || false
    [[ "$output" =~ "| * | modify1 |  CREATE PROCEDURE modify1() BEGIN     |" ]] || false
    [[ "$output" =~ "|   |         | -DECLARE a INT DEFAULT 1;             |" ]] || false
    [[ "$output" =~ "|   |         | -SELECT a                             |" ]] || false
    [[ "$output" =~ "|   |         | -  AS RESULT;                         |" ]] || false
    [[ "$output" =~ "|   |         | +SELECT 2                             |" ]] || false
    [[ "$output" =~ "|   |         | +  AS RESULTING                       |" ]] || false
    [[ "$output" =~ "|   |         | +  FROM DUAL;                         |" ]] || false
    [[ "$output" =~ "|   |         |  END                                  |" ]] || false
    [[ "$output" =~ "| * | modify2 | -CREATE PROCEDURE modify2() SELECT 42 |" ]] || false
    [[ "$output" =~ "|   |         | +CREATE PROCEDURE modify2() SELECT 43 |" ]] || false
    [[ "$output" =~ "| - | remove  | CREATE PROCEDURE remove() BEGIN       |" ]] || false
    [[ "$output" =~ "|   |         | SELECT 8;                             |" ]] || false
    [[ "$output" =~ "|   |         | END                                   |" ]] || false
    [[ "$output" =~ "+---+---------+---------------------------------------+" ]] || false

   # Look at the in-place diff
    run dolt diff original --diff-mode=in-place
    [ "$status" -eq 0 ]
    # Verify that standard diffs are still working
    [[ "$output" =~ "|   | PK |" ]] || false
    [[ "$output" =~ "+---+----+" ]] || false
    [[ "$output" =~ "| - | 2  |" ]] || false
    [[ "$output" =~ "| + | 4  |" ]] || false
    # Check the overall stored procedure diff (excluding dates since they're variable)
    [[ "$output" =~ "+---+---------+---------------------------------------+" ]] || false
    [[ "$output" =~ "|   | name    | create_stmt                           |" ]] || false
    [[ "$output" =~ "+---+---------+---------------------------------------+" ]] || false
    [[ "$output" =~ "| + | adding  | CREATE PROCEDURE adding() BEGIN       |" ]] || false
    [[ "$output" =~ "|   |         | SELECT 9;                             |" ]] || false
    [[ "$output" =~ "|   |         | END                                   |" ]] || false
    [[ "$output" =~ "| * | modify1 | CREATE PROCEDURE modify1() BEGIN      |" ]] || false
    [[ "$output" =~ "|   |         | DECLARE a INT DEFAULT 1;              |" ]] || false
    [[ "$output" =~ "|   |         | SELECT a2                             |" ]] || false
    [[ "$output" =~ "|   |         |   AS RESULTING                        |" ]] || false
    [[ "$output" =~ "|   |         |   FROM DUAL;                          |" ]] || false
    [[ "$output" =~ "|   |         | END                                   |" ]] || false
    [[ "$output" =~ "| * | modify2 | CREATE PROCEDURE modify2() SELECT 423 |" ]] || false
    [[ "$output" =~ "| - | remove  | CREATE PROCEDURE remove() BEGIN       |" ]] || false
    [[ "$output" =~ "|   |         | SELECT 8;                             |" ]] || false
    [[ "$output" =~ "|   |         | END                                   |" ]] || false
    [[ "$output" =~ "+---+---------+---------------------------------------+" ]] || false

    # Look at the context diff
    run dolt diff original --diff-mode=context
    [ "$status" -eq 0 ]
    # Verify that standard diffs are still working
    [[ "$output" =~ "|   | PK |" ]] || false
    [[ "$output" =~ "+---+----+" ]] || false
    [[ "$output" =~ "| - | 2  |" ]] || false
    [[ "$output" =~ "| + | 4  |" ]] || false
    # Check the overall stored procedure diff (excluding dates since they're variable)
    [[ "$output" =~ "+---+---------+--------------------------------------+" ]] || false
    [[ "$output" =~ "|   | name    | create_stmt                          |" ]] || false
    [[ "$output" =~ "+---+---------+--------------------------------------+" ]] || false
    [[ "$output" =~ "| + | adding  | CREATE PROCEDURE adding() BEGIN      |" ]] || false
    [[ "$output" =~ "|   |         | SELECT 9;                            |" ]] || false
    [[ "$output" =~ "|   |         | END                                  |" ]] || false
    [[ "$output" =~ "| * | modify1 |  CREATE PROCEDURE modify1() BEGIN    |" ]] || false
    [[ "$output" =~ "|   |         | -DECLARE a INT DEFAULT 1;            |" ]] || false
    [[ "$output" =~ "|   |         | -SELECT a                            |" ]] || false
    [[ "$output" =~ "|   |         | -  AS RESULT;                        |" ]] || false
    [[ "$output" =~ "|   |         | +SELECT 2                            |" ]] || false
    [[ "$output" =~ "|   |         | +  AS RESULTING                      |" ]] || false
    [[ "$output" =~ "|   |         | +  FROM DUAL;                        |" ]] || false
    [[ "$output" =~ "|   |         |  END                                 |" ]] || false
    [[ "$output" =~ "| < | modify2 | CREATE PROCEDURE modify2() SELECT 42 |" ]] || false
    [[ "$output" =~ "| > | modify2 | CREATE PROCEDURE modify2() SELECT 43 |" ]] || false
    [[ "$output" =~ "| - | remove  | CREATE PROCEDURE remove() BEGIN      |" ]] || false
    [[ "$output" =~ "|   |         | SELECT 8;                            |" ]] || false
    [[ "$output" =~ "|   |         | END                                  |" ]] || false
    [[ "$output" =~ "+---+---------+--------------------------------------+" ]] || false

    # Ensure that the context diff is the default
    run dolt diff original
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| * | modify1 |  CREATE PROCEDURE modify1() BEGIN    |" ]] || false
    [[ "$output" =~ "|   |         | -SELECT a                            |" ]] || false
    [[ "$output" =~ "|   |         | +SELECT 2                            |" ]] || false
    [[ "$output" =~ "| < | modify2 | CREATE PROCEDURE modify2() SELECT 42 |" ]] || false
    [[ "$output" =~ "| > | modify2 | CREATE PROCEDURE modify2() SELECT 43 |" ]] || false
}

@test "diff: reverse diff" {
    # We're not using the test table, so we might as well delete it
    dolt sql <<SQL
DROP TABLE test;
CREATE TABLE tbl (PK BIGINT PRIMARY KEY);
INSERT INTO tbl VALUES (1), (2), (3);
DELIMITER //
CREATE PROCEDURE modify1() BEGIN
DECLARE a INT DEFAULT 1;
SELECT a
  AS RESULT;
END//
CREATE PROCEDURE modify2() SELECT 42;//
CREATE PROCEDURE remove() BEGIN
SELECT 8;
END//
SQL
    dolt add -A
    dolt commit -m "First commit"
    dolt branch original

    dolt sql <<SQL
DELETE FROM tbl WHERE pk = 2;
INSERT INTO tbl VALUES (4);
DROP PROCEDURE modify1;
DROP PROCEDURE modify2;
DROP PROCEDURE remove;
DELIMITER //
CREATE PROCEDURE modify1() BEGIN
SELECT 2
  AS RESULTING
  FROM DUAL;
END//
CREATE PROCEDURE modify2() SELECT 43;//
CREATE PROCEDURE adding() BEGIN
SELECT 9;
END//
SQL
    dolt add -A
    dolt commit -m "Second commit"

    # Look at the context diff
    run dolt diff original -R
    [ "$status" -eq 0 ]
    echo "$output"
    # Verify that standard diffs are still working
    [[ "$output" =~ "|   | PK |" ]] || false
    [[ "$output" =~ "+---+----+" ]] || false
    [[ "$output" =~ "| - | 4  |" ]] || false
    [[ "$output" =~ "| + | 2  |" ]] || false
    # Check the overall stored procedure diff (excluding dates since they're variable)
    [[ "$output" =~ "+---+---------+--------------------------------------+" ]] || false
    [[ "$output" =~ "|   | name    | create_stmt                          |" ]] || false
    [[ "$output" =~ "+---+---------+--------------------------------------+" ]] || false
    [[ "$output" =~ "| - | adding  | CREATE PROCEDURE adding() BEGIN      |" ]] || false
    [[ "$output" =~ "|   |         | SELECT 9;                            |" ]] || false
    [[ "$output" =~ "|   |         | END                                  |" ]] || false
    [[ "$output" =~ "| * | modify1 |  CREATE PROCEDURE modify1() BEGIN    |" ]] || false
    [[ "$output" =~ "|   |         | -SELECT 2                            |" ]] || false
    [[ "$output" =~ "|   |         | -  AS RESULTING                      |" ]] || false
    [[ "$output" =~ "|   |         | -  FROM DUAL;                        |" ]] || false
    [[ "$output" =~ "|   |         | +DECLARE a INT DEFAULT 1;            |" ]] || false
    [[ "$output" =~ "|   |         | +SELECT a                            |" ]] || false
    [[ "$output" =~ "|   |         | +  AS RESULT;                        |" ]] || false
    [[ "$output" =~ "|   |         |  END                                 |" ]] || false
    [[ "$output" =~ "| < | modify2 | CREATE PROCEDURE modify2() SELECT 43 |" ]] || false
    [[ "$output" =~ "| > | modify2 | CREATE PROCEDURE modify2() SELECT 42 |" ]] || false
    [[ "$output" =~ "| + | remove  | CREATE PROCEDURE remove() BEGIN      |" ]] || false
    [[ "$output" =~ "|   |         | SELECT 8;                            |" ]] || false
    [[ "$output" =~ "|   |         | END                                  |" ]] || false
    [[ "$output" =~ "+---+---------+--------------------------------------+" ]] || false
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

    # Two dot
    run dolt diff head..
    [ "$status" -eq 0 ]
    [ "$output" = "" ]

    run dolt diff head^..
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+ | 0" ]] || false

    run dolt diff head^..head
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+ | 0" ]] || false

    run dolt diff head..head^
    [ "$status" -eq 0 ]
    [[ "$output" =~ "- | 0" ]] || false

    run dolt diff ..head^
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

@test "diff: two and three dot diff" {
    # TODO: remove this once dolt checkout is migrated
    if [ "$SQL_ENGINE" = "remote-engine" ]; then
      skip "This test relies on dolt checkout, which has not been migrated yet."
    fi

    dolt checkout main
    dolt sql -q 'insert into test values (0,0,0,0,0,0)'
    dolt add .
    dolt commit -m table
    dolt checkout -b branch1
    dolt sql -q 'insert into test values (1,1,1,1,1,1)'
    dolt add .
    dolt commit -m row
    dolt checkout main
    dolt sql -q 'insert into test values (2,2,2,2,2,2)'
    dolt add .
    dolt commit -m newrow

    # Two dot shows all changes between branches
    run dolt diff branch1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "- | 1" ]] || false
    [[ "$output" =~ "+ | 2" ]] || false

    run dolt diff branch1..
    [ "$status" -eq 0 ]
    [[ "$output" =~ "- | 1" ]] || false
    [[ "$output" =~ "+ | 2" ]] || false

    run dolt diff branch1..main
    [ "$status" -eq 0 ]
    [[ "$output" =~ "- | 1" ]] || false
    [[ "$output" =~ "+ | 2" ]] || false

    run dolt diff branch1 main
    [ "$status" -eq 0 ]
    [[ "$output" =~ "- | 1" ]] || false
    [[ "$output" =~ "+ | 2" ]] || false

    run dolt diff ..branch1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+ | 1" ]] || false
    [[ "$output" =~ "- | 2" ]] || false

    run dolt diff main..branch1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+ | 1" ]] || false
    [[ "$output" =~ "- | 2" ]] || false

    run dolt diff main branch1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+ | 1" ]] || false
    [[ "$output" =~ "- | 2" ]] || false

    # Three dot shows changes between common ancestor and branch
    run dolt diff branch1...
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "- | 1" ]] || false
    [[ "$output" =~ "+ | 2" ]] || false

    run dolt diff $(dolt merge-base branch1 HEAD)
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "- | 1" ]] || false
    [[ "$output" =~ "+ | 2" ]] || false

    run dolt diff --merge-base branch1
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "- | 1" ]] || false
    [[ "$output" =~ "+ | 2" ]] || false

    run dolt diff branch1...main
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "- | 1" ]] || false
    [[ "$output" =~ "+ | 2" ]] || false

    run dolt diff --merge-base branch1 main
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "- | 1" ]] || false
    [[ "$output" =~ "+ | 2" ]] || false

    run dolt diff main...branch1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+ | 1" ]] || false
    [[ ! "$output" =~ "- | 2" ]] || false

    run dolt diff --merge-base main branch1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+ | 1" ]] || false
    [[ ! "$output" =~ "- | 2" ]] || false

    run dolt diff --merge-base main branch1 test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+ | 1" ]] || false
    [[ ! "$output" =~ "- | 2" ]] || false

    run dolt diff $(dolt merge-base branch1 main) main
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "- | 1" ]] || false
    [[ "$output" =~ "+ | 2" ]] || false

    run dolt diff $(dolt merge-base main branch1) branch1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+ | 1" ]] || false
    [[ ! "$output" =~ "- | 2" ]] || false
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

    # Two dot
    run dolt diff head^..head test other
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+ | 0" ]] || false
    [[ "$output" =~ "+ | 9" ]] || false

    run dolt diff head^..head fake
    [ "$status" -ne 0 ]
    [[ "$output" =~ "table fake does not exist in either revision" ]] || false

    run dolt diff head^.. test other
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+ | 0" ]] || false
    [[ "$output" =~ "+ | 9" ]] || false

    run dolt diff head^.. fake
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
    run dolt diff head^..head dolomite
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

@test "diff: new foreign key added and resolves" {
    dolt sql <<SQL
create table parent (i int primary key);
create table child (j int primary key, foreign key (j) references parent (i));
SQL

    run dolt diff
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+  CONSTRAINT \`child_ibfk_1\` FOREIGN KEY (\`j\`) REFERENCES \`parent\` (\`i\`)" ]] || false
}

@test "diff: new foreign key added without foreign key check, and does not resolve" {
    dolt sql <<SQL
set foreign_key_checks=0;
create table parent (i int primary key);
create table child (j int primary key, foreign key (j) references parent (i));
SQL
    dolt add -A
    dolt commit -m "init commit"

    run dolt diff
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "+  CONSTRAINT \`child_ibfk_1\` FOREIGN KEY (\`j\`) REFERENCES \`parent\` (\`i\`)" ]] || false
}

@test "diff: existing foreign key that was resolved is deleted" {
    dolt sql <<SQL
create table parent (i int primary key);
create table child (j int primary key, constraint fk foreign key (j) references parent (i));
SQL
    run dolt diff
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+  CONSTRAINT \`fk\` FOREIGN KEY (\`j\`) REFERENCES \`parent\` (\`i\`)" ]] || false

    dolt add -A
    dolt commit -m "init commit"
    dolt sql -q "alter table child drop foreign key fk"

    run dolt diff
    [ "$status" -eq 0 ]
    [[ "$output" =~ "-  CONSTRAINT \`fk\` FOREIGN KEY (\`j\`) REFERENCES \`parent\` (\`i\`)" ]] || false
}

@test "diff: existing foreign key that was not resolved is deleted" {
    dolt sql <<SQL
set foreign_key_checks=0;
create table parent (i int primary key);
create table child (j int primary key, constraint fk foreign key (j) references parent (i));
SQL
    run dolt diff
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+  CONSTRAINT \`fk\` FOREIGN KEY (\`j\`) REFERENCES \`parent\` (\`i\`)" ]] || false

    dolt add -A
    dolt commit -m "init commit"
    dolt sql -q "alter table child drop foreign key fk"

    run dolt diff
    [ "$status" -eq 0 ]
    [[ "$output" =~ "-  CONSTRAINT \`fk\` FOREIGN KEY (\`j\`) REFERENCES \`parent\` (\`i\`)" ]] || false
}

@test "diff: existing foreign key that was resolved is modified" {
    dolt sql <<SQL
create table parent (i int primary key);
create table child (j int primary key, constraint fk foreign key (j) references parent (i));
SQL
    run dolt diff
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+  CONSTRAINT \`fk\` FOREIGN KEY (\`j\`) REFERENCES \`parent\` (\`i\`)" ]] || false

    dolt add -A
    dolt commit -m "init commit"
    dolt sql -q "alter table child drop foreign key fk"
    dolt sql -q "alter table child rename column j to k"
    dolt sql -q "alter table child add constraint fk foreign key (k) references parent (i)"

    run dolt diff
    [ "$status" -eq 0 ]
    [[ "$output" =~ "-  CONSTRAINT \`fk\` FOREIGN KEY (\`j\`) REFERENCES \`parent\` (\`i\`)" ]] || false
    [[ "$output" =~ "+  CONSTRAINT \`fk\` FOREIGN KEY (\`k\`) REFERENCES \`parent\` (\`i\`)" ]] || false
}

@test "diff: existing foreign key that was not resolved is modified" {
    dolt sql <<SQL
set foreign_key_checks=0;
create table parent (i int primary key);
create table child (j int primary key, constraint fk foreign key (j) references parent (i));
SQL
    run dolt diff
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+  CONSTRAINT \`fk\` FOREIGN KEY (\`j\`) REFERENCES \`parent\` (\`i\`)" ]] || false

    dolt add -A
    dolt commit -m "init commit"
    dolt sql -q "alter table child drop foreign key fk"
    dolt sql -q "alter table child rename column j to k"
    dolt sql -q "alter table child add constraint fk foreign key (k) references parent (i)"

    run dolt diff
    [ "$status" -eq 0 ]
    [[ "$output" =~ "-  CONSTRAINT \`fk\` FOREIGN KEY (\`j\`) REFERENCES \`parent\` (\`i\`)" ]] || false
    [[ "$output" =~ "+  CONSTRAINT \`fk\` FOREIGN KEY (\`k\`) REFERENCES \`parent\` (\`i\`)" ]] || false

}

@test "diff: resolved FKs don't show up in diff results" {
    dolt sql <<SQL
SET @@foreign_key_checks=0;
CREATE TABLE dept_emp (
  emp_no int NOT NULL,
  dept_no char(4) COLLATE utf8mb4_0900_ai_ci NOT NULL,
  PRIMARY KEY (emp_no,dept_no),
  KEY dept_no (dept_no),
  CONSTRAINT dept_emp_ibfk_1 FOREIGN KEY (emp_no) REFERENCES employees (emp_no) ON DELETE CASCADE
);
CREATE TABLE employees (
  emp_no int NOT NULL,
  nickname varchar(100),
  PRIMARY KEY (emp_no)
);
insert into employees values (100, "bob");
insert into dept_emp values (100, 1);
SQL
    dolt commit -Am "Importing data, with unresolved FKs"

    # update a row to trigger FKs to be resolved
    dolt sql -q "UPDATE employees SET nickname = 'bobby' WHERE emp_no = 100;"
    dolt commit -am "Updating data, and resolving FKs"

    # check that the diff output doesn't mention FKs getting resolved or the dept_emp table
    run dolt diff HEAD~ HEAD
    [ "$status" -eq 0 ]
    ! [[ "$output" =~ "dept_emp" ]] || false
    ! [[ "$output" =~ "FOREIGN KEY" ]] || false
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
    [[ "$output" =~ '+  KEY `fk2` (`c2`),' ]] || false
    [[ "$output" =~ '+  CONSTRAINT `fk2` FOREIGN KEY (`c2`) REFERENCES `parent` (`c2`)' ]] || false

    dolt diff parent
    run dolt diff parent
    [ "$status" -eq 0 ]
    [[ "$output" =~ '-  KEY `c1` (`c1`)' ]] || false
    [[ "$output" =~ '+  KEY `c2` (`c2`)' ]] || false
}

@test "diff: with where clause" {
    # TODO: remove this once dolt checkout is migrated
    if [ "$SQL_ENGINE" = "remote-engine" ]; then
      skip "This test relies on dolt checkout, which has not been migrated yet."
    fi

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

    # Two dot
    run dolt diff test1..test2
    [ "$status" -eq 0 ]
    [[ "$output" =~ "44" ]] || false
    [[ "$output" =~ "55" ]] || false

    run dolt diff test1..test2 --where "from_pk=4 OR to_pk=5"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "44" ]] || false
    [[ "$output" =~ "55" ]] || false

    run dolt diff test1..test2 --where "pk=4"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Error running diff query" ]] || false
    [[ "$output" =~ "where pk=4" ]] || false
}

@test "diff: with where clause errors" {
    # TODO: remove this once dolt checkout is migrated
    if [ "$SQL_ENGINE" = "remote-engine" ]; then
      skip "This test relies on dolt checkout, which has not been migrated yet."
    fi

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

@test "diff: --cached/--staged" {
    run dolt diff --staged
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
    run dolt diff --staged
    [ $status -eq 0 ]
    [ "$output" = "$CORRECT_DIFF" ]

    # Make sure it ignores changes to the working set that aren't staged
    dolt sql -q "create table test2 (pk int, c1 int, primary key(pk))"
    run dolt diff --cached
    [ $status -eq 0 ]
    [ "$output" = "$CORRECT_DIFF" ]
}

@test "diff: with invalid ref does not panic" {
    # TODO: remove this once dolt checkout is migrated
    if [ "$SQL_ENGINE" = "remote-engine" ]; then
      skip "This test relies on dolt checkout, which has not been migrated yet."
    fi


    dolt add .
    dolt commit -m table
    dolt checkout -b test-branch
    dolt sql -q "insert into test values (0, 0, 0, 0, 0, 0)"
    dolt add test
    dolt commit -m "added row"
    FIRST_COMMIT=`dolt log | grep commit | cut -d " " -f 2 | tail -1`
    run dolt diff $FIRST_COMMIT test-branch
    [ $status -eq 0 ]
    [[ ! $output =~ "panic" ]] || false
    run dolt diff main@$FIRST_COMMIT test-branch
    [ $status -eq 1 ]
    [[ ! $output =~ "panic" ]] || false
    run dolt diff ref.with.period test-branch
    [ $status -eq 1 ]
    [[ ! $output =~ "panic" ]] || false

    run dolt diff $FIRST_COMMIT..test-branch
    [ $status -eq 0 ]
    [[ ! $output =~ "panic" ]] || false
    run dolt diff main@$FIRST_COMMIT..test-branch
    [ $status -eq 1 ]
    [[ ! $output =~ "panic" ]] || false
    run dolt diff ref.with.period..test-branch
    [ $status -eq 1 ]
    [[ ! $output =~ "panic" ]] || false

    run dolt diff $FIRST_COMMIT...test-branch
    [ $status -eq 0 ]
    [[ ! $output =~ "panic" ]] || false
    run dolt diff main@$FIRST_COMMIT...test-branch
    [ $status -eq 1 ]
    [[ ! $output =~ "panic" ]] || false
    run dolt diff ref.with.period...test-branch
    [ $status -eq 1 ]
    [[ ! $output =~ "panic" ]] || false

    run dolt diff --merge-base $FIRST_COMMIT test-branch
    [ $status -eq 0 ]
    [[ ! $output =~ "panic" ]] || false
    run dolt diff --merge-base main@$FIRST_COMMIT test-branch
    [ $status -eq 1 ]
    [[ ! $output =~ "panic" ]] || false
    run dolt diff --merge-base ref.with.period test-branch
    [ $status -eq 1 ]
    [[ ! $output =~ "panic" ]] || false
}

@test "diff: binary data in sql output is hex encoded" {
    dolt sql <<SQL
DROP TABLE test;
CREATE TABLE t (PK VARBINARY(100) PRIMARY KEY, c1 BINARY(3));
INSERT INTO t VALUES (0xead543, 0x1EE4), (0x0e, NULL);
SQL
    dolt commit -Am "creating table t"

    run dolt diff -r=sql HEAD~ HEAD
    [ $status -eq 0 ]
    [[ $output =~ 'INSERT INTO `t` (`PK`,`c1`) VALUES (0x0e,NULL);' ]] || false
    [[ $output =~ 'INSERT INTO `t` (`PK`,`c1`) VALUES (0xead543,0x1ee400);' ]] || false
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
    # TODO: remove this once dolt checkout is migrated
    if [ "$SQL_ENGINE" = "remote-engine" ]; then
      skip "This test relies on dolt checkout, which has not been migrated yet."
    fi

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

    run dolt diff main..another-branch
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
    [[ "$output" =~ 'UPDATE `t` SET `val1`=2 WHERE `pk`=1;' ]] || false

    dolt commit -am "cm2"

    dolt sql -q "UPDATE t SET val1=3, val2=4 where pk = 1"
    dolt diff -r sql
    run dolt diff -r sql
    [ $status -eq 0 ]
    [[ "$output" =~ 'UPDATE `t` SET `val1`=3,`val2`=4 WHERE `pk`=1;' ]] || false

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
    [[ "$output" =~ 'UPDATE `t` SET `val1`=30,`val3`=4 WHERE `pk`=1;' ]] || false
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
    [[ "$output" =~ 'DELETE FROM `t` WHERE `pk`=1 AND `val`=1;' ]] || false
    [[ "$output" =~ 'DELETE FROM `t` WHERE `pk`=1 AND `val`=1;' ]] || false
    [[ "$output" =~ 'INSERT INTO `t` (`pk`,`val`) VALUES (1,2);' ]] || false
    [[ "$output" =~ 'INSERT INTO `t` (`pk`,`val`) VALUES (1,2);' ]] || false
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
    [ "${lines[2]}" = "Primary key sets differ between revisions for table 't', skipping data diff" ]

    dolt commit -am "cm6"

    dolt sql -q "alter table t add column pk2 int"
    dolt sql -q "alter table t drop primary key"
    dolt sql -q "alter table t add primary key (pk, val)"
    run dolt diff -r sql
    [ $status -eq 0 ]
    [ "${lines[0]}" = 'ALTER TABLE `t` ADD `pk2` int;' ]
    [ "${lines[1]}" = 'ALTER TABLE `t` DROP PRIMARY KEY;' ]
    [ "${lines[2]}" = 'ALTER TABLE `t` ADD PRIMARY KEY (pk,val);' ]
    [ "${lines[3]}" = "Primary key sets differ between revisions for table 't', skipping data diff" ]
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
    [ "${lines[2]}" = "Primary key sets differ between revisions for table 't', skipping data diff" ]

    dolt diff
    run dolt diff
    [ $status -eq 0 ]
    [[ "$output" =~ '+  PRIMARY KEY (`pk`)' ]] || false
    [[ "$output" =~ "Primary key sets differ between revisions for table 't', skipping data diff" ]] || false


    dolt commit -am 'added primary key'

    dolt sql -q "alter table t drop primary key"

    dolt diff -r sql
    run dolt diff -r sql
    [ $status -eq 0 ]
    [ "${lines[0]}" = 'ALTER TABLE `t` DROP PRIMARY KEY;' ]
    [[ "$output" =~ "Primary key sets differ between revisions for table 't', skipping data diff" ]] || false

    dolt diff
    run dolt diff
    [ $status -eq 0 ]
    [[ "$output" =~ '-  PRIMARY KEY (`pk`)' ]] || false
    [[ "$output" =~ "Primary key sets differ between revisions for table 't', skipping data diff" ]] || false
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
    [[ "$output" =~ "--- a/test" ]] || false
    [[ "$output" =~ "+++ b/test" ]] || false
    [[ "$output" =~ "diff --dolt a/test2 b/test2" ]] || false
    [[ "$output" =~ "--- a/test2" ]] || false
    [[ "$output" =~ "+++ b/test2" ]] || false

    run dolt diff --limit
    [ "$status" -ne 0 ]
}

@test "diff: allowed across primary key renames" {
    dolt sql <<SQL
CREATE TABLE t1 (pk int PRIMARY KEY, col1 int);
INSERT INTO t1 VALUES (1, 1);
CREATE TABLE t2 (pk1a int, pk1b int, col1 int, PRIMARY KEY (pk1a, pk1b));
INSERT INTO t2 VALUES (1, 1, 1);
call dolt_add('.');
SQL
    dolt commit -am "initial"

    dolt sql <<SQL
ALTER TABLE t1 RENAME COLUMN pk to pk2;
UPDATE t1 set col1 = 100;
ALTER TABLE t2 RENAME COLUMN pk1a to pk2a;
ALTER TABLE t2 RENAME COLUMN pk1b to pk2b;
UPDATE t2 set col1 = 100;
call dolt_add('.');
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
    [[ "$output" =~ "$EXPECTED_TABLE" ]] || false

    EXPECTED_TABLE=$(cat <<'EOF'
+---+------+------+------+------+------+
|   | pk1a | pk1b | col1 | pk2a | pk2b |
+---+------+------+------+------+------+
| < | 1    | 1    | 1    | NULL | NULL |
| > | NULL | NULL | 100  | 1    | 1    |
+---+------+------+------+------+------+
EOF
)
    [[ "$output" =~ "$EXPECTED_TABLE" ]] || false
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

    run dolt diff --stat
    [ $status -eq 0 ]
    [[ ! "$output" =~ "1 Row Modified" ]] || false
}

@test "diff: dolt_schema table changes are special" {
    dolt sql <<SQL
CREATE TABLE people (name varchar(255), nickname varchar(255), gender varchar(255), age int);
CREATE TABLE average_age (average double);
CREATE TRIGGER avg_age AFTER INSERT ON people
    for each row
        update average_age set average = (SELECT AVG(age) FROM people);
SQL
    dolt add .
    dolt commit -m "commit 1"

    dolt sql <<SQL
CREATE VIEW adults AS SELECT name FROM people WHERE age >= 18;
DROP TRIGGER avg_age;
CREATE TRIGGER avg_age AFTER INSERT ON people
    FOR EACH ROW
        update average_age set average = (SELECT AVG(age) FROM people);
SQL
    dolt add .
    dolt commit -m "commit 2"

    run dolt diff HEAD~1 HEAD
    [ $status -eq 0 ]
    [[ "$output" =~ "CREATE TRIGGER avg_age AFTER INSERT ON people"                         ]] || false
    [[ "$output" =~ "-    for each row"                                                     ]] || false
    [[ "$output" =~ "+    FOR EACH ROW"                                                     ]] || false
    [[ "$output" =~ "      update average_age set average = (SELECT AVG(age) FROM people);" ]] || false
    [[ "$output" =~ "+CREATE VIEW adults AS SELECT name FROM people WHERE age >= 18;"       ]] || false

    dolt sql <<SQL
DROP VIEW adults;
CREATE VIEW adults AS SELECT nickname FROM people WHERE age >= 18;
DROP TRIGGER avg_age;
SQL
    dolt add .
    dolt commit -m "commit 3"

    run dolt diff HEAD~1 HEAD
    [ $status -eq 0 ]
    [[ "$output" =~ "-CREATE TRIGGER avg_age AFTER INSERT ON people"                           ]] || false
    [[ "$output" =~ "-    FOR EACH ROW"                                                        ]] || false
    [[ "$output" =~ "-        update average_age set average = (SELECT AVG(age) FROM people);" ]] || false
    [[ "$output" =~ "-CREATE VIEW adults AS SELECT name FROM people WHERE age >= 18;"          ]] || false
    [[ "$output" =~ "+CREATE VIEW adults AS SELECT nickname FROM people WHERE age >= 18;"      ]] || false
}

@test "diff: get diff on dolt_schemas table with different result output formats" {
    # TODO: remove this once dolt checkout is migrated
    if [ "$SQL_ENGINE" = "remote-engine" ]; then
      skip "This test relies on dolt checkout, which has not been migrated yet."
    fi

    dolt add .
    dolt commit -am "commit 1"
    dolt sql <<SQL
CREATE TABLE mytable(pk BIGINT PRIMARY KEY AUTO_INCREMENT, v1 BIGINT);
CREATE TRIGGER trigger1 BEFORE INSERT ON mytable FOR EACH ROW SET new.v1 = -new.v1;
CREATE EVENT event1 ON SCHEDULE EVERY '1:2' MINUTE_SECOND DISABLE DO INSERT INTO mytable (v1) VALUES (1);
SQL

    dolt add .
    dolt commit -m "commit 2"

    dolt sql <<SQL
INSERT INTO mytable VALUES (1, 1);
CREATE VIEW view1 AS SELECT v1 FROM mytable;
DROP TRIGGER trigger1;
CREATE TRIGGER trigger1 BEFORE INSERT ON mytable FOR EACH ROW SET new.v1 = -2*new.v1;
DROP EVENT event1;
CREATE EVENT event1 ON SCHEDULE EVERY '1:2' MINUTE_SECOND DISABLE DO INSERT INTO mytable (v1) VALUES (2);
SQL

    run dolt diff -r json
    [ $status -eq 0 ]

    run dolt diff
    [ $status -eq 0 ]
    [[ "$output" =~ "-CREATE DEFINER = \`root\`@\`localhost\` EVENT \`event1\` ON SCHEDULE EVERY '1:2' MINUTE_SECOND STARTS".*"ON COMPLETION NOT PRESERVE DISABLE DO INSERT INTO mytable (v1) VALUES (1);" ]] || false
    [[ "$output" =~ "+CREATE DEFINER = \`root\`@\`localhost\` EVENT \`event1\` ON SCHEDULE EVERY '1:2' MINUTE_SECOND STARTS".*"ON COMPLETION NOT PRESERVE DISABLE DO INSERT INTO mytable (v1) VALUES (2);" ]] || false
    [[ "$output" =~ "-CREATE TRIGGER trigger1 BEFORE INSERT ON mytable FOR EACH ROW SET new.v1 = -new.v1;" ]] || false
    [[ "$output" =~ "+CREATE TRIGGER trigger1 BEFORE INSERT ON mytable FOR EACH ROW SET new.v1 = -2*new.v1;" ]] || false
    [[ "$output" =~ "+CREATE VIEW view1 AS SELECT v1 FROM mytable;" ]] || false

    run dolt diff -r sql
    [ $status -eq 0 ]
    [[ "$output" =~ "INSERT INTO \`mytable\` (\`pk\`,\`v1\`) VALUES (1,-1);" ]] || false
    [[ "$output" =~ "DROP EVENT \`event1\`;" ]] || false
    [[ "$output" =~ "CREATE DEFINER = \`root\`@\`localhost\` EVENT \`event1\` ON SCHEDULE EVERY '1:2' MINUTE_SECOND STARTS".*"ON COMPLETION NOT PRESERVE DISABLE DO INSERT INTO mytable (v1) VALUES (2);" ]] || false
    [[ "$output" =~ "DROP TRIGGER \`trigger1\`;" ]] || false
    [[ "$output" =~ "CREATE TRIGGER trigger1 BEFORE INSERT ON mytable FOR EACH ROW SET new.v1 = -2*new.v1;" ]] || false
    [[ "$output" =~ "CREATE VIEW view1 AS SELECT v1 FROM mytable;" ]] || false
}

@test "diff: table-only option" {
    dolt sql <<SQL
create table t1 (i int);
create table t2 (i int);
create table t3 (i int);
SQL

    dolt add .
    dolt commit -m "commit 1"

    dolt sql <<SQL
drop table t1;
alter table t2 add column j int;
insert into t3 values (1);
create table t4 (i int);
SQL

    run dolt diff --summary
    [ $status -eq 0 ]
    [ "${lines[0]}" = "+------------+-----------+-------------+---------------+" ]
    [ "${lines[1]}" = "| Table name | Diff type | Data change | Schema change |" ]
    [ "${lines[2]}" = "+------------+-----------+-------------+---------------+" ]
    [ "${lines[3]}" = "| t1         | dropped   | false       | true          |" ]
    [ "${lines[4]}" = "| t2         | modified  | false       | true          |" ]
    [ "${lines[5]}" = "| t3         | modified  | true        | false         |" ]
    [ "${lines[6]}" = "| t4         | added     | false       | true          |" ]
    [ "${lines[7]}" = "+------------+-----------+-------------+---------------+" ]

    run dolt diff --name-only
    [ $status -eq 0 ]
    [ "${lines[0]}" = "t1" ]
    [ "${lines[1]}" = "t2" ]
    [ "${lines[2]}" = "t3" ]
    [ "${lines[3]}" = "t4" ]

    run dolt diff --name-only --schema
    [ $status -eq 1 ]
    [[ $output =~ "invalid Arguments" ]] || false

    run dolt diff --name-only --data
    [ $status -eq 1 ]
    [[ $output =~ "invalid Arguments" ]] || false

    run dolt diff --name-only --stat
    [ $status -eq 1 ]
    [[ $output =~ "invalid Arguments" ]] || false

    run dolt diff --name-only --summary
    [ $status -eq 1 ]
    [[ $output =~ "invalid Arguments" ]] || false
}

# https://github.com/dolthub/dolt/issues/8133
@test "diff: schema change int to float" {
    dolt reset --hard

    dolt sql <<SQL
create table t1 (pk int primary key, val int);

insert into t1 values (1, 1);
call dolt_commit('-Am', 'commit');

alter table t1 modify column val float;
update t1 set val = val + 0.234;

SQL

    run dolt diff
    [ $status -eq 0 ]
    [[ "$output" =~ "| < | 1  | 1     |" ]] || false
    [[ "$output" =~ "| > | 1  | 1.234 |" ]] || false

    run dolt diff --reverse
    [ $status -eq 0 ]
    [[ "$output" =~ "| < | 1  | 1.234 |" ]] || false
    [[ "$output" =~ "| > | 1  | 1     |" ]] || false
}

# https://github.com/dolthub/dolt/issues/8133
@test "diff: schema change int to decimal" {
    dolt reset --hard

    dolt sql <<SQL
create table t1 (pk int primary key, val int);

insert into t1 values (1, 1);
call dolt_commit('-Am', 'commit');

alter table t1 modify column val decimal(20,4);
update t1 set val = val + 0.234;

SQL

    run dolt diff
    [ $status -eq 0 ]
    [[ "$output" =~ "| < | 1  | 1.0000 |" ]] || false
    [[ "$output" =~ "| > | 1  | 1.2340 |" ]] || false

    run dolt diff --reverse
    [ $status -eq 0 ]
    [[ "$output" =~ "| < | 1  | 1.2340 |" ]] || false
    [[ "$output" =~ "| > | 1  | 1.0000 |" ]] || false
}

# https://github.com/dolthub/dolt/issues/8133
@test "diff: schema change float to double" {
    dolt reset --hard

    # The int 16777217 can be represented perfectly as a double, but not as a float.
    # When the type is changes, we need to set the value again to make sure the double value is correct.
    dolt sql <<SQL
create table t1 (pk int primary key, val float);

insert into t1 values (1,16777217 );
call dolt_commit('-Am', 'commit');

alter table t1 modify column val double;
update t1 set val = 16777217;

SQL

    run dolt diff
    [ $status -eq 0 ]
    [[ "$output" =~ "| < | 1  | 1.6777216e+07 |" ]] || false
    [[ "$output" =~ "| > | 1  | 1.6777217e+07 |" ]] || false

    run dolt diff --reverse
    [ $status -eq 0 ]
    [[ "$output" =~ "| < | 1  | 1.6777217e+07 |" ]] || false
    [[ "$output" =~ "| > | 1  | 1.6777216e+07 |" ]] || false
}

# https://github.com/dolthub/dolt/issues/8133
@test "diff: schema change int to bigint" {
    dolt reset --hard

    dolt sql <<SQL
create table t1 (pk int primary key, val smallint);

insert into t1 values (1, 32760);
call dolt_commit('-Am', 'commit');

alter table t1 modify column val bigint;
update t1 set val = val + 10000;
SQL

    run dolt diff
    [ $status -eq 0 ]
    [[ "$output" =~ "| < | 1  | 32760 |" ]] || false
    [[ "$output" =~ "| > | 1  | 42760 |" ]] || false

    run dolt diff --reverse
    [ $status -eq 0 ]
    [[ "$output" =~ "| < | 1  | 42760 |" ]] || false
    [[ "$output" =~ "| > | 1  | 32760 |" ]] || false
}

# https://github.com/dolthub/dolt/issues/8133
@test "diff: schema change DATE to TIMESTAMP" {
    dolt reset --hard

    dolt sql <<SQL
create table t1 (pk int primary key, val date);

insert into t1 values (1, '2023-01-01');
call dolt_commit('-Am', 'commit');

alter table t1 modify column val timestamp;
update t1 set val = '2023-01-01 12:21:42';

SQL

    run dolt diff
    [ $status -eq 0 ]
    [[ "$output" =~ "| < | 1  | 2023-01-01 00:00:00 |" ]] || false
    [[ "$output" =~ "| > | 1  | 2023-01-01 12:21:42 |" ]] || false

    run dolt diff --reverse
    [ $status -eq 0 ]
    [[ "$output" =~ "| < | 1  | 2023-01-01 12:21:42 |" ]] || false
    [[ "$output" =~ "| > | 1  | 2023-01-01 00:00:00 |" ]] || false
}

@test "diff: diff --reverse" {
  run dolt diff -R
  [ $status -eq 0 ]
  [[ $output =~ "diff --dolt a/test b/test" ]] || false
  [[ $output =~ "deleted table" ]] || false

  run dolt diff --reverse
  [ $status -eq 0 ]
  [[ $output =~ "diff --dolt a/test b/test" ]] || false
  [[ $output =~ "deleted table" ]] || false
}

@test "diff: duplicate commit_hash in diff" {
  dolt reset --hard
  dolt sql -q "create table t1 (i int primary key);"
  dolt sql -q "create table t2 (j int primary key);"
  dolt add .
  dolt commit -m "commit 1"
  run dolt sql -q "select table_name from dolt_diff where commit_hash = hashof('HEAD')"
  [ $status -eq 0 ]
  [[ "$output" =~ "t1" ]] || false
  [[ "$output" =~ "t2" ]] || false
}


@test "diff: enum data change" {
    dolt sql <<SQL
drop table test;
create table test (pk int primary key, size ENUM('x-small', 'small', 'medium', 'large', 'x-large'));
insert into test values (1,'x-small');
insert into test values (2,'small');
insert into test values (3,'medium');
SQL
    dolt add .
    dolt commit -am "First commit"

    dolt sql <<SQL
insert into test values (4,'large');
delete from test where pk = 1;
update test set size = 'x-large' where pk = 2;
SQL

    run dolt diff

    EXPECTED=$(cat <<'EOF'
+---+----+---------+
|   | pk | size    |
+---+----+---------+
| - | 1  | x-small |
| < | 2  | small   |
| > | 2  | x-large |
| + | 4  | large   |
+---+----+---------+
EOF
)

    [ "$status" -eq 0 ]
    [[ "$output" =~ "$EXPECTED" ]] || false

    run dolt diff --data --schema
    [ "$status" -eq 0 ]
    [[ "$output" =~ "$EXPECTED" ]] || false

    run dolt diff --data
    [[ "$output" =~ "$EXPECTED" ]] || false
}

@test "diff: enum and schema changes" {
    dolt sql <<SQL
drop table test;
create table test (pk int primary key, size ENUM('x-small', 'small', 'medium', 'large', 'x-large'));
insert into test values (1,'x-small');
insert into test values (2,'small');
insert into test values (3,'medium');
SQL
    dolt add .
    dolt commit -am "First commit"

    dolt sql <<SQL
alter table test add column c1 int;
insert into test values (4,'large',1);
delete from test where pk = 1;
update test set size = 'x-large' where pk = 2;
SQL

    run dolt diff

    EXPECTED=$(cat <<'EOF'
 CREATE TABLE `test` (
   `pk` int NOT NULL,
   `size` enum('x-small','small','medium','large','x-large'),
+  `c1` int,
   PRIMARY KEY (`pk`)
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;
+---+----+---------+------+
|   | pk | size    | c1   |
+---+----+---------+------+
| - | 1  | x-small | NULL |
| < | 2  | small   | NULL |
| > | 2  | x-large | NULL |
| + | 4  | large   | 1    |
+---+----+---------+------+
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
   `size` enum('x-small','small','medium','large','x-large'),
+  `c1` int,
   PRIMARY KEY (`pk`)
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;
EOF
)

    [[ "$output" =~ "$EXPECTED" ]] || false
    # Count the line numbers to make sure there are no data changes output
    [ "${#lines[@]}" -eq 9 ]

    run dolt diff --data
    EXPECTED=$(cat <<'EOF'
+---+----+---------+------+
|   | pk | size    | c1   |
+---+----+---------+------+
| - | 1  | x-small | NULL |
| < | 2  | small   | NULL |
| > | 2  | x-large | NULL |
| + | 4  | large   | 1    |
+---+----+---------+------+
EOF
)

    [[ "$output" =~ "$EXPECTED" ]] || false
    # Count the line numbers to make sure there are no schema changes output
    [ "${#lines[@]}" -eq 11 ]
}
