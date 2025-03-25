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

    dolt diff HEAD~1 HEAD
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

# https://github.com/dolthub/dolt/issues/8551
@test "diff: schema change increase decimal precision" {
    dolt reset --hard

    dolt sql <<SQL
create table t1 (d decimal(10, 2));
insert into t1 values (123.45);
call dolt_commit('-Am', 'commit');

alter table t1 modify column d decimal(10, 4);
SQL

    run dolt diff
    [ $status -eq 0 ]
    [[ "$output" =~ "| - | 123.4500 |" ]] || false
    [[ "$output" =~ "| + | 123.4500 |" ]] || false
}

# https://github.com/dolthub/dolt/issues/8551
@test "diff: schema change decrease decimal precision" {
    dolt reset --hard

    dolt sql <<SQL
create table t1 (d decimal(10, 4));
insert into t1 values (123.4567);
call dolt_commit('-Am', 'commit');

alter table t1 modify column d decimal(10, 2);
SQL

    run dolt diff
    [ $status -eq 0 ]
    [[ "$output" =~ "| - | 123.46 |" ]] || false
    [[ "$output" =~ "| + | 123.46 |" ]] || false
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

@test "diff: autoincrement registers as schema diff" {
    dolt sql <<SQL
drop table test;
create table test (pk int primary key auto_increment);
insert into test values (1);
SQL
    dolt add .
    dolt commit -am "First commit"

    dolt sql <<SQL
insert into test values (2);
delete from test where pk = 2;
SQL

    run dolt diff

    # Somehow, bats rejects this Heredoc if it contains unmatched parentheses.
    # So we add a parenthesis to the first line and remove it with tail.
    EXPECTED=$(tail -n 8 <<'EOF'
(
diff --dolt a/test b/test
--- a/test
+++ b/test
 CREATE TABLE `test` (
   `pk` int NOT NULL AUTO_INCREMENT,
   PRIMARY KEY (`pk`)
-) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;
+) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;
EOF
)

    [ "$status" -eq 0 ]
    [[ "$output" =~ "$EXPECTED" ]] || false

    run dolt diff --data --schema
    [ "$status" -eq 0 ]
    [[ "$output" =~ "$EXPECTED" ]] || false

    dolt diff --schema
    run dolt diff --schema

    [[ "$output" =~ "$EXPECTED" ]] || false
    # Count the line numbers to make sure there are no data changes output
    [ "${#lines[@]}" -eq 8 ]

    run dolt diff --data
    EXPECTED=$(cat <<'EOF'
diff --dolt a/test b/test
--- a/test
+++ b/test
EOF
)

    [[ "$output" =~ "$EXPECTED" ]] || false
    # Count the line numbers to make sure there are no schema changes output
    [ "${#lines[@]}" -eq 3 ]
}