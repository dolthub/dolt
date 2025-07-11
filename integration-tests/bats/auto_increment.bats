#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common

    dolt sql <<SQL
CREATE TABLE test (
  pk int NOT NULL PRIMARY KEY AUTO_INCREMENT,
  c0 int
);
SQL
    dolt commit -Am "initial commit"

}

teardown() {
    assert_feature_version
    teardown_common
}

@test "auto_increment: insert into auto_increment table" {
    dolt sql -q "INSERT INTO test VALUES (1,11),(2,22),(3,33);"

    run dolt sql -q "INSERT INTO test (c0) VALUES (44);"
    [ "$status" -eq 0 ]
    run dolt sql -q "SELECT * FROM test WHERE c0 = 44 ORDER BY pk;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "4,44" ]] || false

    run dolt sql -q "INSERT INTO test (c0) VALUES (55),(66);"
    [ "$status" -eq 0 ]
    run dolt sql -q "SELECT * FROM test WHERE c0 > 50 ORDER BY pk;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "5,55" ]] || false
    [[ "$output" =~ "6,66" ]] || false
}

@test "auto_increment: create auto_increment table with out-of-line PK def" {
    run dolt sql <<SQL
CREATE TABLE ai (
    pk int AUTO_INCREMENT,
    c0 int,
    PRIMARY KEY(pk)
);
INSERT INTO ai VALUES (NULL,1),(NULL,2),(NULL,3);
SQL
    [ "$status" -eq 0 ]
    run dolt sql -q "SELECT * FROM ai ORDER BY pk;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,3" ]] || false
}

@test "auto_increment: insert into empty auto_increment table" {
    run dolt sql -q "INSERT INTO test (c0) VALUES (1);"
    [ "$status" -eq 0 ]
    run dolt sql -q "SELECT * FROM test WHERE c0 = 1 ORDER BY pk;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,1" ]] || false
}

@test "auto_increment: insert into auto_increment table with skipped keys" {
    dolt sql -q "INSERT INTO test VALUES (1,1),(100,100);"

    run dolt sql -q "INSERT INTO test (c0) VALUES (101);"
    [ "$status" -eq 0 ]
    run dolt sql -q "INSERT INTO test VALUES (2,2);"
    [ "$status" -eq 0 ]
    run dolt sql -q "SELECT * FROM test ORDER BY pk;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "100,100" ]] || false
    [[ "$output" =~ "101,101" ]] || false
}

@test "auto_increment: insert into auto_increment table with NULL" {
    dolt sql -q "INSERT INTO test VALUES (1,1);"

    run dolt sql -q "INSERT INTO test (pk,c0) VALUES (NULL,2);"
    [ "$status" -eq 0 ]
    run dolt sql -q "SELECT * FROM test WHERE c0 > 1 ORDER BY pk;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "2,2" ]] || false

    run dolt sql -q "INSERT INTO test VALUES (NULL,3), (10,10), (NULL,11);"
    [ "$status" -eq 0 ]
    run dolt sql -q "SELECT * FROM test WHERE c0 > 2 ORDER BY pk;" -r csv
    echo $output
    [ "$status" -eq 0 ]
    [[ "$output" =~ "3,3" ]] || false
    [[ "$output" =~ "10,10" ]] || false
    [[ "$output" =~ "11,11" ]] || false
}

@test "auto_increment: insert into auto_increment table with 0" {
    dolt sql -q "INSERT INTO test VALUES (1,1);"

    run dolt sql -q "INSERT INTO test (pk,c0) VALUES (0,2);"
    [ "$status" -eq 0 ]
    run dolt sql -q "SELECT * FROM test WHERE c0 > 1 ORDER BY pk;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "2,2" ]] || false

    run dolt sql -q "INSERT INTO test VALUES (0,3), (10,10), (0,11);"
    [ "$status" -eq 0 ]
    run dolt sql -q "SELECT * FROM test WHERE c0 > 2 ORDER BY pk;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "3,3" ]] || false
    [[ "$output" =~ "10,10" ]] || false
    [[ "$output" =~ "11,11" ]] || false
}

@test "auto_increment: insert into auto_increment table via batch mode" {
    run dolt sql <<SQL
INSERT INTO test (c0) VALUES (1);
INSERT INTO test (c0) VALUES (2);
INSERT INTO test (pk,c0) VALUES (19,19);
INSERT INTO test (c0) VALUES (20);
INSERT INTO test (c0) VALUES (21);
SQL
    [ "$status" -eq 0 ]

    run dolt sql -q "SELECT * FROM test ORDER BY pk;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "19,19" ]] || false
    [[ "$output" =~ "20,20" ]] || false
    [[ "$output" =~ "21,21" ]] || false
}

@test "auto_increment: insert into table via batch mode" {
    # asserts proper batch mode handling
    run dolt sql <<SQL
CREATE TABLE test2 (
    pk int PRIMARY KEY,
    c0 int
);
INSERT INTO test2 VALUES (1,1),(2,2),(3,3);
INSERT INTO test2 SELECT (pk + 10), c0 FROM test2;
INSERT INTO test2 SELECT (pk + 20), c0 FROM test2;
SQL
    [ "$status" -eq 0 ]

    run dolt sql -q "select * from test2 ORDER BY pk" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,3" ]] || false
    [[ "$output" =~ "11,1" ]] || false
    [[ "$output" =~ "12,2" ]] || false
    [[ "$output" =~ "13,3" ]] || false
    [[ "$output" =~ "21,1" ]] || false
    [[ "$output" =~ "22,2" ]] || false
    [[ "$output" =~ "23,3" ]] || false
}

@test "auto_increment: FLOAT AUTO_INCREMENT should be rejected" {
    run dolt sql <<SQL
CREATE TABLE auto_float (
    pk float NOT NULL PRIMARY KEY AUTO_INCREMENT,
    c0 int
);
SQL
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Incorrect column specifier for column 'pk'" ]] || false
}

@test "auto_increment: create auto_increment tables with integer types" {
    # signed integer types only (MySQL 8.4.5+ behavior)
    for TYPE in TINYINT SMALLINT MEDIUMINT INT BIGINT
    do
        dolt sql <<SQL
CREATE TABLE auto_$TYPE (
    pk $TYPE NOT NULL PRIMARY KEY AUTO_INCREMENT,
    c0 int
);
INSERT INTO auto_$TYPE VALUES (1,1);
SQL
        echo "$TYPE"
        run dolt sql -q "INSERT INTO auto_$TYPE (c0) VALUES (2);"
        [ "$status" -eq 0 ]
        run dolt sql -q "SELECT * FROM auto_$TYPE WHERE c0 > 1 ORDER BY pk;" -r csv
        [ "$status" -eq 0 ]
        [[ "$output" =~ "2,2" ]] || false
    done

    # unsigned integer types
    for TYPE in TINYINT SMALLINT MEDIUMINT INT BIGINT
    do
        dolt sql <<SQL
CREATE TABLE auto2_$TYPE (
    pk $TYPE UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    c0 int
);
INSERT INTO auto2_$TYPE VALUES (1,1);
SQL
        echo "$TYPE"
        run dolt sql -q "INSERT INTO auto2_$TYPE (c0) VALUES (2);"
        [ "$status" -eq 0 ]
        run dolt sql -q "SELECT * FROM auto2_$TYPE WHERE c0 > 1 ORDER BY pk;" -r csv
        [ "$status" -eq 0 ]
        [[ "$output" =~ "2,2" ]] || false
    done
}

@test "auto_increment: FLOAT and DOUBLE AUTO_INCREMENT should be rejected" {
    # floating point types should be rejected in MySQL 8.4.5+
    for TYPE in FLOAT DOUBLE
    do
        run dolt sql <<SQL
CREATE TABLE auto_$TYPE (
    pk $TYPE NOT NULL PRIMARY KEY AUTO_INCREMENT,
    c0 int
);
SQL
        echo "Testing $TYPE rejection"
        [ "$status" -eq 1 ]
        [[ "$output" =~ "Incorrect column specifier for column 'pk'" ]] || false
    done
}

@test "auto_increment: invalid AUTO_INCREMENT definitions fail" {
    run dolt sql -q "CREATE TABLE bad (pk int PRIMARY KEY, c0 int AUTO_INCREMENT);"
    [ "$status" -ne 0 ]
    [[ "$output" =~ "there can be only one auto_increment column and it must be defined as a key" ]] || false

    run dolt sql -q "CREATE TABLE bad (pk1 int AUTO_INCREMENT, pk2 int AUTO_INCREMENT, PRIMARY KEY (pk1,pk2));"
    [ "$status" -ne 0 ]
    [[ "$output" =~ "there can be only one auto_increment column and it must be defined as a key" ]] || false

    run dolt sql -q "CREATE TABLE bad (pk1 int AUTO_INCREMENT DEFAULT 10, c0 int);"
    [ "$status" -ne 0 ]
    [[ "$output" =~ "there can be only one auto_increment column and it must be defined as a key" ]] || false
}

@test "auto_increment: AUTO_INCREMENT merge main branch ahead" {
    dolt sql -q "INSERT INTO test (c0) VALUES (0),(1),(2)"
    dolt add -A
    dolt commit -m "made some inserts"

    dolt checkout -b other
    dolt sql -q "INSERT INTO test VALUES (10,10),(NULL,11);"
    dolt add -A
    dolt commit -m "inserted 10 & 11 on other"

    dolt checkout main
    dolt sql -q "INSERT INTO test VALUES (20,20),(NULL,21);"
    dolt add -A
    dolt commit -m "inserted 20 & 21 on main"
    dolt merge other

    dolt sql -q "INSERT INTO test VALUES (NULL,22);"
    run dolt sql -q "SELECT pk FROM test WHERE c0 = 22 ORDER BY pk;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "22" ]] || false
}

@test "auto_increment: AUTO_INCREMENT merge other branch ahead" {
    dolt sql -q "INSERT INTO test (c0) VALUES (0),(1),(2)"
    dolt add -A
    dolt commit -m "made some inserts"

    dolt branch other
    dolt sql -q "INSERT INTO test VALUES (10,10),(NULL,11);"
    dolt add -A
    dolt commit -m "inserted 10 & 11 on main"

    dolt checkout other
    dolt sql -q "INSERT INTO test VALUES (20,20),(NULL,21);"
    dolt add -A
    dolt commit -m "inserted 20 & 21 on other"

    dolt checkout main
    dolt merge other -m "merge other"
    dolt sql -q "INSERT INTO test VALUES (NULL,22);"
    run dolt sql -q "SELECT pk FROM test WHERE c0 = 22 ORDER BY pk;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "22" ]] || false
}

@test "auto_increment: AUTO_INCREMENT with ALTER TABLE" {
    run dolt sql -q "ALTER TABLE test AUTO_INCREMENT = 10;"
    [ "$status" -eq 0 ]
    dolt sql -q "INSERT INTO test VALUES (NULL,10);"
    run dolt sql -q "SELECT * FROM test ORDER BY pk;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "10,10" ]] || false

    dolt sql <<SQL
ALTER TABLE test AUTO_INCREMENT = 20;
INSERT INTO test VALUES (NULL,20),(30,30),(NULL,31);
SQL
    run dolt sql -q "SELECT * FROM test ORDER BY pk;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "10,10" ]] || false
    [[ "$output" =~ "20,20" ]] || false
    [[ "$output" =~ "30,30" ]] || false
    [[ "$output" =~ "31,31" ]] || false
}

@test "auto_increment: adding index to AUTO_INCREMENT doesn't reset sequence" {
    dolt sql <<SQL
CREATE TABLE index_test (
    pk int PRIMARY KEY AUTO_INCREMENT,
    c0 int
);

INSERT INTO index_test (c0) VALUES (1),(2),(3);

ALTER TABLE index_test ADD INDEX (c0);

INSERT INTO index_test (c0) VALUES (4),(5),(6);
SQL

    run dolt sql -q "select * from index_test ORDER BY pk" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[0]}" =~ "pk,c0" ]] || false
    [[ "${lines[1]}" =~ "1,1" ]] || false
    [[ "${lines[2]}" =~ "2,2" ]] || false
    [[ "${lines[3]}" =~ "3,3" ]] || false
    [[ "${lines[4]}" =~ "4,4" ]] || false
    [[ "${lines[5]}" =~ "5,5" ]] || false
    [[ "${lines[6]}" =~ "6,6" ]] || false
}

@test "auto_increment: INSERT INTO SELECT ..." {
    dolt sql <<SQL
    CREATE TABLE other (pk int PRIMARY KEY);
    INSERT INTO other VALUES (1),(2),(3);
SQL

    dolt sql -q "INSERT INTO test (c0) SELECT pk FROM other;"

    run dolt sql -q "SELECT * FROM test ORDER BY pk;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[0]}" =~ "pk,c0" ]] || false
    [[ "${lines[1]}" =~ "1,1" ]] || false
    [[ "${lines[2]}" =~ "2,2" ]] || false
    [[ "${lines[3]}" =~ "3,3" ]] || false
}

@test "auto_increment: truncate in session correctly resets auto increment values" {
    dolt sql <<SQL
CREATE TABLE t (
    pk int PRIMARY KEY AUTO_INCREMENT,
    c0 int
);

INSERT INTO t (c0) VALUES (1),(2),(3);
TRUNCATE t;
INSERT INTO t (c0) VALUES (1),(2),(3);
TRUNCATE t;
INSERT INTO t (c0) VALUES (1),(2),(3);
SQL

    run dolt sql -q "SELECT * FROM t ORDER BY pk;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[0]}" =~ "pk,c0" ]] || false
    [[ "${lines[1]}" =~ "1,1" ]] || false
    [[ "${lines[2]}" =~ "2,2" ]] || false
    [[ "${lines[3]}" =~ "3,3" ]] || false
}

@test "auto_increment: separate insert statements doesn't cause problems" {
    dolt sql <<SQL
CREATE TABLE t (
    pk int PRIMARY KEY AUTO_INCREMENT,
    c0 int
);

INSERT INTO t (c0) VALUES (1),(2),(3);
INSERT INTO t (c0) VALUES (4),(5),(6);
SQL

    run dolt sql -q "SELECT * FROM t ORDER BY pk;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[0]}" =~ "pk,c0" ]] || false
    [[ "${lines[1]}" =~ "1,1" ]] || false
    [[ "${lines[2]}" =~ "2,2" ]] || false
    [[ "${lines[3]}" =~ "3,3" ]] || false
    [[ "${lines[4]}" =~ "4,4" ]] || false
    [[ "${lines[5]}" =~ "5,5" ]] || false
    [[ "${lines[6]}" =~ "6,6" ]] || false
}

@test "auto_increment: skipping keys" {
dolt sql <<SQL
CREATE TABLE t (
    pk int PRIMARY KEY AUTO_INCREMENT,
    c0 int
);

INSERT INTO t (c0) VALUES (1), (2);
INSERT INTO t VALUES (4, 4);
INSERT INTO t (c0) VALUES (5),(6),(7);
SQL

    run dolt sql -q "SELECT * FROM t ORDER BY pk;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[0]}" =~ "pk,c0" ]] || false
    [[ "${lines[1]}" =~ "1,1" ]] || false
    [[ "${lines[2]}" =~ "2,2" ]] || false
    [[ "${lines[3]}" =~ "4,4" ]] || false
    [[ "${lines[4]}" =~ "5,5" ]] || false
    [[ "${lines[5]}" =~ "6,6" ]] || false
    [[ "${lines[6]}" =~ "7,7" ]] || false
    ! [[ "$output" =~ "3" ]] || false
}

@test "auto_increment: go forward then backwards" {
    dolt sql <<SQL
CREATE TABLE t (
    pk int PRIMARY KEY AUTO_INCREMENT,
    c0 int
);

INSERT INTO t (c0) VALUES (1), (2);
INSERT INTO t VALUES (4, 4);
INSERT INTO t (c0) VALUES (5),(6),(7);
INSERT into t VALUES (3, 3);
INSERT INTO t (c0) VALUES (8);
SQL

    run dolt sql -q "SELECT * FROM t ORDER BY pk;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[0]}" =~ "pk,c0" ]] || false
    [[ "${lines[1]}" =~ "1,1" ]] || false
    [[ "${lines[2]}" =~ "2,2" ]] || false
    [[ "${lines[3]}" =~ "3,3" ]] || false
    [[ "${lines[4]}" =~ "4,4" ]] || false
    [[ "${lines[5]}" =~ "5,5" ]] || false
    [[ "${lines[6]}" =~ "6,6" ]] || false
    [[ "${lines[7]}" =~ "7,7" ]] || false
    [[ "${lines[8]}" =~ "8,8" ]] || false
}

@test "auto_increment: dolt_merge() works with no auto increment overlap" {
    dolt sql <<SQL
CREATE TABLE t (
    pk int PRIMARY KEY AUTO_INCREMENT,
    c0 int
);

CALL DOLT_ADD('.');
INSERT INTO t (c0) VALUES (1), (2);
call dolt_commit('-a', '-m', 'cm1');
call dolt_checkout('-b', 'test');

INSERT INTO t (c0) VALUES (3), (4);
call dolt_commit('-a', '-m', 'cm2');
call dolt_checkout('main');

call dolt_merge('test');
INSERT INTO t VALUES (NULL,5),(6,6),(NULL,7);
SQL

    run dolt sql -q "SELECT * FROM t ORDER BY pk;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[0]}" =~ "pk,c0" ]] || false
    [[ "${lines[1]}" =~ "1,1" ]] || false
    [[ "${lines[2]}" =~ "2,2" ]] || false
    [[ "${lines[3]}" =~ "3,3" ]] || false
    [[ "${lines[4]}" =~ "4,4" ]] || false
    [[ "${lines[5]}" =~ "5,5" ]] || false
    [[ "${lines[6]}" =~ "6,6" ]] || false
    [[ "${lines[7]}" =~ "7,7" ]] || false
}

@test "auto_increment: Jump in auto increment values after a dolt merge" {
    dolt sql <<SQL
CREATE TABLE t (
    pk int PRIMARY KEY AUTO_INCREMENT,
    c0 int
);

CALL DOLT_ADD('.');
INSERT INTO t (c0) VALUES (1), (2);
call dolt_commit('-a', '-m', 'cm1');
call dolt_checkout('-b', 'test');

INSERT INTO t (c0) VALUES (3), (4);
call dolt_commit('-a', '-m', 'cm2');
call dolt_checkout('main');

call dolt_merge('test');
INSERT INTO t VALUES (10,10),(NULL,11);
SQL

    run dolt sql -q "SELECT * FROM t ORDER BY pk;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[0]}" =~ "pk,c0" ]] || false
    [[ "${lines[1]}" =~ "1,1" ]] || false
    [[ "${lines[2]}" =~ "2,2" ]] || false
    [[ "${lines[3]}" =~ "3,3" ]] || false
    [[ "${lines[4]}" =~ "4,4" ]] || false
    [[ "${lines[5]}" =~ "10,10" ]] || false
    [[ "${lines[6]}" =~ "11,11" ]] || false

    run dolt sql -q "SELECT COUNT(*) FROM t"
    [[ "$output" =~ "6" ]] || false
}

@test "auto_increment: dolt_merge() with a gap in an auto increment key" {
    dolt sql <<SQL
CREATE TABLE t (
    pk int PRIMARY KEY AUTO_INCREMENT,
    c0 int
);

CALL DOLT_ADD('.');
INSERT INTO t (c0) VALUES (1), (2);
call dolt_commit('-a', '-m', 'cm1');
call dolt_checkout('-b', 'test');

INSERT INTO t VALUES (4,4), (5,5);
call dolt_commit('-a', '-m', 'cm2');
call dolt_checkout('main');

call dolt_merge('test');
INSERT INTO t VALUES (3,3),(NULL,6);
SQL

    run dolt sql -q "SELECT * FROM t ORDER BY pk;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[0]}" =~ "pk,c0" ]] || false
    [[ "${lines[1]}" =~ "1,1" ]] || false
    [[ "${lines[2]}" =~ "2,2" ]] || false
    [[ "${lines[3]}" =~ "3,3" ]] || false
    [[ "${lines[4]}" =~ "4,4" ]] || false
    [[ "${lines[5]}" =~ "5,5" ]] || false
    [[ "${lines[6]}" =~ "6,6" ]] || false

    run dolt sql -q "SELECT COUNT(*) FROM t"
    [[ "$output" =~ "6" ]] || false
}

@test "auto_increment: dolt_merge() with lesser auto increment keys" {
    dolt sql <<SQL
CREATE TABLE t (
    pk int PRIMARY KEY AUTO_INCREMENT,
    c0 int
);

CALL DOLT_ADD('.');
INSERT INTO t VALUES (4, 4), (5, 5);
call dolt_commit('-a', '-m', 'cm1');
call dolt_checkout('-b', 'test');

INSERT INTO t VALUES (1,1), (2, 2);
call dolt_commit('-a', '-m', 'cm2');
call dolt_checkout('main');

call dolt_merge('test');
INSERT INTO t VALUES (NULL,6);
SQL

    run dolt sql -q "SELECT * FROM t ORDER BY pk;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[0]}" =~ "pk,c0" ]] || false
    [[ "${lines[1]}" =~ "1,1" ]] || false
    [[ "${lines[2]}" =~ "2,2" ]] || false
    [[ "${lines[3]}" =~ "4,4" ]] || false
    [[ "${lines[4]}" =~ "5,5" ]] || false
    [[ "${lines[5]}" =~ "6,6" ]] || false
}

@test "auto_increment: alter table autoincrement on table with no AI key nops" {
    dolt sql -q "create table test2(pk int primary key, name varchar(255), type int);"
    run dolt sql -q "alter table test2 auto_increment = 2;"
    [ "$status" -eq 0 ]

    dolt sql -q "insert into test2 values (0, 'john', 0)"
    run dolt sql -q "SELECT * from test2 ORDER BY pk" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "0,john,0" ]] || false
}

@test "auto_increment: alter table change column to auto inc" {
    dolt sql -q "create table t(pk int primary key, v int);"
    dolt sql -q "insert into t values (1,1), (2,2), (3,3);"
    dolt sql -q "ALTER TABLE t CHANGE COLUMN pk pk int NOT NULL AUTO_INCREMENT PRIMARY KEY;"

    dolt sql -q 'insert into t(pk) values (NULL), (NULL), (NULL)'
    run dolt sql -q "SELECT pk FROM t ORDER BY pk" -r csv
    [[ "${lines[0]}" =~ "pk" ]] || false
    [[ "${lines[1]}" =~ "1" ]] || false
    [[ "${lines[2]}" =~ "2" ]] || false
    [[ "${lines[3]}" =~ "3" ]] || false
    [[ "${lines[4]}" =~ "4" ]] || false
    [[ "${lines[5]}" =~ "5" ]] || false
    [[ "${lines[6]}" =~ "6" ]] || false
}

@test "auto_increment: alter table change to remove auto inc" {
    dolt sql -q "create table t(pk int primary key, v int);"
    dolt sql -q "insert into t values (1,1), (2,2);"
    dolt sql -q "ALTER TABLE t CHANGE COLUMN pk pk int NOT NULL AUTO_INCREMENT PRIMARY KEY;"

    run dolt sql -q 'show create table t'
    [ "$status" -eq 0 ]
    [[ "$output" =~ AUTO_INCREMENT ]] || false
    
    dolt sql -q 'insert into t(pk) values (NULL), (NULL)'
    dolt sql -q "ALTER TABLE t CHANGE COLUMN pk pk int NOT NULL PRIMARY KEY;"

    # null insert into key column is no longer possible
    run dolt sql -q 'insert into t(pk) values (NULL), (NULL)'
    [ "$status" -ne 0 ]
    run dolt sql -q 'insert into t(v) values (5), (6)'
    [ "$status" -ne 0 ]

    run dolt sql -q 'show create table t'
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ AUTO_INCREMENT ]] || false

    dolt sql -q 'insert into t(pk) values (5), (6)'
    
    run dolt sql -q "SELECT pk FROM t ORDER BY pk" -r csv
    [[ "${lines[0]}" =~ "pk" ]] || false
    [[ "${lines[1]}" =~ "1" ]] || false
    [[ "${lines[2]}" =~ "2" ]] || false
    [[ "${lines[3]}" =~ "3" ]] || false
    [[ "${lines[4]}" =~ "4" ]] || false
    [[ "${lines[5]}" =~ "5" ]] || false
    [[ "${lines[6]}" =~ "6" ]] || false
}

@test "auto_increment: alter table modify column to auto inc" {
    dolt sql -q "create table t(pk int primary key, v int);"
    dolt sql -q "insert into t values (1,1), (2,2), (3,3);"
    dolt sql -q "ALTER TABLE t modify COLUMN pk int NOT NULL AUTO_INCREMENT;"

    dolt sql -q 'insert into t(pk) values (NULL), (NULL), (NULL)'
    run dolt sql -q "SELECT * FROM t ORDER BY pk" -r csv
    [[ "${lines[0]}" =~ "pk" ]] || false
    [[ "${lines[1]}" =~ "1" ]] || false
    [[ "${lines[2]}" =~ "2" ]] || false
    [[ "${lines[3]}" =~ "3" ]] || false
    [[ "${lines[4]}" =~ "4" ]] || false
    [[ "${lines[5]}" =~ "5" ]] || false
    [[ "${lines[6]}" =~ "6" ]] || false
}

@test "auto_increment: alter table modify column to auto inc with different types" {
    # Test successful ALTER with integer types
    dolt sql -q "create table t(pk int unsigned primary key, v int);"
    dolt sql -q "insert into t values (1,1);"
    dolt sql -q "ALTER TABLE t modify COLUMN pk int unsigned NOT NULL AUTO_INCREMENT;"

    dolt sql -q 'insert into t(pk) values (NULL)'
    run dolt sql -q "SELECT * FROM t ORDER BY pk" -r csv
    [[ "${lines[0]}" =~ "pk" ]] || false
    [[ "${lines[1]}" =~ "1" ]] || false
    [[ "${lines[2]}" =~ "2" ]] || false

    # Test rejected ALTER with float type
    dolt sql -q "drop table t"
    dolt sql -q "create table t(pk float primary key, v int);"
    dolt sql -q "insert into t values (1,1);"
    run dolt sql -q "ALTER TABLE t modify COLUMN pk float NOT NULL AUTO_INCREMENT;"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Incorrect column specifier for column 'pk'" ]] || false

    # type changes in the alter statement
    dolt sql -q "drop table t"
    dolt sql -q "create table t(pk int unsigned primary key, v int);"
    dolt sql -q "insert into t values (1,1);"
    dolt sql -q "ALTER TABLE t modify COLUMN pk int NOT NULL AUTO_INCREMENT;"

    dolt sql -q 'insert into t(pk) values (NULL)'
    run dolt sql -q "SELECT * FROM t ORDER BY pk" -r csv
    [[ "${lines[0]}" =~ "pk" ]] || false
    [[ "${lines[1]}" =~ "1" ]] || false
    [[ "${lines[2]}" =~ "2" ]] || false

    dolt sql -q "drop table t"
    dolt sql -q "create table t(pk float primary key, v int);"
    dolt sql -q "insert into t values (1,1);"
    dolt sql -q "ALTER TABLE t modify COLUMN pk int NOT NULL AUTO_INCREMENT;"

    dolt sql -q 'insert into t(pk) values (NULL)'
    run dolt sql -q "SELECT * FROM t ORDER BY pk" -r csv
    [[ "${lines[0]}" =~ "pk" ]] || false
    [[ "${lines[1]}" =~ "1" ]] || false
    [[ "${lines[2]}" =~ "2" ]] || false    
}

@test "auto_increment: alter table add constraint for different database" {
    dolt sql  <<SQL
CREATE DATABASE public;
CREATE TABLE public.test (pk integer NOT NULL, c1 integer, c2 integer);
ALTER TABLE public.test ADD CONSTRAINT serial_pk_pkey PRIMARY KEY (pk);
ALTER TABLE public.test MODIFY pk integer auto_increment;
SQL

    run dolt sql -q "SHOW CREATE TABLE public.test"
    [ $status -eq 0 ]
    [[ "$output" =~ "NOT NULL AUTO_INCREMENT" ]] || false
}

@test "auto_increment: globally distinct auto increment values" {
    dolt sql  <<SQL
call dolt_branch('branch1');
call dolt_branch('branch2');

insert into test (c0) values (1), (2);
call dolt_commit('-am', 'main values');

call dolt_checkout('branch1');
insert into test (c0) values (3), (4);
call dolt_commit('-am', 'branch1 values');

call dolt_checkout('branch2');
insert into test (c0) values (5), (6);
call dolt_commit('-am', 'branch2 values');
SQL

    run dolt sql -q 'select * from test' -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false

    dolt checkout branch1
    run dolt sql -q 'select * from test' -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ "3,3" ]] || false
    [[ "$output" =~ "4,4" ]] || false

    dolt checkout branch2
    run dolt sql -q 'select * from test' -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ "5,5" ]] || false
    [[ "$output" =~ "6,6" ]] || false

    # Should have the same result across multiple invocations of sql as well
    dolt checkout main
    dolt sql  <<SQL
create table t1 (ai bigint primary key auto_increment, c0 int);
call dolt_add('.');
call dolt_commit('-am', 'empty table');
call dolt_branch('branch3');
call dolt_branch('branch4');
insert into t1 (c0) values (1), (2);
call dolt_commit('-am', 'main values');
SQL

    dolt sql  <<SQL    
call dolt_checkout('branch3');
insert into t1 (c0) values (3), (4);
call dolt_commit('-am', 'branch3 values');
SQL

    dolt sql  <<SQL        
call dolt_checkout('branch4');
insert into t1 (c0) values (5), (6);
call dolt_commit('-am', 'branch4 values');
SQL

    run dolt sql -q 'select * from t1' -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false

    dolt checkout branch3
    run dolt sql -q 'select * from t1' -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ "3,3" ]] || false
    [[ "$output" =~ "4,4" ]] || false

    dolt checkout branch4
    run dolt sql -q 'select * from t1' -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ "5,5" ]] || false
    [[ "$output" =~ "6,6" ]] || false    
}

@test "auto_increment: newly cloned database" {
    dolt sql  <<SQL
call dolt_branch('branch1');
call dolt_branch('branch2');

insert into test (c0) values (1), (2);
call dolt_commit('-am', 'main values');

call dolt_checkout('branch1');
insert into test (c0) values (3), (4);
call dolt_commit('-am', 'branch1 values');

call dolt_checkout('branch2');
insert into test (c0) values (5), (6);
call dolt_commit('-am', 'branch2 values');
SQL

    dolt remote add remote1 file://./remote1
    dolt push remote1 main
    dolt push remote1 branch1
    dolt push remote1 branch2

    dolt clone file://./remote1 clone
    cd clone

    # The clone should find the values on the remote branches that haven't been
    # checked out locally
    dolt sql  <<SQL    
insert into test (c0) values (7), (8);
SQL

    run dolt sql -q 'select * from test' -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ "7,7" ]] || false
    [[ "$output" =~ "8,8" ]] || false
}

@test "auto_increment: manually set auto increment to original value" {
    dolt sql  <<SQL
drop table test;
create table test (id int auto_increment primary key);
insert into test values ();
insert into test values ();
SQL

    run dolt sql -q "select * from test order by 1" -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ "2" ]] || false

    dolt sql -q "delete from test"
    dolt sql  <<SQL
alter table test auto_increment=1;
insert into test values ();
insert into test values ();
SQL

    run dolt sql -q "select * from test order by 1" -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ "2" ]] || false

    dolt sql  <<SQL
alter table test auto_increment=1;
insert into test values ();
insert into test values ();
SQL

    # auto_increment update ignored because it's lower than current table max
    run dolt sql -q "select * from test where id > 2 order by 1" -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ "4" ]] || false
}


@test "auto_increment: auto inc shows up in SHOW CREATE (AS OF)" {
    dolt checkout -b other
    dolt sql -q "create table t(pk int primary key auto_increment);"
    dolt sql -q "insert into t values (1);"
    dolt commit -Am "create table"

    dolt sql -q "insert into t values (2);"
    dolt commit -Am "insert into table"

    run dolt sql -q 'show create table t'
    [ "$status" -eq 0 ]
    [[ "$output" =~ AUTO_INCREMENT ]] || false

    run dolt sql -q 'show create table t as of other'
    [ "$status" -eq 0 ]
    [[ "$output" =~ AUTO_INCREMENT ]] || false

    run dolt sql -q 'show create table t as of HEAD'
    [ "$status" -eq 0 ]
    [[ "$output" =~ AUTO_INCREMENT ]] || false

    dolt sql -q 'show create table t as of `HEAD^`'
    run dolt sql -q 'show create table t as of `HEAD^`'
    [ "$status" -eq 0 ]
    [[ "$output" =~ AUTO_INCREMENT ]] || false
}

@test "auto_increment: setting auto_increment to 1 vs 0 produces identical hashes and no diffs" {
    dolt branch auto_increment_0
    dolt branch auto_increment_1

    dolt checkout auto_increment_0
    dolt sql -q "ALTER TABLE test AUTO_INCREMENT=0;"
    run dolt commit -Am "explicitly set AUTO_INCREMENT to 0"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "no changes added to commit" ]] || false
    # Make a new commit by touching an unrelated table
    dolt sql -q "CREATE TABLE other (pk int primary key)"
    run dolt commit -Am "create other table"

    dolt checkout auto_increment_1
    dolt sql -q "ALTER TABLE test AUTO_INCREMENT=1;"
    run dolt commit -Am "explicitly set AUTO_INCREMENT to 1"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "no changes added to commit" ]] || false
    # Make a new commit by touching an unrelated table
    dolt sql -q "CREATE TABLE other (pk char(1) primary key)"
    run dolt commit -Am "create other table"

    # Verify that tables produce no diff
    run dolt diff main auto_increment_0 test
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 0 ]

    run dolt diff main auto_increment_1 test
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 0 ]

    # Verify that tables have the same hash
    dolt checkout main
    main_hash=$(dolt sql -r csv -q "SELECT DOLT_HASHOF_TABLE('test')")
    dolt checkout auto_increment_0
    ai0_hash=$(dolt sql -r csv -q "SELECT DOLT_HASHOF_TABLE('test')")
    dolt checkout auto_increment_1
    ai1_hash=$(dolt sql -r csv -q "SELECT DOLT_HASHOF_TABLE('test')")

    [[ "$main_hash" = "$ai0_hash" ]] || false
    [[ "$main_hash" = "$ai1_hash" ]] || false
}
