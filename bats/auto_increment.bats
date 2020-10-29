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

}

teardown() {
    teardown_common
}

@test "insert into auto_increment table" {
    dolt sql -q "INSERT INTO test VALUES (1,11),(2,22),(3,33);"

    run dolt sql -q "INSERT INTO test (c0) VALUES (44);"
    [ "$status" -eq 0 ]
    run dolt sql -q "SELECT * FROM test WHERE c0 = 44;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "4,44" ]] || false

    run dolt sql -q "INSERT INTO test (c0) VALUES (55),(66);"
    [ "$status" -eq 0 ]
    run dolt sql -q "SELECT * FROM test WHERE c0 > 50;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "5,55" ]] || false
    [[ "$output" =~ "6,66" ]] || false
}

@test "create auto_increment table with out-of-line PK def" {
    run dolt sql <<SQL
CREATE TABLE ai (
    pk int AUTO_INCREMENT,
    c0 int,
    PRIMARY KEY(pk)
);
INSERT INTO ai VALUES (NULL,1),(NULL,2),(NULL,3);
SQL
    [ "$status" -eq 0 ]
    run dolt sql -q "SELECT * FROM ai;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,3" ]] || false
}

@test "insert into empty auto_increment table" {
    run dolt sql -q "INSERT INTO test (c0) VALUES (1);"
    [ "$status" -eq 0 ]
    run dolt sql -q "SELECT * FROM test WHERE c0 = 1;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,1" ]] || false
}

@test "insert into auto_increment table with skipped keys" {
    dolt sql -q "INSERT INTO test VALUES (1,1),(100,100);"

    run dolt sql -q "INSERT INTO test (c0) VALUES (101);"
    [ "$status" -eq 0 ]
    run dolt sql -q "INSERT INTO test VALUES (2,2);"
    [ "$status" -eq 0 ]
    run dolt sql -q "SELECT * FROM test;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "100,100" ]] || false
    [[ "$output" =~ "101,101" ]] || false
}

@test "insert into auto_increment table with NULL" {
    dolt sql -q "INSERT INTO test VALUES (1,1);"

    run dolt sql -q "INSERT INTO test (pk,c0) VALUES (NULL,2);"
    [ "$status" -eq 0 ]
    run dolt sql -q "SELECT * FROM test WHERE c0 > 1;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "2,2" ]] || false

    run dolt sql -q "INSERT INTO test VALUES (NULL,3), (10,10), (NULL,11);"
    [ "$status" -eq 0 ]
    run dolt sql -q "SELECT * FROM test WHERE c0 > 2;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "3,3" ]] || false
    [[ "$output" =~ "10,10" ]] || false
    [[ "$output" =~ "11,11" ]] || false
}

@test "insert into auto_increment table with 0" {
    dolt sql -q "INSERT INTO test VALUES (1,1);"

    run dolt sql -q "INSERT INTO test (pk,c0) VALUES (0,2);"
    [ "$status" -eq 0 ]
    run dolt sql -q "SELECT * FROM test WHERE c0 > 1;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "2,2" ]] || false

    run dolt sql -q "INSERT INTO test VALUES (0,3), (10,10), (0,11);"
    [ "$status" -eq 0 ]
    run dolt sql -q "SELECT * FROM test WHERE c0 > 2;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "3,3" ]] || false
    [[ "$output" =~ "10,10" ]] || false
    [[ "$output" =~ "11,11" ]] || false
}

@test "insert into auto_increment table via batch mode" {
    run dolt sql <<SQL
INSERT INTO test (c0) VALUES (1);
INSERT INTO test (c0) VALUES (2);
INSERT INTO test (pk,c0) VALUES (19,19);
INSERT INTO test (c0) VALUES (20);
INSERT INTO test (c0) VALUES (21);
SQL
    [ "$status" -eq 0 ]

    run dolt sql -q "SELECT * FROM test;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "19,19" ]] || false
    [[ "$output" =~ "20,20" ]] || false
    [[ "$output" =~ "21,21" ]] || false
}

@test "insert into table via batch mode" {
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

    run dolt sql -q "select * from test2 order by pk" -r csv
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

@test "insert into auto_increment table with correct floating point rounding" {
    dolt sql <<SQL
CREATE TABLE auto_float (
    pk float NOT NULL PRIMARY KEY AUTO_INCREMENT,
    c0 int
);
SQL

    dolt sql -q "INSERT INTO auto_float (c0) VALUES (1);"
    dolt sql -q "INSERT INTO auto_float (pk, c0) VALUES (2.1,2);"
    dolt sql -q "INSERT INTO auto_float (c0) VALUES (3);"
    dolt sql -q "INSERT INTO auto_float (pk, c0) VALUES (3.9,4);"
    dolt sql -q "INSERT INTO auto_float (c0) VALUES (5);"

    run dolt sql -q "SELECT * FROM auto_float;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2.1,2" ]] || false
    [[ "$output" =~ "3,3" ]] || false
    [[ "$output" =~ "3.9,4" ]] || false
    [[ "$output" =~ "5,5" ]] || false
}

@test "create auto_increment tables with all numeric types" {
    # signed integer types and floating point
    for TYPE in TINYINT SMALLINT MEDIUMINT INT BIGINT FLOAT DOUBLE
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
        run dolt sql -q "SELECT * FROM auto_$TYPE WHERE c0 > 1;" -r csv
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
        run dolt sql -q "SELECT * FROM auto2_$TYPE WHERE c0 > 1;" -r csv
        [ "$status" -eq 0 ]
        [[ "$output" =~ "2,2" ]] || false
    done
}

@test "invalid AUTO_INCREMENT definitions fail" {
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