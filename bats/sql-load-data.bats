#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "simple load data from file into table" {
    cat <<DELIM > 1pk5col-ints.csv
pk||c1||c2||c3||c4||c5
0||1||2||3||4||5
1||1||2||3||4||5
DELIM

    run dolt sql << SQL
SET secure_file_priv='./';
CREATE TABLE test(pk int primary key, c1 int, c2 int, c3 int, c4 int, c5 int);
LOAD DATA INFILE '1pk5col-ints.csv' INTO TABLE test CHARACTER SET UTF8MB4 FIELDS TERMINATED BY '||' ESCAPED BY '' LINES TERMINATED BY '\n' IGNORE 1 LINES;
SQL

    [ "$status" -eq 0 ]

    run dolt sql -r csv -q "select * from test"

    [ "$status" -eq 0 ]
    [ "${lines[0]}" = "pk,c1,c2,c3,c4,c5" ]
    [ "${lines[1]}" = "0,1,2,3,4,5" ]
    [ "${lines[2]}" = "1,1,2,3,4,5" ]
}

@test "load data into unknown table throws error" {
    run dolt sql << SQL
SET secure_file_priv='./';
LOAD DATA INFILE '1pk5col-ints.csv' INTO TABLE test CHARACTER SET UTF8MB4 FIELDS TERMINATED BY '||' ESCAPED BY '' LINES TERMINATED BY '\n' IGNORE 1 LINES;
SQL

    [ "$status" -eq 1 ]
    [[ "$output" =~  "table not found: test" ]] || false
}

@test "load data without unknown file throws error" {
      run dolt sql << SQL
SET secure_file_priv='./';
CREATE TABLE test(pk int primary key, c1 int, c2 int, c3 int, c4 int, c5 int);
LOAD DATA INFILE 'hello-ints.csv' INTO TABLE test CHARACTER SET UTF8MB4 FIELDS TERMINATED BY '||' ESCAPED BY '' LINES TERMINATED BY '\n' IGNORE 1 LINES;
SQL

    [ "$status" -eq 1 ]
    [[ "$output" =~  "no such file or directory" ]] || false
}

@test "load data works with enclosed terms" {
    cat <<DELIM > 1pk5col-ints.csv
pk||c1||c2||c3||c4||c5
"0"||"1"||"2"||"3"||"4"||"5"
"1"||"1"||"2"||"3"||"4"||"5"
DELIM

    run dolt sql << SQL
SET secure_file_priv='./';
CREATE TABLE test(pk int primary key, c1 int, c2 int, c3 int, c4 int, c5 int);
LOAD DATA INFILE '1pk5col-ints.csv' INTO TABLE test CHARACTER SET UTF8MB4 FIELDS TERMINATED BY '||' ENCLOSED BY '"'  ESCAPED BY '' LINES TERMINATED BY '\n' IGNORE 1 LINES;
SQL

    echo $output
    [ "$status" -eq 0 ]

    run dolt sql -r csv -q "select * from test"

    [ "$status" -eq 0 ]
    [ "${lines[0]}" = "pk,c1,c2,c3,c4,c5" ]
    [ "${lines[1]}" = "0,1,2,3,4,5" ]
    [ "${lines[2]}" = "1,1,2,3,4,5" ]
}


@test "load data works with prefixed terms" {
    cat <<DELIM > prefixed.txt
pk
sssHi
sssHello
ignore me
sssYo
DELIM

    run dolt sql << SQL
SET secure_file_priv='./';
CREATE TABLE test(pk longtext);
LOAD DATA INFILE 'prefixed.txt' INTO TABLE test CHARACTER SET UTF8MB4 LINES STARTING BY 'sss' IGNORE 1 LINES;
SQL

    echo $output
    [ "$status" -eq 0 ]

    run dolt sql -r csv -q "select * from test ORDER BY pk"

    [ "$status" -eq 0 ]
    [ "${lines[0]}" = "pk" ]
    [ "${lines[1]}" = "Hello" ]
    [ "${lines[2]}" = "Hi" ]
    [ "${lines[3]}" = "Yo" ]
}

@test "load data works when the number of input columns in the file is less than the number of schema columns" {
    cat <<DELIM > 1pk2col-ints.csv
pk,c1
0,1
1,1
DELIM

    run dolt sql << SQL
SET secure_file_priv='./';
CREATE TABLE test(pk int primary key, c1 int, c2 int);
LOAD DATA INFILE '1pk2col-ints.csv' INTO TABLE test FIELDS TERMINATED BY ',' IGNORE 1 LINES;
SQL

    echo $output
    [ "$status" -eq 0 ]

    run dolt sql -r csv -q "select * from test"

    [ "$status" -eq 0 ]
    [ "${lines[0]}" = "pk,c1,c2" ]
    [ "${lines[1]}" = "0,1," ]
    [ "${lines[2]}" = "1,1," ]
}

@test "load data works with fields separated by tabs" {
  skip "This needs to be fixed."

    cat <<DELIM > 1pk2col-ints.csv
pk  c1
0 1
1 1
DELIM

    run dolt sql << SQL
SET secure_file_priv='./';
CREATE TABLE test(pk int primary key, c1 int, c2 int);
LOAD DATA INFILE '1pk2col-ints.csv' INTO TABLE test FIELDS TERMINATED BY '\t' IGNORE 1 LINES;
SQL

    echo $output
    [ "$status" -eq 0 ]

    run dolt sql -r csv -q "select * from test"

    [ "$status" -eq 0 ]
    [ "${lines[0]}" = "pk,c1,c2" ]
    [ "${lines[1]}" = "0,1," ]
    [ "${lines[2]}" = "1,1," ]
}

@test "load data recognizes certain nulls" {
    cat <<DELIM > 1pk2col-ints.csv
pk
\N
NULL
DELIM

    run dolt sql << SQL
SET secure_file_priv='./';
CREATE TABLE test(pk longtext);
LOAD DATA INFILE '1pk2col-ints.csv' INTO TABLE test FIELDS IGNORE 1 LINES;
SQL

    [ "$status" -eq 0 ]

    run dolt sql -q "select COUNT(*) from test WHERE pk IS NULL"
    [ "$status" -eq 0 ]
    [[ "$output" =~  "2" ]] || false
}

@test "load data works when column order is mismatched" {
    skip "This needs to be fixed."

    cat <<DELIM > 1pk2col-ints.csv
pk,c1
"hi","1"
"hello","2"
DELIM

    run dolt sql << SQL
SET secure_file_priv='./';
CREATE TABLE test(pk int, c1 longtext);
LOAD DATA INFILE '1pk2col-ints.csv' INTO TABLE test FIELDS ENCLOSED BY '"' TERMINATED BY ',' IGNORE 1 LINES (c1,pk);
SQL

    [ "$status" -eq 0 ]

    run dolt sql -r csv -q "select * from test"

    [ "$status" -eq 0 ]
    [ "${lines[0]}" = "pk,c1" ]
    [ "${lines[1]}" = "1,hi" ]
    [ "${lines[2]}" = "2,hello" ]
}