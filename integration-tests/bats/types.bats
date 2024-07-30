#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "types: sql engine can parse all numeric data literals" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  i BIGINT,
  ui BIGINT UNSIGNED,
  f DOUBLE,
  PRIMARY KEY (pk)
);
SQL
    run dolt sql -q 'insert into test (pk,i) values (0,9223372036854775807);'
    [ "$status" -eq "0" ]
    skip "We can't parse values above the INT64 max in go-mysql-server yet"
    run dolt sql -q 'insert into test (pk,ui) values (1,18446744073709551615);'
    [ "$status" -eq "0" ]
    skip "We can't parse large float values in go-mysql-server yet"
    run dolt sql -q 'insert into test (pk,f) values (2, 8.988465674311578540726371186585217839905e+307);'
    [ "$status" -eq "0" ]
    run dolt sql -q 'insert into test (pk,f) values (3, 170141173319264429905852091742258462720);'
    [ "$status" -eq "0" ]
    TODO: DECIMAL literals
}

@test "types: BIGINT" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v BIGINT,
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` bigint" ]] || false
    dolt sql -q "INSERT INTO test VALUES (1, 4611686018427387903);"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 4611686018427387903 " ]] || false
    dolt sql -q "UPDATE test SET v=v*2+1 WHERE pk=1;"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 9223372036854775807 " ]] || false
    run dolt sql -q "INSERT INTO test VALUES (2, 40000000000000000000);"
    [ "$status" -eq "1" ]
    run dolt sql -q "INSERT INTO test VALUES (2, -40000000000000000000);"
    [ "$status" -eq "1" ]

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test';" -r csv
    [[ "$output" =~ 'test,pk,1,,NO,bigint,,,19,0,,,,bigint,PRI,"","insert,references,select,update","","",' ]] || false
    [[ "$output" =~ 'test,v,2,,YES,bigint,,,19,0,,,,bigint,"","","insert,references,select,update","","",' ]] || false
}

@test "types: BIGINT UNSIGNED" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v BIGINT UNSIGNED,
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` bigint unsigned" ]] || false
    cat <<DELIM > uint64-max.csv
pk,v
0, 18446744073709551615
DELIM
    run dolt table import -u test uint64-max.csv
    [ "$status" -eq "0" ]
    dolt sql -r csv -q "SELECT * FROM test"
    run dolt sql -r csv -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [ "${lines[1]}" = "0,18446744073709551615" ]

    cat <<DELIM > too-big.csv
pk,v
2,40000000000000000000
DELIM
    run dolt table import -u test too-big.csv
    [ "$status" -eq "1" ]
    run dolt sql -q "INSERT INTO test VALUES (2, -1);"
    [ "$status" -eq "1" ]

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS  where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,bigint,,,20,0,,,,bigint unsigned,"","","insert,references,select,update","","",' ]] || false
}

@test "types: BINARY(10)" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v BINARY(10),
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` binary(10)" ]] || false
    dolt sql -q "INSERT INTO test VALUES (1, 'abcdefg');"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " abcdefg " ]] || false
    dolt sql -q "UPDATE test SET v='1234567890' WHERE pk=1;"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 1234567890 " ]] || false
    run dolt sql -q "INSERT INTO test VALUES (2, '12345678901');"
    [ "$status" -eq "1" ]

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,binary,10,10,,,,,,binary(10),"","","insert,references,select,update","","",' ]] || false
}

@test "types: BIT(10)" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v BIT(10),
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` bit(10)" ]] || false
    dolt sql -q "INSERT INTO test VALUES (1, 511);"
    run dolt sql -q "SELECT pk, CONVERT(v, UNSIGNED) FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 511 " ]] || false
    dolt sql -q "UPDATE test SET v=v*2+1 WHERE pk=1;"
    run dolt sql -q "SELECT pk, CONVERT(v, UNSIGNED) FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 1023 " ]] || false
    run dolt sql -q "INSERT INTO test VALUES (2, 1024);"
    [ "$status" -eq "1" ]
    run dolt sql -q "INSERT INTO test VALUES (2, -1);"
    [ "$status" -eq "1" ]

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,bit,,,10,,,,,bit(10),"","","insert,references,select,update","","",' ]] || false
}

@test "types: BLOB" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v BLOB,
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` blob" ]] || false
    dolt sql -q "INSERT INTO test VALUES (1, 'abcdefg');"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " abcdefg " ]] || false
    dolt sql -q "UPDATE test SET v='1234567890' WHERE pk=1;"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 1234567890 " ]] || false

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,blob,65535,65535,,,,,,blob,"","","insert,references,select,update","","",' ]] || false
}

@test "types: BOOL" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v BOOL,
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` tinyint(1)" ]] || false

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,tinyint,,,3,0,,,,tinyint(1),"","","insert,references,select,update","","",' ]] || false
}

@test "types: BOOLEAN" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v BOOLEAN,
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` tinyint(1)" ]] || false
    dolt sql -q "INSERT INTO test VALUES (1, true);"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 1 " ]] || false
    dolt sql -q "REPLACE INTO test VALUES (1, false);"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 0 " ]] || false

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,tinyint,,,3,0,,,,tinyint(1),"","","insert,references,select,update","","",' ]] || false
}

@test "types: CHAR(10)" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v CHAR(10),
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` char(10)" ]] || false
    dolt sql -q "INSERT INTO test VALUES (1, 'abcdefg');"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " abcdefg " ]] || false
    dolt sql -q "UPDATE test SET v='1234567890' WHERE pk=1;"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 1234567890 " ]] || false
    run dolt sql -q "INSERT INTO test VALUES (2, '12345678901');"
    [ "$status" -eq "1" ]

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,char,10,40,,,,utf8mb4,utf8mb4_0900_bin,char(10),"","","insert,references,select,update","","",' ]] || false
}

@test "types: CHARACTER(10)" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v CHARACTER(10),
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` char(10)" ]] || false

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,char,10,40,,,,utf8mb4,utf8mb4_0900_bin,char(10),"","","insert,references,select,update","","",' ]] || false
}

@test "types: CHARACTER VARYING(10)" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v CHARACTER VARYING(10),
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` varchar(10)" ]] || false

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,varchar,10,40,,,,utf8mb4,utf8mb4_0900_bin,varchar(10),"","","insert,references,select,update","","",' ]] || false
}

@test "types: DATE" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v DATE,
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` date" ]] || false
    dolt sql -q "INSERT INTO test VALUES (1, '2020-02-10 11:12:13.456789');"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 2020-02-10 " ]] || false
    dolt sql -q "REPLACE INTO test VALUES (1, '1000-01-01 00:00:00');"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 1000-01-01 " ]] || false
    dolt sql -q "REPLACE INTO test VALUES (1, '1000-01-02');"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 1000-01-02 " ]] || false
    dolt sql -q "REPLACE INTO test VALUES (1, '1000-01-3');"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 1000-01-03 " ]] || false
    dolt sql -q "REPLACE INTO test VALUES (1, '1000-1-04');"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 1000-01-04 " ]] || false
    dolt sql -q "REPLACE INTO test VALUES (1, '1000-1-5');"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 1000-01-05 " ]] || false
    dolt sql -q "REPLACE INTO test VALUES (1, '9999-01-01 23:59:59.999999');"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 9999-01-01 " ]] || false
    run dolt sql -q "INSERT INTO test VALUES (2, '999-01-01 00:00:00');"
    [ "$status" -eq "1" ]
    run dolt sql -q "INSERT INTO test VALUES (2, '10000-01-01 00:00:00');"
    [ "$status" -eq "1" ]

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,date,,,,,0,,,date,"","","insert,references,select,update","","",' ]] || false
}

@test "types: DATETIME" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v DATETIME(6),
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` datetime(6)" ]] || false
    dolt sql -q "INSERT INTO test VALUES (1, '2020-02-10 11:12:13.456789');"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 2020-02-10 11:12:13.456789 " ]] || false
    dolt sql -q "REPLACE INTO test VALUES (1, '1000-01-01 00:00:00');"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 1000-01-01 00:00:00 " ]] || false
    dolt sql -q "REPLACE INTO test VALUES (1, '1000-01-02');"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 1000-01-02 00:00:00 " ]] || false
    dolt sql -q "REPLACE INTO test VALUES (1, '1000-01-3');"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 1000-01-03 00:00:00 " ]] || false
    dolt sql -q "REPLACE INTO test VALUES (1, '1000-1-04');"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 1000-01-04 00:00:00 " ]] || false
    dolt sql -q "REPLACE INTO test VALUES (1, '1000-1-5');"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 1000-01-05 00:00:00 " ]] || false
    dolt sql -q "REPLACE INTO test VALUES (1, '9999-01-01 23:59:59.999999');"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 9999-01-01 23:59:59.999999 " ]] || false
    run dolt sql -q "INSERT INTO test VALUES (2, '999-01-01 00:00:00');"
    [ "$status" -eq "1" ]
    run dolt sql -q "INSERT INTO test VALUES (2, '10000-01-01 00:00:00');"
    [ "$status" -eq "1" ]

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,datetime,,,,,6,,,datetime(6),"","","insert,references,select,update","","",' ]] || false

    dolt sql <<SQL
drop table test;
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v DATETIME,
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` datetime" ]] || false
    dolt sql -q "INSERT INTO test VALUES (1, '2020-02-10 11:12:13.456789');"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 2020-02-10 11:12:13 " ]] || false

}

@test "types: DEC" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v DEC,
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` decimal(10,0)" ]] || false

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,decimal,,,10,0,,,,"decimal(10,0)","","","insert,references,select,update","","",' ]] || false
}

@test "types: DEC(9)" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v DEC(9),
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` decimal(9,0)" ]] || false

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,decimal,,,9,0,,,,"decimal(9,0)","","","insert,references,select,update","","",' ]] || false
}

@test "types: DEC(9,5)" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v DEC(9,5),
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` decimal(9,5)" ]] || false

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,decimal,,,9,5,,,,"decimal(9,5)","","","insert,references,select,update","","",' ]] || false
}

@test "types: DECIMAL" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v DECIMAL,
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` decimal(10,0)" ]] || false

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,decimal,,,10,0,,,,"decimal(10,0)","","","insert,references,select,update","","",' ]] || false
}

@test "types: DECIMAL(9)" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v DECIMAL(9),
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` decimal(9,0)" ]] || false

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,decimal,,,9,0,,,,"decimal(9,0)","","","insert,references,select,update","","",' ]] || false
}

@test "types: DECIMAL(9,5)" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v DECIMAL(9,5),
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` decimal(9,5)" ]] || false
    dolt sql -q "INSERT INTO test VALUES (1, 1234.56789);"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 1234.56789 " ]] || false
    dolt sql -q "UPDATE test SET v=v*2 WHERE pk=1;"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 2469.13578 " ]] || false
    run dolt sql -q "INSERT INTO test VALUES (2, 10000);"
    [ "$status" -eq "1" ]

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,decimal,,,9,5,,,,"decimal(9,5)","","","insert,references,select,update","","",' ]] || false
}

@test "types: DOUBLE" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v DOUBLE,
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` double" ]] || false
    dolt sql -q "INSERT INTO test VALUES (0, 1.25);"
    run dolt sql -r csv -q "SELECT * FROM test WHERE pk=0"
    [ "$status" -eq "0" ]
    [ "${lines[1]}" = "0,1.25" ]
    cat <<DELIM > double.csv
pk,v
1,8.988465674311578540726371186585217839905e+307
DELIM
    run dolt table import -u test double.csv
    [ "$status" -eq "0" ]
    dolt sql -r csv -q "SELECT * FROM test WHERE pk=1"
    run dolt sql -r csv -q "SELECT * FROM test WHERE pk=1"
    [ "$status" -eq "0" ]
    [ "${lines[1]}" = "1,8.988465674311579e+307" ]
    cat <<DELIM > double.csv
pk,v
3,3.5953862697246314162905484746340871359614113505168999e+308
4,-3.5953862697246314162905484746340871359614113505168999e+308
DELIM
    run dolt table import -u test double.csv
    [ "$status" -ne "0" ]

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,double,,,22,,,,,double,"","","insert,references,select,update","","",' ]] || false
}

@test "types: DOUBLE PRECISION" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v DOUBLE PRECISION,
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` double" ]] || false

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,double,,,22,,,,,double,"","","insert,references,select,update","","",' ]] || false
}

@test "types: Double with precision and scale correctly gets interpreted as a decimal" {
    skip "Double with precision and scale parsing is incorrect"
    
    dolt sql -q "CREATE TABLE t(pk double(5, 1))"

    run dolt sql -q "INSERT INTO t values (33333.1)"
    [ "$status" -eq "0" ]

    run dolt sql -r csv -q "select * from t"
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk" ]] || false
    [[ "$output" =~ "33333.1" ]] || false

    # Should fail
    run dolt sql -q "INSERT INTO t values (23232.312321)"
    [ "$status" -eq "1" ]

    # Double with precision and scale should be interpreted as a decimal
    run dolt sql -r csv "select column_name, numeric_scale, numeric_precision from t"
    [ "$status" -eq "0" ]
    [[ "$output" =~ "column_name, numeric_scale, numeric_precision" ]] || false
    [[ "$output" =~ "pk,5,2" ]] || false

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,double,,,5,1,,,,double(5,1),"","","insert,references,select,update","","",' ]] || false
}

@test "types: ENUM('a','b','c')" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v ENUM('a','b','c'),
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` enum('a','b','c')" ]] || false
    dolt sql -q "INSERT INTO test VALUES (1, 'a');"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " a " ]] || false
    dolt sql -q "UPDATE test SET v=2 WHERE pk=1;"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " b " ]] || false
    run dolt sql -q "INSERT INTO test VALUES (2, 'd');"
    [ "$status" -eq "1" ]
    run dolt sql -q "INSERT INTO test VALUES (2, '');"
    [ "$status" -eq "1" ]

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ "test,v,2,,YES,enum,1,4,,,,utf8mb4,utf8mb4_0900_bin,\"enum('a','b','c')\",\"\",\"\",\"insert,references,select,update\",\"\",\"\"," ]] || false
}

@test "types: FIXED" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v FIXED,
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` decimal(10,0)" ]] || false

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,decimal,,,10,0,,,,"decimal(10,0)","","","insert,references,select,update","","",' ]] || false
}

@test "types: FIXED(9)" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v FIXED(9),
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` decimal(9,0)" ]] || false

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,decimal,,,9,0,,,,"decimal(9,0)","","","insert,references,select,update","","",' ]] || false
}

@test "types: FIXED(9,5)" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v FIXED(9,5),
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` decimal(9,5)" ]] || false

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,decimal,,,9,5,,,,"decimal(9,5)","","","insert,references,select,update","","",' ]] || false
}

@test "types: FLOAT" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v FLOAT,
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` float" ]] || false
    dolt sql -q "INSERT INTO test VALUES (0, 1.25);"
    run dolt sql -r csv -q "SELECT * FROM test WHERE pk=0"
    [ "$status" -eq "0" ]
    [ "${lines[1]}" = "0,1.25" ]
    cat <<DELIM > float.csv
pk,v
1,170141173319264429905852091742258462720
DELIM
    run dolt table import -u test float.csv
    [ "$status" -eq "0" ]
    run dolt sql -r csv -q "SELECT * FROM test WHERE pk=1"
    [ "$status" -eq "0" ]
    [ "${lines[1]}" = "1,1.7014117e+38" ]
    cat <<DELIM > float.csv
pk,v
2,340282346638528859811704183484516925440
DELIM
    run dolt table import -u test float.csv
    [ "$status" -eq "0" ]
     dolt sql -r csv -q "SELECT * FROM test WHERE pk=2"
    run dolt sql -r csv -q "SELECT * FROM test WHERE pk=2"
    [ "$status" -eq "0" ]
    [ "${lines[1]}" = "2,3.4028235e+38" ]
    cat <<DELIM > float.csv
pk,v
3,680564693277057719623408366969033850880
4,-680564693277057719623408366969033850880
DELIM
    run dolt table import -u test float.csv
    [ "$status" -ne "0" ]

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,float,,,12,,,,,float,"","","insert,references,select,update","","",' ]] || false
}

@test "types: INT" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v INT,
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` int" ]] || false
    dolt sql -q "INSERT INTO test VALUES (1, 1073741823);"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 1073741823 " ]] || false
    dolt sql -q "UPDATE test SET v=v*2+1 WHERE pk=1;"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 2147483647 " ]] || false
    run dolt sql -q "INSERT INTO test VALUES (2, 2147483648);"
    [ "$status" -eq "1" ]
    run dolt sql -q "INSERT INTO test VALUES (2, -2147483649);"
    [ "$status" -eq "1" ]

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,int,,,10,0,,,,int,"","","insert,references,select,update","","",' ]] || false
}

@test "types: INT UNSIGNED" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v INT UNSIGNED,
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` int unsigned" ]] || false
    dolt sql -q "INSERT INTO test VALUES (1, 2147483647);"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 2147483647 " ]] || false
    dolt sql -q "UPDATE test SET v=v*2+1 WHERE pk=1;"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 4294967295 " ]] || false
    run dolt sql -q "INSERT INTO test VALUES (2, 4294967296);"
    [ "$status" -eq "1" ]
    run dolt sql -q "INSERT INTO test VALUES (2, -1);"
    [ "$status" -eq "1" ]

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,int,,,10,0,,,,int unsigned,"","","insert,references,select,update","","",' ]] || false
}

@test "types: INTEGER" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v INTEGER,
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` int" ]] || false

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,int,,,10,0,,,,int,"","","insert,references,select,update","","",' ]] || false
}

@test "types: INTEGER UNSIGNED" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v INTEGER UNSIGNED,
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` int unsigned" ]] || false

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,int,,,10,0,,,,int unsigned,"","","insert,references,select,update","","",' ]] || false
}

@test "types: LONG" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v LONG,
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` mediumtext" ]] || false

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,mediumtext,4194303,16777215,,,,utf8mb4,utf8mb4_0900_bin,mediumtext,"","","insert,references,select,update","","",' ]] || false
}

@test "types: LONG VARCHAR" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v LONG VARCHAR,
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` mediumtext" ]] || false

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,mediumtext,4194303,16777215,,,,utf8mb4,utf8mb4_0900_bin,mediumtext,"","","insert,references,select,update","","",' ]] || false
}

@test "types: LONGBLOB" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v LONGBLOB,
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` longblob" ]] || false
    dolt sql -q "INSERT INTO test VALUES (1, 'abcdefg');"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " abcdefg " ]] || false
    dolt sql -q "UPDATE test SET v='1234567890' WHERE pk=1;"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 1234567890 " ]] || false

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,longblob,4294967295,4294967295,,,,,,longblob,"","","insert,references,select,update","","",' ]] || false
}

@test "types: LONGTEXT" {
    dolt sql <<"SQL"
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v LONGTEXT,
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` longtext" ]] || false
    dolt sql -q "INSERT INTO test VALUES (1, 'abcdefg');"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " abcdefg " ]] || false
    dolt sql -q "UPDATE test SET v='1234567890' WHERE pk=1;"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 1234567890 " ]] || false

    dolt sql <<"SQL"
DELETE FROM test;
INSERT INTO test VALUES (1, '1'), (2, '11'), (3, '1'), (4, '12'), (5, '01'), (6, '0');
SQL
    run dolt sql -q "SELECT * FROM test WHERE v > '0'" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v" ]] || false
    [[ "$output" =~ "5,01" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "3,1" ]] || false
    [[ "$output" =~ "2,11" ]] || false
    [[ "$output" =~ "4,12" ]] || false
    [[ "${#lines[@]}" = "6" ]] || false
    run dolt sql -q "SELECT * FROM test WHERE v <= '11'" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v" ]] || false
    [[ "$output" =~ "2,11" ]] || false
    [[ "$output" =~ "3,1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "5,01" ]] || false
    [[ "$output" =~ "6,0" ]] || false
    [[ "${#lines[@]}" = "6" ]] || false

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,longtext,1073741823,4294967295,,,,utf8mb4,utf8mb4_0900_bin,longtext,"","","insert,references,select,update","","",' ]] || false
}

@test "types: MEDIUMBLOB" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v MEDIUMBLOB,
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` mediumblob" ]] || false
    dolt sql -q "INSERT INTO test VALUES (1, 'abcdefg');"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " abcdefg " ]] || false
    dolt sql -q "UPDATE test SET v='1234567890' WHERE pk=1;"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 1234567890 " ]] || false

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,mediumblob,16777215,16777215,,,,,,mediumblob,"","","insert,references,select,update","","",' ]] || false
}

@test "types: MEDIUMINT" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v MEDIUMINT,
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` mediumint" ]] || false
    dolt sql -q "INSERT INTO test VALUES (1, 4194303);"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 4194303 " ]] || false
    dolt sql -q "UPDATE test SET v=v*2+1 WHERE pk=1;"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 8388607 " ]] || false
    run dolt sql -q "INSERT INTO test VALUES (2, 8388608);"
    [ "$status" -eq "1" ]
    run dolt sql -q "INSERT INTO test VALUES (2, -8388609);"
    [ "$status" -eq "1" ]

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,mediumint,,,7,0,,,,mediumint,"","","insert,references,select,update","","",' ]] || false
}

@test "types: MEDIUMINT UNSIGNED" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v MEDIUMINT UNSIGNED,
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` mediumint unsigned" ]] || false
    dolt sql -q "INSERT INTO test VALUES (1, 8388607);"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 8388607 " ]] || false
    dolt sql -q "UPDATE test SET v=v*2+1 WHERE pk=1;"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 16777215 " ]] || false
    run dolt sql -q "INSERT INTO test VALUES (2, 16777216);"
    [ "$status" -eq "1" ]
    run dolt sql -q "INSERT INTO test VALUES (2, -1);"
    [ "$status" -eq "1" ]

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,mediumint,,,7,0,,,,mediumint unsigned,"","","insert,references,select,update","","",' ]] || false
}

@test "types: MEDIUMTEXT" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v MEDIUMTEXT,
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` mediumtext" ]] || false
    dolt sql -q "INSERT INTO test VALUES (1, 'abcdefg');"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " abcdefg " ]] || false
    dolt sql -q "UPDATE test SET v='1234567890' WHERE pk=1;"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 1234567890 " ]] || false

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,mediumtext,4194303,16777215,,,,utf8mb4,utf8mb4_0900_bin,mediumtext,"","","insert,references,select,update","","",' ]] || false
}

@test "types: NATIONAL CHAR(10)" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v NATIONAL CHAR(10),
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` char(10) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci" ]] || false

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,char,10,30,,,,utf8mb3,utf8mb3_general_ci,char(10),"","","insert,references,select,update","","",' ]] || false
}

@test "types: NATIONAL CHARACTER(10)" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v NATIONAL CHARACTER(10),
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` char(10) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci" ]] || false

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,char,10,30,,,,utf8mb3,utf8mb3_general_ci,char(10),"","","insert,references,select,update","","",' ]] || false
}

@test "types: NATIONAL CHARACTER VARYING(10)" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v NATIONAL CHARACTER VARYING(10),
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` varchar(10) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci" ]] || false

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,varchar,10,30,,,,utf8mb3,utf8mb3_general_ci,varchar(10),"","","insert,references,select,update","","",' ]] || false
}

@test "types: NATIONAL VARCHAR(10)" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v NATIONAL VARCHAR(10),
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` varchar(10) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci" ]] || false

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,varchar,10,30,,,,utf8mb3,utf8mb3_general_ci,varchar(10),"","","insert,references,select,update","","",' ]] || false
}

@test "types: NCHAR(10)" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v NCHAR(10),
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` char(10) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci" ]] || false

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,char,10,30,,,,utf8mb3,utf8mb3_general_ci,char(10),"","","insert,references,select,update","","",' ]] || false
}

@test "types: NVARCHAR(10)" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v NVARCHAR(10),
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` varchar(10) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci" ]] || false

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,varchar,10,30,,,,utf8mb3,utf8mb3_general_ci,varchar(10),"","","insert,references,select,update","","",' ]] || false
}

@test "types: NUMERIC" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v NUMERIC,
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` decimal(10,0)" ]] || false

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,decimal,,,10,0,,,,"decimal(10,0)","","","insert,references,select,update","","",' ]] || false
}

@test "types: NUMERIC(9)" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v NUMERIC(9),
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` decimal(9,0)" ]] || false

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,decimal,,,9,0,,,,"decimal(9,0)","","","insert,references,select,update","","",' ]] || false
}

@test "types: NUMERIC(9,5)" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v NUMERIC(9,5),
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` decimal(9,5)" ]] || false

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,decimal,,,9,5,,,,"decimal(9,5)","","","insert,references,select,update","","",' ]] || false
}

@test "types: REAL" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v REAL,
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` double" ]] || false

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,double,,,22,,,,,double,"","","insert,references,select,update","","",' ]] || false
}

@test "types: SET('a','b','c')" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v SET('a','b','c'),
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` set('a','b','c')" ]] || false
    dolt sql -q "INSERT INTO test VALUES (1, 'b,a');"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " a,b " ]] || false
    dolt sql -q "UPDATE test SET v='b,a,c,c' WHERE pk=1;"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " a,b,c " ]] || false
    dolt sql -q "REPLACE INTO test VALUES (1, '');"
    run dolt sql -q "INSERT INTO test VALUES (2, 'd');"
    [ "$status" -eq "1" ]
    run dolt sql -q "INSERT INTO test VALUES (2, 'a,d');"
    [ "$status" -eq "1" ]

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ "test,v,2,,YES,set,5,20,,,,utf8mb4,utf8mb4_0900_bin,\"set('a','b','c')\",\"\",\"\",\"insert,references,select,update\",\"\",\"\"," ]] || false
}

@test "types: SMALLINT" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v SMALLINT,
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` smallint" ]] || false
    dolt sql -q "INSERT INTO test VALUES (1, 16383);"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 16383 " ]] || false
    dolt sql -q "UPDATE test SET v=v*2+1 WHERE pk=1;"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 32767 " ]] || false
    run dolt sql -q "INSERT INTO test VALUES (2, 32768);"
    [ "$status" -eq "1" ]
    run dolt sql -q "INSERT INTO test VALUES (2, -32769);"
    [ "$status" -eq "1" ]

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,smallint,,,5,0,,,,smallint,"","","insert,references,select,update","","",' ]] || false
}

@test "types: SMALLINT UNSIGNED" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v SMALLINT UNSIGNED,
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` smallint unsigned" ]] || false
    dolt sql -q "INSERT INTO test VALUES (1, 32767);"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 32767 " ]] || false
    dolt sql -q "UPDATE test SET v=v*2+1 WHERE pk=1;"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 65535 " ]] || false
    run dolt sql -q "INSERT INTO test VALUES (2, 65536);"
    [ "$status" -eq "1" ]
    run dolt sql -q "INSERT INTO test VALUES (2, -1);"
    [ "$status" -eq "1" ]

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,smallint,,,5,0,,,,smallint unsigned,"","","insert,references,select,update","","",' ]] || false
}

@test "types: TEXT" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v TEXT,
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` text" ]] || false
    dolt sql -q "INSERT INTO test VALUES (1, 'abcdefg');"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " abcdefg " ]] || false
    dolt sql -q "UPDATE test SET v='1234567890' WHERE pk=1;"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 1234567890 " ]] || false

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,text,16383,65535,,,,utf8mb4,utf8mb4_0900_bin,text,"","","insert,references,select,update","","",' ]] || false
}

@test "types: TIME" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v TIME,
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` time" ]] || false
    dolt sql -q "INSERT INTO test VALUES (1, '11:22:33.444444');"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 11:22:33.444444 " ]] || false
    dolt sql -q "UPDATE test SET v='11:22' WHERE pk=1;"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 11:22:00 " ]] || false
    dolt sql -q "REPLACE INTO test VALUES (1, '850:00:00');"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 838:59:59 " ]] || false
    dolt sql -q "REPLACE INTO test VALUES (1, '-850:00:00');"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " -838:59:59 " ]] || false

    # check information_schema.COLUMNS table
    # TODO: time precision is not supported, this type's 'datetime_precision' and 'column_type' should be '0', 'time' respectively.
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,time,,,,,6,,,time(6),"","","insert,references,select,update","","",' ]] || false
}

@test "types: TIMESTAMP" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v TIMESTAMP(6),
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` timestamp" ]] || false
    dolt sql -q "INSERT INTO test VALUES (1, '2020-02-10 11:12:13.456789');"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 2020-02-10 11:12:13.456789 " ]] || false
    dolt sql -q "REPLACE INTO test VALUES (1, '1970-01-01 00:00:01');"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 1970-01-01 00:00:01 " ]] || false
    dolt sql -q "REPLACE INTO test VALUES (1, '2038-01-19 03:14:07.999999');"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 2038-01-19 03:14:07.999999 " ]] || false
    run dolt sql -q "INSERT INTO test VALUES (2, '1970-01-01 00:00:00');"
    [ "$status" -eq "1" ]
    run dolt sql -q "INSERT INTO test VALUES (2, '2038-01-19 03:14:08');"
    [ "$status" -eq "1" ]

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,timestamp,,,,,0,,,timestamp(6),"","","insert,references,select,update","","",' ]] || false

    dolt sql <<SQL
drop table test;
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v TIMESTAMP,
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` timestamp" ]] || false
    dolt sql -q "INSERT INTO test VALUES (1, '2020-02-10 11:12:13.456789');"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 2020-02-10 11:12:13 " ]] || false
}

@test "types: TINYBLOB" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v TINYBLOB,
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` tinyblob" ]] || false
    dolt sql -q "INSERT INTO test VALUES (1, 'abcdefg');"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " abcdefg " ]] || false
    dolt sql -q "UPDATE test SET v='1234567890' WHERE pk=1;"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 1234567890 " ]] || false

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,tinyblob,255,255,,,,,,tinyblob,"","","insert,references,select,update","","",' ]] || false
}

@test "types: TINYINT" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v TINYINT,
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` tinyint" ]] || false
    dolt sql -q "INSERT INTO test VALUES (1, 63);"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 63 " ]] || false
    dolt sql -q "UPDATE test SET v=v*2+1 WHERE pk=1;"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 127 " ]] || false
    run dolt sql -q "INSERT INTO test VALUES (2, 128);"
    [ "$status" -eq "1" ]
    run dolt sql -q "INSERT INTO test VALUES (2, -129);"
    [ "$status" -eq "1" ]

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,tinyint,,,3,0,,,,tinyint,"","","insert,references,select,update","","",' ]] || false
}

@test "types: TINYINT UNSIGNED" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v TINYINT UNSIGNED,
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` tinyint unsigned" ]] || false
    dolt sql -q "INSERT INTO test VALUES (1, 127);"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 127 " ]] || false
    dolt sql -q "UPDATE test SET v=v*2+1 WHERE pk=1;"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 255 " ]] || false
    run dolt sql -q "INSERT INTO test VALUES (2, 256);"
    [ "$status" -eq "1" ]
    run dolt sql -q "INSERT INTO test VALUES (2, -1);"
    [ "$status" -eq "1" ]

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,tinyint,,,3,0,,,,tinyint unsigned,"","","insert,references,select,update","","",' ]] || false
}

@test "types: TINYTEXT" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v TINYTEXT,
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` tinytext" ]] || false
    dolt sql -q "INSERT INTO test VALUES (1, 'abcdefg');"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " abcdefg " ]] || false
    dolt sql -q "UPDATE test SET v='1234567890' WHERE pk=1;"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 1234567890 " ]] || false

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,tinytext,63,255,,,,utf8mb4,utf8mb4_0900_bin,tinytext,"","","insert,references,select,update","","",' ]] || false
}

@test "types: VARBINARY(10)" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v VARBINARY(10),
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` varbinary(10)" ]] || false
    dolt sql -q "INSERT INTO test VALUES (1, 'abcdefg');"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " abcdefg " ]] || false
    dolt sql -q "UPDATE test SET v='1234567890' WHERE pk=1;"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 1234567890 " ]] || false
    run dolt sql -q "INSERT INTO test VALUES (2, '12345678901');"
    [ "$status" -eq "1" ]

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,varbinary,10,10,,,,,,varbinary(10),"","","insert,references,select,update","","",' ]] || false
}

@test "types: VARCHAR(10)" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v VARCHAR(10),
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` varchar(10)" ]] || false
    dolt sql -q "INSERT INTO test VALUES (1, 'abcdefg');"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " abcdefg " ]] || false
    dolt sql -q "UPDATE test SET v='1234567890' WHERE pk=1;"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 1234567890 " ]] || false
    run dolt sql -q "INSERT INTO test VALUES (2, '12345678901');"
    [ "$status" -eq "1" ]

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,varchar,10,40,,,,utf8mb4,utf8mb4_0900_bin,varchar(10),"","","insert,references,select,update","","",' ]] || false
}

@test "types: VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v VARCHAR(10) CHARACTER SET utf32 COLLATE utf32_general_ci,
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` varchar(10) CHARACTER SET utf32 COLLATE utf32_general_ci" ]] || false
    dolt sql -q "INSERT INTO test VALUES (1, 'abcdefg');"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " abcdefg " ]] || false
    dolt sql -q "UPDATE test SET v='1234567890' WHERE pk=1;"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 1234567890 " ]] || false
    run dolt sql -q "INSERT INTO test VALUES (2, '12345678901');"
    [ "$status" -eq "1" ]

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,varchar,10,40,,,,utf32,utf32_general_ci,varchar(10),"","","insert,references,select,update","","",' ]] || false
}

@test "types: YEAR" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  v YEAR,
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` year" ]] || false
    dolt sql -q "INSERT INTO test VALUES (1, 1901);"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 1901 " ]] || false
    dolt sql -q "UPDATE test SET v='2155' WHERE pk=1;"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 2155 " ]] || false
    run dolt sql -q "INSERT INTO test VALUES (2, 1900);"
    [ "$status" -eq "1" ]
    run dolt sql -q "INSERT INTO test VALUES (2, '2156');"
    [ "$status" -eq "1" ]

    # check information_schema.COLUMNS table
    run dolt sql -q "select * from information_schema.COLUMNS where table_name = 'test' and column_name = 'v';" -r csv
    [[ "$output" =~ 'test,v,2,,YES,year,,,,,,,,year,"","","insert,references,select,update","","",' ]] || false
}
