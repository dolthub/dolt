#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}

@test "types: BIGINT" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  v BIGINT COMMENT 'tag:1',
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` BIGINT COMMENT 'tag:1'" ]] || false
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
}

@test "types: BIGINT UNSIGNED" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  v BIGINT UNSIGNED COMMENT 'tag:1',
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` BIGINT UNSIGNED COMMENT 'tag:1'" ]] || false
    dolt sql -q "INSERT INTO test VALUES (1, 9223372036854775807);"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 9223372036854775807 " ]] || false
    skip "We can't parse values above the INT64 max in go-mysql-server yet"
    dolt sql -q "UPDATE test SET v=v*2+1 WHERE pk=1;"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 18446744073709551615 " ]] || false
    run dolt sql -q "INSERT INTO test VALUES (2, 40000000000000000000);"
    [ "$status" -eq "1" ]
    run dolt sql -q "INSERT INTO test VALUES (2, -1);"
    [ "$status" -eq "1" ]
}

@test "types: BINARY(10)" {
    skip "This is not yet persisted in dolt"
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  v BINARY(10) COMMENT 'tag:1',
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` BINARY(10) COMMENT 'tag:1'" ]] || false
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
}

@test "types: BIT(10)" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  v BIT(10) COMMENT 'tag:1',
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` BIT(10) COMMENT 'tag:1'" ]] || false
    dolt sql -q "INSERT INTO test VALUES (1, 511);"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 511 " ]] || false
    dolt sql -q "UPDATE test SET v=v*2+1 WHERE pk=1;"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 1023 " ]] || false
    run dolt sql -q "INSERT INTO test VALUES (2, 1024);"
    [ "$status" -eq "1" ]
    run dolt sql -q "INSERT INTO test VALUES (2, -1);"
    [ "$status" -eq "1" ]
}

@test "types: BLOB" {
    skip "This is not yet persisted in dolt"
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  v BLOB COMMENT 'tag:1',
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` BLOB COMMENT 'tag:1'" ]] || false
    dolt sql -q "INSERT INTO test VALUES (1, 'abcdefg');"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " abcdefg " ]] || false
    dolt sql -q "UPDATE test SET v='1234567890' WHERE pk=1;"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 1234567890 " ]] || false
}

@test "types: BOOLEAN" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  v BOOLEAN COMMENT 'tag:1',
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` TINYINT COMMENT 'tag:1'" ]] || false
    dolt sql -q "INSERT INTO test VALUES (1, true);"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 1 " ]] || false
    dolt sql -q "REPLACE INTO test VALUES (1, false);"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 0 " ]] || false
}

@test "types: CHAR(10)" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  v CHAR(10) COMMENT 'tag:1',
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` CHAR(10) COMMENT 'tag:1'" ]] || false
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
}

@test "types: DATE" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  v DATE COMMENT 'tag:1',
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` DATE COMMENT 'tag:1'" ]] || false
    dolt sql -q "INSERT INTO test VALUES (1, '2020-02-10 11:12:13.456789');"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 2020-02-10 00:00:00 +0000 UTC " ]] || false
    dolt sql -q "REPLACE INTO test VALUES (1, '1000-01-01 00:00:00');"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 1000-01-01 00:00:00 +0000 UTC " ]] || false
    dolt sql -q "REPLACE INTO test VALUES (1, '9999-01-01 23:59:59.999999');"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 9999-01-01 00:00:00 +0000 UTC " ]] || false
    run dolt sql -q "INSERT INTO test VALUES (2, '999-01-01 00:00:00');"
    [ "$status" -eq "1" ]
    run dolt sql -q "INSERT INTO test VALUES (2, '10000-01-01 00:00:00');"
    [ "$status" -eq "1" ]
}

@test "types: DATETIME" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  v DATETIME COMMENT 'tag:1',
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` DATETIME COMMENT 'tag:1'" ]] || false
    dolt sql -q "INSERT INTO test VALUES (1, '2020-02-10 11:12:13.456789');"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 2020-02-10 11:12:13.456789 +0000 UTC " ]] || false
    dolt sql -q "REPLACE INTO test VALUES (1, '1000-01-01 00:00:00');"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 1000-01-01 00:00:00 +0000 UTC " ]] || false
    dolt sql -q "REPLACE INTO test VALUES (1, '9999-01-01 23:59:59.999999');"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 9999-01-01 23:59:59.999999 +0000 UTC " ]] || false
    run dolt sql -q "INSERT INTO test VALUES (2, '999-01-01 00:00:00');"
    [ "$status" -eq "1" ]
    run dolt sql -q "INSERT INTO test VALUES (2, '10000-01-01 00:00:00');"
    [ "$status" -eq "1" ]
}

@test "types: DECIMAL" {
    skip "This is not yet persisted in dolt"
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  v DECIMAL COMMENT 'tag:1',
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` DECIMAL(10, 0) COMMENT 'tag:1'" ]] || false
}

@test "types: DECIMAL(9)" {
    skip "This is not yet persisted in dolt"
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  v DECIMAL(9) COMMENT 'tag:1',
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` DECIMAL(9, 0) COMMENT 'tag:1'" ]] || false
}

@test "types: DECIMAL(9, 5)" {
    skip "This is not yet persisted in dolt"
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  v DECIMAL(10, 5) COMMENT 'tag:1',
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` DECIMAL(10, 5) COMMENT 'tag:1'" ]] || false
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
}

@test "types: DOUBLE" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  v DOUBLE COMMENT 'tag:1',
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` DOUBLE COMMENT 'tag:1'" ]] || false
    dolt sql -q "INSERT INTO test VALUES (1, 1.25);"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 1.25 " ]] || false
    skip "We can't parse large float values in go-mysql-server yet"
    dolt sql -q "REPLACE INTO test VALUES (1, 8.988465674311578540726371186585217839905e+307);"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 8.988465674311578540726371186585217839905e+307 " ]] || false
    dolt sql -q "UPDATE test SET v=v*2 WHERE pk=1;"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 1.797693134862315708145274237317043567981e+308 " ]] || false
    run dolt sql -q "INSERT INTO test VALUES (2, 3.5953862697246314162905484746340871359614113505168999e+308);"
    [ "$status" -eq "1" ]
    run dolt sql -q "INSERT INTO test VALUES (2, -3.5953862697246314162905484746340871359614113505168999e+308);"
    [ "$status" -eq "1" ]
}

@test "types: ENUM('a','b','c')" {
    skip "This is not yet persisted in dolt"
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  v ENUM('a','b','c') COMMENT 'tag:1',
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` ENUM('a','b','c') COMMENT 'tag:1'" ]] || false
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
}

@test "types: FLOAT" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  v FLOAT COMMENT 'tag:1',
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` FLOAT COMMENT 'tag:1'" ]] || false
    dolt sql -q "INSERT INTO test VALUES (1, 1.25);"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 1.25 " ]] || false
    skip "We can't parse large float values in go-mysql-server yet"
    dolt sql -q "REPLACE INTO test VALUES (1, 170141173319264429905852091742258462720);"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 170141173319264429905852091742258462720 " ]] || false
    dolt sql -q "UPDATE test SET v=v*2 WHERE pk=1;"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 340282346638528859811704183484516925440 " ]] || false
    run dolt sql -q "INSERT INTO test VALUES (2, 680564693277057719623408366969033850880);"
    [ "$status" -eq "1" ]
    run dolt sql -q "INSERT INTO test VALUES (2, -680564693277057719623408366969033850880);"
    [ "$status" -eq "1" ]
}

@test "types: INT" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  v INT COMMENT 'tag:1',
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` INT COMMENT 'tag:1'" ]] || false
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
}

@test "types: INT UNSIGNED" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  v INT UNSIGNED COMMENT 'tag:1',
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` INT UNSIGNED COMMENT 'tag:1'" ]] || false
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
}

@test "types: INTEGER" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  v INTEGER COMMENT 'tag:1',
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` INT COMMENT 'tag:1'" ]] || false
}

@test "types: INTEGER UNSIGNED" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  v INTEGER UNSIGNED COMMENT 'tag:1',
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` INT UNSIGNED COMMENT 'tag:1'" ]] || false
}

@test "types: LONGBLOB" {
    skip "This is not yet persisted in dolt"
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  v LONGBLOB COMMENT 'tag:1',
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` LONGBLOB COMMENT 'tag:1'" ]] || false
    dolt sql -q "INSERT INTO test VALUES (1, 'abcdefg');"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " abcdefg " ]] || false
    dolt sql -q "UPDATE test SET v='1234567890' WHERE pk=1;"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 1234567890 " ]] || false
}

@test "types: LONGTEXT" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  v LONGTEXT COMMENT 'tag:1',
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` LONGTEXT COMMENT 'tag:1'" ]] || false
    dolt sql -q "INSERT INTO test VALUES (1, 'abcdefg');"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " abcdefg " ]] || false
    dolt sql -q "UPDATE test SET v='1234567890' WHERE pk=1;"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 1234567890 " ]] || false
}

@test "types: MEDIUMBLOB" {
    skip "This is not yet persisted in dolt"
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  v MEDIUMBLOB COMMENT 'tag:1',
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` MEDIUMBLOB COMMENT 'tag:1'" ]] || false
    dolt sql -q "INSERT INTO test VALUES (1, 'abcdefg');"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " abcdefg " ]] || false
    dolt sql -q "UPDATE test SET v='1234567890' WHERE pk=1;"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 1234567890 " ]] || false
}

@test "types: MEDIUMINT" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  v MEDIUMINT COMMENT 'tag:1',
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` MEDIUMINT COMMENT 'tag:1'" ]] || false
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
}

@test "types: MEDIUMINT UNSIGNED" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  v MEDIUMINT UNSIGNED COMMENT 'tag:1',
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` MEDIUMINT UNSIGNED COMMENT 'tag:1'" ]] || false
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
}

@test "types: MEDIUMTEXT" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  v MEDIUMTEXT COMMENT 'tag:1',
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` MEDIUMTEXT COMMENT 'tag:1'" ]] || false
    dolt sql -q "INSERT INTO test VALUES (1, 'abcdefg');"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " abcdefg " ]] || false
    dolt sql -q "UPDATE test SET v='1234567890' WHERE pk=1;"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 1234567890 " ]] || false
}

@test "types: REAL" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  v REAL COMMENT 'tag:1',
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` DOUBLE COMMENT 'tag:1'" ]] || false
}

@test "types: SET('a','b','c')" {
    skip "This is not yet persisted in dolt"
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  v SET('a','b','c') COMMENT 'tag:1',
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` SET('a','b','c') COMMENT 'tag:1'" ]] || false
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
}

@test "types: SMALLINT" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  v SMALLINT COMMENT 'tag:1',
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` SMALLINT COMMENT 'tag:1'" ]] || false
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
}

@test "types: SMALLINT UNSIGNED" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  v SMALLINT UNSIGNED COMMENT 'tag:1',
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` SMALLINT UNSIGNED COMMENT 'tag:1'" ]] || false
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
}

@test "types: TEXT" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  v TEXT COMMENT 'tag:1',
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` TEXT COMMENT 'tag:1'" ]] || false
    dolt sql -q "INSERT INTO test VALUES (1, 'abcdefg');"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " abcdefg " ]] || false
    dolt sql -q "UPDATE test SET v='1234567890' WHERE pk=1;"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 1234567890 " ]] || false
}

@test "types: TIME" {
    skip "This is not yet persisted in dolt"
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  v TIME COMMENT 'tag:1',
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` TIME COMMENT 'tag:1'" ]] || false
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
}

@test "types: TIMESTAMP" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  v TIMESTAMP COMMENT 'tag:1',
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` TIMESTAMP COMMENT 'tag:1'" ]] || false
    dolt sql -q "INSERT INTO test VALUES (1, '2020-02-10 11:12:13.456789');"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 2020-02-10 11:12:13.456789 +0000 UTC " ]] || false
    dolt sql -q "REPLACE INTO test VALUES (1, '1970-01-01 00:00:01');"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 1970-01-01 00:00:01 +0000 UTC " ]] || false
    dolt sql -q "REPLACE INTO test VALUES (1, '2038-01-19 03:14:07.999999');"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 2038-01-19 03:14:07.999999 +0000 UTC " ]] || false
    run dolt sql -q "INSERT INTO test VALUES (2, '1970-01-01 00:00:00');"
    [ "$status" -eq "1" ]
    run dolt sql -q "INSERT INTO test VALUES (2, '2038-01-19 03:14:08');"
    [ "$status" -eq "1" ]
}

@test "types: TINYBLOB" {
    skip "This is not yet persisted in dolt"
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  v TINYBLOB COMMENT 'tag:1',
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` TINYBLOB COMMENT 'tag:1'" ]] || false
    dolt sql -q "INSERT INTO test VALUES (1, 'abcdefg');"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " abcdefg " ]] || false
    dolt sql -q "UPDATE test SET v='1234567890' WHERE pk=1;"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 1234567890 " ]] || false
}

@test "types: TINYINT" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  v TINYINT COMMENT 'tag:1',
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` TINYINT COMMENT 'tag:1'" ]] || false
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
}

@test "types: TINYINT UNSIGNED" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  v TINYINT UNSIGNED COMMENT 'tag:1',
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` TINYINT UNSIGNED COMMENT 'tag:1'" ]] || false
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
}

@test "types: TINYTEXT" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  v TINYTEXT COMMENT 'tag:1',
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` TINYTEXT COMMENT 'tag:1'" ]] || false
    dolt sql -q "INSERT INTO test VALUES (1, 'abcdefg');"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " abcdefg " ]] || false
    dolt sql -q "UPDATE test SET v='1234567890' WHERE pk=1;"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq "0" ]
    [[ "${lines[3]}" =~ " 1234567890 " ]] || false
}

@test "types: VARBINARY(10)" {
    skip "This is not yet persisted in dolt"
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  v VARBINARY(10) COMMENT 'tag:1',
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` VARBINARY(10) COMMENT 'tag:1'" ]] || false
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
}

@test "types: VARCHAR(10)" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  v VARCHAR(10) COMMENT 'tag:1',
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` VARCHAR(10) COMMENT 'tag:1'" ]] || false
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
}

@test "types: VARCHAR(10) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  v VARCHAR(10) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin COMMENT 'tag:1',
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` VARCHAR(10) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin COMMENT 'tag:1'" ]] || false
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
}

@test "types: YEAR" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  v YEAR COMMENT 'tag:1',
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v\` YEAR COMMENT 'tag:1'" ]] || false
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
}