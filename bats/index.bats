#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    dolt sql <<SQL
CREATE TABLE onepk (
  pk1 BIGINT PRIMARY KEY,
  v1 BIGINT,
  v2 BIGINT
);
CREATE TABLE twopk (
  pk1 BIGINT,
  pk2 BIGINT,
  v1 BIGINT,
  v2 BIGINT,
  PRIMARY KEY(pk1, pk2)
);
SQL
}

teardown() {
    teardown_common
}

@test "index: CREATE TABLE INDEX" {
    dolt sql <<SQL
CREATE TABLE test(
  pk BIGINT PRIMARY KEY,
  v1 BIGINT,
  v2 BIGINT,
  INDEX (v1)
);
SQL
    run dolt index ls test
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v1(v1)" ]] || false
    run dolt schema show test
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'KEY `v1` (`v1`)' ]] || false
}

@test "index: CREATE TABLE UNIQUE KEY" {
    dolt sql <<SQL
CREATE TABLE test(
  pk BIGINT PRIMARY KEY,
  v1 BIGINT,
  v2 BIGINT,
  UNIQUE KEY (v1)
);
CREATE TABLE test2(
  pk BIGINT PRIMARY KEY,
  v1 BIGINT,
  v2 BIGINT,
  UNIQUE (v1)
);
SQL
    run dolt index ls test
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v1(v1)" ]] || false
    run dolt schema show test
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'UNIQUE KEY `v1` (`v1`)' ]] || false

	run dolt index ls test2
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v1(v1)" ]] || false
    run dolt schema show test2
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'UNIQUE KEY `v1` (`v1`)' ]] || false
}

@test "index: CREATE TABLE INDEX named with comment" {
    dolt sql <<SQL
CREATE TABLE test(
  pk BIGINT PRIMARY KEY,
  v1 BIGINT,
  v2 BIGINT,
  INDEX idx_v1 (v1, v2) COMMENT 'hello there'
);
SQL
    run dolt index ls test
    [ "$status" -eq "0" ]
    [[ "$output" =~ "idx_v1(v1, v2)" ]] || false
    run dolt schema show test
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'KEY `idx_v1` (`v1`,`v2`)'" COMMENT 'hello there'" ]] || false
}

@test "index: CREATE TABLE INDEX multiple" {
    dolt sql <<SQL
CREATE TABLE test(
  pk BIGINT PRIMARY KEY,
  v1 BIGINT,
  v2 BIGINT,
  INDEX (v1),
  INDEX (v1, v2)
);
SQL
    run dolt index ls test
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v1(v1)" ]] || false
	[[ "$output" =~ "v1v2(v1, v2)" ]] || false
    run dolt schema show test
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'KEY `v1` (`v1`)' ]] || false
	[[ "$output" =~ 'KEY `v1v2` (`v1`,`v2`)' ]] || false
}

@test "index: CREATE INDEX then INSERT" {
    dolt sql <<SQL
CREATE INDEX idx_v1 ON onepk(v1);
INSERT INTO onepk VALUES (1, 99, 51), (2, 11, 55), (3, 88, 52), (4, 22, 54), (5, 77, 53);
SQL
    run dolt index ls onepk
    [ "$status" -eq "0" ]
    [[ "$output" =~ "idx_v1(v1)" ]] || false
    run dolt index cat onepk idx_v1 -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v1,pk1" ]] || false
    [[ "$output" =~ "11,2" ]] || false
    [[ "$output" =~ "22,4" ]] || false
    [[ "$output" =~ "77,5" ]] || false
    [[ "$output" =~ "88,3" ]] || false
    [[ "$output" =~ "99,1" ]] || false
    [[ "${#lines[@]}" = "6" ]] || false
    run dolt schema show onepk
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'KEY `idx_v1` (`v1`)' ]] || false
    run dolt sql -q "SELECT pk1 FROM onepk WHERE v1 = 77" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1" ]] || false
    [[ "$output" =~ "5" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    
    dolt sql <<SQL
CREATE INDEX idx_v ON twopk(v2, v1);
INSERT INTO twopk VALUES (1, 99, 51, 63), (2, 11, 55, 64), (3, 88, 52, 61), (4, 22, 54, 65), (5, 77, 53, 61);
SQL
    run dolt index ls twopk
    [ "$status" -eq "0" ]
    [[ "$output" =~ "idx_v(v2, v1)" ]] || false
    run dolt index cat twopk idx_v -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v2,v1,pk1,pk2" ]] || false
    [[ "$output" =~ "61,52,3,88" ]] || false
    [[ "$output" =~ "61,53,5,77" ]] || false
    [[ "$output" =~ "63,51,1,99" ]] || false
    [[ "$output" =~ "64,55,2,11" ]] || false
    [[ "$output" =~ "65,54,4,22" ]] || false
    [[ "${#lines[@]}" = "6" ]] || false
    run dolt schema show twopk
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'KEY `idx_v` (`v2`,`v1`)' ]] || false
    run dolt sql -q "SELECT pk1, pk2 FROM twopk WHERE v2 = 61 AND v1 = 53" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2" ]] || false
    [[ "$output" =~ "5,77" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
}

@test "index: INSERT then CREATE INDEX" {
    dolt sql <<SQL
INSERT INTO onepk VALUES (1, 99, 51), (2, 11, 55), (3, 88, 52), (4, 22, 54), (5, 77, 53);
CREATE INDEX idx_v1 ON onepk(v1);
SQL
    run dolt index ls onepk
    [ "$status" -eq "0" ]
    [[ "$output" =~ "idx_v1(v1)" ]] || false
    run dolt index cat onepk idx_v1 -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v1,pk1" ]] || false
    [[ "$output" =~ "11,2" ]] || false
    [[ "$output" =~ "22,4" ]] || false
    [[ "$output" =~ "77,5" ]] || false
    [[ "$output" =~ "88,3" ]] || false
    [[ "$output" =~ "99,1" ]] || false
    [[ "${#lines[@]}" = "6" ]] || false
    run dolt schema show onepk
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'KEY `idx_v1` (`v1`)' ]] || false
    run dolt sql -q "SELECT pk1 FROM onepk WHERE v1 = 77" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1" ]] || false
    [[ "$output" =~ "5" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    
    dolt sql <<SQL
INSERT INTO twopk VALUES (1, 99, 51, 63), (2, 11, 55, 64), (3, 88, 52, 61), (4, 22, 54, 65), (5, 77, 53, 61);
CREATE INDEX idx_v ON twopk(v2, v1);
SQL
    run dolt index ls twopk
    [ "$status" -eq "0" ]
    [[ "$output" =~ "idx_v(v2, v1)" ]] || false
    run dolt index cat twopk idx_v -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v2,v1,pk1,pk2" ]] || false
    [[ "$output" =~ "61,52,3,88" ]] || false
    [[ "$output" =~ "61,53,5,77" ]] || false
    [[ "$output" =~ "63,51,1,99" ]] || false
    [[ "$output" =~ "64,55,2,11" ]] || false
    [[ "$output" =~ "65,54,4,22" ]] || false
    [[ "${#lines[@]}" = "6" ]] || false
    run dolt schema show twopk
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'KEY `idx_v` (`v2`,`v1`)' ]] || false
    run dolt sql -q "SELECT pk1, pk2 FROM twopk WHERE v2 = 61 AND v1 = 53" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2" ]] || false
    [[ "$output" =~ "5,77" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
}

@test "index: INSERT then ALTER TABLE CREATE INDEX" {
    dolt sql <<SQL
INSERT INTO onepk VALUES (1, 99, 51), (2, 11, 55), (3, 88, 52), (4, 22, 54), (5, 77, 53);
ALTER TABLE onepk ADD INDEX idx_v1 (v1);
SQL
    run dolt index ls onepk
    [ "$status" -eq "0" ]
    [[ "$output" =~ "idx_v1(v1)" ]] || false
    run dolt index cat onepk idx_v1 -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v1,pk1" ]] || false
    [[ "$output" =~ "11,2" ]] || false
    [[ "$output" =~ "22,4" ]] || false
    [[ "$output" =~ "77,5" ]] || false
    [[ "$output" =~ "88,3" ]] || false
    [[ "$output" =~ "99,1" ]] || false
    [[ "${#lines[@]}" = "6" ]] || false
    run dolt schema show onepk
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'KEY `idx_v1` (`v1`)' ]] || false
    run dolt sql -q "SELECT pk1 FROM onepk WHERE v1 = 77" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1" ]] || false
    [[ "$output" =~ "5" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    
    dolt sql <<SQL
INSERT INTO twopk VALUES (1, 99, 51, 63), (2, 11, 55, 64), (3, 88, 52, 61), (4, 22, 54, 65), (5, 77, 53, 61);
ALTER TABLE twopk ADD INDEX idx_v (v2, v1);
SQL
    run dolt index ls twopk
    [ "$status" -eq "0" ]
    [[ "$output" =~ "idx_v(v2, v1)" ]] || false
    run dolt index cat twopk idx_v -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v2,v1,pk1,pk2" ]] || false
    [[ "$output" =~ "61,52,3,88" ]] || false
    [[ "$output" =~ "61,53,5,77" ]] || false
    [[ "$output" =~ "63,51,1,99" ]] || false
    [[ "$output" =~ "64,55,2,11" ]] || false
    [[ "$output" =~ "65,54,4,22" ]] || false
    [[ "${#lines[@]}" = "6" ]] || false
    run dolt schema show twopk
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'KEY `idx_v` (`v2`,`v1`)' ]] || false
    run dolt sql -q "SELECT pk1, pk2 FROM twopk WHERE v2 = 61 AND v1 = 53" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2" ]] || false
    [[ "$output" =~ "5,77" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
}

@test "index: ALTER TABLE CREATE INDEX unnamed" {
    dolt sql <<SQL
ALTER TABLE onepk ADD INDEX (v1);
SQL
    run dolt index ls onepk
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v1(v1)" ]] || false
    run dolt schema show onepk
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'KEY `v1` (`v1`)' ]] || false
}

@test "index: INSERT then REPLACE" {
    dolt sql <<SQL
CREATE INDEX idx_v1 ON onepk(v1);
INSERT INTO onepk VALUES (1, 99, 51), (2, 11, 55), (3, 88, 52), (4, 22, 54), (5, 77, 53);
REPLACE INTO onepk VALUES (1, 98, -1), (2, 11, 55), (3, 87, 52), (4, 102, 54), (6, 77, 53);
SQL
    dolt index cat onepk idx_v1 -r=csv
    run dolt index cat onepk idx_v1 -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v1,pk1" ]] || false
    [[ "$output" =~ "11,2" ]] || false
    [[ "$output" =~ "77,5" ]] || false
    [[ "$output" =~ "77,6" ]] || false
    [[ "$output" =~ "87,3" ]] || false
    [[ "$output" =~ "98,1" ]] || false
    [[ "$output" =~ "102,4" ]] || false
    [[ "${#lines[@]}" = "7" ]] || false
    run dolt sql -q "SELECT pk1 FROM onepk WHERE v1 = 77" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1" ]] || false
    [[ "$output" =~ "5" ]] || false
    [[ "$output" =~ "6" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT pk1 FROM onepk WHERE v1 = 22" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT pk1 FROM onepk WHERE v1 = 102" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1" ]] || false
    [[ "$output" =~ "4" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    
    dolt sql <<SQL
CREATE INDEX idx_v ON twopk(v2, v1);
INSERT INTO twopk VALUES (1, 99, 51, 63), (2, 11, 55, 64), (3, 88, 52, 61), (4, 22, 54, 65), (5, 77, 53, 61);
REPLACE INTO twopk VALUES (1, 99, -1, 63), (2, 11, 55, 64), (3, 87, 59, 60), (4, 102, -4, 65), (6, 77, 13, -59);
SQL
    run dolt index cat twopk idx_v -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~  "v2,v1,pk1,pk2" ]] || false
    [[ "$output" =~ "-59,13,6,77" ]] || false
    [[ "$output" =~ "60,59,3,87" ]] || false
    [[ "$output" =~ "61,52,3,88" ]] || false
    [[ "$output" =~ "61,53,5,77" ]] || false
    [[ "$output" =~ "63,-1,1,99" ]] || false
    [[ "$output" =~ "64,55,2,11" ]] || false
    [[ "$output" =~ "65,-4,4,102" ]] || false
    [[ "$output" =~ "65,54,4,22" ]] || false
    [[ "${#lines[@]}" = "9" ]] || false
    run dolt sql -q "SELECT pk1, pk2 FROM twopk WHERE v2 = 61 AND v1 = 53" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2" ]] || false
    [[ "$output" =~ "5,77" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT pk1, pk2 FROM twopk WHERE v2 = 63 AND v1 = 51" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT pk1, pk2 FROM twopk WHERE v2 = 63 AND v1 = -1" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2" ]] || false
    [[ "$output" =~ "1,99" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
}

@test "index: INSERT then UPDATE" {
    dolt sql <<SQL
CREATE INDEX idx_v1 ON onepk(v1);
INSERT INTO onepk VALUES (1, 99, 51), (2, 11, 55), (3, 88, 52), (4, 22, 54), (5, 77, 53);
UPDATE onepk SET v1 = v1 - 1 WHERE pk1 >= 3;
SQL
    run dolt index cat onepk idx_v1 -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v1,pk1" ]] || false
    [[ "$output" =~ "11,2" ]] || false
    [[ "$output" =~ "21,4" ]] || false
    [[ "$output" =~ "76,5" ]] || false
    [[ "$output" =~ "87,3" ]] || false
    [[ "$output" =~ "99,1" ]] || false
    [[ "${#lines[@]}" = "6" ]] || false
    run dolt sql -q "SELECT pk1 FROM onepk WHERE v1 = 76" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1" ]] || false
    [[ "$output" =~ "5" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    
    dolt sql <<SQL
CREATE INDEX idx_v ON twopk(v2, v1);
INSERT INTO twopk VALUES (1, 99, 51, 63), (2, 11, 55, 64), (3, 88, 52, 61), (4, 22, 54, 65), (5, 77, 53, 61);
UPDATE twopk SET v1 = v1 + 4, v2 = v2 - v1 WHERE pk1 <= 3;
SQL
    run dolt index cat twopk idx_v -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v2,v1,pk1,pk2" ]] || false
    [[ "$output" =~ "5,56,3,88" ]] || false
    [[ "$output" =~ "5,59,2,11" ]] || false
    [[ "$output" =~ "8,55,1,99" ]] || false
    [[ "$output" =~ "61,53,5,77" ]] || false
    [[ "$output" =~ "65,54,4,22" ]] || false
    [[ "${#lines[@]}" = "6" ]] || false
    run dolt sql -q "SELECT pk1, pk2 FROM twopk WHERE v2 = 5 AND v1 = 56" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2" ]] || false
    [[ "$output" =~ "3,88" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
}

@test "index: INSERT then DELETE some" {
    dolt sql <<SQL
CREATE INDEX idx_v1 ON onepk(v1);
INSERT INTO onepk VALUES (1, 99, 51), (2, 11, 55), (3, 88, 52), (4, 22, 54), (5, 77, 53);
DELETE FROM onepk WHERE v1 % 2 = 0;
SQL
    run dolt index cat onepk idx_v1 -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v1,pk1" ]] || false
    [[ "$output" =~ "11,2" ]] || false
    [[ "$output" =~ "77,5" ]] || false
    [[ "$output" =~ "99,1" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
    run dolt sql -q "SELECT pk1 FROM onepk WHERE v1 = 77" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1" ]] || false
    [[ "$output" =~ "5" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT pk1 FROM onepk WHERE v1 = 88" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    
    dolt sql <<SQL
CREATE INDEX idx_v ON twopk(v2, v1);
INSERT INTO twopk VALUES (1, 99, 51, 63), (2, 11, 55, 64), (3, 88, 52, 61), (4, 22, 54, 65), (5, 77, 53, 61);
DELETE FROM twopk WHERE v2 - v1 < 10;
SQL
    run dolt index cat twopk idx_v -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v2,v1,pk1,pk2" ]] || false
    [[ "$output" =~ "63,51,1,99" ]] || false
    [[ "$output" =~ "65,54,4,22" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT pk1, pk2 FROM twopk WHERE v2 = 65 AND v1 = 54" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2" ]] || false
    [[ "$output" =~ "4,22" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT pk1, pk2 FROM twopk WHERE v2 = 61 AND v1 = 52" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
}

@test "index: INSERT then DELETE all" {
    dolt sql <<SQL
CREATE INDEX idx_v1 ON onepk(v1);
INSERT INTO onepk VALUES (1, 99, 51), (2, 11, 55), (3, 88, 52), (4, 22, 54), (5, 77, 53);
DELETE FROM onepk WHERE v1 != -1;
SQL
    run dolt index cat onepk idx_v1 -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v1,pk1" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT pk1 FROM onepk WHERE v1 = 88" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    
    dolt sql <<SQL
INSERT INTO onepk VALUES (1, 99, 51), (2, 11, 55), (3, 88, 52), (4, 22, 54), (5, 77, 53);
DELETE FROM onepk;
SQL
    run dolt index cat onepk idx_v1 -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v1,pk1" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT pk1 FROM onepk WHERE v1 = 88" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    
    dolt sql <<SQL
CREATE INDEX idx_v ON twopk(v2, v1);
INSERT INTO twopk VALUES (1, 99, 51, 63), (2, 11, 55, 64), (3, 88, 52, 61), (4, 22, 54, 65), (5, 77, 53, 61);
DELETE FROM twopk WHERE v1 != -1 AND v1 != -1;
SQL
    run dolt index cat twopk idx_v -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v2,v1,pk1,pk2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT pk1, pk2 FROM twopk WHERE v2 = 61 AND v1 = 52" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    
     dolt sql <<SQL
INSERT INTO twopk VALUES (1, 99, 51, 63), (2, 11, 55, 64), (3, 88, 52, 61), (4, 22, 54, 65), (5, 77, 53, 61);
DELETE FROM twopk;
SQL
    run dolt index cat twopk idx_v -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v2,v1,pk1,pk2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT pk1, pk2 FROM twopk WHERE v2 = 61 AND v1 = 52" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
}

@test "index: CREATE INDEX with same name" {
    dolt sql <<SQL
INSERT INTO onepk VALUES (1, 99, 51), (2, 11, 55), (3, 88, 52), (4, 22, 54), (5, 77, 53);
CREATE INDEX idx_v1 ON onepk(v1);
SQL
    run dolt sql -q "CREATE INDEX idx_v1 ON onepk(v2)"
    [ "$status" -eq "1" ]
    run dolt index ls onepk
    [ "$status" -eq "0" ]
    [[ "$output" =~ "idx_v1(v1)" ]] || false
    # Found bug where the above would error, yet somehow wipe the index table
    run dolt index cat onepk idx_v1 -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v1,pk1" ]] || false
    [[ "$output" =~ "88,3" ]] || false
    [[ "$output" =~ "99,1" ]] || false
    [[ "${#lines[@]}" = "6" ]] || false
    run dolt schema show onepk
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'KEY `idx_v1` (`v1`)' ]] || false
    
    dolt sql <<SQL
INSERT INTO twopk VALUES (1, 99, 51, 63), (2, 11, 55, 64), (3, 88, 52, 61), (4, 22, 54, 65), (5, 77, 53, 61);
CREATE INDEX idx_v ON twopk(v2, v1);
SQL
    run dolt sql -q "CREATE INDEX idx_v ON twopk(v1, v2)"
    [ "$status" -eq "1" ]
    run dolt index ls twopk
    [ "$status" -eq "0" ]
    [[ "$output" =~ "idx_v(v2, v1)" ]] || false
    # Found bug where the above would error, yet somehow wipe the index table
    run dolt index cat twopk idx_v -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v2,v1,pk1,pk2" ]] || false
    [[ "$output" =~ "63,51,1,99" ]] || false
    [[ "$output" =~ "64,55,2,11" ]] || false
    [[ "${#lines[@]}" = "6" ]] || false
    run dolt schema show twopk
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'KEY `idx_v` (`v2`,`v1`)' ]] || false
}

@test "index: CREATE INDEX with same columns" {
    dolt sql <<SQL
INSERT INTO onepk VALUES (1, 99, 51), (2, 11, 55), (3, 88, 52), (4, 22, 54), (5, 77, 53);
CREATE INDEX idx_v1 ON onepk(v1);
SQL
    run dolt sql -q "CREATE INDEX idx_bad ON onepk(v1)"
    [ "$status" -eq "1" ]
    run dolt index ls onepk
    [ "$status" -eq "0" ]
    ! [[ "$output" =~ "idx_bad(v1)" ]] || false
    run dolt schema show onepk
    [ "$status" -eq "0" ]
    ! [[ "$output" =~ 'KEY `idx_bad` (`v1`)' ]] || false
    
    dolt sql <<SQL
INSERT INTO twopk VALUES (1, 99, 51, 63), (2, 11, 55, 64), (3, 88, 52, 61), (4, 22, 54, 65), (5, 77, 53, 61);
CREATE INDEX idx_v ON twopk(v2, v1);
SQL
    run dolt sql -q "CREATE INDEX idx_bud ON twopk(v2, v1)"
    [ "$status" -eq "1" ]
    run dolt index ls twopk
    [ "$status" -eq "0" ]
    ! [[ "$output" =~ "idx_bud(v2, v1)" ]] || false
    run dolt schema show twopk
    [ "$status" -eq "0" ]
    ! [[ "$output" =~ 'KEY `idx_bud` (`v2`,`v1`)' ]] || false
}

@test "index: Disallow 'dolt_' name prefix" {
    run dolt sql -q "CREATE INDEX dolt_idx_v1 ON onepk(v1)"
    [ "$status" -eq "1" ]
    run dolt sql -q "ALTER TABLE onepk ADD INDEX dolt_idx_v1 (v1)"
    [ "$status" -eq "1" ]
}

@test "index: DROP INDEX" {
    dolt sql <<SQL
CREATE INDEX idx_v1 ON onepk(v1);
CREATE INDEX idx_v1pk1 ON onepk(v1,pk1);
CREATE INDEX idx_v2v1 ON twopk(v2, v1);
CREATE INDEX idx_v1v2 ON twopk(v1, v2);
SQL
    run dolt index ls onepk
    [ "$status" -eq "0" ]
    [[ "$output" =~ "idx_v1(v1)" ]] || false
    [[ "$output" =~ "idx_v1pk1(v1, pk1)" ]] || false
    run dolt schema show onepk
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'KEY `idx_v1` (`v1`)' ]] || false
    [[ "$output" =~ 'KEY `idx_v1pk1` (`v1`,`pk1`)' ]] || false
    run dolt index ls twopk
    [ "$status" -eq "0" ]
    [[ "$output" =~ "idx_v2v1(v2, v1)" ]] || false
    [[ "$output" =~ "idx_v1v2(v1, v2)" ]] || false
    run dolt schema show twopk
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'KEY `idx_v2v1` (`v2`,`v1`)' ]] || false
    [[ "$output" =~ 'KEY `idx_v1v2` (`v1`,`v2`)' ]] || false
    
    dolt sql <<SQL
DROP INDEX idx_v1 ON onepk;
DROP INDEX idx_v2v1 ON twopk;
SQL
    run dolt index ls onepk
    [ "$status" -eq "0" ]
    [[ "$output" =~ "idx_v1pk1(v1, pk1)" ]] || false
    ! [[ "$output" =~ "idx_v1(v1)" ]] || false
    run dolt schema show onepk
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'KEY `idx_v1pk1` (`v1`,`pk1`)' ]] || false
    ! [[ "$output" =~ 'KEY `idx_v1` (`v1`)' ]] || false
    run dolt index ls twopk
    [ "$status" -eq "0" ]
    [[ "$output" =~ "idx_v1v2(v1, v2)" ]] || false
    ! [[ "$output" =~ "idx_v2v1(v2, v1)" ]] || false
    run dolt schema show twopk
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'KEY `idx_v1v2` (`v1`,`v2`)' ]] || false
    ! [[ "$output" =~ 'KEY `idx_v2v1` (`v2`,`v1`)' ]] || false
}

@test "index: ALTER TABLE DROP INDEX" {
    dolt sql <<SQL
CREATE INDEX idx_v1 ON onepk(v1);
CREATE INDEX idx_v1pk1 ON onepk(v1,pk1);
CREATE INDEX idx_v2v1 ON twopk(v2, v1);
CREATE INDEX idx_v1v2 ON twopk(v1, v2);
ALTER TABLE onepk DROP INDEX idx_v1;
ALTER TABLE twopk DROP INDEX idx_v2v1;
SQL
    run dolt index ls onepk
    [ "$status" -eq "0" ]
    [[ "$output" =~ "idx_v1pk1(v1, pk1)" ]] || false
    ! [[ "$output" =~ "idx_v1(v1)" ]] || false
    run dolt schema show onepk
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'KEY `idx_v1pk1` (`v1`,`pk1`)' ]] || false
    ! [[ "$output" =~ 'KEY `idx_v1` (`v1`)' ]] || false
    run dolt index ls twopk
    [ "$status" -eq "0" ]
    [[ "$output" =~ "idx_v1v2(v1, v2)" ]] || false
    ! [[ "$output" =~ "idx_v2v1(v2, v1)" ]] || false
    run dolt schema show twopk
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'KEY `idx_v1v2` (`v1`,`v2`)' ]] || false
    ! [[ "$output" =~ 'KEY `idx_v2v1` (`v2`,`v1`)' ]] || false
}

@test "index: ALTER TABLE RENAME INDEX" {
    dolt sql <<SQL
CREATE INDEX idx_v1 ON onepk(v1);
CREATE INDEX idx_v1pk1 ON onepk(v1,pk1);
SQL
    run dolt sql -q "ALTER TABLE onepk RENAME INDEX idx_v1 TO idx_v1pk1"
    [ "$status" -eq "1" ]
    dolt sql -q "ALTER TABLE onepk RENAME INDEX idx_v1 TO idx_vfirst"
    run dolt index ls onepk
    [ "$status" -eq "0" ]
    [[ "$output" =~ "idx_v1pk1(v1, pk1)" ]] || false
    [[ "$output" =~ "idx_vfirst(v1)" ]] || false
    ! [[ "$output" =~ "idx_v1(v1)" ]] || false
    run dolt schema show onepk
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'KEY `idx_v1pk1` (`v1`,`pk1`)' ]] || false
    [[ "$output" =~ 'KEY `idx_vfirst` (`v1`)' ]] || false
    ! [[ "$output" =~ 'KEY `idx_v1` (`v1`)' ]] || false
}

@test "index: RENAME TABLE" {
    dolt sql <<SQL
CREATE INDEX idx_v1 ON onepk(v1);
CREATE INDEX idx_v1v2 ON onepk(v1,v2);
INSERT INTO onepk VALUES (1, 99, 51), (2, 11, 55), (3, 88, 52);
RENAME TABLE onepk TO newpk;
INSERT INTO newpk VALUES (4, 22, 54), (5, 77, 53);
SQL
    run dolt index ls onepk
    [ "$status" -eq "1" ]
    run dolt index ls newpk
    [ "$status" -eq "0" ]
    [[ "$output" =~ "idx_v1(v1)" ]] || false
    [[ "$output" =~ "idx_v1v2(v1, v2)" ]] || false
    run dolt schema show newpk
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'KEY `idx_v1` (`v1`)' ]] || false
    [[ "$output" =~ 'KEY `idx_v1v2` (`v1`,`v2`)' ]] || false
    run dolt index cat newpk idx_v1 -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v1,pk1" ]] || false
    [[ "$output" =~ "22,4" ]] || false
    [[ "$output" =~ "77,5" ]] || false
    [[ "${#lines[@]}" = "6" ]] || false
    run dolt index cat newpk idx_v1v2 -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v1,v2,pk1" ]] || false
    [[ "$output" =~ "11,55,2" ]] || false
    [[ "$output" =~ "22,54,4" ]] || false
    [[ "${#lines[@]}" = "6" ]] || false
    run dolt sql -q "SELECT pk1 FROM newpk WHERE v1 = 77" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1" ]] || false
    [[ "$output" =~ "5" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT pk1 FROM newpk WHERE v1 = 88 AND v2 = 52" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1" ]] || false
    [[ "$output" =~ "3" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
}

@test "index: dolt table mv" {
    dolt sql <<SQL
CREATE INDEX idx_v1 ON onepk(v1);
CREATE INDEX idx_v1v2 ON onepk(v1,v2);
INSERT INTO onepk VALUES (1, 99, 51), (2, 11, 55), (3, 88, 52);
SQL
    dolt table mv onepk newpk
    dolt sql -q "INSERT INTO newpk VALUES (4, 22, 54), (5, 77, 53)"
    run dolt index ls onepk
    [ "$status" -eq "1" ]
    run dolt index ls newpk
    [ "$status" -eq "0" ]
    [[ "$output" =~ "idx_v1(v1)" ]] || false
    [[ "$output" =~ "idx_v1v2(v1, v2)" ]] || false
    run dolt schema show newpk
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'KEY `idx_v1` (`v1`)' ]] || false
    [[ "$output" =~ 'KEY `idx_v1v2` (`v1`,`v2`)' ]] || false
    run dolt index cat newpk idx_v1 -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v1,pk1" ]] || false
    [[ "$output" =~ "77,5" ]] || false
    [[ "$output" =~ "88,3" ]] || false
    [[ "${#lines[@]}" = "6" ]] || false
    run dolt index cat newpk idx_v1v2 -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v1,v2,pk1" ]] || false
    [[ "$output" =~ "77,53,5" ]] || false
    [[ "$output" =~ "88,52,3" ]] || false
    [[ "${#lines[@]}" = "6" ]] || false
    run dolt sql -q "SELECT pk1 FROM newpk WHERE v1 = 77" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1" ]] || false
    [[ "$output" =~ "5" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT pk1 FROM newpk WHERE v1 = 88 AND v2 = 52" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1" ]] || false
    [[ "$output" =~ "3" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
}

@test "index: DROP TABLE" {
    dolt sql <<SQL
CREATE INDEX idx_v1 ON onepk(v1);
INSERT INTO onepk VALUES (1, 99, 51), (2, 11, 55), (3, 88, 52), (4, 22, 54), (5, 77, 53);
DROP TABLE onepk;
SQL
    run dolt index ls onepk
    [ "$status" -eq "1" ]
    run dolt schema show onepk
    [ "$status" -eq "0" ]
    [[ "$output" =~ "onepk not found" ]] || false
    run dolt sql -q "SELECT * FROM onepk WHERE v1 = 77"
    [ "$status" -eq "1" ]
}

@test "index: dolt table rm" {
    dolt sql <<SQL
CREATE INDEX idx_v1 ON onepk(v1);
INSERT INTO onepk VALUES (1, 99, 51), (2, 11, 55), (3, 88, 52), (4, 22, 54), (5, 77, 53);
SQL
    dolt table rm onepk
    run dolt index ls onepk
    [ "$status" -eq "1" ]
    run dolt schema show onepk
    [ "$status" -eq "0" ]
    [[ "$output" =~ "onepk not found" ]] || false
    run dolt sql -q "SELECT * FROM onepk WHERE v1 = 77"
    [ "$status" -eq "1" ]
}

@test "index: dolt table cp" {
    dolt sql <<SQL
CREATE INDEX idx_v1 ON onepk(v1);
INSERT INTO onepk VALUES (1, 99, 51), (2, 11, 55), (3, 88, 52), (4, 22, 54), (5, 77, 53);
SQL
    dolt table cp onepk onepk_new
    run dolt index ls onepk_new
    [ "$status" -eq "0" ]
    [[ "$output" =~ "idx_v1(v1)" ]] || false
    run dolt index cat onepk_new idx_v1 -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v1,pk1" ]] || false
    [[ "$output" =~ "22,4" ]] || false
    [[ "$output" =~ "77,5" ]] || false
    [[ "${#lines[@]}" = "6" ]] || false
    run dolt schema show onepk_new
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'KEY `idx_v1` (`v1`)' ]] || false
    run dolt sql -q "SELECT pk1 FROM onepk_new WHERE v1 = 77" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1" ]] || false
    [[ "$output" =~ "5" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
}

@test "index: TRUNCATE TABLE" {
    skip "TRUNCATE not yet supported"
    dolt sql <<SQL
CREATE INDEX idx_v1 ON onepk(v1);
INSERT INTO onepk VALUES (1, 99, 51), (2, 11, 55), (3, 88, 52), (4, 22, 54), (5, 77, 53);
TRUNCATE TABLE onepk;
SQL
    run dolt index ls onepk
    [ "$status" -eq "0" ]
    [[ "$output" =~ "idx_v1(v1)" ]] || false
    run dolt index cat onepk idx_v1 -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v1,pk1" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt schema show onepk
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'KEY `idx_v1` (`v1`)' ]] || false
    run dolt sql -q "SELECT pk1 FROM onepk WHERE v1 = 77" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
}

@test "index: SELECT = Primary Key" {
    dolt sql <<SQL
INSERT INTO onepk VALUES (1, 99, 51), (2, 11, 55), (3, 88, 52), (4, 22, 54), (5, 77, 53);
INSERT INTO twopk VALUES (1, 99, 51, 63), (2, 11, 55, 64), (3, 88, 52, 61), (4, 22, 54, 65), (5, 77, 53, 61);
SQL
    # found
    run dolt sql -q "SELECT * FROM onepk WHERE pk1 = 5" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,v1,v2" ]] || false
    [[ "$output" =~ "5,77,53" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    # not found
    run dolt sql -q "SELECT * FROM onepk WHERE pk1 = 999" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # found partial pk
    run dolt sql -q "SELECT * FROM twopk WHERE pk1 = 2" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "$output" =~ "2,11,55,64" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    # not found partial pk
    run dolt sql -q "SELECT * FROM twopk WHERE pk1 = 999" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # found
    run dolt sql -q "SELECT * FROM twopk WHERE pk1 = 5 AND pk2 = 77" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "$output" =~ "5,77,53,61" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    # not found key 1
    run dolt sql -q "SELECT * FROM twopk WHERE pk1 = 999 AND pk2 = 22" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # not found key 2
    run dolt sql -q "SELECT * FROM twopk WHERE pk1 = 1 AND pk2 = 999" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # not found key mismatch
    run dolt sql -q "SELECT * FROM twopk WHERE pk1 = 88 AND pk2 = 3" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
}

@test "index: SELECT = Secondary Index" {
    dolt sql <<SQL
CREATE INDEX idx_v1 ON onepk(v1);
CREATE INDEX idx_v ON twopk(v2, v1);
INSERT INTO onepk VALUES (1, 99, 51), (2, 11, 55), (3, 88, 52), (4, 22, 54), (5, 77, 53);
INSERT INTO twopk VALUES (1, 99, 51, 63), (2, 11, 55, 64), (3, 88, 52, 61), (4, 22, 54, 65), (5, 77, 53, 61);
SQL
    # found
    run dolt sql -q "SELECT * FROM onepk WHERE v1 = 77" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,v1,v2" ]] || false
    [[ "$output" =~ "5,77,53" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    # not found
    run dolt sql -q "SELECT * FROM onepk WHERE v1 = 111" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # not indexed & found
    run dolt sql -q "SELECT * FROM onepk WHERE v2 = 54" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,v1,v2" ]] || false
    [[ "$output" =~ "4,22,54" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    # not indexed & not found
    run dolt sql -q "SELECT * FROM onepk WHERE v2 = 111" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # found partial index
    run dolt sql -q "SELECT * FROM twopk WHERE v2 = 64" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "$output" =~ "2,11,55,64" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    # not found partial index
    run dolt sql -q "SELECT * FROM twopk WHERE v2 = 111" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # found
    run dolt sql -q "SELECT * FROM twopk WHERE v2 = 61 AND v1 = 53" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "$output" =~ "5,77,53,61" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    # not found key 1
    run dolt sql -q "SELECT * FROM twopk WHERE v2 = 111 AND v1 = 53" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # not found key 2
    run dolt sql -q "SELECT * FROM twopk WHERE v2 = 61 AND v1 = 111" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # not found key mismatch
    run dolt sql -q "SELECT * FROM twopk WHERE v2 = 61 AND v1 = 54" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # not indexed & found
    run dolt sql -q "SELECT * FROM twopk WHERE v1 = 53" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "$output" =~ "5,77,53,61" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    # not indexed & not found
    run dolt sql -q "SELECT * FROM twopk WHERE v1 = 111" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
}

@test "index: SELECT > Primary Key" {
    dolt sql <<SQL
INSERT INTO onepk VALUES (1, 99, 51), (2, 11, 55), (3, 88, 52), (4, 22, 54), (5, 77, 53);
INSERT INTO twopk VALUES (1, 99, 51, 63), (2, 11, 55, 64), (3, 88, 52, 61), (4, 22, 54, 65), (5, 77, 53, 61);
SQL
    # found
    run dolt sql -q "SELECT * FROM onepk WHERE pk1 > 2" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,v1,v2" ]] || false
    [[ "$output" =~ "3,88,52" ]] || false
    [[ "$output" =~ "4,22,54" ]] || false
    [[ "$output" =~ "5,77,53" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
    # not found
    run dolt sql -q "SELECT * FROM onepk WHERE pk1 > 999" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # found partial pk
    run dolt sql -q "SELECT * FROM twopk WHERE pk1 > 2" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "$output" =~ "3,88,52,61" ]] || false
    [[ "$output" =~ "4,22,54,65" ]] || false
    [[ "$output" =~ "5,77,53,61" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
    # not found partial pk
    run dolt sql -q "SELECT * FROM twopk WHERE pk1 > 999" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # found
    run dolt sql -q "SELECT * FROM twopk WHERE pk1 > 4 AND pk2 > 22" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "$output" =~ "5,77,53,61" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    # not found key 1
    run dolt sql -q "SELECT * FROM twopk WHERE pk1 > 999 AND pk2 > 11" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # not found key 2
    run dolt sql -q "SELECT * FROM twopk WHERE pk1 > 2 AND pk2 > 999" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # not found key mismatch
    run dolt sql -q "SELECT * FROM twopk WHERE pk1 > 3 AND pk2 > 99" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
}

@test "index: SELECT > Secondary Index" {
    dolt sql <<SQL
CREATE INDEX idx_v1 ON onepk(v1);
CREATE INDEX idx_v ON twopk(v2, v1);
INSERT INTO onepk VALUES (1, 99, 51), (2, 11, 55), (3, 88, 52), (4, 22, 54), (5, 77, 53);
INSERT INTO twopk VALUES (1, 99, 51, 63), (2, 11, 55, 64), (3, 88, 52, 61), (4, 22, 54, 65), (5, 77, 53, 61);
SQL
    # found
    run dolt sql -q "SELECT * FROM onepk WHERE v1 > 70" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,v1,v2" ]] || false
    [[ "$output" =~ "5,77,53" ]] || false
    [[ "$output" =~ "3,88,52" ]] || false
    [[ "$output" =~ "1,99,51" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
    # not found
    run dolt sql -q "SELECT * FROM onepk WHERE v1 > 111" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # not indexed & found
    run dolt sql -q "SELECT * FROM onepk WHERE v2 > 53" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,v1,v2" ]] || false
    [[ "$output" =~ "2,11,55" ]] || false
    [[ "$output" =~ "4,22,54" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    # not indexed & not found
    run dolt sql -q "SELECT * FROM onepk WHERE v2 > 111" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # found partial index
    run dolt sql -q "SELECT * FROM twopk WHERE v2 > 63" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "$output" =~ "2,11,55,64" ]] || false
    [[ "$output" =~ "4,22,54,65" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    # not found partial index
    run dolt sql -q "SELECT * FROM twopk WHERE v2 > 111" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # found
    run dolt sql -q "SELECT * FROM twopk WHERE v2 > 61 AND v1 > 53" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "$output" =~ "2,11,55,64" ]] || false
    [[ "$output" =~ "4,22,54,65" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    # not found key 1
    run dolt sql -q "SELECT * FROM twopk WHERE v2 > 111 AND v1 > 53" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # not found key 2
    run dolt sql -q "SELECT * FROM twopk WHERE v2 > 61 AND v1 > 111" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # not found key mismatch
    run dolt sql -q "SELECT * FROM twopk WHERE v2 > 64 AND v1 > 54" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # not indexed & found
    run dolt sql -q "SELECT * FROM twopk WHERE v1 > 53" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "$output" =~ "2,11,55,64" ]] || false
    [[ "$output" =~ "4,22,54,65" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    # not indexed & not found
    run dolt sql -q "SELECT * FROM twopk WHERE v1 > 111" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
}

@test "index: SELECT < Primary Key" {
    dolt sql <<SQL
INSERT INTO onepk VALUES (1, 99, 51), (2, 11, 55), (3, 88, 52), (4, 22, 54), (5, 77, 53);
INSERT INTO twopk VALUES (1, 99, 51, 63), (2, 11, 55, 64), (3, 88, 52, 61), (4, 22, 54, 65), (5, 77, 53, 61);
SQL
    # found
    run dolt sql -q "SELECT * FROM onepk WHERE pk1 < 3" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,v1,v2" ]] || false
    [[ "$output" =~ "1,99,51" ]] || false
    [[ "$output" =~ "2,11,55" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    # not found
    run dolt sql -q "SELECT * FROM onepk WHERE pk1 < 0" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # found partial key
    run dolt sql -q "SELECT * FROM twopk WHERE pk1 < 4" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "$output" =~ "1,99,51,63" ]] || false
    [[ "$output" =~ "2,11,55,64" ]] || false
    [[ "$output" =~ "3,88,52,61" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
    # not found partial key
    run dolt sql -q "SELECT * FROM twopk WHERE pk1 < 0" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # found
    run dolt sql -q "SELECT * FROM twopk WHERE pk1 < 3 AND pk2 < 99" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "$output" =~ "2,11,55,64" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    # not found key 1
    run dolt sql -q "SELECT * FROM twopk WHERE pk1 < 0 AND pk2 < 77" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # not found key 2
    run dolt sql -q "SELECT * FROM twopk WHERE pk1 < 3 AND pk2 < 0" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # not found key mismatch
    run dolt sql -q "SELECT * FROM twopk WHERE pk1 < 2 AND pk2 < 22" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
}

@test "index: SELECT < Secondary Index" {
    dolt sql <<SQL
CREATE INDEX idx_v1 ON onepk(v1);
CREATE INDEX idx_v ON twopk(v2, v1);
INSERT INTO onepk VALUES (1, 99, 51), (2, 11, 55), (3, 88, 52), (4, 22, 54), (5, 77, 53);
INSERT INTO twopk VALUES (1, 99, 51, 63), (2, 11, 55, 64), (3, 88, 52, 61), (4, 22, 54, 65), (5, 77, 53, 61);
SQL
    # found
    run dolt sql -q "SELECT * FROM onepk WHERE v1 < 99" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,v1,v2" ]] || false
    [[ "$output" =~ "2,11,55" ]] || false
    [[ "$output" =~ "4,22,54" ]] || false
    [[ "$output" =~ "5,77,53" ]] || false
    [[ "$output" =~ "3,88,52" ]] || false
    [[ "${#lines[@]}" = "5" ]] || false
    # not found
    run dolt sql -q "SELECT * FROM onepk WHERE v1 < 0" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # not indexed & found
    run dolt sql -q "SELECT * FROM onepk WHERE v2 < 53" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,v1,v2" ]] || false
    [[ "$output" =~ "1,99,51" ]] || false
    [[ "$output" =~ "3,88,52" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    # not indexed & not found
    run dolt sql -q "SELECT * FROM onepk WHERE v2 < 0" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # found partial index
    run dolt sql -q "SELECT * FROM twopk WHERE v2 < 64" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "$output" =~ "5,77,53,61" ]] || false
    [[ "$output" =~ "3,88,52,61" ]] || false
    [[ "$output" =~ "1,99,51,63" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
    # not found partial index
    run dolt sql -q "SELECT * FROM twopk WHERE v2 < 0" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # found
    run dolt sql -q "SELECT * FROM twopk WHERE v2 < 64 AND v1 < 53" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "$output" =~ "3,88,52,61" ]] || false
    [[ "$output" =~ "1,99,51,63" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    # not found key 1
    run dolt sql -q "SELECT * FROM twopk WHERE v2 < 0 AND v1 < 53" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # not found key 2
    run dolt sql -q "SELECT * FROM twopk WHERE v2 < 61 AND v1 < 0" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # not found key mismatch
    run dolt sql -q "SELECT * FROM twopk WHERE v2 < 62 AND v1 < 52" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # not indexed & found
    run dolt sql -q "SELECT * FROM twopk WHERE v1 < 53" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "$output" =~ "3,88,52,61" ]] || false
    [[ "$output" =~ "1,99,51,63" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    # not indexed & not found
    run dolt sql -q "SELECT * FROM twopk WHERE v1 < 0" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
}

@test "index: SELECT >= Primary Key" {
    dolt sql <<SQL
INSERT INTO onepk VALUES (1, 99, 51), (2, 11, 55), (3, 88, 52), (4, 22, 54), (5, 77, 53);
INSERT INTO twopk VALUES (1, 99, 51, 63), (2, 11, 55, 64), (3, 88, 52, 61), (4, 22, 54, 65), (5, 77, 53, 61);
SQL
    # found
    run dolt sql -q "SELECT * FROM onepk WHERE pk1 >= 2" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,v1,v2" ]] || false
    [[ "$output" =~ "2,11,55" ]] || false
    [[ "$output" =~ "3,88,52" ]] || false
    [[ "$output" =~ "4,22,54" ]] || false
    [[ "$output" =~ "5,77,53" ]] || false
    [[ "${#lines[@]}" = "5" ]] || false
    # not found
    run dolt sql -q "SELECT * FROM onepk WHERE pk1 >= 999" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # found partial pk
    run dolt sql -q "SELECT * FROM twopk WHERE pk1 >= 2" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "$output" =~ "2,11,55,64" ]] || false
    [[ "$output" =~ "3,88,52,61" ]] || false
    [[ "$output" =~ "4,22,54,65" ]] || false
    [[ "$output" =~ "5,77,53,61" ]] || false
    [[ "${#lines[@]}" = "5" ]] || false
    # not found partial pk
    run dolt sql -q "SELECT * FROM twopk WHERE pk1 >= 999" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # found
    run dolt sql -q "SELECT * FROM twopk WHERE pk1 >= 4 AND pk2 >= 22" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "$output" =~ "4,22,54,65" ]] || false
    [[ "$output" =~ "5,77,53,61" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    # not found key 1
    run dolt sql -q "SELECT * FROM twopk WHERE pk1 >= 999 AND pk2 >= 11" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # not found key 2
    run dolt sql -q "SELECT * FROM twopk WHERE pk1 >= 2 AND pk2 >= 999" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # not found key mismatch
    run dolt sql -q "SELECT * FROM twopk WHERE pk1 >= 4 AND pk2 >= 88" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
}

@test "index: SELECT >= Secondary Index" {
    dolt sql <<SQL
CREATE INDEX idx_v1 ON onepk(v1);
CREATE INDEX idx_v ON twopk(v2, v1);
INSERT INTO onepk VALUES (1, 99, 51), (2, 11, 55), (3, 88, 52), (4, 22, 54), (5, 77, 53);
INSERT INTO twopk VALUES (1, 99, 51, 63), (2, 11, 55, 64), (3, 88, 52, 61), (4, 22, 54, 65), (5, 77, 53, 61);
SQL
    # found
    run dolt sql -q "SELECT * FROM onepk WHERE v1 >= 70" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,v1,v2" ]] || false
    [[ "$output" =~ "5,77,53" ]] || false
    [[ "$output" =~ "3,88,52" ]] || false
    [[ "$output" =~ "1,99,51" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
    # not found
    run dolt sql -q "SELECT * FROM onepk WHERE v1 >= 111" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # not indexed & found
    run dolt sql -q "SELECT * FROM onepk WHERE v2 >= 53" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,v1,v2" ]] || false
    [[ "$output" =~ "2,11,55" ]] || false
    [[ "$output" =~ "4,22,54" ]] || false
    [[ "$output" =~ "5,77,53" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
    # not indexed & not found
    run dolt sql -q "SELECT * FROM onepk WHERE v2 >= 111" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # found partial index
    run dolt sql -q "SELECT * FROM twopk WHERE v2 >= 63" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "$output" =~ "1,99,51,63" ]] || false
    [[ "$output" =~ "2,11,55,64" ]] || false
    [[ "$output" =~ "4,22,54,65" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
    # not found partial index
    run dolt sql -q "SELECT * FROM twopk WHERE v2 >= 111" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # found
    run dolt sql -q "SELECT * FROM twopk WHERE v2 >= 61 AND v1 >= 53" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "$output" =~ "5,77,53,61" ]] || false
    [[ "$output" =~ "2,11,55,64" ]] || false
    [[ "$output" =~ "4,22,54,65" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
    # not found key 1
    run dolt sql -q "SELECT * FROM twopk WHERE v2 >= 111 AND v1 >= 53" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # not found key 2
    run dolt sql -q "SELECT * FROM twopk WHERE v2 >= 61 AND v1 >= 111" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # not found key mismatch
    run dolt sql -q "SELECT * FROM twopk WHERE v2 >= 65 AND v1 >= 55" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # not indexed & found
    run dolt sql -q "SELECT * FROM twopk WHERE v1 >= 53" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "$output" =~ "5,77,53,61" ]] || false
    [[ "$output" =~ "2,11,55,64" ]] || false
    [[ "$output" =~ "4,22,54,65" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
    # not indexed & not found
    run dolt sql -q "SELECT * FROM twopk WHERE v1 >= 111" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
}

@test "index: SELECT <= Primary Key" {
    dolt sql <<SQL
INSERT INTO onepk VALUES (1, 99, 51), (2, 11, 55), (3, 88, 52), (4, 22, 54), (5, 77, 53);
INSERT INTO twopk VALUES (1, 99, 51, 63), (2, 11, 55, 64), (3, 88, 52, 61), (4, 22, 54, 65), (5, 77, 53, 61);
SQL
    # found
    run dolt sql -q "SELECT * FROM onepk WHERE pk1 <= 3" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,v1,v2" ]] || false
    [[ "$output" =~ "1,99,51" ]] || false
    [[ "$output" =~ "2,11,55" ]] || false
    [[ "$output" =~ "3,88,52" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
    # not found
    run dolt sql -q "SELECT * FROM onepk WHERE pk1 <= 0" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # found partial key
    run dolt sql -q "SELECT * FROM twopk WHERE pk1 <= 4" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "$output" =~ "1,99,51,63" ]] || false
    [[ "$output" =~ "2,11,55,64" ]] || false
    [[ "$output" =~ "3,88,52,61" ]] || false
    [[ "$output" =~ "4,22,54,65" ]] || false
    [[ "${#lines[@]}" = "5" ]] || false
    # not found partial key
    run dolt sql -q "SELECT * FROM twopk WHERE pk1 <= 0" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # found
    run dolt sql -q "SELECT * FROM twopk WHERE pk1 <= 3 AND pk2 <= 99" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "$output" =~ "1,99,51,63" ]] || false
    [[ "$output" =~ "2,11,55,64" ]] || false
    [[ "$output" =~ "3,88,52,61" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
    # not found key 1
    run dolt sql -q "SELECT * FROM twopk WHERE pk1 <= 0 AND pk2 <= 77" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # not found key 2
    run dolt sql -q "SELECT * FROM twopk WHERE pk1 <= 3 AND pk2 <= 0" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # not found key mismatch
    run dolt sql -q "SELECT * FROM twopk WHERE pk1 <= 1 AND pk2 <= 88" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
}

@test "index: SELECT <= Secondary Index" {
    dolt sql <<SQL
CREATE INDEX idx_v1 ON onepk(v1);
CREATE INDEX idx_v ON twopk(v2, v1);
INSERT INTO onepk VALUES (1, 99, 51), (2, 11, 55), (3, 88, 52), (4, 22, 54), (5, 77, 53);
INSERT INTO twopk VALUES (1, 99, 51, 63), (2, 11, 55, 64), (3, 88, 52, 61), (4, 22, 54, 65), (5, 77, 53, 61);
SQL
    # found
    run dolt sql -q "SELECT * FROM onepk WHERE v1 <= 99" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,v1,v2" ]] || false
    [[ "$output" =~ "2,11,55" ]] || false
    [[ "$output" =~ "4,22,54" ]] || false
    [[ "$output" =~ "5,77,53" ]] || false
    [[ "$output" =~ "3,88,52" ]] || false
    [[ "$output" =~ "1,99,51" ]] || false
    [[ "${#lines[@]}" = "6" ]] || false
    # not found
    run dolt sql -q "SELECT * FROM onepk WHERE v1 <= 0" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # not indexed & found
    run dolt sql -q "SELECT * FROM onepk WHERE v2 <= 53" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,v1,v2" ]] || false
    [[ "$output" =~ "1,99,51" ]] || false
    [[ "$output" =~ "3,88,52" ]] || false
    [[ "$output" =~ "5,77,53" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
    # not indexed & not found
    run dolt sql -q "SELECT * FROM onepk WHERE v2 <= 0" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # found partial index
    run dolt sql -q "SELECT * FROM twopk WHERE v2 <= 64" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "$output" =~ "5,77,53,61" ]] || false
    [[ "$output" =~ "3,88,52,61" ]] || false
    [[ "$output" =~ "1,99,51,63" ]] || false
    [[ "$output" =~ "2,11,55,64" ]] || false
    [[ "${#lines[@]}" = "5" ]] || false
    # not found partial index
    run dolt sql -q "SELECT * FROM twopk WHERE v2 <= 0" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # found
    run dolt sql -q "SELECT * FROM twopk WHERE v2 <= 64 AND v1 <= 53" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "$output" =~ "3,88,52,61" ]] || false
    [[ "$output" =~ "5,77,53,61" ]] || false
    [[ "$output" =~ "1,99,51,63" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
    # not found key 1
    run dolt sql -q "SELECT * FROM twopk WHERE v2 <= 0 AND v1 <= 53" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # not found key 2
    run dolt sql -q "SELECT * FROM twopk WHERE v2 <= 61 AND v1 <= 0" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # not found key mismatch
    run dolt sql -q "SELECT * FROM twopk WHERE v2 <= 61 AND v1 <= 51" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # not indexed & found
    run dolt sql -q "SELECT * FROM twopk WHERE v1 <= 53" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "$output" =~ "3,88,52,61" ]] || false
    [[ "$output" =~ "5,77,53,61" ]] || false
    [[ "$output" =~ "1,99,51,63" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
    # not indexed & not found
    run dolt sql -q "SELECT * FROM twopk WHERE v1 <= 0" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
}

@test "index: SELECT BETWEEN Primary Key" {
    dolt sql <<SQL
INSERT INTO onepk VALUES (1, 99, 51), (2, 11, 55), (3, 88, 52), (4, 22, 54), (5, 77, 53);
INSERT INTO twopk VALUES (1, 99, 51, 63), (2, 11, 55, 64), (3, 88, 52, 61), (4, 22, 54, 65), (5, 77, 53, 61);
SQL
    # found
    run dolt sql -q "SELECT * FROM onepk WHERE pk1 BETWEEN 2 AND 4" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,v1,v2" ]] || false
    [[ "$output" =~ "2,11,55" ]] || false
    [[ "$output" =~ "3,88,52" ]] || false
    [[ "$output" =~ "4,22,54" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
    # not found
    run dolt sql -q "SELECT * FROM onepk WHERE pk1 BETWEEN 6 AND 9" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # found partial key
    run dolt sql -q "SELECT * FROM twopk WHERE pk1 BETWEEN 1 AND 4" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "$output" =~ "1,99,51,63" ]] || false
    [[ "$output" =~ "2,11,55,64" ]] || false
    [[ "$output" =~ "3,88,52,61" ]] || false
    [[ "$output" =~ "4,22,54,65" ]] || false
    [[ "${#lines[@]}" = "5" ]] || false
    # not found partial key
    run dolt sql -q "SELECT * FROM twopk WHERE pk1 BETWEEN 6 AND 7" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # found
    run dolt sql -q "SELECT * FROM twopk WHERE pk1 BETWEEN 1 AND 3 AND pk2 BETWEEN 10 AND 90" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "$output" =~ "2,11,55,64" ]] || false
    [[ "$output" =~ "3,88,52,61" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    # not found key 1
    run dolt sql -q "SELECT * FROM twopk WHERE pk1 BETWEEN 6 AND 8 AND pk2 BETWEEN 20 AND 80" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # not found key 2
    run dolt sql -q "SELECT * FROM twopk WHERE pk1 BETWEEN 1 AND 3 AND pk2 BETWEEN 100 AND 111" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # not found key mismatch
    run dolt sql -q "SELECT * FROM twopk WHERE pk1 BETWEEN 3 AND 5 AND pk2 BETWEEN 10 AND 20" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
}

@test "index: SELECT BETWEEN Secondary Index" {
    dolt sql <<SQL
CREATE INDEX idx_v1 ON onepk(v1);
CREATE INDEX idx_v ON twopk(v2, v1);
INSERT INTO onepk VALUES (1, 99, 51), (2, 11, 55), (3, 88, 52), (4, 22, 54), (5, 77, 53);
INSERT INTO twopk VALUES (1, 99, 51, 63), (2, 11, 55, 64), (3, 88, 52, 61), (4, 22, 54, 65), (5, 77, 53, 61);
SQL
    # found
    run dolt sql -q "SELECT * FROM onepk WHERE v1 BETWEEN 11 AND 99" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,v1,v2" ]] || false
    [[ "$output" =~ "2,11,55" ]] || false
    [[ "$output" =~ "4,22,54" ]] || false
    [[ "$output" =~ "5,77,53" ]] || false
    [[ "$output" =~ "3,88,52" ]] || false
    [[ "$output" =~ "1,99,51" ]] || false
    [[ "${#lines[@]}" = "6" ]] || false
    # not found
    run dolt sql -q "SELECT * FROM onepk WHERE v1 BETWEEN 30 AND 70" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # not indexed & found
    run dolt sql -q "SELECT * FROM onepk WHERE v2 BETWEEN 50 AND 53" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,v1,v2" ]] || false
    [[ "$output" =~ "1,99,51" ]] || false
    [[ "$output" =~ "3,88,52" ]] || false
    [[ "$output" =~ "5,77,53" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
    # not indexed & not found
    run dolt sql -q "SELECT * FROM onepk WHERE v2 BETWEEN 20 AND 50" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # found partial index
    run dolt sql -q "SELECT * FROM twopk WHERE v2 BETWEEN 0 AND 64" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "$output" =~ "5,77,53,61" ]] || false
    [[ "$output" =~ "3,88,52,61" ]] || false
    [[ "$output" =~ "1,99,51,63" ]] || false
    [[ "$output" =~ "2,11,55,64" ]] || false
    [[ "${#lines[@]}" = "5" ]] || false
    # not found partial index
    run dolt sql -q "SELECT * FROM twopk WHERE v2 BETWEEN 70 AND 90" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # found
    run dolt sql -q "SELECT * FROM twopk WHERE v2 BETWEEN 60 AND 64 AND v1 BETWEEN 50 AND 53" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "$output" =~ "3,88,52,61" ]] || false
    [[ "$output" =~ "5,77,53,61" ]] || false
    [[ "$output" =~ "1,99,51,63" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
    # not found key 1
    run dolt sql -q "SELECT * FROM twopk WHERE v2 BETWEEN 50 AND 53 AND v1 BETWEEN 50 AND 53" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # not found key 2
    run dolt sql -q "SELECT * FROM twopk WHERE v2 BETWEEN 60 AND 64 AND v1 BETWEEN 60 AND 64" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # not found key mismatch
    run dolt sql -q "SELECT * FROM twopk WHERE v2 BETWEEN 60 AND 62 AND v1 BETWEEN 54 AND 55" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    # not indexed & found
    run dolt sql -q "SELECT * FROM twopk WHERE v1 BETWEEN 51 AND 53" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "$output" =~ "3,88,52,61" ]] || false
    [[ "$output" =~ "5,77,53,61" ]] || false
    [[ "$output" =~ "1,99,51,63" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
    # not indexed & not found
    run dolt sql -q "SELECT * FROM twopk WHERE v1 BETWEEN 90 AND 100" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
}

@test "index: EXPLAIN SELECT = IndexedJoin" {
    dolt sql <<SQL
CREATE INDEX idx_v1 ON onepk(v1);
CREATE INDEX idx_v ON twopk(v2, v1);
INSERT INTO onepk VALUES (1, 11, 111), (2, 22, 222), (3, 33, 333), (4, 44, 444), (5, 55, 555);
INSERT INTO twopk VALUES (5, 95, 222, 11), (4, 4, 333, 55), (3, 93, 444, 33), (2, 92, 111, 22), (1, 91, 555, 44);
SQL
    run dolt sql -q "SELECT * FROM onepk JOIN twopk ON onepk.v1 = twopk.v2;" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,v1,v2,pk1,pk2,v1,v2" ]] || false
    [[ "$output" =~ "1,11,111,5,95,222,11" ]] || false
    [[ "$output" =~ "2,22,222,2,92,111,22" ]] || false
    [[ "$output" =~ "3,33,333,3,93,444,33" ]] || false
    [[ "$output" =~ "4,44,444,1,91,555,44" ]] || false
    [[ "$output" =~ "5,55,555,4,4,333,55" ]] || false
    [[ "${#lines[@]}" = "6" ]] || false
    run dolt sql -q "EXPLAIN SELECT * FROM onepk JOIN twopk ON onepk.v1 = twopk.v2;"
    [ "$status" -eq "0" ]
    [[ "$output" =~ "IndexedJoin(onepk.v1 = twopk.v2)" ]] || false
    run dolt sql -q "SELECT * FROM onepk JOIN twopk ON onepk.pk1 = twopk.pk1;" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "1,11,111,1,91,555,44" ]] || false
    [[ "$output" =~ "2,22,222,2,92,111,22" ]] || false
    [[ "$output" =~ "3,33,333,3,93,444,33" ]] || false
    [[ "$output" =~ "4,44,444,4,4,333,55" ]] || false
    [[ "$output" =~ "5,55,555,5,95,222,11" ]] || false
    [[ "${#lines[@]}" = "6" ]] || false
    run dolt sql -q "EXPLAIN SELECT * FROM onepk JOIN twopk ON onepk.pk1 = twopk.pk1;"
    [ "$status" -eq "0" ]
    [[ "$output" =~ "IndexedJoin(onepk.pk1 = twopk.pk1)" ]] || false
}

@test "index: ALTER TABLE ADD COLUMN" {
    dolt sql <<SQL
CREATE INDEX idx_v1 ON onepk(v1);
CREATE INDEX idx_v ON twopk(v2, v1);
INSERT INTO onepk VALUES (1, 99, 51), (2, 11, 55), (3, 88, 52), (4, 22, 54), (5, 77, 53);
INSERT INTO twopk VALUES (1, 99, 51, 63), (2, 11, 55, 64), (3, 88, 52, 61), (4, 22, 54, 65), (5, 77, 53, 61);
ALTER TABLE onepk ADD COLUMN v3 BIGINT;
ALTER TABLE twopk ADD COLUMN v3 BIGINT NOT NULL DEFAULT 7;
SQL
    run dolt index cat onepk idx_v1 -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v1,pk1" ]] || false
    [[ "$output" =~ "11,2" ]] || false
    [[ "$output" =~ "99,1" ]] || false
    [[ "${#lines[@]}" = "6" ]] || false
    run dolt schema show onepk
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'KEY `idx_v1` (`v1`)' ]] || false
    run dolt sql -q "SELECT * FROM onepk WHERE v1 = 77" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,v1,v2,v3" ]] || false
    [[ "$output" =~ "5,77,53," ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM onepk WHERE v1 = 77" -r=tabular
    [ "$status" -eq "0" ]
    [[ "$output" =~ "<NULL>" ]] || false
    
    run dolt index cat twopk idx_v -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v2,v1,pk1,pk2" ]] || false
    [[ "$output" =~ "61,53,5,77" ]] || false
    [[ "$output" =~ "65,54,4,22" ]] || false
    [[ "${#lines[@]}" = "6" ]] || false
    run dolt schema show twopk
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'KEY `idx_v` (`v2`,`v1`)' ]] || false
    run dolt sql -q "SELECT * FROM twopk WHERE v2 = 61 AND v1 = 53" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2,v1,v2,v3" ]] || false
    [[ "$output" =~ "5,77,53,61,7" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
}

@test "index: ALTER TABLE CHANGE COLUMN NULL" {
    dolt sql <<SQL
CREATE TABLE onepk_new (
  pk1 BIGINT PRIMARY KEY,
  v1 BIGINT NOT NULL,
  v2 BIGINT
);
CREATE INDEX idx_v1 ON onepk_new(v1);
CREATE INDEX idx_v2v1 ON onepk_new(v2,v1);
INSERT INTO onepk_new VALUES (1, 99, 51), (2, 11, 55), (3, 88, 52), (4, 22, 54), (5, 77, 53);
ALTER TABLE onepk_new CHANGE COLUMN v1 vnew BIGINT NULL;
SQL
    run dolt index cat onepk_new idx_v1 -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "vnew,pk1" ]] || false
    [[ "$output" =~ "11,2" ]] || false
    [[ "$output" =~ "88,3" ]] || false
    [[ "${#lines[@]}" = "6" ]] || false
    run dolt index cat onepk_new idx_v2v1 -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v2,vnew,pk1" ]] || false
    [[ "$output" =~ "51,99,1" ]] || false
    [[ "$output" =~ "53,77,5" ]] || false
    [[ "${#lines[@]}" = "6" ]] || false
    run dolt schema show onepk_new
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'KEY `idx_v1` (`vnew`)' ]] || false
    [[ "$output" =~ 'KEY `idx_v2v1` (`v2`,`vnew`)' ]] || false
    run dolt sql -q "SELECT * FROM onepk_new WHERE vnew = 77" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,vnew,v2" ]] || false
    [[ "$output" =~ "5,77,53" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
}

@test "index: ALTER TABLE MODIFY COLUMN NULL" {
    dolt sql <<SQL
CREATE TABLE onepk_new (
  pk1 BIGINT PRIMARY KEY,
  v1 BIGINT NOT NULL,
  v2 BIGINT
);
CREATE INDEX idx_v1 ON onepk_new(v1);
CREATE INDEX idx_v2v1 ON onepk_new(v2,v1);
INSERT INTO onepk_new VALUES (1, 99, 51), (2, 11, 55), (3, 88, 52), (4, 22, 54), (5, 77, 53);
ALTER TABLE onepk_new MODIFY COLUMN v1 BIGINT NULL;
SQL
    run dolt index cat onepk_new idx_v1 -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v1,pk1" ]] || false
    [[ "$output" =~ "11,2" ]] || false
    [[ "$output" =~ "99,1" ]] || false
    [[ "${#lines[@]}" = "6" ]] || false
    run dolt index cat onepk_new idx_v2v1 -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v2,v1,pk1" ]] || false
    [[ "$output" =~ "51,99,1" ]] || false
    [[ "$output" =~ "55,11,2" ]] || false
    [[ "${#lines[@]}" = "6" ]] || false
    run dolt schema show onepk_new
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'KEY `idx_v1` (`v1`)' ]] || false
    [[ "$output" =~ 'KEY `idx_v2v1` (`v2`,`v1`)' ]] || false
    run dolt sql -q "SELECT * FROM onepk_new WHERE v1 = 77" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,v1,v2" ]] || false
    [[ "$output" =~ "5,77,53" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
}

@test "index: ALTER TABLE CHANGE COLUMN NOT NULL" {
    dolt sql <<SQL
CREATE INDEX idx_v1 ON onepk(v1);
CREATE INDEX idx_v2v1 ON onepk(v2,v1);
INSERT INTO onepk VALUES (1, 99, 51), (2, 11, 55), (3, 88, 52), (4, 22, 54), (5, 77, 53);
ALTER TABLE onepk CHANGE COLUMN v1 vnew BIGINT NOT NULL;
SQL
    run dolt index cat onepk idx_v1 -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "vnew,pk1" ]] || false
    [[ "$output" =~ "11,2" ]] || false
    [[ "$output" =~ "99,1" ]] || false
    [[ "${#lines[@]}" = "6" ]] || false
    run dolt index cat onepk idx_v2v1 -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v2,vnew,pk1" ]] || false
    [[ "$output" =~ "51,99,1" ]] || false
    [[ "$output" =~ "55,11,2" ]] || false
    [[ "${#lines[@]}" = "6" ]] || false
    run dolt schema show onepk
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'KEY `idx_v1` (`vnew`)' ]] || false
    [[ "$output" =~ 'KEY `idx_v2v1` (`v2`,`vnew`)' ]] || false
    run dolt sql -q "SELECT * FROM onepk WHERE vnew = 77" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,vnew,v2" ]] || false
    [[ "$output" =~ "5,77,53" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
}

@test "index: ALTER TABLE MODIFY COLUMN NOT NULL" {
    dolt sql <<SQL
CREATE INDEX idx_v1 ON onepk(v1);
CREATE INDEX idx_v2v1 ON onepk(v2,v1);
INSERT INTO onepk VALUES (1, 99, 51), (2, 11, 55), (3, 88, 52), (4, 22, 54), (5, 77, 53);
ALTER TABLE onepk MODIFY COLUMN v1 BIGINT NOT NULL;
SQL
    run dolt index cat onepk idx_v1 -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v1,pk1" ]] || false
    [[ "$output" =~ "11,2" ]] || false
    [[ "$output" =~ "99,1" ]] || false
    [[ "${#lines[@]}" = "6" ]] || false
    run dolt index cat onepk idx_v2v1 -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v2,v1,pk1" ]] || false
    [[ "$output" =~ "51,99,1" ]] || false
    [[ "$output" =~ "55,11,2" ]] || false
    [[ "${#lines[@]}" = "6" ]] || false
    run dolt schema show onepk
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'KEY `idx_v1` (`v1`)' ]] || false
    [[ "$output" =~ 'KEY `idx_v2v1` (`v2`,`v1`)' ]] || false
    run dolt sql -q "SELECT * FROM onepk WHERE v1 = 77" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,v1,v2" ]] || false
    [[ "$output" =~ "5,77,53" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
}

@test "index: ALTER TABLE RENAME COLUMN" {
    dolt sql <<SQL
CREATE INDEX idx_v1 ON onepk(v1);
CREATE INDEX idx_v2v1 ON onepk(v2,v1);
INSERT INTO onepk VALUES (1, 99, 51), (2, 11, 55), (3, 88, 52), (4, 22, 54), (5, 77, 53);
ALTER TABLE onepk RENAME COLUMN v1 TO vnew;
SQL
    run dolt index cat onepk idx_v1 -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "vnew,pk1" ]] || false
    [[ "$output" =~ "11,2" ]] || false
    [[ "$output" =~ "99,1" ]] || false
    [[ "${#lines[@]}" = "6" ]] || false
    run dolt index cat onepk idx_v2v1 -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v2,vnew,pk1" ]] || false
    [[ "$output" =~ "51,99,1" ]] || false
    [[ "$output" =~ "55,11,2" ]] || false
    [[ "${#lines[@]}" = "6" ]] || false
    run dolt schema show onepk
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'KEY `idx_v1` (`vnew`)' ]] || false
    [[ "$output" =~ 'KEY `idx_v2v1` (`v2`,`vnew`)' ]] || false
    run dolt sql -q "SELECT * FROM onepk WHERE vnew = 77" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,vnew,v2" ]] || false
    [[ "$output" =~ "5,77,53" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
}

@test "index: ALTER TABLE DROP COLUMN" {
    dolt sql <<SQL
CREATE INDEX idx_v1 ON onepk(v1);
CREATE INDEX idx_v2 ON onepk(v2);
CREATE INDEX idx_v1v2 ON onepk(v1,v2);
CREATE INDEX idx_v2v1 ON onepk(v2,v1);
INSERT INTO onepk VALUES (1, 99, 51), (2, 11, 55), (3, 88, 52), (4, 22, 54), (5, 77, 53);
ALTER TABLE onepk DROP COLUMN v1;
SQL
    run dolt index ls onepk
    [ "$status" -eq "0" ]
    [[ "$output" =~ "idx_v2(v2)" ]] || false
    ! [[ "$output" =~ "idx_v1(v1)" ]] || false
    ! [[ "$output" =~ "idx_v1v2(v1, v2)" ]] || false
    ! [[ "$output" =~ "idx_v2v1(v2, v1)" ]] || false
    run dolt schema show onepk
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'KEY `idx_v2` (`v2`)' ]] || false
    ! [[ "$output" =~ 'KEY `idx_v1` (`v1`)' ]] || false
    ! [[ "$output" =~ 'KEY `idx_v2v1` (`v2`,`v1`)' ]] || false
    ! [[ "$output" =~ 'KEY `idx_v1v2` (`v1`,`v2`)' ]] || false
    run dolt index cat onepk idx_v2 -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v2,pk1" ]] || false
    [[ "$output" =~ "51,1" ]] || false
    [[ "$output" =~ "55,2" ]] || false
    [[ "${#lines[@]}" = "6" ]] || false
    run dolt sql -q "SELECT * FROM onepk WHERE v2 = 53" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,v2" ]] || false
    [[ "$output" =~ "5,53" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
}

@test "index: Two commits, then check previous" {
    dolt sql <<SQL
CREATE INDEX idx_v1 ON onepk(v1);
INSERT INTO onepk VALUES (1, 99, 51), (2, 11, 55), (3, 88, 52), (4, 22, 54), (5, 77, 53);
SQL
    dolt add -A
    dolt commit -m "test commit"
    dolt sql -q "UPDATE onepk SET v1 = v1 - 1 WHERE pk1 >= 3"
    dolt add -A
    dolt commit -m "test commmit 2"
    run dolt index cat onepk idx_v1 -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v1,pk1" ]] || false
    [[ "$output" =~ "11,2" ]] || false
    [[ "$output" =~ "21,4" ]] || false
    [[ "$output" =~ "76,5" ]] || false
    [[ "$output" =~ "87,3" ]] || false
    [[ "$output" =~ "99,1" ]] || false
    [[ "${#lines[@]}" = "6" ]] || false
    run dolt sql -q "SELECT pk1 FROM onepk WHERE v1 = 76" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1" ]] || false
    [[ "$output" =~ "5" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    dolt checkout -b last_commit HEAD~1
    run dolt index cat onepk idx_v1 -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v1,pk1" ]] || false
    [[ "$output" =~ "11,2" ]] || false
    [[ "$output" =~ "22,4" ]] || false
    [[ "$output" =~ "77,5" ]] || false
    [[ "$output" =~ "88,3" ]] || false
    [[ "$output" =~ "99,1" ]] || false
    [[ "${#lines[@]}" = "6" ]] || false
    run dolt schema show onepk
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'KEY `idx_v1` (`v1`)' ]] || false
    run dolt sql -q "SELECT pk1 FROM onepk WHERE v1 = 77" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1" ]] || false
    [[ "$output" =~ "5" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT pk1 FROM onepk WHERE v1 = 76" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
}

@test "index: UNIQUE INSERT, UPDATE, REPLACE" {
    dolt sql <<SQL
CREATE UNIQUE INDEX idx_v1 ON onepk(v1);
INSERT INTO onepk VALUES (1, 99, 51), (2, 11, 55), (3, 88, 52), (4, 22, 54), (5, 77, 53);
SQL
    run dolt index ls onepk
    [ "$status" -eq "0" ]
    [[ "$output" =~ "idx_v1(v1)" ]] || false
    run dolt index cat onepk idx_v1 -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v1,pk1" ]] || false
    [[ "$output" =~ "11,2" ]] || false
    [[ "$output" =~ "99,1" ]] || false
    [[ "${#lines[@]}" = "6" ]] || false
    run dolt schema show onepk
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'UNIQUE KEY `idx_v1` (`v1`)' ]] || false
    run dolt sql -q "SELECT pk1 FROM onepk WHERE v1 = 77" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1" ]] || false
    [[ "$output" =~ "5" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "INSERT INTO onepk VALUES (6, 77, 56)"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "UNIQUE" ]] || false
    run dolt sql -q "INSERT INTO onepk VALUES (6, 78, 56)"
    [ "$status" -eq "0" ]
    run dolt sql -q "UPDATE onepk SET v1 = 22 WHERE pk1 = 1"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "UNIQUE" ]] || false
    run dolt sql -q "UPDATE onepk SET v1 = 23 WHERE pk1 = 1"
    [ "$status" -eq "0" ]
    run dolt sql -q "REPLACE INTO onepk VALUES (2, 88, 55)"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "UNIQUE" ]] || false
    run dolt sql -q "REPLACE INTO onepk VALUES (2, 89, 55)"
    [ "$status" -eq "0" ]
}

@test "index: UNIQUE allows multiple NULL values" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT PRIMARY KEY,
  v1 BIGINT,
  UNIQUE INDEX (v1)
);
CREATE TABLE test2 (
  pk BIGINT PRIMARY KEY,
  v1 BIGINT,
  v2 BIGINT,
  UNIQUE INDEX (v1, v2)
);
INSERT INTO test VALUES (0, NULL), (1, NULL), (2, NULL);
INSERT INTO test2 VALUES (0, NULL, NULL), (1, NULL, NULL), (2, 1, NULL), (3, 1, NULL), (4, NULL, 1), (5, NULL, 1);
SQL
    run dolt sql -q "SELECT * FROM test" -r=json
    [ "$status" -eq "0" ]
    [[ "$output" =~ '{"rows": [{"pk":0},{"pk":1},{"pk":2}]}' ]] || false
    run dolt sql -q "SELECT * FROM test WHERE v1 IS NULL" -r=json
    [ "$status" -eq "0" ]
    [[ "$output" =~ '{"rows": [{"pk":0},{"pk":1},{"pk":2}]}' ]] || false
    run dolt sql -q "SELECT * FROM test2" -r=json
    [ "$status" -eq "0" ]
    [[ "$output" =~ '{"rows": [{"pk":0},{"pk":1},{"pk":2,"v1":1},{"pk":3,"v1":1},{"pk":4,"v2":1},{"pk":5,"v2":1}]}' ]] || false
}

@test "index: dolt table import -u" {
    dolt sql -q "CREATE INDEX idx_v1 ON onepk(v1)"
    dolt table import -u onepk `batshelper index_onepk.csv`
    run dolt index ls onepk
    [ "$status" -eq "0" ]
    [[ "$output" =~ "idx_v1(v1)" ]] || false
    run dolt index cat onepk idx_v1 -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v1,pk1" ]] || false
    [[ "$output" =~ "11,2" ]] || false
    [[ "$output" =~ "22,4" ]] || false
    [[ "$output" =~ "77,5" ]] || false
    [[ "$output" =~ "88,3" ]] || false
    [[ "$output" =~ "99,1" ]] || false
    [[ "${#lines[@]}" = "6" ]] || false
    run dolt schema show onepk
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'KEY `idx_v1` (`v1`)' ]] || false
    run dolt sql -q "SELECT pk1 FROM onepk WHERE v1 = 77" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1" ]] || false
    [[ "$output" =~ "5" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
}

@test "index: dolt table import -r" {
    dolt sql <<SQL
CREATE INDEX idx_v1 ON onepk(v1);
INSERT INTO onepk VALUES (1, 98, 50), (2, 10, 54), (3, 87, 51), (4, 21, 53), (5, 76, 52);
SQL
    dolt table import -r onepk `batshelper index_onepk.csv`
    run dolt index ls onepk
    [ "$status" -eq "0" ]
    [[ "$output" =~ "idx_v1(v1)" ]] || false
    run dolt index cat onepk idx_v1 -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v1,pk1" ]] || false
    [[ "$output" =~ "11,2" ]] || false
    [[ "$output" =~ "22,4" ]] || false
    [[ "$output" =~ "77,5" ]] || false
    [[ "$output" =~ "88,3" ]] || false
    [[ "$output" =~ "99,1" ]] || false
    [[ "${#lines[@]}" = "6" ]] || false
    run dolt schema show onepk
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'KEY `idx_v1` (`v1`)' ]] || false
    run dolt sql -q "SELECT pk1 FROM onepk WHERE v1 = 77" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1" ]] || false
    [[ "$output" =~ "5" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
}

@test "index: UNIQUE dolt table import -u" {
    dolt sql -q "CREATE UNIQUE INDEX idx_v1 ON onepk(v1)"
    dolt table import -u onepk `batshelper index_onepk.csv`
    run dolt index ls onepk
    [ "$status" -eq "0" ]
    [[ "$output" =~ "idx_v1(v1)" ]] || false
    run dolt index cat onepk idx_v1 -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v1,pk1" ]] || false
    [[ "$output" =~ "11,2" ]] || false
    [[ "$output" =~ "22,4" ]] || false
    [[ "$output" =~ "77,5" ]] || false
    [[ "$output" =~ "88,3" ]] || false
    [[ "$output" =~ "99,1" ]] || false
    [[ "${#lines[@]}" = "6" ]] || false
    run dolt schema show onepk
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'UNIQUE KEY `idx_v1` (`v1`)' ]] || false
    run dolt sql -q "SELECT pk1 FROM onepk WHERE v1 = 77" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1" ]] || false
    [[ "$output" =~ "5" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    dolt sql <<SQL
DELETE FROM onepk;
INSERT INTO onepk VALUES (6, 11, 55);
SQL
    run dolt table import -u onepk `batshelper index_onepk.csv`
    [ "$status" -eq "1" ]
    [[ "$output" =~ "UNIQUE" ]] || false
}

@test "index: UNIQUE dolt table import -r" {
    dolt sql <<SQL
CREATE UNIQUE INDEX idx_v1 ON onepk(v1);
INSERT INTO onepk VALUES (1, 98, 50), (2, 10, 54), (3, 87, 51), (4, 21, 53), (5, 76, 52);
SQL
    dolt table import -r onepk `batshelper index_onepk.csv`
    run dolt index ls onepk
    [ "$status" -eq "0" ]
    [[ "$output" =~ "idx_v1(v1)" ]] || false
    run dolt index cat onepk idx_v1 -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v1,pk1" ]] || false
    [[ "$output" =~ "11,2" ]] || false
    [[ "$output" =~ "22,4" ]] || false
    [[ "$output" =~ "77,5" ]] || false
    [[ "$output" =~ "88,3" ]] || false
    [[ "$output" =~ "99,1" ]] || false
    [[ "${#lines[@]}" = "6" ]] || false
    run dolt schema show onepk
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'UNIQUE KEY `idx_v1` (`v1`)' ]] || false
    run dolt sql -q "SELECT pk1 FROM onepk WHERE v1 = 77" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1" ]] || false
    [[ "$output" =~ "5" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    dolt sql -q "DELETE FROM onepk"
    run dolt table import -r onepk `batshelper index_onepk_non_unique.csv`
    [ "$status" -eq "1" ]
    [[ "$output" =~ "UNIQUE" ]] || false
}

@test "index: Merge without conflicts" {
    dolt sql -q "CREATE INDEX idx_v1 ON onepk(v1);"
    dolt add -A
    dolt commit -m "baseline commit"
    dolt checkout -b other
    dolt checkout master
    dolt sql -q "INSERT INTO onepk VALUES (1, 11, 101), (2, 22, 202), (3, 33, 303), (4, 44, 404)"
    dolt add -A
    dolt commit -m "master changes"
    dolt checkout other
    dolt sql -q "INSERT INTO onepk VALUES (5, 55, 505), (6, 66, 606), (7, 77, 707), (8, 88, 808)"
    dolt add -A
    dolt commit -m "other changes"
    dolt checkout master
    dolt merge other
    run dolt index ls onepk
    [ "$status" -eq "0" ]
    [[ "$output" =~ "idx_v1(v1)" ]] || false
    run dolt index cat onepk idx_v1 -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~  "v1,pk1" ]] || false
    [[ "$output" =~ "11,1" ]] || false
    [[ "$output" =~ "22,2" ]] || false
    [[ "$output" =~ "33,3" ]] || false
    [[ "$output" =~ "44,4" ]] || false
    [[ "$output" =~ "55,5" ]] || false
    [[ "$output" =~ "66,6" ]] || false
    [[ "$output" =~ "77,7" ]] || false
    [[ "$output" =~ "88,8" ]] || false
    [[ "${#lines[@]}" = "9" ]] || false
    run dolt schema show onepk
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'KEY `idx_v1` (`v1`)' ]] || false
    run dolt sql -q "SELECT * FROM onepk WHERE v1 = 55" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,v1,v2" ]] || false
    [[ "$output" =~ "5,55,505" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
}

@test "index: Merge abort" {
    dolt sql -q "CREATE INDEX idx_v1 ON onepk(v1);"
    dolt add -A
    dolt commit -m "baseline commit"
    dolt checkout -b other
    dolt checkout master
    dolt sql -q "INSERT INTO onepk VALUES (1, 11, 101), (2, 22, 202), (3, -33, 33), (4, 44, 404)"
    dolt add -A
    dolt commit -m "master changes"
    dolt checkout other
    dolt sql -q "INSERT INTO onepk VALUES (1, -11, 11), (2, -22, 22), (3, -33, 33), (4, -44, 44), (5, -55, 55)"
    dolt add -A
    dolt commit -m "other changes"
    dolt checkout master
    dolt merge other
    run dolt index cat onepk idx_v1 -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v1,pk1" ]] || false
    [[ "$output" =~ "-55,5" ]] || false
    [[ "$output" =~ "-33,3" ]] || false
    [[ "$output" =~ "11,1" ]] || false
    [[ "$output" =~ "22,2" ]] || false
    [[ "$output" =~ "44,4" ]] || false
    [[ "${#lines[@]}" = "6" ]] || false
    dolt merge --abort
    run dolt index cat onepk idx_v1 -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v1,pk1" ]] || false
    [[ "$output" =~ "-33,3" ]] || false
    [[ "$output" =~ "11,1" ]] || false
    [[ "$output" =~ "22,2" ]] || false
    [[ "$output" =~ "44,4" ]] || false
    [[ "${#lines[@]}" = "5" ]] || false
}

@test "index: Merge resolving all OURS" {
    dolt sql -q "CREATE INDEX idx_v1 ON onepk(v1);"
    dolt add -A
    dolt commit -m "baseline commit"
    dolt checkout -b other
    dolt checkout master
    dolt sql -q "INSERT INTO onepk VALUES (1, 11, 101), (2, 22, 202), (3, -33, 33), (4, 44, 404)"
    dolt add -A
    dolt commit -m "master changes"
    dolt checkout other
    dolt sql -q "INSERT INTO onepk VALUES (1, -11, 11), (2, -22, 22), (3, -33, 33), (4, -44, 44), (5, -55, 55)"
    dolt add -A
    dolt commit -m "other changes"
    dolt checkout master
    dolt merge other
    dolt conflicts resolve --ours onepk
    run dolt index cat onepk idx_v1 -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v1,pk1" ]] || false
    [[ "$output" =~ "-55,5" ]] || false
    [[ "$output" =~ "-33,3" ]] || false
    [[ "$output" =~ "11,1" ]] || false
    [[ "$output" =~ "22,2" ]] || false
    [[ "$output" =~ "44,4" ]] || false
    [[ "${#lines[@]}" = "6" ]] || false
}

@test "index: Merge resolving all THEIRS" {
    dolt sql -q "CREATE INDEX idx_v1 ON onepk(v1);"
    dolt add -A
    dolt commit -m "baseline commit"
    dolt checkout -b other
    dolt checkout master
    dolt sql -q "INSERT INTO onepk VALUES (1, 11, 101), (2, 22, 202), (3, -33, 33), (4, 44, 404)"
    dolt add -A
    dolt commit -m "master changes"
    dolt checkout other
    dolt sql -q "INSERT INTO onepk VALUES (1, -11, 11), (2, -22, 22), (3, -33, 33), (4, -44, 44), (5, -55, 55)"
    dolt add -A
    dolt commit -m "other changes"
    dolt checkout master
    dolt merge other
    dolt conflicts resolve --theirs onepk
    run dolt index cat onepk idx_v1 -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v1,pk1" ]] || false
    [[ "$output" =~ "-55,5" ]] || false
    [[ "$output" =~ "-44,4" ]] || false
    [[ "$output" =~ "-33,3" ]] || false
    [[ "$output" =~ "-22,2" ]] || false
    [[ "$output" =~ "-11,1" ]] || false
    [[ "${#lines[@]}" = "6" ]] || false
}

@test "index: Merge individually resolving OURS/THEIRS" {
    dolt sql -q "CREATE INDEX idx_v1 ON onepk(v1);"
    dolt add -A
    dolt commit -m "baseline commit"
    dolt checkout -b other
    dolt checkout master
    dolt sql -q "INSERT INTO onepk VALUES (1, 11, 101), (2, 22, 202), (3, -33, 33), (4, 44, 404)"
    dolt add -A
    dolt commit -m "master changes"
    dolt checkout other
    dolt sql -q "INSERT INTO onepk VALUES (1, -11, 11), (2, -22, 22), (3, -33, 33), (4, -44, 44), (5, -55, 55)"
    dolt add -A
    dolt commit -m "other changes"
    dolt checkout master
    dolt merge other
    dolt conflicts resolve onepk 4
    dolt sql <<SQL
UPDATE onepk SET v1 = -11, v2 = 11 WHERE pk1 = 1;
UPDATE onepk SET v1 = -22, v2 = 22 WHERE pk1 = 2;
SQL
    dolt conflicts resolve onepk 1 2
    run dolt index cat onepk idx_v1 -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v1,pk1" ]] || false
    [[ "$output" =~ "-55,5" ]] || false
    [[ "$output" =~ "-33,3" ]] || false
    [[ "$output" =~ "-22,2" ]] || false
    [[ "$output" =~ "-11,1" ]] || false
    [[ "$output" =~ "44,4" ]] || false
    [[ "${#lines[@]}" = "6" ]] || false
}

@test "index: Merge violates UNIQUE" {
    dolt sql -q "CREATE UNIQUE INDEX idx_v1 ON onepk(v1);"
    dolt add -A
    dolt commit -m "baseline commit"
    dolt checkout -b other
    dolt checkout master
    dolt sql -q "INSERT INTO onepk VALUES (1, 11, 101), (2, 22, 202), (3, 33, 303), (4, 44, 404)"
    dolt add -A
    dolt commit -m "master changes"
    dolt checkout other
    dolt sql -q "INSERT INTO onepk VALUES (1, 11, 101), (2, 22, 202), (3, 33, 303), (5, 44, 505)"
    dolt add -A
    dolt commit -m "other changes"
    dolt checkout master
    run dolt merge other
    [ "$status" -eq "1" ]
    [[ "$output" =~ "UNIQUE" ]] || false
}
