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
    [[ "$output" =~ 'INDEX `idx_v1` (`v1`)' ]] || false
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
    [[ "$output" =~ 'INDEX `idx_v` (`v2`,`v1`)' ]] || false
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
    [[ "$output" =~ 'INDEX `idx_v1` (`v1`)' ]] || false
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
    [[ "$output" =~ 'INDEX `idx_v` (`v2`,`v1`)' ]] || false
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
    [[ "$output" =~ 'INDEX `idx_v1` (`v1`)' ]] || false
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
    [[ "$output" =~ 'INDEX `idx_v` (`v2`,`v1`)' ]] || false
    run dolt sql -q "SELECT pk1, pk2 FROM twopk WHERE v2 = 61 AND v1 = 53" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,pk2" ]] || false
    [[ "$output" =~ "5,77" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
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
    [[ "$output" =~ 'INDEX `idx_v1` (`v1`)' ]] || false
    
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
    [[ "$output" =~ 'INDEX `idx_v` (`v2`,`v1`)' ]] || false
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
    ! [[ "$output" =~ 'INDEX `idx_bad` (`v1`)' ]] || false
    
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
    ! [[ "$output" =~ 'INDEX `idx_bud` (`v2`,`v1`)' ]] || false
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
    [[ "$output" =~ 'INDEX `idx_v1` (`v1`)' ]] || false
    [[ "$output" =~ 'INDEX `idx_v1pk1` (`v1`,`pk1`)' ]] || false
    run dolt index ls twopk
    [ "$status" -eq "0" ]
    [[ "$output" =~ "idx_v2v1(v2, v1)" ]] || false
    [[ "$output" =~ "idx_v1v2(v1, v2)" ]] || false
    run dolt schema show twopk
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'INDEX `idx_v2v1` (`v2`,`v1`)' ]] || false
    [[ "$output" =~ 'INDEX `idx_v1v2` (`v1`,`v2`)' ]] || false
    
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
    [[ "$output" =~ 'INDEX `idx_v1pk1` (`v1`,`pk1`)' ]] || false
    ! [[ "$output" =~ 'INDEX `idx_v1` (`v1`)' ]] || false
    run dolt index ls twopk
    [ "$status" -eq "0" ]
    [[ "$output" =~ "idx_v1v2(v1, v2)" ]] || false
    ! [[ "$output" =~ "idx_v2v1(v2, v1)" ]] || false
    run dolt schema show twopk
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'INDEX `idx_v1v2` (`v1`,`v2`)' ]] || false
    ! [[ "$output" =~ 'INDEX `idx_v2v1` (`v2`,`v1`)' ]] || false
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
    [[ "$output" =~ 'INDEX `idx_v1pk1` (`v1`,`pk1`)' ]] || false
    ! [[ "$output" =~ 'INDEX `idx_v1` (`v1`)' ]] || false
    run dolt index ls twopk
    [ "$status" -eq "0" ]
    [[ "$output" =~ "idx_v1v2(v1, v2)" ]] || false
    ! [[ "$output" =~ "idx_v2v1(v2, v1)" ]] || false
    run dolt schema show twopk
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'INDEX `idx_v1v2` (`v1`,`v2`)' ]] || false
    ! [[ "$output" =~ 'INDEX `idx_v2v1` (`v2`,`v1`)' ]] || false
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
    [[ "$output" =~ 'INDEX `idx_v1pk1` (`v1`,`pk1`)' ]] || false
    [[ "$output" =~ 'INDEX `idx_vfirst` (`v1`)' ]] || false
    ! [[ "$output" =~ 'INDEX `idx_v1` (`v1`)' ]] || false
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
    [[ "$output" =~ 'INDEX `idx_v1` (`v1`)' ]] || false
    [[ "$output" =~ 'INDEX `idx_v1v2` (`v1`,`v2`)' ]] || false
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
    [[ "$output" =~ 'INDEX `idx_v1` (`v1`)' ]] || false
    [[ "$output" =~ 'INDEX `idx_v1v2` (`v1`,`v2`)' ]] || false
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
    [[ "$output" =~ 'INDEX `idx_v1` (`v1`)' ]] || false
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
    [[ "$output" =~ 'INDEX `idx_v1` (`v1`)' ]] || false
    run dolt sql -q "SELECT pk1 FROM onepk WHERE v1 = 77" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
}

@test "index: SELECT = Full Match" {
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

@test "index: SELECT > Full Match" {
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

@test "index: SELECT < Full Match" {
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

@test "index: SELECT >= Full Match" {
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

@test "index: SELECT <= Full Match" {
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
    [[ "$output" =~ 'INDEX `idx_v1` (`v1`)' ]] || false
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
    [[ "$output" =~ 'INDEX `idx_v` (`v2`,`v1`)' ]] || false
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
    [[ "$output" =~ 'INDEX `idx_v1` (`vnew`)' ]] || false
    [[ "$output" =~ 'INDEX `idx_v2v1` (`v2`,`vnew`)' ]] || false
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
    [[ "$output" =~ 'INDEX `idx_v1` (`v1`)' ]] || false
    [[ "$output" =~ 'INDEX `idx_v2v1` (`v2`,`v1`)' ]] || false
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
    [[ "$output" =~ 'INDEX `idx_v1` (`vnew`)' ]] || false
    [[ "$output" =~ 'INDEX `idx_v2v1` (`v2`,`vnew`)' ]] || false
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
    [[ "$output" =~ 'INDEX `idx_v1` (`v1`)' ]] || false
    [[ "$output" =~ 'INDEX `idx_v2v1` (`v2`,`v1`)' ]] || false
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
    [[ "$output" =~ 'INDEX `idx_v1` (`vnew`)' ]] || false
    [[ "$output" =~ 'INDEX `idx_v2v1` (`v2`,`vnew`)' ]] || false
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
    [[ "$output" =~ 'INDEX `idx_v2` (`v2`)' ]] || false
    ! [[ "$output" =~ 'INDEX `idx_v1` (`v1`)' ]] || false
    ! [[ "$output" =~ 'INDEX `idx_v2v1` (`v2`,`v1`)' ]] || false
    ! [[ "$output" =~ 'INDEX `idx_v1v2` (`v1`,`v2`)' ]] || false
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
    [[ "$output" =~ 'INDEX `idx_v1` (`v1`)' ]] || false
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

@test "index: UNIQUE INDEX" {
    skip "not yet implemented"
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
    [[ "$output" =~ 'UNIQUE INDEX `idx_v1` (`v1`)' ]] || false
    run dolt sql -q "SELECT pk1 FROM onepk WHERE v1 = 77" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1" ]] || false
    [[ "$output" =~ "5" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "INSERT INTO onepk VALUES (6, 77, 56)"
    [ "$status" -eq "1" ]
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
    [[ "$output" =~ 'INDEX `idx_v1` (`v1`)' ]] || false
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
    [[ "$output" =~ 'INDEX `idx_v1` (`v1`)' ]] || false
    run dolt sql -q "SELECT pk1 FROM onepk WHERE v1 = 77" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1" ]] || false
    [[ "$output" =~ "5" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
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
    [[ "$output" =~ 'INDEX `idx_v1` (`v1`)' ]] || false
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
