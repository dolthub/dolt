#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common

    dolt sql <<SQL
CREATE TABLE onepk (
  pk1 BIGINT PRIMARY KEY,
  v1 JSON
);
CREATE TABLE twopk (
  pk1 BIGINT,
  pk2 BIGINT,
  v1 JSON,
  PRIMARY KEY(pk1, pk2)
);
SQL
    dolt add .
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "vector-index: rebuild smoke test" {
    skip_if_remote
    dolt sql -q "CREATE TABLE t (pk int PRIMARY KEY, col1 JSON);"
    dolt sql -q "INSERT INTO t VALUES (1, '[0]');"
    dolt sql -q "CREATE VECTOR INDEX t_idx on t(col1);"
    dolt index cat t t_idx -r csv
    run dolt index cat t t_idx -r csv
    [ "$status" -eq "0" ]
    [[ "${lines[0]}" =~ "col1,pk" ]] || false
    [[ "${lines[1]}" =~ "[0],1" ]] || false
    [[ "${#lines[@]}" == "2" ]] || false

    run dolt index rebuild t t_idx
    [ "$status" -eq "0" ]

    run dolt index cat t t_idx -r csv
    [ "$status" -eq "0" ]
    [[ "${lines[0]}" =~ "col1,pk" ]] || false
    [[ "${lines[1]}" =~ "[0],1" ]] || false
    [[ "${#lines[@]}" == "2" ]] || false
}

@test "vector-index: CREATE TABLE VECTOR INDEX" {
    dolt sql <<SQL
CREATE TABLE test(
  pk BIGINT PRIMARY KEY,
  v1 JSON,
  VECTOR INDEX (v1)
);
SQL
    run dolt index ls test
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v1(v1)" ]] || false
    run dolt schema show test
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'VECTOR KEY `v1` (`v1`)' ]] || false
}

@test "vector-index: CREATE TABLE UNIQUE KEY" {
  skip "UNIQUE VECTOR not supported"
    dolt sql <<SQL
CREATE TABLE test(
  pk BIGINT PRIMARY KEY,
  v1 JSON,
  UNIQUE VECTOR INDEX (v1)
);
SQL
    run dolt index ls test
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v1(v1)" ]] || false
    run dolt schema show test
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'UNIQUE VECTOR KEY `v1` (`v1`)' ]] || false
}

@test "vector-index: CREATE TABLE INDEX named with comment" {
    dolt sql <<SQL
CREATE TABLE test(
  pk BIGINT PRIMARY KEY,
  v1 JSON,
  VECTOR INDEX idx_v1 (v1) COMMENT 'hello there'
);
SQL
    run dolt index ls test
    [ "$status" -eq "0" ]
    [[ "$output" =~ "idx_v1(v1)" ]] || false
    run dolt schema show test
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'VECTOR KEY `idx_v1` (`v1`)'" COMMENT 'hello there'" ]] || false
}

@test "vector-index: CREATE VECTOR INDEX then INSERT" {
    dolt sql <<SQL
CREATE VECTOR INDEX idx_v1 ON onepk(v1);
INSERT INTO onepk VALUES (1, '[99, 51]'), (2, '[11, 55]'), (3, '[88, 52]'), (4, '[22, 54]'), (5, '[77, 53]');
SQL
    run dolt index ls onepk
    [ "$status" -eq "0" ]
    [[ "$output" =~ "idx_v1(v1)" ]] || false
    run dolt index cat onepk idx_v1 -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v1,pk1" ]] || false
    [[ "$output" =~ '"[11,55]",2' ]] || false
    [[ "$output" =~ '"[22,54]",4' ]] || false
    [[ "$output" =~ '"[77,53]",5' ]] || false
    [[ "$output" =~ '"[88,52]",3' ]] || false
    [[ "$output" =~ '"[99,51]",1' ]] || false
    [[ "${#lines[@]}" = "6" ]] || false
    run dolt schema show onepk
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'VECTOR KEY `idx_v1` (`v1`)' ]] || false
    run dolt sql -q "SELECT pk1 FROM onepk ORDER BY VEC_DISTANCE(v1, '[77, 53]') LIMIT 1;" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1" ]] || false
    [[ "$output" =~ "5" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
}

@test "vector-index: INSERT then CREATE VECTOR INDEX" {
    dolt sql <<SQL
INSERT INTO onepk VALUES (1, '[99, 51]'), (2, '[11, 55]'), (3, '[88, 52]'), (4, '[22, 54]'), (5, '[77, 53]');
CREATE VECTOR INDEX idx_v1 ON onepk(v1);
SQL
    run dolt index ls onepk
    [ "$status" -eq "0" ]
    [[ "$output" =~ "idx_v1(v1)" ]] || false
    run dolt index cat onepk idx_v1 -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v1,pk1" ]] || false
    [[ "$output" =~ '"[11,55]",2' ]] || false
    [[ "$output" =~ '"[22,54]",4' ]] || false
    [[ "$output" =~ '"[77,53]",5' ]] || false
    [[ "$output" =~ '"[88,52]",3' ]] || false
    [[ "$output" =~ '"[99,51]",1' ]] || false
    [[ "${#lines[@]}" = "6" ]] || false
    run dolt schema show onepk
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'VECTOR KEY `idx_v1` (`v1`)' ]] || false
    run dolt sql -q "SELECT pk1 FROM onepk ORDER BY VEC_DISTANCE(v1, '[88,52]') LIMIT 1;" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1" ]] || false
    [[ "$output" =~ "3" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
}

@test "vector-index: INSERT then ALTER TABLE CREATE VECTOR INDEX" {
    dolt sql <<SQL
INSERT INTO onepk VALUES (1, '[99, 51]'), (2, '[11, 55]'), (3, '[88, 52]'), (4, '[22, 54]'), (5, '[77, 53]');
ALTER TABLE onepk ADD VECTOR INDEX idx_v1 (v1);
SQL
    run dolt index ls onepk
    [ "$status" -eq "0" ]
    [[ "$output" =~ "idx_v1(v1)" ]] || false
    run dolt index cat onepk idx_v1 -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v1,pk1" ]] || false
    [[ "$output" =~ '"[11,55]",2' ]] || false
    [[ "$output" =~ '"[22,54]",4' ]] || false
    [[ "$output" =~ '"[77,53]",5' ]] || false
    [[ "$output" =~ '"[88,52]",3' ]] || false
    [[ "$output" =~ '"[99,51]",1' ]] || false
    [[ "${#lines[@]}" = "6" ]] || false
    run dolt schema show onepk
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'VECTOR KEY `idx_v1` (`v1`)' ]] || false
    run dolt sql -q "SELECT pk1 FROM onepk ORDER BY VEC_DISTANCE(v1, '[99,51]') LIMIT 1;" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1" ]] || false
    [[ "$output" =~ "1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
}

@test "vector-index: ALTER TABLE CREATE VECTOR INDEX unnamed" {
    dolt sql <<SQL
ALTER TABLE onepk ADD VECTOR INDEX (v1);
SQL
    run dolt index ls onepk
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v1(v1)" ]] || false
    run dolt schema show onepk
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'VECTOR KEY `v1` (`v1`)' ]] || false
}

@test "vector-index: INSERT then REPLACE" {
    dolt sql <<SQL
CREATE VECTOR INDEX idx_v1 ON onepk(v1);
INSERT INTO onepk VALUES (1, '[99, 51]'), (2, '[11, 55]'), (3, '[88, 52]'), (4, '[22, 54]'), (5, '[77, 53]');
REPLACE INTO onepk VALUES (1, '[98, -1]'), (2, '[11, 55]'), (3, '[87, 52]'), (4, '[102, 54]'), (6, '[77, 53]');
SQL
    dolt index cat onepk idx_v1 -r=csv
    run dolt index cat onepk idx_v1 -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v1,pk1" ]] || false
    [[ "$output" =~ '"[11,55]",2' ]] || false
    [[ "$output" =~ '"[77,53]",5' ]] || false
    [[ "$output" =~ '"[77,53]",6' ]] || false
    [[ "$output" =~ '"[87,52]",3' ]] || false
    [[ "$output" =~ '"[98,-1]",1' ]] || false
    [[ "$output" =~ '"[102,54]",4' ]] || false
    [[ "${#lines[@]}" = "7" ]] || false
    dolt sql -q "SELECT pk1 FROM onepk ORDER BY VEC_DISTANCE(v1, '[77,53]') LIMIT 2;" -r=csv
    run dolt sql -q "SELECT pk1 FROM onepk ORDER BY VEC_DISTANCE(v1, '[77,53]') LIMIT 2;" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1" ]] || false
    [[ "$output" =~ "5" ]] || false
    [[ "$output" =~ "6" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT pk1,v1 FROM onepk ORDER BY VEC_DISTANCE(v1, '[99,51]') LIMIT 1;" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1,v1" ]] || false
    [[ "$output" =~ '4,"[102,54]"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT pk1 FROM onepk ORDER BY VEC_DISTANCE(v1, '[98,-1]') LIMIT 1;" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1" ]] || false
    [[ "$output" =~ "1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
}

@test "vector-index: INSERT then UPDATE" {
    dolt sql <<'SQL'
CREATE VECTOR INDEX idx_v1 ON onepk(v1);
INSERT INTO onepk VALUES (1, '[99, 51]'), (2, '[11, 55]'), (3, '[88, 52]'), (4, '[22, 54]'), (5, '[77, 53]');
UPDATE onepk SET v1 = JSON_ARRAY(v1->"$[1]", v1->"$[0]") WHERE pk1 >= 3;
SQL
    run dolt index cat onepk idx_v1 -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v1,pk1" ]] || false
    [[ "$output" =~ '"[11,55]",2' ]] || false
    [[ "$output" =~ '"[54,22]",4' ]] || false
    [[ "$output" =~ '"[53,77]",5' ]] || false
    [[ "$output" =~ '"[52,88]",3' ]] || false
    [[ "$output" =~ '"[99,51]",1' ]] || false
    [[ "${#lines[@]}" = "6" ]] || false
    run dolt sql -q "SELECT pk1 FROM onepk ORDER BY VEC_DISTANCE(v1, '[53,77]') LIMIT 1" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1" ]] || false
    [[ "$output" =~ "5" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
}

@test "vector-index: INSERT then DELETE some" {
    dolt sql <<'SQL'
CREATE VECTOR INDEX idx_v1 ON onepk(v1);
INSERT INTO onepk VALUES (1, '[99, 51]'), (2, '[11, 55]'), (3, '[88, 52]'), (4, '[22, 54]'), (5, '[77, 53]');
DELETE FROM onepk WHERE v1->>"$[0]" % 2 = 0;
SQL
    run dolt index cat onepk idx_v1 -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v1,pk1" ]] || false
    [[ "$output" =~ '"[99,51]",1' ]] || false
    [[ "$output" =~ '"[11,55]",2' ]] || false
    [[ "$output" =~ '"[77,53]",5' ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
    run dolt sql -q "SELECT pk1 FROM onepk ORDER BY VEC_DISTANCE(v1, '[77,53]') LIMIT 1;" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1" ]] || false
    [[ "$output" =~ "5" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT pk1 FROM onepk ORDER BY VEC_DISTANCE(v1, '[22,54]') LIMIT 1;" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1" ]] || false
    ! [[ "$output" =~ "4" ]] || false
    [[ "$output" =~ "2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
}

@test "vector-index: INSERT then DELETE all" {
    dolt sql <<'SQL'
CREATE VECTOR INDEX idx_v1 ON onepk(v1);
INSERT INTO onepk VALUES (1, '[99, 51]'), (2, '[11, 55]'), (3, '[88, 52]'), (4, '[22, 54]'), (5, '[77, 53]');
DELETE FROM onepk WHERE v1->>"$[0]" = 99;
SQL
    run dolt index cat onepk idx_v1 -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v1,pk1" ]] || false
    [[ "$output" =~ '"[11,55]",2' ]] || false
    [[ "$output" =~ '"[88,52]",3' ]] || false
    [[ "$output" =~ '"[22,54]",4' ]] || false
    [[ "$output" =~ '"[77,53]",5' ]] || false
    [[ "${#lines[@]}" = "5" ]] || false
    run dolt sql -q "SELECT pk1 FROM onepk ORDER BY VEC_DISTANCE(v1, '[22,54]') LIMIT 1;" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1" ]] || false
    [[ "$output" =~ "4" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT pk1 FROM onepk ORDER BY VEC_DISTANCE(v1, '[99,51]') LIMIT 1;" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1" ]] || false
    [[ "$output" =~ "3" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false

    dolt sql <<'SQL'
DELETE FROM onepk;
SQL
    run dolt index cat onepk idx_v1 -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v1,pk1" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT pk1 FROM onepk ORDER BY VEC_DISTANCE(v1, '[99,51]') LIMIT 1;" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
}

@test "vector-index: CREATE INDEX with same name" {
    dolt sql <<'SQL'
INSERT INTO onepk VALUES (1, '[99, 51]'), (2, '[11, 55]'), (3, '[88, 52]'), (4, '[22, 54]'), (5, '[77, 53]');
CREATE VECTOR INDEX idx_v1 ON onepk(v1);
SQL
    run dolt sql -q "CREATE VECTOR INDEX idx_v1 ON onepk(v2)"
    [ "$status" -eq "1" ]
    run dolt index ls onepk
    [ "$status" -eq "0" ]
    [[ "$output" =~ "idx_v1(v1)" ]] || false
    # Found bug where the above would error, yet somehow wipe the index table
    run dolt index cat onepk idx_v1 -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v1,pk1" ]] || false
    [[ "$output" =~ '"[88,52]",3' ]] || false
    [[ "$output" =~ '"[99,51]",1' ]] || false
    [[ "${#lines[@]}" = "6" ]] || false
    run dolt schema show onepk
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'VECTOR KEY `idx_v1` (`v1`)' ]] || false
}

@test "vector-index: CREATE INDEX with same columns" {
    dolt sql <<'SQL'
INSERT INTO onepk VALUES (1, '[99, 51]'), (2, '[11, 55]'), (3, '[88, 52]'), (4, '[22, 54]'), (5, '[77, 53]');
CREATE VECTOR INDEX idx_v1 ON onepk(v1);
SQL
    run dolt sql -q "CREATE VECTOR INDEX idx_v1_dup ON onepk(v1)"
    [ "$status" -eq "0" ]
    run dolt index ls onepk
    [ "$status" -eq "0" ]
    [[ "$output" =~ "idx_v1_dup(v1)" ]] || false
    run dolt schema show onepk
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'VECTOR KEY `idx_v1_dup` (`v1`)' ]] || false
}

@test "vector-index: Disallow 'dolt_' name prefix" {
    run dolt sql -q "CREATE VECTOR INDEX dolt_idx_v1 ON onepk(v1)"
    [ "$status" -eq "1" ]
    run dolt sql -q "ALTER TABLE onepk ADD INDEX dolt_idx_v1 (v1)"
    [ "$status" -eq "1" ]
}

@test "vector-index: DROP INDEX" {
    dolt sql <<SQL
CREATE VECTOR INDEX idx_v1 ON onepk(v1);
SQL
    run dolt index ls onepk
    [ "$status" -eq "0" ]
    [[ "$output" =~ "idx_v1(v1)" ]] || false
    run dolt schema show onepk
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'VECTOR KEY `idx_v1` (`v1`)' ]] || false

    dolt sql <<SQL
DROP INDEX idx_v1 ON onepk;
SQL
    run dolt index ls onepk
    [ "$status" -eq "0" ]
    ! [[ "$output" =~ "idx_v1(v1)" ]] || false
    run dolt schema show onepk
    [ "$status" -eq "0" ]
    ! [[ "$output" =~ 'VECTOR KEY `idx_v1` (`v1`)' ]] || false
}

@test "vector-index: ALTER TABLE DROP INDEX" {
    dolt sql <<SQL
CREATE VECTOR INDEX idx_v1 ON onepk(v1);
ALTER TABLE onepk DROP INDEX idx_v1;
SQL
    run dolt index ls onepk
    [ "$status" -eq "0" ]
    ! [[ "$output" =~ "idx_v1(v1)" ]] || false
    run dolt schema show onepk
    [ "$status" -eq "0" ]
    ! [[ "$output" =~ 'KEY `idx_v1` (`v1`)' ]] || false
}

@test "vector-index: ALTER TABLE RENAME INDEX" {
    dolt sql <<SQL
CREATE VECTOR INDEX idx_v1 ON onepk(v1);
CREATE VECTOR INDEX idx_v2 ON onepk(v1);
SQL
    run dolt sql -q "ALTER TABLE onepk RENAME INDEX idx_v1 TO idx_v2"
    [ "$status" -eq "1" ]
    dolt sql -q "ALTER TABLE onepk RENAME INDEX idx_v1 TO idx_vfirst"
    run dolt index ls onepk
    [ "$status" -eq "0" ]
    [[ "$output" =~ "idx_vfirst(v1)" ]] || false
    [[ "$output" =~ "idx_v2(v1)" ]] || false
    ! [[ "$output" =~ "idx_v1(v1)" ]] || false
    run dolt schema show onepk
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'VECTOR KEY `idx_v2` (`v1`)' ]] || false
    [[ "$output" =~ 'VECTOR KEY `idx_vfirst` (`v1`)' ]] || false
    ! [[ "$output" =~ 'VECTOR KEY `idx_v1` (`v1`)' ]] || false
}

@test "vector-index: TRUNCATE TABLE" {
    dolt sql <<SQL
CREATE VECTOR INDEX idx_v1 ON onepk(v1);
INSERT INTO onepk VALUES (1, '[99, 51]'), (2, '[11, 55]'), (3, '[88, 52]'), (4, '[22, 54]'), (5, '[77, 53]');
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
    [[ "$output" =~ 'VECTOR KEY `idx_v1` (`v1`)' ]] || false
    run dolt sql -q "SELECT pk1 FROM onepk ORDER BY VEC_DISTANCE(v1, '[99,51]') LIMIT 1;" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk1" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
}
