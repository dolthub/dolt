#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "sql-patch: output reconciles INSERT query" {
    dolt checkout -b firstbranch
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
    dolt table import -u test `batshelper 1pk5col-ints.csv`
    dolt add test
    dolt commit -m "Added one initial row"

    dolt checkout -b newbranch
    dolt sql -q 'INSERT INTO test (pk, c1, c2, c3, c4, c5) VALUES (2, 11, 0, 0, 0, 0)'
    dolt add test
    dolt commit -m "Added three rows"

    run dolt diff -r sql firstbranch newbranch
    [ "$status" -eq 0 ]
    diff_output=$output

    run dolt sql -q "CALL DOLT_PATCH('firstbranch','newbranch')" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[0]}" =~ "statement" ]] || false
    [[ "$output" =~ "$diff_output" ]] || false
}

@test "sql-patch: output reconciles UPDATE query" {
    dolt checkout -b firstbranch
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
    dolt table import -u test `batshelper 1pk5col-ints.csv`
    dolt add test
    dolt commit -m "Added one initial row"

    dolt checkout -b newbranch
    dolt sql -q 'UPDATE test SET c1=11, c5=6 WHERE pk=0'
    dolt add test
    dolt commit -m "modified first row"

    run dolt diff -r sql firstbranch newbranch
    [ "$status" -eq 0 ]
    diff_output=${lines[0]}

    run dolt sql -q "CALL DOLT_PATCH('firstbranch','newbranch')"
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "statement" ]] || false
    [[ "${lines[3]}" =~ "$diff_output" ]] || false
}

@test "sql-patch: output reconciles DELETE query" {
    dolt checkout -b firstbranch
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
    dolt table import -u test `batshelper 1pk5col-ints.csv`
    dolt add test
    dolt commit -m "Added one initial row"

    dolt checkout -b newbranch
    dolt sql -q 'DELETE FROM test WHERE pk=0'
    dolt add test
    dolt commit -m "deleted first row"

    run dolt diff -r sql firstbranch newbranch
    [ "$status" -eq 0 ]
    diff_output=$output

    run dolt sql -q "CALL DOLT_PATCH('firstbranch','newbranch')" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[0]}" =~ "statement" ]] || false
    [[ "$output" =~ "$diff_output" ]] || false
}

@test "sql-patch: output reconciles change to PRIMARY KEY field in row " {
    dolt checkout -b firstbranch
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
    dolt table import -u test `batshelper 1pk5col-ints.csv`
    dolt add test
    dolt commit -m "Added one initial row"

    dolt checkout -b newbranch
    dolt sql -q 'UPDATE test SET pk=2 WHERE pk=1'
    dolt add test
    dolt commit -m "modified first row"

    run dolt diff -r sql firstbranch newbranch
    [ "$status" -eq 0 ]
    diff_output_0=${lines[0]}
    diff_output_1=${lines[1]}

    run dolt sql -q "CALL DOLT_PATCH('firstbranch','newbranch')"
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "statement" ]] || false
    [[ "${lines[3]}" =~ "$diff_output_0" ]] || false
    [[ "${lines[4]}" =~ "$diff_output_1" ]] || false
}

@test "sql-patch: output reconciles RENAME, DROP and ADD column" {
    dolt checkout -b firstbranch
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
    dolt sql -q 'insert into test values (1,1,1,1,1,1)'
    dolt add .
    dolt commit -m "added row"

    dolt checkout -b newbranch
    dolt sql -q "alter table test rename column c1 to c0"
    dolt sql -q "alter table test drop column c4"
    dolt sql -q "alter table test add c6 bigint"
    dolt add .
    dolt commit -m "renamed column"

    run dolt diff -r sql firstbranch newbranch
    [ "$status" -eq 0 ]
    diff_output_0=${lines[0]}
    diff_output_1=${lines[1]}
    diff_output_2=${lines[2]}

    run dolt sql -q "CALL DOLT_PATCH('firstbranch','newbranch')"
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "statement" ]] || false
    [[ "${lines[3]}" =~ "$diff_output_0" ]] || false
    [[ "${lines[4]}" =~ "$diff_output_1" ]] || false
    [[ "${lines[5]}" =~ "$diff_output_2" ]] || false
}

@test "sql-patch: reconciles CREATE TABLE with row INSERTS" {
    dolt checkout -b firstbranch
    dolt checkout -b newbranch
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  PRIMARY KEY (pk)
);
SQL
    dolt sql -q 'insert into test values (1,1)'
    dolt sql -q 'insert into test values (2,2)'
    dolt commit -Am "created new table"

    run dolt diff -r sql firstbranch newbranch
    [ "$status" -eq 0 ]
    diff_output_0=${lines[0]}
    diff_output_1=${lines[1]}
    diff_output_2=${lines[2]}
    diff_output_3=${lines[3]}
    diff_output_4=${lines[4]}
    diff_output_5=${lines[5]}
    diff_output_6=${lines[6]}

    run dolt sql -q "CALL DOLT_PATCH('firstbranch','newbranch')"
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "statement" ]] || false
    [[ "${lines[3]}" =~ "$diff_output_0" ]] || false
    [[ "${lines[4]}" =~ "$diff_output_1" ]] || false
    [[ "${lines[5]}" =~ "$diff_output_2" ]] || false
    [[ "${lines[6]}" =~ "$diff_output_3" ]] || false
    [[ "${lines[7]}" =~ "$diff_output_4" ]] || false
    [[ "${lines[8]}" =~ "$diff_output_5" ]] || false
    [[ "${lines[9]}" =~ "$diff_output_6" ]] || false
}

@test "sql-patch: reconciles DROP TABLE" {
    dolt checkout -b firstbranch
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  PRIMARY KEY (pk)
);
SQL
    dolt sql -q 'insert into test values (1,1,1)'
    dolt add .
    dolt commit -m "setup table"

    dolt checkout -b newbranch
    dolt sql -q 'drop table test'
    dolt add .
    dolt commit -m "removed table"

    run dolt diff -r sql firstbranch newbranch
    [ "$status" -eq 0 ]
    diff_output_0=${lines[0]}

    run dolt sql -q "CALL DOLT_PATCH('firstbranch','newbranch')"
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "statement" ]] || false
    [[ "${lines[3]}" =~ "$diff_output_0" ]] || false
    [[ ! "$output" =~ "DELETE FROM" ]] || false
}

@test "sql-patch: reconciles RENAME TABLE with schema changes" {
    dolt checkout -b firstbranch
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
    dolt sql -q 'insert into test values (1,1,1,1,1,1)'
    dolt add .
    dolt commit -m "created table"

    dolt checkout -b newbranch
    dolt sql -q 'ALTER TABLE test RENAME COLUMN c2 to col2'
    dolt sql -q 'ALTER TABLE test ADD COLUMN c6 int'
    dolt sql -q='RENAME TABLE test TO newname'
    dolt sql -q 'ALTER TABLE newname DROP COLUMN c3'
    dolt sql -q 'insert into newname values (2,1,1,1,1,1)'
    dolt add .
    dolt commit -m "renamed table and added data"

    run dolt diff -r sql firstbranch newbranch
    [ "$status" -eq 0 ]
    diff_output_0=${lines[0]}
    diff_output_1=${lines[1]}
    diff_output_2=${lines[2]}
    diff_output_3=${lines[3]}

    run dolt sql -q "CALL DOLT_PATCH('firstbranch','newbranch')"
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "statement" ]] || false
    [[ "${lines[3]}" =~ "$diff_output_0" ]] || false
    [[ "${lines[4]}" =~ "$diff_output_1" ]] || false
    [[ "${lines[5]}" =~ "$diff_output_2" ]] || false
    [[ "${lines[6]}" =~ "$diff_output_3" ]] || false
}

@test "sql-patch: diff sql recreates tables with all types" {
    dolt checkout -b firstbranch
    dolt checkout -b newbranch
    dolt sql <<SQL
CREATE TABLE test (
  \`pk\` BIGINT NOT NULL COMMENT 'tag:0',
  \`int\` BIGINT COMMENT 'tag:1',
  \`string\` LONGTEXT COMMENT 'tag:2',
  \`boolean\` BOOLEAN COMMENT 'tag:3',
  \`float\` DOUBLE COMMENT 'tag:4',
  \`uint\` BIGINT UNSIGNED COMMENT 'tag:5',
  \`uuid\` CHAR(36) CHARACTER SET ascii COLLATE ascii_bin COMMENT 'tag:6',
  PRIMARY KEY (pk)
);
SQL
    # dolt table import -u test `batshelper 1pksupportedtypes.csv`
    dolt add .
    dolt commit -m "created new table"

    run dolt diff -r sql firstbranch newbranch
    [ "$status" -eq 0 ]
    diff_output_0=${lines[0]}
    diff_output_1=${lines[1]}
    diff_output_2=${lines[2]}
    diff_output_3=${lines[3]}
    diff_output_4=${lines[4]}
    diff_output_5=${lines[5]}
    diff_output_6=${lines[6]}
    diff_output_7=${lines[7]}
    diff_output_8=${lines[8]}
    diff_output_9=${lines[9]}

    run dolt sql -q "CALL DOLT_PATCH('firstbranch','newbranch')"
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "statement" ]] || false
    [[ "${lines[3]}" =~ "$diff_output_0" ]] || false
    [[ "${lines[4]}" =~ "$diff_output_1" ]] || false
    [[ "${lines[5]}" =~ "$diff_output_2" ]] || false
    [[ "${lines[6]}" =~ "$diff_output_3" ]] || false
    [[ "${lines[7]}" =~ "$diff_output_4" ]] || false
    [[ "${lines[8]}" =~ "$diff_output_5" ]] || false
    [[ "${lines[9]}" =~ "$diff_output_6" ]] || false
    [[ "${lines[10]}" =~ "$diff_output_7" ]] || false
    [[ "${lines[11]}" =~ "$diff_output_8" ]] || false
    [[ "${lines[12]}" =~ "$diff_output_9" ]] || false
}

@test "sql-patch: reconciles multi PRIMARY KEY and FOREIGN KEY" {
        dolt sql <<SQL
CREATE TABLE parent (
    id int PRIMARY KEY,
    id_ext int,
    v1 int,
    v2 text COMMENT 'tag:1',
    INDEX v1 (v1)
);
CREATE TABLE child (
    id int primary key,
    v1 int
);
SQL
    dolt sql -q "ALTER TABLE child ADD CONSTRAINT fk_named FOREIGN KEY (v1) REFERENCES parent(v1);"
    dolt sql -q "insert into parent values (0, 1, 2, NULL);"
    dolt sql -q "ALTER TABLE parent DROP PRIMARY KEY"
    dolt sql -q "ALTER TABLE parent ADD PRIMARY KEY(id, id_ext);"

    run dolt diff -r sql
    [ "$status" -eq 0 ]
    diff_output_0=${lines[0]}
    diff_output_1=${lines[1]}
    diff_output_2=${lines[2]}
    diff_output_3=${lines[3]}
    diff_output_4=${lines[4]}
    diff_output_5=${lines[5]}
    diff_output_6=${lines[6]}
    diff_output_7=${lines[7]}
    diff_output_8=${lines[8]}
    diff_output_9=${lines[9]}
    diff_output_10=${lines[10]}
    diff_output_11=${lines[11]}
    diff_output_12=${lines[12]}
    diff_output_13=${lines[13]}
    diff_output_14=${lines[14]}
    diff_output_15=${lines[15]}

    run dolt sql -q "CALL DOLT_PATCH()"
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "statement" ]] || false
    [[ "${lines[3]}" =~ "$diff_output_0" ]] || false
    [[ "${lines[4]}" =~ "$diff_output_1" ]] || false
    [[ "${lines[5]}" =~ "$diff_output_2" ]] || false
    [[ "${lines[6]}" =~ "$diff_output_3" ]] || false
    [[ "${lines[7]}" =~ "$diff_output_4" ]] || false
    [[ "${lines[8]}" =~ "$diff_output_5" ]] || false
    [[ "${lines[9]}" =~ "$diff_output_6" ]] || false
    [[ "${lines[10]}" =~ "$diff_output_7" ]] || false
    [[ "${lines[11]}" =~ "$diff_output_8" ]] || false
    [[ "${lines[12]}" =~ "$diff_output_9" ]] || false
    [[ "${lines[13]}" =~ "$diff_output_10" ]] || false
    [[ "${lines[14]}" =~ "$diff_output_11" ]] || false
    [[ "${lines[15]}" =~ "$diff_output_12" ]] || false
    [[ "${lines[16]}" =~ "$diff_output_13" ]] || false
    [[ "${lines[17]}" =~ "$diff_output_14" ]] || false
    [[ "${lines[18]}" =~ "$diff_output_15" ]] || false
}

@test "sql-patch: reconciles CHECK CONSTRAINTS" {
    dolt sql <<SQL
create table foo (
    pk int,
    c1 int,
    CHECK (c1 > 3),
    PRIMARY KEY (pk)
);
SQL

    run dolt diff -r sql
    [ "$status" -eq 0 ]
    diff_output_0=${lines[0]}
    diff_output_1=${lines[1]}
    diff_output_2=${lines[2]}
    diff_output_3=${lines[3]}
    diff_output_4=${lines[4]}
    diff_output_5=${lines[5]}

    run dolt sql -q "CALL DOLT_PATCH()"
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "statement" ]] || false
    [[ "${lines[3]}" =~ "$diff_output_0" ]] || false
    [[ "${lines[4]}" =~ "$diff_output_1" ]] || false
    [[ "${lines[5]}" =~ "$diff_output_2" ]] || false
    [[ "${lines[6]}" =~ "$diff_output_3" ]] || false
    [[ "${lines[7]}" =~ "$diff_output_4" ]] || false
    [[ "${lines[8]}" =~ "$diff_output_5" ]] || false
}

@test "sql-patch: any error causing no data diff is shown as warnings." {
        dolt sql <<SQL
CREATE TABLE parent (
    id int PRIMARY KEY,
    id_ext int,
    v1 int,
    v2 text COMMENT 'tag:1',
    INDEX v1 (v1)
);
CREATE TABLE child (
    id int primary key,
    v1 int
);
SQL
    dolt commit -Am "add tables"
    dolt sql -q "ALTER TABLE child ADD CONSTRAINT fk_named FOREIGN KEY (v1) REFERENCES parent(v1);"
    dolt sql -q "insert into parent values (0, 1, 2, NULL);"
    dolt sql -q "ALTER TABLE parent DROP PRIMARY KEY"
    dolt sql -q "ALTER TABLE parent ADD PRIMARY KEY(id, id_ext);"

    run dolt diff -r sql child
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Incompatible schema change, skipping data diff for table 'child'" ]] || false
    diff_output_0=${lines[0]}
    diff_output_1=${lines[1]}

    run dolt sql -q "CALL DOLT_PATCH('child'); SHOW WARNINGS;"
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "statement" ]] || false
    [[ "${lines[3]}" =~ "$diff_output_0" ]] || false
    [[ "${lines[4]}" =~ "$diff_output_1" ]] || false
    [[ "$output" =~ "Incompatible schema change, skipping data diff for table 'child'" ]] || false

    run dolt diff -r sql parent
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Primary key sets differ between revisions for table 'parent', skipping data diff" ]] || false
    diff_output_0=${lines[0]}
    diff_output_1=${lines[1]}

    run dolt sql -q "CALL DOLT_PATCH('parent'); SHOW WARNINGS;"
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "statement" ]] || false
    [[ "${lines[3]}" =~ "$diff_output_0" ]] || false
    [[ "${lines[4]}" =~ "$diff_output_1" ]] || false
    [[ "$output" =~ "Primary key sets differ between revisions for table 'parent', skipping data diff" ]] || false
}
