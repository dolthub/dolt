#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}

@test "Manually specifying tag numbers" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:1234',
  c1 BIGINT COMMENT 'tag:5678',
  PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ $status -eq 0 ]
    [[ "$output" =~ "tag:1234" ]] || false
    [[ "$output" =~ "tag:5678" ]] || false
}

@test "Users cannot partially specify tag numbers" {
    run dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:1234',
  c1 BIGINT,
  PRIMARY KEY (pk)
);
SQL
    [ $status -ne 0 ]
    [[ "$output" =~ "must define tags for all or none of the schema columns" ]] || false
}

@test "Renaming a column should preserve the tag number" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:1234',
  c1 BIGINT COMMENT 'tag:5678',
  PRIMARY KEY (pk));
SQL
    dolt sql -q "alter table test rename column c1 to c0"
    run dolt schema show
    [ $status -eq 0 ]
    [[ "$output" =~ "tag:1234" ]] || false
    [[ "$output" =~ "tag:5678" ]] || false
}

@test "Renaming a table should preserve the tag number" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:1234',
  c1 BIGINT COMMENT 'tag:5678',
  PRIMARY KEY (pk));
SQL
    dolt sql -q "alter table test rename to new_name"
    run dolt schema show
    [ $status -eq 0 ]
    [[ "$output" =~ "new_name" ]] || false
    [[ "$output" =~ "tag:1234" ]] || false
    [[ "$output" =~ "tag:5678" ]] || false
}

@test "Reusing a tag number should fail in create table" {
    run dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:1234',
  c1 BIGINT COMMENT 'tag:1234',
  PRIMARY KEY (pk));
SQL
    [ $status -ne 0 ]
    [[ "$output" =~ "two different columns with the same tag" ]] || false
}

@test "Alter table should not allow duplicate tags" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:1234',
  c1 BIGINT COMMENT 'tag:5678',
  PRIMARY KEY (pk));
SQL
    run dolt sql -q "alter table test add column c0 bigint comment 'tag:8910'"
    run dolt schema show
    [ $status -eq 0 ]
    [[ "$output" =~ "tag:1234" ]] || false
    [[ "$output" =~ "tag:5678" ]] || false
    [[ "$output" =~ "tag:8910" ]] || false
    run dolt sql -q "alter table test add column c2 bigint comment 'tag:8910'"
    [ $status -ne 0 ]
    [[ "$output" =~ "Cannot create column c2, the tag 8910 was already used in table test" ]] || false
}

@test "Cannot reuse tag number of deleted column" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:1234',
  c1 BIGINT COMMENT 'tag:5678',
  PRIMARY KEY (pk));
SQL
    dolt add .
    dolt commit -m 'create table test'
    dolt sql -q 'alter table test drop column c1'
    dolt add .
    dolt commit -m 'dropped column c1'
    run dolt sql -q "alter table test add column c2 int comment 'tag:5678'"
    [ $status -ne 0 ]
    [[ "$output" =~ "Cannot create column c2, the tag 5678 was already used in table test" ]] || false
}

@test "Cannot reuse tag number of deleted column after table rename" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:1234',
  c1 BIGINT COMMENT 'tag:5678',
  PRIMARY KEY (pk));
SQL
    dolt add .
    dolt commit -m 'create table test'
    dolt sql -q 'alter table test drop column c1'
    dolt add .
    dolt commit -m 'dropped column c1'
    dolt sql -q 'alter table test rename to new_name'
    run dolt sql -q "alter table new_name add column c2 int comment 'tag:5678'"
    [ $status -ne 0 ]
    [[ "$output" =~ "Cannot create column c2, the tag 5678 was already used in table new_name" ]] || false
}

@test "Cannot reuse tag number of deleted table" {
    dolt sql <<SQL
CREATE TABLE aaa (
  pk INT NOT NULL COMMENT 'tag:1234',
  PRIMARY KEY (pk));
SQL
    dolt sql <<SQL
CREATE TABLE bbb (
  pk INT NOT NULL COMMENT 'tag:5678',
  PRIMARY KEY (pk));
SQL
    dolt add .
    dolt commit -m 'created tables aaa and bbb'
    dolt sql -q 'drop table aaa'
    dolt add .
    dolt commit -m 'dropped table aaa'
    run dolt sql <<SQL
CREATE TABLE new_table (
  pk BIGINT NOT NULL COMMENT 'tag:1234',
  PRIMARY KEY (pk));
SQL
    [ $status -ne 0 ]
    [[ "$output" =~ "Cannot create column pk, the tag 1234 was already used in table aaa" ]] || false
    run dolt sql -q "alter table bbb add column c1 int comment 'tag:1234'"
    [ $status -ne 0 ]
    [[ "$output" =~ "Cannot create column c1, the tag 1234 was already used in table aaa" ]] || false
}

@test "Should not be able to reuse a committed tag number on a column with a different type" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:1234',
  c1 BIGINT COMMENT 'tag:5678',
  PRIMARY KEY (pk));
SQL
    dolt add test
    dolt commit -m "Committed test table"
    dolt sql -q "alter table test drop column c1"
    dolt add test
    dolt commit	-m "Committed test table with c1 dropped"

    # Adding the tag back with the same name and type should not be allowed
    run dolt sql -q "alter table test add column c1 bigint comment 'tag:5678'"
    [ $status -eq 1 ]

    # Adding the tag back with a different name but same type should not be allowed
    run dolt sql -q "alter table test add column c2 bigint comment 'tag:5678'"
    [ $status -eq 1 ]

    # Adding the tag back with a different type should error
    run dolt sql -q "alter table test add column c1 text comment 'tag:5678'"
    [ $status -ne 0 ]
    run dolt sql -q "alter table test add column c2 text comment 'tag:5678'"
    [ $status -ne 0 ]
}

@test "Can drop and readd table before committing" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:1234',
  c1 BIGINT COMMENT 'tag:5678',
  PRIMARY KEY (pk));
SQL
    run dolt sql -q 'drop table test'
    [ $status -eq 0 ]
    run dolt sql <<SQL
CREATE TABLE new_name (
  pk INT NOT NULL COMMENT 'tag:1234',
  c1 INT COMMENT 'tag:5678',
  PRIMARY KEY (pk));
SQL
    [ $status -eq 0 ]
    run dolt schema show
    [ $status -eq 0 ]
    [[ "$output" =~ "new_name" ]] || false
    [[ "$output" =~ "tag:1234" ]] || false
    [[ "$output" =~ "tag:5678" ]] || false
}

@test "Drop and create table should enforce tag reuse rules across versions" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:1234',
  c1 BIGINT COMMENT 'tag:5678',
  PRIMARY KEY (pk));
SQL
    dolt add test
    dolt commit	-m "Committed test table"
    dolt sql -q "drop table test"
    run dolt sql <<SQL
CREATE TABLE test (
  pk LONGTEXT  NOT NULL COMMENT 'tag:1234',
  c1 LONGTEXT COMMENT 'tag:5678',
  PRIMARY KEY (pk));
SQL
    [ $status -ne 0 ]
}

@test "Merging two branches that added same tag, name, type, and constraints" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:1234',
  c1 BIGINT COMMENT 'tag:5678',
  PRIMARY KEY (pk));
SQL
    dolt add test
    dolt commit -m "Committed test table"
    dolt branch branch1
    dolt branch branch2
    dolt checkout branch1
    dolt sql -q "alter table test add column c2 bigint comment 'tag:8910'"
    dolt add test
    dolt commit -m "Added column c2 bigint tag:8910"
    dolt checkout branch2
    dolt sql -q "alter table test add column c2 bigint comment 'tag:8910'"
    dolt add test
    dolt commit	-m "Added column c2 bigint tag:8910"
    dolt checkout master
    dolt merge branch1
    dolt merge branch2
}

@test "Merging branches that use the same tag referring to different schema fails" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:1234',
  c1 BIGINT COMMENT 'tag:5678',
  PRIMARY KEY (pk));
SQL
    dolt add test
    dolt commit	-m "Committed test table"
    dolt branch	branch1
    dolt branch	branch2
    dolt checkout branch1
    dolt sql -q "alter table test add column c2 bigint comment 'tag:8910'"
    dolt add test
    dolt commit	-m "Added column c2 bigint tag:8910"
    dolt checkout branch2
    dolt sql -q "alter table test add column c2 longtext comment 'tag:8910'"
    dolt add test
    dolt commit -m "Added column c2 longtext tag:8910"
    dolt checkout master
    dolt merge branch1
    run dolt merge branch2
    [ $status -ne 0 ]
}

@test "Merging branches that use the same tag referring to different column names fails" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:1234',
  c1 BIGINT COMMENT 'tag:5678',
  PRIMARY KEY (pk));
SQL
    dolt add test
    dolt commit -m "Committed test table"
    dolt branch branch1
    dolt branch branch2
    dolt checkout branch1
    dolt sql -q "alter table test add column c2 bigint comment 'tag:8910'"
    dolt add test
    dolt commit -m "Added column c2 bigint tag:8910"
    dolt checkout branch2
    dolt sql -q "alter table test add column c0 bigint comment 'tag:8910'"
    dolt add test
    dolt commit -m "Added column c0 bigint tag:8910"
    dolt checkout master
    dolt merge branch1
    run dolt merge branch2
    [ $status -eq 1 ]
}

@test "Merging branches that both created the same column succeeds" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  PRIMARY KEY (pk));
SQL
    dolt add test
    dolt commit -m "Committed test table"
    dolt branch branch1
    dolt branch branch2
    dolt checkout branch1
    dolt sql -q "alter table test add column c2 bigint comment 'tag:2'"
    dolt sql -q "alter table test add column c3 double"
    dolt add test
    dolt commit -m "Added columns c2 bigint tag:8910 and c3 double to branch1"
    dolt checkout branch2
    dolt sql -q "alter table test add column c2 bigint comment 'tag:2'"
    # column c3 will have the same tag on both branches due to deterministic tag generation
    dolt sql -q "alter table test add column c3 double"
    dolt add test
    dolt commit -m "Added columns c2 bigint tag:8910 and c3 double to branch2"
    dolt checkout master
    dolt merge branch1
    run dolt merge branch2
    [ $status -eq 0 ]
    run dolt schema show
    [[ "${lines[2]}" =~ "\`pk\` BIGINT NOT NULL COMMENT 'tag:0'" ]] || false
    [[ "${lines[3]}" =~ "\`c1\` BIGINT COMMENT 'tag:1'" ]] || false
    [[ "${lines[4]}" =~ "\`c2\` BIGINT COMMENT 'tag:2'" ]] || false
    [[ "${lines[5]}" =~ "\`c3\` DOUBLE COMMENT " ]] || false
}

@test "Merging branches that both created the same table succeeds" {
    dolt branch branch1
    dolt branch branch2
    dolt checkout branch1
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  c1 BIGINT,
  PRIMARY KEY (pk));
SQL
    dolt add test
    dolt commit -m "Committed test table"

    dolt checkout branch2
dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  c1 BIGINT,
  PRIMARY KEY (pk));
SQL
    dolt add test
    # pk and c1 will have the same tags on both branches due to deterministic tag generation
    dolt commit -m "Committed test table"
    dolt checkout master
    dolt merge branch1
    run dolt merge branch2
    [ $status -eq 0 ]
    run dolt schema show
    [[ "${lines[2]}" =~ "\`pk\` BIGINT NOT NULL COMMENT " ]] || false
    [[ "${lines[3]}" =~ "\`c1\` BIGINT COMMENT " ]] || false
}

@test "Tags must be unique across tables" {
    dolt sql <<SQL
CREATE TABLE aaa (
  pk INT NOT NULL COMMENT 'tag:1234',
  PRIMARY KEY (pk));
SQL
    dolt sql <<SQL
CREATE TABLE bbb (
  pk INT NOT NULL COMMENT 'tag:5678',
  PRIMARY KEY (pk));
SQL
    run dolt sql <<SQL
CREATE TABLE test (
  pk INT NOT NULL COMMENT 'tag:1234',
  c1 INT COMMENT 'tag:5678',
  PRIMARY KEY (pk));
SQL
    [ $status -ne 0 ]
    [[ "$output" =~ "Cannot create column pk, the tag 1234 was already used in table aaa" ]] || false
    [[ "$output" =~ "Cannot create column c1, the tag 5678 was already used in table bbb" ]] || false

    run dolt sql -q "ALTER TABLE aaa ADD COLUMN c1 INT COMMENT 'tag:5678';"
    [ $status -ne 0 ]
    [[ "$output" =~ "Cannot create column c1, the tag 5678 was already used in table bbb" ]] || false
}

@test "Deterministic tag generation produces consistent results" {
    dolt branch other
    dolt sql <<SQL
CREATE TABLE test1 (
  pk1 BIGINT NOT NULL,
  c1 BIGINT,
  c2 DOUBLE,
  c3 LONGTEXT,
  PRIMARY KEY (pk1));
SQL
    dolt add test1
    dolt commit -m "Committed test table"

    # If anything changes to deterministic tag generation, this will break
    run dolt schema show
    [[ "${lines[2]}" =~ "COMMENT 'tag:10458'" ]] || false
    [[ "${lines[3]}" =~ "COMMENT 'tag:5951'" ]] || false
    [[ "${lines[4]}" =~ "COMMENT 'tag:10358'" ]] || false
    [[ "${lines[5]}" =~ "COMMENT 'tag:11314'" ]] || false
}

@test "dolt table import -c uses deterministic tag generation" {
    cat <<DELIM > data.csv
pk,c1,c2,c3,c4,c5
0,1,2,3,4,5
a,b,c,d,e,f
DELIM
    run dolt table import -c ints_table data.csv
    [ $status -eq 0 ]
    dolt schema show
    run dolt schema show
    [ $status -eq 0 ]
    [[ "${lines[2]}" =~ "COMMENT 'tag:6302'" ]] || false
    [[ "${lines[3]}" =~ "COMMENT 'tag:12880'" ]] || false
    [[ "${lines[4]}" =~ "COMMENT 'tag:15463'" ]] || false
    [[ "${lines[5]}" =~ "COMMENT 'tag:14526'" ]] || false
    [[ "${lines[6]}" =~ "COMMENT 'tag:5634'" ]] || false
    [[ "${lines[7]}" =~ "COMMENT 'tag:12796'" ]] || false
}
