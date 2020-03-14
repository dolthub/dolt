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
    [[ "$output" =~ "A column with the tag 8910 already exists in table test." ]] || false
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

    # Adding the tag back with a different type should error
    skip "No checks for tag reuse across commits right now"
    run dolt sql -q "alter table test add column c1 text comment 'tag:5678'"
    [ $status -ne 0 ]
    run dolt sql -q "alter table test add column c2 text comment 'tag:5678'"
    [ $status -ne 0 ]
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
    skip "Dropping and recreating tables does not enforce tag reuse rules"
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:1234',
  c1 BIGINT COMMENT 'tag:5678',
  PRIMARY KEY (pk));
SQL
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
    dolt commit -m "Added column c2 bigint tag:8910"
    dolt checkout master
    dolt merge branch1
    run dolt merge branch2
    [ $status -ne 0 ]
}

