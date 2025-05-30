#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}


@test "merge-3way-schema-changes: add a NOT NULL column with default value on a branch" {
    dolt sql -q "create table t (pk int primary key);"
    dolt commit -Am "ancestor"

    dolt checkout -b right
    dolt sql -q "insert into t values (1);"
    dolt sql -q "alter table t add column col1 int not null default 0;"
    dolt commit -am "right"

    dolt checkout main
    dolt sql -q "insert into t values (2);"
    dolt commit -am "left"

    dolt merge right

    run dolt sql -q "select * from t" -r csv
    log_status_eq 0
    [[ "$output" =~ "1,0" ]] || false
    [[ "$output" =~ "2,0" ]] || false
}

@test "merge-3way-schema-changes: migrate a schema merge too large to fit in a MutableMap" {
  # MutableMap will cache 64K changes before flushing, so creating a table with more than 64K rows

  dolt sql -q "create table test_table(pk int primary key, t varchar(20))"
  python <<PYTHON | dolt table import -u test_table
import csv
import sys
writer = csv.writer(sys.stdout)
writer.writerow(["pk", "t"])
for i in range(70_000):
  writer.writerow([i, "hello world"])
PYTHON
  dolt add .
  dolt commit -m "create table"

  dolt branch schema_change
  dolt branch data_change

  dolt checkout schema_change
  dolt sql -q "alter table test_table modify column t text"
  dolt add .
  dolt commit -m "modify column t to text"

  dolt checkout data_change
  dolt sql -q "update test_table set t = 'new text'"
  dolt add .
  dolt commit -m "update column t"

  # Test that merge completes successfully and has the expected data
  dolt merge schema_change
  run dolt sql -q "show create table test_table"
  log_status_eq 0
  [[ "$output" =~ '`t` text,' ]] || false
  run dolt sql -q "select count(*) from test_table where t = 'new text'"
  log_status_eq 0
  echo "$output"
  [[ "$output" =~ '70000' ]] || false
}