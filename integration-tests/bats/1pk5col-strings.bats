#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    dolt sql <<SQL
CREATE TABLE test (
  pk varchar(20) NOT NULL COMMENT 'tag:0',
  c1 LONGTEXT COMMENT 'tag:1',
  c2 LONGTEXT COMMENT 'tag:2',
  c3 LONGTEXT COMMENT 'tag:3',
  c4 LONGTEXT COMMENT 'tag:4',
  c5 LONGTEXT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "1pk5col-strings: dolt sql with string comparison operators" {
    dolt sql -q "insert into test values ('tim', 'is', 'super', 'duper', 'rad', 'fo sho')"
    dolt sql -q "insert into test values ('zach', 'is', 'super', 'duper', 'not', 'rad')"
    dolt sql -q "insert into test values ('this', 'test', 'is', 'a', 'good', 'test')"
    run dolt sql -q "select * from test"
    [ "$status" -eq 0 ]
    # All row counts are offset by 4 to account for table printing
    [ "${#lines[@]}" -eq 7 ]
    run dolt sql -q "select * from test where pk='tim'"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    run dolt sql -q "select * from test where pk>'tim'"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    run dolt sql -q "select * from test where pk>='tim'"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]
    run dolt sql -q "select * from test where pk<>'tim'"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]
    run dolt sql -q "select * from test where pk='bob'"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 0 ]
}

@test "1pk5col-strings: interact with a strings type table with sql" {
    run dolt sql -q "insert into test (pk,c1,c2,c3,c4,c5) values ('tim','is','super','duper','rad','fo sho')"
    [ "$status" -eq 0 ]
    [[ "$output" = "" ]] || false
    run dolt sql -q "select * from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "c5" ]] || false
    [[ "$output" =~ "tim" ]] || false
    run dolt sql -q "select pk,c1,c4 from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "c4" ]] || false
    [[ "$output" =~ "tim" ]] || false
    [[ ! "$output" =~ "super" ]] || false
}

@test "1pk5col-strings: insert must use quoted strings" {
    run dolt sql -q "insert into test (pk,c1,c2,c3,c4,c5) values (tim,is,super,duper,rad,'fo sho')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Error parsing SQL" ]] || false
}

@test "1pk5col-strings: create and view a table with NULL and empty string values" {
    dolt sql -q "insert into test values ('tim', '', '', '', '', '')"
    dolt sql -q "insert into test (pk) values ('aaron')"
    dolt sql -q "insert into test (pk) values ('brian')"
    run dolt sql -q "select * from test"
    [ "$status" -eq 0 ]
    # select orders by primary key right now so aaron, brian, tim
    [[ "${lines[4]}" =~ "NULL" ]] || false
    [[ ! "${lines[5]}" =~ "NULL" ]] || false
    doltselectoutput=$output
    run dolt sql -q "select * from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "NULL" ]] || false
    [ "$output" = "$doltselectoutput" ]
    # Make sure we don't get a table with no spaces because that bug was
    # generated when making changes to NULL printing
    [[ ! "$output" =~ "|||||" ]] || false
}

@test "1pk5col-strings: semicolons in quoted sql statements" {
    run dolt sql -q "insert into test (pk,c1) values ('test', 'this; should; work')"
    [ "$status" -eq 0 ]
    run dolt sql <<< "insert into test (pk,c1) values ('test2', 'this; should; work')"
    [ "$status" -eq 0 ]
    [[ "$output" = "" ]] || false
    run dolt sql <<< "insert into test (pk,c1) values ('test3', 'this \\\\'' should \\\\'' work')"
    [ "$status" -eq 0 ]
    [[ "$output" = "" ]] || false
}
