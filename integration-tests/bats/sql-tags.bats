#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common

    dolt sql <<SQL
CREATE TABLE test (
    pk int primary key
);
INSERT INTO test VALUES (0),(1),(2);
SQL
    dolt add .
    dolt commit -m "created table test"
    dolt sql <<SQL
DELETE FROM test WHERE pk = 0;
INSERT INTO test VALUES (3);
SQL
    dolt add .
    dolt commit -m "made changes"
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "sql-tags: DOLT_TAG works with a explicit ref" {
    run dolt sql -q "CALL DOLT_TAG('v1', 'HEAD^')"
    [ $status -eq 0 ]
    run dolt tag
    [ $status -eq 0 ]
    [[ "$output" =~ "v1" ]] || false
}

@test "sql-tags: DOLT_TAG works with implicit head ref" {
    run dolt sql -q "CALL DOLT_TAG('v1')"
    [ $status -eq 0 ]
    run dolt tag
    [ $status -eq 0 ]
    [[ "$output" =~ "v1" ]] || false
}

@test "sql-tags: DOLT_TAG works with author arg defined" {
    run dolt sql -q "CALL DOLT_TAG('v1', '--author', 'Jane Doe <jane@doe.com>')"
    [ $status -eq 0 ]
    run dolt tag -v
    [[ "$output" =~ "v1" ]] || false
    [[ "$output" =~ "Tagger: Jane Doe <jane@doe.com>" ]] || false
}

@test "sql-tags: create tag v1.2.3" {
    skip "Noms doesn't support '.' in dataset names"
    run dolt sql -q "CALL DOLT_TAG('v1.2.3')"
    [ $status -eq 0 ]
}

@test "sql-tags: delete a tag" {
    dolt sql -q "CALL DOLT_TAG('v1')"
    dolt sql -q "CALL DOLT_TAG('-d','v1')"
    run dolt tag
    [ $status -eq 0 ]
    [ "$output" = "" ]
}

@test "sql-tags: use a tag as ref for diff" {
    dolt sql -q "CALL DOLT_TAG('v1', 'HEAD^')"
    run dolt diff v1
    [ $status -eq 0 ]
    [[ "$output" =~ "- | 0" ]]
    [[ "$output" =~ "+ | 3" ]]
}

@test "sql-tags: use a tag as a ref for merge" {
    dolt sql -q "CALL DOLT_TAG('v1', 'HEAD')"
    dolt checkout -b other HEAD^
    dolt sql -q "insert into test values (8),(9)"
    dolt add -A && dolt commit -m 'made changes'
    run dolt merge v1
    [ $status -eq 0 ]
    run dolt sql -q "select * from test"
    [ $status -eq 0 ]
    [[ "$output" =~ "1" ]]
    [[ "$output" =~ "2" ]]
    [[ "$output" =~ "3" ]]
    [[ "$output" =~ "8" ]]
    [[ "$output" =~ "9" ]]
}
