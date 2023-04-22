#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common

    dolt sql <<SQL
CREATE TABLE _dolt_ignore (
  pattern VARCHAR(max) PRIMARY KEY,
  ignored BOOLEAN NOT NULL
);
INSERT INTO _dolt_ignore VALUES
  ("ignoreme", true),
  ("dontignore", false),

  ("*_ignore", true),
  ("do_not_ignore", false),

  ("commit_*", false),
  ("commit_me_not", true),

  ("commit_?", true);
SQL

# commit_ignore is ambiguous

}

teardown() {
    assert_feature_version
    teardown_common
}

get_staged_tables() {
    dolt status | awk '
        match($0, /new table:\ */) { print substr($0, RSTART+RLENGTH) }
        /Untracked files:/ { exit }
    '
}

get_working_tables() {
    dolt status | awk '
        BEGIN { working = 0 }
        (working == 1) && match($0, /new table:\ */) { print substr($0, RSTART+RLENGTH) }
        /Untracked files:/ { working = 1 }
    '
}

@test "dolt_ignore simple matches" {

    dolt sql <<SQL
CREATE TABLE ignoreme (pk int);
CREATE TABLE dontignore (pk int);
SQL

    dolt add -A

    working=$(get_working_tables)
    staged=$(get_staged_tables)

    [[ ! -z $(echo "$working" | grep "ignoreme") ]] || false
    [[ ! -z $(echo "$staged" | grep "dontignore") ]] || false
}

@test "dolt_ignore specific overrides" {

    dolt sql <<SQL
CREATE TABLE please_ignore (pk int);
CREATE TABLE do_not_ignore (pk int);
CREATE TABLE commit_me (pk int);
CREATE TABLE commit_me_not(pk int);
SQL

    dolt add -A

    working=$(get_working_tables)
    staged=$(get_staged_tables)

    [[ ! -z $(echo "$working" | grep "please_ignore") ]] || false
    [[ ! -z $(echo "$staged" | grep "do_not_ignore") ]] || false
    [[ ! -z $(echo "$staged" | grep "commit_me") ]] || false
    [[ ! -z $(echo "$working" | grep "commit_me_not") ]] || false
}

@test "dolt_ignore conflict" {

    dolt sql <<SQL
CREATE TABLE commit_ignore (pk int);
SQL

    run dolt add -A

    [ "$status" -eq 1 ]
    [ "$output" =~ "dolt_ignore has multiple conflicting rules for commit_ignore"

}

@test "dolt_ignore question mark" {
    dolt sql <<SQL
CREATE TABLE commit_1 (pk int);
CREATE TABLE commit_11 (pk int);
SQL

    dolt add -A

    working=$(get_working_tables)
    staged=$(get_staged_tables)

    [[ ! -z $(echo "$working" | grep "commit_1") ]] || false
    [[ ! -z $(echo "$staged" | grep "commit_11") ]] || false
}