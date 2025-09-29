#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common

    dolt sql <<SQL
INSERT INTO dolt_ignore VALUES
  ("ignoreme", true),
  ("dontignore", false),

  ("*_ignore", true),
  ("do_not_ignore", false),

  ("%_ignore_too", true),

  ("commit_*", false),
  ("commit_me_not", true),

  ("test*", true),
  ("test?*", false),
  ("test?", true);
SQL

}

teardown() {
    assert_feature_version
    teardown_common
}

get_staged_tables() {
    dolt status | awk '
        match($0, /new table:\ */) { print substr($0, RSTART+RLENGTH) }
        /Untracked tables:/ { exit }
        /Tables with conflicting dolt_ignore patterns:/ { exit }
    '
}

get_working_tables() {
    dolt status | awk '
        BEGIN { working = 0 }
        (working == 1) && match($0, /new table:\ */) { print substr($0, RSTART+RLENGTH) }
        /Untracked tables:/ { working = 1 }
        /Tables with conflicting dolt_ignore patterns:/ { working = 0 }
    '
}

get_ignored_tables() {
    dolt status --ignored | awk '
        BEGIN { working = 0 }
        (working == 1) && match($0, /new table:\ */) { print substr($0, RSTART+RLENGTH) }
        /Ignored tables:/ { working = 1 }
        /Tables with conflicting dolt_ignore patterns:/ { working = 0 }
    '
}

get_conflict_tables() {
    dolt status | awk '
        BEGIN { working = 0 }
        (working == 1) && match($0, /new table:\ */) { print substr($0, RSTART+RLENGTH) }
        /Tables with conflicting dolt_ignore patterns:/ { working = 1 }
    '
}



@test "ignore: allow using dolt_ignore with AS OF" {

    dolt branch start


    dolt sql -q "INSERT INTO dolt_ignore VALUES ('dolt_ignore', false)"
    dolt sql -q "INSERT INTO dolt_ignore VALUES ('test1', true)"

    dolt add dolt_ignore
    dolt commit -m "Insert into dolt_ignore"

    dolt sql -q "INSERT INTO dolt_ignore VALUES ('test2', true)"
    dolt add dolt_ignore

    dolt sql -q "INSERT INTO dolt_ignore VALUES ('test3', true)"

    run dolt sql -q "SELECT * FROM dolt_ignore AS OF 'WORKING'"

    [ "$status" -eq 0 ]
    [[ "$output" =~ "test1" ]] || false
    [[ "$output" =~ "test2" ]] || false
    [[ "$output" =~ "test3" ]] || false

    run dolt sql -q "SELECT * FROM dolt_ignore AS OF 'STAGED'"

    [ "$status" -eq 0 ]
    [[ "$output" =~ "test1" ]] || false
    [[ "$output" =~ "test2" ]] || false
    [[ ! "$output" =~ "test3" ]] || false

    run dolt sql -q "SELECT * FROM dolt_ignore AS OF 'HEAD'"

    [ "$status" -eq 0 ]
    [[ "$output" =~ "test1" ]] || false
    [[ ! "$output" =~ "test2" ]] || false
    [[ ! "$output" =~ "test3" ]] || false

    run dolt sql -q "SELECT * FROM dolt_ignore AS OF 'main'"

    [ "$status" -eq 0 ]
    [[ "$output" =~ "test1" ]] || false
    [[ ! "$output" =~ "test2" ]] || false
    [[ ! "$output" =~ "test3" ]] || false

    run dolt sql -q "SELECT * FROM dolt_ignore AS OF 'start'"

    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "test1" ]] || false
    [[ ! "$output" =~ "test2" ]] || false
    [[ ! "$output" =~ "test3" ]] || false

    run dolt sql -q "SELECT * FROM dolt_ignore AS OF 'HEAD^'"

    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "test1" ]] || false
    [[ ! "$output" =~ "test2" ]] || false
    [[ ! "$output" =~ "test3" ]] || false

}

@test "ignore: dolt_diff_summary respects dolt_ignore patterns" {
    # First commit some initial tables
    dolt sql -q "CREATE TABLE initial_table (pk int primary key)"
    dolt add .
    dolt commit -m "Add initial table"

    # Add ignore pattern and create new tables
    dolt sql -q "INSERT INTO dolt_ignore VALUES ('should_ignore_new', true)"
    dolt sql -q "CREATE TABLE should_ignore_new (pk int primary key)"
    dolt sql -q "CREATE TABLE should_not_ignore_new (pk int primary key)"

    # Test that dolt_diff_summary respects ignore patterns - should only show non-ignored tables
    run dolt sql -q "SELECT COUNT(*) FROM dolt_diff_summary('HEAD', 'WORKING')" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1" ]] || false

    # Verify which table is shown
    run dolt sql -q "SELECT to_table_name FROM dolt_diff_summary('HEAD', 'WORKING')" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "should_not_ignore_new" ]] || false
    [[ ! "$output" =~ "should_ignore_new" ]] || false
}

@test "ignore: dolt_diff_summary respects wildcard ignore patterns" {
    # First commit initial table
    dolt sql -q "CREATE TABLE initial (pk int primary key)"
    dolt add .
    dolt commit -m "Add initial table"

    # Add wildcard ignore pattern and create new tables
    dolt sql -q "INSERT INTO dolt_ignore VALUES ('temp_*', true)"
    dolt sql -q "CREATE TABLE temp_new_table (pk int primary key)"
    dolt sql -q "CREATE TABLE regular_new_table (pk int primary key)"

    # Should only show the regular table
    run dolt sql -q "SELECT COUNT(*) FROM dolt_diff_summary('HEAD', 'WORKING')" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1" ]] || false

    run dolt sql -q "SELECT to_table_name FROM dolt_diff_summary('HEAD', 'WORKING')" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "regular_new_table" ]] || false
    [[ ! "$output" =~ "temp_new_table" ]] || false
}

@test "ignore: dolt_diff_summary respects ignore patterns for dropped tables" {
    # Create and commit tables that will be dropped
    dolt sql -q "CREATE TABLE will_be_ignored (pk int primary key)"
    dolt sql -q "CREATE TABLE will_not_be_ignored (pk int primary key)"
    dolt add .
    dolt commit -m "Add tables to be dropped"

    # Add ignore pattern and drop tables
    dolt sql -q "INSERT INTO dolt_ignore VALUES ('will_be_ignored', true)"
    dolt sql -q "DROP TABLE will_be_ignored"
    dolt sql -q "DROP TABLE will_not_be_ignored"

    # Should only show the non-ignored dropped table
    run dolt sql -q "SELECT COUNT(*) FROM dolt_diff_summary('HEAD', 'WORKING')" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1" ]] || false

    run dolt sql -q "SELECT from_table_name, diff_type FROM dolt_diff_summary('HEAD', 'WORKING')" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "will_not_be_ignored" ]] || false
    [[ "$output" =~ "dropped" ]] || false
    [[ ! "$output" =~ "will_be_ignored" ]] || false
}

@test "ignore: dolt_diff_summary specific table query respects ignore patterns" {
    # Create and commit initial table
    dolt sql -q "CREATE TABLE initial_table (pk int primary key)"
    dolt add .
    dolt commit -m "Add initial table"

    # Add ignore pattern and create new tables
    dolt sql -q "INSERT INTO dolt_ignore VALUES ('ignored_table_new', true)"
    dolt sql -q "CREATE TABLE ignored_table_new (pk int primary key)"
    dolt sql -q "CREATE TABLE not_ignored_table (pk int primary key)"

    # Specific query for ignored table should return empty
    run dolt sql -q "SELECT COUNT(*) FROM dolt_diff_summary('HEAD', 'WORKING', 'ignored_table_new')" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "0" ]] || false

    # Specific query for non-ignored table should work
    run dolt sql -q "SELECT COUNT(*) FROM dolt_diff_summary('HEAD', 'WORKING', 'not_ignored_table')" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1" ]] || false
}