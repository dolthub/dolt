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

@test "ignore: simple matches" {

    dolt sql <<SQL
CREATE TABLE ignoreme (pk int);
CREATE TABLE dontignore (pk int);
CREATE TABLE nomatch (pk int);
SQL

    dolt add -A

    staged=$(get_staged_tables)
    ignored=$(get_ignored_tables)

    [[ ! -z $(echo "$ignored" | grep "ignoreme") ]] || false
    [[ ! -z $(echo "$staged" | grep "dontignore") ]] || false
    [[ ! -z $(echo "$staged" | grep "nomatch") ]] || false
}

@test "ignore: specific overrides" {

    dolt sql <<SQL
CREATE TABLE please_ignore (pk int);
CREATE TABLE please_ignore_too (pk int);
CREATE TABLE do_not_ignore (pk int);
CREATE TABLE commit_me (pk int);
CREATE TABLE commit_me_not(pk int);
SQL

    dolt add -A

    ignored=$(get_ignored_tables)
    staged=$(get_staged_tables)

    [[ ! -z $(echo "$ignored" | grep "please_ignore") ]] || false
    [[ ! -z $(echo "$ignored" | grep "please_ignore_too") ]] || false
    [[ ! -z $(echo "$staged" | grep "do_not_ignore") ]] || false
    [[ ! -z $(echo "$staged" | grep "commit_me") ]] || false
    [[ ! -z $(echo "$ignored" | grep "commit_me_not") ]] || false
}

@test "ignore: conflict" {

    dolt sql <<SQL
CREATE TABLE commit_ignore (pk int);
SQL

    run dolt add -A

    [ "$status" -eq 1 ]
    [[ "$output" =~ "the table commit_ignore matches conflicting patterns in dolt_ignore" ]] || false
    [[ "$output" =~ "ignored:     *_ignore" ]] || false
    [[ "$output" =~ "not ignored: commit_*" ]] || false

}

@test "ignore: question mark" {

    dolt sql <<SQL
CREATE TABLE test (pk int);
CREATE TABLE test1 (pk int);
CREATE TABLE test11 (pk int);
SQL

    dolt add -A

    ignored=$(get_ignored_tables)
    staged=$(get_staged_tables)

    [[ ! -z $(echo "$ignored" | grep "test$") ]] || false
    [[ ! -z $(echo "$ignored" | grep "test1$") ]] || false
    [[ ! -z $(echo "$staged" | grep "test11$") ]] || false
}

@test "ignore: don't stash ignored tables" {

    dolt sql <<SQL
CREATE TABLE ignoreme (pk int);
SQL

    run dolt stash -u

    [ "$status" -eq 0 ]
    [[ "$output" =~ "No local changes to save" ]] || false
}

@test "ignore: error when trying to stash table with dolt_ignore conflict" {

    dolt sql <<SQL
CREATE TABLE commit_ignore (pk int);
SQL

    run dolt stash -u

    [ "$status" -eq 1 ]
    [[ "$output" =~ "the table commit_ignore matches conflicting patterns in dolt_ignore" ]] || false
    [[ "$output" =~ "ignored:     *_ignore" ]] || false
    [[ "$output" =~ "not ignored: commit_*" ]] || false
}

@test "ignore: stash ignored and untracked tables when --all is passed" {

    dolt sql <<SQL
CREATE TABLE ignoreme (pk int);
CREATE TABLE dontignore (pk int);
SQL

    dolt stash -a

    working=$(get_working_tables)
    ignored=$(get_ignored_tables)

    [[ -z $(echo "$ignored" | grep "ignoreme") ]] || false
    [[ -z $(echo "$working" | grep "dontignore") ]] || false

    dolt stash pop

    working=$(get_working_tables)
    ignored=$(get_ignored_tables)

    [[ ! -z $(echo "$ignored" | grep "ignoreme") ]] || false
    [[ ! -z $(echo "$working" | grep "dontignore") ]] || false
}

@test "ignore: stash table with dolt_ignore conflict when --all is passed" {

    dolt sql <<SQL
CREATE TABLE commit_ignore (pk int);
SQL

    dolt stash -a

    conflicts=$(get_conflict_tables)

    [[ -z $(echo "$conflicts" | grep "commit_ignore") ]] || false

    dolt stash pop

    conflicts=$(get_conflict_tables)

    [[ ! -z $(echo "$conflicts" | grep "commit_ignore") ]] || false

}

@test "ignore: allow staging ignored tables if 'add --force' is supplied" {

    dolt sql <<SQL
CREATE TABLE ignoreme (pk int);
SQL

    dolt add -A --force

    staged=$(get_staged_tables)

    [[ ! -z $(echo "$staged" | grep "ignoreme") ]] || false
}

@test "ignore: don't auto-stage ignored tables" {

    dolt sql <<SQL
CREATE TABLE ignoreme (pk int);
CREATE TABLE nomatch (pk int);
SQL

    dolt commit -m "commit1" -A

    run dolt show

    [ "$status" -eq 0 ]

    ! [["$output" =~ "diff --dolt a/ignoreme b/ignoreme"]] || false

}

@test "ignore: dolt status doesn't show ignored tables when --ignored is not supplied" {

    dolt sql <<SQL
CREATE TABLE ignoreme (pk int);
CREATE TABLE nomatch (pk int);
SQL

    run dolt status

    [ "$status" -eq 0 ]

    [[ "$output" =~ "nomatch" ]] || false
    ! [["$output" =~ "Ignored tables"]] || false
    ! [["$output" =~ "ignoreme"]] || false

}

@test "ignore: dolt status shows ignored tables when --ignored is not supplied" {

    dolt sql <<SQL
CREATE TABLE ignoreme (pk int);
CREATE TABLE nomatch (pk int);
SQL

    run dolt status --ignored

    [ "$status" -eq 0 ]

    [[ "$output" =~ "nomatch" ]] || false
    [[ "$output" =~ "Ignored tables" ]] || false
    [[ "$output" =~ "ignoreme" ]] || false

}

@test "ignore: don't display new but ignored tables in dolt diff" {

    dolt sql <<SQL
CREATE TABLE ignoreme (pk int);
CREATE TABLE nomatch (pk int);
SQL

    run dolt diff

    [ "$status" -eq 0 ]

    [[ "$output" =~ "nomatch" ]] || false
    ! [["$output" =~ "ignoreme"]] || false
}

@test "ignore: don't display new but ignored tables in reverse diff" {

    dolt sql <<SQL
CREATE TABLE ignoreme (pk int);
CREATE TABLE nomatch (pk int);
SQL

    run dolt diff -R

    [ "$status" -eq 0 ]

    [[ "$output" =~ "nomatch" ]] || false
    ! [["$output" =~ "ignoreme"]] || false
}

@test "ignore: DO display modified ignored tables in dolt diff after staging" {

    dolt sql <<SQL
CREATE TABLE ignoreme (pk int);
SQL

    dolt add --force ignoreme

    dolt sql <<SQL
INSERT INTO ignoreme VALUES (1);
SQL

    run dolt diff

    [ "$status" -eq 0 ]

    echo "$output"

    [[ "$output" =~ "ignoreme" ]] || false
}

@test "ignore: DO display modified ignored tables in reverse diff after staging" {

    dolt sql <<SQL
CREATE TABLE ignoreme (pk int);
SQL

    dolt add --force ignoreme

    dolt sql <<SQL
INSERT INTO ignoreme VALUES (1);
SQL

    run dolt diff -R

    [ "$status" -eq 0 ]

    echo "$output"

    [[ "$output" =~ "ignoreme" ]] || false
}

@test "ignore: DO display modified ignored tables in dolt diff after committing" {

    dolt sql <<SQL
CREATE TABLE ignoreme (pk int);
SQL

    dolt add --force ignoreme
    dolt commit -m "commit1"

    dolt sql <<SQL
INSERT INTO ignoreme VALUES (1);
SQL

    run dolt diff

    [ "$status" -eq 0 ]

    echo "$output"

    [[ "$output" =~ "ignoreme" ]] || false
}

@test "ignore: detect when equivalent patterns have different values" {

    dolt sql <<SQL
INSERT INTO dolt_ignore VALUES
  ("**_test", true),
  ("*_test", false),

  ("*_foo", true),
  ("%_foo", false);
CREATE TABLE a_test (pk int);
CREATE TABLE a_foo (pk int);
SQL

    conflict=$(get_conflict_tables)

    echo "$conflict"

    [[ ! -z $(echo "$conflict" | grep "a_test") ]] || false
    [[ ! -z $(echo "$conflict" | grep "a_foo") ]] || false

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