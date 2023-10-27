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