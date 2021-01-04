#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common

    dolt sql <<SQL
CREATE TABLE test (
    pk int primary key
);
SQL

    dolt commit -a -m "Add a table"
}

teardown() {
    teardown_common
}

@test "DOLT_RESET --hard works on unstaged and staged table changes" {
    dolt sql -q "INSERT INTO test VALUES (1)"

    run dolt sql -q "SELECT DOLT_RESET('--hard')"
    [ $status -eq 0 ]

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch master" ]] || false
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false

    dolt sql -q "INSERT INTO test VALUES (1)"

    dolt add .

    run dolt sql -q "SELECT DOLT_RESET('--hard')"
    [ $status -eq 0 ]

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch master" ]] || false
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false
}

@test "DOLT_RESET --hard works on staged docs" {
    skip "Skipping until functionality to delete local document works."
    echo ~license~ > LICENSE.md
    echo ~readme~ > README.md
    dolt add .

    run dolt sql -q "SELECT DOLT_RESET('--hard')"
    [ $status -eq 0 ]

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch master" ]] || false
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false
}

@test "DOLT_RESET --soft works on unstaged and staged table changes" {
    dolt sql -q "INSERT INTO test VALUES (1)"

    # Table should still be unstaged
    run dolt sql -q "SELECT DOLT_RESET('--soft')"
    [ $status -eq 0 ]

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes not staged for commit:" ]] || false
    [[ "$output" =~ ([[:space:]]*modified:[[:space:]]*test) ]] || false

    dolt add .

    run dolt sql -q "SELECT DOLT_RESET('--soft')"
    [ $status -eq 0 ]

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes not staged for commit:" ]] || false
    [[ "$output" =~ ([[:space:]]*modified:[[:space:]]*test) ]] || false
}

@test "DOLT_RESET --soft works on staged docs" {
    echo ~license~ > LICENSE.md
    dolt add .

    run dolt sql -q "SELECT DOLT_RESET('--soft')"
    [ $status -eq 0 ]

    run dolt status
    [ "$status" -eq 0 ]
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ "Untracked files:" ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
}

@test "DOLT_RESET works on specific tables" {
    dolt sql -q "INSERT INTO test VALUES (1)"

    # Table should still be unstaged
    run dolt sql -q "SELECT DOLT_RESET('test')"

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes not staged for commit:" ]] || false
    [[ "$output" =~ ([[:space:]]*modified:[[:space:]]*test) ]] || false

    dolt sql -q "CREATE TABLE test2 (pk int primary key);"

    dolt add .
    run dolt sql -q "SELECT DOLT_RESET('test', 'test2')"

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes not staged for commit:" ]] || false
    [[ "$output" =~ ([[:space:]]*modified:[[:space:]]*test) ]] || false
    [[ "$output" =~ ([[:space:]]*new table:[[:space:]]*test2) ]] || false
}
