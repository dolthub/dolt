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

@test "DOLT_RESET --hard does not ignore staged docs" {
    # New docs gets referred as untracked file.
    echo ~license~ > LICENSE.md
    dolt add .

    run dolt sql -q "SELECT DOLT_RESET('--hard')"
    [ $status -eq 0 ]

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Untracked files:" ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false

    # Tracked file gets reset
    dolt commit -a -m "Add a the license file"
    echo ~edited-license~ > LICENSE.md

    dolt add .

    run dolt sql -q "SELECT DOLT_RESET('--hard')"
    [ $status -eq 0 ]

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes not staged for commit:" ]] || false
    [[ "$output" =~ ([[:space:]]*modified:[[:space:]]*LICENSE.md) ]] || false
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

@test "DOLT_RESET --soft ignores staged docs" {
    echo ~license~ > LICENSE.md
    dolt add .

    run dolt sql -q "SELECT DOLT_RESET('--soft')"
    [ $status -eq 0 ]

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false

    # Explicitly defining the file ignores it.
    run dolt sql -q "SELECT DOLT_RESET('LICENSE.md')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ ("error: the table(s) LICENSE.md do not exist") ]] || false
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

@test "DOLT_RESET --soft and --hard on the same table" {
     # Make a change to the table and do a soft reset
     dolt sql -q "INSERT INTO test VALUES (1)"

     run dolt sql -q "SELECT DOLT_RESET('test')"
     [ "$status" -eq 0 ]

     run dolt status
     [ "$status" -eq 0 ]
     [[ "$output" =~ "Changes not staged for commit:" ]] || false
     [[ "$output" =~ ([[:space:]]*modified:[[:space:]]*test) ]] || false

      # Add and unstage the table with a soft reset. Make sure the same data exists.
     dolt add .

     run dolt sql -q "SELECT DOLT_RESET('test')"
     [ "$status" -eq 0 ]

     run dolt status
     [ "$status" -eq 0 ]
     [[ "$output" =~ "Changes not staged for commit:" ]] || false
     [[ "$output" =~ ([[:space:]]*modified:[[:space:]]*test) ]] || false

     run dolt sql -r csv -q "select * from test"
     [[ "$output" =~ pk ]] || false
     [[ "$output" =~ 1  ]] || false

     # Do a hard reset and validate the insert was wiped properly
     run dolt sql -q "SELECT DOLT_RESET('--hard')"

     run dolt status
     [ "$status" -eq 0 ]
     [[ "$output" =~ "On branch master" ]] || false
     [[ "$output" =~ "nothing to commit, working tree clean" ]] || false

     run dolt sql -r csv -q "select * from test"
     [[ "$output" =~ pk ]] || false
     [[ "$output" != 1  ]] || false
}
