#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    skip_nbf_dolt_1

    dolt sql <<SQL
CREATE TABLE test (
    pk int primary key
);
SQL

    dolt commit -a -m "Add a table"
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "sql-reset: DOLT_RESET --hard works on unstaged and staged table changes" {
    dolt sql -q "INSERT INTO test VALUES (1)"

    run dolt sql -q "SELECT DOLT_RESET('--hard')"
    [ $status -eq 0 ]

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false

    dolt sql -q "INSERT INTO test VALUES (1)"

    dolt add .

    run dolt sql -q "SELECT DOLT_RESET('--hard')"
    [ $status -eq 0 ]

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false

    dolt sql -q "INSERT INTO test VALUES (1)"

    # Reset to head results in clean main.
    run dolt sql -q "SELECT DOLT_RESET('--hard', 'head');"
    [ "$status" -eq 0 ]

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false
}

@test "sql-reset: CALL DOLT_RESET --hard works on unstaged and staged table changes" {
    dolt sql -q "INSERT INTO test VALUES (1)"

    run dolt sql -q "CALL DOLT_RESET('--hard')"
    [ $status -eq 0 ]

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false

    dolt sql -q "INSERT INTO test VALUES (1)"

    dolt add .

    run dolt sql -q "CALL DOLT_RESET('--hard')"
    [ $status -eq 0 ]

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false

    dolt sql -q "INSERT INTO test VALUES (1)"

    # Reset to head results in clean main.
    run dolt sql -q "CALL DOLT_RESET('--hard', 'head');"
    [ "$status" -eq 0 ]

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false
}

@test "sql-reset: CALL DRESET --hard works on unstaged and staged table changes" {
    dolt sql -q "INSERT INTO test VALUES (1)"

    run dolt sql -q "CALL DRESET('--hard')"
    [ $status -eq 0 ]

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false

    dolt sql -q "INSERT INTO test VALUES (1)"

    dolt add .

    run dolt sql -q "CALL DRESET('--hard')"
    [ $status -eq 0 ]

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false

    dolt sql -q "INSERT INTO test VALUES (1)"

    # Reset to head results in clean main.
    run dolt sql -q "CALL DRESET('--hard', 'head');"
    [ "$status" -eq 0 ]

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false
}

@test "sql-reset: DOLT_RESET --hard does not ignore staged docs" {
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

@test "sql-reset: CALL DOLT_RESET --hard does not ignore staged docs" {
    # New docs gets referred as untracked file.
    echo ~license~ > LICENSE.md
    dolt add .

    run dolt sql -q "CALL DOLT_RESET('--hard')"
    [ $status -eq 0 ]

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Untracked files:" ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false

    # Tracked file gets reset
    dolt commit -a -m "Add a the license file"
    echo ~edited-license~ > LICENSE.md

    dolt add .

    run dolt sql -q "CALL DOLT_RESET('--hard')"
    [ $status -eq 0 ]

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes not staged for commit:" ]] || false
    [[ "$output" =~ ([[:space:]]*modified:[[:space:]]*LICENSE.md) ]] || false
}

@test "sql-reset: DOLT_RESET --soft works on unstaged and staged table changes" {
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

@test "sql-reset: CALL DOLT_RESET --soft works on unstaged and staged table changes" {
    dolt sql -q "INSERT INTO test VALUES (1)"

    # Table should still be unstaged
    run dolt sql -q "CALL DOLT_RESET('--soft')"
    [ $status -eq 0 ]

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes not staged for commit:" ]] || false
    [[ "$output" =~ ([[:space:]]*modified:[[:space:]]*test) ]] || false

    dolt add .

    run dolt sql -q "CALL DOLT_RESET('--soft')"
    [ $status -eq 0 ]

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes not staged for commit:" ]] || false
    [[ "$output" =~ ([[:space:]]*modified:[[:space:]]*test) ]] || false
}

@test "sql-reset: DOLT_RESET --soft ignores staged docs" {
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

@test "sql-reset: CALL DOLT_RESET --soft ignores staged docs" {
    echo ~license~ > LICENSE.md
    dolt add .

    run dolt sql -q "CALL DOLT_RESET('--soft')"
    [ $status -eq 0 ]

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false

    # Explicitly defining the file ignores it.
    run dolt sql -q "CALL DOLT_RESET('LICENSE.md')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ ("error: the table(s) LICENSE.md do not exist") ]] || false
}

@test "sql-reset: DOLT_RESET works on specific tables" {
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

@test "sql-reset: CALL DOLT_RESET works on specific tables" {
    dolt sql -q "INSERT INTO test VALUES (1)"

    # Table should still be unstaged
    run dolt sql -q "CALL DOLT_RESET('test')"

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes not staged for commit:" ]] || false
    [[ "$output" =~ ([[:space:]]*modified:[[:space:]]*test) ]] || false

    dolt sql -q "CREATE TABLE test2 (pk int primary key);"

    dolt add .
    run dolt sql -q "CALL DOLT_RESET('test', 'test2')"

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes not staged for commit:" ]] || false
    [[ "$output" =~ ([[:space:]]*modified:[[:space:]]*test) ]] || false
    [[ "$output" =~ ([[:space:]]*new table:[[:space:]]*test2) ]] || false
}

@test "sql-reset: DOLT_RESET --soft and --hard on the same table" {
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
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false

    run dolt sql -r csv -q "select * from test"
    [[ "$output" =~ pk ]] || false
    [[ "$output" != 1  ]] || false
}

@test "sql-reset: CALL DOLT_RESET --soft and --hard on the same table" {
    # Make a change to the table and do a soft reset
    dolt sql -q "INSERT INTO test VALUES (1)"
    run dolt sql -q "CALL DOLT_RESET('test')"
    [ "$status" -eq 0 ]

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes not staged for commit:" ]] || false
    [[ "$output" =~ ([[:space:]]*modified:[[:space:]]*test) ]] || false

    # Add and unstage the table with a soft reset. Make sure the same data exists.
    dolt add .

    run dolt sql -q "CALL DOLT_RESET('test')"
    [ "$status" -eq 0 ]

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes not staged for commit:" ]] || false
    [[ "$output" =~ ([[:space:]]*modified:[[:space:]]*test) ]] || false

    run dolt sql -r csv -q "select * from test"
    [[ "$output" =~ pk ]] || false
    [[ "$output" =~ 1  ]] || false

    # Do a hard reset and validate the insert was wiped properly
    run dolt sql -q "CALL DOLT_RESET('--hard')"

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false

    run dolt sql -r csv -q "select * from test"
    [[ "$output" =~ pk ]] || false
    [[ "$output" != 1  ]] || false
}

@test "sql-reset: DOLT_RESET('--hard') doesn't remove newly created table." {
    dolt sql << SQL
CREATE TABLE test2 (
    pk int primary key
);
SQL
    dolt sql -q "SELECT DOLT_RESET('--hard');"

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Untracked files:" ]] || false
    [[ "$output" =~ ([[:space:]]*new table:[[:space:]]*test2) ]] || false

    dolt add .
    dolt sql -q "SELECT DOLT_RESET('--hard');"
    run dolt status

    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false
}

@test "sql-reset: CALL DOLT_RESET('--hard') doesn't remove newly created table." {
    dolt sql << SQL
CREATE TABLE test2 (
    pk int primary key
);
SQL
    dolt sql -q "CALL DOLT_RESET('--hard');"

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Untracked files:" ]] || false
    [[ "$output" =~ ([[:space:]]*new table:[[:space:]]*test2) ]] || false

    dolt add .
    dolt sql -q "CALL DOLT_RESET('--hard');"
    run dolt status

    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false
}

@test "sql-reset: No rows in dolt_diff table after DOLT_RESET('--hard') on committed table." {
    run dolt sql << SQL
INSERT INTO test VALUES (1);
SELECT DOLT_RESET('--hard');
SELECT count(*)=0 FROM dolt_diff_test;
SQL
    [ $status -eq 0 ]
    # Represents that the diff table marks a change from the recent commit.
    [[ "$output" =~ "true" ]] || false
}

@test "sql-reset: No rows in dolt_diff table after CALL DOLT_RESET('--hard') on committed table." {
    run dolt sql << SQL
INSERT INTO test VALUES (1);
CALL DOLT_RESET('--hard');
SELECT count(*)=0 FROM dolt_diff_test;
SQL
    [ $status -eq 0 ]
    # Represents that the diff table marks a change from the recent commit.
    [[ "$output" =~ "true" ]] || false
}

@test "sql-reset: No rows in dolt_status table after DOLT_RESET('--hard') on committed table." {
      run dolt sql << SQL
INSERT INTO test VALUES (1);
SELECT DOLT_RESET('--hard');
SELECT count(*)=0 FROM dolt_status;
SQL
    [ $status -eq 0 ]
    [[ "$output" =~ "true" ]] || false
}

@test "sql-reset: No rows in dolt_status table after CALL DOLT_RESET('--hard') on committed table." {
      run dolt sql << SQL
INSERT INTO test VALUES (1);
CALL DOLT_RESET('--hard');
SELECT count(*)=0 FROM dolt_status;
SQL
    [ $status -eq 0 ]
    [[ "$output" =~ "true" ]] || false
}

@test "sql-reset: DOLT_RESET --hard properly maintains session variables." {
    head_variable=@@dolt_repo_$$_head
    head_hash=$(get_head_commit)
    run dolt sql << SQL
INSERT INTO test VALUES (1);
SELECT DOLT_RESET('--hard');
SELECT $head_variable;
SQL

    [ $status -eq 0 ]
    [[ "$output" =~ $head_hash ]] || false
}

@test "sql-reset: CALL DOLT_RESET --hard properly maintains session variables." {
    head_variable=@@dolt_repo_$$_head
    head_hash=$(get_head_commit)
    run dolt sql << SQL
INSERT INTO test VALUES (1);
CALL DOLT_RESET('--hard');
SELECT $head_variable;
SQL

    [ $status -eq 0 ]
    [[ "$output" =~ $head_hash ]] || false
}

@test "sql-reset: dolt_status still has the same information in the face of a DOLT_RESET" {
    run dolt sql << SQL
INSERT INTO test VALUES (1);
SQL

    dolt sql -q "SELECT DOLT_RESET('test');"
    run dolt sql -q "SELECT * FROM dolt_status;"

    [ $status -eq 0 ]
    [[ "$output" =~ "false" ]] || false
}

@test "sql-reset: dolt_status still has the same information in the face of a CALL DOLT_RESET" {
    run dolt sql << SQL
INSERT INTO test VALUES (1);
SQL

    dolt sql -q "CALL DOLT_RESET('test');"
    run dolt sql -q "SELECT * FROM dolt_status;"

    [ $status -eq 0 ]
    [[ "$output" =~ "false" ]] || false
}

@test "sql-reset: DOLT_RESET soft maintains staged session variable" {
    working_hash_var=@@dolt_repo_$$_working
    run dolt sql -q "SELECT $working_hash_var"
    working_hash=$output

    run dolt sql << SQL
INSERT INTO test VALUES (1);
SELECT DOLT_ADD('.');
SELECT DOLT_RESET('test');
SELECT $working_hash_var
SQL

    [ $status -eq 0 ]

    # These should not match as @@_working should become a new staged hash different from the original working.
    [[ ! "$output" =~ $working_hash ]] || false

    run dolt sql -q "SELECT DOLT_RESET('--hard');"
    [ $status -eq 0 ]

    run dolt sql -q "SELECT $working_hash_var"
    [ $status -eq 0 ]

    # Matches exactly.
    [[ "$output" = "$working_hash" ]] || false
}

@test "sql-reset: CALL DOLT_RESET soft maintains staged session variable" {
    working_hash_var=@@dolt_repo_$$_working
    run dolt sql -q "SELECT $working_hash_var"
    working_hash=$output

    run dolt sql << SQL
INSERT INTO test VALUES (1);
SELECT DOLT_ADD('.');
SELECT DOLT_RESET('test');
SELECT $working_hash_var
SQL

    [ $status -eq 0 ]

    # These should not match as @@_working should become a new staged hash different from the original working.
    [[ ! "$output" =~ $working_hash ]] || false

    run dolt sql -q "CALL DOLT_RESET('--hard');"
    [ $status -eq 0 ]

    run dolt sql -q "SELECT $working_hash_var"
    [ $status -eq 0 ]

    # Matches exactly.
    [[ "$output" = "$working_hash" ]] || false
}

get_head_commit() {
    dolt log -n 1 | grep -m 1 commit | cut -c 13-44
}

