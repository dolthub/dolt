#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

setup_ancestor() {
    dolt sql <<SQL
CREATE TABLE test1 (
  pk int NOT NULL,
  c1 int,
  c2 int,
  PRIMARY KEY (pk)
);
INSERT INTO test1 values (0,1,1);
SQL

    dolt add .
    dolt commit -m "added tables"
}

merge_without_conflicts() {
    setup_ancestor

    dolt checkout -b merge_branch
    dolt SQL -q "UPDATE test1 set c1 = 2;"
    dolt add test1
    dolt commit -m "update pk 0 = 2,1 to test1"

    dolt checkout main
    dolt SQL -q "UPDATE test1 set c2 = 2;"
    dolt add test1
    dolt commit -m "update pk 0 = 1,2 to test1"

    run dolt merge merge_branch -m "merge merge_branch"
}

merge_with_conflicts() {
    setup_ancestor

    dolt checkout -b merge_branch
    dolt SQL -q "UPDATE test1 set c1 = 2, c2 = 2;"
    dolt add test1
    dolt commit -m "update pk 0 = 2,2 to test1"

    dolt checkout main
    dolt SQL -q "UPDATE test1 set c2 = 3, c2 = 3;"
    dolt add test1
    dolt commit -m "update pk 0 = 3,3 to test1"

    run dolt merge merge_branch -m "merge merge_branch"
}

@test "reset: dolt reset --hard should clear an uncommitted merge state" {
    merge_without_conflicts

    run dolt sql -q "SELECT * from dolt_merge_status;"
    [[ "$output" =~ "false" ]] || false

    run dolt reset --hard
    [ $status -eq 0 ]

    run dolt status
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false

    run dolt merge --abort
    [[ "$output" =~ "fatal: There is no merge to abort" ]] || false

    run dolt sql -q "SELECT * from dolt_merge_status;"
    [[ "$output" =~ "false" ]] || false
}

@test "reset: dolt reset --hard should clear a conflicted merge state" {
    merge_with_conflicts

    run dolt sql -q "SELECT * from dolt_merge_status;"
    [[ "$output" =~ "true" ]] || false
    [[ "$output" =~ "merge_branch" ]] || false
    [[ "$output" =~ "refs/heads/main" ]] || false
    [[ "$output" =~ "test1" ]] || false

    run dolt reset --hard
    [ $status -eq 0 ]

    run dolt status
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false

    run dolt merge --abort
    [[ "$output" =~ "fatal: There is no merge to abort" ]] || false

    run dolt sql -q "SELECT * from dolt_merge_status;"
    [[ "$output" =~ "false" ]] || false
}

@test "reset: dolt reset head works" {
    setup_ancestor

    dolt sql -q "insert into test1 values (1, 1, 1)"
    dolt add test1

    run dolt status
    [ $status -eq 0 ]
    [[ "$output" =~ "Changes to be committed:" ]] || false

    run dolt reset head
    [ $status -eq 0 ]

    run dolt status
    [ $status -eq 0 ]
    [[ "$output" =~ "Changes not staged for commit:" ]] || false
}

@test "reset: --hard works on unstaged and staged table changes" {
    setup_ancestor

    dolt sql -q "insert into test1 values (1, 1, 1)"

    run dolt reset --hard
    [ $status -eq 0 ]

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false

    dolt sql -q "insert into test1 values (1, 1, 1)"
    dolt add .

    run dolt reset --hard
    [ $status -eq 0 ]

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false

    dolt sql -q "insert into test1 values (1, 1, 1)"

    # Reset to head results in clean main.
    run dolt reset --hard head
    [ "$status" -eq 0 ]

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false
}

@test "reset: --soft works on unstaged and staged table changes" {
    setup_ancestor

    dolt sql -q "INSERT INTO test1 VALUES (1, 1, 1)"

    # Table should still be unstaged
    run dolt reset --soft
    [ $status -eq 0 ]

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes not staged for commit:" ]] || false
    [[ "$output" =~ ([[:space:]]*modified:[[:space:]]*test) ]] || false

    dolt add .

    run dolt reset --soft
    [ $status -eq 0 ]

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ ([[:space:]]*modified:[[:space:]]*test) ]] || false
}

@test "reset: reset works on specific tables" {
    setup_ancestor

    dolt sql -q "INSERT INTO test1 VALUES (1,1,1)"

    # Table should still be unstaged
    run dolt reset test1

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes not staged for commit:" ]] || false
    [[ "$output" =~ ([[:space:]]*modified:[[:space:]]*test) ]] || false

    dolt sql -q "CREATE TABLE test2 (pk int primary key);"

    dolt add .
    run dolt reset test1 test2

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes not staged for commit:" ]] || false
    [[ "$output" =~ ([[:space:]]*modified:[[:space:]]*test) ]] || false
    [[ "$output" =~ ([[:space:]]*new table:[[:space:]]*test2) ]] || false
}

@test "reset: --soft and --hard on the same table" {
    setup_ancestor

    # Make a change to the table and do a soft reset
    dolt sql -q "INSERT INTO test1 VALUES (1, 1, 1)"
    run dolt reset test1
    [ "$status" -eq 0 ]

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes not staged for commit:" ]] || false
    [[ "$output" =~ ([[:space:]]*modified:[[:space:]]*test) ]] || false

    # Add and unstage the table with a soft reset. Make sure the same data exists.
    dolt add .

    run dolt reset test1
    [ "$status" -eq 0 ]

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes not staged for commit:" ]] || false
    [[ "$output" =~ ([[:space:]]*modified:[[:space:]]*test) ]] || false

    run dolt sql -r csv -q "select * from test1"
    [[ "$output" =~ pk ]] || false
    [[ "$output" =~ 1  ]] || false

    # Do a hard reset and validate the insert was wiped properly
    run dolt reset --hard

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false

    run dolt sql -r csv -q "select * from test1"
    [[ "$output" =~ pk ]] || false
    [[ "$output" != 1  ]] || false
}

@test "reset: --hard doesn't remove newly created table." {
    setup_ancestor

    dolt sql << SQL
CREATE TABLE test2 (
    pk int primary key
);
SQL
    run dolt reset --hard
    [ "$status" -eq 0 ]

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Untracked tables:" ]] || false
    [[ "$output" =~ ([[:space:]]*new table:[[:space:]]*test2) ]] || false

    dolt add .
    dolt reset --hard
    run dolt status

    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false
}

@test "reset: dolt reset soft with ~ works" {
    dolt sql -q "CREATE TABLE test (pk int PRIMARY KEY);"
    dolt add .
    dolt commit -am "cm1"

    dolt sql -q "INSERT INTO test values (1);"
    dolt commit -am "cm2"

    # Make a dirty change
    dolt sql -q "INSERT INTO test values (2)"
    run dolt reset HEAD~
    [ "$status" -eq 0 ]

    # Verify that the changes are still there
    run dolt sql -q "SELECT sum(pk) FROM test;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "3" ]] || false

    # Now verify that commit log has changes
    run dolt sql -q "SELECT count(*) from dolt_log"
    [[ "$output" =~ "2" ]] || false

    run dolt reset HEAD~1
    [ "$status" -eq 0 ]

    # Verify that the changes are still there
    run dolt sql -q "SELECT sum(pk) FROM test;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "3" ]] || false

    dolt status
    run dolt status
    [[ "$output" =~ "Untracked tables:" ]] || false
    [[ "$output" =~ "  (use \"dolt add <table>\" to include in what will be committed)" ]] || false
    [[ "$output" =~ "	new table:        test" ]] || false

    # Now verify that commit log has changes
    run dolt sql -q "SELECT count(*) from dolt_log"
    [[ "$output" =~ "1" ]] || false
}

@test "reset: reset handles ignored tables" {
    setup_ancestor

    dolt sql << SQL
CREATE TABLE test2 (
    pk int primary key
);
INSERT INTO test2 VALUES (9);
INSERT INTO test1 VALUES (1, 2, 3);
SQL
    dolt sql -q "insert into dolt_ignore values ('test2', true)"

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" != "test2" ]] || false

    run dolt reset --hard
    [ "$status" -eq 0 ]

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Untracked tables:" ]] || false
    [[ "$output" =~ "  (use \"dolt add <table>\" to include in what will be committed)" ]] || false
    [[ "$output" =~ "	new table:        dolt_ignore" ]] || false

    run dolt sql -q "select * from test2"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "9" ]] || false
}
