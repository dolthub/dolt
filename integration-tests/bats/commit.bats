#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "commit: -ALL (-A) adds all tables including new ones to the staged set." {
    dolt sql -q "CREATE table t (pk int primary key);"
    dolt sql -q "INSERT INTO t VALUES (1);"
    dolt add t
    dolt commit -m "add table t"
    dolt reset --hard
    run dolt sql -q "SELECT * from t;"
    [ $status -eq 0 ]
    [[ "$output" =~ "| 1" ]] || false

    dolt sql -q "DELETE from t where pk = 1;"
    dolt commit -ALL -m "update table t"
    dolt reset --hard
    run dolt sql -q "SELECT * from t;"
    [ $status -eq 0 ]
    [[ ! "$output" =~ "| 1" ]] || false

    dolt sql -q "DROP TABLE t;"
    dolt commit -Am "drop table t;"
    dolt reset --hard
    run dolt ls
    [ $status -eq 0 ]
    [[ "$output" =~ "No tables in working set" ]] || false

    dolt sql -q "CREATE table t2 (pk int primary key);"
    dolt commit -Am "add table t2"
    dolt reset --hard
    run dolt ls
    [ $status -eq 0 ]
    [[ "$output" =~ "t2" ]] || false
}

@test "commit: --all (-a) adds existing, changed tables" {
    dolt sql -q "CREATE table t1 (pk int primary key);"
    dolt add t1
    run dolt commit -m "add table t1"
    [ $status -eq 0 ]
    [[ "$output" =~ "add table t1" ]] || false

    dolt sql -q "INSERT INTO t1 VALUES (1);"
    dolt sql -q "CREATE table t2 (pk int primary key);"
    run dolt commit -a -m "updating t1"
    [ $status -eq 0 ]
    [[ "$output" =~ "updating t1" ]] || false

    run dolt status
    [[ "$output" =~ "t2" ]] || false
    [[ ! "$output" =~ "t1" ]] || false

    run dolt show HEAD
    [[ "$output" =~ "| 1" ]] || false
}

@test "commit: -m sets commit message properly" {
    dolt sql -q "CREATE table t1 (pk int primary key);"
    dolt add t1
    run dolt commit -m "add table t1"
    [ $status -eq 0 ]
    [[ "$output" =~ "add table t1" ]] || false
}

@test "commit: all flag, message, author all work properly" {
    dolt sql -q "CREATE table t1 (pk int primary key);"
    dolt add t1
    run dolt commit -m "add table t1" --author "John Doe <john@doe.com>"
    [ $status -eq 0 ]
    echo "$output"
    [[ "$output" =~ "add table t1" ]] || false
    [[ "$output" =~ "John Doe" ]] || false
    [[ "$output" =~ "john@doe.com" ]] || false
}

@test "commit: failed to open commit editor." {
    export EDITOR="foo"
    export DOLT_TEST_FORCE_OPEN_EDITOR="1"
    dolt sql -q "CREATE table t (pk int primary key);"
    dolt sql -q "INSERT INTO t VALUES (1);"
    dolt add t
    run dolt commit
    [ $status -eq 1 ]
    [[ "$output" =~ "Failed to open commit editor" ]] || false
}

@test "commit: --skip-empty correctly skips committing when no changes are staged" {
  dolt sql -q "create table t(pk int primary key);"
  dolt add t
  original_head=$(get_head_commit)

  # When --allow-empty and --skip-empty are both specified, the user should get an error
  run dolt commit --allow-empty --skip-empty -m "commit message"
  [ $status -eq 1 ]
  [[ "$output" =~ 'error: cannot use both --allow-empty and --skip-empty' ]] || false
  [ $original_head = $(get_head_commit) ]

  # When changes are staged, --skip-empty has no effect
  dolt commit --skip-empty -m "commit message"
  new_head=$(get_head_commit)
  [ $original_head != $new_head ]

  # When no changes are staged, --skip-empty skips creating the commit
  run dolt commit --skip-empty -m "commit message"
  [ $new_head = $(get_head_commit) ]
  [[ "$output" =~ "Skipping empty commit" ]] || false
}

@test "commit: -f works correctly" {
    run dolt sql <<SQL
SET FOREIGN_KEY_CHECKS=0;
CREATE TABLE colors (
    id INT NOT NULL,
    color VARCHAR(32) NOT NULL,

    PRIMARY KEY (id),
    INDEX color_index(color)
);
CREATE TABLE objects (
    id INT NOT NULL,
    name VARCHAR(64) NOT NULL,
    color VARCHAR(32),

    PRIMARY KEY(id),
    FOREIGN KEY (color) REFERENCES colors(color)
);

INSERT INTO objects (id,name,color) VALUES (1,'truck','red'),(2,'ball','green'),(3,'shoe','blue');
SQL
    [ $status -eq 0 ]

    run dolt commit -A -f -m "Commit1"
    [ $status -eq 0 ]
    [[ "$output" =~ "Commit1" ]] || false
}

@test "commit: --amend works correctly" {
    dolt sql -q "CREATE table t (pk int primary key);"
    dolt add t

    run dolt commit -m "adding table t"
    [ $status -eq 0 ]
    [[ "$output" =~ "adding table t" ]] || false

    dolt sql -q "INSERT INTO t VALUES (1)"
    dolt add t

    run dolt commit --amend
    [ $status -eq 0 ]
    [[ "$output" =~ "adding table t" ]] || false

    run dolt log
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 8 ]
}

@test "commit: dolt commit with unstaged tables leaves them in the working set" {
    dolt sql -q "CREATE table t1 (pk int primary key);"
    dolt sql -q "CREATE table t2 (pk int primary key);"
    dolt add t1

    run dolt commit -m "adding table t1"
    [ $status -eq 0 ]
    [[ "$output" =~ "adding table t1" ]] || false

    run dolt status
    [ $status -eq 0 ]
    [[ "$output" =~ "new table:        t2" ]] || false
}

@test "commit: dolt commit works correctly with multiple branches" {
    dolt branch branch2
    dolt checkout -b branch1
    dolt sql -q "CREATE table t1 (pk int primary key);"
    dolt add t1

    run dolt commit -m "adding table t1 on branch1"
    [ $status -eq 0 ]
    [[ "$output" =~ "adding table t1 on branch1" ]] || false

    dolt checkout branch2
    dolt sql -q "CREATE table t2 (pk int primary key);"
    dolt add t2

    run dolt commit -m "adding table t2 on branch2"
    [ $status -eq 0 ]
    [[ "$output" =~ "adding table t2 on branch2" ]] || false

    run dolt log
    [ $status -eq 0 ]
    [[ "$output" =~ "adding table t2 on branch2" ]] || false
    [[ ! "$output" =~ "adding table t1 on branch1" ]] || false
}

@test "commit: no config set" {
    dolt config --global --unset user.email
    dolt config --global --unset user.name

    dolt sql -q "CREATE table t (pk int primary key);"
    dolt add t

    run dolt commit -m "adding table t"
    [ $status -eq 1 ]
    echo "$output"
    [[ "$output" =~ "Could not determine user.name" ]] || false
    [[ "$output" =~ "Log into DoltHub: dolt login" ]] || false
    [[ "$output" =~ "OR add name to config: dolt config [--global|--local] --add user.name \"FIRST LAST\"" ]] || false
}
