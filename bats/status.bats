#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}

@test "no changes" {
    dolt status
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch master" ]] || false
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false

    dolt sql <<SQL
CREATE TABLE test (pk int PRIMARY KEY);
INSERT INTO  test VALUES (0),(1),(2);
SQL
    dolt add -A && dolt commit -m "new table"
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch master" ]] || false
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false
}

@test "staged, unstaged, untracked tables" {
    dolt sql <<SQL
CREATE TABLE t (pk int PRIMARY KEY);
CREATE TABLE u (pk int PRIMARY KEY);
SQL
    dolt add -A && dolt commit -m "tables t, u"
    dolt sql <<SQL
INSERT INTO  t VALUES (1),(2),(3);
INSERT INTO  u VALUES (1),(2),(3);
CREATE TABLE v (pk int PRIMARY KEY);
SQL
    dolt add t
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch master" ]] || false
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ "  (use \"dolt reset <table>...\" to unstage)" ]] || false
    [[ "$output" =~ "	modified:       t" ]] || false
    [[ "$output" =~ "Changes not staged for commit:" ]] || false
    [[ "$output" =~ "  (use \"dolt add <table>\" to update what will be committed)" ]] || false
    [[ "$output" =~ "  (use \"dolt checkout <table>\" to discard changes in working directory)" ]] || false
    [[ "$output" =~ "	modified:       u" ]] || false
    [[ "$output" =~ "Untracked files:" ]] || false
    [[ "$output" =~ "  (use \"dolt add <table|doc>\" to include in what will be committed)" ]] || false
    [[ "$output" =~ "	new table:      v" ]] || false
}

@test "deleted table" {
    dolt sql <<SQL
CREATE TABLE t (pk int PRIMARY KEY);
CREATE TABLE u (pk int PRIMARY KEY);
SQL
    dolt add -A && dolt commit -m "tables t, u"
    dolt table rm t u
    dolt add t
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch master" ]] || false
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ "  (use \"dolt reset <table>...\" to unstage)" ]] || false
    [[ "$output" =~ "	deleted:        t" ]] || false
    [[ "$output" =~ "Changes not staged for commit:" ]] || false
    [[ "$output" =~ "  (use \"dolt add <table>\" to update what will be committed)" ]] || false
    [[ "$output" =~ "  (use \"dolt checkout <table>\" to discard changes in working directory)" ]] || false
    [[ "$output" =~ "	deleted:        u" ]] || false
}

@test "tables in conflict" {
    dolt sql <<SQL
CREATE TABLE t (pk int PRIMARY KEY, c0 int);
INSERT INTO t VALUES (1,1);
SQL
    dolt add -A && dolt commit -m "created table t"
    dolt checkout -b other
    dolt sql -q "INSERT INTO t VALUES (2,12);"
    dolt add -A && dolt commit -m "added values on branch other"
    dolt checkout master
    dolt sql -q "INSERT INTO t VALUES (2,2);"
    dolt add -A && dolt commit -m "added values on branch master"
    run dolt merge other
    [ "$status" -eq 0 ]
    [[ "$output" =~ "CONFLICT (content): Merge conflict in t" ]] || false
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch master" ]] || false
    [[ "$output" =~ "You have unmerged tables." ]] || false
    [[ "$output" =~ "  (fix conflicts and run \"dolt commit\")" ]] || false
    [[ "$output" =~ "  (use \"dolt merge --abort\" to abort the merge)" ]] || false
    [[ "$output" =~ "Unmerged paths:" ]] || false
    [[ "$output" =~ "  (use \"dolt add <file>...\" to mark resolution)" ]] || false
    [[ "$output" =~ "	both modified:  t" ]] || false
}

@test "renamed table" {
    dolt sql <<SQL
CREATE TABLE test (pk int PRIMARY KEY);
SQL
    dolt add test
    dolt commit -m 'added table test'
    run dolt sql -q 'alter table test rename to quiz'
    [ "$status" -eq 0 ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "deleted:        test" ]] || false
    [[ "$output" =~ "new table:      quiz" ]] || false
    dolt add .
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "renamed:        test -> quiz" ]] || false
}