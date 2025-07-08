#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

get_head_commit() {
    dolt log -n 1 | grep -m 1 commit | cut -c 13-44
}

@test "status: db collation" {
    dolt sql -q "create database colldb"
    cd colldb

    dolt sql -q "alter database colldb collate utf8mb4_spanish_ci"
    run dolt status
    [ $status -eq 0 ]
    [[ $output =~ "Changes not staged for commit:" ]] || false
    [[ ! $output =~ "Changes to be committed:" ]] || false
    [[ $output =~ "modified:         __DATABASE__colldb" ]] || false

    dolt add .
    run dolt status
    [ $status -eq 0 ]
    [[ ! $output =~ "Changes not staged for commit:" ]] || false
    [[ $output =~ "Changes to be committed:" ]] || false
    [[ $output =~ "modified:         __DATABASE__colldb" ]] || false

    dolt commit -m "collation"
    run dolt status
    [ $status -eq 0 ]
    [[ $output =~ "On branch main" ]] || false
    [[ $output =~ "nothing to commit, working tree clean" ]] || false
}

@test "status: no changes" {
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false

    dolt sql <<SQL
CREATE TABLE test (pk int PRIMARY KEY);
INSERT INTO  test VALUES (0),(1),(2);
SQL
    dolt add -A && dolt commit -m "new table"
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false
}

@test "status: staged, unstaged, untracked, ignored tables" {
    dolt sql <<SQL
CREATE TABLE t (pk int PRIMARY KEY);
CREATE TABLE u (pk int PRIMARY KEY);
INSERT INTO dolt_ignore VALUES ('generated_*', 1);
SQL
    dolt add -A && dolt commit -m "tables t, u"
    dolt sql <<SQL
INSERT INTO  t VALUES (1),(2),(3);
INSERT INTO  u VALUES (1),(2),(3);
CREATE TABLE v (pk int PRIMARY KEY);
CREATE TABLE generated_foo (pk int PRIMARY KEY);
SQL
    dolt add t
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ "  (use \"dolt reset <table>...\" to unstage)" ]] || false
    [[ "$output" =~ "	modified:         t" ]] || false
    [[ "$output" =~ "Changes not staged for commit:" ]] || false
    [[ "$output" =~ "  (use \"dolt add <table>\" to update what will be committed)" ]] || false
    [[ "$output" =~ "  (use \"dolt checkout <table>\" to discard changes in working directory)" ]] || false
    [[ "$output" =~ "	modified:         u" ]] || false
    [[ "$output" =~ "Untracked tables:" ]] || false
    [[ "$output" =~ "  (use \"dolt add <table>\" to include in what will be committed)" ]] || false
    [[ "$output" =~ "	new table:        v" ]] || false
    ! [[ "$output" =~ "   new table:        generated_foo" ]] || false
}

@test "status: deleted table" {
    dolt sql <<SQL
CREATE TABLE t (pk int PRIMARY KEY);
CREATE TABLE u (pk int PRIMARY KEY);
SQL
    dolt add -A && dolt commit -m "tables t, u"
    dolt table rm t u
    dolt add t
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ "  (use \"dolt reset <table>...\" to unstage)" ]] || false
    [[ "$output" =~ "	deleted:          t" ]] || false
    [[ "$output" =~ "Changes not staged for commit:" ]] || false
    [[ "$output" =~ "  (use \"dolt add <table>\" to update what will be committed)" ]] || false
    [[ "$output" =~ "  (use \"dolt checkout <table>\" to discard changes in working directory)" ]] || false
    [[ "$output" =~ "	deleted:          u" ]] || false
}

@test "status: checkout current branch" {
    run dolt checkout main
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Already on branch 'main'" ]] || false
}

@test "status: tables in conflict" {
    dolt sql <<SQL
CREATE TABLE t (pk int PRIMARY KEY, c0 int);
INSERT INTO t VALUES (1,1);
SQL
    dolt add -A && dolt commit -m "created table t"
    dolt checkout -b other
    dolt sql -q "INSERT INTO t VALUES (2,12);"
    dolt add -A && dolt commit -m "added values on branch other"
    dolt checkout main
    dolt sql -q "INSERT INTO t VALUES (2,2);"
    dolt add -A && dolt commit -m "added values on branch main"
    run dolt merge other
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONFLICT (content): Merge conflict in t" ]] || false
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "You have unmerged tables." ]] || false
    [[ "$output" =~ "  (fix conflicts and run \"dolt commit\")" ]] || false
    [[ "$output" =~ "  (use \"dolt merge --abort\" to abort the merge)" ]] || false
    [[ "$output" =~ "Unmerged paths:" ]] || false
    [[ "$output" =~ "  (use \"dolt add <table>...\" to mark resolution)" ]] || false
    [[ "$output" =~ "	both modified:    t" ]] || false
}

@test "status: renamed table" {
    dolt sql <<SQL
CREATE TABLE test (pk int PRIMARY KEY);
SQL
    dolt add test
    dolt commit -m 'added table test'
    run dolt sql -q 'alter table test rename to quiz'
    [ "$status" -eq 0 ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "deleted:          test" ]] || false
    [[ "$output" =~ "new table:        quiz" ]] || false
    dolt add .
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "renamed:          test -> quiz" ]] || false
}

@test "status: unstaged changes after reset" {
    dolt sql <<SQL
CREATE TABLE one (pk int PRIMARY KEY);
CREATE TABLE two (pk int PRIMARY KEY);
INSERT INTO  one VALUES (0);
INSERT INTO  two VALUES (0);
SQL
    dolt add -A && dolt commit -m "create tables one, two"
    dolt sql <<SQL
INSERT INTO  one VALUES (1);
DROP TABLE   two;
CREATE TABLE three (pk int PRIMARY KEY);
SQL
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "modified:         one" ]] || false
    [[ "$output" =~ "deleted:          two" ]] || false
    [[ "$output" =~ "new table:        three" ]] || false
    run dolt reset
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Unstaged changes after reset:" ]] || false
    [[ "$output" =~ "M	one" ]] || false
    [[ "$output" =~ "D	two" ]] || false
}

@test "status: dolt reset --hard <commit-spec>" {
    dolt sql -q "CREATE TABLE test (pk int PRIMARY KEY);"
    dolt add -A && dolt commit -m "made table test"
    dolt sql -q "INSERT INTO test VALUES (1);"
    dolt add -A && dolt commit -m "inserted 1"
    dolt sql -q "INSERT INTO test VALUES (2);"
    dolt add -A && dolt commit -m "inserted 2"
    dolt sql -q "INSERT INTO test VALUES (3);"

    run dolt reset --hard HEAD^
    [ "$status" -eq 0 ]

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false

    run dolt sql -q "SELECT sum(pk) FROM test" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1" ]] || false
}

@test "status: dolt checkout . should result in empty status" {
    dolt sql -q "CREATE TABLE test (pk int PRIMARY KEY);"
    dolt sql -q "INSERT INTO test VALUES (1)"
    dolt add -A && dolt commit -m "made table test"

    dolt sql -q "INSERT INTO test VALUES (2)"
    run dolt checkout .
    [ "$status" -eq 0 ]

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false

    run dolt sql -q "SELECT sum(pk) FROM test" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1" ]] || false

    dolt checkout -b test
    dolt sql -q "INSERT INTO test VALUES (2)"
    dolt add -A && dolt commit -m "insert into test value 2"

    run dolt sql -q "SELECT sum(pk) FROM test" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "3" ]] || false
}

@test "status: dolt reset --hard with more than one additional arg throws an error" {
    run dolt reset --hard HEAD HEAD2
    [ "$status" -eq 1 ]
}

@test "status: dolt reset hard with ~ works" {
    dolt sql -q "CREATE TABLE test (pk int PRIMARY KEY);"
    dolt add .
    dolt commit -am "cm1"

    dolt sql -q "INSERT INTO test values (1);"
    dolt commit -am "cm2"

    dolt sql -q "INSERT INTO test VALUES (2);"
    dolt commit -am "cm3"

    # Do a hard reset back one commit and confirm the appropriate values.
    run dolt reset --hard HEAD~1
    [ "$status" -eq 0 ]

    run dolt sql -q "SELECT sum(pk) FROM test;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1" ]] || false

    # Since this is a hard reset double check the status
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false

    # Run again with ~2 this time
    dolt sql -q "INSERT INTO test VALUES (2);"
    dolt commit -am "cm3"

    # Do a hard reset back two commits and confirm the appropriate values.
    run dolt reset --hard HEAD~2
    [ "$status" -eq 0 ]

    run dolt sql -q "SELECT sum(pk) FROM test;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "NULL" ]] || false

    # Since this is a hard reset double check the status
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false
}

@test "status: dolt reset soft with ~ works" {
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

@test "status: dolt reset with a renamed table" {
    dolt sql <<SQL
CREATE TABLE one (
  pk BIGINT PRIMARY KEY,
  v1 BIGINT,
  v2 BIGINT
);
SQL
    dolt add .
    dolt commit -am "added table"
    dolt sql -q "rename table one to one_super"

    dolt reset --hard
}

@test "status: dolt reset works with commit hash ref" {
    dolt sql -q "CREATE TABLE tb1 (pk int PRIMARY KEY);"
    dolt sql -q "INSERT INTO tb1 values (1);"
    dolt add .
    dolt commit -am "cm1"

    cm1=$(get_head_commit)

    dolt sql -q "CREATE TABLE tb2 (pk int PRIMARY KEY);"
    dolt sql -q "INSERT INTO tb2 values (11);"
    dolt add .
    dolt commit -am "cm2"

    cm2=$(get_head_commit)

    dolt sql -q "CREATE TABLE tb3 (pk int PRIMARY KEY);"
    dolt sql -q "INSERT INTO tb3 values (11);"
    dolt add .
    dolt commit -am "cm3"

    cm3=$(get_head_commit)

    # Try a soft reset to commit 3. Nothing should change
    dolt reset $cm3
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false

    # Do a soft reset to commit 2.
    dolt reset $cm2
    dolt status
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Untracked tables:" ]] || false
    [[ "$output" =~ "  (use \"dolt add <table>\" to include in what will be committed)" ]] || false
    [[ "$output" =~ "	new table:        tb3" ]] || false
    ! [[ "$output" =~ "tb2" ]] || false

    run dolt sql -q "SELECT COUNT(*) FROM tb3"
    [[ "$output" =~ "1" ]] || false

    run dolt sql -q "SELECT COUNT(*) FROM dolt_log"
    [[ "$output" =~ "3" ]] || false # includes init commit

    dolt add .
    dolt commit -am "commit 3"

    # Do a soft reset to commit 1
    dolt reset $cm1
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Untracked tables:" ]] || false
    [[ "$output" =~ "  (use \"dolt add <table>\" to include in what will be committed)" ]] || false
    [[ "$output" =~ "	new table:        tb3" ]] || false
    [[ "$output" =~ "	new table:        tb2" ]] || false
    ! [[ "$output" =~ "tb1" ]] || false

    run dolt sql -q "SELECT COUNT(*) FROM dolt_log"
    [[ "$output" =~ "2" ]] || false # includes init commit
}

@test "status: dolt reset works with branch ref" {
    dolt sql -q "CREATE TABLE tbl(pk int);"
    dolt add .
    dolt sql -q "INSERT into tbl VALUES (1)"
    dolt commit -am "cm1"

    # create a new branch and make a change
    dolt checkout -b test
    dolt sql -q "INSERT INTO tbl VALUES (2),(3)"
    dolt sql -q "CREATE TABLE tbl2(pk int);"
    dolt add .
    dolt commit -am "test cm1"

    # go back to main and merge
    dolt checkout main
    dolt merge test
    dolt sql -q "INSERT INTO tbl VALUES (4)"
    dolt commit -am "cm2"

    # execute the reset
    dolt reset test
    run dolt status

    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes not staged for commit:" ]] || false
    [[ "$output" =~ "  (use \"dolt add <table>\" to update what will be committed)" ]] || false
    [[ "$output" =~ "  (use \"dolt checkout <table>\" to discard changes in working directory)" ]] || false
    [[ "$output" =~ "	modified:         tbl" ]] || false
    ! [[ "$output" =~ "	new table:      tb2" ]] || false
}

@test "status: dolt reset ref properly manages staged changes as well" {
    dolt sql -q "CREATE TABLE tbl(pk int);"
    dolt add .
    dolt sql -q "INSERT into tbl VALUES (1)"
    dolt commit -am "cm1"

    dolt sql -q "INSERT INTO tbl VALUES (2)"
    dolt add .

    dolt reset HEAD
    run dolt status

    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes not staged for commit:" ]] || false
    [[ "$output" =~ "  (use \"dolt add <table>\" to update what will be committed)" ]] || false
    [[ "$output" =~ "  (use \"dolt checkout <table>\" to discard changes in working directory)" ]] || false
    [[ "$output" =~ "	modified:         tbl" ]] || false
}

@test "status: dolt reset throws errors for unknown ref/table" {
    run dolt reset test
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Failed to reset changes." ]] || false
    [[ "$output" =~ "branch not found" ]] || false
}

@test "status: roots runs even if status fails" {
    skip "todo: fix roots with chunk journal"
    mv .dolt/repo_state.json .dolt/repo_state.backup

    run dolt status
    [ "$status" -ne 0 ]

    # Trying to assert against the output of this command
    # can easily cause bats to fail with bats warning: Executed N instead of expected N tests

    run dolt roots
    [ "$status" -eq 0 ]
    [[ $(echo "$output" | grep -o "refs/heads/main" | xargs) =~ "refs/heads/main" ]] || false

    mv .dolt/repo_state.backup .dolt/repo_state.json
    [ "$status" -eq 0 ]
}

@test "status: ignored tables show up" {
    dolt sql <<SQL
CREATE TABLE t (pk int PRIMARY KEY);
CREATE TABLE u (pk int PRIMARY KEY);
INSERT INTO dolt_ignore VALUES ('generated_*', 1);
SQL
    dolt add -A && dolt commit -m "tables t, u"
    dolt sql <<SQL
INSERT INTO  t VALUES (1),(2),(3);
INSERT INTO  u VALUES (1),(2),(3);
CREATE TABLE v (pk int PRIMARY KEY);
CREATE TABLE generated_foo (pk int PRIMARY KEY);
SQL
    dolt add t
    run dolt status --ignored
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ "  (use \"dolt reset <table>...\" to unstage)" ]] || false
    [[ "$output" =~ "	modified:         t" ]] || false
    [[ "$output" =~ "Changes not staged for commit:" ]] || false
    [[ "$output" =~ "  (use \"dolt add <table>\" to update what will be committed)" ]] || false
    [[ "$output" =~ "  (use \"dolt checkout <table>\" to discard changes in working directory)" ]] || false
    [[ "$output" =~ "	modified:         u" ]] || false
    [[ "$output" =~ "Untracked tables:" ]] || false
    [[ "$output" =~ "  (use \"dolt add <table>\" to include in what will be committed)" ]] || false
    [[ "$output" =~ "	new table:        v" ]] || false
    [[ "$output" =~ "Ignored tables:" ]] || false
    [[ "$output" =~ "  (use \"dolt add -f <table>\" to include in what will be committed)" ]] || false
    [[ "$output" =~ "	new table:        generated_foo" ]] || false
}

@test "status: with remote" {
    mkdir remote
    mkdir repo1

    cd repo1
    dolt init
    dolt remote add origin file://../remote
    dolt push origin main

    cd ..
    dolt clone file://./remote repo2
    cd repo2

    run dolt status
    [ "$status" -eq 0 ]
    [[ "${lines[0]}" = "On branch main" ]] || false
    [[ "${lines[1]}" = "Your branch is up to date with 'origin/main'." ]] || false
    [[ "${lines[2]}" = "nothing to commit, working tree clean" ]] || false

    cd ../repo1
    dolt commit --allow-empty -m "cm1"
    dolt push origin main

    cd ../repo2
    dolt fetch origin
    run dolt status
    [ "$status" -eq 0 ]
    [[ "${lines[0]}" = "On branch main" ]] || false
    [[ "${lines[1]}" = "Your branch is behind 'origin/main' by 1 commit, and can be fast-forwarded." ]] || false
    [[ "${lines[2]}" = "  (use \"dolt pull\" to update your local branch)" ]] || false

    dolt commit --allow-empty -m "cm2"
    run dolt status
    [ "$status" -eq 0 ]
    [[ "${lines[0]}" = "On branch main" ]] || false
    [[ "${lines[1]}" = "Your branch and 'origin/main' have diverged," ]] || false
    [[ "${lines[2]}" = "and have 1 and 1 different commits each, respectively." ]] || false
    [[ "${lines[3]}" = "  (use \"dolt pull\" to update your local branch)" ]] || false

    dolt reset --hard origin/main
    dolt commit --allow-empty -m "cm3"
    run dolt status
    [ "$status" -eq 0 ]
    [[ "${lines[0]}" = "On branch main" ]] || false
    [[ "${lines[1]}" = "Your branch is ahead of 'origin/main' by 1 commit." ]] || false
    [[ "${lines[2]}" = "  (use \"dolt push\" to publish your local commits)" ]] || false
}

@test "status: with local upstream" {
    dolt branch br1 --track

    run dolt --branch br1 status
    [ "$status" -eq 0 ]
    [[ "${lines[0]}" = "On branch br1" ]] || false
    [[ "${lines[1]}" = "Your branch is up to date with 'main'." ]] || false
    [[ "${lines[2]}" = "nothing to commit, working tree clean" ]] || false

    dolt commit --allow-empty -m "Empty commit"
    run dolt --branch br1 status
    [ "$status" -eq 0 ]
    [[ "${lines[0]}" = "On branch br1" ]] || false
    [[ "${lines[1]}" = "Your branch is behind 'main' by 1 commit, and can be fast-forwarded." ]] || false
}

@test "status: tables with no observable changes don't show in status but can be staged" {
dolt sql <<SQL
create table t1 (id int primary key);
insert into t1 values (1);
SQL
    dolt add -A && dolt commit -m "create table t1"
    dolt sql <<SQL
create table temp__t1 (id int primary key);
insert into temp__t1 values (1);
drop table t1;
rename table temp__t1 to t1;
SQL
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false
    [[ ! $output =~ "t1" ]] || false

    run dolt diff t1
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 0 ]

    dolt add t1

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "modified:         t1" ]] || false
}

@test "status: after remove remote" {
    mkdir remote
    mkdir repo1

    cd repo1
    dolt init
    dolt remote add origin file://../remote
    dolt push origin main

    cd ..
    dolt clone file://./remote repo2
    cd repo2

    run dolt remote -v
    [ "$status" -eq 0 ]
    [[ "$output" =~ "origin file:///" ]] || false

    dolt remote rm origin

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch main" ]] || false
}
