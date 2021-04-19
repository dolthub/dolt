#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common

    dolt sql <<SQL
CREATE TABLE test (
    pk int primary key
);

INSERT INTO test VALUES (0),(1),(2);
SQL
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "sql-checkout: DOLT_CHECKOUT just works" {
    run dolt sql -q "SELECT DOLT_CHECKOUT('-b', 'feature-branch')"
    [ $status -eq 0 ]

    run dolt sql -q "SELECT DOLT_CHECKOUT('master');"
    [ $status -eq 0 ]
}

@test "sql-checkout: DOLT_CHECKOUT -b throws error on branches that already exist" {
    run dolt sql -q "SELECT DOLT_CHECKOUT('-b', 'master')"
    [ $status -eq 1 ]
}

@test "sql-checkout: DOLT_CHECKOUT throws error on branches that don't exist" {
    run dolt sql -q "SELECT DOLT_CHECKOUT('feature-branch')"
    [ $status -eq 1 ]
}

@test "sql-checkout: DOLT_CHECKOUT -b throws error on empty branch" {
    run dolt sql -q "SELECT DOLT_CHECKOUT('-b', '')"
    [ $status -eq 1 ]
}

@test "sql-checkout: DOLT_CHECKOUT properly carries unstaged and staged changes between new and existing branches." {
    run dolt sql -q "SELECT DOLT_CHECKOUT('-b', 'feature-branch')"
    [ $status -eq 0 ]

    run dolt ls
    [ $status -eq 0 ]
    [[ "$output" =~ "test" ]] || false

    run dolt status
    [ $status -eq 0 ]
    [[ "$output" =~ "On branch feature-branch" ]] || false
    [[ "$output" =~ "Untracked files" ]] || false
    [[ "$output" =~ ([[:space:]]*new table:[[:space:]]*test) ]] || false

    run dolt add .
    [ $status -eq 0 ]

    run dolt sql -q "SELECT DOLT_CHECKOUT('-b', 'feature-branch2')"
    [ $status -eq 0 ]

    run dolt ls
    [ $status -eq 0 ]
    [[ "$output" =~ "test" ]] || false

    run dolt status
    [ $status -eq 0 ]
    [[ "$output" =~ "Changes to be committed" ]] || false
    [[ "$output" =~ ([[:space:]]*new table:[[:space:]]*test) ]] || false

    run dolt sql -q "SELECT DOLT_CHECKOUT('master')"
    [ $status -eq 0 ]

    run dolt ls
    [ $status -eq 0 ]
    [[ "$output" =~ "test" ]] || false

    run dolt status
    [ $status -eq 0 ]
    [[ "$output" =~ "Changes to be committed" ]] || false
    [[ "$output" =~ ([[:space:]]*new table:[[:space:]]*test) ]] || false
}

@test "sql-checkout: DOLT CHECKOUT -b properly maintains session variables" {
    head_variable=@@dolt_repo_$$_head
    head_hash=$(get_head_commit)
    working_variable=@@dolt_repo_$$_working
    working_hash=$(get_working_hash)

    run dolt sql << SQL
SELECT DOLT_CHECKOUT('-b', 'feature-branch');
SELECT $working_variable;
SQL

    [ $status -eq 0 ]
    [[ "$output" =~ "$working_hash" ]] || false

    run dolt sql -q "SELECT $head_variable"
    [ $status -eq 0 ]
    [[ "$output" =~ "$head_hash" ]] || false
}

@test "sql-checkout: DOLT_CHECKOUT -b maintains system tables between two branches" {
    run dolt sql -q "SELECT * FROM dolt_diff_test";
    [ $status -eq 0 ]
    diff=$output

    run dolt sql << SQL
SELECT DOLT_CHECKOUT('-b', 'feature-branch');
SELECT * FROM dolt_diff_test;
SQL
    [ $status -eq 0 ]
    [[ "$output" =~ "$diff" ]] || false
}

@test "sql-checkout: DOLT_CHECKOUT paired with commit, add, reset, and merge works with no problems." {
    run dolt sql << SQL
SELECT DOLT_CHECKOUT('-b', 'feature-branch');
INSERT INTO test VALUES (4);
SELECT DOLT_ADD('.');
SELECT DOLT_COMMIT('-m', 'Commit1', '--author', 'John Doe <john@doe.com>');
SQL

    [ $status -eq 0 ]

    run dolt log -n 1
    [ $status -eq 0 ]
    [[ "$output" =~ "Commit1" ]] || false
    [[ "$output" =~ "John Doe" ]] || false

    dolt log -n 1
    dolt checkout master
    run dolt merge feature-branch

    [ $status -eq 0 ]
    run dolt log -n 1
    [[ "$output" =~ "Commit1" ]] || false
    [[ "$output" =~ "John Doe" ]] || false
}

@test "sql-checkout: DOLT_CHECKOUT works with tables." {
    run dolt sql << SQL
SELECT DOLT_CHECKOUT('-b', 'feature-branch');
SELECT DOLT_COMMIT('-a', '-m', 'commit');
INSERT INTO test VALUES (4);
SELECT DOLT_CHECKOUT('test');
SQL

    [ $status -eq 0 ]

    run dolt sql -q "SELECT * FROM test;" -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ "pk" ]] || false
    [[ "$output" =~ "0" ]] || false
    [[ "$output" =~ "1" ]] || false
    [[ "$output" =~ "2" ]] || false
    [[ ! "$output" =~ "4" ]] || false
}

@test "sql-checkout: DOLT_CHECKOUT between branches operating on the same table works." {
    run dolt sql << SQL
CREATE TABLE one_pk (
  pk1 BIGINT NOT NULL,
  c1 BIGINT,
  c2 BIGINT,
  PRIMARY KEY (pk1)
);
SELECT DOLT_COMMIT('-a', '-m', 'add tables');
SELECT DOLT_CHECKOUT('-b', 'feature-branch');
SELECT DOLT_CHECKOUT('master');
INSERT INTO one_pk (pk1,c1,c2) VALUES (0,0,0);
SELECT DOLT_COMMIT('-a', '-m', 'changed master');
SELECT DOLT_CHECKOUT('feature-branch');
INSERT INTO one_pk (pk1,c1,c2) VALUES (0,1,1);
SQL
    [ $status -eq 0 ]

    run dolt sql -q "SELECT * FROM one_pk" -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ "pk1,c1,c2" ]] || false
    [[ "$output" =~ "0,1,1" ]] || false
    [[ ! "$output" =~ "0,0,0" ]] || false

    dolt commit -a -m "changed feature branch"
    dolt checkout master
    run dolt sql -q "SELECT * FROM one_pk" -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ "pk1,c1,c2" ]] || false
    [[ ! "$output" =~ "0,1,1" ]] || false
    [[ "$output" =~ "0,0,0" ]] || false
}

get_head_commit() {
    dolt log -n 1 | grep -m 1 commit | cut -c 8-
}

get_working_hash() {
  dolt sql -q "select @@dolt_repo_$$_working" | sed -n 4p | sed -e 's/|//' -e 's/|//'  -e 's/ //'
}
