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

    # dolt sql -q "select dolt_checkout() should not change the branch
    # It changes the branch for that session which ends after the SQL
    # statements are executed. 
    run dolt status
    [ $status -eq 0 ]
    [[ "$output" =~ "main" ]] || false

    run dolt branch
    [ $status -eq 0 ]
    [[ "$output" =~ "feature-branch" ]] || false

    run dolt sql -q "SELECT DOLT_CHECKOUT('main');"
    [ $status -eq 0 ]

    run dolt status
    [ $status -eq 0 ]
    [[ "$output" =~ "main" ]] || false
}

@test "sql-checkout: DOLT_CHECKOUT -b throws error on branches that already exist" {
    run dolt sql -q "SELECT DOLT_CHECKOUT('-b', 'main')"
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

@test "sql-checkout: DOLT_CHECKOUT updates the head ref session var" {
    run dolt sql  <<SQL
SELECT DOLT_CHECKOUT('-b', 'feature-branch');
select @@dolt_repo_$$_head_ref;
SQL

    [ $status -eq 0 ]
    [[ "$output" =~ "refs/heads/feature-branch" ]] || false
}

@test "sql-checkout: DOLT_CHECKOUT changes branches, leaves behind working set unmodified." {
    dolt add . && dolt commit -m "0, 1, and 2 in test table"
    dolt sql -q "insert into test values (4);"

    run dolt status
    [ $status -eq 0 ]
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ ([[:space:]]*modified:[[:space:]]*test) ]] || false

    # After switching to a new branch, we don't see working set changes
    run dolt sql << SQL 
SELECT DOLT_CHECKOUT('-b', 'feature-branch');
select * from test where pk > 3;
SQL
    [ $status -eq 0 ]
    [[ ! "$output" =~ "4" ]] || false

    # the branch was created by dolt_checkout
    run dolt branch
    [ $status -eq 0 ]
    [[ "$output" =~ "feature-branch" ]] || false

    # but the shell is still on branch main, with the same changes as before
    run dolt status
    [ $status -eq 0 ]
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ ([[:space:]]*modified:[[:space:]]*test) ]] || false

    run dolt sql << SQL 
select * from test where pk > 3;
SQL
    [ $status -eq 0 ]
    [[ "$output" =~ "4" ]] || false
    
    run dolt sql << SQL
SELECT DOLT_CHECKOUT('-b', 'feature-branch2');
insert into test values (5);
select * from test where pk > 3;
SQL
    [ $status -eq 0 ]
    [[ ! "$output" =~ "4" ]] || false
    [[ "$output" =~ "5" ]] || false

    # working set from main has 4, but not 5
    run dolt sql -q "select * from test where pk > 3"
    [ $status -eq 0 ]
    [[ "$output" =~ "4" ]] || false
    [[ ! "$output" =~ "5" ]] || false

    run dolt status
    [ $status -eq 0 ]
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ ([[:space:]]*modified:[[:space:]]*test) ]] || false

    # In a new session, the value inserted should still be there
    run dolt sql << SQL
SELECT DOLT_CHECKOUT('feature-branch2');
select * from test where pk > 3;
SQL
    [ $status -eq 0 ]
    [[ ! "$output" =~ "4" ]] || false
    [[ "$output" =~ "5" ]] || false

    # This is an error on the command line, but not in SQL
    run dolt sql -q "SELECT DOLT_CHECKOUT('main')"
    [ $status -eq 0 ]
}

@test "sql-checkout: DOLT_CHECKOUT works with dolt_diff tables" {
    dolt add . && dolt commit -m "1, 2, and 3 in test table"

    run dolt sql -q "SELECT * FROM dolt_diff_test";
    [ $status -eq 0 ]
    emptydiff=$output

    run dolt sql << SQL
SELECT DOLT_CHECKOUT('-b', 'feature-branch');
SELECT * FROM dolt_diff_test;
SQL
    [ $status -eq 0 ]
    [[ "$output" =~ "$emptydiff" ]] || false

    run dolt sql << SQL
SELECT DOLT_CHECKOUT('feature-branch');
SELECT * FROM dolt_diff_test;
SQL
    [ $status -eq 0 ]
    [[ "$output" =~ "$emptydiff" ]] || false
    
    # add some changes to the working set
    dolt sql -q "insert into test values (4)"
    run dolt sql -q "SELECT * FROM dolt_diff_test";
    [ $status -eq 0 ]
    [[ ! "$output" =~ "$emptydiff" ]] || false

    run dolt sql << SQL
SELECT DOLT_CHECKOUT('-b', 'feature-branch2');
SELECT * FROM dolt_diff_test;
SQL
    [ $status -eq 0 ]
    [[ "$output" =~ "$emptydiff" ]] || false

    run dolt sql << SQL
SELECT DOLT_CHECKOUT('feature-branch2');
SELECT * FROM dolt_diff_test;
SQL
    [ $status -eq 0 ]
    [[ "$output" =~ "$emptydiff" ]] || false

}

@test "sql-checkout: DOLT_CHECKOUT followed by DOLT_COMMIT" {
    dolt add . && dolt commit -m "0, 1, and 2 in test table"    
    
    run dolt sql << SQL
SELECT DOLT_CHECKOUT('-b', 'feature-branch');
INSERT INTO test VALUES (4);
SELECT DOLT_ADD('.');
SELECT DOLT_COMMIT('-m', 'Added 4', '--author', 'John Doe <john@doe.com>');
SQL
    [ $status -eq 0 ]

    dolt status

    # on branch main, no changes visible
    run dolt log -n 1
    [[ ! "$output" =~ "Added 4" ]] || false
    [[ "$output" =~ "0, 1, and 2" ]] || false

    dolt checkout feature-branch
    run dolt log -n 1
    [ $status -eq 0 ]
    [[ "$output" =~ "Added 4" ]] || false
    [[ "$output" =~ "John Doe" ]] || false

    dolt checkout main
    run dolt merge feature-branch

    [ $status -eq 0 ]
    run dolt log -n 1
    [[ "$output" =~ "Added 4" ]] || false
    [[ "$output" =~ "John Doe" ]] || false
}

@test "sql-checkout: DOLT_CHECKOUT with table name clears working set changes" {
    dolt add . && dolt commit -m "0, 1, and 2 in test table"
    
    run dolt sql << SQL
SELECT DOLT_CHECKOUT('-b', 'feature-branch');
INSERT INTO test VALUES (4);
select * from test where pk > 3;
SQL

    [ $status -eq 0 ]
    [[ "$output" =~ "4" ]] || false

    run dolt sql << SQL
SELECT DOLT_CHECKOUT('feature-branch');
SELECT DOLT_CHECKOUT('test');
select * from test where pk > 3;
SQL

    [ $status -eq 0 ]
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
SELECT DOLT_CHECKOUT('main');
INSERT INTO one_pk (pk1,c1,c2) VALUES (0,0,0);
SELECT DOLT_COMMIT('-a', '-m', 'changed main');
SELECT DOLT_CHECKOUT('feature-branch');
INSERT INTO one_pk (pk1,c1,c2) VALUES (0,1,1);
select dolt_commit('-a', '-m', "changed feature-branch");
SQL
    [ $status -eq 0 ]

    run dolt sql -q "SELECT * FROM one_pk" -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ "pk1,c1,c2" ]] || false
    [[ ! "$output" =~ "0,1,1" ]] || false
    [[ "$output" =~ "0,0,0" ]] || false

    dolt checkout feature-branch
    run dolt sql -q "SELECT * FROM one_pk" -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ "pk1,c1,c2" ]] || false
    [[ "$output" =~ "0,1,1" ]] || false
    [[ ! "$output" =~ "0,0,0" ]] || false
}

@test "sql-checkout: DOLT_CHECKOUT does not throw an error when checking out to the same branch" {
  run dolt sql -q "SELECT DOLT_CHECKOUT('main')"
  [ $status -eq 0 ]
  [[ "$output" =~ "0" ]] || false
}

get_head_commit() {
    dolt log -n 1 | grep -m 1 commit | cut -c 8-
}

get_working_hash() {
  dolt sql -q "select @@dolt_repo_$$_working" | sed -n 4p | sed -e 's/|//' -e 's/|//'  -e 's/ //'
}
