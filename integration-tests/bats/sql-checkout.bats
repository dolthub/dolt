#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

bats_require_minimum_version 1.5.0

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
    run dolt sql -q "call dolt_checkout('-b', 'feature-branch')"
    [ $status -eq 0 ]

    # dolt sql -q "call dolt_checkout() should not change the branch
    # It changes the branch for that session which ends after the SQL
    # statements are executed. 
    run dolt status
    [ $status -eq 0 ]
    [[ "$output" =~ "main" ]] || false

    run dolt branch
    [ $status -eq 0 ]
    [[ "$output" =~ "feature-branch" ]] || false

    run dolt sql -q "call dolt_checkout('main');"
    [ $status -eq 0 ]

    run dolt status
    [ $status -eq 0 ]
    [[ "$output" =~ "main" ]] || false
}

@test "sql-checkout: DOLT_CHECKOUT warns when used as lone statement in dolt sql -q" {
    export NO_COLOR=1
    dolt branch feature-branch

    for query in \
        "call dolt_checkout('feature-branch')" \
        "CALL DOLT_CHECKOUT('feature-branch');" \
        "/* leading comment */ Call Dolt_Checkout('feature-branch'); /* trailing comment */" \
        " ; call dolt_checkout('feature-branch'); ; "
    do
        run --separate-stderr dolt sql -q "$query"
        [ "$status" -eq 0 ]
        [[ "$stderr" =~ "Your branch in the CLI is unchanged" ]] || false
        [[ ! "$output" =~ "Warning:" ]] || false
    done

    run dolt branch --show-current
    [ "$status" -eq 0 ]
    [ "$output" = "main" ]
}

@test "sql-checkout: DOLT_CHECKOUT branch creation warns without changing the CLI branch" {
    export NO_COLOR=1
    run --separate-stderr dolt sql -q "call dolt_checkout('-b', 'feature-branch')"
    [ "$status" -eq 0 ]
    [[ "$stderr" =~ "Your branch in the CLI is unchanged" ]] || false

    run dolt branch --show-current
    [ "$status" -eq 0 ]
    [ "$output" = "main" ]
    run dolt sql -r csv -q "select name from dolt_branches where name = 'feature-branch'"
    [ "$status" -eq 0 ]
    [ "$output" = $'name\nfeature-branch' ]
}

@test "sql-checkout: DOLT_CHECKOUT does not warn when combined with other statements in dolt sql -q" {
    dolt branch feature-branch

    for query in \
        "call dolt_checkout('feature-branch'); select active_branch();" \
        "select 1; call dolt_checkout('feature-branch'); select active_branch();" \
        "select 1; call dolt_checkout('feature-branch');" \
        "call dolt_checkout('feature-branch'); call dolt_checkout('main');"
    do
        run --separate-stderr dolt sql -r csv -q "$query"
        [ "$status" -eq 0 ]
        [ -z "$stderr" ]
        [[ ! "$output" =~ "Warning:" ]] || false
        if [[ "$query" =~ "active_branch" ]]; then
            [[ "$output" = *$'active_branch()\nfeature-branch' ]] || false
        fi
    done

    run dolt branch --show-current
    [ "$status" -eq 0 ]
    [ "$output" = "main" ]
}

@test "sql-checkout: unrelated queries and failed checkouts do not warn" {
    for query in "select 'dolt_checkout'" "call dolt_branch('feature-branch')" "/* only a comment */"
    do
        run --separate-stderr dolt sql -q "$query"
        [ "$status" -eq 0 ]
        [ -z "$stderr" ]
    done

    run --separate-stderr dolt sql -q "call dolt_checkout('missing-branch')"
    [ "$status" -ne 0 ]
    [[ "$stderr" =~ "missing-branch" ]] || false
    [[ ! "$stderr" =~ "Warning:" ]] || false

    run --separate-stderr dolt sql --continue -q "call dolt_checkout('missing-branch')"
    [ "$status" -eq 0 ]
    [[ "$stderr" =~ "missing-branch" ]] || false
    [[ ! "$stderr" =~ "Warning:" ]] || false
}

@test "sql-checkout: redirected lone checkout warns without changing the CLI branch" {
    export NO_COLOR=1
    dolt branch feature-branch
    echo "call dolt_checkout('feature-branch');" > queries.sql

    run --separate-stderr dolt sql < queries.sql
    [ "$status" -eq 0 ]
    [[ "$stderr" =~ "Your branch in the CLI is unchanged" ]] || false
    [[ ! "$output" =~ "Warning:" ]] || false

    run dolt branch --show-current
    [ "$status" -eq 0 ]
    [ "$output" = "main" ]
}

@test "sql-checkout: redirected multiple statements and failed checkouts do not warn" {
    dolt branch feature-branch
    echo "call dolt_checkout('feature-branch'); select active_branch();" > queries.sql

    run --separate-stderr dolt sql -r csv < queries.sql
    [ "$status" -eq 0 ]
    [ -z "$stderr" ]
    [[ "$output" = *$'active_branch()\nfeature-branch' ]] || false

    echo "call dolt_checkout('missing-branch');" > queries.sql
    run --separate-stderr dolt sql < queries.sql
    [ "$status" -ne 0 ]
    [[ "$stderr" =~ "missing-branch" ]] || false
    [[ ! "$stderr" =~ "Warning:" ]] || false
}

@test "sql-checkout: DOLT_CHECKOUT -b throws error on branches that already exist" {
    run dolt sql -q "call dolt_checkout('-b', 'main')"
    [ $status -eq 1 ]
}

@test "sql-checkout: DOLT_CHECKOUT throws error on branches that don't exist" {
    run dolt sql -q "call dolt_checkout('feature-branch')"
    [ $status -eq 1 ]
}

@test "sql-checkout: DOLT_CHECKOUT -b throws error on empty branch" {
    run dolt sql -q "call dolt_checkout('-b', '')"
    [ $status -eq 1 ]
}

@test "sql-checkout: DOLT_CHECKOUT updates the head ref session var" {
    export DOLT_DBNAME_REPLACE="true"
    run dolt sql  <<SQL
call dolt_checkout('-b', 'feature-branch');
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
call dolt_checkout('-b', 'feature-branch');
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
call dolt_checkout('-b', 'feature-branch2');
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
call dolt_checkout('feature-branch2');
select * from test where pk > 3;
SQL
    [ $status -eq 0 ]
    [[ ! "$output" =~ "4" ]] || false
    [[ "$output" =~ "5" ]] || false

    # This is an error on the command line, but not in SQL
    run dolt sql -q "call dolt_checkout('main')"
    [ $status -eq 0 ]
}

@test "sql-checkout: DOLT_CHECKOUT works with dolt_diff tables" {
    dolt add . && dolt commit -m "1, 2, and 3 in test table"

    run dolt sql -q "SELECT * FROM dolt_diff_test";
    [ $status -eq 0 ]
    emptydiff=$output

    run dolt sql << SQL
call dolt_checkout('-b', 'feature-branch');
SELECT * FROM dolt_diff_test;
SQL
    [ $status -eq 0 ]
    [[ "$output" =~ "$emptydiff" ]] || false

    run dolt sql << SQL
call dolt_checkout('feature-branch');
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
call dolt_checkout('-b', 'feature-branch2');
SELECT * FROM dolt_diff_test;
SQL
    [ $status -eq 0 ]
    [[ "$output" =~ "$emptydiff" ]] || false

    run dolt sql << SQL
call dolt_checkout('feature-branch2');
SELECT * FROM dolt_diff_test;
SQL
    [ $status -eq 0 ]
    [[ "$output" =~ "$emptydiff" ]] || false

}

@test "sql-checkout: DOLT_CHECKOUT followed by DOLT_COMMIT" {
    dolt add . && dolt commit -m "0, 1, and 2 in test table"    
    
    run dolt sql << SQL
call dolt_checkout('-b', 'feature-branch');
INSERT INTO test VALUES (4);
call dolt_add('.');
call dolt_commit('-m', 'Added 4', '--author', 'John Doe <john@doe.com>');
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
    run dolt merge feature-branch --no-commit
    [ $status -eq 0 ]

    run dolt log -n 1
    [[ "$output" =~ "Added 4" ]] || false
    [[ "$output" =~ "John Doe" ]] || false
}

@test "sql-checkout: DOLT_CHECKOUT with table name clears working set changes" {
    dolt add . && dolt commit -m "0, 1, and 2 in test table"
    
    run dolt sql << SQL
call dolt_checkout('-b', 'feature-branch');
INSERT INTO test VALUES (4);
select * from test where pk > 3;
SQL

    [ $status -eq 0 ]
    [[ "$output" =~ "4" ]] || false

    run dolt sql << SQL
call dolt_checkout('feature-branch');
call dolt_checkout('test');
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
call dolt_add('.');
call dolt_commit('-a', '-m', 'add tables');
call dolt_checkout('-b', 'feature-branch');
call dolt_checkout('main');
INSERT INTO one_pk (pk1,c1,c2) VALUES (0,0,0);
call dolt_commit('-a', '-m', 'changed main');
call dolt_checkout('feature-branch');
INSERT INTO one_pk (pk1,c1,c2) VALUES (0,1,1);
call dolt_commit('-a', '-m', "changed feature-branch");
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
  run dolt sql -q "call dolt_checkout('main')"
  [ $status -eq 0 ]
  [[ "$output" =~ "0" ]] || false
}

@test "sql-checkout: CALL DOLT_CHECKOUT can successfully checkout a branch that does not have a workingset" {
  # Some code paths in dolt, especially in older versions of dolt, would create
  # branches without working sets. CLI `dolt checkout` will check these out
  # fine. CALL DOLT_CHECKOUT needs to be able to too.

  h=`get_head_commit`

  # First we test the case where there is no remote tracking branch associate with this branch.

  dolt admin set-ref --branch no_working_set --to "$h"
  run dolt sql -q 'CALL DOLT_CHECKOUT("no_working_set")'
  [ $status -eq 0 ]
  [[ "$output" =~ "0" ]] || false

  # Then we test the same behavior but with a remote tracking branch around as well.

  dolt remote add origin https://localhost:50051/doesnot/work
  dolt admin set-ref --remote-name origin --remote-branch no_working_set --to "$h"
  run dolt sql -q 'CALL DOLT_CHECKOUT("no_working_set")'
  [ $status -eq 0 ]
  [[ "$output" =~ "0" ]] || false
}

@test "sql-checkout: 'CALL DOLT_CHECKOUT --move' moves the working set" {
    dolt branch other
    run dolt sql -r csv << SQL
call dolt_checkout('other', '--move');
select active_branch();
select * from dolt_status;
SQL
    [ $status -eq 0 ]
    [[ "${lines[3]}" =~ "other" ]] || false
    [[ "${lines[5]}" =~ "test,0,new table" ]] || false
}

@test "sql-checkout: DOLT_CHECKOUT with --no-overwrite-ignore aborts when ignored table would be overwritten" {
    dolt sql <<SQL
CREATE TABLE ignored_tbl (pk int PRIMARY KEY, val int);
INSERT INTO ignored_tbl VALUES (1, 100);
INSERT INTO dolt_ignore VALUES ('ignored_tbl', true);
SQL
    dolt add -A --force
    dolt commit -m "setup ignored table on main"

    dolt checkout -b other
    dolt sql -q "INSERT INTO ignored_tbl VALUES (2, 200)"
    dolt add -A --force
    dolt commit -m "modify ignored table on other" --force

    dolt checkout main

    run dolt sql -q "CALL DOLT_CHECKOUT('--no-overwrite-ignore', 'other')"
    [ $status -ne 0 ]
    [[ "$output" =~ "ignored tables would be overwritten by checkout" ]] || false
    [[ "$output" =~ "ignored_tbl" ]] || false
}

@test "sql-checkout: DOLT_CHECKOUT with --overwrite-ignore succeeds" {
    dolt sql <<SQL
CREATE TABLE ignored_tbl (pk int PRIMARY KEY, val int);
INSERT INTO ignored_tbl VALUES (1, 100);
INSERT INTO dolt_ignore VALUES ('ignored_tbl', true);
SQL
    dolt add -A --force
    dolt commit -m "setup ignored table on main"

    dolt checkout -b other
    dolt sql -q "INSERT INTO ignored_tbl VALUES (2, 200)"
    dolt add -A --force
    dolt commit -m "modify ignored table on other" --force

    dolt checkout main

    run dolt sql -q "CALL DOLT_CHECKOUT('--overwrite-ignore', 'other')"
    [ $status -eq 0 ]
}

@test "sql-checkout: DOLT_CHECKOUT with --move and --no-overwrite-ignore aborts" {
    dolt sql <<SQL
CREATE TABLE ignored_tbl (pk int PRIMARY KEY, val int);
INSERT INTO ignored_tbl VALUES (1, 100);
INSERT INTO dolt_ignore VALUES ('ignored_tbl', true);
SQL
    dolt add -A --force
    dolt commit -m "setup ignored table on main"

    dolt checkout -b other
    dolt sql -q "INSERT INTO ignored_tbl VALUES (2, 200)"
    dolt add -A --force
    dolt commit -m "modify ignored table on other" --force

    dolt checkout main

    run dolt sql -q "CALL DOLT_CHECKOUT('--move', '--no-overwrite-ignore', 'other')"
    [ $status -ne 0 ]
    [[ "$output" =~ "ignored tables would be overwritten by checkout" ]] || false
}

@test "sql-checkout: DOLT_CHECKOUT --overwrite-ignore and --no-overwrite-ignore are mutually exclusive" {
    dolt branch other
    run dolt sql -q "CALL DOLT_CHECKOUT('--overwrite-ignore', '--no-overwrite-ignore', 'other')"
    [ $status -ne 0 ]
    [[ "$output" =~ "mutually exclusive" ]] || false
}

@test "sql-checkout: DOLT_CHECKOUT -b with --no-overwrite-ignore aborts when ignored table differs at start point" {
    dolt sql <<SQL
CREATE TABLE ignored_tbl (pk int PRIMARY KEY, val int);
INSERT INTO ignored_tbl VALUES (1, 100);
INSERT INTO dolt_ignore VALUES ('ignored_tbl', true);
SQL
    dolt add -A --force
    dolt commit -m "add ignored table on main" --force

    dolt sql -q "INSERT INTO ignored_tbl VALUES (2, 200)"
    dolt add -A --force
    dolt commit -m "modify ignored table" --force

    # Direct SQL call (no --move): creating branch from HEAD~1 where ignored_tbl differs
    run dolt sql -q "CALL DOLT_CHECKOUT('-b', 'newbranch', '--no-overwrite-ignore', 'HEAD~1')"
    [ "$status" -ne 0 ]
    [[ "$output" =~ "ignored tables would be overwritten by checkout" ]] || false
    [[ "$output" =~ "ignored_tbl" ]] || false
}

@test "sql-checkout: DOLT_CHECKOUT -b with --overwrite-ignore succeeds when ignored table differs at start point" {
    dolt sql <<SQL
CREATE TABLE ignored_tbl (pk int PRIMARY KEY, val int);
INSERT INTO ignored_tbl VALUES (1, 100);
INSERT INTO dolt_ignore VALUES ('ignored_tbl', true);
SQL
    dolt add -A --force
    dolt commit -m "add ignored table on main" --force

    dolt sql -q "INSERT INTO ignored_tbl VALUES (2, 200)"
    dolt add -A --force
    dolt commit -m "modify ignored table" --force

    run dolt sql -q "CALL DOLT_CHECKOUT('-b', 'newbranch', '--overwrite-ignore', 'HEAD~1')"
    [ "$status" -eq 0 ]
}

@test "sql-checkout: DOLT_CHECKOUT -b with --no-overwrite-ignore from HEAD succeeds (hashes identical)" {
    dolt sql <<SQL
CREATE TABLE ignored_tbl (pk int PRIMARY KEY, val int);
INSERT INTO ignored_tbl VALUES (1, 100);
INSERT INTO dolt_ignore VALUES ('ignored_tbl', true);
SQL
    dolt add -A --force
    dolt commit -m "add ignored table on main" --force

    # Creating from HEAD: hashes always match, no overwrite possible
    run dolt sql -q "CALL DOLT_CHECKOUT('-b', 'newbranch', '--no-overwrite-ignore')"
    [ "$status" -eq 0 ]
}

get_head_commit() {
    dolt log -n 1 | grep -m 1 commit | awk '{print $2}'
}
