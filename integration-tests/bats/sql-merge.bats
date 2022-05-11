#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    skip_nbf_dolt_1

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

@test "sql-merge: DOLT_MERGE with unknown branch name throws an error" {
    dolt sql -q "SELECT DOLT_COMMIT('-a', '-m', 'Step 1');"

    run dolt sql -q "SELECT DOLT_MERGE('feature-branch');"
    [ $status -eq 1 ]
}

@test "sql-merge: CALL DOLT_MERGE with unknown branch name throws an error" {
    dolt sql -q "CALL DOLT_COMMIT('-a', '-m', 'Step 1');"

    run dolt sql -q "CALL DOLT_MERGE('feature-branch');"
    [ $status -eq 1 ]
}

@test "sql-merge: DOLT_MERGE works with ff" {
    dolt sql <<SQL
SELECT DOLT_COMMIT('-a', '-m', 'Step 1');
SELECT DOLT_CHECKOUT('-b', 'feature-branch');
INSERT INTO test VALUES (3);
UPDATE test SET pk=1000 WHERE pk=0;
SELECT DOLT_COMMIT('-a', '-m', 'this is a ff');
SELECT DOLT_CHECKOUT('main');
SQL
    run dolt sql -q "SELECT DOLT_MERGE('feature-branch');"
    [ $status -eq 0 ]

    run dolt log -n 1
    [ $status -eq 0 ]
    [[ "$output" =~ "this is a ff" ]] || false

    run dolt sql -q "SELECT COUNT(*) FROM dolt_log"
    [ $status -eq 0 ]
    [[ "$output" =~ "3" ]] || false

    run dolt status
    [ $status -eq 0 ]
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false

    run dolt sql -q "SELECT * FROM test;" -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ "pk" ]] || false
    [[ "$output" =~ "1" ]] || false
    [[ "$output" =~ "2" ]] || false
    [[ "$output" =~ "3" ]] || false
    [[ "$output" =~ "1000" ]] || false

    run dolt sql -q "SELECT COUNT(*) FROM test;" -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ "4" ]] || false
}

@test "sql-merge: CALL DOLT_MERGE works with ff" {
    dolt sql <<SQL
CALL DOLT_COMMIT('-a', '-m', 'Step 1');
CALL DOLT_CHECKOUT('-b', 'feature-branch');
INSERT INTO test VALUES (3);
UPDATE test SET pk=1000 WHERE pk=0;
CALL DOLT_COMMIT('-a', '-m', 'this is a ff');
CALL DOLT_CHECKOUT('main');
SQL
    run dolt sql -q "CALL DOLT_MERGE('feature-branch');"
    [ $status -eq 0 ]

    run dolt log -n 1
    [ $status -eq 0 ]
    [[ "$output" =~ "this is a ff" ]] || false

    run dolt sql -q "SELECT COUNT(*) FROM dolt_log"
    [ $status -eq 0 ]
    [[ "$output" =~ "3" ]] || false

    run dolt status
    [ $status -eq 0 ]
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false

    run dolt sql -q "SELECT * FROM test;" -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ "pk" ]] || false
    [[ "$output" =~ "1" ]] || false
    [[ "$output" =~ "2" ]] || false
    [[ "$output" =~ "3" ]] || false
    [[ "$output" =~ "1000" ]] || false

    run dolt sql -q "SELECT COUNT(*) FROM test;" -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ "4" ]] || false
}

@test "sql-merge: CALL DMERGE works with ff" {
    dolt sql <<SQL
CALL DCOMMIT('-a', '-m', 'Step 1');
CALL DCHECKOUT('-b', 'feature-branch');
INSERT INTO test VALUES (3);
UPDATE test SET pk=1000 WHERE pk=0;
CALL DCOMMIT('-a', '-m', 'this is a ff');
CALL DCHECKOUT('main');
SQL
    run dolt sql -q "CALL DMERGE('feature-branch');"
    [ $status -eq 0 ]

    run dolt log -n 1
    [ $status -eq 0 ]
    [[ "$output" =~ "this is a ff" ]] || false

    run dolt sql -q "SELECT COUNT(*) FROM dolt_log"
    [ $status -eq 0 ]
    [[ "$output" =~ "3" ]] || false

    run dolt status
    [ $status -eq 0 ]
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false

    run dolt sql -q "SELECT * FROM test;" -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ "pk" ]] || false
    [[ "$output" =~ "1" ]] || false
    [[ "$output" =~ "2" ]] || false
    [[ "$output" =~ "3" ]] || false
    [[ "$output" =~ "1000" ]] || false

    run dolt sql -q "SELECT COUNT(*) FROM test;" -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ "4" ]] || false
}

@test "sql-merge: DOLT_MERGE works in the session for fastforward." {
     run dolt sql << SQL
SELECT DOLT_COMMIT('-a', '-m', 'Step 1');
SELECT DOLT_CHECKOUT('-b', 'feature-branch');
INSERT INTO test VALUES (3);
SELECT DOLT_COMMIT('-a', '-m', 'this is a ff');
SELECT DOLT_CHECKOUT('main');
SELECT DOLT_MERGE('feature-branch');
SELECT COUNT(*) > 0 FROM test WHERE pk=3;
SQL
    [ $status -eq 0 ]
    [[ "$output" =~ "true" ]] || false

    run dolt sql -r csv -q "select count(*) from dolt_status"
    [ "${#lines[@]}" -eq 2 ]
    [ "${lines[1]}" = "0" ]

    run dolt sql -r csv -q "select hash from dolt_branches where branch='main'"
    MAIN_HASH=${lines[1]}

    run dolt sql -r csv -q "select hash from dolt_branches where branch='feature-branch'"
    FB_HASH=${lines[1]}

    [ "$MAIN_HASH" = "$FB_HASH" ]
}

@test "sql-merge: CALL DOLT_MERGE works in the session for fastforward." {
     run dolt sql << SQL
CALL DOLT_COMMIT('-a', '-m', 'Step 1');
CALL DOLT_CHECKOUT('-b', 'feature-branch');
INSERT INTO test VALUES (3);
CALL DOLT_COMMIT('-a', '-m', 'this is a ff');
CALL DOLT_CHECKOUT('main');
CALL DOLT_MERGE('feature-branch');
SELECT COUNT(*) > 0 FROM test WHERE pk=3;
SQL
    [ $status -eq 0 ]
    [[ "$output" =~ "true" ]] || false

    run dolt sql -r csv -q "select count(*) from dolt_status"
    [ "${#lines[@]}" -eq 2 ]
    [ "${lines[1]}" = "0" ]

    run dolt sql -r csv -q "select hash from dolt_branches where branch='main'"
    MAIN_HASH=${lines[1]}

    run dolt sql -r csv -q "select hash from dolt_branches where branch='feature-branch'"
    FB_HASH=${lines[1]}

    [ "$MAIN_HASH" = "$FB_HASH" ]
}

@test "sql-merge: DOLT_MERGE with autocommit off works in fast-forward." {
     dolt sql << SQL
set autocommit = off;
SELECT DOLT_COMMIT('-a', '-m', 'Step 1');
SELECT DOLT_CHECKOUT('-b', 'feature-branch');
INSERT INTO test VALUES (3);
SELECT DOLT_COMMIT('-a', '-m', 'this is a ff');
SELECT DOLT_CHECKOUT('main');
SELECT DOLT_MERGE('feature-branch');
SELECT DOLT_CHECKOUT('-b', 'new-branch');
SQL

    run dolt sql -r csv -q "select * from test order by pk"
    [ "${#lines[@]}" -eq 5 ]
    [ "${lines[1]}" = "0" ]
    [ "${lines[2]}" = "1" ]
    [ "${lines[3]}" = "2" ]
    [ "${lines[4]}" = "3" ]
}

@test "sql-merge: CALL DOLT_MERGE with autocommit off works in fast-forward." {
     dolt sql << SQL
set autocommit = off;
CALL DOLT_COMMIT('-a', '-m', 'Step 1');
CALL DOLT_CHECKOUT('-b', 'feature-branch');
INSERT INTO test VALUES (3);
CALL DOLT_COMMIT('-a', '-m', 'this is a ff');
CALL DOLT_CHECKOUT('main');
CALL DOLT_MERGE('feature-branch');
SELECT DOLT_CHECKOUT('-b', 'new-branch');
SQL

    run dolt sql -r csv -q "select * from test order by pk"
    [ "${#lines[@]}" -eq 5 ]
    [ "${lines[1]}" = "0" ]
    [ "${lines[2]}" = "1" ]
    [ "${lines[3]}" = "2" ]
    [ "${lines[4]}" = "3" ]
}

@test "sql-merge: DOLT_MERGE no-ff works with autocommit off." {
     dolt sql << SQL
set autocommit = off;
SELECT DOLT_COMMIT('-a', '-m', 'Step 1');
SELECT DOLT_CHECKOUT('-b', 'feature-branch');
INSERT INTO test VALUES (3);
SELECT DOLT_COMMIT('-a', '-m', 'this is a ff');
SELECT DOLT_CHECKOUT('main');
SELECT DOLT_MERGE('feature-branch', '-no-ff');
COMMIT;
SQL

    run dolt sql -r csv -q "select * from test order by pk"
    [ "${#lines[@]}" -eq 5 ]
    [ "${lines[1]}" = "0" ]
    [ "${lines[2]}" = "1" ]
    [ "${lines[3]}" = "2" ]
    [ "${lines[4]}" = "3" ]
}

@test "sql-merge: CALL DOLT_MERGE no-ff works with autocommit off." {
     dolt sql << SQL
set autocommit = off;
CALL DOLT_COMMIT('-a', '-m', 'Step 1');
CALL DOLT_CHECKOUT('-b', 'feature-branch');
INSERT INTO test VALUES (3);
CALL DOLT_COMMIT('-a', '-m', 'this is a ff');
CALL DOLT_CHECKOUT('main');
CALL DOLT_MERGE('feature-branch', '-no-ff');
COMMIT;
SQL

    run dolt sql -r csv -q "select * from test order by pk"
    [ "${#lines[@]}" -eq 5 ]
    [ "${lines[1]}" = "0" ]
    [ "${lines[2]}" = "1" ]
    [ "${lines[3]}" = "2" ]
    [ "${lines[4]}" = "3" ]
}

@test "sql-merge: End to End Conflict Resolution with autocommit off." {
    dolt sql << SQL
CREATE TABLE test2 (pk int primary key, val int);
INSERT INTO test2 VALUES (0, 0);
SET autocommit = 0;
SELECT DOLT_COMMIT('-a', '-m', 'Step 1');
SELECT DOLT_CHECKOUT('-b', 'feature-branch');
INSERT INTO test2 VALUES (1, 1);
UPDATE test2 SET val=1000 WHERE pk=0;
SELECT DOLT_COMMIT('-a', '-m', 'this is a normal commit');
SELECT DOLT_CHECKOUT('main');
UPDATE test2 SET val=1001 WHERE pk=0;
SELECT DOLT_COMMIT('-a', '-m', 'update a value');
SELECT DOLT_MERGE('feature-branch', '-m', 'this is a merge');
DELETE FROM dolt_conflicts_test2;
SELECT DOLT_COMMIT('-a', '-m', 'remove conflicts');
SQL

    run dolt sql -r csv -q "select * from test2 order by pk"
    [ "${#lines[@]}" -eq 3 ]
    [ "${lines[1]}" = "0,1001" ]
    [ "${lines[2]}" = "1,1" ]
}

@test "sql-merge: CALL End to End Conflict Resolution with autocommit off." {
    dolt sql << SQL
CREATE TABLE test2 (pk int primary key, val int);
INSERT INTO test2 VALUES (0, 0);
SET autocommit = 0;
CALL DOLT_COMMIT('-a', '-m', 'Step 1');
CALL DOLT_CHECKOUT('-b', 'feature-branch');
INSERT INTO test2 VALUES (1, 1);
UPDATE test2 SET val=1000 WHERE pk=0;
CALL DOLT_COMMIT('-a', '-m', 'this is a normal commit');
CALL DOLT_CHECKOUT('main');
UPDATE test2 SET val=1001 WHERE pk=0;
CALL DOLT_COMMIT('-a', '-m', 'update a value');
CALL DOLT_MERGE('feature-branch', '-m', 'this is a merge');
DELETE FROM dolt_conflicts_test2;
CALL DOLT_COMMIT('-a', '-m', 'remove conflicts');
SQL

    run dolt sql -r csv -q "select * from test2 order by pk"
    [ "${#lines[@]}" -eq 3 ]
    [ "${lines[1]}" = "0,1001" ]
    [ "${lines[2]}" = "1,1" ]
}

@test "sql-merge: DOLT_MERGE works with autocommit off." {
    dolt sql << SQL
set autocommit = off;
SELECT DOLT_COMMIT('-a', '-m', 'Step 1');
SELECT DOLT_CHECKOUT('-b', 'feature-branch');
INSERT INTO test VALUES (3);
SELECT DOLT_COMMIT('-a', '-m', 'this is a normal commit');
SELECT DOLT_CHECKOUT('main');
INSERT INTO test VALUES (5);
SELECT DOLT_COMMIT('-a', '-m', 'this is a normal commit');
SELECT DOLT_MERGE('feature-branch');
COMMIT;
SQL

    run dolt sql -r csv -q "select * from test order by pk"
    [ "${#lines[@]}" -eq 6 ]
    [ "${lines[1]}" = "0" ]
    [ "${lines[2]}" = "1" ]
    [ "${lines[3]}" = "2" ]
    [ "${lines[4]}" = "3" ]
    [ "${lines[5]}" = "5" ]
}

@test "sql-merge: CALL DOLT_MERGE works with autocommit off." {
    dolt sql << SQL
set autocommit = off;
CALL DOLT_COMMIT('-a', '-m', 'Step 1');
CALL DOLT_CHECKOUT('-b', 'feature-branch');
INSERT INTO test VALUES (3);
CALL DOLT_COMMIT('-a', '-m', 'this is a normal commit');
CALL DOLT_CHECKOUT('main');
INSERT INTO test VALUES (5);
CALL DOLT_COMMIT('-a', '-m', 'this is a normal commit');
CALL DOLT_MERGE('feature-branch');
COMMIT;
SQL

    run dolt sql -r csv -q "select * from test order by pk"
    [ "${#lines[@]}" -eq 6 ]
    [ "${lines[1]}" = "0" ]
    [ "${lines[2]}" = "1" ]
    [ "${lines[3]}" = "2" ]
    [ "${lines[4]}" = "3" ]
    [ "${lines[5]}" = "5" ]
}

@test "sql-merge: DOLT_MERGE correctly returns head and working session variables." {
    dolt sql << SQL
SELECT DOLT_COMMIT('-a', '-m', 'Step 1');
SELECT DOLT_CHECKOUT('-b', 'feature-branch');
INSERT INTO test VALUES (3);
SELECT DOLT_COMMIT('-a', '-m', 'this is a ff');
SQL
    head_variable=@@dolt_repo_$$_head

    dolt checkout feature-branch
    head_hash=$(get_head_commit)

    dolt sql << SQL
SELECT DOLT_CHECKOUT('main');
SELECT DOLT_MERGE('feature-branch');
SQL

    run dolt sql -q "SELECT $head_variable"
    [ $status -eq 0 ]
    [[ "$output" =~ $head_hash ]] || false

    dolt checkout main
    run dolt sql -q "SELECT $head_variable"
    [ $status -eq 0 ]
    [[ "$output" =~ $head_hash ]] || false
}

@test "sql-merge: CALL DOLT_MERGE correctly returns head and working session variables." {
    dolt sql << SQL
CALL DOLT_COMMIT('-a', '-m', 'Step 1');
CALL DOLT_CHECKOUT('-b', 'feature-branch');
INSERT INTO test VALUES (3);
CALL DOLT_COMMIT('-a', '-m', 'this is a ff');
SQL
    head_variable=@@dolt_repo_$$_head

    dolt checkout feature-branch
    head_hash=$(get_head_commit)

    dolt sql << SQL
CALL DOLT_CHECKOUT('main');
CALL DOLT_MERGE('feature-branch');
SQL

    run dolt sql -q "SELECT $head_variable"
    [ $status -eq 0 ]
    [[ "$output" =~ $head_hash ]] || false

    dolt checkout main
    run dolt sql -q "SELECT $head_variable"
    [ $status -eq 0 ]
    [[ "$output" =~ $head_hash ]] || false
}

@test "sql-merge: DOLT_MERGE correctly merges branches with differing content in same table without conflicts" {
    dolt sql << SQL
SELECT DOLT_COMMIT('-a', '-m', 'Step 1');
SELECT DOLT_CHECKOUT('-b', 'feature-branch');
INSERT INTO test VALUES (3);
SELECT DOLT_COMMIT('-a', '-m', 'Insert 3');
SELECT DOLT_CHECKOUT('main');
INSERT INTO test VALUES (10000);
SELECT DOLT_COMMIT('-a', '-m', 'Insert 10000');
SQL

    run dolt sql << SQL
SELECT DOLT_MERGE('feature-branch');
SELECT COUNT(*) = 2 FROM test WHERE pk > 2;
SQL

    [ $status -eq 0 ]
    [[ "$output" =~ "true" ]] || false
    [[ "$output" =~ "true" ]] || false
    [[ "${lines[1]}" =~ "DOLT_MERGE('feature-branch')" ]] || false # validate that merge returns 1 not "Updating..."
    [[ "${lines[3]}" =~ "1" ]] || false
    ! [[ "$output" =~ "Updating" ]] || false

    run dolt sql -q "SELECT * FROM test" -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ "pk" ]] || false
    [[ "$output" =~ "0" ]] || false
    [[ "$output" =~ "1" ]] || false
    [[ "$output" =~ "2" ]] || false
    [[ "$output" =~ "3" ]] || false
    [[ "$output" =~ "10000" ]] || false

    run dolt log -n 1
    [ $status -eq 0 ]
    [[ "$output" =~ "Insert 10000" ]] || false

    run dolt sql -q "SELECT COUNT(*) FROM dolt_log"
    [ $status -eq 0 ]
    [[ "$output" =~ "3" ]] || false

    run dolt status
    [[ "$output" =~ "All conflicts and constraint violations fixed but you are still merging" ]] || false
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ ([[:space:]]*modified:[[:space:]]*test) ]] || false

    run dolt sql -q "SELECT DOLT_COMMIT('-a', '-m', 'Finish up Merge')";
    [ $status -eq 0 ]

    run dolt status
    [ $status -eq 0 ]
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false

    run dolt log -n 1
    [ $status -eq 0 ]
    [[ "$output" =~ "Finish up Merge" ]] || false
}

@test "sql-merge: CALL DOLT_MERGE correctly merges branches with differing content in same table without conflicts" {
    dolt sql << SQL
CALL DOLT_COMMIT('-a', '-m', 'Step 1');
CALL DOLT_CHECKOUT('-b', 'feature-branch');
INSERT INTO test VALUES (3);
CALL DOLT_COMMIT('-a', '-m', 'Insert 3');
CALL DOLT_CHECKOUT('main');
INSERT INTO test VALUES (10000);
CALL DOLT_COMMIT('-a', '-m', 'Insert 10000');
SQL

    run dolt sql << SQL
CALL DOLT_MERGE('feature-branch');
SELECT COUNT(*) = 2 FROM test WHERE pk > 2;
SQL

    [ $status -eq 0 ]
    ! [[ "$output" =~ "Updating" ]] || false

    run dolt sql -q "SELECT * FROM test" -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ "pk" ]] || false
    [[ "$output" =~ "0" ]] || false
    [[ "$output" =~ "1" ]] || false
    [[ "$output" =~ "2" ]] || false
    [[ "$output" =~ "3" ]] || false
    [[ "$output" =~ "10000" ]] || false

    run dolt log -n 1
    [ $status -eq 0 ]
    [[ "$output" =~ "Insert 10000" ]] || false

    run dolt sql -q "SELECT COUNT(*) FROM dolt_log"
    [ $status -eq 0 ]
    [[ "$output" =~ "3" ]] || false

    run dolt status
    [[ "$output" =~ "All conflicts and constraint violations fixed but you are still merging" ]] || false
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ ([[:space:]]*modified:[[:space:]]*test) ]] || false

    run dolt sql -q "SELECT DOLT_COMMIT('-a', '-m', 'Finish up Merge')";
    [ $status -eq 0 ]

    run dolt status
    [ $status -eq 0 ]
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false

    run dolt log -n 1
    [ $status -eq 0 ]
    [[ "$output" =~ "Finish up Merge" ]] || false
}

@test "sql-merge: DOLT_MERGE works with no-ff" {
        run dolt sql << SQL
SELECT DOLT_COMMIT('-a', '-m', 'Step 1');
SELECT DOLT_CHECKOUT('-b', 'feature-branch');
INSERT INTO test VALUES (3);
SELECT DOLT_COMMIT('-a', '-m', 'update feature-branch');
SELECT DOLT_CHECKOUT('main');
SELECT DOLT_MERGE('feature-branch', '-no-ff', '-m', 'this is a no-ff');
SELECT COUNT(*) = 4 FROM dolt_log
SQL
    [ $status -eq 0 ]
    [[ "$output" =~ "true" ]] || false

    run dolt log -n 1
    [ $status -eq 0 ]
    [[ "$output" =~ "this is a no-ff" ]] || false
}

@test "sql-merge: CALL DOLT_MERGE works with no-ff" {
        run dolt sql << SQL
CALL DOLT_COMMIT('-a', '-m', 'Step 1');
CALL DOLT_CHECKOUT('-b', 'feature-branch');
INSERT INTO test VALUES (3);
CALL DOLT_COMMIT('-a', '-m', 'update feature-branch');
CALL DOLT_CHECKOUT('main');
CALL DOLT_MERGE('feature-branch', '-no-ff', '-m', 'this is a no-ff');
SELECT COUNT(*) = 4 FROM dolt_log
SQL
    [ $status -eq 0 ]
    [[ "$output" =~ "true" ]] || false

    run dolt log -n 1
    [ $status -eq 0 ]
    [[ "$output" =~ "this is a no-ff" ]] || false
}

@test "sql-merge: DOLT_MERGE -no-ff correctly changes head and working session variables." {
    dolt sql << SQL
SELECT DOLT_COMMIT('-a', '-m', 'Step 1');
SELECT DOLT_CHECKOUT('-b', 'feature-branch');
INSERT INTO test VALUES (3);
SELECT DOLT_COMMIT('-a', '-m', 'update feature-branch');
SELECT DOLT_CHECKOUT('main');
SQL
    head_variable=@@dolt_repo_$$_head
    head_hash=$(get_head_commit)
    working_variable=@@dolt_repo_$$_working
    working_hash=$(get_working_hash)

    run dolt sql -q "SELECT DOLT_MERGE('feature-branch', '-no-ff', '-m', 'this is a no-ff');"
    [ $status -eq 0 ]

    run dolt sql -q "SELECT $head_variable"
    [ $status -eq 0 ]
    [[ ! "$output" =~ $head_hash ]] || false

    run dolt sql -q "SELECT $working_variable"
    [ $status -eq 0 ]
    [[ ! "$output" =~ $working_hash ]] || false
}

@test "sql-merge: CALL DOLT_MERGE -no-ff correctly changes head and working session variables." {
    dolt sql << SQL
CALL DOLT_COMMIT('-a', '-m', 'Step 1');
CALL DOLT_CHECKOUT('-b', 'feature-branch');
INSERT INTO test VALUES (3);
CALL DOLT_COMMIT('-a', '-m', 'update feature-branch');
CALL DOLT_CHECKOUT('main');
SQL
    head_variable=@@dolt_repo_$$_head
    head_hash=$(get_head_commit)
    working_variable=@@dolt_repo_$$_working
    working_hash=$(get_working_hash)

    run dolt sql -q "CALL DOLT_MERGE('feature-branch', '-no-ff', '-m', 'this is a no-ff');"
    [ $status -eq 0 ]

    run dolt sql -q "SELECT $head_variable"
    [ $status -eq 0 ]
    [[ ! "$output" =~ $head_hash ]] || false

    run dolt sql -q "SELECT $working_variable"
    [ $status -eq 0 ]
    [[ ! "$output" =~ $working_hash ]] || false
}

@test "sql-merge: DOLT_MERGE detects merge conflicts, fails to commit and leaves working set clean when dolt_allow_commit_conflicts = 0" {
    # The dolt_merge fails here, and leaves the working set clean, no conflicts, no merge in progress
    run dolt sql << SQL
SET dolt_allow_commit_conflicts = 0;
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
SELECT DOLT_COMMIT('-a', '-m', 'changed feature branch');
SELECT DOLT_CHECKOUT('main');
SELECT DOLT_MERGE('feature-branch');
SQL
    [ $status -eq 1 ]
    [[ $output =~ "merge has unresolved conflicts" ]] || false

    run dolt status
    [ $status -eq 0 ]
    [[ $output =~ "working tree clean" ]] || false

    run dolt merge --abort
    [ $status -eq 1 ]
    [[ $output =~ "no merge to abort" ]] || false

    # make sure a clean SQL session doesn't have any merge going
    run dolt sql -q "SELECT DOLT_MERGE('--abort');"
    [ $status -eq 1 ]
    [[ $output =~ "no merge to abort" ]] || false

    run dolt sql -q "SELECT * FROM one_pk;" -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ "pk1,c1,c2" ]] || false
    [[ "$output" =~ "0,0,0" ]] || false
    [[ ! "$output" =~ "0,1,1" ]] || false

    dolt checkout feature-branch
    run dolt sql -q "SELECT * FROM one_pk;" -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ "pk1,c1,c2" ]] || false
    [[ ! "$output" =~ "0,0,0" ]] || false
    [[ "$output" =~ "0,1,1" ]] || false
}

@test "sql-merge: CALL DOLT_MERGE detects merge conflicts, fails to commit and leaves working set clean when dolt_allow_commit_conflicts = 0" {
    # The dolt_merge fails here, and leaves the working set clean, no conflicts, no merge in progress
    run dolt sql << SQL
SET dolt_allow_commit_conflicts = 0;
CREATE TABLE one_pk (
  pk1 BIGINT NOT NULL,
  c1 BIGINT,
  c2 BIGINT,
  PRIMARY KEY (pk1)
);
CALL DOLT_COMMIT('-a', '-m', 'add tables');
CALL DOLT_CHECKOUT('-b', 'feature-branch');
CALL DOLT_CHECKOUT('main');
INSERT INTO one_pk (pk1,c1,c2) VALUES (0,0,0);
CALL DOLT_COMMIT('-a', '-m', 'changed main');
CALL DOLT_CHECKOUT('feature-branch');
INSERT INTO one_pk (pk1,c1,c2) VALUES (0,1,1);
CALL DOLT_COMMIT('-a', '-m', 'changed feature branch');
CALL DOLT_CHECKOUT('main');
CALL DOLT_MERGE('feature-branch');
SQL
    [ $status -eq 1 ]
    [[ $output =~ "merge has unresolved conflicts" ]] || false

    run dolt status
    [ $status -eq 0 ]
    [[ $output =~ "working tree clean" ]] || false

    run dolt merge --abort
    [ $status -eq 1 ]
    [[ $output =~ "no merge to abort" ]] || false

    # make sure a clean SQL session doesn't have any merge going
    run dolt sql -q "CALL DOLT_MERGE('--abort');"
    [ $status -eq 1 ]
    [[ $output =~ "no merge to abort" ]] || false

    run dolt sql -q "SELECT * FROM one_pk;" -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ "pk1,c1,c2" ]] || false
    [[ "$output" =~ "0,0,0" ]] || false
    [[ ! "$output" =~ "0,1,1" ]] || false

    dolt checkout feature-branch
    run dolt sql -q "SELECT * FROM one_pk;" -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ "pk1,c1,c2" ]] || false
    [[ ! "$output" =~ "0,0,0" ]] || false
    [[ "$output" =~ "0,1,1" ]] || false
}

@test "sql-merge: DOLT_MERGE detects conflicts, returns them in dolt_conflicts table" {
    run dolt sql  << SQL
SET dolt_allow_commit_conflicts = 0;
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
SELECT DOLT_COMMIT('-a', '-m', 'changed feature branch');
SELECT DOLT_CHECKOUT('main');
SELECT DOLT_MERGE('feature-branch');
SQL
    [ $status -eq 1 ]
    [[ $output =~ "merge has unresolved conflicts" ]] || false

    # back on the command line, our session state is clean
    run dolt status
    [ $status -eq 0 ]
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "working tree clean" ]] || false
    [[ ! "$output" =~ "You have unmerged tables" ]] || false
    [[ ! "$output" =~ ([[:space:]]*both modified:[[:space:]]*one_pk) ]] || false

    # now merge, examine the conflicts, and abort
    run dolt sql -r csv  << SQL
SET autocommit = off;
SELECT DOLT_MERGE('feature-branch');
SELECT * FROM dolt_conflicts;
SELECT DOLT_MERGE('--abort');
SQL
    [ $status -eq 0 ]
    [[ "${lines[2]}" =~ "table,num_conflicts" ]] || false
    [[ "${lines[3]}" =~ "one_pk,1" ]] || false
    [[ "${lines[4]}" =~ "DOLT_MERGE('--abort')" ]] || false
    [[ "${lines[5]}" =~ "1" ]] || false

    # now resolve commits
    run dolt sql  << SQL
SET autocommit = off;
SELECT DOLT_MERGE('feature-branch');
REPLACE INTO one_pk (pk1, c1, c2) SELECT their_pk1, their_c1, their_c2 FROM dolt_conflicts_one_pk WHERE their_pk1 IS NOT NULL;
DELETE FROM one_pk WHERE pk1 in (SELECT base_pk1 FROM dolt_conflicts_one_pk WHERE their_pk1 IS NULL);
DELETE FROM dolt_conflicts_one_pk;
SELECT DOLT_COMMIT('-a', '-m', 'Finish Resolving');
SQL
    [ $status -eq 0 ]

    run dolt sql -q "SELECT * FROM one_pk" -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ "pk1,c1,c2" ]] || false
    [[ "$output" =~ "0,1,1" ]] || false

    run dolt sql -q "SELECT COUNT(*) from dolt_status;"
    [ $status -eq 0 ]
    [[ "$output" =~ "0" ]] || false
}

@test "sql-merge: DOLT_MERGE(--abort) clears session state and allows additional edits" {
    run dolt sql  << SQL
set autocommit = off;
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
SELECT DOLT_COMMIT('-a', '-m', 'changed feature branch');
SELECT DOLT_CHECKOUT('main');
SELECT DOLT_MERGE('feature-branch');
SELECT DOLT_MERGE('--abort');
insert into one_pk values (9,9,9);
commit;
SQL
    [ $status -eq 0 ]

    # We can see the latest inserted row back on the command line
    run dolt status
    [ $status -eq 0 ]
    [[ "$output" =~ ([[:space:]]*modified:[[:space:]]*one_pk) ]] || false

    run dolt diff
    [ $status -eq 0 ]
    [[ "$output" =~ "9" ]] || false

    run dolt sql -r csv -q "select * from one_pk where pk1 > 3";
    [ $status -eq 0 ]
    [[ "$output" =~ "9,9,9" ]] || false
}

@test "sql-merge: CALL DOLT_MERGE(--abort) clears session state and allows additional edits" {
    run dolt sql  << SQL
set autocommit = off;
CREATE TABLE one_pk (
  pk1 BIGINT NOT NULL,
  c1 BIGINT,
  c2 BIGINT,
  PRIMARY KEY (pk1)
);
CALL DOLT_COMMIT('-a', '-m', 'add tables');
CALL DOLT_CHECKOUT('-b', 'feature-branch');
CALL DOLT_CHECKOUT('main');
INSERT INTO one_pk (pk1,c1,c2) VALUES (0,0,0);
CALL DOLT_COMMIT('-a', '-m', 'changed main');
CALL DOLT_CHECKOUT('feature-branch');
INSERT INTO one_pk (pk1,c1,c2) VALUES (0,1,1);
CALL DOLT_COMMIT('-a', '-m', 'changed feature branch');
CALL DOLT_CHECKOUT('main');
CALL DOLT_MERGE('feature-branch');
CALL DOLT_MERGE('--abort');
insert into one_pk values (9,9,9);
commit;
SQL
    [ $status -eq 0 ]

    # We can see the latest inserted row back on the command line
    run dolt status
    [ $status -eq 0 ]
    [[ "$output" =~ ([[:space:]]*modified:[[:space:]]*one_pk) ]] || false

    run dolt diff
    [ $status -eq 0 ]
    [[ "$output" =~ "9" ]] || false

    run dolt sql -r csv -q "select * from one_pk where pk1 > 3";
    [ $status -eq 0 ]
    [[ "$output" =~ "9,9,9" ]] || false
}

@test "sql-merge: DOLT_MERGE(--abort) clears index state" {
    run dolt sql  << SQL
set autocommit = off;
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
SELECT DOLT_COMMIT('-a', '-m', 'changed feature branch');
SELECT DOLT_CHECKOUT('main');
SELECT DOLT_MERGE('feature-branch');
SELECT DOLT_MERGE('--abort');
commit;
SQL
    [ $status -eq 0 ]

    run dolt status
    [ "$status" -eq 0 ]
    [[ "${lines[0]}" =~ "On branch main" ]] || false
    [[ "${lines[1]}" =~ "nothing to commit, working tree clean" ]] || false
}

@test "sql-merge: CALL DOLT_MERGE(--abort) clears index state" {
    run dolt sql  << SQL
set autocommit = off;
CREATE TABLE one_pk (
  pk1 BIGINT NOT NULL,
  c1 BIGINT,
  c2 BIGINT,
  PRIMARY KEY (pk1)
);
CALL DOLT_COMMIT('-a', '-m', 'add tables');
CALL DOLT_CHECKOUT('-b', 'feature-branch');
CALL DOLT_CHECKOUT('main');
INSERT INTO one_pk (pk1,c1,c2) VALUES (0,0,0);
CALL DOLT_COMMIT('-a', '-m', 'changed main');
CALL DOLT_CHECKOUT('feature-branch');
INSERT INTO one_pk (pk1,c1,c2) VALUES (0,1,1);
CALL DOLT_COMMIT('-a', '-m', 'changed feature branch');
CALL DOLT_CHECKOUT('main');
CALL DOLT_MERGE('feature-branch');
CALL DOLT_MERGE('--abort');
commit;
SQL
    [ $status -eq 0 ]

    run dolt status
    [ "$status" -eq 0 ]
    [[ "${lines[0]}" =~ "On branch main" ]] || false
    [[ "${lines[1]}" =~ "nothing to commit, working tree clean" ]] || false
}

@test "sql-merge: DOLT_MERGE can commit unresolved conflicts with dolt_allow_commit_conflicts set" {
     dolt sql << SQL
set dolt_allow_commit_conflicts = on;
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
SELECT DOLT_COMMIT('-a', '-m', 'changed feature branch');
SELECT DOLT_CHECKOUT('main');
SELECT DOLT_MERGE('feature-branch');
SQL

    run dolt sql -r csv -q "SELECT count(*) from dolt_conflicts"
    [[ "$output" =~ "1" ]] || false

    run dolt sql -q "SELECT DOLT_MERGE('feature-branch');"
    [ $status -eq 1 ]
    [[ $output =~ "merge has unresolved conflicts" ]] || false
}

@test "sql-merge: CALL DOLT_MERGE can commit unresolved conflicts with dolt_allow_commit_conflicts on" {
     dolt sql << SQL
set dolt_allow_commit_conflicts = on;
CREATE TABLE one_pk (
  pk1 BIGINT NOT NULL,
  c1 BIGINT,
  c2 BIGINT,
  PRIMARY KEY (pk1)
);
CALL DOLT_COMMIT('-a', '-m', 'add tables');
CALL DOLT_CHECKOUT('-b', 'feature-branch');
CALL DOLT_CHECKOUT('main');
INSERT INTO one_pk (pk1,c1,c2) VALUES (0,0,0);
CALL DOLT_COMMIT('-a', '-m', 'changed main');
CALL DOLT_CHECKOUT('feature-branch');
INSERT INTO one_pk (pk1,c1,c2) VALUES (0,1,1);
CALL DOLT_COMMIT('-a', '-m', 'changed feature branch');
CALL DOLT_CHECKOUT('main');
CALL DOLT_MERGE('feature-branch');
SQL

    run dolt sql -r csv -q "SELECT count(*) from dolt_conflicts"
    [[ "$output" =~ "1" ]] || false

    run dolt sql -q "SELECT DOLT_MERGE('feature-branch');"
    [ $status -eq 1 ]
    [[ $output =~ "merge has unresolved conflicts" ]] || false
}

@test "sql-merge: DOLT_MERGE during an active merge throws an error" {
    run dolt sql << SQL
SELECT DOLT_COMMIT('-a', '-m', 'Step 1');
SELECT DOLT_CHECKOUT('-b', 'feature-branch');
INSERT INTO test VALUES (3);
SELECT DOLT_COMMIT('-a', '-m', 'Insert 3');
SELECT DOLT_CHECKOUT('main');
INSERT INTO test VALUES (500000);
SELECT DOLT_COMMIT('-a', '-m', 'Insert 500000');
SELECT DOLT_MERGE('feature-branch');
SELECT DOLT_MERGE('feature-branch');
SQL

    [ $status -eq 1 ]
    [[ $output =~ "merging is not possible because you have not committed an active merge" ]] || false
}

@test "sql-merge: CALL DOLT_MERGE during an active merge throws an error" {
    run dolt sql << SQL
CALL DOLT_COMMIT('-a', '-m', 'Step 1');
CALL DOLT_CHECKOUT('-b', 'feature-branch');
INSERT INTO test VALUES (3);
CALL DOLT_COMMIT('-a', '-m', 'Insert 3');
CALL DOLT_CHECKOUT('main');
INSERT INTO test VALUES (500000);
CALL DOLT_COMMIT('-a', '-m', 'Insert 500000');
CALL DOLT_MERGE('feature-branch');
CALL DOLT_MERGE('feature-branch');
SQL

    [ $status -eq 1 ]
    [[ $output =~ "merging is not possible because you have not committed an active merge" ]] || false
}

@test "sql-merge: DOLT_MERGE works with ff and squash" {
    run dolt sql << SQL
SELECT DOLT_COMMIT('-a', '-m', 'Step 1');
SELECT DOLT_CHECKOUT('-b', 'feature-branch');
INSERT INTO test VALUES (3);
SELECT DOLT_COMMIT('-a', '-m', 'this is a ff');
SELECT DOLT_CHECKOUT('main');
SELECT DOLT_MERGE('feature-branch', '--squash');
SELECT COUNT(*) > 0 FROM test WHERE pk=3;
SQL
    [ $status -eq 0 ]
    [[ "$output" =~ "true" ]] || false

    run dolt log -n 1
    [ $status -eq 0 ]
    [[ "$output" =~ "Step 1" ]] || false

    run dolt sql -q "SELECT COUNT(*) FROM dolt_log"
    [ $status -eq 0 ]
    [[ "$output" =~ "2" ]] || false

    run dolt status
    [ $status -eq 0 ]
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ ([[:space:]]*modified:[[:space:]]*test) ]] || false

    run dolt sql -q "SELECT DOLT_COMMIT('-a', '-m', 'hi');"
    [ $status -eq 0 ]

    run dolt status
    [ $status -eq 0 ]
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false
}

@test "sql-merge: CALL DOLT_MERGE works with ff and squash" {
    run dolt sql << SQL
CALL DOLT_COMMIT('-a', '-m', 'Step 1');
CALL DOLT_CHECKOUT('-b', 'feature-branch');
INSERT INTO test VALUES (3);
CALL DOLT_COMMIT('-a', '-m', 'this is a ff');
CALL DOLT_CHECKOUT('main');
CALL DOLT_MERGE('feature-branch', '--squash');
SELECT COUNT(*) > 0 FROM test WHERE pk=3;
SQL
    [ $status -eq 0 ]
    [[ "$output" =~ "true" ]] || false

    run dolt log -n 1
    [ $status -eq 0 ]
    [[ "$output" =~ "Step 1" ]] || false

    run dolt sql -q "SELECT COUNT(*) FROM dolt_log"
    [ $status -eq 0 ]
    [[ "$output" =~ "2" ]] || false

    run dolt status
    [ $status -eq 0 ]
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ ([[:space:]]*modified:[[:space:]]*test) ]] || false

    run dolt sql -q "CALL DOLT_COMMIT('-a', '-m', 'hi');"
    [ $status -eq 0 ]

    run dolt status
    [ $status -eq 0 ]
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false
}

@test "sql-merge: DOLT_MERGE with no-ff and squash works." {
    dolt sql << SQL
SELECT DOLT_COMMIT('-a', '-m', 'Step 1');
SELECT DOLT_CHECKOUT('-b', 'feature-branch');
INSERT INTO test VALUES (3);
SELECT DOLT_COMMIT('-a', '-m', 'Insert 3');
SELECT DOLT_CHECKOUT('main');
INSERT INTO test VALUES (500000);
SELECT DOLT_COMMIT('-a', '-m', 'Insert 500000');
SELECT DOLT_MERGE('feature-branch', '--squash');
SQL

    run dolt status
    [ $status -eq 0 ]
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ ([[:space:]]*modified:[[:space:]]*test) ]] || false

    run dolt sql -q "SELECT DOLT_COMMIT('-a', '-m', 'Finish up Merge')";
    [ $status -eq 0 ]

    run dolt status
    [ $status -eq 0 ]
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false

    run dolt log -n 1
    [ $status -eq 0 ]
    [[ "$output" =~ "Finish up Merge" ]] || false
}

@test "sql-merge: CALL DOLT_MERGE with no-ff and squash works." {
    dolt sql << SQL
CALL DOLT_COMMIT('-a', '-m', 'Step 1');
CALL DOLT_CHECKOUT('-b', 'feature-branch');
INSERT INTO test VALUES (3);
CALL DOLT_COMMIT('-a', '-m', 'Insert 3');
CALL DOLT_CHECKOUT('main');
INSERT INTO test VALUES (500000);
CALL DOLT_COMMIT('-a', '-m', 'Insert 500000');
CALL DOLT_MERGE('feature-branch', '--squash');
SQL

    run dolt status
    [ $status -eq 0 ]
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ ([[:space:]]*modified:[[:space:]]*test) ]] || false

    run dolt sql -q "CALL DOLT_COMMIT('-a', '-m', 'Finish up Merge')";
    [ $status -eq 0 ]

    run dolt status
    [ $status -eq 0 ]
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false

    run dolt log -n 1
    [ $status -eq 0 ]
    [[ "$output" =~ "Finish up Merge" ]] || false
}

@test "sql-merge: DOLT_MERGE throws errors with working set changes." {
    run dolt sql << SQL
SELECT DOLT_COMMIT('-a', '-m', 'Step 1');
SELECT DOLT_CHECKOUT('-b', 'feature-branch');
INSERT INTO test VALUES (3);
SELECT DOLT_COMMIT('-a', '-m', 'this is a ff');
SELECT DOLT_CHECKOUT('main');
CREATE TABLE tbl (
    pk int primary key
);
SELECT DOLT_MERGE('feature-branch');
SQL
    [ $status -eq 1 ]
    [[ "$output" =~ "cannot merge with uncommitted changes" ]] || false
}

@test "sql-merge: CALL DOLT_MERGE throws errors with working set changes." {
    run dolt sql << SQL
CALL DOLT_COMMIT('-a', '-m', 'Step 1');
CALL DOLT_CHECKOUT('-b', 'feature-branch');
INSERT INTO test VALUES (3);
CALL DOLT_COMMIT('-a', '-m', 'this is a ff');
CALL DOLT_CHECKOUT('main');
CREATE TABLE tbl (
    pk int primary key
);
CALL DOLT_MERGE('feature-branch');
SQL
    [ $status -eq 1 ]
    [[ "$output" =~ "cannot merge with uncommitted changes" ]] || false
}

@test "sql-merge: DOLT_MERGE with a long series of changing operations works." {
    dolt sql << SQL
SELECT DOLT_COMMIT('-a', '-m', 'Step 1');
SELECT DOLT_CHECKOUT('-b', 'feature-branch');
INSERT INTO test VALUES (3);
INSERT INTO test VALUES (4);
INSERT INTO test VALUES (21232);
DELETE FROM test WHERE pk=4;
UPDATE test SET pk=21 WHERE pk=21232;
SELECT DOLT_COMMIT('-a', '-m', 'Insert 3');
SELECT DOLT_CHECKOUT('main');
INSERT INTO test VALUES (500000);
INSERT INTO test VALUES (500001);
DELETE FROM test WHERE pk=500001;
UPDATE test SET pk=60 WHERE pk=500000;
SELECT DOLT_COMMIT('-a', '-m', 'Insert 60');
SELECT DOLT_MERGE('feature-branch');
SQL


    run dolt status
    [ $status -eq 0 ]
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ ([[:space:]]*modified:[[:space:]]*test) ]] || false

    run dolt sql -q "SELECT DOLT_COMMIT('-a', '-m', 'Finish up Merge')";
    [ $status -eq 0 ]

    run dolt status
    [ $status -eq 0 ]
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false

    run dolt log -n 1
    [ $status -eq 0 ]
    [[ "$output" =~ "Finish up Merge" ]] || false

    run dolt sql -q "SELECT * FROM test;" -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ "pk" ]] || false
    [[ "$output" =~ "0" ]] || false
    [[ "$output" =~ "1" ]] || false
    [[ "$output" =~ "2" ]] || false
    [[ "$output" =~ "3" ]] || false
    [[ "$output" =~ "21" ]] || false
    [[ "$output" =~ "60" ]] || false

    run dolt sql -q "SELECT COUNT(*) FROM test;" -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ "6" ]] || false
}

@test "sql-merge: CALL DOLT_MERGE with a long series of changing operations works." {
    dolt sql << SQL
CALL DOLT_COMMIT('-a', '-m', 'Step 1');
CALL DOLT_CHECKOUT('-b', 'feature-branch');
INSERT INTO test VALUES (3);
INSERT INTO test VALUES (4);
INSERT INTO test VALUES (21232);
DELETE FROM test WHERE pk=4;
UPDATE test SET pk=21 WHERE pk=21232;
CALL DOLT_COMMIT('-a', '-m', 'Insert 3');
CALL DOLT_CHECKOUT('main');
INSERT INTO test VALUES (500000);
INSERT INTO test VALUES (500001);
DELETE FROM test WHERE pk=500001;
UPDATE test SET pk=60 WHERE pk=500000;
CALL DOLT_COMMIT('-a', '-m', 'Insert 60');
CALL DOLT_MERGE('feature-branch');
SQL


    run dolt status
    [ $status -eq 0 ]
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ ([[:space:]]*modified:[[:space:]]*test) ]] || false

    run dolt sql -q "CALL DOLT_COMMIT('-a', '-m', 'Finish up Merge')";
    [ $status -eq 0 ]

    run dolt status
    [ $status -eq 0 ]
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false

    run dolt log -n 1
    [ $status -eq 0 ]
    [[ "$output" =~ "Finish up Merge" ]] || false

    run dolt sql -q "SELECT * FROM test;" -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ "pk" ]] || false
    [[ "$output" =~ "0" ]] || false
    [[ "$output" =~ "1" ]] || false
    [[ "$output" =~ "2" ]] || false
    [[ "$output" =~ "3" ]] || false
    [[ "$output" =~ "21" ]] || false
    [[ "$output" =~ "60" ]] || false

    run dolt sql -q "SELECT COUNT(*) FROM test;" -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ "6" ]] || false
}

@test "sql-merge: DOLT_MERGE with conflicts renders the dolt_conflicts table" {
    run dolt sql  --continue << SQL
set autocommit = off;
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
SELECT DOLT_COMMIT('-a', '-m', 'changed feature branch');
SELECT DOLT_CHECKOUT('main');
SELECT DOLT_MERGE('feature-branch');
SHOW WARNINGS;
SELECT COUNT(*) FROM dolt_conflicts where num_conflicts > 0;
rollback;
SQL
    [ $status -eq 0 ]
    [[ "$output" =~ "| DOLT_MERGE('feature-branch') |" ]] || false
    [[ "$output" =~ "| 0                            |" ]] || false # conflict should return 0
    [[ "$output" =~ "| Warning | 1105 | merge has unresolved conflicts. please use the dolt_conflicts table to resolve |" ]] || false
    [[ "$output" =~ "| COUNT(*) |" ]] || false
    [[ "$output" =~ "| 1        |" ]] || false
}

@test "sql-merge: DOLT_MERGE with conflicts is queryable when autocommit is on" {
    skip "This needs to work"
    run dolt sql  --continue << SQL
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
SELECT DOLT_COMMIT('-a', '-m', 'changed feature branch');
SELECT DOLT_CHECKOUT('main');
SELECT DOLT_MERGE('feature-branch');
SHOW WARNINGS;
SELECT COUNT(*) FROM dolt_conflicts where num_conflicts > 0;
rollback;
SQL
    [ $status -eq 0 ]
    [[ "$output" =~ "| DOLT_MERGE('feature-branch') |" ]] || false
    [[ "$output" =~ "| 0                            |" ]] || false # conflict should return 0
    [[ "$output" =~ "| Warning | 1105 | merge has unresolved conflicts. please use the dolt_conflicts table to resolve |" ]] || false
    [[ "$output" =~ "| COUNT(*) |" ]] || false
    [[ "$output" =~ "| 1        |" ]] || false
}

@test "sql-merge: up-to-date branch does not error" {
    dolt commit -am "commit all changes"
    run dolt sql << SQL
SELECT DOLT_CHECKOUT('-b', 'feature-branch');
SELECT DOLT_CHECKOUT('main');
INSERT INTO test VALUES (3);
SELECT DOLT_COMMIT('-a', '-m', 'a commit');
SELECT DOLT_MERGE('feature-branch');
SHOW WARNINGS;
SQL
   [ $status -eq 0 ]
   [[ "$output" =~ "current fast forward from a to b. a is ahead of b already" ]] || false
}

@test "sql-merge: up-to-date branch does not error on CALL" {
    dolt commit -am "commit all changes"
    run dolt sql << SQL
CALL DOLT_CHECKOUT('-b', 'feature-branch');
CALL DOLT_CHECKOUT('main');
INSERT INTO test VALUES (3);
CALL DOLT_COMMIT('-a', '-m', 'a commit');
CALL DOLT_MERGE('feature-branch');
SHOW WARNINGS;
SQL
   [ $status -eq 0 ]
   [[ "$output" =~ "current fast forward from a to b. a is ahead of b already" ]] || false
}

@test "sql-merge: adding and dropping primary keys any number of times not produce schema merge conflicts" {
    dolt commit -am "commit all changes"
    dolt sql -q "create table test_null (i int)"
    dolt commit -am "initial"

    dolt checkout -b b1
    dolt sql -q "alter table test_null add primary key(i)"
    dolt sql -q "alter table test_null drop primary key"
    dolt sql -q "alter table test_null add primary key(i)"
    dolt sql -q "alter table test_null drop primary key"
    dolt sql -q "alter table test_null add primary key(i)"
    dolt commit -am "b1 primary key changes"

    dolt checkout main
    dolt sql -q "alter table test_null add primary key(i)"
    dolt commit -am "main primary key changes"

    run dolt merge b1
    [ $status -eq 0 ]
}

@test "sql-merge: identical schema changes with data changes merges correctly" {
    dolt sql -q "create table t (i int primary key)"
    dolt commit -am "initial commit"
    dolt branch b1
    dolt branch b2
    dolt checkout b1
    dolt sql -q "alter table t add column j int"
    dolt sql -q "insert into t values (1, 1)"
    dolt commit -am "changes to b1"
    dolt checkout b2
    dolt sql -q "alter table t add column j int"
    dolt sql -q "insert into t values (2, 2)"
    dolt commit -am "changes to b2"
    dolt checkout main
    run dolt merge b1
    [ $status -eq 0 ]
    run dolt merge b2
    [ $status -eq 0 ]
}

# TODO: what happens when the data conflicts with new check?
@test "sql-merge: non-conflicting data and constraint changes are preserved" {
    dolt sql -q "create table t (i int)"
    dolt commit -am "initial commit"

    dolt checkout -b other
    dolt sql -q "insert into t values (1)"
    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ "$output" =~ "1" ]] || false
    dolt commit -am "changes to other"

    dolt checkout main
    dolt sql -q "alter table t add check (i < 10)"
    run dolt sql -q "show create table t"
    [ $status -eq 0 ]
    [[ "$output" =~ "(\`i\` < 10)" ]] || false
    dolt commit -am "changes to main"

    run dolt merge other
    [ $status -eq 0 ]

    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ "$output" =~ "1" ]] || false
    run dolt sql -q "show create table t"
    [ $status -eq 0 ]
    [[ "$output" =~ "(\`i\` < 10)" ]] || false
}

@test "sql-merge: non-overlapping check constraints merge successfully" {
    dolt sql -q "create table t (i int, j int)"
    dolt commit -am "initial commit"

    dolt checkout -b other
    dolt sql -q "alter table t add constraint c0 check (i > 0)"
    run dolt sql -q "show create table t"
    [ $status -eq 0 ]
    [[ "$output" =~ "CONSTRAINT \`c0\` CHECK ((\`i\` > 0))" ]] || false
    dolt commit -am "changes to other"

    dolt checkout main
    dolt sql -q "alter table t add constraint c1 check (j < 0)"
    run dolt sql -q "show create table t"
    [ $status -eq 0 ]
    [[ "$output" =~ "CONSTRAINT \`c1\` CHECK ((\`j\` < 0))" ]] || false
    dolt commit -am "changes to main"

    run dolt merge other
    [ $status -eq 0 ]

    run dolt sql -q "show create table t"
    [ $status -eq 0 ]
    [[ "$output" =~ "CONSTRAINT \`c0\` CHECK ((\`i\` > 0))" ]] || false
    [[ "$output" =~ "CONSTRAINT \`c1\` CHECK ((\`j\` < 0))" ]] || false
}

@test "sql-merge: different check constraints on same column throw conflict" {
    dolt sql -q "create table t (i int)"
    dolt commit -am "initial commit"

    dolt checkout -b other
    dolt sql -q "alter table t add constraint c0 check (i > 0)"
    run dolt sql -q "show create table t"
    [ $status -eq 0 ]
    [[ "$output" =~ "CONSTRAINT \`c0\` CHECK ((\`i\` > 0))" ]] || false
    dolt commit -am "changes to other"

    dolt checkout main
    dolt sql -q "alter table t add constraint c1 check (i < 0)"
    run dolt sql -q "show create table t"
    [ $status -eq 0 ]
    [[ "$output" =~ "CONSTRAINT \`c1\` CHECK ((\`i\` < 0))" ]] || false
    dolt commit -am "changes to main"

    run dolt merge other
    [ $status -eq 1 ]
    [[ "$output" =~ "our check 'c1' and their check 'c0' both reference the same column(s)" ]] || false
}

# TODO: what happens when the new data conflicts with modified check?
@test "sql-merge: non-conflicting constraint modification is preserved" {
    dolt sql -q "create table t (i int)"
    dolt sql -q "alter table t add constraint c check (i > 0)"
    dolt commit -am "initial commit"

    dolt checkout -b other
    dolt sql -q "insert into t values (1)"
    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ "$output" =~ "1" ]] || false
    run dolt sql -q "show create table t"
    [ $status -eq 0 ]
    [[ "$output" =~ "CONSTRAINT \`c\` CHECK ((\`i\` > 0))" ]] || false
    dolt commit -am "changes to other"

    dolt checkout main
    dolt sql -q "alter table t drop constraint c"
    dolt sql -q "alter table t add constraint c check (i < 10)"
    run dolt sql -q "show create table t"
    [ $status -eq 0 ]
    [[ "$output" =~ "CONSTRAINT \`c\` CHECK ((\`i\` < 10))" ]] || false
    dolt commit -am "changes to main"

    run dolt merge other
    [ $status -eq 0 ]

    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ "$output" =~ "1" ]] || false
    run dolt sql -q "show create table t"
    [ $status -eq 0 ]
    [[ "$output" =~ "CONSTRAINT \`c\` CHECK ((\`i\` < 10))" ]] || false
}

# TODO: expected behavior for dropping constraints?
@test "sql-merge: dropping constraint in one branch drops from both" {
    dolt sql -q "create table t (i int)"
    dolt sql -q "alter table t add constraint c check (i > 0)"
    dolt commit -am "initial commit"

    dolt checkout -b other
    dolt sql -q "insert into t values (1)"
    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ "$output" =~ "1" ]] || false
    run dolt sql -q "show create table t"
    [ $status -eq 0 ]
    [[ "$output" =~ "CONSTRAINT \`c\` CHECK ((\`i\` > 0))" ]] || false
    dolt commit -am "changes to other"

    dolt checkout main
    dolt sql -q "alter table t drop constraint c"
    run dolt sql -q "show create table t"
    [ $status -eq 0 ]
    [[ !("$output" =~ "CONSTRAINT \`c\` CHECK ((\`i\` > 0))") ]] || false
    dolt commit -am "changes to main"

    run dolt merge other
    [ $status -eq 0 ]

    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ "$output" =~ "1" ]] || false
    run dolt sql -q "show create table t"
    [ $status -eq 0 ]
    [[ !("$output" =~ "CONSTRAINT \`c\` CHECK ((\`i\` > 0))") ]] || false
}

@test "sql-merge: dropping constraint on both branches merges successfully" {
    dolt sql -q "create table t (i int)"
    dolt sql -q "alter table t add constraint c check (i > 0)"
    dolt commit -am "initial commit"

    dolt checkout -b other
    dolt sql -q "alter table t drop constraint c"
    run dolt sql -q "show create table t"
    [ $status -eq 0 ]
    [[ !("$output" =~ "CONSTRAINT \`c\` CHECK ((\`i\` > 0))") ]] || false
    dolt commit -am "changes to other"

    dolt checkout main
    dolt sql -q "alter table t drop constraint c"
    run dolt sql -q "show create table t"
    [ $status -eq 0 ]
    [[ !("$output" =~ "CONSTRAINT \`c\` CHECK ((\`i\` > 0))") ]] || false
    dolt commit -am "changes to main"

    run dolt merge other
    [ $status -eq 0 ]

    run dolt sql -q "show create table t"
    [ $status -eq 0 ]
    [[ !("$output" =~ "CONSTRAINT \`c\` CHECK ((\`i\` < 10))") ]] || false
}

@test "sql-merge: dropping constraint in one branch and modifying same in other results in conflict" {
    dolt sql -q "create table t (i int)"
    dolt sql -q "alter table t add constraint c check (i > 0)"
    dolt commit -am "initial commit"

    dolt checkout -b other
    dolt sql -q "alter table t drop constraint c"
    run dolt sql -q "show create table t"
    [ $status -eq 0 ]
    [[ !("$output" =~ "CONSTRAINT \`c\` CHECK ((\`i\` > 0))") ]] || false
    dolt commit -am "changes to other"

    dolt checkout main
    dolt sql -q "alter table t drop constraint c"
    dolt sql -q "alter table t add constraint c check (i < 10)"
    run dolt sql -q "show create table t"
    [ $status -eq 0 ]
    [[ "$output" =~ "CONSTRAINT \`c\` CHECK ((\`i\` < 10))" ]] || false
    dolt commit -am "changes to main"

    run dolt merge other
    [ $status -eq 1 ]
    [[ "$output" =~ "check 'c' was deleted in theirs but modified in ours" ]] || false
}

@test "sql-merge: merging with not null and check constraints preserves both constraints" {
    dolt sql -q "create table t (i int)"
    dolt commit -am "initial commit"

    dolt branch b1
    dolt branch b2

    dolt checkout b1
    dolt sql -q "alter table t add check (i > 0)"
    run dolt sql -q "show create table t"
    [ $status -eq 0 ]
    [[ "$output" =~ "(\`i\` > 0)" ]] || false
    dolt commit -am "changes to b1"

    dolt checkout b2
    dolt sql -q "alter table t modify i int not null"
    run dolt sql -q "show create table t"
    [ $status -eq 0 ]
    [[ "$output" =~ "\`i\` int NOT NULL" ]] || false
    dolt commit -am "changes to b2"

    dolt checkout main
    run dolt merge b1
    [ $status -eq 0 ]
    run dolt merge b2
    [ $status -eq 0 ]

    run dolt sql -q "show create table t"
    [ $status -eq 0 ]
    [[ "$output" =~ "\`i\` int NOT NULL" ]] || false
    [[ "$output" =~ "(\`i\` > 0)" ]] || false
}

@test "sql-merge: check constraint with name collision" {
    dolt sql -q "create table t (i int)"
    dolt commit -am "initial commit"

    dolt branch b1
    dolt branch b2

    dolt checkout b1
    dolt sql -q "alter table t add constraint c check (i > 0)"
    run dolt sql -q "show create table t"
    [ $status -eq 0 ]
    [[ "$output" =~ "CONSTRAINT \`c\` CHECK ((\`i\` > 0))" ]] || false
    dolt commit -am "changes to b1"

    dolt checkout b2
    dolt sql -q "alter table t add constraint c check (i < 10)"
    run dolt sql -q "show create table t"
    [ $status -eq 0 ]
    [[ "$output" =~ "CONSTRAINT \`c\` CHECK ((\`i\` < 10))" ]] || false
    dolt commit -am "changes to b2"

    dolt checkout main
    run dolt merge b1
    [ $status -eq 0 ]
    run dolt merge b2
    [ $status -eq 1 ]
    [[ "$output" =~ "two checks with the name 'c' but different definitions" ]] || false
}

@test "sql-merge: check constraint for deleted column in another table" {
    dolt sql -q "create table t (i int primary key, j int)"
    dolt commit -am "initial commit"

    dolt branch b1
    dolt branch b2

    dolt checkout b1
    dolt sql -q "alter table t add constraint c check (j > 0)"
    run dolt sql -q "show create table t"
    [ $status -eq 0 ]
    [[ "$output" =~ "CONSTRAINT \`c\` CHECK ((\`j\` > 0))" ]] || false
    dolt commit -am "changes to b1"

    dolt checkout b2
    dolt sql -q "alter table t drop column j"
    dolt commit -am "changes to b2"

    dolt checkout main
    run dolt merge b1
    [ $status -eq 0 ]
    run dolt merge b2
    [ $status -eq 1 ]
    [[ "$output" =~ "check 'c' references a column that will be deleted after merge" ]] || false
}

get_head_commit() {
    dolt log -n 1 | grep -m 1 commit | cut -c 13-44
}

get_working_hash() {
  dolt sql -q "select @@dolt_repo_$$_working" | sed -n 4p | sed -e 's/|//' -e 's/|//'  -e 's/ //'
}
