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
dolt add .
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "sql-merge: DOLT_MERGE with no-ff displays hash." {
    dolt add .
    dolt commit -m "dummy commit"
    oldHead=$(dolt sql -r csv -q "select hashof('HEAD')" | sed -n '2 p')
    mergeHead=$(dolt sql -r csv -q "call dolt_merge('--no-ff', 'main')" | sed -n '2 p' | head -c 32)
    newHead=$(dolt sql -r csv -q "select hashof('HEAD')" | sed -n '2 p')
    echo $mergeHead
    echo $newHead
    [ ! "$mergeHead" = "$oldHead" ]
    [ "$mergeHead" = "$newHead" ]
}

@test "sql-merge: DOLT_MERGE with unknown branch name throws an error" {
    dolt sql -q "call dolt_commit('-a', '-m', 'Step 1');"

    run dolt sql -q "call dolt_merge('feature-branch');"
    log_status_eq 1
}

@test "sql-merge: CALL DOLT_MERGE with unknown branch name throws an error" {
    dolt sql -q "CALL DOLT_COMMIT('-a', '-m', 'Step 1');"

    run dolt sql -q "CALL DOLT_MERGE('feature-branch');"
    log_status_eq 1
}

@test "sql-merge: DOLT_MERGE works with ff" {
    dolt sql <<SQL
call dolt_commit('-a', '-m', 'Step 1');
call dolt_checkout('-b', 'feature-branch');
INSERT INTO test VALUES (3);
UPDATE test SET pk=1000 WHERE pk=0;
call dolt_commit('-a', '-m', 'this is a ff');
call dolt_checkout('main');
SQL
    run dolt sql -q "call dolt_merge('feature-branch');"
    log_status_eq 0

    run dolt log -n 1
    log_status_eq 0
    [[ "$output" =~ "this is a ff" ]] || false

    run dolt sql -q "SELECT COUNT(*) FROM dolt_log"
    log_status_eq 0
    [[ "$output" =~ "3" ]] || false

    run dolt status
    log_status_eq 0
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false

    run dolt sql -q "SELECT * FROM test;" -r csv
    log_status_eq 0
    [[ "$output" =~ "pk" ]] || false
    [[ "$output" =~ "1" ]] || false
    [[ "$output" =~ "2" ]] || false
    [[ "$output" =~ "3" ]] || false
    [[ "$output" =~ "1000" ]] || false

    run dolt sql -q "SELECT COUNT(*) FROM test;" -r csv
    log_status_eq 0
    [[ "$output" =~ "4" ]] || false
}

@test "sql-merge: DOLT_MERGE works in the session for fastforward." {
     run dolt sql << SQL
call dolt_commit('-a', '-m', 'Step 1');
call dolt_checkout('-b', 'feature-branch');
INSERT INTO test VALUES (3);
call dolt_commit('-a', '-m', 'this is a ff');
call dolt_checkout('main');
call dolt_merge('feature-branch');
SELECT COUNT(*) > 0 FROM test WHERE pk=3;
SQL
    log_status_eq 0
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
call dolt_commit('-a', '-m', 'Step 1');
call dolt_checkout('-b', 'feature-branch');
INSERT INTO test VALUES (3);
call dolt_commit('-a', '-m', 'this is a ff');
call dolt_checkout('main');
call dolt_merge('feature-branch');
call dolt_checkout('-b', 'new-branch');
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
call dolt_commit('-a', '-m', 'Step 1');
call dolt_checkout('-b', 'feature-branch');
INSERT INTO test VALUES (3);
call dolt_commit('-a', '-m', 'this is a ff');
call dolt_checkout('main');
call dolt_merge('feature-branch', '-no-ff');
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
CALL DOLT_ADD('.');
call dolt_commit('-a', '-m', 'Step 1');
call dolt_checkout('-b', 'feature-branch');
INSERT INTO test2 VALUES (1, 1);
UPDATE test2 SET val=1000 WHERE pk=0;
call dolt_commit('-a', '-m', 'this is a normal commit');
call dolt_checkout('main');
UPDATE test2 SET val=1001 WHERE pk=0;
call dolt_commit('-a', '-m', 'update a value');
call dolt_merge('feature-branch', '-m', 'this is a merge');
DELETE FROM dolt_conflicts_test2;
call dolt_commit('-a', '-m', 'remove conflicts');
SQL

    run dolt sql -r csv -q "select * from test2 order by pk"
    [ "${#lines[@]}" -eq 3 ]
    [ "${lines[1]}" = "0,1001" ]
    [ "${lines[2]}" = "1,1" ]
}

@test "sql-merge: DOLT_MERGE works with autocommit off." {
    dolt sql << SQL
set autocommit = off;
call dolt_commit('-a', '-m', 'Step 1');
call dolt_checkout('-b', 'feature-branch');
INSERT INTO test VALUES (3);
call dolt_commit('-a', '-m', 'this is a normal commit');
call dolt_checkout('main');
INSERT INTO test VALUES (5);
call dolt_commit('-a', '-m', 'this is a normal commit');
call dolt_merge('feature-branch');
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
call dolt_commit('-a', '-m', 'Step 1');
call dolt_checkout('-b', 'feature-branch');
INSERT INTO test VALUES (3);
call dolt_commit('-a', '-m', 'this is a ff');
SQL
    head_variable=@@dolt_repo_$$_head

    dolt checkout feature-branch
    head_hash=$(get_head_commit)

    dolt sql << SQL
call dolt_checkout('main');
call dolt_merge('feature-branch');
SQL

    run dolt sql -q "SELECT $head_variable"
    log_status_eq 0
    [[ "$output" =~ $head_hash ]] || false

    dolt checkout main
    run dolt sql -q "SELECT $head_variable"
    log_status_eq 0
    [[ "$output" =~ $head_hash ]] || false
}

@test "sql-merge: DOLT_MERGE correctly merges branches with differing content in same table without conflicts" {
    dolt sql << SQL
call dolt_commit('-a', '-m', 'Step 1');
call dolt_checkout('-b', 'feature-branch');
INSERT INTO test VALUES (3);
call dolt_commit('-a', '-m', 'Insert 3');
call dolt_checkout('main');
INSERT INTO test VALUES (10000);
call dolt_commit('-a', '-m', 'Insert 10000');
SQL

    run dolt sql << SQL
call dolt_merge('feature-branch', '--no-commit');
SELECT COUNT(*) = 2 FROM test WHERE pk > 2;
SQL

    log_status_eq 0
    [[ "$output" =~ "true" ]] || false
    [[ "$output" =~ "true" ]] || false
    [[ "${lines[3]}" =~ "0" ]] || false
    ! [[ "$output" =~ "Updating" ]] || false

    run dolt sql -q "SELECT * FROM test" -r csv
    log_status_eq 0
    [[ "$output" =~ "pk" ]] || false
    [[ "$output" =~ "0" ]] || false
    [[ "$output" =~ "1" ]] || false
    [[ "$output" =~ "2" ]] || false
    [[ "$output" =~ "3" ]] || false
    [[ "$output" =~ "10000" ]] || false

    run dolt log -n 1
    log_status_eq 0
    [[ "$output" =~ "Insert 10000" ]] || false

    run dolt sql -q "SELECT COUNT(*) FROM dolt_log"
    log_status_eq 0
    [[ "$output" =~ "3" ]] || false

    run dolt status
    [[ "$output" =~ "All conflicts and constraint violations fixed but you are still merging" ]] || false
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ ([[:space:]]*modified:[[:space:]]*test) ]] || false

    run dolt sql -q "call dolt_commit('-a', '-m', 'Finish up Merge')";
    log_status_eq 0

    run dolt status
    log_status_eq 0
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false

    run dolt log -n 1
    log_status_eq 0
    [[ "$output" =~ "Finish up Merge" ]] || false
}

@test "sql-merge: DOLT_MERGE works with no-ff" {
        run dolt sql << SQL
call dolt_commit('-a', '-m', 'Step 1');
call dolt_checkout('-b', 'feature-branch');
INSERT INTO test VALUES (3);
call dolt_commit('-a', '-m', 'update feature-branch');
call dolt_checkout('main');
call dolt_merge('feature-branch', '-no-ff', '-m', 'this is a no-ff');
SELECT COUNT(*) = 4 FROM dolt_log
SQL
    log_status_eq 0
    [[ "$output" =~ "true" ]] || false

    run dolt log -n 1
    log_status_eq 0
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
    log_status_eq 0
    [[ "$output" =~ "true" ]] || false

    run dolt log -n 1
    log_status_eq 0
    [[ "$output" =~ "this is a no-ff" ]] || false
}

@test "sql-merge: DOLT_MERGE -no-ff correctly changes head and working session variables." {
    dolt sql << SQL
call dolt_commit('-a', '-m', 'Step 1');
call dolt_checkout('-b', 'feature-branch');
INSERT INTO test VALUES (3);
call dolt_commit('-a', '-m', 'update feature-branch');
call dolt_checkout('main');
SQL
    head_variable=@@dolt_repo_$$_head
    head_hash=$(get_head_commit)
    working_variable=@@dolt_repo_$$_working
    working_hash=$(get_working_hash)

    run dolt sql -q "call dolt_merge('feature-branch', '-no-ff', '-m', 'this is a no-ff');"
    log_status_eq 0

    run dolt sql -q "SELECT $head_variable"
    log_status_eq 0
    [[ ! "$output" =~ $head_hash ]] || false

    run dolt sql -q "SELECT $working_variable"
    log_status_eq 0
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
CALL DOLT_ADD('.');
call dolt_commit('-a', '-m', 'add tables');
call dolt_checkout('-b', 'feature-branch');
call dolt_checkout('main');
INSERT INTO one_pk (pk1,c1,c2) VALUES (0,0,0);
call dolt_commit('-a', '-m', 'changed main');
call dolt_checkout('feature-branch');
INSERT INTO one_pk (pk1,c1,c2) VALUES (0,1,1);
call dolt_commit('-a', '-m', 'changed feature branch');
call dolt_checkout('main');
call dolt_merge('feature-branch');
SQL
    log_status_eq 1
    [[ $output =~ "Merge conflict detected, transaction rolled back. Merge conflicts must be resolved using the dolt_conflicts tables before committing a transaction. To commit transactions with merge conflicts, set @@dolt_allow_commit_conflicts = 1" ]] || false

    run dolt status
    log_status_eq 0
    [[ $output =~ "working tree clean" ]] || false

    run dolt merge --abort
    log_status_eq 1
    [[ $output =~ "no merge to abort" ]] || false

    # make sure a clean SQL session doesn't have any merge going
    run dolt sql -q "call dolt_merge('--abort');"
    log_status_eq 1
    [[ $output =~ "no merge to abort" ]] || false

    run dolt sql -q "SELECT * FROM one_pk;" -r csv
    log_status_eq 0
    [[ "$output" =~ "pk1,c1,c2" ]] || false
    [[ "$output" =~ "0,0,0" ]] || false
    [[ ! "$output" =~ "0,1,1" ]] || false

    dolt checkout feature-branch
    run dolt sql -q "SELECT * FROM one_pk;" -r csv
    log_status_eq 0
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
CALL DOLT_ADD('.');
call dolt_commit('-a', '-m', 'add tables');
call dolt_checkout('-b', 'feature-branch');
call dolt_checkout('main');
INSERT INTO one_pk (pk1,c1,c2) VALUES (0,0,0);
call dolt_commit('-a', '-m', 'changed main');
call dolt_checkout('feature-branch');
INSERT INTO one_pk (pk1,c1,c2) VALUES (0,1,1);
call dolt_commit('-a', '-m', 'changed feature branch');
call dolt_checkout('main');
call dolt_merge('feature-branch');
SQL
    log_status_eq 1
    [[ $output =~ "Merge conflict detected, transaction rolled back. Merge conflicts must be resolved using the dolt_conflicts tables before committing a transaction. To commit transactions with merge conflicts, set @@dolt_allow_commit_conflicts = 1" ]] || false

    # back on the command line, our session state is clean
    run dolt status
    log_status_eq 0
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "working tree clean" ]] || false
    [[ ! "$output" =~ "You have unmerged tables" ]] || false
    [[ ! "$output" =~ ([[:space:]]*both modified:[[:space:]]*one_pk) ]] || false

    # now merge, examine the conflicts, and abort
    run dolt sql -r csv  << SQL
SET autocommit = off;
call dolt_merge('feature-branch');
SELECT * FROM dolt_conflicts;
call dolt_merge('--abort');
SQL
    log_status_eq 0
    [[ "${lines[2]}" =~ "table,num_conflicts" ]] || false
    [[ "${lines[3]}" =~ "one_pk,1" ]] || false
    [[ "${lines[5]}" =~ "0" ]] || false

    # now resolve commits
    run dolt sql  << SQL
SET autocommit = off;
call dolt_merge('feature-branch');
REPLACE INTO one_pk (pk1, c1, c2) SELECT their_pk1, their_c1, their_c2 FROM dolt_conflicts_one_pk WHERE their_pk1 IS NOT NULL;
DELETE FROM one_pk WHERE pk1 in (SELECT base_pk1 FROM dolt_conflicts_one_pk WHERE their_pk1 IS NULL);
DELETE FROM dolt_conflicts_one_pk;
call dolt_commit('-a', '-m', 'Finish Resolving');
SQL
    log_status_eq 0

    run dolt sql -q "SELECT * FROM one_pk" -r csv
    log_status_eq 0
    [[ "$output" =~ "pk1,c1,c2" ]] || false
    [[ "$output" =~ "0,1,1" ]] || false

    run dolt sql -q "SELECT COUNT(*) from dolt_status;"
    log_status_eq 0
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
CALL DOLT_ADD('.');
call dolt_commit('-a', '-m', 'add tables');
call dolt_checkout('-b', 'feature-branch');
call dolt_checkout('main');
INSERT INTO one_pk (pk1,c1,c2) VALUES (0,0,0);
call dolt_commit('-a', '-m', 'changed main');
call dolt_checkout('feature-branch');
INSERT INTO one_pk (pk1,c1,c2) VALUES (0,1,1);
call dolt_commit('-a', '-m', 'changed feature branch');
call dolt_checkout('main');
call dolt_merge('feature-branch');
call dolt_merge('--abort');
insert into one_pk values (9,9,9);
commit;
SQL
    log_status_eq 0

    # We can see the latest inserted row back on the command line
    run dolt status
    log_status_eq 0
    [[ "$output" =~ ([[:space:]]*modified:[[:space:]]*one_pk) ]] || false

    run dolt diff
    log_status_eq 0
    [[ "$output" =~ "9" ]] || false

    run dolt sql -r csv -q "select * from one_pk where pk1 > 3";
    log_status_eq 0
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
CALL DOLT_ADD('.');
call dolt_commit('-a', '-m', 'add tables');
call dolt_checkout('-b', 'feature-branch');
call dolt_checkout('main');
INSERT INTO one_pk (pk1,c1,c2) VALUES (0,0,0);
call dolt_commit('-a', '-m', 'changed main');
call dolt_checkout('feature-branch');
INSERT INTO one_pk (pk1,c1,c2) VALUES (0,1,1);
call dolt_commit('-a', '-m', 'changed feature branch');
call dolt_checkout('main');
call dolt_merge('feature-branch');
call dolt_merge('--abort');
commit;
SQL
    log_status_eq 0

    run dolt status
    log_status_eq 0
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
CALL DOLT_ADD('.');
call dolt_commit('-a', '-m', 'add tables');
call dolt_checkout('-b', 'feature-branch');
call dolt_checkout('main');
INSERT INTO one_pk (pk1,c1,c2) VALUES (0,0,0);
call dolt_commit('-a', '-m', 'changed main');
call dolt_checkout('feature-branch');
INSERT INTO one_pk (pk1,c1,c2) VALUES (0,1,1);
call dolt_commit('-a', '-m', 'changed feature branch');
call dolt_checkout('main');
call dolt_merge('feature-branch');
SQL

    run dolt sql -r csv -q "SELECT count(*) from dolt_conflicts"
    [[ "$output" =~ "1" ]] || false

    run dolt sql -q "call dolt_merge('feature-branch');"
    log_status_eq 1
    [[ $output =~ "merging is not possible because you have not committed an active merge" ]] || false
}

@test "sql-merge: DOLT_MERGE during an active merge throws an error" {
    run dolt sql << SQL
call dolt_commit('-a', '-m', 'Step 1');
call dolt_checkout('-b', 'feature-branch');
INSERT INTO test VALUES (3);
call dolt_commit('-a', '-m', 'Insert 3');
call dolt_checkout('main');
INSERT INTO test VALUES (500000);
call dolt_commit('-a', '-m', 'Insert 500000');
call dolt_merge('feature-branch', '--no-commit');
call dolt_merge('feature-branch');
SQL

    log_status_eq 1
    [[ $output =~ "merging is not possible because you have not committed an active merge" ]] || false
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
    log_status_eq 0
    [[ "$output" =~ "true" ]] || false

    run dolt log -n 1
    log_status_eq 0
    [[ "$output" =~ "Step 1" ]] || false

    run dolt sql -q "SELECT COUNT(*) FROM dolt_log"
    log_status_eq 0
    [[ "$output" =~ "2" ]] || false

    run dolt status
    log_status_eq 0
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ ([[:space:]]*modified:[[:space:]]*test) ]] || false

    run dolt sql -q "CALL DOLT_COMMIT('-a', '-m', 'hi');"
    log_status_eq 0

    run dolt status
    log_status_eq 0
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false
}

@test "sql-merge: DOLT_MERGE with no-ff and squash works." {
    dolt sql << SQL
call dolt_commit('-a', '-m', 'Step 1');
call dolt_checkout('-b', 'feature-branch');
INSERT INTO test VALUES (3);
call dolt_commit('-a', '-m', 'Insert 3');
call dolt_checkout('main');
INSERT INTO test VALUES (500000);
call dolt_commit('-a', '-m', 'Insert 500000');
call dolt_merge('feature-branch', '--squash', '--no-commit');
SQL

    run dolt status
    log_status_eq 0
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ ([[:space:]]*modified:[[:space:]]*test) ]] || false

    run dolt sql -q "call dolt_commit('-a', '-m', 'Finish up Merge')";
    log_status_eq 0

    run dolt status
    log_status_eq 0
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false

    run dolt log -n 1
    log_status_eq 0
    [[ "$output" =~ "Finish up Merge" ]] || false
}

@test "sql-merge: DOLT_MERGE with a long series of changing operations works." {
    dolt sql << SQL
call dolt_commit('-a', '-m', 'Step 1');
call dolt_checkout('-b', 'feature-branch');
INSERT INTO test VALUES (3);
INSERT INTO test VALUES (4);
INSERT INTO test VALUES (21232);
DELETE FROM test WHERE pk=4;
UPDATE test SET pk=21 WHERE pk=21232;
call dolt_commit('-a', '-m', 'Insert 3');
call dolt_checkout('main');
INSERT INTO test VALUES (500000);
INSERT INTO test VALUES (500001);
DELETE FROM test WHERE pk=500001;
UPDATE test SET pk=60 WHERE pk=500000;
call dolt_commit('-a', '-m', 'Insert 60');
call dolt_merge('feature-branch', '--no-commit');
SQL

    run dolt status
    log_status_eq 0
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ ([[:space:]]*modified:[[:space:]]*test) ]] || false

    run dolt sql -q "call dolt_commit('-a', '-m', 'Finish up Merge')";
    log_status_eq 0

    run dolt status
    log_status_eq 0
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false

    run dolt log -n 1
    log_status_eq 0
    [[ "$output" =~ "Finish up Merge" ]] || false

    run dolt sql -q "SELECT * FROM test;" -r csv
    log_status_eq 0
    [[ "$output" =~ "pk" ]] || false
    [[ "$output" =~ "0" ]] || false
    [[ "$output" =~ "1" ]] || false
    [[ "$output" =~ "2" ]] || false
    [[ "$output" =~ "3" ]] || false
    [[ "$output" =~ "21" ]] || false
    [[ "$output" =~ "60" ]] || false

    run dolt sql -q "SELECT COUNT(*) FROM test;" -r csv
    log_status_eq 0
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
CALL DOLT_ADD('.');
call dolt_commit('-a', '-m', 'add tables');
call dolt_checkout('-b', 'feature-branch');
call dolt_checkout('main');
INSERT INTO one_pk (pk1,c1,c2) VALUES (0,0,0);
call dolt_commit('-a', '-m', 'changed main');
call dolt_checkout('feature-branch');
INSERT INTO one_pk (pk1,c1,c2) VALUES (0,1,1);
call dolt_commit('-a', '-m', 'changed feature branch');
call dolt_checkout('main');
call dolt_merge('feature-branch');
SHOW WARNINGS;
SELECT COUNT(*) FROM dolt_conflicts where num_conflicts > 0;
rollback;
SQL
    log_status_eq 0
    [[ "$output" =~ "| conflicts |" ]] || false
    [[ "$output" =~ "| 1         |" ]] || false # conflict should return 1//
    [[ "$output" =~ "| Warning | 1105 | merge has unresolved conflicts or constraint violations |" ]] || false
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
call dolt_commit('-a', '-m', 'add tables');
call dolt_checkout('-b', 'feature-branch');
call dolt_checkout('main');
INSERT INTO one_pk (pk1,c1,c2) VALUES (0,0,0);
call dolt_commit('-a', '-m', 'changed main');
call dolt_checkout('feature-branch');
INSERT INTO one_pk (pk1,c1,c2) VALUES (0,1,1);
call dolt_commit('-a', '-m', 'changed feature branch');
call dolt_checkout('main');
call dolt_merge('feature-branch');
SHOW WARNINGS;
SELECT COUNT(*) FROM dolt_conflicts where num_conflicts > 0;
rollback;
SQL
    log_status_eq 0
    [[ "$output" =~ "| DOLT_MERGE('feature-branch') |" ]] || false
    [[ "$output" =~ "| 0                            |" ]] || false # conflict should return 0
    [[ "$output" =~ "| Warning | 1105 | merge has unresolved conflicts. please use the dolt_conflicts table to resolve |" ]] || false
    [[ "$output" =~ "| COUNT(*) |" ]] || false
    [[ "$output" =~ "| 1        |" ]] || false
}

@test "sql-merge: up-to-date branch does not error" {
    dolt commit -am "commit all changes"
    run dolt sql << SQL
call dolt_checkout('-b', 'feature-branch');
call dolt_checkout('main');
INSERT INTO test VALUES (3);
call dolt_commit('-a', '-m', 'a commit');
call dolt_merge('feature-branch');
SHOW WARNINGS;
SQL
   log_status_eq 0
   [[ "$output" =~ "cannot fast forward from a to b. a is ahead of b already" ]] || false
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
   log_status_eq 0
   [[ "$output" =~ "cannot fast forward from a to b. a is ahead of b already" ]] || false
}

@test "sql-merge: adding and dropping primary keys any number of times not produce schema merge conflicts" {
    dolt commit -am "commit all changes"
    dolt sql -q "create table test_null (i int)"
    dolt add .
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

    run dolt sql -q "call dolt_merge('b1', '-m', 'merge')"
    log_status_eq 0
}

@test "sql-merge: identical schema changes with data changes merges correctly" {
    dolt sql -q "create table t (i int primary key)"
    dolt add .
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
    run dolt sql -q "call dolt_merge('b1', '-m', 'merge b1')"
    log_status_eq 0
    run dolt sql -q "call dolt_merge('b2', '-m', 'merge b2')"
    log_status_eq 0
}

# TODO: what happens when the data conflicts with new check?
@test "sql-merge: non-conflicting data and constraint changes are preserved" {
    dolt sql -q "create table t (i int)"
    dolt add .
    dolt commit -am "initial commit"

    dolt checkout -b other
    dolt sql -q "insert into t values (1)"
    run dolt sql -q "select * from t"
    log_status_eq 0
    [[ "$output" =~ "1" ]] || false
    dolt commit -am "changes to other"

    dolt checkout main
    dolt sql -q "alter table t add check (i < 10)"
    run dolt sql -q "show create table t"
    log_status_eq 0
    [[ "$output" =~ "(\`i\` < 10)" ]] || false
    dolt commit -am "changes to main"

    run dolt sql -q "call dolt_merge('other', '-m', 'merge other')"
    log_status_eq 0

    run dolt sql -q "select * from t"
    log_status_eq 0
    [[ "$output" =~ "1" ]] || false
    run dolt sql -q "show create table t"
    log_status_eq 0
    [[ "$output" =~ "(\`i\` < 10)" ]] || false
}

@test "sql-merge: non-overlapping check constraints merge successfully" {
    dolt sql -q "create table t (i int, j int)"
    dolt add .
    dolt commit -am "initial commit"

    dolt checkout -b other
    dolt sql -q "alter table t add constraint c0 check (i > 0)"
    run dolt sql -q "show create table t"
    log_status_eq 0
    [[ "$output" =~ "CONSTRAINT \`c0\` CHECK ((\`i\` > 0))" ]] || false
    dolt commit -am "changes to other"

    dolt checkout main
    dolt sql -q "alter table t add constraint c1 check (j < 0)"
    run dolt sql -q "show create table t"
    log_status_eq 0
    [[ "$output" =~ "CONSTRAINT \`c1\` CHECK ((\`j\` < 0))" ]] || false
    dolt commit -am "changes to main"

    run dolt sql -q "call dolt_merge('other', '-m', 'merge other')"
    log_status_eq 0

    run dolt sql -q "show create table t"
    log_status_eq 0
    [[ "$output" =~ "CONSTRAINT \`c0\` CHECK ((\`i\` > 0))" ]] || false
    [[ "$output" =~ "CONSTRAINT \`c1\` CHECK ((\`j\` < 0))" ]] || false
}

@test "sql-merge: different check constraints on same column throw conflict" {
    dolt sql -q "create table t (i int)"
    dolt add .
    dolt commit -am "initial commit"

    dolt checkout -b other
    dolt sql -q "alter table t add constraint c0 check (i > 0)"
    run dolt sql -q "show create table t"
    log_status_eq 0
    [[ "$output" =~ "CONSTRAINT \`c0\` CHECK ((\`i\` > 0))" ]] || false
    dolt commit -am "changes to other"

    dolt checkout main
    dolt sql -q "alter table t add constraint c1 check (i < 0)"
    run dolt sql -q "show create table t"
    log_status_eq 0
    [[ "$output" =~ "CONSTRAINT \`c1\` CHECK ((\`i\` < 0))" ]] || false
    dolt commit -am "changes to main"

    dolt sql -q "set @@dolt_force_transaction_commit=1; call dolt_merge('other', '-m', 'merge other')"

    run dolt status
    log_status_eq 0
    [[ "$output" =~ "schema conflict:" ]] || false
    run dolt sql -q "select count(*) from dolt_schema_conflicts"
    log_status_eq 0
    [[ "$output" =~ "1" ]] || false
    dolt sql -q "call dolt_conflicts_resolve('--ours', 't')"
    dolt sql -q "show create table t"
    run dolt sql -q "show create table t"
    log_status_eq 0
    [[ "$output" =~ "CONSTRAINT \`c1\` CHECK ((\`i\` < 0))" ]] || false
}

@test "sql-merge: dropping constraint on both branches merges successfully" {
    dolt sql -q "create table t (i int)"
    dolt add .
    dolt sql -q "alter table t add constraint c check (i > 0)"
    dolt commit -am "initial commit"

    dolt checkout -b other
    dolt sql -q "alter table t drop constraint c"
    run dolt sql -q "show create table t"
    log_status_eq 0
    [[ ! ("$output" =~ "CONSTRAINT \`c\` CHECK ((\`i\` > 0))") ]] || false
    dolt commit -am "changes to other"

    dolt checkout main
    dolt sql -q "alter table t drop constraint c"
    run dolt sql -q "show create table t"
    log_status_eq 0
    [[ ! ("$output" =~ "CONSTRAINT \`c\` CHECK ((\`i\` > 0))") ]] || false
    dolt commit -am "changes to main"

    run dolt sql -q "call dolt_merge('other', '-m', 'merge other')"
    log_status_eq 0

    run dolt sql -q "show create table t"
    log_status_eq 0
    [[ ! ("$output" =~ "CONSTRAINT \`c\` CHECK ((\`i\` > 0))") ]] || false
}

@test "sql-merge: dropping constraint in one branch and modifying same in other results in conflict" {
    dolt sql -q "create table t (i int)"
    dolt add .
    dolt sql -q "alter table t add constraint c check (i > 0)"
    dolt commit -am "initial commit"

    dolt checkout -b other
    dolt sql -q "alter table t drop constraint c"
    run dolt sql -q "show create table t"
    log_status_eq 0
    [[ ! ("$output" =~ "CONSTRAINT \`c\` CHECK ((\`i\` > 0))") ]] || false
    dolt commit -am "changes to other"

    dolt checkout main
    dolt sql -q "alter table t drop constraint c"
    dolt sql -q "alter table t add constraint c check (i < 10)"
    run dolt sql -q "show create table t"
    log_status_eq 0
    [[ "$output" =~ "CONSTRAINT \`c\` CHECK ((\`i\` < 10))" ]] || false
    dolt commit -am "changes to main"

    dolt sql -q "set @@dolt_force_transaction_commit=1; call dolt_merge('other', '-m', 'merge other')"
    run dolt status
    log_status_eq 0
    [[ "$output" =~ "schema conflict:" ]] || false
    run dolt sql -q "select count(*) from dolt_schema_conflicts"
    log_status_eq 0
    [[ "$output" =~ "1" ]] || false
    dolt sql -q "call dolt_conflicts_resolve('--ours', 't')"
    run dolt sql -q "show create table t"
    log_status_eq 0
    [[ ! ("$output" =~ "CONSTRAINT \`c\` CHECK ((\`i\` > 0))") ]] || false
}

@test "sql-merge: merging with not null and check constraints preserves both constraints" {
    dolt sql -q "create table t (i int)"
    dolt add .
    dolt commit -am "initial commit"

    dolt branch b1
    dolt branch b2

    dolt checkout b1
    dolt sql -q "alter table t add check (i > 0)"
    run dolt sql -q "show create table t"
    log_status_eq 0
    [[ "$output" =~ "(\`i\` > 0)" ]] || false
    dolt commit -am "changes to b1"

    dolt checkout b2
    dolt sql -q "alter table t modify i int not null"
    run dolt sql -q "show create table t"
    log_status_eq 0
    [[ "$output" =~ "\`i\` int NOT NULL" ]] || false
    dolt commit -am "changes to b2"

    dolt checkout main
    run dolt sql -q "call dolt_merge('b1', '-m', 'merge b1')"
    log_status_eq 0
    run dolt sql -q "call dolt_merge('b2', '-m', 'merge b2')"
    log_status_eq 0

    run dolt sql -q "show create table t"
    log_status_eq 0
    [[ "$output" =~ "\`i\` int NOT NULL" ]] || false
    [[ "$output" =~ "(\`i\` > 0)" ]] || false
}

@test "sql-merge: check constraint with name collision" {
    dolt sql -q "create table t (i int)"
    dolt add .
    dolt commit -am "initial commit"

    dolt branch b1
    dolt branch b2

    dolt checkout b1
    dolt sql -q "alter table t add constraint c check (i > 0)"
    run dolt sql -q "show create table t"
    log_status_eq 0
    [[ "$output" =~ "CONSTRAINT \`c\` CHECK ((\`i\` > 0))" ]] || false
    dolt commit -am "changes to b1"

    dolt checkout b2
    dolt sql -q "alter table t add constraint c check (i < 10)"
    run dolt sql -q "show create table t"
    log_status_eq 0
    [[ "$output" =~ "CONSTRAINT \`c\` CHECK ((\`i\` < 10))" ]] || false
    dolt commit -am "changes to b2"

    dolt checkout main
    run dolt merge b1 -m "merge b1" --commit
    log_status_eq 0
    dolt sql -q "set @@dolt_force_transaction_commit=1; call dolt_merge('b2', '-m', 'merge b2')"
    run dolt status
    log_status_eq 0
    [[ "$output" =~ "schema conflict:" ]] || false
    run dolt sql -q "select count(*) from dolt_schema_conflicts"
    log_status_eq 0
    [[ "$output" =~ "1" ]] || false
    dolt sql -q "call dolt_conflicts_resolve('--ours', 't')"
    run dolt sql -q "show create table t"
    log_status_eq 0
    [[ "$output" =~ "CONSTRAINT \`c\` CHECK ((\`i\` > 0))" ]] || false
}

@test "sql-merge: check constraint for deleted column in another table" {
    dolt sql -q "create table t (i int primary key, j int)"
    dolt add .
    dolt commit -am "initial commit"

    dolt branch b1
    dolt branch b2

    dolt checkout b1
    dolt sql -q "alter table t add constraint c check (j > 0)"
    run dolt sql -q "show create table t"
    log_status_eq 0
    [[ "$output" =~ "CONSTRAINT \`c\` CHECK ((\`j\` > 0))" ]] || false
    dolt commit -am "changes to b1"

    dolt checkout b2
    dolt sql -q "alter table t drop column j"
    dolt commit -am "changes to b2"

    dolt checkout main
    run dolt sql -q "call dolt_merge('b1', '-m', 'merge b1')"
    log_status_eq 0
    dolt sql -q "set @@dolt_force_transaction_commit=1; call dolt_merge('b2', '-m', 'merge b2')"
    run dolt status
    log_status_eq 0
    [[ "$output" =~ "schema conflict:" ]] || false
    run dolt sql -q "select count(*) from dolt_schema_conflicts"
    log_status_eq 0
    [[ "$output" =~ "1" ]] || false
    dolt sql -q "call dolt_conflicts_resolve('--ours', 't')"
    run dolt sql -q "show create table t"
    log_status_eq 0
    [[ "$output" =~ "CONSTRAINT \`c\` CHECK ((\`j\` > 0))" ]] || false
}

@test "sql-merge: DOLT_MERGE with author flag specified" {
    dolt sql <<SQL
CALL DOLT_COMMIT('-a', '-m', 'Step 1');
CALL DOLT_CHECKOUT('-b', 'feature-branch');
INSERT INTO test VALUES (3);
CALL DOLT_COMMIT('-a', '-m', 'add 3 from other');
CALL DOLT_CHECKOUT('main');
INSERT INTO test VALUES (4);
CALL DOLT_COMMIT('-a', '-m', 'add 4 from main');
SQL

    run dolt config --list
    [ "$status" -eq 0 ]
    [[ "$output" =~ "user.email = bats@email.fake" ]] || false
    [[ "$output" =~ "user.name = Bats Tests" ]] || false

    run dolt sql -q "CALL DOLT_MERGE('feature-branch', '--author', 'John Doe <john@doe.com>');"
    log_status_eq 0

    run dolt log -n 1
    [ "$status" -eq 0 ]
    regex='John Doe <john@doe.com>'
    [[ "$output" =~ "$regex" ]] || false
}

@test "sql-merge: ff merge doesn't stomp working changes" {
  dolt sql <<SQL
CREATE TABLE test2 (pk int);
CALL DOLT_COMMIT('-m', 'initial commit');
CALL DOLT_CHECKOUT('-b', 'merge_branch');
INSERT INTO test VALUES (9);
CALL DOLT_COMMIT('-a', '-m', 'modify test');
CALL DOLT_CHECKOUT('main');
INSERT INTO test2 VALUES (8);
SQL
    run dolt status
    log_status_eq 0
    [[ "$output" =~ "test2" ]] || false
    [[ ! "$output" =~ "test1" ]] || false

    run dolt sql -q "CALL DOLT_MERGE('merge_branch');"
    log_status_eq 0

    run dolt sql -q "select COUNT(*) from test"
    [[ "$output" =~ 4 ]] || false

    run dolt status
    log_status_eq 0
    [[ "$output" =~ "test2" ]] || false
    [[ ! "$output" =~ "test1" ]] || false
}

@test "sql-merge: no-ff merge doesn't stomp working changes and doesn't fast forward" {
    dolt commit -m "initial commit"
    dolt checkout -b merge_branch
    dolt SQL -q "INSERT INTO test values (9)"
    dolt add test
    dolt commit -m "modify test"

    dolt checkout main
    dolt SQL -q "CREATE TABLE t2 (pk int)"
    dolt SQL -q "INSERT INTO t2 values (0)"
    run dolt status
    log_status_eq 0
    [[ "$output" =~ "t2" ]] || false
    [[ ! "$output" =~ "test" ]] || false

    run dolt sql -q "CALL DOLT_MERGE('merge_branch', '--no-ff', '-m', 'no-ff merge');"
    log_status_eq 0
    [[ ! "$output" =~ "| 1            |" ]] || false

    run dolt status
    log_status_eq 0
    [[ "$output" =~ "t2" ]] || false
    [[ ! "$output" =~ "test" ]] || false

    run dolt log
    log_status_eq 0
    [[ "$output" =~ "no-ff merge" ]] || false
}

@test "sql-merge: squash merge" {
    dolt commit -m "initial commit"
    dolt checkout -b merge_branch
    dolt sql -q "INSERT INTO test values (3)"
    dolt add test
    dolt commit -m "add pk 3 to test"

    dolt checkout main
    dolt sql -q "INSERT INTO test values (4)"
    dolt add test
    dolt commit -m "add pk 4 to test1"

    dolt sql -q "CREATE TABLE t2 (pk int)"
    dolt sql -q "INSERT INTO t2 values (9)"
    run dolt status
    log_status_eq 0
    [[ "$output" =~ "t2" ]] || false
    [[ ! "$output" =~ "test1" ]] || false

    run dolt sql -q "CALL DOLT_MERGE('--squash', 'merge_branch', '--no-commit');"
    log_status_eq 0
    [[ "$output" =~ "| hash | fast_forward | conflicts |" ]] || false
    [[ "$output" =~ "| 0            | 0         |" ]] || false

    run dolt status
    log_status_eq 0
    [[ "$output" =~ "t2" ]] || false
    [[ "$output" =~ "test" ]] || false

    dolt add .
    dolt commit -m "squash merge"

    # make sure the squashed commit is not in the log.
    run dolt log
    log_status_eq 0
    [[ "$output" =~ "add pk 4 to test" ]] || false
    [[ ! "$output" =~ "add pk 3 to test" ]] || false
}

@test "sql-merge: --abort restores working changes" {
    dolt commit -m "initial commit"
    dolt branch other

    dolt sql -q "INSERT INTO test VALUES (9),(8);"
    dolt commit -am "added rows to test on main"

    dolt checkout other
    dolt sql -q "INSERT INTO test VALUES (7),(6);"
    dolt commit -am "added rows to test on other"

    dolt checkout main
    # dirty the working set with changes to test2
    dolt SQL -q "CREATE TABLE t2 (pk int)"
    dolt sql -q "INSERT INTO t2 VALUES (5);"

    dolt merge other --no-commit
    dolt sql -q "call dolt_merge('--abort');"

    run dolt sql -q "SELECT * from dolt_merge_status"
    [[ "$output" =~ "| is_merging | source | source_commit | target | unmerged_tables |" ]] || false
    [[ "$output" =~ "| false      | NULL   | NULL          | NULL   | NULL            |" ]] || false

    # per Git, working set changes to test2 should remain
    dolt sql -q "SELECT * FROM t2" -r csv
    run dolt sql -q "SELECT * FROM t2" -r csv
    log_status_eq 0
    [[ "${lines[1]}" =~ "5" ]] || false
}

@test "sql-merge: 3way merge doesn't stomp working changes" {
    dolt commit -m "initial commit"
    dolt checkout -b merge_branch
    dolt SQL -q "INSERT INTO test values (9)"
    dolt add test
    dolt commit -m "add pk 9 to test1"

    dolt checkout main
    dolt SQL -q "INSERT INTO test values (8)"
    dolt add test
    dolt commit -m "add pk 8 to test1"

    dolt SQL -q "CREATE TABLE t2 (pk int)"
    dolt SQL -q "INSERT INTO t2 values (7)"
    run dolt status
    log_status_eq 0
    [[ "$output" =~ "t2" ]] || false
    [[ ! "$output" =~ "test" ]] || false

    run dolt sql -q "CALL DOLT_MERGE('merge_branch', '--no-commit');"
    log_status_eq 0
    [[ ! "$output" =~ "| 1            |" ]] || false

    run dolt status
    echo -e "\n\noutput: " $output "\n\n"
    log_status_eq 0
    [[ "$output" =~ "t2" ]] || false
    [[ "$output" =~ "test" ]] || false

   run dolt sql -q "SELECT * from dolt_merge_status"
   [[ "$output" =~ "true" ]] || false
   [[ "$output" =~ "merge_branch" ]] || false
   [[ "$output" =~ "refs/heads/main" ]] || false

    # make sure all the commits make it into the log
    dolt add .
    dolt commit -m "squash merge"

    run dolt log
    log_status_eq 0
    [[ "$output" =~ "add pk 9 to test" ]] || false
    [[ "$output" =~ "add pk 8 to test" ]] || false
}

get_head_commit() {
    dolt log -n 1 | grep -m 1 commit | cut -c 13-44
}

get_working_hash() {
  dolt sql -q "select @@dolt_repo_$$_working" | sed -n 4p | sed -e 's/|//' -e 's/|//'  -e 's/ //'
}
