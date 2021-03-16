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

@test "sql-merge: DOLT_MERGE with unknown branch name throws an error" {
    dolt sql -q "SELECT DOLT_COMMIT('-a', '-m', 'Step 1');"

    run dolt sql -q "SELECT DOLT_MERGE('feature-branch');"
    [ $status -eq 1 ]
}

@test "sql-merge: DOLT_MERGE works with ff" {
        dolt sql << SQL
SELECT DOLT_COMMIT('-a', '-m', 'Step 1');
SELECT DOLT_CHECKOUT('-b', 'feature-branch');
INSERT INTO test VALUES (3);
UPDATE test SET pk=1000 WHERE pk=0;
SELECT DOLT_COMMIT('-a', '-m', 'this is a ff');
SELECT DOLT_CHECKOUT('master');
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

@test "sql-merge: DOLT_MERGE works in the session for fastforward." {
     run dolt sql << SQL
SELECT DOLT_COMMIT('-a', '-m', 'Step 1');
SELECT DOLT_CHECKOUT('-b', 'feature-branch');
INSERT INTO test VALUES (3);
SELECT DOLT_COMMIT('-a', '-m', 'this is a ff');
SELECT DOLT_CHECKOUT('master');
SELECT DOLT_MERGE('feature-branch');
SELECT COUNT(*) > 0 FROM test WHERE pk=3;
SQL
    [ $status -eq 0 ]
    [[ "$output" =~ "true" ]] || false
}

@test "sql-merge: DOLT_MERGE correctly returns head and working session variables." {
    dolt sql << SQL
SELECT DOLT_COMMIT('-a', '-m', 'Step 1');
SELECT DOLT_CHECKOUT('-b', 'feature-branch');
INSERT INTO test VALUES (3);
SELECT DOLT_COMMIT('-a', '-m', 'this is a ff');
SQL
    head_variable=@@dolt_repo_$$_head
    head_hash=$(get_head_commit)

    dolt sql << SQL
SELECT DOLT_CHECKOUT('master');
SELECT DOLT_MERGE('feature-branch');
SQL

    run dolt sql -q "SELECT $head_variable"
    [ $status -eq 0 ]
    [[ "$output" =~ $head_hash ]] || false
}

@test "sql-merge: DOLT_MERGE correctly merges branches with differing content in same table without conflicts" {
    run dolt sql << SQL
SELECT DOLT_COMMIT('-a', '-m', 'Step 1');
SELECT DOLT_CHECKOUT('-b', 'feature-branch');
INSERT INTO test VALUES (3);
SELECT DOLT_COMMIT('-a', '-m', 'Insert 3');
SELECT DOLT_CHECKOUT('master');
INSERT INTO test VALUES (10000);
SELECT DOLT_COMMIT('-a', '-m', 'Insert 10000');
SELECT DOLT_MERGE('feature-branch');
SELECT COUNT(*) = 2 FROM test WHERE pk > 2;
SQL

    [ $status -eq 0 ]
    [[ "$output" =~ "true" ]] || false

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
    [[ "$output" =~ "All conflicts fixed but you are still merging" ]] || false
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
SELECT DOLT_CHECKOUT('master');
SELECT DOLT_MERGE('feature-branch', '-no-ff', '-m', 'this is a no-ff');
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
SELECT DOLT_CHECKOUT('master');
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

@test "sql-merge: DOLT_MERGE properly detects merge conflicts, returns and error and then aborts." {
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
SELECT DOLT_COMMIT('-a', '-m', 'changed feature branch');
SELECT DOLT_CHECKOUT('master');
SELECT DOLT_MERGE('feature-branch');
SQL
    [ $status -eq 1 ]
    [[ $output =~ "merge has conflicts" ]] || false

    run dolt sql -q "SELECT DOLT_MERGE('--abort');"
    [ $status -eq 0 ]

    run dolt status
    [ $status -eq 0 ]
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false

    run dolt sql -q "SELECT * FROM one_pk;" -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ "pk1,c1,c2" ]] || false
    [[ "$output" =~ "0,0,0" ]] || false
    [[ ! "$output" =~ "0,1,1" ]] || false
}

@test "sql-merge: DOLT_MERGE properly detects merge conflicts and renders the conflicts in dolt_conflicts." {
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
SELECT DOLT_COMMIT('-a', '-m', 'changed feature branch');
SELECT DOLT_CHECKOUT('master');
SELECT DOLT_MERGE('feature-branch');
SQL
    [ $status -eq 1 ]
    [[ $output =~ "merge has conflicts" ]] || false

    run dolt status
    [ $status -eq 0 ]
    [[ "$output" =~ "On branch master" ]] || false
    [[ "$output" =~ "You have unmerged tables" ]] || false
    [[ "$output" =~ ([[:space:]]*both modified:[[:space:]]*one_pk) ]] || false

    run dolt sql -q "SELECT * FROM dolt_conflicts" -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ "table,num_conflicts" ]] || false
    [[ "$output" =~ "one_pk,1" ]] || false

    # Go through the process of resolving commits
    run dolt sql << SQL
REPLACE INTO one_pk (pk1, c1, c2) SELECT their_pk1, their_c1, their_c2 FROM dolt_conflicts_one_pk WHERE their_pk1 IS NOT NULL;
DELETE FROM one_pk WHERE pk1 in (SELECT base_pk1 FROM dolt_conflicts_one_pk WHERE their_pk1 IS NULL);
DELETE FROM dolt_conflicts_one_pk;
SQL
    [ $status -eq 0 ]

    run dolt sql -q "SELECT * FROM dolt_conflicts" -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ "table,num_conflicts" ]] || false
    [[ "$output" =~ "one_pk,0" ]] || false

    run dolt sql -q "SELECT DOLT_COMMIT('-a', '-m', 'Finish Resolving');"
    [ $status -eq 0 ]

    run dolt sql -q "SELECT * FROM one_pk" -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ "pk1,c1,c2" ]] || false
    [[ "$output" =~ "0,1,1" ]] || false

    run dolt sql -q "SELECT COUNT(*) from dolt_status;"
    [ $status -eq 0 ]
    [[ "$output" =~ "0" ]] || false
}

@test "sql-merge: DOLT_MERGE with unresolved conflicts throws an error" {
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
SELECT DOLT_COMMIT('-a', '-m', 'changed feature branch');
SELECT DOLT_CHECKOUT('master');
SELECT DOLT_MERGE('feature-branch');
SQL
    [ $status -eq 1 ]
    [[ $output =~ "merge has conflicts" ]] || false

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
SELECT DOLT_CHECKOUT('master');
INSERT INTO test VALUES (500000);
SELECT DOLT_COMMIT('-a', '-m', 'Insert 500000');
SELECT DOLT_MERGE('feature-branch');
SELECT DOLT_MERGE('feature-branch');
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
SELECT DOLT_CHECKOUT('master');
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
    [[ "$output" =~ "On branch master" ]] || false
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ ([[:space:]]*modified:[[:space:]]*test) ]] || false

    run dolt sql -q "SELECT DOLT_COMMIT('-a', '-m', 'hi');"
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
SELECT DOLT_CHECKOUT('master');
INSERT INTO test VALUES (500000);
SELECT DOLT_COMMIT('-a', '-m', 'Insert 500000');
SELECT DOLT_MERGE('feature-branch', '--squash');
SQL

    run dolt status
    [ $status -eq 0 ]
    [[ "$output" =~ "On branch master" ]] || false
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

@test "sql-merge: DOLT_MERGE throws errors with working set changes." {
    run dolt sql << SQL
SELECT DOLT_COMMIT('-a', '-m', 'Step 1');
SELECT DOLT_CHECKOUT('-b', 'feature-branch');
INSERT INTO test VALUES (3);
SELECT DOLT_COMMIT('-a', '-m', 'this is a ff');
SELECT DOLT_CHECKOUT('master');
CREATE TABLE tbl (
    pk int primary key
);
SELECT DOLT_MERGE('feature-branch');
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
SELECT DOLT_CHECKOUT('master');
INSERT INTO test VALUES (500000);
INSERT INTO test VALUES (500001);
DELETE FROM test WHERE pk=500001;
UPDATE test SET pk=60 WHERE pk=500000;
SELECT DOLT_COMMIT('-a', '-m', 'Insert 60');
SELECT DOLT_MERGE('feature-branch');
SQL


    run dolt status
    [ $status -eq 0 ]
    [[ "$output" =~ "On branch master" ]] || false
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

get_head_commit() {
    dolt log -n 1 | grep -m 1 commit | cut -c 8-
}

get_working_hash() {
  dolt sql -q "select @@dolt_repo_$$_working" | sed -n 4p | sed -e 's/|//' -e 's/|//'  -e 's/ //'
}
