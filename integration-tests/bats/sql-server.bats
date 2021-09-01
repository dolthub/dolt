#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash
load $BATS_TEST_DIRNAME/helper/query-server-common.bash

make_repo() {
  mkdir "$1"
  cd "$1"
  dolt init
  cd ..
}

setup() {
    setup_no_dolt_init
    make_repo repo1
    make_repo repo2
}

teardown() {
    stop_sql_server
    teardown_common
}

@test "sql-server: port in use" {
    cd repo1

    let PORT="$$ % (65536-1024) + 1024"
    dolt sql-server --host 0.0.0.0 --port=$PORT --user dolt &
    SERVER_PID=$! # will get killed by teardown_common
    sleep 5 # not using python wait so this works on windows

    run dolt sql-server --host 0.0.0.0 --port=$PORT --user dolt
    [ "$status" -eq 1 ]
    [[ "$output" =~ "in use" ]] || false
}

@test "sql-server: multi-client" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    cd repo1
    start_sql_multi_user_server repo1

    cd $BATS_TEST_DIRNAME
    let PORT="$$ % (65536-1024) + 1024"
    python3 server_multiclient_test.py $PORT
}

@test "sql-server: test autocommit" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    cd repo1
    start_sql_server repo1

    # No tables at the start
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "No tables in working set" ]] || false

    # create table with autocommit off and verify there are still no tables
    server_query repo1 0 "CREATE TABLE one_pk (
        pk BIGINT NOT NULL COMMENT 'tag:0',
        c1 BIGINT COMMENT 'tag:1',
        c2 BIGINT COMMENT 'tag:2',
        PRIMARY KEY (pk)
    )" ""
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "No tables in working set" ]] || false

    # create table with autocommit on and verify table creation
    server_query repo1 1 "CREATE TABLE one_pk (
        pk BIGINT NOT NULL COMMENT 'tag:0',
        c1 BIGINT COMMENT 'tag:1',
        c2 BIGINT COMMENT 'tag:2',
        PRIMARY KEY (pk)
    )" ""
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "one_pk" ]] || false
}


@test "sql-server: test dolt sql interface works properly with autocommit" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    cd repo1
    start_sql_server repo1

    # No tables at the start
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "No tables in working set" ]] || false

    # create table with autocommit off and verify there are still no tables
    server_query repo1 0 "CREATE TABLE one_pk (
        pk BIGINT NOT NULL COMMENT 'tag:0',
        c1 BIGINT COMMENT 'tag:1',
        c2 BIGINT COMMENT 'tag:2',
        PRIMARY KEY (pk)
    )" ""
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "No tables in working set" ]] || false

    # check that dolt_commit throws an error when there are no changes to commit
    run server_query repo1 0 "SELECT DOLT_COMMIT('-a', '-m', 'Commit1')"
    [ "$status" -eq 1 ]

    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "No tables in working set" ]] || false

    # create table with autocommit on and verify table creation
    server_query repo1 1 "CREATE TABLE one_pk (
        pk BIGINT NOT NULL COMMENT 'tag:0',
        c1 BIGINT COMMENT 'tag:1',
        c2 BIGINT COMMENT 'tag:2',
        PRIMARY KEY (pk)
    )" ""
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "one_pk" ]] || false

    # check that dolt_commit works properly when autocommit is on
    run dolt sql -q "SELECT DOLT_COMMIT('-a', '-m', 'Commit1')"
    [ "$status" -eq 0 ]

    # check that dolt_commit throws error now that there are no working set changes.
    run dolt sql -q "SELECT DOLT_COMMIT('-a', '-m', 'Commit1')"
    [ "$status" -eq 1 ]

    # Make a change to the working set but not the staged set.
    run dolt sql -q "INSERT INTO one_pk (pk,c1,c2) VALUES (2,2,2),(3,3,3)"

    # check that dolt_commit throws error now that there are no staged changes.
    run dolt sql -q "SELECT DOLT_COMMIT('-m', 'Commit1')"
    [ "$status" -eq 1 ]

    run dolt log
    [ $status -eq 0 ]
    [[ "$output" =~ "Commit1" ]] || false
}

@test "sql-server: test basic querying via dolt sql-server" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    cd repo1
    start_sql_server repo1

    server_query repo1 1 "SHOW tables" ""
    server_query repo1 1 "CREATE TABLE one_pk (
        pk BIGINT NOT NULL COMMENT 'tag:0',
        c1 BIGINT COMMENT 'tag:1',
        c2 BIGINT COMMENT 'tag:2',
        PRIMARY KEY (pk)
    )" ""
    server_query repo1 1 "SHOW tables" "Table\none_pk"
    insert_query repo1 1 "INSERT INTO one_pk (pk) VALUES (0)"
    server_query repo1 1 "SELECT * FROM one_pk ORDER BY pk" "pk,c1,c2\n0,None,None"
    insert_query repo1 1 "INSERT INTO one_pk (pk,c1) VALUES (1,1)"
    insert_query repo1 1 "INSERT INTO one_pk (pk,c1,c2) VALUES (2,2,2),(3,3,3)"
    server_query repo1 1 "SELECT * FROM one_pk ORDER by pk" "pk,c1,c2\n0,None,None\n1,1,None\n2,2,2\n3,3,3"
    update_query repo1 1 "UPDATE one_pk SET c2=c1 WHERE c2 is NULL and c1 IS NOT NULL"
}

@test "sql-server: test multiple queries on the same connection" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    cd repo1
    start_sql_server repo1

    multi_query repo1 1 "CREATE TABLE one_pk (
        pk BIGINT NOT NULL COMMENT 'tag:0',
        c1 BIGINT COMMENT 'tag:1',
        c2 BIGINT COMMENT 'tag:2',
        PRIMARY KEY (pk)
    );
    INSERT INTO one_pk (pk) VALUES (0);
    INSERT INTO one_pk (pk,c1) VALUES (1,1);
    INSERT INTO one_pk (pk,c1,c2) VALUES (2,2,2),(3,3,3);"

    server_query repo1 1 "SELECT * FROM one_pk ORDER by pk" "pk,c1,c2\n0,None,None\n1,1,None\n2,2,2\n3,3,3"
}

@test "sql-server: test manual commit" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."


    cd repo1
    start_sql_server repo1

    # check that only master branch exists
    server_query repo1 0 "SELECT name, latest_commit_message FROM dolt_branches" "name,latest_commit_message\nmaster,Initialize data repository"

    # check that new connections are set to master by default
    server_query repo1 0 "SELECT name, latest_commit_message FROM dolt_branches WHERE hash = @@repo1_head" "name,latest_commit_message\nmaster,Initialize data repository"

    # check no tables on master
    server_query repo1 0 "SHOW Tables" ""

    # make some changes to master and commit to branch test_branch
    multi_query repo1 0 "
    SET @@repo1_head=hashof('master');
    CREATE TABLE one_pk (
        pk BIGINT NOT NULL COMMENT 'tag:0',
        c1 BIGINT COMMENT 'tag:1',
        c2 BIGINT COMMENT 'tag:2',
        PRIMARY KEY (pk)
    );
    INSERT INTO one_pk (pk) VALUES (0);
    INSERT INTO one_pk (pk,c1) VALUES (1,1);
    INSERT INTO one_pk (pk,c1,c2) VALUES (2,2,2),(3,3,3);
    SET @@repo1_head=commit('-m', 'test commit message', '--author', 'John Doe <john@example.com>');
    INSERT INTO dolt_branches (name,hash) VALUES ('test_branch', @@repo1_head);"

    # validate new branch was created
    server_query repo1 0 "SELECT name,latest_commit_message FROM dolt_branches" "name,latest_commit_message\nmaster,Initialize data repository\ntest_branch,test commit message"

    # Check that the author information is correct.
    server_query repo1 0 "SELECT latest_committer,latest_committer_email FROM dolt_branches ORDER BY latest_commit_date DESC LIMIT 1" "latest_committer,latest_committer_email\nJohn Doe,john@example.com"

    # validate no tables on master still
    server_query repo1 0 "SHOW tables" ""

    # validate tables and data on test_branch
    server_query repo1 0 "SET @@repo1_head=hashof('test_branch');SHOW tables" ";Table\none_pk"
    server_query repo1 0 "SET @@repo1_head=hashof('test_branch');SELECT * FROM one_pk ORDER by pk" ";pk,c1,c2\n0,None,None\n1,1,None\n2,2,2\n3,3,3"
}

@test "sql-server: test manual merge" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    cd repo1
    start_sql_server repo1

    # check that only master branch exists
    server_query repo1 0 "SELECT name, latest_commit_message FROM dolt_branches" "name,latest_commit_message\nmaster,Initialize data repository"

    # check that new connections are set to master by default
    server_query repo1 0 "SELECT name, latest_commit_message FROM dolt_branches WHERE hash = @@repo1_head" "name,latest_commit_message\nmaster,Initialize data repository"

    # check no tables on master
    server_query repo1 0 "SHOW Tables" ""

    # make some changes to master and commit to branch test_branch
    multi_query repo1 0 "
    SET @@repo1_head=hashof('master');
    CREATE TABLE one_pk (
        pk BIGINT NOT NULL COMMENT 'tag:0',
        c1 BIGINT COMMENT 'tag:1',
        c2 BIGINT COMMENT 'tag:2',
        PRIMARY KEY (pk)
    );
    INSERT INTO one_pk (pk) VALUES (0);
    INSERT INTO one_pk (pk,c1) VALUES (1,1);
    INSERT INTO one_pk (pk,c1,c2) VALUES (2,2,2),(3,3,3);
    SET @@repo1_head=commit('-m', 'test commit message');
    INSERT INTO dolt_branches (name,hash) VALUES ('test_branch', @@repo1_head);"

    # validate new branch was created
    server_query repo1 0 "SELECT name,latest_commit_message FROM dolt_branches" "name,latest_commit_message\nmaster,Initialize data repository\ntest_branch,test commit message"

    # validate no tables on master still
    server_query repo1 0 "SHOW tables" ""

    # Merge the test_branch into master. This should a fast forward merge.
    multi_query repo1 0 "
    SET @@repo1_head = merge('test_branch');
    INSERT INTO dolt_branches (name, hash) VALUES('master', @@repo1_head);"

    # Validate tables and data on master
    server_query repo1 0 "SET @@repo1_head=hashof('master');SHOW tables" ";Table\none_pk"
    server_query repo1 0 "SET @@repo1_head=hashof('master');SELECT * FROM one_pk ORDER by pk" ";pk,c1,c2\n0,None,None\n1,1,None\n2,2,2\n3,3,3"

    # Validate the commit master matches that of test_branch (this is a fast forward) by matching commit hashes.
    server_query repo1 0 "select COUNT(hash) from dolt_branches where hash IN (select hash from dolt_branches WHERE name = 'test_branch')" "COUNT(hash)\n2"

    # make some changes to test_branch and commit. Make some changes to master and commit. Merge.
    multi_query repo1 0 "
    SET @@repo1_head=hashof('master');
    UPDATE one_pk SET c1=10 WHERE pk=2;
    SET @@repo1_head=commit('-m', 'Change c 1 to 10');
    INSERT INTO dolt_branches (name,hash) VALUES ('master', @@repo1_head);

    SET @@repo1_head=hashof('test_branch');
    INSERT INTO one_pk (pk,c1,c2) VALUES (4,4,4);
    SET @@repo1_head=commit('-m', 'add 4');
    INSERT INTO dolt_branches (name,hash) VALUES ('test_branch', @@repo1_head);"

    multi_query repo1 0 "
    SET @@repo1_head=hashof('master');
    SET @@repo1_head=merge('test_branch');
    INSERT INTO dolt_branches (name, hash) VALUES('master', @@repo1_head);"

    # Validate tables and data on master
    server_query repo1 0 "SET @@repo1_head=hashof('master');SHOW tables" ";Table\none_pk"
    server_query repo1 0 "SET @@repo1_head=hashof('master');SELECT * FROM one_pk ORDER by pk" ";pk,c1,c2\n0,None,None\n1,1,None\n2,10,2\n3,3,3\n4,4,4"

    # Validate the a merge commit was written by making sure the hashes of the two branches don't match
    server_query repo1 0 "select COUNT(hash) from dolt_branches where hash IN (select hash from dolt_branches WHERE name = 'test_branch')" "COUNT(hash)\n1"
}


@test "sql-server: test manual squash" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    cd repo1
    start_sql_server repo1

    # check that only master branch exists
    server_query repo1 0 "SELECT name, latest_commit_message FROM dolt_branches" "name,latest_commit_message\nmaster,Initialize data repository"

    # check that new connections are set to master by default
    server_query repo1 0 "SELECT name, latest_commit_message FROM dolt_branches WHERE hash = @@repo1_head" "name,latest_commit_message\nmaster,Initialize data repository"

    # check no tables on master
    server_query repo1 0 "SHOW Tables" ""

    # make some changes to master and commit to branch test_branch
    multi_query repo1 0 "
    SET @@repo1_head=hashof('master');
    CREATE TABLE one_pk (
        pk BIGINT NOT NULL,
        c1 BIGINT,
        c2 BIGINT,
        PRIMARY KEY (pk)
    );
    INSERT INTO one_pk (pk) VALUES (0);
    INSERT INTO one_pk (pk,c1) VALUES (1,1);
    INSERT INTO one_pk (pk,c1,c2) VALUES (2,2,2),(3,3,3);
    SET @@repo1_head=commit('-m', 'test commit message');
    INSERT INTO dolt_branches (name,hash) VALUES ('test_branch', @@repo1_head);
    INSERT INTO one_pk (pk,c1,c2) VALUES (4,4,4),(5,5,5);
    SET @@repo1_head=commit('-m', 'second commit');
    INSERT INTO dolt_branches (name,hash) VALUES ('test_branch', @@repo1_head);
    "

    # validate new branch was created
    server_query repo1 0 "SELECT name,latest_commit_message FROM dolt_branches" "name,latest_commit_message\nmaster,Initialize data repository\ntest_branch,second commit"

    # validate no tables on master still
    server_query repo1 0 "SHOW tables" ""

    # Squash the test_branch into master even though it is a fast-forward merge.
    multi_query repo1 0 "
    SET @@repo1_working = squash('test_branch');
    SET @@repo1_head = COMMIT('-m', 'cm1');
    UPDATE dolt_branches SET hash = @@repo1_head WHERE name= 'master';"

    # Validate tables and data on master
    server_query repo1 0 "SET @@repo1_head=hashof('master');SHOW tables" ";Table\none_pk"
    server_query repo1 0 "SET @@repo1_head=hashof('master');SELECT * FROM one_pk ORDER by pk" ";pk,c1,c2\n0,None,None\n1,1,None\n2,2,2\n3,3,3\n4,4,4\n5,5,5"

    # Validate that the squash operations resulted in one commit to master than before
    server_query repo1 0 "SET @@repo1_head=hashof('master');select COUNT(*) from dolt_log" ";COUNT(*)\n2"

    # make some changes to master and commit. Make some changes to test_branch and commit. Squash/Merge.
    multi_query repo1 0 "
    SET @@repo1_head=hashof('master');
    UPDATE one_pk SET c1=10 WHERE pk=2;
    SET @@repo1_head=commit('-m', 'Change c 1 to 10');
    UPDATE dolt_branches SET hash = @@repo1_head WHERE name= 'master';

    SET @@repo1_head=hashof('test_branch');
    INSERT INTO one_pk (pk,c1,c2) VALUES (6,6,6);
    SET @@repo1_head=commit('-m', 'add 6');
    INSERT INTO one_pk (pk,c1,c2) VALUES (7,7,7);
    SET @@repo1_head=commit('-m', 'add 7');
    INSERT INTO dolt_branches (name,hash) VALUES ('test_branch', @@repo1_head);"

    # Validate that running a squash operation without updating the working variable itself alone does not
    # change the working root value
    server_query repo1 0 "SET @@repo1_head=hashof('master');SET @junk = squash('test_branch');SELECT * FROM one_pk ORDER by pk" ";;pk,c1,c2\n0,None,None\n1,1,None\n2,10,2\n3,3,3\n4,4,4\n5,5,5"

    multi_query repo1 0 "
    SET @@repo1_head=hashof('master');
    SET @@repo1_working = squash('test_branch');
    SET @@repo1_head = COMMIT('-m', 'cm2');
    UPDATE dolt_branches SET hash = @@repo1_head WHERE name= 'master';"

    # Validate tables and data on master
    server_query repo1 0 "SET @@repo1_head=hashof('master');SHOW tables" ";Table\none_pk"
    server_query repo1 0 "SET @@repo1_head=hashof('master');SELECT * FROM one_pk ORDER by pk" ";pk,c1,c2\n0,None,None\n1,1,None\n2,10,2\n3,3,3\n4,4,4\n5,5,5\n6,6,6\n7,7,7"

    # Validate that the squash operations resulted in one commit to master than before
    server_query repo1 0 "select COUNT(*) from dolt_log" "COUNT(*)\n4"

    # Validate the a squash commit was written by making sure the hashes of the two branches don't match
    server_query repo1 0 "select COUNT(hash) from dolt_branches where hash IN (select hash from dolt_branches WHERE name = 'test_branch')" "COUNT(hash)\n1"

    # check that squash with unknown branch throws an error
    run server_query repo1 0 "SET @@repo1_working = squash('fake');" ""
    [ "$status" -eq 1 ]

    # TODO: this throws an error on COMMIT because it has conflicts on the root it's trying to commit
    multi_query repo1 0 "
    SELECT DOLT_CHECKOUT('master');
    INSERT INTO one_pk values (8,8,8);"

    skip "Unclear behavior below here, need a simpler test of these assertions"
    
    # check that squash with uncommitted changes throws an error
    run server_query repo1 0 "SET @@repo1_working = squash('test_branch');" ""
    [ "$status" -eq 1 ]
}

@test "sql-server: test reset_hard" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    cd repo1
    dolt sql <<SQL
CREATE TABLE test (
    pk int PRIMARY KEY,
    c0 int
);
INSERT INTO test VALUES (1,1),(2,2),(3,3);
SQL
    dolt add -A && dolt commit -m "added table test"

    start_sql_server repo1

    # add some working changes
    server_query repo1 1 "INSERT INTO test VALUES (7,7);"
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false

    multi_query repo1 1 "
        SET @@repo1_head = reset('hard');
        REPLACE INTO dolt_branches (name,hash) VALUES ('master', @@repo1_head);"

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "working tree clean" ]] || false
    run dolt sql -q "SELECT sum(pk), sum(c0) FROM test;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "6,6" ]] || false

    multi_query repo1 1 "
        INSERT INTO test VALUES (8,8);
        SET @@repo1_head = reset('hard');
        REPLACE INTO dolt_branches (name,hash) VALUES ('master', @@repo1_head);"

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "working tree clean" ]] || false
    run dolt sql -q "SELECT sum(pk), sum(c0) FROM test;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "6,6" ]] || false
}

@test "sql-server: test multi db with use statements" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    start_multi_db_server repo1

    # create a table in repo1
    server_query repo1 1 "CREATE TABLE r1_one_pk (
        pk BIGINT NOT NULL COMMENT 'tag:0',
        c1 BIGINT COMMENT 'tag:1',
        c2 BIGINT COMMENT 'tag:2',
        PRIMARY KEY (pk)
    )" ""

    # create a table in repo2
    server_query repo1 1 "USE repo2; CREATE TABLE r2_one_pk (
        pk BIGINT NOT NULL COMMENT 'tag:0',
        c3 BIGINT COMMENT 'tag:1',
        c4 BIGINT COMMENT 'tag:2',
        PRIMARY KEY (pk)
    )" ";"

    # validate tables in repos
    server_query repo1 1 "SHOW tables" "Table\nr1_one_pk"
    server_query repo1 1 "USE repo2;SHOW tables" ";Table\nr2_one_pk"

    # put data in both
    multi_query repo1 1 "
    INSERT INTO r1_one_pk (pk) VALUES (0);
    INSERT INTO r1_one_pk (pk,c1) VALUES (1,1);
    INSERT INTO r1_one_pk (pk,c1,c2) VALUES (2,2,2),(3,3,3);
    USE repo2;
    INSERT INTO r2_one_pk (pk) VALUES (0);
    INSERT INTO r2_one_pk (pk,c3) VALUES (1,1);
    INSERT INTO r2_one_pk (pk,c3,c4) VALUES (2,2,2),(3,3,3)"

    server_query repo1 1 "SELECT * FROM repo1.r1_one_pk ORDER BY pk" "pk,c1,c2\n0,None,None\n1,1,None\n2,2,2\n3,3,3"
    server_query repo1 1 "SELECT * FROM repo2.r2_one_pk ORDER BY pk" "pk,c3,c4\n0,None,None\n1,1,None\n2,2,2\n3,3,3"

    multi_query repo1 1 "
    DELETE FROM r1_one_pk where pk=0;
    USE repo2;
    DELETE FROM r2_one_pk where pk=0"

    server_query repo1 1 "SELECT * FROM repo1.r1_one_pk ORDER BY pk" "pk,c1,c2\n1,1,None\n2,2,2\n3,3,3"
    server_query repo1 1 "SELECT * FROM repo2.r2_one_pk ORDER BY pk" "pk,c3,c4\n1,1,None\n2,2,2\n3,3,3"

    multi_query repo1 1 "
    UPDATE r1_one_pk SET c2=1 WHERE pk=1;
    USE repo2;
    UPDATE r2_one_pk SET c4=1 where pk=1"

    server_query repo1 1 "SELECT * FROM repo1.r1_one_pk ORDER BY pk" "pk,c1,c2\n1,1,1\n2,2,2\n3,3,3"
    server_query repo1 1 "SELECT * FROM repo2.r2_one_pk ORDER BY pk" "pk,c3,c4\n1,1,1\n2,2,2\n3,3,3"
}


@test "sql-server: test multi db without use statements" {
    skip "autocommit fails when the current db is not the one being written"
    start_multi_db_server repo1

    # create a table in repo1
    server_query repo1 1 "CREATE TABLE repo1.r1_one_pk (
        pk BIGINT NOT NULL COMMENT 'tag:0',
        c1 BIGINT COMMENT 'tag:1',
        c2 BIGINT COMMENT 'tag:2',
        PRIMARY KEY (pk)
    )" ""

    # create a table in repo2
    server_query repo1 1 "USE repo2; CREATE TABLE repo2.r2_one_pk (
        pk BIGINT NOT NULL COMMENT 'tag:0',
        c3 BIGINT COMMENT 'tag:1',
        c4 BIGINT COMMENT 'tag:2',
        PRIMARY KEY (pk)
    )" ";"

    # validate tables in repos
    server_query repo1 1 "SHOW tables" "Table\nr1_one_pk"
    server_query repo1 1 "USE repo2;SHOW tables" ";Table\nr2_one_pk"

    # put data in both
    multi_query repo1 1 "
    INSERT INTO repo1.r1_one_pk (pk) VALUES (0);
    INSERT INTO repo1.r1_one_pk (pk,c1) VALUES (1,1);
    INSERT INTO repo1.r1_one_pk (pk,c1,c2) VALUES (2,2,2),(3,3,3);
    USE repo2;
    INSERT INTO repo2.r2_one_pk (pk) VALUES (0);
    INSERT INTO repo2.r2_one_pk (pk,c3) VALUES (1,1);
    INSERT INTO repo2.r2_one_pk (pk,c3,c4) VALUES (2,2,2),(3,3,3)"

    server_query repo1 1 "SELECT * FROM repo1.r1_one_pk" "pk,c1,c2\n0,None,None\n1,1,None\n2,2,2\n3,3,3"
    server_query repo1 1 "SELECT * FROM repo2.r2_one_pk" "pk,c3,c4\n0,None,None\n1,1,None\n2,2,2\n3,3,3"

    multi_query repo1 1 "
    DELETE FROM repo1.r1_one_pk where pk=0;
    USE repo2;
    DELETE FROM repo2.r2_one_pk where pk=0"

    server_query repo1 1 "SELECT * FROM repo1.r1_one_pk" "pk,c1,c2\n1,1,None\n2,2,2\n3,3,3"
    server_query repo1 1 "SELECT * FROM repo2.r2_one_pk" "pk,c3,c4\n1,1,None\n2,2,2\n3,3,3"

    multi_query repo1 1 "
    UPDATE repo1.r1_one_pk SET c2=1 WHERE pk=1;
    USE repo2;
    UPDATE repo2.r2_one_pk SET c4=1 where pk=1"

    server_query repo1 1 "SELECT * FROM repo1.r1_one_pk" "pk,c1,c2\n1,1,1\n2,2,2\n3,3,3"
    server_query repo1 1 "SELECT * FROM repo2.r2_one_pk" "pk,c3,c4\n1,1,1\n2,2,2\n3,3,3"
}

@test "sql-server: test CREATE and DROP database via sql-server" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    cd repo1
    start_sql_server repo1

    multi_query repo1 1 "
    CREATE DATABASE memdb;
    USE memdb;
    CREATE TABLE pk(pk int primary key);
    INSERT INTO pk (pk) VALUES (0);
    "

    server_query repo1 1 "SELECT * FROM memdb.pk ORDER BY pk" "pk\n0"
    server_query repo1 1 "DROP DATABASE memdb" ""
    server_query repo1 1 "SHOW DATABASES" "Database\ninformation_schema\nrepo1"

}

@test "sql-server: DOLT_ADD, DOLT_COMMIT, DOLT_CHECKOUT, DOLT_MERGE work together in server mode" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

     cd repo1
     start_sql_server repo1

     multi_query repo1 1 "
     CREATE TABLE test (
         pk int primary key
     );
     INSERT INTO test VALUES (0),(1),(2);
     SELECT DOLT_ADD('.');
     SELECT DOLT_COMMIT('-a', '-m', 'Step 1');
     SELECT DOLT_CHECKOUT('-b', 'feature-branch');
     "

     server_query repo1 1 "SELECT * FROM test" "pk\n0\n1\n2"

     multi_query repo1 1 "
     SELECT DOLT_CHECKOUT('feature-branch');
     INSERT INTO test VALUES (3);
     INSERT INTO test VALUES (4);
     INSERT INTO test VALUES (21232);
     DELETE FROM test WHERE pk=4;
     UPDATE test SET pk=21 WHERE pk=21232;
     "

     server_query repo1 1 "SELECT * FROM test" "pk\n0\n1\n2"
     
     multi_query repo1 1 "
     SELECT DOLT_CHECKOUT('feature-branch');
     SELECT DOLT_COMMIT('-a', '-m', 'Insert 3');
     "
     
     multi_query repo1 1 "
     INSERT INTO test VALUES (500000);
     INSERT INTO test VALUES (500001);
     DELETE FROM test WHERE pk=500001;
     UPDATE test SET pk=60 WHERE pk=500000;
     SELECT DOLT_ADD('.');
     SELECT DOLT_COMMIT('-m', 'Insert 60');
     SELECT DOLT_MERGE('feature-branch');
     SELECT DOLT_COMMIT('-a', '-m', 'Finish up Merge');
     "
     
     server_query repo1 1 "SELECT * FROM test order by pk" "pk\n0\n1\n2\n3\n21\n60"

     run dolt status
     [ $status -eq 0 ]
     [[ "$output" =~ "nothing to commit, working tree clean" ]] || false
}

@test "sql-server: DOLT_MERGE ff works" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

     cd repo1
     start_sql_server repo1

     multi_query repo1 1 "
     CREATE TABLE test (
          pk int primary key
     );
     INSERT INTO test VALUES (0),(1),(2);
     SELECT DOLT_ADD('.');
     SELECT DOLT_COMMIT('-m', 'Step 1');
     SELECT DOLT_CHECKOUT('-b', 'feature-branch');
     INSERT INTO test VALUES (3);
     UPDATE test SET pk=1000 WHERE pk=0;
     SELECT DOLT_COMMIT('-a', '-m', 'this is a ff');
     SELECT DOLT_CHECKOUT('master');
     SELECT DOLT_MERGE('feature-branch');
     "

     server_query repo1 1 "SELECT * FROM test" "pk\n1\n2\n3\n1000"

     server_query repo1 1 "SELECT COUNT(*) FROM dolt_log" "COUNT(*)\n3"
}

@test "sql-server: LOAD DATA LOCAL INFILE works" {
    skip "LOAD DATA currently relies on setting secure_file_priv sys var which is incorrect"
     skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

     cd repo1
     start_sql_server repo1

     multi_query repo1 1 "
     CREATE TABLE test(pk int primary key, c1 int, c2 int, c3 int, c4 int, c5 int);
     SET local_infile=1;
     LOAD DATA LOCAL INFILE '$BATS_TEST_DIRNAME/helper/1pk5col-ints.csv' INTO TABLE test CHARACTER SET UTF8MB4 FIELDS TERMINATED BY ',' ESCAPED BY '' LINES TERMINATED BY '\n' IGNORE 1 LINES;
     "

     server_query repo1 1 "SELECT * FROM test" "pk,c1,c2,c3,c4,c5\n0,1,2,3,4,5\n1,1,2,3,4,5"
}

@test "sql-server: Run queries on database without ever selecting it" {
     skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

     start_multi_db_server repo1

     # create table with autocommit on and verify table creation
     unselected_server_query 1 "CREATE TABLE repo2.one_pk (
        pk int,
        PRIMARY KEY (pk)
      )" ""

     insert_query repo1 1 "INSERT INTO repo2.one_pk VALUES (0), (1), (2)"
     unselected_server_query 1 "SELECT * FROM repo2.one_pk" "pk\n0\n1\n2"

     unselected_update_query 1 "UPDATE repo2.one_pk SET pk=3 WHERE pk=2"
     unselected_server_query 1 "SELECT * FROM repo2.one_pk" "pk\n0\n1\n3"

     unselected_update_query 1 "DELETE FROM repo2.one_pk WHERE pk=3"
     unselected_server_query 1 "SELECT * FROM repo2.one_pk" "pk\n0\n1"

     # Empty commit statements should not error
     unselected_server_query 1 "commit"

     # create a new database and table and rerun
     unselected_server_query 1 "CREATE DATABASE testdb" ""
     unselected_server_query 1 "CREATE TABLE testdb.one_pk (
        pk int,
        PRIMARY KEY (pk)
      )" ""

     insert_query repo1 1 "INSERT INTO testdb.one_pk VALUES (0), (1), (2)"
     unselected_server_query 1 "SELECT * FROM testdb.one_pk" "pk\n0\n1\n2"

     unselected_update_query 1 "UPDATE testdb.one_pk SET pk=3 WHERE pk=2"
     unselected_server_query 1 "SELECT * FROM testdb.one_pk" "pk\n0\n1\n3"

     unselected_update_query 1 "DELETE FROM testdb.one_pk WHERE pk=3"
     unselected_server_query 1 "SELECT * FROM testdb.one_pk" "pk\n0\n1"

     # one last query on insert db.
     insert_query repo1 1 "INSERT INTO repo2.one_pk VALUES (4)"
     unselected_server_query 1 "SELECT * FROM repo2.one_pk" "pk\n0\n1\n4"
}

@test "sql-server: JSON queries" {
    cd repo1
    start_sql_server repo1

    # create table with autocommit on and verify table creation
    server_query repo1 1 "CREATE TABLE js_test (
        pk int NOT NULL,
        js json,
        PRIMARY KEY (pk)
    )" ""
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "js_test" ]] || false

    insert_query repo1 1 "INSERT INTO js_test VALUES (1, '{\"a\":1}');"
    server_query repo1 1 "SELECT * FROM js_test;" "pk,js\n1,{\"a\": 1}"
}

@test "sql-server: manual commit table can be dropped (validates superschema structure)" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    cd repo1
    start_sql_server repo1

    # check no tables on master
    server_query repo1 1 "SHOW Tables" ""

    # make some changes to master and commit to branch test_branch
    multi_query repo1 1 "
    SET @@repo1_head=hashof('master');
    CREATE TABLE one_pk (
        pk BIGINT NOT NULL,
        c1 BIGINT,
        c2 BIGINT,
        PRIMARY KEY (pk)
    );
    INSERT INTO one_pk (pk,c1,c2) VALUES (2,2,2),(3,3,3);
    SET @@repo1_head=commit('-m', 'test commit message', '--author', 'John Doe <john@example.com>');
    INSERT INTO dolt_branches (name,hash) VALUES ('master', @@repo1_head);"

    dolt add .
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "one_pk" ]] || false

    dolt sql -q "drop table one_pk"
    dolt commit -am "Dropped table one_pk"

    run dolt ls
    [ "$status" -eq 0 ]
    ! [[ "$output" =~ "one_pk" ]] || false
}

# TODO: Need to update testing logic allow queries for a multiple session.
@test "sql-server: Create a temporary table and validate that it doesn't persist after a session closes" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    cd repo1
    start_sql_server repo1

    # check no tables on master
    server_query repo1 1 "SHOW Tables" ""

    # Create a temporary table with some indexes
    server_query repo1 1 "CREATE TEMPORARY TABLE one_pk (
        pk int,
        c1 int,
        c2 int,
        PRIMARY KEY (pk),
        INDEX idx_v1 (c1, c2) COMMENT 'hello there'
    )" ""
    server_query repo1 1 "SHOW tables" "" # validate that it does have show tables
}

@test "sql-server: connect to another branch with connection string" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    cd repo1
    dolt checkout -b "feature-branch"
    dolt checkout master
    start_sql_server repo1

    server_query "repo1/feature-branch" 1 "CREATE TABLE test (
        pk int,
        c1 int,
        PRIMARY KEY (pk)
    )" ""

    server_query repo1 1 "SHOW tables" "" # no tables on master

    server_query "repo1/feature-branch" 1 "SHOW Tables" "Table\ntest"
}

@test "sql-server: connect to a commit with connection string" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    cd repo1
    dolt sql -q "create table test (pk int primary key)"
    dolt commit -a -m "Created new table"
    dolt sql -q "insert into test values (1), (2), (3)"
    dolt commit -a -m "Inserted 3 values"
    dolt sql -q "insert into test values (4), (5), (6)"
    dolt commit -a -m "Inserted 3 more values"

    start_sql_server repo1

    # get the second-to-last commit hash
    hash=`dolt log | grep commit | cut -d" " -f2 | tail -n+2 | head -n1`

    server_query "repo1/$hash" 1 "select count(*) from test" "count(*)\n3"

    # fails
    server_query "repo1/$hash" 1 "insert into test values (7)" "" "read-only"

    # server should still be alive after an error
    server_query "repo1/$hash" 1 "select count(*) from test" "count(*)\n3"
}

@test "sql-server: select a branch with the USE syntax" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    cd repo1
    dolt checkout -b "feature-branch"
    dolt checkout master
    start_sql_server repo1

    multi_query repo1 1 '
    USE `repo1/feature-branch`;
    CREATE TABLE test ( 
        pk int,
        c1 int,
        PRIMARY KEY (pk)
    )' ""

    server_query repo1 1 "SHOW tables" "" # no tables on master

    server_query "repo1/feature-branch" 1 "SHOW Tables" "Table\ntest"
}

@test "sql-server: SET GLOBAL default branch as ref" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    cd repo1
    dolt checkout -b "new"
    dolt checkout master
    start_sql_server repo1

    multi_query repo1 1 '
    select dolt_checkout("new");
    CREATE TABLE t (a int primary key, b int);
    INSERT INTO t VALUES (2,2),(3,3);' ""

    server_query repo1 1 "SHOW tables" "" # no tables on master
    server_query repo1 1 "set GLOBAL dolt_default_branch = 'refs/heads/new';" ""
    server_query repo1 1 "select @@GLOBAL.dolt_default_branch;" "@@GLOBAL.dolt_default_branch\nrefs/heads/new"
    server_query repo1 1 "select active_branch()" "active_branch()\nnew"
    server_query repo1 1 "SHOW tables" "Table\nt"
}

@test "sql-server: SET GLOBAL default branch as branch name" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    cd repo1
    dolt checkout -b "new"
    dolt checkout master
    start_sql_server repo1

    multi_query repo1 1 '
    select dolt_checkout("new");
    CREATE TABLE t (a int primary key, b int);
    INSERT INTO t VALUES (2,2),(3,3);' ""

    server_query repo1 1 "SHOW tables" "" # no tables on master
    server_query repo1 1 "set GLOBAL dolt_default_branch = 'new';" ""
    server_query repo1 1 "select @@GLOBAL.dolt_default_branch;" "@@GLOBAL.dolt_default_branch\nnew"
    server_query repo1 1 "select active_branch()" "active_branch()\nnew"
    server_query repo1 1 "SHOW tables" "Table\nt"
}

@test "sql-server: require_secure_transport no key or cert" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."
    cd repo1
    let PORT="$$ % (65536-1024) + 1024"
    cat >config.yml <<EOF
listener:
  require_secure_transport: true
EOF
    run dolt sql-server --host 0.0.0.0 --port=$PORT --user dolt --config ./config.yml
    [ "$status" -eq 1 ]
}

@test "sql-server: tls_key non-existant" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."
    cd repo1
    cp "$BATS_TEST_DIRNAME"/../../go/cmd/dolt/commands/sqlserver/testdata/chain_key.pem .
    cp "$BATS_TEST_DIRNAME"/../../go/cmd/dolt/commands/sqlserver/testdata/chain_cert.pem .
    let PORT="$$ % (65536-1024) + 1024"
    cat >config.yml <<EOF
listener:
  tls_cert: doesnotexist_cert.pem
  tls_key: chain_key.pem
EOF
    run dolt sql-server --host 0.0.0.0 --port=$PORT --user dolt --config ./config.yml
    [ "$status" -eq 1 ]
}

@test "sql-server: tls_cert non-existant" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."
    cd repo1
    cp "$BATS_TEST_DIRNAME"/../../go/cmd/dolt/commands/sqlserver/testdata/chain_key.pem .
    cp "$BATS_TEST_DIRNAME"/../../go/cmd/dolt/commands/sqlserver/testdata/chain_cert.pem .
    let PORT="$$ % (65536-1024) + 1024"
    cat >config.yml <<EOF
listener:
  tls_cert: chain_cert.pem
  tls_key: doesnotexist.pem
EOF
    run dolt sql-server --host 0.0.0.0 --port=$PORT --user dolt --config ./config.yml
    [ "$status" -eq 1 ]
}

@test "sql-server: tls only server" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."
    cd repo1
    cp "$BATS_TEST_DIRNAME"/../../go/cmd/dolt/commands/sqlserver/testdata/chain_key.pem .
    cp "$BATS_TEST_DIRNAME"/../../go/cmd/dolt/commands/sqlserver/testdata/chain_cert.pem .
    let PORT="$$ % (65536-1024) + 1024"
    cat >config.yml <<EOF
log_level: debug
user:
  name: dolt
listener:
  host: "0.0.0.0"
  port: $PORT
  tls_cert: chain_cert.pem
  tls_key: chain_key.pem
  require_secure_transport: true
EOF
    dolt sql-server --config ./config.yml &
    SERVER_PID=$!
    # We do things manually here because we need TLS support.
    python3 -c '
import mysql.connector
import sys
import time
i=0
while True:
  try:
    with mysql.connector.connect(host="127.0.0.1", user="dolt", port='"$PORT"', database="repo1", connection_timeout=1) as c:
      cursor = c.cursor()
      cursor.execute("show tables")
      for (t) in cursor:
        print(t)
      sys.exit(0)
  except mysql.connector.Error as err:
    if err.errno != 2003:
      raise err
    else:
      i += 1
      time.sleep(1)
      if i == 10:
        raise err
'
}

@test "sql-server: auto increment for a table should reset between drops" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    cd repo1
    start_sql_server repo1

    server_query repo1 1 "CREATE TABLE t1(pk int auto_increment primary key, val int)" ""
    insert_query repo1 1 "INSERT INTO t1 VALUES (0, 1),(0, 2)"
    server_query repo1 1 "SELECT * FROM t1" "pk,val\n1,1\n2,2"

    # drop the table and try again
    server_query repo1 1 "drop table t1;"
    server_query repo1 1 "CREATE TABLE t1(pk int auto_increment primary key, val int)" ""
    insert_query repo1 1 "INSERT INTO t1 VALUES (0, 1),(0, 2)"
    server_query repo1 1 "SELECT * FROM t1" "pk,val\n1,1\n2,2"
}
