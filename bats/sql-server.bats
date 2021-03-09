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
    server_query 0 "CREATE TABLE one_pk (
        pk BIGINT NOT NULL COMMENT 'tag:0',
        c1 BIGINT COMMENT 'tag:1',
        c2 BIGINT COMMENT 'tag:2',
        PRIMARY KEY (pk)
    )" ""
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "No tables in working set" ]] || false

    # create table with autocommit on and verify table creation
    server_query 1 "CREATE TABLE one_pk (
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
    server_query 0 "CREATE TABLE one_pk (
        pk BIGINT NOT NULL COMMENT 'tag:0',
        c1 BIGINT COMMENT 'tag:1',
        c2 BIGINT COMMENT 'tag:2',
        PRIMARY KEY (pk)
    )" ""
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "No tables in working set" ]] || false

    # check that dolt_commit throws an error when autocommit is off
    run server_query 0 "SELECT DOLT_COMMIT('-a', '-m', 'Commit1')" ""
    [ "$status" -eq 1 ]

    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "No tables in working set" ]] || false

    # create table with autocommit on and verify table creation
    server_query 1 "CREATE TABLE one_pk (
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

    server_query 1 "SHOW tables" ""
    server_query 1 "CREATE TABLE one_pk (
        pk BIGINT NOT NULL COMMENT 'tag:0',
        c1 BIGINT COMMENT 'tag:1',
        c2 BIGINT COMMENT 'tag:2',
        PRIMARY KEY (pk)
    )" ""
    server_query 1 "SHOW tables" "Table\none_pk"
    insert_query 1 "INSERT INTO one_pk (pk) VALUES (0)"
    server_query 1 "SELECT * FROM one_pk ORDER BY pk" "pk,c1,c2\n0,None,None"
    insert_query 1 "INSERT INTO one_pk (pk,c1) VALUES (1,1)"
    insert_query 1 "INSERT INTO one_pk (pk,c1,c2) VALUES (2,2,2),(3,3,3)"
    server_query 1 "SELECT * FROM one_pk ORDER by pk" "pk,c1,c2\n0,None,None\n1,1,None\n2,2,2\n3,3,3"
    update_query 1 "UPDATE one_pk SET c2=c1 WHERE c2 is NULL and c1 IS NOT NULL"
}

@test "sql-server: test multiple queries on the same connection" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    cd repo1
    start_sql_server repo1

    multi_query 1 "CREATE TABLE one_pk (
        pk BIGINT NOT NULL COMMENT 'tag:0',
        c1 BIGINT COMMENT 'tag:1',
        c2 BIGINT COMMENT 'tag:2',
        PRIMARY KEY (pk)
    );
    INSERT INTO one_pk (pk) VALUES (0);
    INSERT INTO one_pk (pk,c1) VALUES (1,1);
    INSERT INTO one_pk (pk,c1,c2) VALUES (2,2,2),(3,3,3);"

    server_query 1 "SELECT * FROM one_pk ORDER by pk" "pk,c1,c2\n0,None,None\n1,1,None\n2,2,2\n3,3,3"
}

@test "sql-server: test manual commit" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    cd repo1
    start_sql_server repo1

    # check that only master branch exists
    server_query 0 "SELECT name, latest_commit_message FROM dolt_branches" "name,latest_commit_message\nmaster,Initialize data repository"

    # check that new connections are set to master by default
    server_query 0 "SELECT name, latest_commit_message FROM dolt_branches WHERE hash = @@repo1_head" "name,latest_commit_message\nmaster,Initialize data repository"

    # check no tables on master
    server_query 0 "SHOW Tables" ""

    # make some changes to master and commit to branch test_branch
    multi_query 0 "
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
    server_query 0 "SELECT name,latest_commit_message FROM dolt_branches" "name,latest_commit_message\nmaster,Initialize data repository\ntest_branch,test commit message"

    # Check that the author information is correct.
    server_query 0 "SELECT latest_committer,latest_committer_email FROM dolt_branches ORDER BY latest_commit_date DESC LIMIT 1" "latest_committer,latest_committer_email\nJohn Doe,john@example.com"

    # validate no tables on master still
    server_query 0 "SHOW tables" ""

    # validate tables and data on test_branch
    server_query 0 "SET @@repo1_head=hashof('test_branch');SHOW tables" ";Table\none_pk"
    server_query 0 "SET @@repo1_head=hashof('test_branch');SELECT * FROM one_pk ORDER by pk" ";pk,c1,c2\n0,None,None\n1,1,None\n2,2,2\n3,3,3"
}

@test "sql-server: test manual merge" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    cd repo1
    start_sql_server repo1

    # check that only master branch exists
    server_query 0 "SELECT name, latest_commit_message FROM dolt_branches" "name,latest_commit_message\nmaster,Initialize data repository"

    # check that new connections are set to master by default
    server_query 0 "SELECT name, latest_commit_message FROM dolt_branches WHERE hash = @@repo1_head" "name,latest_commit_message\nmaster,Initialize data repository"

    # check no tables on master
    server_query 0 "SHOW Tables" ""

    # make some changes to master and commit to branch test_branch
    multi_query 0 "
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
    server_query 0 "SELECT name,latest_commit_message FROM dolt_branches" "name,latest_commit_message\nmaster,Initialize data repository\ntest_branch,test commit message"

    # validate no tables on master still
    server_query 0 "SHOW tables" ""

    # Merge the test_branch into master. This should a fast forward merge.
    multi_query 0 "
    SET @@repo1_head = merge('test_branch');
    INSERT INTO dolt_branches (name, hash) VALUES('master', @@repo1_head);"

    # Validate tables and data on master
    server_query 0 "SET @@repo1_head=hashof('master');SHOW tables" ";Table\none_pk"
    server_query 0 "SET @@repo1_head=hashof('master');SELECT * FROM one_pk ORDER by pk" ";pk,c1,c2\n0,None,None\n1,1,None\n2,2,2\n3,3,3"

    # Validate the commit master matches that of test_branch (this is a fast forward) by matching commit hashes.
    server_query 0 "select COUNT(hash) from dolt_branches where hash IN (select hash from dolt_branches WHERE name = 'test_branch')" "COUNT(dolt_branches.hash)\n2"

    # make some changes to test_branch and commit. Make some changes to master and commit. Merge.
    multi_query 0 "
    SET @@repo1_head=hashof('master');
    UPDATE one_pk SET c1=10 WHERE pk=2;
    SET @@repo1_head=commit('-m', 'Change c 1 to 10');
    INSERT INTO dolt_branches (name,hash) VALUES ('master', @@repo1_head);

    SET @@repo1_head=hashof('test_branch');
    INSERT INTO one_pk (pk,c1,c2) VALUES (4,4,4);
    SET @@repo1_head=commit('-m', 'add 4');
    INSERT INTO dolt_branches (name,hash) VALUES ('test_branch', @@repo1_head);"

    multi_query 0 "
    SET @@repo1_head=hashof('master');
    SET @@repo1_head=merge('test_branch');
    INSERT INTO dolt_branches (name, hash) VALUES('master', @@repo1_head);"

    # Validate tables and data on master
    server_query 0 "SET @@repo1_head=hashof('master');SHOW tables" ";Table\none_pk"
    server_query 0 "SET @@repo1_head=hashof('master');SELECT * FROM one_pk ORDER by pk" ";pk,c1,c2\n0,None,None\n1,1,None\n2,10,2\n3,3,3\n4,4,4"

    # Validate the a merge commit was written by making sure the hashes of the two branches don't match
    server_query 0 "select COUNT(hash) from dolt_branches where hash IN (select hash from dolt_branches WHERE name = 'test_branch')" "COUNT(dolt_branches.hash)\n1"
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
    server_query 1 "INSERT INTO test VALUES (7,7);"
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false

    multi_query 1 "
        SET @@repo1_head = reset('hard');
        REPLACE INTO dolt_branches (name,hash) VALUES ('master', @@repo1_head);"

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "working tree clean" ]] || false
    run dolt sql -q "SELECT sum(pk), sum(c0) FROM test;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "6,6" ]] || false

    multi_query 1 "
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
    server_query 1 "CREATE TABLE r1_one_pk (
        pk BIGINT NOT NULL COMMENT 'tag:0',
        c1 BIGINT COMMENT 'tag:1',
        c2 BIGINT COMMENT 'tag:2',
        PRIMARY KEY (pk)
    )" ""

    # create a table in repo2
    server_query 1 "USE repo2; CREATE TABLE r2_one_pk (
        pk BIGINT NOT NULL COMMENT 'tag:0',
        c3 BIGINT COMMENT 'tag:1',
        c4 BIGINT COMMENT 'tag:2',
        PRIMARY KEY (pk)
    )" ";"

    # validate tables in repos
    server_query 1 "SHOW tables" "Table\nr1_one_pk"
    server_query 1 "USE repo2;SHOW tables" ";Table\nr2_one_pk"

    # put data in both
    multi_query 1 "
    INSERT INTO r1_one_pk (pk) VALUES (0);
    INSERT INTO r1_one_pk (pk,c1) VALUES (1,1);
    INSERT INTO r1_one_pk (pk,c1,c2) VALUES (2,2,2),(3,3,3);
    USE repo2;
    INSERT INTO r2_one_pk (pk) VALUES (0);
    INSERT INTO r2_one_pk (pk,c3) VALUES (1,1);
    INSERT INTO r2_one_pk (pk,c3,c4) VALUES (2,2,2),(3,3,3)"

    server_query 1 "SELECT * FROM repo1.r1_one_pk ORDER BY pk" "pk,c1,c2\n0,None,None\n1,1,None\n2,2,2\n3,3,3"
    server_query 1 "SELECT * FROM repo2.r2_one_pk ORDER BY pk" "pk,c3,c4\n0,None,None\n1,1,None\n2,2,2\n3,3,3"

    multi_query 1 "
    DELETE FROM r1_one_pk where pk=0;
    USE repo2;
    DELETE FROM r2_one_pk where pk=0"

    server_query 1 "SELECT * FROM repo1.r1_one_pk ORDER BY pk" "pk,c1,c2\n1,1,None\n2,2,2\n3,3,3"
    server_query 1 "SELECT * FROM repo2.r2_one_pk ORDER BY pk" "pk,c3,c4\n1,1,None\n2,2,2\n3,3,3"

    multi_query 1 "
    UPDATE r1_one_pk SET c2=1 WHERE pk=1;
    USE repo2;
    UPDATE r2_one_pk SET c4=1 where pk=1"

    server_query 1 "SELECT * FROM repo1.r1_one_pk ORDER BY pk" "pk,c1,c2\n1,1,1\n2,2,2\n3,3,3"
    server_query 1 "SELECT * FROM repo2.r2_one_pk ORDER BY pk" "pk,c3,c4\n1,1,1\n2,2,2\n3,3,3"
}


@test "sql-server: test multi db without use statements" {
    skip "autocommit fails when the current db is not the one being written"
    start_multi_db_server repo1

    # create a table in repo1
    server_query 1 "CREATE TABLE repo1.r1_one_pk (
        pk BIGINT NOT NULL COMMENT 'tag:0',
        c1 BIGINT COMMENT 'tag:1',
        c2 BIGINT COMMENT 'tag:2',
        PRIMARY KEY (pk)
    )" ""

    # create a table in repo2
    server_query 1 "USE repo2; CREATE TABLE repo2.r2_one_pk (
        pk BIGINT NOT NULL COMMENT 'tag:0',
        c3 BIGINT COMMENT 'tag:1',
        c4 BIGINT COMMENT 'tag:2',
        PRIMARY KEY (pk)
    )" ";"

    # validate tables in repos
    server_query 1 "SHOW tables" "Table\nr1_one_pk"
    server_query 1 "USE repo2;SHOW tables" ";Table\nr2_one_pk"

    # put data in both
    multi_query 1 "
    INSERT INTO repo1.r1_one_pk (pk) VALUES (0);
    INSERT INTO repo1.r1_one_pk (pk,c1) VALUES (1,1);
    INSERT INTO repo1.r1_one_pk (pk,c1,c2) VALUES (2,2,2),(3,3,3);
    USE repo2;
    INSERT INTO repo2.r2_one_pk (pk) VALUES (0);
    INSERT INTO repo2.r2_one_pk (pk,c3) VALUES (1,1);
    INSERT INTO repo2.r2_one_pk (pk,c3,c4) VALUES (2,2,2),(3,3,3)"

    server_query 1 "SELECT * FROM repo1.r1_one_pk" "pk,c1,c2\n0,None,None\n1,1,None\n2,2,2\n3,3,3"
    server_query 1 "SELECT * FROM repo2.r2_one_pk" "pk,c3,c4\n0,None,None\n1,1,None\n2,2,2\n3,3,3"

    multi_query 1 "
    DELETE FROM repo1.r1_one_pk where pk=0;
    USE repo2;
    DELETE FROM repo2.r2_one_pk where pk=0"

    server_query 1 "SELECT * FROM repo1.r1_one_pk" "pk,c1,c2\n1,1,None\n2,2,2\n3,3,3"
    server_query 1 "SELECT * FROM repo2.r2_one_pk" "pk,c3,c4\n1,1,None\n2,2,2\n3,3,3"

    multi_query 1 "
    UPDATE repo1.r1_one_pk SET c2=1 WHERE pk=1;
    USE repo2;
    UPDATE repo2.r2_one_pk SET c4=1 where pk=1"

    server_query 1 "SELECT * FROM repo1.r1_one_pk" "pk,c1,c2\n1,1,1\n2,2,2\n3,3,3"
    server_query 1 "SELECT * FROM repo2.r2_one_pk" "pk,c3,c4\n1,1,1\n2,2,2\n3,3,3"
}

@test "sql-server: test CREATE and DROP database via sql-server" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    cd repo1
    start_sql_server repo1

    multi_query 1 "
    CREATE DATABASE memdb;
    USE memdb;
    CREATE TABLE pk(pk int primary key);
    INSERT INTO pk (pk) VALUES (0);
    "

    server_query 1 "SELECT * FROM memdb.pk ORDER BY pk" "pk\n0"
    server_query 1 "DROP DATABASE memdb" ""
    server_query 1 "SHOW DATABASES" "Database\ninformation_schema\nrepo1"

}

@test "sql-server: DOLT_ADD, DOLT_COMMIT, DOLT_CHECKOUT, DOLT_MERGE work together in server mode" {
      skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

     cd repo1
     start_sql_server repo1


     multi_query 1 "
     CREATE TABLE test (
         pk int primary key
     );
     INSERT INTO test VALUES (0),(1),(2);
     SELECT DOLT_ADD('.');
     SELECT DOLT_COMMIT('-a', '-m', 'Step 1');
     SELECT DOLT_CHECKOUT('-b', 'feature-branch');
     "

     server_query 1 "SELECT * FROM test" "pk\n0\n1\n2"
     run dolt branch
     [ "$status" -eq 0 ]
     [[ "$output" =~ "* feature-branch" ]] || false

     multi_query 1 "
     INSERT INTO test VALUES (3);
     INSERT INTO test VALUES (4);
     INSERT INTO test VALUES (21232);
     DELETE FROM test WHERE pk=4;
     UPDATE test SET pk=21 WHERE pk=21232;
     "
     server_query 1 "SELECT * FROM test" "pk\n0\n1\n2\n3\n21"

     multi_query 1 "
     SELECT DOLT_COMMIT('-a', '-m', 'Insert 3');
     SELECT DOLT_CHECKOUT('master');
     "
     server_query 1 "SELECT * FROM test" "pk\n0\n1\n2"

     multi_query 1 "
     INSERT INTO test VALUES (500000);
     INSERT INTO test VALUES (500001);
     DELETE FROM test WHERE pk=500001;
     UPDATE test SET pk=60 WHERE pk=500000;
     SELECT DOLT_ADD('.');
     SELECT DOLT_COMMIT('-m', 'Insert 60');
     SELECT DOLT_MERGE('feature-branch');
     SELECT DOLT_COMMIT('-a', '-m', 'Finish up Merge');
     "
     server_query 1 "SELECT * FROM test" "pk\n0\n1\n2\n3\n21\n60"

     run dolt status
     [ $status -eq 0 ]
     [[ "$output" =~ "nothing to commit, working tree clean" ]] || false
}

@test "sql-server: DOLT_MERGE ff works" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

     cd repo1
     start_sql_server repo1

     multi_query 1 "
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

     server_query 1 "SELECT * FROM test" "pk\n1\n2\n3\n1000"

     server_query 1 "SELECT COUNT(*) FROM dolt_log" "COUNT(*)\n3"
}

@test "sql-server: LOAD DATA LOCAL INFILE works" {
     skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

     cd repo1
     start_sql_server repo1

     multi_query 1 "
     CREATE TABLE test(pk int primary key, c1 int, c2 int, c3 int, c4 int, c5 int);
     SET local_infile=1;
     LOAD DATA LOCAL INFILE '$BATS_TEST_DIRNAME/helper/1pk5col-ints.csv' INTO TABLE test CHARACTER SET UTF8MB4 FIELDS TERMINATED BY ',' ESCAPED BY '' LINES TERMINATED BY '\n' IGNORE 1 LINES;
     "

     server_query 1 "SELECT * FROM test" "pk,c1,c2,c3,c4,c5\n0,1,2,3,4,5\n1,1,2,3,4,5"
}