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
    skiponwindows "tests are flaky on Windows"
    setup_no_dolt_init
    mkdir $BATS_TMPDIR/sql-server-test$$
    nativevar DOLT_ROOT_PATH $BATS_TMPDIR/sql-server-test$$ /p
    dolt config --global --add user.email "test@test.com"
    dolt config --global --add user.name "test"
    make_repo repo1
    make_repo repo2
}

teardown() {
    stop_sql_server
    teardown_common
}

@test "sql-server: server with no dbs yet should be able to clone" {
    # make directories outside of the existing init'ed dolt repos to ensure that
    # we are starting a sql-server with no existing dolt databases inside it
    tempDir=$(mktemp -d)
    cd $tempDir
    mkdir empty_server
    mkdir remote

    # create a file remote to clone later
    cd $BATS_TMPDIR/dolt-repo-$$/repo1
    dolt remote add remote01 file:///$tempDir/remote
    dolt push remote01 main

    # start the server and ensure there are no databases yet
    cd $tempDir/empty_server
    start_sql_server
    server_query "" 1 "show databases" "Database\ninformation_schema\nmysql"

    # verify that dolt_clone works
    # TODO: Once dolt_clone can be called without a selected database, this can be removed
    server_query "" 1 dolt "" "create database test01;" ""
    server_query "test01" 1 dolt "" "call dolt_clone('file:///$tempDir/remote');" "status\n0"
}

@test "sql-server: server assumes existing user" {
    cd repo1
    dolt sql -q "create user dolt@'%' identified by '123'"

    PORT=$( definePORT )
    dolt sql-server --port=$PORT --user dolt > log.txt 2>&1 &
    SERVER_PID=$!
    sleep 5

    dolt sql-client --host=0.0.0.0 --port=$PORT --user=dolt --password=wrongpassword <<< "exit;"
    run grep 'Error authenticating user using MySQL native password' log.txt
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]
}

@test "sql-server: Database specific system variables should be loaded" {
    cd repo1
    dolt branch dev
    dolt branch other

    start_sql_server
    server_query repo1 1 dolt "" "SET PERSIST repo1_default_branch = 'dev';" ""
    stop_sql_server
    start_sql_server
    server_query repo1 1 dolt "" "SELECT @@repo1_default_branch;" "@@SESSION.repo1_default_branch\ndev"
    stop_sql_server

    # system variable is lost when starting sql-server outside of the folder
    # because global config is used.
    cd ..
    start_sql_server
    server_query repo1 1 dolt "" "SELECT LENGTH(@@repo1_default_branch);" "LENGTH(@@repo1_default_branch)\n0"
    server_query repo1 1 dolt "" "SET PERSIST repo1_default_branch = 'other';" ""
    stop_sql_server
    start_sql_server
    server_query repo1 1 dolt "" "SELECT @@repo1_default_branch;" "@@SESSION.repo1_default_branch\nother"
    stop_sql_server

    # ensure we didn't blow away local setting
    cd repo1
    start_sql_server_with_args --user dolt --doltcfg-dir './'
    server_query repo1 1 dolt "" "SELECT @@repo1_default_branch;" "@@SESSION.repo1_default_branch\ndev"
}

@test "sql-server: user session variables from config" {
  cd repo1
  echo "
privilege_file: privs.json
user_session_vars:
- name: user0
  vars:
    aws_credentials_file: /Users/user0/.aws/config
    aws_credentials_profile: default
- name: user1
  vars:
    aws_credentials_file: /Users/user1/.aws/config
    aws_credentials_profile: lddev" > server.yaml

    dolt sql --privilege-file=privs.json -q "CREATE USER dolt@'127.0.0.1'"
    dolt sql --privilege-file=privs.json -q "CREATE USER user0@'127.0.0.1' IDENTIFIED BY 'pass0'"
    dolt sql --privilege-file=privs.json -q "CREATE USER user1@'127.0.0.1' IDENTIFIED BY 'pass1'"
    dolt sql --privilege-file=privs.json -q "CREATE USER user2@'127.0.0.1' IDENTIFIED BY 'pass2'"

    start_sql_server_with_config "" server.yaml

    run dolt sql-client --host=127.0.0.1 --port=$PORT --user=user0  --password=pass0<<SQL
SELECT @@aws_credentials_file, @@aws_credentials_profile;
SQL
    echo $output
    [[ "$output" =~ /Users/user0/.aws/config.*default ]] || false

    run dolt sql-client --host=127.0.0.1 --port=$PORT --user=user1 --password=pass1<<SQL
SELECT @@aws_credentials_file, @@aws_credentials_profile;
SQL
    echo $output
    [[ "$output" =~ /Users/user1/.aws/config.*lddev ]] || false

    run dolt sql-client --host=127.0.0.1 --port=$PORT --user=user2 --password=pass2<<SQL
SELECT @@aws_credentials_file, @@aws_credentials_profile;
SQL
    echo $output
    [[ "$output" =~ NULL.*NULL ]] || false

    run dolt sql-client --host=127.0.0.1 --port=$PORT --user=user2 --password=pass2<<SQL
SET @@aws_credentials_file="/Users/should_fail";
SQL
    echo $output
    [[ "$output" =~ "Variable 'aws_credentials_file' is a read only variable" ]] || false
}


@test "sql-server: test command line modification" {
    skiponwindows "Missing dependencies"

    cd repo1
    start_sql_server repo1

    # No tables at the start
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "No tables in working set" ]] || false

    server_query repo1 1 dolt "" "CREATE TABLE one_pk (
        pk BIGINT NOT NULL,
        c1 BIGINT,
        c2 BIGINT,
        PRIMARY KEY (pk)    )" ""
    run dolt ls

    [ "$status" -eq 0 ]
    [[ "$output" =~ "one_pk" ]] || false

    # Add rows on the command line
    run dolt sql --user=dolt -q "insert into one_pk values (1,1,1)"
    [ "$status" -eq 1 ]

    server_query repo1 1 dolt "" "SELECT * FROM one_pk ORDER by pk" ""

    # Test import as well (used by doltpy)
    echo 'pk,c1,c2' > import.csv
    echo '2,2,2' >> import.csv
    run dolt table import -u one_pk import.csv
    [ "$status" -eq 1 ]
    server_query repo1 1 "SELECT * FROM one_pk ORDER by pk" ""
}

@test "sql-server: test dolt sql interface works properly with autocommit" {
    skiponwindows "Missing dependencies"

    cd repo1
    start_sql_server repo1

    # No tables at the start
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "No tables in working set" ]] || false

    # create table with autocommit off and verify there are still no tables
    server_query repo1 0 dolt "" "CREATE TABLE one_pk (
        pk BIGINT NOT NULL COMMENT 'tag:0',
        c1 BIGINT COMMENT 'tag:1',
        c2 BIGINT COMMENT 'tag:2',
        PRIMARY KEY (pk)
    )" ""
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "No tables in working set" ]] || false

    # check that dolt_commit throws an error when there are no changes to commit
    server_query repo1 0 dolt "" "CALL DOLT_COMMIT('-a', '-m', 'Commit1')" 1

    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "No tables in working set" ]] || false

    # create table with autocommit on and verify table creation
    server_query repo1 1 dolt "" "CREATE TABLE one_pk (
        pk BIGINT NOT NULL COMMENT 'tag:0',
        c1 BIGINT COMMENT 'tag:1',
        c2 BIGINT COMMENT 'tag:2',
        PRIMARY KEY (pk)
    )" ""
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "one_pk" ]] || false

    dolt sql --user=dolt -q "CALL DOLT_ADD('.')"
    # check that dolt_commit works properly when autocommit is on
    run dolt sql --user=dolt -q "SELECT DOLT_COMMIT('-a', '-m', 'Commit1')"
    [ "$status" -eq 0 ]

    # check that dolt_commit throws error now that there are no working set changes.
    run dolt sql --user=dolt -q "SELECT DOLT_COMMIT('-a', '-m', 'Commit1')"
    [ "$status" -eq 1 ]

    # Make a change to the working set but not the staged set.
    run dolt sql --user=dolt -q "INSERT INTO one_pk (pk,c1,c2) VALUES (2,2,2),(3,3,3)"

    # check that dolt_commit throws error now that there are no staged changes.
    run dolt sql --user=dolt -q "SELECT DOLT_COMMIT('-m', 'Commit1')"
    [ "$status" -eq 1 ]

    run dolt log
    [ $status -eq 0 ]
    [[ "$output" =~ "Commit1" ]] || false
}

@test "sql-server: test reset_hard" {
    skiponwindows "Missing dependencies"

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
    server_query repo1 1 dolt "" "INSERT INTO test VALUES (7,7);"
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false

    server_query repo1 1 dolt "" "SELECT DOLT_RESET('--hard');"

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "working tree clean" ]] || false
    run dolt sql --user=dolt -q "SELECT sum(pk), sum(c0) FROM test;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "6,6" ]] || false

    server_query repo1 1 dolt "" "
        INSERT INTO test VALUES (8,8);
        SELECT DOLT_RESET('--hard');"

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "working tree clean" ]] || false
    run dolt sql --user=dolt -q "SELECT sum(pk), sum(c0) FROM test;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "6,6" ]] || false
}

@test "sql-server: test multi db with use statements" {
    skiponwindows "Missing dependencies"

    start_multi_db_server repo1

    # create a table in repo1
    server_query repo1 1 dolt "" "CREATE TABLE r1_one_pk (
        pk BIGINT NOT NULL COMMENT 'tag:0',
        c1 BIGINT COMMENT 'tag:1',
        c2 BIGINT COMMENT 'tag:2',
        PRIMARY KEY (pk)
    )" ""

    # create a table in repo2
    server_query repo1 1 dolt "" "USE repo2; CREATE TABLE r2_one_pk (
        pk BIGINT NOT NULL COMMENT 'tag:0',
        c3 BIGINT COMMENT 'tag:1',
        c4 BIGINT COMMENT 'tag:2',
        PRIMARY KEY (pk)
    )" ";"

    # validate tables in repos
    server_query repo1 1 dolt "" "SHOW tables" "Tables_in_repo1\nr1_one_pk"
    server_query repo1 1 dolt "" "USE repo2;SHOW tables" ";Tables_in_repo2\nr2_one_pk"

    # put data in both
    server_query repo1 1 dolt "" "
    INSERT INTO r1_one_pk (pk) VALUES (0);
    INSERT INTO r1_one_pk (pk,c1) VALUES (1,1);
    INSERT INTO r1_one_pk (pk,c1,c2) VALUES (2,2,2),(3,3,3);
    USE repo2;
    INSERT INTO r2_one_pk (pk) VALUES (0);
    INSERT INTO r2_one_pk (pk,c3) VALUES (1,1);
    INSERT INTO r2_one_pk (pk,c3,c4) VALUES (2,2,2),(3,3,3)"

    server_query repo1 1 dolt "" "SELECT * FROM repo1.r1_one_pk ORDER BY pk" "pk,c1,c2\n0,None,None\n1,1,None\n2,2,2\n3,3,3"
    server_query repo1 1 dolt "" "SELECT * FROM repo2.r2_one_pk ORDER BY pk" "pk,c3,c4\n0,None,None\n1,1,None\n2,2,2\n3,3,3"

    server_query repo1 1 dolt "" "
    DELETE FROM r1_one_pk where pk=0;
    USE repo2;
    DELETE FROM r2_one_pk where pk=0"

    server_query repo1 1 dolt "" "SELECT * FROM repo1.r1_one_pk ORDER BY pk" "pk,c1,c2\n1,1,None\n2,2,2\n3,3,3"
    server_query repo1 1 dolt "" "SELECT * FROM repo2.r2_one_pk ORDER BY pk" "pk,c3,c4\n1,1,None\n2,2,2\n3,3,3"

    server_query repo1 1 dolt "" "
    UPDATE r1_one_pk SET c2=1 WHERE pk=1;
    USE repo2;
    UPDATE r2_one_pk SET c4=1 where pk=1"

    server_query repo1 1 dolt "" "SELECT * FROM repo1.r1_one_pk ORDER BY pk" "pk,c1,c2\n1,1,1\n2,2,2\n3,3,3"
    server_query repo1 1 dolt "" "SELECT * FROM repo2.r2_one_pk ORDER BY pk" "pk,c3,c4\n1,1,1\n2,2,2\n3,3,3"
}

@test "sql-server: test multi db without use statements" {
    skip "autocommit fails when the current db is not the one being written"
    start_multi_db_server repo1

    # create a table in repo1
    server_query repo1 1 dolt "" "CREATE TABLE repo1.r1_one_pk (
        pk BIGINT NOT NULL COMMENT 'tag:0',
        c1 BIGINT COMMENT 'tag:1',
        c2 BIGINT COMMENT 'tag:2',
        PRIMARY KEY (pk)
    )" ""

    # create a table in repo2
    server_query repo1 1 dolt "" "USE repo2; CREATE TABLE repo2.r2_one_pk (
        pk BIGINT NOT NULL COMMENT 'tag:0',
        c3 BIGINT COMMENT 'tag:1',
        c4 BIGINT COMMENT 'tag:2',
        PRIMARY KEY (pk)
    )" ";"

    # validate tables in repos
    server_query repo1 1 dolt "" "SHOW tables" "Table\nr1_one_pk"
    server_query repo1 1 dolt "" "USE repo2;SHOW tables" ";Table\nr2_one_pk"

    # put data in both
    server_query repo1 1 dolt "" "
    INSERT INTO repo1.r1_one_pk (pk) VALUES (0);
    INSERT INTO repo1.r1_one_pk (pk,c1) VALUES (1,1);
    INSERT INTO repo1.r1_one_pk (pk,c1,c2) VALUES (2,2,2),(3,3,3);
    USE repo2;
    INSERT INTO repo2.r2_one_pk (pk) VALUES (0);
    INSERT INTO repo2.r2_one_pk (pk,c3) VALUES (1,1);
    INSERT INTO repo2.r2_one_pk (pk,c3,c4) VALUES (2,2,2),(3,3,3)"

    server_query repo1 1 dolt "" "SELECT * FROM repo1.r1_one_pk" "pk,c1,c2\n0,None,None\n1,1,None\n2,2,2\n3,3,3"
    server_query repo1 1 dolt "" "SELECT * FROM repo2.r2_one_pk" "pk,c3,c4\n0,None,None\n1,1,None\n2,2,2\n3,3,3"

    server_query repo1 1 dolt "" "
    DELETE FROM repo1.r1_one_pk where pk=0;
    USE repo2;
    DELETE FROM repo2.r2_one_pk where pk=0"

    server_query repo1 1 dolt "" "SELECT * FROM repo1.r1_one_pk" "pk,c1,c2\n1,1,None\n2,2,2\n3,3,3"
    server_query repo1 1 dolt "" "SELECT * FROM repo2.r2_one_pk" "pk,c3,c4\n1,1,None\n2,2,2\n3,3,3"

    server_query repo1 1 dolt "" "
    UPDATE repo1.r1_one_pk SET c2=1 WHERE pk=1;
    USE repo2;
    UPDATE repo2.r2_one_pk SET c4=1 where pk=1"

    server_query repo1 1 dolt "" "SELECT * FROM repo1.r1_one_pk" "pk,c1,c2\n1,1,1\n2,2,2\n3,3,3"
    server_query repo1 1 dolt "" "SELECT * FROM repo2.r2_one_pk" "pk,c3,c4\n1,1,1\n2,2,2\n3,3,3"
}

@test "sql-server: DOLT_ADD, DOLT_COMMIT, DOLT_CHECKOUT, DOLT_MERGE work together in server mode" {
    skiponwindows "Missing dependencies"

     cd repo1
     start_sql_server repo1

     server_query repo1 1 dolt "" "
     CREATE TABLE test (
         pk int primary key
     );
     INSERT INTO test VALUES (0),(1),(2);
     SELECT DOLT_ADD('.');
     SELECT DOLT_COMMIT('-a', '-m', 'Step 1');
     SELECT DOLT_CHECKOUT('-b', 'feature-branch');
     "

     server_query repo1 1 dolt "" "SELECT * FROM testorder by pk" "pk\n0\n1\n2"

     server_query repo1 1 dolt "" "
     SELECT DOLT_CHECKOUT('feature-branch');
     INSERT INTO test VALUES (3);
     INSERT INTO test VALUES (4);
     INSERT INTO test VALUES (21232);
     DELETE FROM test WHERE pk=4;
     UPDATE test SET pk=21 WHERE pk=21232;
     "

     server_query repo1 1 dolt "" "SELECT * FROM test" "pk\n0\n1\n2"

     server_query repo1 1 dolt "" "
     SELECT DOLT_CHECKOUT('feature-branch');
     SELECT DOLT_COMMIT('-a', '-m', 'Insert 3');
     "

     server_query repo1 1 dolt "" "
     INSERT INTO test VALUES (500000);
     INSERT INTO test VALUES (500001);
     DELETE FROM test WHERE pk=500001;
     UPDATE test SET pk=60 WHERE pk=500000;
     SELECT DOLT_ADD('.');
     SELECT DOLT_COMMIT('-m', 'Insert 60');
     SELECT DOLT_MERGE('feature-branch','-m','merge feature-branch');
     "

     server_query repo1 1 dolt "" "SELECT * FROM test order by pk" "pk\n0\n1\n2\n3\n21\n60"

     run dolt status
     [ $status -eq 0 ]
     [[ "$output" =~ "nothing to commit, working tree clean" ]] || false
}

@test "sql-server: DOLT_MERGE ff works" {
    skiponwindows "Missing dependencies"

     cd repo1
     start_sql_server repo1

     server_query repo1 1 dolt "" "
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
     SELECT DOLT_CHECKOUT('main');
     SELECT DOLT_MERGE('feature-branch');
     "

     server_query repo1 1 dolt "" "SELECT * FROM test ORDER BY pk" "pk\n1\n2\n3\n1000"

     server_query repo1 1 dolt "" "SELECT COUNT(*) FROM dolt_log" "COUNT(*)\n3"
}

@test "sql-server: Run queries on database without ever selecting it" {
     skiponwindows "Missing dependencies"

     start_multi_db_server repo1

     # create table with autocommit on and verify table creation
     server_query "" 1 dolt "" "CREATE TABLE repo2.one_pk (
        pk int,
        PRIMARY KEY (pk)
      )"

     server_query "" 1 dolt "" "INSERT INTO repo2.one_pk VALUES (0), (1), (2)"
     server_query "" 1 dolt "" "SELECT * FROM repo2.one_pk" "pk\n0\n1\n2"

     server_query "" 1 dolt "" "UPDATE repo2.one_pk SET pk=3 WHERE pk=2"
     server_query "" 1 dolt "" "SELECT * FROM repo2.one_pk" "pk\n0\n1\n3"

     server_query "" 1 dolt "" "DELETE FROM repo2.one_pk WHERE pk=3"
     server_query "" 1 dolt "" "SELECT * FROM repo2.one_pk" "pk\n0\n1"

     # Empty commit statements should not error
     server_query "" 1 dolt "" "commit"

     # create a new database and table and rerun
     server_query "" 1 dolt "" "CREATE DATABASE testdb" ""
     server_query "" 1 dolt "" "CREATE TABLE testdb.one_pk (
        pk int,
        PRIMARY KEY (pk)
      )" ""

     server_query "" 1 dolt "" "INSERT INTO testdb.one_pk VALUES (0), (1), (2)"
     server_query "" 1 dolt "" "SELECT * FROM testdb.one_pk" "pk\n0\n1\n2"

     server_query "" 1 dolt "" "UPDATE testdb.one_pk SET pk=3 WHERE pk=2"
     server_query "" 1 dolt "" "SELECT * FROM testdb.one_pk" "pk\n0\n1\n3"

     server_query "" 1 dolt "" "DELETE FROM testdb.one_pk WHERE pk=3"
     server_query "" 1 dolt "" "SELECT * FROM testdb.one_pk" "pk\n0\n1"

     # one last query on insert db.
     server_query "" 1 dolt "" "INSERT INTO repo2.one_pk VALUES (4)"
     server_query "" 1 dolt "" "SELECT * FROM repo2.one_pk" "pk\n0\n1\n4"

     # verify changes outside the session
     cd repo2
     run dolt sql --user=dolt -q "show tables"
     [ "$status" -eq 0 ]
     [[ "$output" =~ "one_pk" ]] || false

     run dolt sql --user=dolt -q "select * from one_pk"
     [ "$status" -eq 0 ]
     [[ "$output" =~ "0" ]] || false
     [[ "$output" =~ "1" ]] || false
     [[ "$output" =~ "4" ]] || false
}

@test "sql-server: create database without USE" {
     skiponwindows "Missing dependencies"

     start_multi_db_server repo1

     server_query "" 1 dolt "" "CREATE DATABASE newdb" ""
     server_query "" 1 dolt "" "CREATE TABLE newdb.test (a int primary key)" ""

     # verify changes outside the session
     cd newdb
     run dolt sql --user=dolt -q "show tables"
     [ "$status" -eq 0 ]
     [[ "$output" =~ "test" ]] || false
}

@test "sql-server: manual commit table can be dropped (validates superschema structure)" {
    skiponwindows "Missing dependencies"

    cd repo1
    start_sql_server repo1

    # check no tables on main
    server_query repo1 1 dolt "" "SHOW Tables" ""

    # make some changes to main and commit to branch test_branch
    server_query repo1 1 dolt "" "
    SET @@repo1_head_ref='main';
    CREATE TABLE one_pk (
        pk BIGINT NOT NULL,
        c1 BIGINT,
        c2 BIGINT,
        PRIMARY KEY (pk)
    );
    INSERT INTO one_pk (pk,c1,c2) VALUES (2,2,2),(3,3,3);
    CALL DOLT_ADD('.');
    SELECT commit('-am', 'test commit message', '--author', 'John Doe <john@example.com>');
    CALL DOLT_BRANCH('main', @@repo1_head);"

    server_query repo1 1 dolt "" "call dolt_add('.')" "status\n0"
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "one_pk" ]] || false

    run dolt sql --user=dolt -q "drop table one_pk"
    [ "$status" -eq 1 ]

    server_query repo1 1 dolt "" "drop table one_pk" ""
    server_query repo1 1 dolt "" "call dolt_commit('-am', 'Dropped table one_pk')"

    run dolt ls
    [ "$status" -eq 0 ]
    ! [[ "$output" =~ "one_pk" ]] || false
}

# TODO: Need to update testing logic allow queries for a multiple session.
@test "sql-server: Create a temporary table and validate that it doesn't persist after a session closes" {
    skiponwindows "Missing dependencies"

    cd repo1
    start_sql_server repo1

    # check no tables on main
    server_query repo1 1 dolt "" "SHOW Tables" ""

    # Create a temporary table with some indexes
    server_query repo1 1 dolt "" "CREATE TEMPORARY TABLE one_pk (
        pk int,
        c1 int,
        c2 int,
        PRIMARY KEY (pk),
        INDEX idx_v1 (c1, c2) COMMENT 'hello there'
    )" ""
    server_query repo1 1 dolt "" "SHOW tables" "" # validate that it does have show tables
}

@test "sql-server: connect to another branch with connection string" {
    skiponwindows "Missing dependencies"

    cd repo1
    dolt checkout -b "feature-branch"
    dolt checkout main
    start_sql_server repo1

    server_query "repo1/feature-branch" 1 dolt "" "CREATE TABLE test (
        pk int,
        c1 int,
        PRIMARY KEY (pk)
    )" ""

    server_query repo1 1 dolt "" "SHOW tables" "" # no tables on main

    server_query "repo1/feature-branch" 1 dolt "" "SHOW Tables" "Tables_in_repo1/feature-branch\ntest"
}

@test "sql-server: connect to a commit with connection string" {
    skiponwindows "Missing dependencies"

    cd repo1
    dolt sql -q "create table test (pk int primary key)"
    dolt add .
    dolt commit -a -m "Created new table"
    dolt sql -q "insert into test values (1), (2), (3)"
    dolt commit -a -m "Inserted 3 values"
    dolt sql -q "insert into test values (4), (5), (6)"
    dolt commit -a -m "Inserted 3 more values"

    start_sql_server repo1

    # get the second-to-last commit hash
    hash=`dolt log | grep commit | cut -d" " -f2 | tail -n+2 | head -n1`

    server_query "repo1/$hash" 1 dolt "" "select count(*) from test" "count(*)\n3"

    # fails
    server_query "repo1/$hash" 1 dolt "" "insert into test values (7)" "" "read-only"

    # server should still be alive after an error
    server_query "repo1/$hash" 1 dolt "" "select count(*) from test" "count(*)\n3"
}

@test "sql-server: SET GLOBAL default branch as ref" {
    skiponwindows "Missing dependencies"

    cd repo1
    dolt checkout -b "new"
    dolt checkout main
    start_sql_server repo1

    server_query repo1 1 dolt "" '
    select dolt_checkout("new");
    CREATE TABLE t (a int primary key, b int);
    INSERT INTO t VALUES (2,2),(3,3);' ""

    server_query repo1 1 dolt "" "SHOW tables" "" # no tables on main
    server_query repo1 1 dolt "" "set GLOBAL repo1_default_branch = 'refs/heads/new';" ""
    server_query repo1 1 dolt "" "select @@GLOBAL.repo1_default_branch;" "@@GLOBAL.repo1_default_branch\nrefs/heads/new"
    server_query repo1 1 dolt "" "select active_branch()" "active_branch()\nnew"
    server_query repo1 1 dolt "" "SHOW tables" "Tables_in_repo1\nt"
}

@test "sql-server: SET GLOBAL default branch as branch name" {
    skiponwindows "Missing dependencies"

    cd repo1
    dolt checkout -b "new"
    dolt checkout main
    start_sql_server repo1

    server_query repo1 1 dolt "" '
    select dolt_checkout("new");
    CREATE TABLE t (a int primary key, b int);
    INSERT INTO t VALUES (2,2),(3,3);' ""

    server_query repo1 1 dolt "" "SHOW tables" "" # no tables on main
    server_query repo1 1 dolt "" "set GLOBAL repo1_default_branch = 'new';" ""
    server_query repo1 1 dolt "" "select @@GLOBAL.repo1_default_branch;" "@@GLOBAL.repo1_default_branch\nnew"
    server_query repo1 1 dolt "" "select active_branch()" "active_branch()\nnew"
    server_query repo1 1 dolt "" "SHOW tables" "Tables_in_repo1\nt"
}

@test "sql-server: disable_client_multi_statements makes create trigger work" {
    skiponwindows "Missing dependencies"
    cd repo1
    dolt sql -q 'create table test (id int primary key)'
    PORT=$( definePORT )
    cat >config.yml <<EOF
log_level: debug
behavior:
  disable_client_multi_statements: true
user:
  name: dolt
listener:
  host: "0.0.0.0"
  port: $PORT
EOF
    dolt sql-server --config ./config.yml &
    SERVER_PID=$!
    # We do things manually here because we need to control CLIENT_MULTI_STATEMENTS.
    python3 -c '
import mysql.connector
import sys
import time
i=0
while True:
  try:
    with mysql.connector.connect(host="127.0.0.1", user="dolt", port='"$PORT"', database="repo1", connection_timeout=1) as c:
      cursor = c.cursor()
      cursor.execute("""
CREATE TRIGGER test_on_insert BEFORE INSERT ON test
FOR EACH ROW
BEGIN
  SIGNAL SQLSTATE '\''45000'\'' SET MESSAGE_TEXT = '\''You cannot insert into this table'\'';
END""")
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

@test "sql-server: client_multi_statements work" {
    skiponwindows "Missing dependencies"
    cd repo1
    dolt sql -q 'create table test (id int primary key)'
    PORT=$( definePORT )
    cat >config.yml <<EOF
log_level: debug
user:
  name: dolt
listener:
  host: "0.0.0.0"
  port: $PORT
EOF
    dolt sql-server --config ./config.yml &
    SERVER_PID=$!
    # We do things manually here because we need to control CLIENT_MULTI_STATEMENTS.
    python3 -c '
import mysql.connector
import sys
import time
i=0
while True:
  try:
    with mysql.connector.connect(host="127.0.0.1", user="dolt", port='"$PORT"', database="repo1", connection_timeout=1) as c:
      cursor = c.cursor()
      cursor.execute("""
CREATE TRIGGER test_on_insert BEFORE INSERT ON test
FOR EACH ROW
BEGIN
  SIGNAL SQLSTATE '\''45000'\'' SET MESSAGE_TEXT = '\''You cannot insert into this table'\'';
END""")
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

@test "sql-server: auto increment is globally distinct across branches and connections" {
    skiponwindows "Missing dependencies"

    cd repo1
    start_sql_server repo1

    server_query repo1 1 dolt "" "CREATE TABLE t1(pk bigint primary key auto_increment, val int)" ""
    server_query repo1 1 dolt "" "INSERT INTO t1 (val) VALUES (1)"
    server_query repo1 1 dolt "" "SELECT * FROM t1" "pk,val\n1,1"

    server_query repo1 1 dolt "" "INSERT INTO t1 (val) VALUES (2)"
    server_query repo1 1 dolt "" "SELECT * FROM t1" "pk,val\n1,1\n2,2"

    run server_query repo1 1 dolt "" "call dolt_add('.')"
    run server_query repo1 1 dolt "" "call dolt_commit('-am', 'table with two values')"
    run server_query repo1 1 dolt "" "call dolt_branch('new_branch')"

    server_query repo1/new_branch 1 dolt "" "INSERT INTO t1 (val) VALUES (3)"
    server_query repo1/new_branch 1 dolt "" "SELECT * FROM t1" "pk,val\n1,1\n2,2\n3,3"

    server_query repo1 1 dolt "" "INSERT INTO t1 (val) VALUES (4)"
    server_query repo1 1 dolt "" "SELECT * FROM t1" "pk,val\n1,1\n2,2\n4,4"
    
    # drop the table on main, should keep counting from 4
    server_query repo1 1 dolt "" "drop table t1;"
    server_query repo1 1 dolt "" "CREATE TABLE t1(pk bigint primary key auto_increment, val int)" ""
    server_query repo1 1 dolt "" "INSERT INTO t1 (val) VALUES (4)"
    server_query repo1 1 dolt "" "SELECT * FROM t1" "pk,val\n4,4"
}

@test "sql-server: sql-push --set-remote within session" {
    skiponwindows "Missing dependencies"

    mkdir rem1
    cd repo1
    dolt remote add origin file://../rem1
    start_sql_server repo1

    dolt push origin main
    run server_query repo1 1 dolt "" "select dolt_push() as p" "p\n0" 1
    [[ "$output" =~ "the current branch has no upstream branch" ]] || false

    server_query repo1 1 dolt "" "select dolt_push('--set-upstream', 'origin', 'main') as p" "p\n1"

    skip "In-memory branch doesn't track upstream correctly"
    server_query repo1 1 dolt "" "select dolt_push() as p" "p\n1"
}

@test "sql-server: replicate to backup after sql-session commit" {
    skiponwindows "Missing dependencies"

    mkdir bac1
    cd repo1
    dolt remote add backup1 file://../bac1
    dolt config --local --add sqlserver.global.DOLT_REPLICATE_TO_REMOTE backup1
    start_sql_server repo1

    server_query repo1 1 dolt "" "
    CREATE TABLE test (
      pk int primary key
    );
    INSERT INTO test VALUES (0),(1),(2);
    SELECT DOLT_ADD('.');
    SELECT DOLT_COMMIT('-m', 'Step 1');"

    cd ..
    dolt clone file://./bac1 repo3
    cd repo3
    run dolt sql -q "select * from test" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[0]}" =~ "pk" ]]
    [[ "${lines[1]}" =~ "0" ]]
    [[ "${lines[2]}" =~ "1" ]]
    [[ "${lines[3]}" =~ "2" ]]
}

@test "sql-server: create database with no starting repo" {
    skiponwindows "Missing dependencies"

    mkdir no_dolt && cd no_dolt
    start_sql_server

    server_query "" 1 dolt "" "create database test1"
    server_query "" 1 dolt "" "show databases" "Database\ninformation_schema\nmysql\ntest1"
    server_query "test1" 1 dolt "" "create table a(x int)"
    server_query "test1" 1 dolt "" "select dolt_add('.')"
    server_query "test1" 1 dolt "" "insert into a values (1), (2)"
    server_query "test1" 1 dolt "" "call dolt_commit('-a', '-m', 'new table a')"

    server_query "" 1 dolt "" "create database test2"
    server_query "test2" 1 dolt "" "create table b(x int)"
    server_query "test2" 1 dolt "" "select dolt_add('.')"
    server_query "test2" 1 dolt "" "insert into b values (1), (2)"
    server_query "test2" 1 dolt "" "select dolt_commit('-a', '-m', 'new table b')"

    cd test1
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "new table a" ]] || false

    run dolt sql --user=dolt -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "a" ]] || false

    cd ../test2
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "new table b" ]] || false

    run dolt sql --user=dolt -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "b" ]] || false

    cd ..

    server_query "" 1 dolt "" "create database test3"
    server_query "test3" 1 dolt "" "create table c(x int)"
    server_query "test3" 1 dolt "" "select dolt_add('.')"
    server_query "test3" 1 dolt "" "insert into c values (1), (2)"
    run server_query "test3" 1 dolt "" "select dolt_commit('-a', '-m', 'new table c')"

    server_query "" 1 dolt "" "drop database test2"

    [ -d test3 ]
    [ ! -d test2 ]

    # make sure the databases exist on restart
    stop_sql_server
    start_sql_server
    server_query "" 1 dolt "" "show databases" "Database\ninformation_schema\nmysql\ntest1\ntest3"
}

@test "sql-server: drop database with active connections" {
    skiponwindows "Missing dependencies"
    skip_nbf_dolt "json ordering of keys differs"

    mkdir no_dolt && cd no_dolt
    start_sql_server

    server_query "" 1 dolt "" "create database test1"
    server_query "" 1 dolt "" "create database test2"
    server_query "" 1 dolt "" "create database test3"

    server_query "" 1 dolt "" "show databases" "Database\ninformation_schema\nmysql\ntest1\ntest2\ntest3"
    server_query "test1" 1 dolt "" "create table a(x int)"
    server_query "test1" 1 dolt "" "select dolt_add('.')"
    server_query "test1" 1 dolt "" "insert into a values (1), (2)"
    run server_query "test1" 1 dolt "" "call dolt_commit('-a', '-m', 'new table a')"

    server_query "test2" 1 dolt "" "create table a(x int)"
    server_query "test2" 1 dolt "" "select dolt_add('.')"
    server_query "test2" 1 dolt "" "insert into a values (3), (4)"
    server_query "test2" 1 dolt "" "call dolt_commit('-a', '-m', 'new table a')"

    server_query "test3" 1 dolt "" "create table a(x int)"
    server_query "test3" 1 dolt "" "select dolt_add('.')"
    server_query "test3" 1 dolt "" "insert into a values (5), (6)"
    server_query "test3" 1 dolt "" "call dolt_commit('-a', '-m', 'new table a')"

    server_query "test1" 1 dolt "" "call dolt_checkout('-b', 'newbranch')"
    server_query "test1/newbranch" 1 dolt "" "select * from a" "x\n1\n2"

    server_query "test2" 1 dolt "" "call dolt_checkout('-b', 'newbranch')"
    server_query "test2/newbranch" 1 dolt "" "select * from a" "x\n3\n4"

    server_query "" 1 dolt "" "drop database TEST1"

    run server_query "test1/newbranch" 1 dolt "" "select * from a" "" 1
    [[ "$output" =~ "database not found" ]] || false

    # can't drop a branch-qualified database name
    run server_query "" 1 dolt "" "drop database \`test2/newbranch\`" "" 1
    [[ "$output" =~ "unable to drop revision database: test2/newbranch" ]] || false


    server_query "" 1 dolt "" "drop database TEST2"

    run server_query "test2/newbranch" 1 dolt "" "select * from a" "" 1
    [[ "$output" =~ "database not found" ]] || false

    server_query "test3" 1 dolt "" "select * from a" "x\n5\n6"
}

@test "sql-server: connect to databases case insensitive" {
    skiponwindows "Missing dependencies"

    mkdir no_dolt && cd no_dolt
    start_sql_server

    server_query "" 1 dolt "" "create database Test1"
    
    server_query "" 1 dolt "" "show databases" "Database\nTest1\ninformation_schema\nmysql"
    server_query "" 1 dolt "" "use test1; create table a(x int);"
    server_query "" 1 dolt "" "use TEST1; insert into a values (1), (2);"
    run server_query "" 1 dolt "" "use test1; select dolt_add('.'); select dolt_commit('-a', '-m', 'new table a');"
    server_query "" 1 dolt "" "use test1; call dolt_checkout('-b', 'newbranch');"
    server_query "" 1 dolt "" "use \`TEST1/newbranch\`; select * from a order by x" ";x\n1\n2"
    server_query "" 1 dolt "" "use \`test1/newbranch\`; select * from a order by x" ";x\n1\n2"
    server_query "" 1 dolt "" "use \`TEST1/NEWBRANCH\`" "" "database not found: TEST1/NEWBRANCH"

    server_query "" 1 dolt "" "create database test2; use test2; select database();" ";;database()\ntest2"
    server_query "" 1 dolt "" "use test2; drop database TEST2; select database();" ";;database()\nNone"
}

@test "sql-server: create and drop database with --data-dir" {
    skiponwindows "Missing dependencies"

    mkdir no_dolt && cd no_dolt
    mkdir db_dir
    start_sql_server_with_args --host 0.0.0.0 --user dolt --data-dir=db_dir

    server_query "" 1 dolt "" "create database test1"
    server_query "" 1 dolt "" "show databases" "Database\ninformation_schema\nmysql\ntest1"
    server_query "test1" 1 dolt "" "create table a(x int)"
    server_query "test1" 1 dolt "" "select dolt_add('.')"
    server_query "test1" 1 dolt "" "insert into a values (1), (2)"

    server_query "test1" 1 dolt "" "call dolt_commit('-a', '-m', 'new table a')"

    [ -d db_dir/test1 ]

    cd db_dir/test1
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "new table a" ]] || false

    cd ../..

    server_query "" 1 dolt "" "create database test3"
    server_query "test3" 1 dolt "" "create table c(x int)"
    server_query "test3" 1 dolt "" "select dolt_add('.')"
    server_query "test3" 1 dolt "" "insert into c values (1), (2)"
    server_query "test3" 1 dolt "" "call dolt_commit('-a', '-m', 'new table c')"

    server_query "" 1 dolt "" "drop database test1"

    [ -d db_dir/test3 ]
    [ ! -d db_dir/test1 ]

    # make sure the databases exist on restart
    stop_sql_server
    start_sql_server_with_args --host 0.0.0.0 --user dolt --data-dir=db_dir
    server_query "" 1 dolt "" "show databases" "Database\ninformation_schema\nmysql\ntest3"
}

@test "sql-server: create database errors" {
    skiponwindows "Missing dependencies"

    mkdir no_dolt && cd no_dolt
    mkdir dir_exists
    touch file_exists
    start_sql_server

    server_query "" 1 dolt "" "create database test1"

    # Error on creation, already exists
    server_query "" 1 dolt "" "create database test1" "" "exists"

    # Files / dirs in the way
    server_query "" 1 dolt "" "create database dir_exists" "" "exists"
    server_query "" 1 dolt "" "create database file_exists" "" "exists"
}

@test "sql-server: create database with existing repo" {
    skiponwindows "Missing dependencies"

    cd repo1
    start_sql_server

    server_query "" 1 dolt "" "create database test1"
    server_query "repo1" 1 dolt "" "show databases" "Database\ninformation_schema\nmysql\nrepo1\ntest1"
    server_query "test1" 1 dolt "" "create table a(x int)"
    server_query "test1" 1 dolt "" "select dolt_add('.')"
    server_query "test1" 1 dolt "" "insert into a values (1), (2)"

    # not bothering to check the results of the commit here
    server_query "test1" 1 dolt "" "call dolt_commit('-a', '-m', 'new table a')"

    server_query "" 1 dolt "" "create database test2"
    server_query "test2" 1 dolt "" "create table b(x int)"
    server_query "test2" 1 dolt "" "select dolt_add('.')"
    server_query "test2" 1 dolt "" "insert into b values (1), (2)"
    # not bothering to check the results of the commit here
    server_query "test2" 1 dolt "" "call dolt_commit('-a', '-m', 'new table b')"

    cd test1
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "new table a" ]] || false

    run dolt sql --user=dolt -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "a" ]] || false

    cd ../test2
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "new table b" ]] || false

    run dolt sql --user=dolt -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "b" ]] || false

    cd ../
    # make sure the databases exist on restart
    stop_sql_server
    start_sql_server
    server_query "" 1 dolt "" "show databases" "Database\ninformation_schema\nmysql\nrepo1\ntest1\ntest2"
}

@test "sql-server: fetch uses database tempdir from different working directory" {
    skiponwindows "Missing dependencies"

    mkdir remote1
    cd repo2
    dolt remote add remote1 file://../remote1
    dolt push -u remote1 main

    cd ..
    rm -rf repo1
    dolt clone file://./remote1 repo1
    cd repo1
    dolt remote add remote1 file://../remote1

    cd ../repo2
    dolt sql -q "create table test (a int)"
    dolt add .
    dolt commit -am "new commit"
    dolt push -u remote1 main

    cd ../repo1
    REPO_PATH=$(pwd)
    cd ..

    echo "
databases:
  - name: repo1
    path: $REPO_PATH
" > server.yaml

    start_sql_server_with_config repo1 server.yaml

    server_query repo1 1 dolt "" "call dolt_fetch() as f" "f\n1"
}

@test "sql-server: run mysql from shell" {
    skiponwindows "Has dependencies that are not installed on Windows CI"
    if [[ `uname` == 'Darwin' ]]; then
      skip "Unsupported in MacOS CI"
    fi

    cd repo1
    dolt sql -q "create table r1t_one (id1 int primary key, col1 varchar(20));"
    dolt sql -q "insert into r1t_one values (1,'aaaa'), (2,'bbbb'), (3,'cccc');"
    dolt sql -q "create table r1t_two (id2 int primary key, col2 varchar(20));"
    dolt add .
    dolt commit -am "create two tables"

    cd ../repo2
    dolt sql -q "create table r2t_one (id1 int primary key, col1 varchar(20));"
    dolt sql -q "create table r2t_two (id2 int primary key, col2 varchar(20));"
    dolt sql -q "create table r2t_three (id3 int primary key, col3 varchar(20));"
    dolt sql -q "insert into r2t_three values (4,'dddd'), (3,'gggg'), (2,'eeee'), (1,'ffff');"
    dolt add .
    dolt commit -am "create three tables"

    cd ..
    start_sql_server_with_args --user dolt -ltrace --no-auto-commit

    run expect $BATS_TEST_DIRNAME/sql-server-mysql.expect $PORT repo1
    [ "$status" -eq 0 ]
}

@test "sql-server: sql-server lock cleanup" {
    cd repo1
    start_sql_server
    stop_sql_server
    start_sql_server
    stop_sql_server
}

@test "sql-server: sql-server locks database" {
    cd repo1
    start_sql_server
    PORT=$( definePORT )
    run dolt sql-server -P $PORT
    [ "$status" -eq 1 ]
}

@test "sql-server: multi dir sql-server locks out childen" {
    start_sql_server
    cd repo2
    PORT=$( definePORT )
    run dolt sql-server -P $PORT
    [ "$status" -eq 1 ]
}

@test "sql-server: sql-server child locks out parent multi dir" {
    cd repo2
    start_sql_server
    cd ..
    PORT=$( definePORT )
    run dolt sql-server -P $PORT
    [ "$status" -eq 1 ]
}

@test "sql-server: sql-server lock for new databases" {
    cd repo1
    start_sql_server
    server_query repo1 1 dolt "" "create database newdb" ""
    cd newdb
    PORT=$( definePORT )
    run dolt sql-server -P $PORT
    [ "$status" -eq 1 ]
}

@test "sql-server: sql-server locks database to writes" {
    cd repo2
    dolt sql -q "create table a (x int primary key)" 
    start_sql_server
    run dolt sql -q "create table b (x int primary key)" 
    [ "$status" -eq 1 ]
    [[ "$output" =~ "database is locked to writes" ]] || false
    run dolt sql -q "insert into b values (0)"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "database is locked to writes" ]] || false
}

@test "sql-server: start server without socket flag should set default socket path" {
    skiponwindows "unix socket is not available on Windows"
    cd repo2
    DEFAULT_DB="repo2"
    PORT=$( definePORT )

    dolt sql-server --port $PORT --user dolt >> log.txt 2>&1 &
    SERVER_PID=$!
    wait_for_connection $PORT 5000

    server_query repo2 1 dolt "" "select 1 dolt ""as col1" "col1\n1"
    run grep '\"/tmp/mysql.sock\"' log.txt
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]

    run dolt sql-client --user=dolt <<< "exit;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "# Welcome to the Dolt MySQL client." ]] || false

    run dolt sql-client --host=0.0.0.0 --port=$PORT --user=dolt <<< "exit;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "# Welcome to the Dolt MySQL client." ]] || false
}

@test "sql-server: start server with socket option undefined should set default socket path" {
    skiponwindows "unix socket is not available on Windows"
    cd repo2
    DEFAULT_DB="repo2"
    PORT=$( definePORT )

    dolt sql-server --port $PORT --user dolt --socket > log.txt 2>&1 &
    SERVER_PID=$!
    wait_for_connection $PORT 5000

    server_query repo2 1 dolt "" "select 1 dolt ""as col1" "col1\n1"

    run grep '\"/tmp/mysql.sock\"' log.txt
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]
}

@test "sql-server: server fails to start up if there is already a file in the socket file path" {
    skiponwindows "unix socket is not available on Windows"
    cd repo2
    touch mysql.sock

    run pwd
    REPO_NAME=$output

    PORT=$( definePORT )
    dolt sql-server --port=$PORT --socket="$REPO_NAME/mysql.sock" --user dolt > log.txt 2>&1 &
    SERVER_PID=$!
    run wait_for_connection $PORT 5000
    [ "$status" -eq 1 ]

    run grep 'address already in use' log.txt
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]
}

@test "sql-server: start server with yaml config with socket file path defined" {
    skiponwindows "unix socket is not available on Windows"
    cd repo2
    DEFAULT_DB="repo2"
    PORT=$( definePORT )

    echo "
log_level: debug

user:
  name: dolt

listener:
  host: localhost
  port: $PORT
  max_connections: 10
  socket: /tmp/mysql.sock

behavior:
  autocommit: true" > server.yaml

    dolt sql-server --config server.yaml > log.txt 2>&1 &
    SERVER_PID=$!
    wait_for_connection $PORT 5000

    server_query repo2 1 dolt "" "select 1 dolt ""as col1" "col1\n1"

    run grep '\"/tmp/mysql.sock\"' log.txt
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]
}

@test "sql-server: start server multidir creates sql-server.lock file in every rep" {
    start_sql_server
    run ls repo1/.dolt
    [[ "$output" =~ "sql-server.lock" ]] || false

    run ls repo2/.dolt
    [[ "$output" =~ "sql-server.lock" ]] || false

    stop_sql_server
    run ls repo1/.dolt
    ! [[ "$output" =~ "sql-server.lock" ]] || false

    run ls repo2/.dolt
    ! [[ "$output" =~ "sql-server.lock" ]] || false
}

@test "sql-server: running a dolt function in the same directory as a running server correctly errors" {
    start_sql_server

    cd repo1
    run dolt commit --allow-empty --am "adasdasd"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "database locked by another sql-server; either clone the database to run a second server" ]] || false

    run dolt gc
    [ "$status" -eq 1 ]
    [[ "$output" =~ "database locked by another sql-server; either clone the database to run a second server" ]] || false

    echo "import socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind(('', 0))
addr = s.getsockname()
print(addr[1])
s.close()
" > port_finder.py

    PORT=$(python3 port_finder.py)
    run dolt sql-server --port=$PORT
    [ "$status" -eq 1 ]
    [[ "$output" =~ "database locked by another sql-server; either clone the database to run a second server" ]] || false

    stop_sql_server

    run dolt gc
    [ "$status" -eq 0 ]
}

@test "sql-server: sigterm running server and restarting works correctly" {
    skip "Skipping while we debug why this test hangs for hours in CI"

    start_sql_server
    run ls repo1/.dolt
    [[ "$output" =~ "sql-server.lock" ]] || false

    run ls repo2/.dolt
    [[ "$output" =~ "sql-server.lock" ]] || false

    kill -9 $SERVER_PID

    run ls repo1/.dolt
    [[ "$output" =~ "sql-server.lock" ]] || false

    run ls repo2/.dolt
    [[ "$output" =~ "sql-server.lock" ]] || false

    start_sql_server
    server_query repo1 1 dolt "" "SELECT 1" "1\n1"
    stop_sql_server

    # Try adding fake pid numbers. Could happen via debugger or something
    echo "423423" > repo1/.dolt/sql-server.lock
    echo "4123423" > repo2/.dolt/sql-server.lock

    start_sql_server
    server_query repo1 1 dolt "" "SELECT 1" "1\n1"
    stop_sql_server

    # Add malicious text to lockfile and expect to fail
    echo "iamamaliciousactor" > repo1/.dolt/sql-server.lock

    run start_sql_server
    [[ "$output" =~ "database locked by another sql-server; either clone the database to run a second server" ]] || false
    [ "$status" -eq 1 ]
}

@test "sql-server: create a database when no current database is set" {
    mkdir new_format && cd new_format
    run dolt init --new-format
    [ $status -eq 0 ]

    PORT=$( definePORT )
    dolt sql-server --host 0.0.0.0 --port=$PORT --user dolt &
    SERVER_PID=$! # will get killed by teardown_common
    sleep 5 # not using python wait so this works on windows

    dolt sql-client --host=0.0.0.0 --port=$PORT --user=dolt <<< "create database mydb1;"
    dolt sql-client --host=0.0.0.0 --port=$PORT --user=dolt <<< "exit;"
    [ -d mydb1 ]

    cd mydb1
    run dolt version
    [ "$status" -eq 0 ]
    [[ ! $output =~ "OLD ( __LD_1__ )" ]] || false
    [[ "$output" =~ "NEW ( __DOLT__ )" ]] || false
}

@test "sql-server: deleting database directory when a running server is using it does not panic" {
    skiponwindows "Missing dependencies"

    mkdir nodb
    cd nodb
    start_sql_server >> server_log.txt 2>&1

    server_query "" 1 dolt "" "CREATE DATABASE mydb1"
    server_query "" 1 dolt "" "CREATE DATABASE mydb2"

    [ -d mydb1 ]
    [ -d mydb2 ]

    rm -rf mydb2

    server_query "" 1 dolt "" "SHOW DATABASES" "" 1

    run grep "panic" server_log.txt
    [ "${#lines[@]}" -eq 0 ]

    run grep "failed to access 'mydb2' database: can no longer find .dolt dir on disk" server_log.txt
    [ "${#lines[@]}" -eq 1 ]

    # this tests fails sometimes as the server is stopped from the above error
    # but stop_sql_server in teardown tries to kill process that is not running anymore,
    # so start the server again, and it will be stopped in teardown
    start_sql_server
}

@test "sql-server: dropping database that the server is running in should drop only the db itself not its nested dbs" {
    skiponwindows "Missing dependencies"

    mkdir mydb
    cd mydb
    dolt init

    start_sql_server >> server_log.txt 2>&1

    # 'doltdb' will be nested database inside 'mydb'
    server_query "" 1 dolt "" "CREATE DATABASE doltdb"
    run dolt sql -q "SHOW DATABASES"
    [[ "$output" =~ "mydb" ]] || false
    [[ "$output" =~ "doltdb" ]] || false

    server_query "" 1 dolt "" "DROP DATABASE mydb"
    run grep "database not found: mydb" server_log.txt
    [ "${#lines[@]}" -eq 0 ]

    [ ! -d .dolt ]

    # nested databases inside dropped database should still exist
    run dolt sql -q "SHOW DATABASES"
    [[ "$output" =~ "doltdb" ]] || false
    [[ ! "$output" =~ "mydb" ]] || false
}

@test "sql-server: dropping database currently selected and that the server is running in" {
    skiponwindows "Missing dependencies"

    mkdir mydb
    cd mydb
    dolt init

    run dolt sql -q "SHOW DATABASES"
    [[ "$output" =~ "mydb" ]] || false

    start_sql_server >> server_log.txt 2>&1
    server_query "mydb" 1 dolt "" "DROP DATABASE mydb;"

    run grep "database not found: mydb" server_log.txt
    [ "${#lines[@]}" -eq 0 ]

    [ ! -d .dolt ]

    run dolt sql -q "SHOW DATABASES"
    [[ ! "$output" =~ "mydb" ]] || false
}

@test "sql-server: dropping database with '-' in it" {
    skiponwindows "Missing dependencies"

    mkdir my-db
    cd my-db
    dolt init
    cd ..

    start_sql_server >> server_log.txt 2>&1
    server_query "" 1 dolt "" "DROP DATABASE my_db;"

    run grep "database not found: my_db" server_log.txt
    [ "${#lines[@]}" -eq 0 ]

    [ ! -d my-db ]
}
