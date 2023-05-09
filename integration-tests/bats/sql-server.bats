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
    make_repo repo1
    make_repo repo2
}

teardown() {
    stop_sql_server 1 && sleep 0.5
    rm -rf $BATS_TMPDIR/sql-server-test$$
    teardown_common
}

@test "sql-server: sanity check" {
    cd repo1
    for i in {1..16};
    do
        dolt sql -q "create table t_$i (pk int primary key, c$i int)"
        dolt add -A
        dolt commit -m "new table t_$i"
    done
}

@test "sql-server: can create savepoint when no database is selected" {
    skiponwindows "Missing dependencies"

    mkdir my-db
    cd my-db
    dolt init
    cd ..

    SAVEPOINT_QUERY=$(cat <<'EOF'
START TRANSACTION;
SAVEPOINT tx1;
EOF
)

    start_sql_server >> server_log.txt 2>&1
    run dolt sql-client -P $PORT -u dolt --use-db '' -q "$SAVEPOINT_QUERY"
    [ $status -eq 0 ]
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
    run dolt sql-client -P $PORT -u dolt --use-db '' -q "show databases"
    [ $status -eq 0 ]
    [[ $output =~ information_schema ]] || false
    [[ $output =~ mysql ]] || false

    # verify that dolt_clone works
    dolt sql-client -P $PORT -u dolt --use-db '' -q "create database test01" ""
    dolt sql-client -P $PORT -u dolt --use-db 'test01' -q"call dolt_clone('file:///$tempDir/remote')" 
}

@test "sql-server: loglevels are case insensitive" {
    # assert that loglevel on command line is not case sensitive
    cd repo1
    PORT=$( definePORT )
    dolt sql-server --loglevel TrAcE --port=$PORT --user dolt --socket "dolt.$PORT.sock" > log.txt 2>&1 &
    SERVER_PID=$!
    wait_for_connection $PORT 5000
    dolt sql-client --host=0.0.0.0 -P $PORT -u dolt --use-db '' -q "show databases;"
    stop_sql_server

    # assert that loglevel in yaml config is not case sensitive
    cat >config.yml <<EOF
log_level: dEBuG
behavior:
  disable_client_multi_statements: true
user:
  name: dolt
listener:
  host: "0.0.0.0"
  port: $PORT
EOF
    dolt sql-server --config ./config.yml --socket "dolt.$PORT.sock" &
    SERVER_PID=$!
    wait_for_connection $PORT 5000
    dolt sql-client --host=0.0.0.0 -P $PORT -u dolt --use-db '' -q "show databases;"
    stop_sql_server
}

@test "sql-server: server assumes existing user" {
    cd repo1
    dolt sql -q "create user dolt@'%' identified by '123'"

    PORT=$( definePORT )
    dolt sql-server --port=$PORT --user dolt --socket "dolt.$PORT.sock" > log.txt 2>&1 &
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
    dolt sql-client -P $PORT -u dolt --use-db '' -q "SET PERSIST repo1_default_branch = 'dev'"
    stop_sql_server
    start_sql_server
    run dolt sql-client -P $PORT -u dolt --use-db repo1 -q "SELECT @@repo1_default_branch;"
    [ $status -eq 0 ]
    [[ $output =~ "@@SESSION.repo1_default_branch" ]] || false
    [[ $output =~ "dev" ]] || false
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

    dolt --privilege-file=privs.json sql -q "CREATE USER dolt@'127.0.0.1'"
    dolt --privilege-file=privs.json sql -q "CREATE USER user0@'127.0.0.1' IDENTIFIED BY 'pass0'"
    dolt --privilege-file=privs.json sql -q "CREATE USER user1@'127.0.0.1' IDENTIFIED BY 'pass1'"
    dolt --privilege-file=privs.json sql -q "CREATE USER user2@'127.0.0.1' IDENTIFIED BY 'pass2'"

    start_sql_server_with_config "" server.yaml

    run dolt sql-client --host=127.0.0.1 --port=$PORT --user=user0  --password=pass0<<SQL
SELECT @@aws_credentials_file, @@aws_credentials_profile;
SQL
    [[ "$output" =~ /Users/user0/.aws/config.*default ]] || false

    run dolt sql-client --host=127.0.0.1 --port=$PORT --user=user1 --password=pass1<<SQL
SELECT @@aws_credentials_file, @@aws_credentials_profile;
SQL
    [[ "$output" =~ /Users/user1/.aws/config.*lddev ]] || false

    run dolt sql-client --host=127.0.0.1 --port=$PORT --user=user2 --password=pass2<<SQL
SELECT @@aws_credentials_file, @@aws_credentials_profile;
SQL
    [[ "$output" =~ NULL.*NULL ]] || false

    run dolt sql-client --host=127.0.0.1 --port=$PORT --user=user2 --password=pass2<<SQL
SET @@aws_credentials_file="/Users/should_fail";
SQL
    [[ "$output" =~ "Variable 'aws_credentials_file' is a read only variable" ]] || false
}


@test "sql-server: inspect sql-server using CLI" {
    skiponwindows "Missing dependencies"

    cd repo1
    start_sql_server repo1

    # No tables at the start
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "No tables in working set" ]] || false

    dolt sql-client -P $PORT -u dolt -q "CREATE TABLE one_pk (
        pk BIGINT NOT NULL,
        c1 BIGINT,
        c2 BIGINT,
        PRIMARY KEY (pk))"

    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "one_pk" ]] || false

    # Add rows on the command line
    run dolt --user=dolt sql -q "insert into one_pk values (1,1,1)"
    [ "$status" -eq 1 ]

    run dolt sql-client -P $PORT -u dolt -q "SELECT * FROM one_pk"
    [ $status -eq 0 ]
    ! [[ $output =~ " 1 " ]] || false

    # Test import as well (used by doltpy)
    echo 'pk,c1,c2' > import.csv
    echo '2,2,2' >> import.csv
    run dolt table import -u one_pk import.csv
    [ "$status" -eq 1 ]
    
    run dolt sql-client -P $PORT -u dolt -q "SELECT * FROM one_pk"
    [ $status -eq 0 ]
    ! [[ $output =~ " 2 " ]] || false
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
    dolt sql-client -P $PORT -u dolt --no-auto-commit -q "" "CREATE TABLE one_pk (
        pk BIGINT NOT NULL,
        c1 BIGINT,
        c2 BIGINT,
        PRIMARY KEY (pk))"
    
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "No tables in working set" ]] || false

    # check that dolt_commit throws an error when there are no changes to commit
    run dolt sql-client -P $PORT -u dolt --no-auto-commit -q "CALL DOLT_COMMIT('-a', '-m', 'Commit1')"
    [ $status -ne 0 ]
    [[ "$output" =~ "nothing to commit" ]] || false

    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "No tables in working set" ]] || false

    # create table with autocommit on and verify table creation
    dolt sql-client -P $PORT -u dolt -q "CREATE TABLE one_pk (
        pk BIGINT NOT NULL COMMENT 'tag:0',
        c1 BIGINT COMMENT 'tag:1',
        c2 BIGINT COMMENT 'tag:2',
        PRIMARY KEY (pk)
    )"
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "one_pk" ]] || false

    dolt sql-client -P $PORT --user=dolt -q "CALL DOLT_ADD('.')"
    # check that dolt_commit works properly when autocommit is on
    run dolt sql-client -P $PORT --user=dolt -q "call dolt_commit('-a', '-m', 'Commit1')"
    [ "$status" -eq 0 ]

    # check that dolt_commit throws error now that there are no working set changes.
    run dolt sql-client -P $PORT --user=dolt -q "call dolt_commit('-a', '-m', 'Commit1')"
    [ "$status" -eq 1 ]

    # Make a change to the working set but not the staged set.
    run dolt sql-client -P $PORT --user=dolt -q "INSERT INTO one_pk (pk,c1,c2) VALUES (2,2,2),(3,3,3)"

    # check that dolt_commit throws error now that there are no staged changes.
    run dolt sql-client -P $PORT --user=dolt -q "call dolt_commit('-m', 'Commit1')"
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
    dolt sql-client -P $PORT -u dolt -q "INSERT INTO test VALUES (7,7);"
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false

    dolt sql-client -P $PORT -u dolt -q "CALL DOLT_RESET('--hard');"

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "working tree clean" ]] || false
    run dolt --user=dolt sql -q "SELECT sum(pk), sum(c0) FROM test;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "6,6" ]] || false

    dolt sql-client -P $PORT -u dolt -q "
        INSERT INTO test VALUES (8,8);
        CALL DOLT_RESET('--hard');"

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "working tree clean" ]] || false
    run dolt --user=dolt sql -q "SELECT sum(pk), sum(c0) FROM test;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "6,6" ]] || false
}

@test "sql-server: test multi db with use statements" {
    skiponwindows "Missing dependencies"

    start_multi_db_server repo1

    # create a table in repo1
    dolt sql-client -P $PORT -u dolt --use-db repo1 -q "CREATE TABLE r1_one_pk (
        pk BIGINT NOT NULL COMMENT 'tag:0',
        c1 BIGINT COMMENT 'tag:1',
        c2 BIGINT COMMENT 'tag:2',
        PRIMARY KEY (pk))"

    # create a table in repo2
    dolt sql-client -P $PORT -u dolt --use-db repo1 -q "USE repo2;
    CREATE TABLE r2_one_pk (
        pk BIGINT NOT NULL COMMENT 'tag:0',
        c3 BIGINT COMMENT 'tag:1',
        c4 BIGINT COMMENT 'tag:2',
        PRIMARY KEY (pk)
    )"

    # validate tables in repos
    run dolt sql-client -P $PORT -u dolt --use-db repo1 -q "SHOW tables"
    [ $status -eq 0 ]
    [[ $output =~ "r1_one_pk" ]] || false
    run dolt sql-client -P $PORT -u dolt --use-db repo1 -q "USE repo2; SHOW tables"
    [ $status -eq 0 ]
    [[ $output =~ "r2_one_pk" ]] || false

    # put data in both
    dolt sql-client -P $PORT -u dolt --use-db repo1 -q "
    INSERT INTO r1_one_pk (pk) VALUES (0);
    INSERT INTO r1_one_pk (pk,c1) VALUES (1,1);
    INSERT INTO r1_one_pk (pk,c1,c2) VALUES (2,2,2),(3,3,3);
    USE repo2;
    INSERT INTO r2_one_pk (pk) VALUES (0);
    INSERT INTO r2_one_pk (pk,c3) VALUES (1,1);
    INSERT INTO r2_one_pk (pk,c3,c4) VALUES (2,2,2),(3,3,3)"

    run dolt sql-client -P $PORT -u dolt --use-db repo1 --result-format csv -q "SELECT * FROM repo1.r1_one_pk ORDER BY pk"
    [ $status -eq 0 ]
    [[ $output =~ "0,," ]] || false
    [[ $output =~ "1,1," ]] || false
    [[ $output =~ "2,2,2" ]] || false
    [[ $output =~ "3,3,3" ]] || false

    run dolt sql-client -P $PORT -u dolt --use-db repo2 --result-format csv -q "SELECT * FROM repo2.r2_one_pk ORDER BY pk"
    [ $status -eq 0 ]
    [[ $output =~ "0,," ]] || false
    [[ $output =~ "1,1," ]] || false
    [[ $output =~ "2,2,2" ]] || false
    [[ $output =~ "3,3,3" ]] || false

    dolt sql-client -P $PORT -u dolt --use-db repo1 -q "
    DELETE FROM r1_one_pk where pk=0;
    USE repo2;
    DELETE FROM r2_one_pk where pk=0"

    run dolt sql-client -P $PORT -u dolt --use-db repo1 --result-format csv -q "SELECT * FROM repo1.r1_one_pk ORDER BY pk"
    [ $status -eq 0 ]
    ! [[ $output =~ "0,," ]] || false
    [[ $output =~ "1,1," ]] || false
    [[ $output =~ "2,2,2" ]] || false
    [[ $output =~ "3,3,3" ]] || false

    run dolt sql-client -P $PORT -u dolt --use-db repo2 --result-format csv -q "SELECT * FROM repo2.r2_one_pk ORDER BY pk"
    [ $status -eq 0 ]
    ! [[ $output =~ "0,," ]] || false
    [[ $output =~ "1,1," ]] || false
    [[ $output =~ "2,2,2" ]] || false
    [[ $output =~ "3,3,3" ]] || false
    
    dolt sql-client -P $PORT -u dolt --use-db repo1 -q "
    UPDATE r1_one_pk SET c2=1 WHERE pk=1;
    USE repo2;
    UPDATE r2_one_pk SET c4=1 where pk=1"

    run dolt sql-client -P $PORT -u dolt --use-db repo1 --result-format csv -q "SELECT * FROM repo1.r1_one_pk ORDER BY pk"
    [ $status -eq 0 ]
    echo $output
    ! [[ $output =~ "0,," ]] || false
    ! [[ $output =~ "1,1, " ]] || false
    [[ $output =~ "1,1,1" ]] || false
    [[ $output =~ "2,2,2" ]] || false
    [[ $output =~ "3,3,3" ]] || false

    run dolt sql-client -P $PORT -u dolt --use-db repo2 --result-format csv -q "SELECT * FROM repo2.r2_one_pk ORDER BY pk"
    [ $status -eq 0 ]
    ! [[ $output =~ "0,," ]] || false
    ! [[ $output =~ "1,1, " ]] || false
    [[ $output =~ "1,1,1" ]] || false
    [[ $output =~ "2,2,2" ]] || false
    [[ $output =~ "3,3,3" ]] || false
}

@test "sql-server: test multi db without use statements" {
    start_multi_db_server repo1

    # create a table in repo1
    dolt sql-client -P $PORT -u dolt --use-db repo1 -q "CREATE TABLE repo1.r1_one_pk (
        pk BIGINT NOT NULL COMMENT 'tag:0',
        c1 BIGINT COMMENT 'tag:1',
        c2 BIGINT COMMENT 'tag:2',
        PRIMARY KEY (pk))"

    # create a table in repo2
    dolt sql-client -P $PORT -u dolt --use-db repo1 -q "CREATE TABLE repo2.r2_one_pk (
        pk BIGINT NOT NULL COMMENT 'tag:0',
        c3 BIGINT COMMENT 'tag:1',
        c4 BIGINT COMMENT 'tag:2',
        PRIMARY KEY (pk)
    )"

    # validate tables in repos
    run dolt sql-client -P $PORT -u dolt --use-db repo1 -q "SHOW tables"
    [ $status -eq 0 ]
    [[ $output =~ "r1_one_pk" ]] || false
    run dolt sql-client -P $PORT -u dolt --use-db repo2 -q "SHOW tables"
    [ $status -eq 0 ]
    [[ $output =~ "r2_one_pk" ]] || false

    # put data in both using database scoped inserts
    dolt sql-client -P $PORT -u dolt --use-db repo1 -q "INSERT INTO repo1.r1_one_pk (pk) VALUES (0)" 
    dolt sql-client -P $PORT -u dolt --use-db repo1 -q "INSERT INTO repo1.r1_one_pk (pk,c1) VALUES (1,1)"
    dolt sql-client -P $PORT -u dolt --use-db repo1 -q "INSERT INTO repo1.r1_one_pk (pk,c1,c2) VALUES (2,2,2),(3,3,3)"
    
    dolt sql-client -P $PORT -u dolt --use-db repo1 -q "INSERT INTO repo2.r2_one_pk (pk) VALUES (0)"
    dolt sql-client -P $PORT -u dolt --use-db repo1 -q "INSERT INTO repo2.r2_one_pk (pk,c3) VALUES (1,1)"
    dolt sql-client -P $PORT -u dolt --use-db repo1 -q "INSERT INTO repo2.r2_one_pk (pk,c3,c4) VALUES (2,2,2),(3,3,3)"

    run dolt sql-client -P $PORT -u dolt --use-db repo1 --result-format csv -q "SELECT * FROM repo1.r1_one_pk ORDER BY pk"
    [ $status -eq 0 ]
    [[ $output =~ "0,," ]] || false
    [[ $output =~ "1,1," ]] || false
    [[ $output =~ "2,2,2" ]] || false
    [[ $output =~ "3,3,3" ]] || false

    run dolt sql-client -P $PORT -u dolt --use-db repo1 --result-format csv -q "SELECT * FROM repo2.r2_one_pk ORDER BY pk"
    [ $status -eq 0 ]
    [[ $output =~ "0,," ]] || false
    [[ $output =~ "1,1," ]] || false
    [[ $output =~ "2,2,2" ]] || false
    [[ $output =~ "3,3,3" ]] || false
    
    dolt sql-client -P $PORT -u dolt --use-db repo1 -q "DELETE FROM repo1.r1_one_pk where pk=0"
    dolt sql-client -P $PORT -u dolt --use-db repo1 -q "DELETE FROM repo2.r2_one_pk where pk=0"
    
    run dolt sql-client -P $PORT -u dolt --use-db repo1 --result-format csv -q "SELECT * FROM repo1.r1_one_pk ORDER BY pk"
    [ $status -eq 0 ]
    ! [[ $output =~ "0,," ]] || false
    [[ $output =~ "1,1," ]] || false
    [[ $output =~ "2,2,2" ]] || false
    [[ $output =~ "3,3,3" ]] || false

    run dolt sql-client -P $PORT -u dolt --use-db repo1 --result-format csv -q "SELECT * FROM repo2.r2_one_pk ORDER BY pk"
    [ $status -eq 0 ]
    ! [[ $output =~ "0,," ]] || false
    [[ $output =~ "1,1," ]] || false
    [[ $output =~ "2,2,2" ]] || false
    [[ $output =~ "3,3,3" ]] || false

    dolt sql-client -P $PORT -u dolt --use-db repo1 -q "UPDATE repo1.r1_one_pk SET c2=1 WHERE pk=1"
    dolt sql-client -P $PORT -u dolt --use-db repo1 -q "UPDATE repo2.r2_one_pk SET c4=1 where pk=1"

    run dolt sql-client -P $PORT -u dolt --use-db repo1 --result-format csv -q "SELECT * FROM repo1.r1_one_pk ORDER BY pk"
    [ $status -eq 0 ]
    echo $output
    ! [[ $output =~ "0,," ]] || false
    ! [[ $output =~ "1,1, " ]] || false
    [[ $output =~ "1,1,1" ]] || false
    [[ $output =~ "2,2,2" ]] || false
    [[ $output =~ "3,3,3" ]] || false

    run dolt sql-client -P $PORT -u dolt --use-db repo1 --result-format csv -q "SELECT * FROM repo2.r2_one_pk ORDER BY pk"
    [ $status -eq 0 ]
    ! [[ $output =~ "0,," ]] || false
    ! [[ $output =~ "1,1, " ]] || false
    [[ $output =~ "1,1,1" ]] || false
    [[ $output =~ "2,2,2" ]] || false
    [[ $output =~ "3,3,3" ]] || false
}

@test "sql-server: DOLT_ADD, DOLT_COMMIT, DOLT_CHECKOUT, DOLT_MERGE work together in server mode" {
    skiponwindows "Missing dependencies"

     cd repo1
     start_sql_server repo1

     dolt sql-client -P $PORT -u dolt --use-db repo1 -q "CREATE TABLE test (
         pk int primary key
     )"
     dolt sql-client -P $PORT -u dolt --use-db repo1 -q "INSERT INTO test VALUES (0),(1),(2)"
     dolt sql-client -P $PORT -u dolt --use-db repo1 -q "CALL DOLT_ADD('test')"
     dolt sql-client -P $PORT -u dolt --use-db repo1 -q "CALL DOLT_COMMIT('-a', '-m', 'Step 1')"
     dolt sql-client -P $PORT -u dolt --use-db repo1 -q "CALL DOLT_CHECKOUT('-b', 'feature-branch')"

     run dolt sql-client -P $PORT -u dolt --use-db repo1 -q "SELECT * FROM test"
     [ $status -eq 0 ]
     [[ $output =~ " 0 " ]] || false
     [[ $output =~ " 1 " ]] || false
     [[ $output =~ " 2 " ]] || false

     dolt sql-client -P $PORT -u dolt --use-db repo1 -q "
     CALL DOLT_CHECKOUT('feature-branch');
     INSERT INTO test VALUES (3);
     INSERT INTO test VALUES (4);
     INSERT INTO test VALUES (21232);
     DELETE FROM test WHERE pk=4;
     UPDATE test SET pk=21 WHERE pk=21232;
     "

     run dolt sql-client -P $PORT -u dolt --use-db repo1 -q "SELECT * FROM test"
     [ $status -eq 0 ]
     [[ $output =~ " 0 " ]] || false
     [[ $output =~ " 1 " ]] || false
     [[ $output =~ " 2 " ]] || false
     ! [[ $output =~ " 3 " ]] || false
     ! [[ $output =~ " 21 " ]] || false

     dolt sql-client -P $PORT -u dolt --use-db repo1 -q "
     CALL DOLT_CHECKOUT('feature-branch');
     CALL DOLT_COMMIT('-a', '-m', 'Insert 3');
     "

     dolt sql-client -P $PORT -u dolt --use-db repo1 -q "
     INSERT INTO test VALUES (500000);
     INSERT INTO test VALUES (500001);
     DELETE FROM test WHERE pk=500001;
     UPDATE test SET pk=60 WHERE pk=500000;
     CALL DOLT_ADD('.');
     CALL DOLT_COMMIT('-m', 'Insert 60');
     CALL DOLT_MERGE('feature-branch','-m','merge feature-branch');
     "

     run dolt sql-client -P $PORT -u dolt --use-db repo1 -q "SELECT * FROM test"
     [ $status -eq 0 ]
     [[ $output =~ " 0 " ]] || false
     [[ $output =~ " 1 " ]] || false
     [[ $output =~ " 2 " ]] || false
     [[ $output =~ " 3 " ]] || false
     [[ $output =~ " 21 " ]] || false
     [[ $output =~ " 60 " ]] || false

     run dolt status
     [ $status -eq 0 ]
     [[ "$output" =~ "nothing to commit, working tree clean" ]] || false
}

@test "sql-server: dolt_branch -d won't delete a db's default branch" {
    cd repo1
    dolt branch branch1
    start_sql_server repo1

    run dolt sql-client -P $PORT -u dolt --use-db repo1 \
      -q "CALL DOLT_CHECKOUT('branch1'); CALL DOLT_BRANCH('-D', 'main');"
    [ $status -eq 1 ]
    [[ $output =~ "default branch for database 'repo1'" ]] || false
}

@test "sql-server: DOLT_MERGE ff works" {
    skiponwindows "Missing dependencies"

     cd repo1
     start_sql_server repo1

     dolt sql-client -P $PORT -u dolt --use-db repo1 -q "
     CREATE TABLE test (
          pk int primary key
     );
     INSERT INTO test VALUES (0),(1),(2);
     call dolt_add('.');
     call dolt_commit('-m', 'Step 1');
     call dolt_checkout('-b', 'feature-branch');
     INSERT INTO test VALUES (3);
     UPDATE test SET pk=1000 WHERE pk=0;
     call dolt_commit('-a', '-m', 'this is a ff');
     call dolt_checkout('main');
     call dolt_merge('feature-branch');
     "

     run dolt sql-client -P $PORT -u dolt --use-db repo1 -q "SELECT * FROM test"
     [ $status -eq 0 ]
     echo $output
     [[ $output =~ " 1 " ]] || false
     [[ $output =~ " 2 " ]] || false
     [[ $output =~ " 3 " ]] || false
     [[ $output =~ " 1000 " ]] || false
     ! [[ $output =~ " 0 " ]] || false

     run dolt sql-client -P $PORT -u dolt --use-db repo1 -q "SELECT COUNT(*) FROM dolt_log"
     [ $status -eq 0 ]
     [[ $output =~ " 3 " ]] || false
}

@test "sql-server: Run queries on database without ever selecting it" {
     skiponwindows "Missing dependencies"

     start_multi_db_server repo1

     # create table with autocommit on and verify table creation
     dolt sql-client -P $PORT -u dolt --use-db '' -q "CREATE TABLE repo2.one_pk (
        pk int,
        PRIMARY KEY (pk))"

     dolt sql-client -P $PORT -u dolt --use-db '' -q "INSERT INTO repo2.one_pk VALUES (0), (1), (2)"
     run dolt sql-client -P $PORT -u dolt --use-db '' -q "SELECT * FROM repo2.one_pk"
     [ $status -eq 0 ]
     [[ $output =~ " 0 " ]] || false
     [[ $output =~ " 1 " ]] || false
     [[ $output =~ " 2 " ]] || false

     dolt sql-client -P $PORT -u dolt --use-db '' -q "UPDATE repo2.one_pk SET pk=3 WHERE pk=2"
     run dolt sql-client -P $PORT -u dolt --use-db '' -q "SELECT * FROM repo2.one_pk"
     [ $status -eq 0 ]
     [[ $output =~ " 0 " ]] || false
     [[ $output =~ " 1 " ]] || false
     [[ $output =~ " 3 " ]] || false
     ! [[ $output =~ " 2 " ]] || false

     dolt sql-client -P $PORT -u dolt --use-db '' -q "DELETE FROM repo2.one_pk WHERE pk=3"
     run dolt sql-client -P $PORT -u dolt --use-db '' -q "SELECT * FROM repo2.one_pk"
     [ $status -eq 0 ]
     [[ $output =~ " 0 " ]] || false
     [[ $output =~ " 1 " ]] || false
     ! [[ $output =~ " 3 " ]] || false

     # Empty commit statements should not error
     dolt sql-client -P $PORT -u dolt --use-db '' -q "commit"

     # create a new database and table and rerun
     dolt sql-client -P $PORT -u dolt --use-db '' -q "CREATE DATABASE testdb"
     dolt sql-client -P $PORT -u dolt --use-db '' -q "CREATE TABLE testdb.one_pk (
        pk int,
        PRIMARY KEY (pk))"

     dolt sql-client -P $PORT -u dolt --use-db '' -q "INSERT INTO testdb.one_pk VALUES (0), (1), (2)"
     run dolt sql-client -P $PORT -u dolt --use-db '' -q "SELECT * FROM testdb.one_pk"
     [ $status -eq 0 ]
     [[ $output =~ " 0 " ]] || false
     [[ $output =~ " 1 " ]] || false
     [[ $output =~ " 2 " ]] || false

     dolt sql-client -P $PORT -u dolt --use-db '' -q "UPDATE testdb.one_pk SET pk=3 WHERE pk=2"
     run dolt sql-client -P $PORT -u dolt --use-db '' -q "SELECT * FROM testdb.one_pk"
     [ $status -eq 0 ]
     [[ $output =~ " 0 " ]] || false
     [[ $output =~ " 1 " ]] || false
     [[ $output =~ " 3 " ]] || false
     ! [[ $output =~ " 2 " ]] || false

     dolt sql-client -P $PORT -u dolt --use-db '' -q "DELETE FROM testdb.one_pk WHERE pk=3"
     run dolt sql-client -P $PORT -u dolt --use-db '' -q "SELECT * FROM testdb.one_pk"
     [ $status -eq 0 ]
     [[ $output =~ " 0 " ]] || false
     [[ $output =~ " 1 " ]] || false
     ! [[ $output =~ " 3 " ]] || false

     # one last query on insert db.
     dolt sql-client -P $PORT -u dolt --use-db '' -q "INSERT INTO repo2.one_pk VALUES (4)"
     run dolt sql-client -P $PORT -u dolt --use-db '' -q "SELECT * FROM repo2.one_pk"
     [ $status -eq 0 ]
     [[ $output =~ " 0 " ]] || false
     [[ $output =~ " 1 " ]] || false
     [[ $output =~ " 4 " ]] || false

     # verify changes outside the session
     cd repo2
     run dolt --user=dolt sql -q "show tables"
     [ "$status" -eq 0 ]
     [[ "$output" =~ "one_pk" ]] || false

     run dolt --user=dolt sql -q "select * from one_pk"
     [ "$status" -eq 0 ]
     [[ "$output" =~ " 0 " ]] || false
     [[ "$output" =~ " 1 " ]] || false
     [[ "$output" =~ " 4 " ]] || false
}

@test "sql-server: create database without USE" {
     skiponwindows "Missing dependencies"

     start_multi_db_server repo1

     dolt sql-client -P $PORT -u dolt --use-db '' -q "CREATE DATABASE newdb" ""
     dolt sql-client -P $PORT -u dolt --use-db '' -q "CREATE TABLE newdb.test (a int primary key)" ""
     stop_sql_server 1

     # verify changes outside the session
     cd newdb
     run dolt --user=dolt sql -q "show tables"
     [ "$status" -eq 0 ]
     [[ "$output" =~ "test" ]] || false
}

@test "sql-server: manual commit table can be dropped (validates superschema structure)" {
    skiponwindows "Missing dependencies"

    cd repo1
    start_sql_server repo1

    # check no tables on main
    run dolt sql-client -P $PORT -u dolt --use-db repo1 -q "SHOW Tables"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 0 ]

    # make some changes to main and commit to branch test_branch
    dolt sql-client -P $PORT -u dolt --use-db repo1 -q "
    CALL DOLT_CHECKOUT('main');
    CREATE TABLE one_pk (
        pk BIGINT NOT NULL,
        c1 BIGINT,
        c2 BIGINT,
        PRIMARY KEY (pk)
    );
    INSERT INTO one_pk (pk,c1,c2) VALUES (2,2,2),(3,3,3);
    CALL DOLT_ADD('.');
    CALL dolt_commit('-am', 'test commit message', '--author', 'John Doe <john@example.com>');"
    
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "one_pk" ]] || false

    run dolt --user=dolt sql -q "drop table one_pk"
    [ "$status" -eq 1 ]

    dolt sql-client -P $PORT -u dolt --use-db repo1 -q "drop table one_pk"
    dolt sql-client -P $PORT -u dolt --use-db repo1 -q "call dolt_add('.')"
    dolt sql-client -P $PORT -u dolt --use-db repo1 -q "call dolt_commit('-am', 'Dropped table one_pk')"

    run dolt ls
    [ "$status" -eq 0 ]
    ! [[ "$output" =~ "one_pk" ]] || false
}

@test "sql-server: connect to another branch with connection string" {
    skiponwindows "Missing dependencies"

    cd repo1
    dolt checkout -b "feature-branch"
    dolt checkout main
    start_sql_server repo1

    dolt sql-client --use-db "repo1/feature-branch" -u dolt -P $PORT -q "CREATE TABLE test (
        pk int,
        c1 int,
        PRIMARY KEY (pk)
    )" ""

    run dolt sql-client -P $PORT -u dolt --use-db repo1 -q "SHOW Tables"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 0 ]

    run dolt sql-client --use-db "repo1/feature-branch" -u dolt -P $PORT -q "SHOW Tables"
    [ $status -eq 0 ]
    [[ $output =~ "feature-branch" ]] || false
    [[ $output =~ "test" ]] || false
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

    run dolt sql-client --use-db "repo1/$hash" -u dolt -P $PORT -q "select count(*) from test"
    [ $status -eq 0 ]
    [[ $output =~ " 3 " ]] || false

    # fails
    run dolt sql-client --use-db "repo1/$hash" -u dolt -P $PORT -q "insert into test values (7)"
    [ $status -ne 0 ]
    [[ $output =~ "read-only" ]] || false

    # server should still be alive after an error
    run dolt sql-client --use-db "repo1/$hash" -u dolt -P $PORT -q "select count(*) from test"
    [ $status -eq 0 ]
    [[ $output =~ " 3 " ]] || false
}

@test "sql-server: SET GLOBAL default branch as ref" {
    skiponwindows "Missing dependencies"

    cd repo1
    dolt checkout -b "new"
    dolt checkout main
    start_sql_server repo1

    run dolt sql-client -P $PORT -u dolt --use-db repo1 -q '
    CALL dolt_checkout("new");
    CREATE TABLE t (a int primary key, b int);
    INSERT INTO t VALUES (2,2),(3,3);'

    run dolt sql-client -P $PORT -u dolt --use-db repo1 -q "SHOW Tables"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 0 ]

    dolt sql-client -P $PORT -u dolt --use-db repo1 -q "set GLOBAL repo1_default_branch = 'refs/heads/new'"
    run dolt sql-client -P $PORT -u dolt --use-db repo1 -q "select @@GLOBAL.repo1_default_branch;"
    [ $status -eq 0 ]
    [[ $output =~ "refs/heads/new" ]] || false
    dolt sql-client -P $PORT -u dolt --use-db repo1 -q "select active_branch()"
    run dolt sql-client -P $PORT -u dolt --use-db repo1 -q "select active_branch()"
    [ $status -eq 0 ]
    [[ $output =~ "new" ]] || false
    run dolt sql-client -P $PORT -u dolt --use-db repo1 -q "SHOW Tables"
    [ $status -eq 0 ]
    [[ $output =~ " t " ]] || false
}

@test "sql-server: SET GLOBAL default branch as branch name" {
    skiponwindows "Missing dependencies"

    cd repo1
    dolt checkout -b "new"
    dolt checkout main
    start_sql_server repo1

    run dolt sql-client -P $PORT -u dolt --use-db repo1 -q '
    call dolt_checkout("new");
    CREATE TABLE t (a int primary key, b int);
    INSERT INTO t VALUES (2,2),(3,3);'

    run dolt sql-client -P $PORT -u dolt --use-db repo1 -q "SHOW Tables"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 0 ]

    dolt sql-client -P $PORT -u dolt --use-db repo1 -q "set GLOBAL repo1_default_branch = 'new'"
    run dolt sql-client -P $PORT -u dolt --use-db repo1 -q "select @@GLOBAL.repo1_default_branch;"
    [ $status -eq 0 ]
    [[ $output =~ " new " ]] || false
    run dolt sql-client -P $PORT -u dolt --use-db repo1 -q "select active_branch()"
    [ $status -eq 0 ]
    [[ $output =~ " new " ]] || false
    run dolt sql-client -P $PORT -u dolt --use-db repo1 -q "SHOW Tables"
    [ $status -eq 0 ]
    [[ $output =~ " t " ]] || false
    stop_sql_server 1
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
    dolt sql-server --config ./config.yml --socket "dolt.$PORT.sock" &
    SERVER_PID=$!
    sleep 1
    
    # We do things manually here because we need to control
    # CLIENT_MULTI_STATEMENTS.
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
    dolt sql-server --config ./config.yml --socket "dolt.$PORT.sock" &
    SERVER_PID=$!
    # We do things manually here because we need to control
    # CLIENT_MULTI_STATEMENTS.
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

    dolt sql-client -P $PORT -u dolt --use-db repo1 -q "CREATE TABLE t1(pk bigint primary key auto_increment, val int)"
    dolt sql-client -P $PORT -u dolt --use-db repo1 -q "INSERT INTO t1 (val) VALUES (1)"
    run dolt sql-client -P $PORT -u dolt --use-db repo1 --result-format csv -q "SELECT * FROM t1"
    [ $status -eq 0 ]
    [[ $output =~ "1,1" ]] || false

    dolt sql-client -P $PORT -u dolt --use-db repo1 -q "INSERT INTO t1 (val) VALUES (2)"
    run dolt sql-client -P $PORT -u dolt --use-db repo1 --result-format csv -q "SELECT * FROM t1"
    [ $status -eq 0 ]
    [[ $output =~ "1,1" ]] || false
    [[ $output =~ "2,2" ]] || false

    dolt sql-client -P $PORT -u dolt --use-db repo1 -q "call dolt_add('.')"
    dolt sql-client -P $PORT -u dolt --use-db repo1 -q "call dolt_commit('-am', 'table with two values')"
    dolt sql-client -P $PORT -u dolt --use-db repo1 -q "call dolt_branch('new_branch')"

    dolt sql-client --use-db repo1/new_branch -u dolt -P $PORT -q "INSERT INTO t1 (val) VALUES (3)"
    run dolt sql-client --use-db repo1/new_branch -u dolt -P $PORT --result-format csv -q "SELECT * FROM t1"
    [ $status -eq 0 ]
    [[ $output =~ "1,1" ]] || false
    [[ $output =~ "2,2" ]] || false
    [[ $output =~ "3,3" ]] || false

    dolt sql-client -P $PORT -u dolt --use-db repo1 -q "INSERT INTO t1 (val) VALUES (4)"
    run dolt sql-client -P $PORT -u dolt --use-db repo1 --result-format csv -q "SELECT * FROM t1"
    [ $status -eq 0 ]
    [[ $output =~ "1,1" ]] || false
    [[ $output =~ "2,2" ]] || false
    [[ $output =~ "4,4" ]] || false
    ! [[ $output =~ "3,3" ]] || false
    
    # drop the table on main, should keep counting from 4
    dolt sql-client -P $PORT -u dolt --use-db repo1 -q "drop table t1"
    dolt sql-client -P $PORT -u dolt --use-db repo1 -q "CREATE TABLE t1(pk bigint primary key auto_increment, val int)" ""
    dolt sql-client -P $PORT -u dolt --use-db repo1 -q "INSERT INTO t1 (val) VALUES (4)"
    run dolt sql-client -P $PORT -u dolt --use-db repo1 --result-format csv -q "SELECT * FROM t1"
    [[ $output =~ "4,4" ]] || false
    ! [[ $output =~ "1,1" ]] || false
    ! [[ $output =~ "2,2" ]] || false
    ! [[ $output =~ "3,3" ]] || false
}

@test "sql-server: sql-push --set-remote within session" {
    skiponwindows "Missing dependencies"

    mkdir rem1
    cd repo1
    dolt remote add origin file://../rem1
    dolt push origin main
    start_sql_server repo1

    run dolt sql-client -P $PORT -u dolt --use-db repo1 -q "call  dolt_push()"
    [ $status -ne 0 ]
    [[ "$output" =~ "the current branch has no upstream branch" ]] || false

    dolt sql-client -P $PORT -u dolt --use-db repo1 -q "call dolt_push('--set-upstream', 'origin', 'main')"

    dolt sql-client -P $PORT -u dolt --use-db repo1 -q "call dolt_push()"
}

@test "sql-server: replicate to backup after sql-session commit" {
    skiponwindows "Missing dependencies"

    mkdir bac1
    cd repo1
    dolt remote add backup1 file://../bac1
    dolt config --local --add sqlserver.global.DOLT_REPLICATE_TO_REMOTE backup1
    start_sql_server repo1

    dolt sql-client -P $PORT -u dolt --use-db repo1 -q "CREATE TABLE test (pk int primary key);"
    dolt sql-client -P $PORT -u dolt --use-db repo1 -q "INSERT INTO test VALUES (0),(1),(2)"
    dolt sql-client -P $PORT -u dolt --use-db repo1 -q "CALL DOLT_ADD('.')"
    dolt sql-client -P $PORT -u dolt --use-db repo1 -q "CALL DOLT_COMMIT('-m', 'Step 1');"
    stop_sql_server 1

    cd ..
    dolt clone file://./bac1 repo3
    cd repo3
    run dolt sql -q "select * from test" -r csv
    [ "$status" -eq 0 ]
    [ "${lines[0]}" = "pk" ] 
    [ "${lines[1]}" = "0" ]
    [ "${lines[2]}" = "1" ]
    [ "${lines[3]}" = "2" ]
}

@test "sql-server: create multiple databases with no starting repo" {
    skiponwindows "Missing dependencies"

    mkdir no_dolt && cd no_dolt
    start_sql_server

    dolt sql-client -P $PORT -u dolt --use-db '' -q "create database test1"
    run dolt sql-client -P $PORT -u dolt --use-db '' -q "show databases"
    [ $status -eq 0 ]
    [[ $output =~ "mysql" ]] || false
    [[ $output =~ "information_schema" ]] || false
    [[ $output =~ "test1" ]] ||	false

    # Make sure the sql-server lock file is set for a newly created database
    [[ -f "$PWD/test1/.dolt/sql-server.lock" ]] || false

    dolt sql-client -P $PORT -u dolt --use-db 'test1' -q "create table a(x int)"
    dolt sql-client -P $PORT -u dolt --use-db 'test1' -q "call dolt_add('.')"
    dolt sql-client -P $PORT -u dolt --use-db 'test1' -q "insert into a values (1), (2)"
    dolt sql-client -P $PORT -u dolt --use-db 'test1' -q "call dolt_commit('-a', '-m', 'new table a')"

    dolt sql-client -P $PORT -u dolt --use-db '' -q "create database test2"
    dolt sql-client -P $PORT -u dolt --use-db 'test2' -q "create table b(x int)"
    dolt sql-client -P $PORT -u dolt --use-db 'test2' -q "call dolt_add('.')"
    dolt sql-client -P $PORT -u dolt --use-db 'test2' -q "insert into b values (1), (2)"
    dolt sql-client -P $PORT -u dolt --use-db 'test2' -q "call dolt_commit('-a', '-m', 'new table b')"
    stop_sql_server 1

    cd test1
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "new table a" ]] || false

    run dolt --user=dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "a" ]] || false

    cd ../test2
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "new table b" ]] || false

    run dolt --user=dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "b" ]] || false

    cd ..

    start_sql_server
    dolt sql-client -P $PORT -u dolt --use-db '' -q "create database test3"
    dolt sql-client -P $PORT -u dolt --use-db 'test3' -q "create table c(x int)"
    dolt sql-client -P $PORT -u dolt --use-db 'test3' -q "call dolt_add('.')"
    dolt sql-client -P $PORT -u dolt --use-db 'test3' -q "insert into c values (1), (2)"
    dolt sql-client -P $PORT -u dolt --use-db 'test3' -q "call dolt_commit('-a', '-m', 'new table c')"
    dolt sql-client -P $PORT -u dolt --use-db '' -q "drop database test2"

    [ -d test3 ]
    [ ! -d test2 ]

    # make sure the databases exist on restart
    stop_sql_server
    start_sql_server
    run dolt sql-client -P $PORT -u dolt --use-db '' -q "show databases"
    [ $status -eq 0 ]
    [[ $output =~ "mysql" ]] || false
    [[ $output =~ "information_schema" ]] || false
    [[ $output =~ "test1" ]] || false
    [[ $output =~ "test3" ]] || false
    ! [[ $output =~ "test2" ]] || false
}

@test "sql-server: can't drop branch qualified database names" {
    skiponwindows "Missing dependencies"

    mkdir no_dolt && cd no_dolt
    start_sql_server

    dolt sql-client -P $PORT -u dolt --use-db '' -q "create database test1"
    dolt sql-client -P $PORT -u dolt --use-db '' -q "create database test2"
    dolt sql-client -P $PORT -u dolt --use-db '' -q "create database test3"

    run dolt sql-client -P $PORT -u dolt --use-db '' -q "show databases"
    [ $status -eq 0 ]
    [[ $output =~ "mysql" ]] || false
    [[ $output =~ "information_schema" ]] || false
    [[ $output =~ "test1" ]] || false
    [[ $output =~ "test2" ]] || false
    [[ $output =~ "test3" ]] || false

    dolt sql-client -P $PORT -u dolt --use-db test1 -q "create table a(x int)"
    dolt sql-client -P $PORT -u dolt --use-db test1 -q "call dolt_add('.')"
    dolt sql-client -P $PORT -u dolt --use-db test1 -q "insert into a values (1), (2)"
    dolt sql-client -P $PORT -u dolt --use-db test1 -q "call dolt_commit('-a', '-m', 'new table a')"

    dolt sql-client -P $PORT -u dolt --use-db test2 -q "create table a(x int)"
    dolt sql-client -P $PORT -u dolt --use-db test2 -q "call dolt_add('.')"
    dolt sql-client -P $PORT -u dolt --use-db test2 -q "insert into a values (3), (4)"
    dolt sql-client -P $PORT -u dolt --use-db test2 -q "call dolt_commit('-a', '-m', 'new table a')"

    dolt sql-client -P $PORT -u dolt --use-db test3 -q "create table a(x int)"
    dolt sql-client -P $PORT -u dolt --use-db test3 -q "call dolt_add('.')"
    dolt sql-client -P $PORT -u dolt --use-db test3 -q "insert into a values (5), (6)"
    dolt sql-client -P $PORT -u dolt --use-db test3 -q "call dolt_commit('-a', '-m', 'new table a')"

    dolt sql-client -P $PORT -u dolt --use-db test1 -q "call dolt_branch('newbranch')"
    run dolt sql-client --use-db "test1/newbranch" -u dolt -P $PORT -q "select * from a"
    [ $status -eq 0 ]
    [[ $output =~ " 1 " ]] || false
    [[ $output =~ " 2 " ]] || false

    dolt sql-client -P $PORT -u dolt --use-db test2 -q "call dolt_branch('newbranch')"
    run dolt sql-client --use-db "test2/newbranch" -u dolt -P $PORT -q "select * from a"
    [ $status -eq 0 ]
    [[ $output =~ " 3 " ]] || false
    [[ $output =~ " 4 " ]] || false

    # uppercase to ensure db names are treated case insensitive
    dolt sql-client -P $PORT -u dolt --use-db '' -q "drop database TEST1"

    run dolt sql-client --use-db "test1/newbranch" -u dolt -P $PORT -q "select * from a"
    [ $status -ne 0 ]
    [[ "$output" =~ "database not found" ]] || false
    
    # can't drop a branch-qualified database name
    run dolt sql-client -P $PORT -u dolt --use-db '' -q "drop database \`test2/newbranch\`"
    [ $status -ne 0 ]
    [[ "$output" =~ "unable to drop revision database: test2/newbranch" ]] || false

    dolt sql-client -P $PORT -u dolt --use-db '' -q "drop database TEST2"

    run dolt sql-client --use-db "test2/newbranch" -u dolt -P $PORT -q "select * from a"
    [ $status -ne 0 ]
    echo $output
    [[ "$output" =~ "database not found" ]] || false

    run dolt sql-client -P $PORT -u dolt --use-db test3 -q "select * from a"
    [ $status -eq 0 ]
    [[ $output =~ " 5 " ]] || false
    [[ $output =~ " 6 " ]] || false
}

@test "sql-server: connect to databases case insensitive" {
    skiponwindows "Missing dependencies"

    mkdir no_dolt && cd no_dolt
    start_sql_server

    dolt sql-client -P $PORT -u dolt --use-db '' -q "create database Test1"

    run dolt sql-client -P $PORT -u dolt --use-db '' -q "show databases"
    [ $status -eq 0 ]
    [[ $output =~ "mysql" ]] || false
    [[ $output =~ "information_schema" ]] || false
    [[ $output =~ "Test1" ]] || false
    dolt sql-client -P $PORT -u dolt --use-db '' -q "use test1; create table a(x int);"
    dolt sql-client -P $PORT -u dolt --use-db '' -q "use TEST1; insert into a values (1), (2);"
    dolt sql-client -P $PORT -u dolt --use-db '' -q "use test1; call dolt_add('.'); call dolt_commit('-a', '-m', 'new table a');"
    dolt sql-client -P $PORT -u dolt --use-db '' -q "use test1; call dolt_checkout('-b', 'newbranch');"
    dolt sql-client -P $PORT -u dolt --use-db '' -q "use \`TEST1/newbranch\`; select * from a order by x" ";x\n1\n2"
    dolt sql-client -P $PORT -u dolt --use-db '' -q "use \`test1/newbranch\`; select * from a order by x" ";x\n1\n2"
    dolt sql-client -P $PORT -u dolt --use-db '' -q "use \`TEST1/NEWBRANCH\`"

    run dolt sql-client -P $PORT -u dolt --use-db '' -q "create database test2; use test2; select database();"
    [ $status -eq 0 ]
    [[ $output =~ "test2" ]] || false
    run dolt sql-client -P $PORT -u dolt --use-db '' -q "use test2; drop database TEST2; select database();"
    [ $status -eq 0 ]
    [[ $output =~ "NULL" ]] || false
}

@test "sql-server: create and drop database with --data-dir" {
    skiponwindows "Missing dependencies"

    mkdir no_dolt && cd no_dolt
    mkdir db_dir
    start_sql_server_with_args --host 0.0.0.0 --user dolt --data-dir=db_dir
    
    dolt sql-client -P $PORT -u dolt --use-db '' -q "create database test1"
    run dolt sql-client -P $PORT -u dolt --use-db '' -q "show databases"
    [ $status -eq 0 ]
    [[ $output =~ "mysql" ]] || false
    [[ $output =~ "information_schema" ]] || false
    [[ $output =~ "test1" ]] || false

    # Make sure the sql-server lock file is set for a newly created database
    [[ -f "$PWD/db_dir/test1/.dolt/sql-server.lock" ]] || false

    dolt sql-client -P $PORT -u dolt --use-db test1 -q "create table a(x int)"
    dolt sql-client -P $PORT -u dolt --use-db test1 -q "call dolt_add('.')"
    dolt sql-client -P $PORT -u dolt --use-db test1 -q "insert into a values (1), (2)"
    dolt sql-client -P $PORT -u dolt --use-db test1 -q "call dolt_commit('-a', '-m', 'new table a')"
    stop_sql_server 1

    [ -d db_dir/test1 ]

    cd db_dir/test1
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "new table a" ]] || false

    cd ../..

    start_sql_server_with_args --host 0.0.0.0 --user dolt --data-dir=db_dir
    dolt sql-client -P $PORT -u dolt --use-db '' -q "create database test3"
    dolt sql-client -P $PORT -u dolt --use-db test3 -q "create table c(x int)"
    dolt sql-client -P $PORT -u dolt --use-db test3 -q "call dolt_add('.')"
    dolt sql-client -P $PORT -u dolt --use-db test3 -q "insert into c values (1), (2)"
    dolt sql-client -P $PORT -u dolt --use-db test3 -q "call dolt_commit('-a', '-m', 'new table c')"
    dolt sql-client -P $PORT -u dolt --use-db '' -q "drop database test1"
    stop_sql_server 1

    [ -d db_dir/test3 ]
    [ ! -d db_dir/test1 ]

    # make sure the databases exist on restart
    stop_sql_server
    start_sql_server_with_args --host 0.0.0.0 --user dolt --data-dir=db_dir
    run dolt sql-client -P $PORT -u dolt --use-db '' -q "show databases"
    [ $status -eq 0 ]
    [[ $output =~ "mysql" ]] || false
    [[ $output =~ "information_schema" ]] || false
    [[ $output =~ "test3" ]] || false
}

@test "sql-server: create database errors" {
    skiponwindows "Missing dependencies"

    mkdir no_dolt && cd no_dolt
    mkdir dir_exists
    touch file_exists
    start_sql_server

    dolt sql-client -P $PORT -u dolt --use-db '' -q "create database test1"

    # Error on creation, already exists
    run dolt sql-client -P $PORT -u dolt --use-db '' -q "create database test1"
    [ $status -ne 0 ]
    [[ $output =~ exists ]] || false

    # Files / dirs in the way
    run dolt sql-client -P $PORT -u dolt --use-db '' -q "create database dir_exists"
    [ $status -ne 0 ]
    [[ $output =~ exists ]] || false
    
    run dolt sql-client -P $PORT -u dolt --use-db '' -q	"create database file_exists"
    [ $status -ne 0 ]
    [[ $output =~ exists ]] || false
}

@test "sql-server: create database with existing repo" {
    skiponwindows "Missing dependencies"

    cd repo1
    start_sql_server

    dolt sql-client -P $PORT -u dolt --use-db repo1 -q "create database test1"
    run dolt sql-client -P $PORT -u dolt --use-db repo1 -q "show databases"
    [ $status -eq 0 ]
    [[ $output =~ "mysql" ]] || false
    [[ $output =~ "information_schema" ]] || false
    [[ $output =~ "test1" ]] || false
    [[ $output =~ "repo1" ]] || false

    dolt sql-client -P $PORT -u dolt --use-db test1 -q "create table a(x int)"
    dolt sql-client -P $PORT -u dolt --use-db test1 -q "call dolt_add('.')"
    dolt sql-client -P $PORT -u dolt --use-db test1 -q "insert into a values (1), (2)"
    dolt sql-client -P $PORT -u dolt --use-db test1 -q "call dolt_commit('-a', '-m', 'new table a')"
    dolt sql-client -P $PORT -u dolt --use-db repo1 -q "create database test2"
    dolt sql-client -P $PORT -u dolt --use-db test2 -q "create table b(x int)"
    dolt sql-client -P $PORT -u dolt --use-db test2 -q "call dolt_add('.')"
    dolt sql-client -P $PORT -u dolt --use-db test2 -q "insert into b values (1), (2)"
    dolt sql-client -P $PORT -u dolt --use-db test2 -q "call dolt_commit('-a', '-m', 'new table b')"
    stop_sql_server 1

    cd test1
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "new table a" ]] || false

    run dolt --user=dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "a" ]] || false

    cd ../test2
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "new table b" ]] || false

    run dolt --user=dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "b" ]] || false

    cd ../
    # make sure the databases exist on restart
    start_sql_server
    run dolt sql-client -P $PORT -u dolt --use-db repo1 -q "show databases"
    [ $status -eq 0 ]
    [[ $output =~ "mysql" ]] || false
    [[ $output =~ "information_schema" ]] || false
    [[ $output =~ "test1" ]] || false
    [[ $output =~ "repo1" ]] || false
    [[ $output =~ "test2" ]] || false
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

    dolt sql-client -P $PORT -u dolt --use-db repo1 -q "call dolt_fetch()"
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
    [[ -f "$PWD/.dolt/sql-server.lock" ]] || false

    PORT=$( definePORT )
    run dolt sql-server -P $PORT --socket "dolt.$PORT.sock"
    [ "$status" -eq 1 ]
}

@test "sql-server: sql-server sets permissions on sql-server.lock" {
    cd repo1
    ! [[ -f "$PWD/.dolt/sql-server.lock" ]] || false
    start_sql_server
    [[ -f "$PWD/.dolt/sql-server.lock" ]] || false


    if [[ `uname` == 'Darwin' ]]; then
      run stat -x "$PWD/.dolt/sql-server.lock"
      [[ "$output" =~ "(0600/-rw-------)" ]] || false
    else
      run stat "$PWD/.dolt/sql-server.lock"
      [[ "$output" =~ "(0600/-rw-------)" ]] || false
    fi
}

@test "sql-server: multi dir sql-server locks out children" {
    start_sql_server
    cd repo2
    PORT=$( definePORT )
    run dolt sql-server -P $PORT --socket "dolt.$PORT.sock"
    [ "$status" -eq 1 ]
}

@test "sql-server: sql-server child locks out parent multi dir" {
    cd repo2
    start_sql_server
    cd ..
    PORT=$( definePORT )
    run dolt sql-server -P $PORT --socket "dolt.$PORT.sock"
    [ "$status" -eq 1 ]
}

@test "sql-server: sql-server lock for new databases" {
    cd repo1
    start_sql_server
    dolt sql-client -P $PORT -u dolt --use-db '' -q "create database newdb"

    # Make sure the sql-server lock file is set for the new database
    [[ -f "$PWD/newdb/.dolt/sql-server.lock" ]] || false

    # Verify that we can't start a sql-server from the new database dir
    cd newdb
    PORT=$( definePORT )
    run dolt sql-server -P $PORT --socket "dolt.$PORT.sock"
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

    run dolt sql-client -P $PORT -u dolt --use-db repo2 -q "select 1 as col1"
    [ $status -eq 0 ]
    [[ $output =~ col1 ]] || false
    [[ $output =~ " 1 " ]] || false

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

    run dolt sql-client -P $PORT -u dolt --use-db repo2 -q "select 1 as col1"
    [ $status -eq 0 ]
    [[ $output =~ col1 ]] || false
    [[ $output =~ " 1 " ]] || false

    run grep '\"/tmp/mysql.sock\"' log.txt
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]
}

@test "sql-server: the second server starts without unix socket set up if there is already a file in the socket file path" {
    skiponwindows "unix socket is not available on Windows"
    cd repo2
    touch mysql.sock

    run pwd
    REPO_NAME=$output

    secondPORT=$( definePORT )
    dolt sql-server --port=$secondPORT --socket="$REPO_NAME/mysql.sock" --user dolt > log.txt 2>&1 &
    SECOND_SERVER_PID=$!
    run wait_for_connection $secondPORT 5000
    [ "$status" -eq 0 ]

    run grep 'unix socket set up failed: file already in use:' log.txt
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]

    # killing the second server should not affect the socket file.
    kill $SECOND_SERVER_PID

    [ -f mysql.sock ]
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
  socket: dolt.$PORT.sock

behavior:
  autocommit: true" > server.yaml

    dolt sql-server --config server.yaml > log.txt 2>&1 &
    SERVER_PID=$!
    wait_for_connection $PORT 5000

    run dolt sql-client -P $PORT -u dolt --use-db repo2 -q "select 1 as col1"
    [ $status -eq 0 ]
    [[ $output =~ col1 ]] || false
    [[ $output =~ " 1 " ]] || false

    run grep "dolt.$PORT.sock" log.txt
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
    run dolt sql-server --port=$PORT --socket "dolt.$PORT.sock"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "database locked by another sql-server; either clone the database to run a second server" ]] || false
    stop_sql_server 1
}

@test "sql-server: sigterm running server and restarting works correctly" {
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
    run dolt sql-client -P $PORT -u dolt --use-db repo2 -q "select 1 as col1"
    [ $status -eq 0 ]
    [[ $output =~ col1 ]] || false
    [[ $output =~ " 1 " ]] || false
    stop_sql_server

    # Try adding fake pid numbers. Could happen via debugger or something
    echo "423423" > repo1/.dolt/sql-server.lock
    echo "4123423" > repo2/.dolt/sql-server.lock

    start_sql_server
    run dolt sql-client -P $PORT -u dolt --use-db repo2 -q "select 1 as col1"
    [ $status -eq 0 ]
    [[ $output =~ col1 ]] || false
    [[ $output =~ " 1 " ]] || false
    stop_sql_server

    # Add malicious text to lockfile and expect to fail
    echo "iamamaliciousactor" > repo1/.dolt/sql-server.lock

    run start_sql_server
    [[ "$output" =~ "database locked by another sql-server; either clone the database to run a second server" ]] || false
    [ "$status" -eq 1 ]

    rm repo1/.dolt/sql-server.lock

    # this test was hanging as the server is stopped from the above error
    # but stop_sql_server in teardown tries to kill process that is not
    # running anymore, so start the server again, and it will be stopped in
    # teardown
    start_sql_server
}

@test "sql-server: create a database when no current database is set" {
    mkdir new_format && cd new_format
    dolt init

    PORT=$( definePORT )
    dolt sql-server --host 0.0.0.0 --port=$PORT --user dolt --socket "dolt.$PORT.sock" &
    SERVER_PID=$! # will get killed by teardown_common
    sleep 5 # not using python wait so this works on windows

    dolt sql-client --host=0.0.0.0 --port=$PORT --user=dolt <<< "create database mydb1;"
    dolt sql-client --host=0.0.0.0 --port=$PORT --user=dolt <<< "exit;"
    stop_sql_server 1
    [ -d mydb1 ]

    cd mydb1
    dolt version
}

@test "sql-server: deleting database directory when a running server is using it does not panic" {
    skiponwindows "Missing dependencies"

    mkdir nodb
    cd nodb
    start_sql_server >> server_log.txt 2>&1

    dolt sql-client -P $PORT -u dolt --use-db '' -q "CREATE DATABASE mydb1"
    dolt sql-client -P $PORT -u dolt --use-db '' -q "CREATE DATABASE mydb2"

    [ -d mydb1 ]
    [ -d mydb2 ]

    rm -rf mydb2

    run dolt sql-client -P $PORT -u dolt --use-db '' -q "SHOW DATABASES"
    [ $status -eq 0 ]

    skip "Forcefully deleting a database doesn't cause direct panics, but also doesn't stop the server"

    run grep "panic" server_log.txt
    [ "${#lines[@]}" -eq 0 ]

    run grep "failed to access 'mydb2' database: can no longer find .dolt dir on disk" server_log.txt
    [ "${#lines[@]}" -eq 1 ]

    # this tests fails sometimes as the server is stopped from the above error
    # but stop_sql_server in teardown tries to kill process that is not
    # running anymore, so start the server again, and it will be stopped in
    # teardown
    start_sql_server
}

@test "sql-server: dropping database that the server is running in should drop only the db itself not its nested dbs" {
    skiponwindows "Missing dependencies"

    mkdir mydb
    cd mydb
    dolt init

    start_sql_server >> server_log.txt 2>&1
    # 'doltdb' will be nested database inside 'mydb'
    dolt sql-client -P $PORT -u dolt --use-db '' -q "CREATE DATABASE doltdb"
    run dolt sql-client -P $PORT -u dolt --use-db '' -q "SHOW DATABASES"
    [[ "$output" =~ "mydb" ]] || false
    [[ "$output" =~ "doltdb" ]] || false

    dolt sql-client -P $PORT -u dolt --use-db '' -q "DROP DATABASE mydb"
    stop_sql_server 1
    [ ! -d .dolt ]

    run grep "database not found: mydb" server_log.txt
    [ "${#lines[@]}" -eq 0 ]

    # nested databases inside dropped database should still exist
    dolt sql -q "SHOW DATABASES"
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
    dolt sql-client -P $PORT -u dolt --use-db '' -q "DROP DATABASE mydb;"

    run grep "database not found: mydb" server_log.txt
    [ "${#lines[@]}" -eq 0 ]

    [ ! -d .dolt ]

    run dolt sql -q "SHOW DATABASES"
    [[ ! "$output" =~ "mydb" ]] || false
}

@test "sql-server: create database, drop it, and then create it again" {
    skiponwindows "Missing dependencies"

    mkdir mydbs
    cd mydbs

    start_sql_server >> server_log.txt 2>&1
    dolt sql-client -P $PORT -u dolt --use-db '' -q "CREATE DATABASE mydb1;"
    [ -d mydb1 ]

    dolt sql-client -P $PORT -u dolt --use-db '' -q "DROP DATABASE mydb1;"
    [ ! -d mydb1 ]

    dolt sql-client -P $PORT -u dolt --use-db '' -q "CREATE DATABASE mydb1;"
    [ -d mydb1 ]

    run dolt sql-client -P $PORT -u dolt --use-db '' -q "SHOW DATABASES;"
    [ $status -eq 0 ]
    [[ "$output" =~ "mydb1" ]] || false
}

@test "sql-server: dropping database with '-' in it" {
    skiponwindows "Missing dependencies"

    mkdir my-db
    cd my-db
    dolt init
    cd ..

    start_sql_server >> server_log.txt 2>&1
    dolt sql-client -P $PORT -u dolt --use-db '' -q "DROP DATABASE my_db;"

    run grep "database not found: my_db" server_log.txt
    [ "${#lines[@]}" -eq 0 ]

    [ ! -d my-db ]
}

@test "sql-server: dolt_clone procedure in empty dir" {
    mkdir rem1
    cd repo1
    dolt sql -q "CREATE TABLE test (pk INT PRIMARY KEY);"
    dolt sql -q "INSERT INTO test VALUES (1), (2), (3);"
    dolt sql -q "CREATE PROCEDURE test() SELECT 42;"
    dolt add -A
    dolt commit -m "initial commit"
    dolt remote add remote1 file://../rem1
    dolt push remote1 main

    cd ..
    dolt sql -q "call dolt_clone('file://./rem1', 'repo3');"
    cd repo3

    # verify databases
    run dolt sql -q "show databases;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "repo3" ]] || false

    run dolt sql -q "select database();"
    [[ "$output" =~ "repo3" ]] || false

    # verify data
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1" ]] || false
    [[ "$output" =~ "2" ]] || false
    [[ "$output" =~ "3" ]] || false

    # verify procedure
    run dolt sql -q "call test()"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "42" ]] || false
}

@test "sql-server: locks made in session should be released on session end" {
    start_sql_server
    EXPECTED=$(echo -e "\"GET_LOCK('mylock', 1000)\"\n1\nIS_FREE_LOCK('mylock')\n0")
    run dolt sql-client -P $PORT -u dolt --use-db '' --result-format csv -q "SELECT GET_LOCK('mylock', 1000); SELECT IS_FREE_LOCK('mylock');"
    [ $status -eq 0 ]
    [[ "$output" =~ "$EXPECTED" ]] || false

    EXPECTED=$(echo -e "IS_FREE_LOCK('mylock')\n1")
    run dolt sql-client -P $PORT -u dolt --use-db '' --result-format csv -q "SELECT IS_FREE_LOCK('mylock');"
    [ $status -eq 0 ]
    [[ "$output" =~ "$EXPECTED" ]] || false
}

@test "sql-server: binary literal is printed as hex string for utf8 charset result set" {
    cd repo1
    start_sql_server
    dolt sql-client -P $PORT -u dolt --use-db repo1 -q "SET character_set_results = utf8; CREATE TABLE mapping(branch_id binary(16) PRIMARY KEY, user_id binary(16) NOT NULL, company_id binary(16) NOT NULL);"

    run dolt sql-client -P $PORT -u dolt --use-db repo1 -q "EXPLAIN SELECT m.* FROM mapping m WHERE user_id = uuid_to_bin('1c4c4e33-8ad7-4421-8450-9d5182816ac3');"
    [ $status -eq 0 ]
    [[ "$output" =~ "0x1C4C4E338AD7442184509D5182816AC3" ]] || false
}
