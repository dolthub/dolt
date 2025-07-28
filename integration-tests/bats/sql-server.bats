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
    if [ "$SQL_ENGINE" = "remote-engine" ]; then
      skip "This test tests remote connections directly, SQL_ENGINE is not needed."
    fi
    setup_no_dolt_init
    make_repo repo1
    make_repo repo2
}

teardown() {
    stop_sql_server 1 && sleep 0.5
    rm -rf $BATS_TMPDIR/sql-server-test$$
    teardown_common
}

@test "sql-server: innodb_autoinc_lock_mode is set to 2" {
    # assert that loglevel on command line is not case sensitive
    cd repo1
    PORT=$( definePORT )

    # assert that innodb_autoinc_lock_mode is set to 2
    cat > config.yml <<EOF
listener:
  host: "0.0.0.0"
  port: $PORT
system_variables:
  innodb_autoinc_lock_mode: 100
EOF
    run dolt sql-server --config ./config.yml
    [ $status -eq 1 ]
    [[ "$output" =~ "Variable 'innodb_autoinc_lock_mode' can't be set to the value of '100'" ]] || false
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

@test "sql-server: committer is sql user" {
  # Start a SQL-server and add a new user "user1"
  cd repo1
  start_sql_server
  dolt sql -q "create user user1@'%';"
  dolt sql -q "grant all privileges on *.* to user1@'%';"

  # By default, commits will be authored by the current sql user (user1)
  dolt -u user1 sql -q "create table t2(pk int primary key);"
  dolt -u user1 sql -q "call dolt_commit('-Am', 'committing as user1');"
  run dolt -u user1 sql -q "select committer, email, message from dolt_log limit 1;"
  [ $status -eq 0 ]
  [[ $output =~ "| user1     | user1@% | committing as user1 |" ]] || false

  # If --author is explicitly provided, then always use that, even if dolt_sql_user_is_committer is enabled
  dolt -u user1 sql -q "create table t3(pk int primary key);"
  dolt -u user1 sql -q "call dolt_commit('--author', 'barbie <barbie@plastic.com>', '-Am', 'committing as barbie');"
  run dolt -u user1 sql -q "select committer, email, message from dolt_log limit 1;"
  [ $status -eq 0 ]
  [[ $output =~ "| barbie    | barbie@plastic.com | committing as barbie |" ]] || false
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
    run dolt sql -q "$SAVEPOINT_QUERY"
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
    run dolt sql -q "show databases"
    [ $status -eq 0 ]
    [[ $output =~ information_schema ]] || false
    [[ $output =~ mysql ]] || false

    # verify that dolt_clone works
    dolt sql -q "create database test01"
    dolt --use-db 'test01' sql -q "call dolt_clone('file:///$tempDir/remote')"
}

@test "sql-server: loglevels are case insensitive" {
    # assert that loglevel on command line is not case sensitive
    cd repo1
    PORT=$( definePORT )
    dolt sql-server --loglevel TrAcE --port=$PORT --socket "dolt.$PORT.sock" > log.txt 2>&1 &
    SERVER_PID=$!
    wait_for_connection $PORT 8500
    dolt sql -q "show databases;"
    stop_sql_server

    # assert that loglevel in yaml config is not case sensitive
    cat >config.yml <<EOF
log_level: dEBuG
behavior:
  disable_client_multi_statements: true
listener:
  host: "0.0.0.0"
  port: $PORT
EOF
    dolt sql-server --config ./config.yml --socket "dolt.$PORT.sock" &
    SERVER_PID=$!
    wait_for_connection $PORT 8500
    dolt sql -q "show databases;"
    stop_sql_server
}
@test "sql-server: logformats are case insensitive" {
    # assert that logformat on command line is not case sensitive
    cd repo1
    PORT=$( definePORT )
    dolt sql-server --logformat jSon --port=$PORT --socket "dolt.$PORT.sock" > log.txt 2>&1 &
    SERVER_PID=$!
    wait_for_connection $PORT 8500 
    dolt sql -q "show databases;"
    stop_sql_server

    # assert that logformat in yaml config is not case sensitive
    cat >config.yml <<EOF
log_format: teXt
behavior:
  disable_client_multi_statements: true
listener:
  host: "0.0.0.0"
  port: $PORT
EOF
    dolt sql-server --config ./config.yml --socket "dolt.$PORT.sock" &
    SERVER_PID=$!
    wait_for_connection $PORT 8500
    dolt sql -q "show databases;"
    stop_sql_server
}

@test "sql-server: logformat json functionality is working" {
    cd repo1
    PORT=$( definePORT )
    dolt sql-server --logformat json --port=$PORT --socket "dolt.$PORT.sock" > log.txt 2>&1 &
    SERVER_PID=$!
    wait_for_connection $PORT 8500 
    dolt sql -q "show databases;"
    stop_sql_server

    # Assert that log is in JSON format (checking if logs contain `{...}`)
    grep -q '^{.*}$' log.txt
}

@test "sql-server: server assumes existing user" {
    cd repo1
    dolt sql -q "create user dolt@'%' identified by '123'"

    PORT=$( definePORT )
    dolt sql-server --port=$PORT --socket "dolt.$PORT.sock" > log.txt 2>&1 &
    SERVER_PID=$!
    sleep 5

    run dolt --user=dolt --password=wrongpassword sql -q "select 1"
    [ "$status" -eq 1 ]
    run grep 'Error authenticating user' log.txt
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
}

@test "sql-server: Database specific system variables should be loaded" {
    cd repo1
    dolt branch dev
    dolt branch other

    start_sql_server
    dolt sql -q "SET PERSIST repo1_default_branch = 'dev'"
    stop_sql_server
    start_sql_server
    run dolt --use-db repo1 sql -q "SELECT @@repo1_default_branch;"
    [ $status -eq 0 ]
    [[ $output =~ "@@repo1_default_branch" ]] || false
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
    aws_credentials_profile: lddev
- name: user3
  vars:
    autocommit: 0" > server.yaml

    dolt --privilege-file=privs.json sql -q "CREATE USER dolt@'127.0.0.1'"
    dolt --privilege-file=privs.json sql -q "CREATE USER user0@'127.0.0.1' IDENTIFIED BY 'pass0'"
    dolt --privilege-file=privs.json sql -q "CREATE USER user1@'127.0.0.1' IDENTIFIED BY 'pass1'"
    dolt --privilege-file=privs.json sql -q "CREATE USER user2@'127.0.0.1' IDENTIFIED BY 'pass2'"
    dolt --privilege-file=privs.json sql -q "CREATE USER user3@'127.0.0.1' IDENTIFIED BY 'pass3'"

    # start_sql_server_with_config calls wait_for_connection and tests the connection with the root user, but
    # since root isn't created in this setup, we set SQL_USER so that wait_for_connection uses the dolt user instead.
    SQL_USER=dolt
    start_sql_server_with_config "" server.yaml

    run dolt --host=127.0.0.1 --port=$PORT --no-tls --user=user0 --password=pass0 sql -q "SELECT @@aws_credentials_file, @@aws_credentials_profile;"
    [[ "$output" =~ /Users/user0/.aws/config.*default ]] || false

    run dolt --host=127.0.0.1 --port=$PORT --no-tls --user=user1 --password=pass1 sql -q "SELECT @@aws_credentials_file, @@aws_credentials_profile;"
    [[ "$output" =~ /Users/user1/.aws/config.*lddev ]] || false

    run dolt --host=127.0.0.1 --port=$PORT --no-tls --user=user2 --password=pass2 sql -q "SELECT @@aws_credentials_file, @@aws_credentials_profile;"
    [[ "$output" =~ "   " ]] || false

    run dolt --host=127.0.0.1 --port=$PORT --no-tls --user=user2 --password=pass2 sql -q "SET @@aws_credentials_file='/Users/should_fail';"
    [[ "$output" =~ "Variable 'aws_credentials_file' is a read only variable" ]] || false

    run dolt --host=127.0.0.1 --port=$PORT --no-tls --user=user3 --password=pass3 sql -q "SELECT @@autocommit;"
    [[ "$output" =~ "0" ]] || false
}

@test "sql-server: read-only mode" {
    skiponwindows "Missing dependencies"

    # Create a second branch to test `call dolt_checkout()`
    # and push to a remote to test `dolt status`
    cd repo1
    dolt sql -q "call dolt_branch('other');"
    mkdir ../repo1-remote
    dolt remote add origin file://../repo1-remote
    dolt push origin main

    # Start up the server in read-only mode
    start_sql_server_with_args "--readonly"

    # Assert that we can still checkout other branches and run dolt status
    # while the sql-server is running in read-only mode
    dolt sql -q "call dolt_checkout('other');"
    dolt sql -q "call dolt_count_commits('--from', 'HEAD', '--to', 'HEAD');"
    dolt status
}

@test "sql-server: read-only mode sets @@read_only system variable" {
    skiponwindows "Missing dependencies"

    cd repo1

    # Start up the server in read-only mode
    start_sql_server_with_args "--readonly"

    # Verify that @@read_only system variable is set to 1
    run dolt sql -q "SELECT @@read_only;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1" ]] || false

    # Verify that @@global.read_only system variable is set to 1
    run dolt sql -q "SELECT @@global.read_only;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1" ]] || false

    # Verify that write operations are blocked
    run dolt sql -q "CREATE TABLE test_readonly_table (id INT PRIMARY KEY);"
    [ "$status" -ne 0 ]
    [[ "$output" =~ "read only mode" ]] || false
}

@test "sql-server: read-only mode via YAML config sets @@read_only system variable" {
    skiponwindows "Missing dependencies"

    cd repo1

    # Create YAML config with read-only enabled
    PORT=$( definePORT )
    cat > config.yml <<EOF
log_level: info
behavior:
  read_only: true
listener:
  host: 0.0.0.0
  port: $PORT
EOF

    # Start up the server with YAML config
    if [ "$IS_WINDOWS" == true ]; then
      dolt sql-server --config config.yml &
    else
      dolt sql-server --config config.yml --socket "dolt.$PORT.sock" &
    fi
    SERVER_PID=$!
    wait_for_connection $PORT 8500

    # Verify that @@read_only system variable is set to 1
    run dolt sql -q "SELECT @@read_only;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1" ]] || false

    # Verify that write operations are blocked
    run dolt sql -q "CREATE TABLE test_readonly_yaml_table (id INT PRIMARY KEY);"
    [ "$status" -ne 0 ]
    [[ "$output" =~ "read only mode" ]] || false
}

@test "sql-server: inspect sql-server using CLI" {
    skiponwindows "Missing dependencies"

    cd repo1
    start_sql_server repo1

    # No tables at the start
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "No tables in working set" ]] || false

    dolt sql -q "CREATE TABLE one_pk (
        pk BIGINT NOT NULL,
        c1 BIGINT,
        c2 BIGINT,
        PRIMARY KEY (pk))"

    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "one_pk" ]] || false

    # Add rows on the command line
    run dolt --verbose-engine-setup sql -q "insert into one_pk values (1,1,1)"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "starting remote mode" ]] || false
    run dolt sql -q "SELECT * FROM one_pk"
    [ $status -eq 0 ]
    [[ $output =~ " 1 " ]] || false

    # Test import as well (used by doltpy)
    echo 'pk,c1,c2' > import.csv
    echo '2,2,2' >> import.csv
    run dolt table import -u one_pk import.csv
    [ "$status" -eq 1 ]

    run dolt sql -q "SELECT * FROM one_pk"
    [ $status -eq 0 ]
    ! [[ $output =~ " 2 " ]] || false
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
    dolt sql -q "INSERT INTO test VALUES (7,7);"
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false

    dolt sql -q "CALL DOLT_RESET('--hard');"

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "working tree clean" ]] || false
    run dolt sql -q "SELECT sum(pk), sum(c0) FROM test;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "6,6" ]] || false

    dolt sql -q "
        INSERT INTO test VALUES (8,8);
        CALL DOLT_RESET('--hard');"

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "working tree clean" ]] || false
    run dolt sql -q "SELECT sum(pk), sum(c0) FROM test;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "6,6" ]] || false
}

@test "sql-server: test multi db with use statements" {
    skiponwindows "Missing dependencies"

    start_multi_db_server repo1

    # create a table in repo1
    dolt sql -q "CREATE TABLE r1_one_pk (
        pk BIGINT NOT NULL COMMENT 'tag:0',
        c1 BIGINT COMMENT 'tag:1',
        c2 BIGINT COMMENT 'tag:2',
        PRIMARY KEY (pk))"

    # create a table in repo2
    dolt sql -q "USE repo2;
    CREATE TABLE r2_one_pk (
        pk BIGINT NOT NULL COMMENT 'tag:0',
        c3 BIGINT COMMENT 'tag:1',
        c4 BIGINT COMMENT 'tag:2',
        PRIMARY KEY (pk)
    )"

    # validate tables in repos
    run dolt sql -q "SHOW tables"
    [ $status -eq 0 ]
    [[ $output =~ "r1_one_pk" ]] || false
    run dolt sql -q "USE repo2; SHOW tables"
    [ $status -eq 0 ]
    [[ $output =~ "r2_one_pk" ]] || false

    # put data in both
    dolt sql -q "
    INSERT INTO r1_one_pk (pk) VALUES (0);
    INSERT INTO r1_one_pk (pk,c1) VALUES (1,1);
    INSERT INTO r1_one_pk (pk,c1,c2) VALUES (2,2,2),(3,3,3);
    USE repo2;
    INSERT INTO r2_one_pk (pk) VALUES (0);
    INSERT INTO r2_one_pk (pk,c3) VALUES (1,1);
    INSERT INTO r2_one_pk (pk,c3,c4) VALUES (2,2,2),(3,3,3)"

    run dolt sql --result-format csv -q "SELECT * FROM repo1.r1_one_pk ORDER BY pk"
    [ $status -eq 0 ]
    [[ $output =~ "0,," ]] || false
    [[ $output =~ "1,1," ]] || false
    [[ $output =~ "2,2,2" ]] || false
    [[ $output =~ "3,3,3" ]] || false

    run dolt sql --result-format csv -q "SELECT * FROM repo2.r2_one_pk ORDER BY pk"
    [ $status -eq 0 ]
    [[ $output =~ "0,," ]] || false
    [[ $output =~ "1,1," ]] || false
    [[ $output =~ "2,2,2" ]] || false
    [[ $output =~ "3,3,3" ]] || false

    dolt sql -q "
    DELETE FROM r1_one_pk where pk=0;
    USE repo2;
    DELETE FROM r2_one_pk where pk=0"

    run dolt sql --result-format csv -q "SELECT * FROM repo1.r1_one_pk ORDER BY pk"
    [ $status -eq 0 ]
    ! [[ $output =~ "0,," ]] || false
    [[ $output =~ "1,1," ]] || false
    [[ $output =~ "2,2,2" ]] || false
    [[ $output =~ "3,3,3" ]] || false

    run dolt sql --result-format csv -q "SELECT * FROM repo2.r2_one_pk ORDER BY pk"
    [ $status -eq 0 ]
    ! [[ $output =~ "0,," ]] || false
    [[ $output =~ "1,1," ]] || false
    [[ $output =~ "2,2,2" ]] || false
    [[ $output =~ "3,3,3" ]] || false

    dolt sql -q "
    UPDATE r1_one_pk SET c2=1 WHERE pk=1;
    USE repo2;
    UPDATE r2_one_pk SET c4=1 where pk=1"

    run dolt sql --result-format csv -q "SELECT * FROM repo1.r1_one_pk ORDER BY pk"
    [ $status -eq 0 ]
    ! [[ $output =~ "0,," ]] || false
    ! [[ $output =~ "1,1, " ]] || false
    [[ $output =~ "1,1,1" ]] || false
    [[ $output =~ "2,2,2" ]] || false
    [[ $output =~ "3,3,3" ]] || false

    run dolt sql --result-format csv -q "SELECT * FROM repo2.r2_one_pk ORDER BY pk"
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
    dolt sql -q "CREATE TABLE repo1.r1_one_pk (
        pk BIGINT NOT NULL COMMENT 'tag:0',
        c1 BIGINT COMMENT 'tag:1',
        c2 BIGINT COMMENT 'tag:2',
        PRIMARY KEY (pk))"

    # create a table in repo2
    dolt sql -q "CREATE TABLE repo2.r2_one_pk (
        pk BIGINT NOT NULL COMMENT 'tag:0',
        c3 BIGINT COMMENT 'tag:1',
        c4 BIGINT COMMENT 'tag:2',
        PRIMARY KEY (pk)
    )"

    # validate tables in repos
    run dolt --use-db repo1 sql -q "SHOW tables"
    [ $status -eq 0 ]
    [[ $output =~ "r1_one_pk" ]] || false
    run dolt --use-db repo2 sql -q "SHOW tables"
    [ $status -eq 0 ]
    [[ $output =~ "r2_one_pk" ]] || false

    # put data in both using database scoped inserts
    dolt sql -q "INSERT INTO repo1.r1_one_pk (pk) VALUES (0)"
    dolt sql -q "INSERT INTO repo1.r1_one_pk (pk,c1) VALUES (1,1)"
    dolt sql -q "INSERT INTO repo1.r1_one_pk (pk,c1,c2) VALUES (2,2,2),(3,3,3)"

    dolt sql -q "INSERT INTO repo2.r2_one_pk (pk) VALUES (0)"
    dolt sql -q "INSERT INTO repo2.r2_one_pk (pk,c3) VALUES (1,1)"
    dolt sql -q "INSERT INTO repo2.r2_one_pk (pk,c3,c4) VALUES (2,2,2),(3,3,3)"

    run dolt sql --result-format csv -q "SELECT * FROM repo1.r1_one_pk ORDER BY pk"
    [ $status -eq 0 ]
    [[ $output =~ "0,," ]] || false
    [[ $output =~ "1,1," ]] || false
    [[ $output =~ "2,2,2" ]] || false
    [[ $output =~ "3,3,3" ]] || false

    run dolt sql --result-format csv -q "SELECT * FROM repo2.r2_one_pk ORDER BY pk"
    [ $status -eq 0 ]
    [[ $output =~ "0,," ]] || false
    [[ $output =~ "1,1," ]] || false
    [[ $output =~ "2,2,2" ]] || false
    [[ $output =~ "3,3,3" ]] || false

    dolt sql -q "DELETE FROM repo1.r1_one_pk where pk=0"
    dolt sql -q "DELETE FROM repo2.r2_one_pk where pk=0"

    run dolt sql --result-format csv -q "SELECT * FROM repo1.r1_one_pk ORDER BY pk"
    [ $status -eq 0 ]
    ! [[ $output =~ "0,," ]] || false
    [[ $output =~ "1,1," ]] || false
    [[ $output =~ "2,2,2" ]] || false
    [[ $output =~ "3,3,3" ]] || false

    run dolt sql --result-format csv -q "SELECT * FROM repo2.r2_one_pk ORDER BY pk"
    [ $status -eq 0 ]
    ! [[ $output =~ "0,," ]] || false
    [[ $output =~ "1,1," ]] || false
    [[ $output =~ "2,2,2" ]] || false
    [[ $output =~ "3,3,3" ]] || false

    dolt sql -q "UPDATE repo1.r1_one_pk SET c2=1 WHERE pk=1"
    dolt sql -q "UPDATE repo2.r2_one_pk SET c4=1 where pk=1"

    run dolt sql --result-format csv -q "SELECT * FROM repo1.r1_one_pk ORDER BY pk"
    [ $status -eq 0 ]
    ! [[ $output =~ "0,," ]] || false
    ! [[ $output =~ "1,1, " ]] || false
    [[ $output =~ "1,1,1" ]] || false
    [[ $output =~ "2,2,2" ]] || false
    [[ $output =~ "3,3,3" ]] || false

    run dolt sql --result-format csv -q "SELECT * FROM repo2.r2_one_pk ORDER BY pk"
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

     dolt sql -q "CREATE TABLE test (
         pk int primary key
     )"
     dolt sql -q "INSERT INTO test VALUES (0),(1),(2)"
     dolt sql -q "CALL DOLT_ADD('test')"
     dolt sql -q "CALL DOLT_COMMIT('-a', '-m', 'Step 1')"
     dolt sql -q "CALL DOLT_CHECKOUT('-b', 'feature-branch')"

     run dolt sql -q "SELECT * FROM test"
     [ $status -eq 0 ]
     [[ $output =~ " 0 " ]] || false
     [[ $output =~ " 1 " ]] || false
     [[ $output =~ " 2 " ]] || false

     dolt sql -q "
     CALL DOLT_CHECKOUT('feature-branch');
     INSERT INTO test VALUES (3);
     INSERT INTO test VALUES (4);
     INSERT INTO test VALUES (21232);
     DELETE FROM test WHERE pk=4;
     UPDATE test SET pk=21 WHERE pk=21232;
     "

     run dolt sql -q "SELECT * FROM test"
     [ $status -eq 0 ]
     [[ $output =~ " 0 " ]] || false
     [[ $output =~ " 1 " ]] || false
     [[ $output =~ " 2 " ]] || false
     ! [[ $output =~ " 3 " ]] || false
     ! [[ $output =~ " 21 " ]] || false

     dolt sql -q "
     CALL DOLT_CHECKOUT('feature-branch');
     CALL DOLT_COMMIT('-a', '-m', 'Insert 3');
     "

     dolt sql -q "
     INSERT INTO test VALUES (500000);
     INSERT INTO test VALUES (500001);
     DELETE FROM test WHERE pk=500001;
     UPDATE test SET pk=60 WHERE pk=500000;
     CALL DOLT_ADD('.');
     CALL DOLT_COMMIT('-m', 'Insert 60');
     CALL DOLT_MERGE('feature-branch','-m','merge feature-branch');
     "

     run dolt sql -q "SELECT * FROM test"
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

    run dolt sql -q "CALL DOLT_CHECKOUT('branch1'); CALL DOLT_BRANCH('-D', 'main');"
    [ $status -eq 1 ]
    [[ $output =~ "default branch for database 'repo1'" ]] || false
}

@test "sql-server: DOLT_MERGE ff works" {
    skiponwindows "Missing dependencies"

     cd repo1
     start_sql_server repo1

     dolt sql -q "
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

     run dolt sql -q "SELECT * FROM test"
     [ $status -eq 0 ]
     [[ $output =~ " 1 " ]] || false
     [[ $output =~ " 2 " ]] || false
     [[ $output =~ " 3 " ]] || false
     [[ $output =~ " 1000 " ]] || false
     ! [[ $output =~ " 0 " ]] || false

     run dolt sql -q "SELECT COUNT(*) FROM dolt_log"
     [ $status -eq 0 ]
     [[ $output =~ " 3 " ]] || false
}

@test "sql-server: Run queries on database without ever selecting it" {
     skiponwindows "Missing dependencies"

     start_multi_db_server repo1

     # create table with autocommit on and verify table creation
     dolt sql -q "CREATE TABLE repo2.one_pk (
        pk int,
        PRIMARY KEY (pk))"

     dolt sql -q "INSERT INTO repo2.one_pk VALUES (0), (1), (2)"
     run dolt sql -q "SELECT * FROM repo2.one_pk"
     [ $status -eq 0 ]
     [[ $output =~ " 0 " ]] || false
     [[ $output =~ " 1 " ]] || false
     [[ $output =~ " 2 " ]] || false

     dolt sql -q "UPDATE repo2.one_pk SET pk=3 WHERE pk=2"
     run dolt sql -q "SELECT * FROM repo2.one_pk"
     [ $status -eq 0 ]
     [[ $output =~ " 0 " ]] || false
     [[ $output =~ " 1 " ]] || false
     [[ $output =~ " 3 " ]] || false
     ! [[ $output =~ " 2 " ]] || false

     dolt sql -q "DELETE FROM repo2.one_pk WHERE pk=3"
     run dolt sql -q "SELECT * FROM repo2.one_pk"
     [ $status -eq 0 ]
     [[ $output =~ " 0 " ]] || false
     [[ $output =~ " 1 " ]] || false
     ! [[ $output =~ " 3 " ]] || false

     # Empty commit statements should not error
     dolt sql -q "commit"

     # create a new database and table and rerun
     dolt sql -q "CREATE DATABASE testdb"
     dolt --port $PORT --host 0.0.0.0 --no-tls --use-db '' sql -q "CREATE TABLE testdb.one_pk (
        pk int,
        PRIMARY KEY (pk))"

     dolt sql -q "INSERT INTO testdb.one_pk VALUES (0), (1), (2)"
     run dolt sql -q "SELECT * FROM testdb.one_pk"
     [ $status -eq 0 ]
     [[ $output =~ " 0 " ]] || false
     [[ $output =~ " 1 " ]] || false
     [[ $output =~ " 2 " ]] || false

     dolt sql -q "UPDATE testdb.one_pk SET pk=3 WHERE pk=2"
     run dolt sql -q "SELECT * FROM testdb.one_pk"
     [ $status -eq 0 ]
     [[ $output =~ " 0 " ]] || false
     [[ $output =~ " 1 " ]] || false
     [[ $output =~ " 3 " ]] || false
     ! [[ $output =~ " 2 " ]] || false

     dolt sql -q "DELETE FROM testdb.one_pk WHERE pk=3"
     run dolt sql -q "SELECT * FROM testdb.one_pk"
     [ $status -eq 0 ]
     [[ $output =~ " 0 " ]] || false
     [[ $output =~ " 1 " ]] || false
     ! [[ $output =~ " 3 " ]] || false

     # one last query on insert db.
     dolt sql -q "INSERT INTO repo2.one_pk VALUES (4)"
     run dolt sql -q "SELECT * FROM repo2.one_pk"
     [ $status -eq 0 ]
     [[ $output =~ " 0 " ]] || false
     [[ $output =~ " 1 " ]] || false
     [[ $output =~ " 4 " ]] || false

     # verify changes outside the session
     cd repo2
     run dolt sql -q "show tables"
     [ "$status" -eq 0 ]
     [[ "$output" =~ "one_pk" ]] || false

     run dolt sql -q "select * from one_pk"
     [ "$status" -eq 0 ]
     [[ "$output" =~ " 0 " ]] || false
     [[ "$output" =~ " 1 " ]] || false
     [[ "$output" =~ " 4 " ]] || false
}

@test "sql-server: create database without USE" {
     skiponwindows "Missing dependencies"

     start_multi_db_server repo1

     dolt sql -q "CREATE DATABASE newdb"
     dolt sql -q "CREATE TABLE newdb.test (a int primary key)"
     stop_sql_server 1

     # verify changes outside the session
     cd newdb
     run dolt sql -q "show tables"
     [ "$status" -eq 0 ]
     [[ "$output" =~ "test" ]] || false
}

@test "sql-server: manual commit table can be dropped (validates superschema structure)" {
    skiponwindows "Missing dependencies"

    cd repo1
    start_sql_server repo1

    # check no tables on main
    run dolt sql -q "SHOW Tables"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 0 ]

    # make some changes to main and commit to branch test_branch
    dolt sql -q "
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

    run dolt --verbose-engine-setup --use-db repo1 sql -q "drop table one_pk"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "starting remote mode" ]] || false

    dolt sql -q "call dolt_add('.')"
    dolt sql -q "call dolt_commit('-am', 'Dropped table one_pk')"

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

    dolt --use-db "repo1/feature-branch" sql -q "CREATE TABLE test (
        pk int,
        c1 int,
        PRIMARY KEY (pk)
    )"

    run dolt sql -q "SHOW Tables"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 0 ]

    run dolt --use-db "repo1/feature-branch" sql -q "SHOW Tables"
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

    run dolt --use-db "repo1/$hash" sql -q"select count(*) from test"
    [ $status -eq 0 ]
    [[ $output =~ " 3 " ]] || false

    # fails
    run dolt --use-db "repo1/$hash" sql -q"insert into test values (7)"
    [ $status -ne 0 ]
    [[ $output =~ "read-only" ]] || false

    # dolt checkout can't create new branches on a read only database
    run dolt --use-db "repo1/$hash" sql -q"call dolt_checkout('-b', 'newBranch');"
    [ $status -ne 0 ]
    [[ $output =~ "unable to create new branch in a read-only database" ]] || false

    # server should still be alive after an error
    run dolt --use-db "repo1/$hash" sql -q"select count(*) from test"
    [ $status -eq 0 ]
    [[ $output =~ " 3 " ]] || false
}

@test "sql-server: SET GLOBAL default branch as ref" {
    skiponwindows "Missing dependencies"

    cd repo1
    dolt checkout -b "new"
    dolt checkout main
    start_sql_server repo1

    run dolt sql -q '
    CALL dolt_checkout("new");
    CREATE TABLE t (a int primary key, b int);
    INSERT INTO t VALUES (2,2),(3,3);'

    run dolt sql -q "SHOW Tables"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 0 ]

    dolt sql -q "set GLOBAL repo1_default_branch = 'refs/heads/new'"
    run dolt sql -q "select @@GLOBAL.repo1_default_branch;"
    [ $status -eq 0 ]
    [[ $output =~ "refs/heads/new" ]] || false
    dolt sql -q "select active_branch()"
    run dolt sql -q "select active_branch()"
    [ $status -eq 0 ]
    [[ $output =~ "new" ]] || false
    run dolt sql -q "SHOW Tables"
    [ $status -eq 0 ]
    [[ $output =~ " t " ]] || false
}

@test "sql-server: SET GLOBAL default branch as branch name" {
    skiponwindows "Missing dependencies"

    cd repo1
    dolt checkout -b "new"
    dolt checkout main
    start_sql_server repo1

    run dolt sql -q '
    call dolt_checkout("new");
    CREATE TABLE t (a int primary key, b int);
    INSERT INTO t VALUES (2,2),(3,3);'

    run dolt sql -q "SHOW Tables"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 0 ]

    dolt sql -q "set GLOBAL repo1_default_branch = 'new'"
    run dolt sql -q "select @@GLOBAL.repo1_default_branch;"
    [ $status -eq 0 ]
    [[ $output =~ " new " ]] || false
    run dolt sql -q "select active_branch()"
    [ $status -eq 0 ]
    [[ $output =~ " new " ]] || false
    run dolt sql -q "SHOW Tables"
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
    with mysql.connector.connect(host="127.0.0.1", user="root", port='"$PORT"', database="repo1", connection_timeout=1) as c:
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
    with mysql.connector.connect(host="127.0.0.1", user="root", port='"$PORT"', database="repo1", connection_timeout=1) as c:
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

    dolt sql -q "CREATE TABLE t1(pk bigint primary key auto_increment, val int)"
    dolt sql -q "INSERT INTO t1 (val) VALUES (1)"
    run dolt sql --result-format=csv -q "SELECT * FROM t1"
    [ $status -eq 0 ]
    [[ $output =~ "1,1" ]] || false

    dolt sql -q "INSERT INTO t1 (val) VALUES (2)"
    run dolt sql --result-format=csv -q "SELECT * FROM t1"
    [ $status -eq 0 ]
    [[ $output =~ "1,1" ]] || false
    [[ $output =~ "2,2" ]] || false

    dolt sql -q "call dolt_add('.')"
    dolt sql -q "call dolt_commit('-am', 'table with two values')"
    dolt sql -q "call dolt_branch('new_branch')"

    dolt --use-db repo1/new_branch sql -q "INSERT INTO t1 (val) VALUES (3)"
    run dolt --use-db repo1/new_branch sql --result-format=csv -q "SELECT * FROM t1"
    [ $status -eq 0 ]
    [[ $output =~ "1,1" ]] || false
    [[ $output =~ "2,2" ]] || false
    [[ $output =~ "3,3" ]] || false

    dolt sql -q "INSERT INTO t1 (val) VALUES (4)"
    run dolt sql --result-format=csv -q "SELECT * FROM t1"
    [ $status -eq 0 ]
    [[ $output =~ "1,1" ]] || false
    [[ $output =~ "2,2" ]] || false
    [[ $output =~ "4,4" ]] || false
    ! [[ $output =~ "3,3" ]] || false

    # drop the table on main, should keep counting from 4
    dolt sql -q "drop table t1"
    dolt sql -q "CREATE TABLE t1(pk bigint primary key auto_increment, val int)"
    dolt sql -q "INSERT INTO t1 (val) VALUES (4)"
    run dolt sql --result-format=csv -q "SELECT * FROM t1"
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

    run dolt sql -q "call  dolt_push()"
    [ $status -ne 0 ]
    [[ "$output" =~ "The current branch main has no upstream branch" ]] || false

    dolt sql -q "call dolt_push('--set-upstream', 'origin', 'main')"

    dolt sql -q "call dolt_push()"
}

@test "sql-server: replicate to backup after sql-session commit" {
    skiponwindows "Missing dependencies"

    mkdir bac1
    cd repo1
    dolt remote add backup1 file://../bac1
    dolt config --local --add sqlserver.global.DOLT_REPLICATE_TO_REMOTE backup1
    start_sql_server repo1

    dolt sql -q "CREATE TABLE test (pk int primary key);"
    dolt sql -q "INSERT INTO test VALUES (0),(1),(2)"
    dolt sql -q "CALL DOLT_ADD('.')"
    dolt sql -q "CALL DOLT_COMMIT('-m', 'Step 1');"
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

    dolt sql -q "create database test1"
    run dolt sql -q "show databases"
    [ $status -eq 0 ]
    [[ $output =~ "mysql" ]] || false
    [[ $output =~ "information_schema" ]] || false
    [[ $output =~ "test1" ]] ||	false

    dolt sql -q "create table a(x int)"
    dolt sql -q "call dolt_add('.')"
    dolt sql -q "insert into a values (1), (2)"
    dolt sql -q "call dolt_commit('-a', '-m', 'new table a')"

    dolt sql -q "create database test2"
    dolt --use-db 'test2' sql -q "create table b(x int)"
    dolt --use-db 'test2' sql -q "call dolt_add('.')"
    dolt --use-db 'test2' sql -q "insert into b values (1), (2)"
    dolt --use-db 'test2' sql -q "call dolt_commit('-a', '-m', 'new table b')"
    stop_sql_server 1

    cd test1
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "new table a" ]] || false

    run dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "a" ]] || false

    cd ../test2
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "new table b" ]] || false

    run dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "b" ]] || false

    cd ..

    start_sql_server
    dolt sql -q "create database test3"
    dolt --use-db 'test3' sql -q "create table c(x int)"
    dolt --use-db 'test3' sql -q "call dolt_add('.')"
    dolt --use-db 'test3' sql -q "insert into c values (1), (2)"
    dolt --use-db 'test3' sql -q "call dolt_commit('-a', '-m', 'new table c')"
    dolt sql -q "drop database test2"

    [ -d test3 ]
    [ ! -d test2 ]

    # make sure the databases exist on restart
    stop_sql_server
    start_sql_server
    run dolt sql -q "show databases"
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

    dolt sql -q "create database test1"
    dolt sql -q "create database test2"
    dolt sql -q "create database test3"

    run dolt sql -q "show databases"
    [ $status -eq 0 ]
    [[ $output =~ "mysql" ]] || false
    [[ $output =~ "information_schema" ]] || false
    [[ $output =~ "test1" ]] || false
    [[ $output =~ "test2" ]] || false
    [[ $output =~ "test3" ]] || false

    dolt --use-db test1 sql -q "create table a(x int)"
    dolt --use-db test1 sql -q "call dolt_add('.')"
    dolt --use-db test1 sql -q "insert into a values (1), (2)"
    dolt --use-db test1 sql -q "call dolt_commit('-a', '-m', 'new table a')"

    dolt --use-db test2 sql -q "create table a(x int)"
    dolt --use-db test2 sql -q "call dolt_add('.')"
    dolt --use-db test2 sql -q "insert into a values (3), (4)"
    dolt --use-db test2 sql -q "call dolt_commit('-a', '-m', 'new table a')"

    dolt --use-db test3 sql -q "create table a(x int)"
    dolt --use-db test3 sql -q "call dolt_add('.')"
    dolt --use-db test3 sql -q "insert into a values (5), (6)"
    dolt --use-db test3 sql -q "call dolt_commit('-a', '-m', 'new table a')"

    dolt --use-db test1 sql -q "call dolt_branch('newbranch')"
    run dolt --use-db "test1/newbranch" sql -q "select * from a"
    [ $status -eq 0 ]
    [[ $output =~ " 1 " ]] || false
    [[ $output =~ " 2 " ]] || false

    dolt --use-db test2 sql -q "call dolt_branch('newbranch')"
    run dolt --use-db "test2/newbranch" sql -q "select * from a"
    [ $status -eq 0 ]
    [[ $output =~ " 3 " ]] || false
    [[ $output =~ " 4 " ]] || false

    # uppercase to ensure db names are treated case insensitive
    dolt sql -q "drop database TEST1"

    run dolt --use-db "test1/newbranch" sql -q "select * from a"
    [ $status -ne 0 ]
    [[ "$output" =~ "The provided --use-db test1 does not exist." ]] || false

    # can't drop a branch-qualified database name
    run dolt sql -q "drop database \`test2/newbranch\`"
    [ $status -ne 0 ]
    [[ "$output" =~ "unable to drop revision database: test2/newbranch" ]] || false

    dolt sql -q "drop database TEST2"

    run dolt --use-db "test2/newbranch" sql -q "select * from a"
    [ $status -ne 0 ]
    [[ "$output" =~ "The provided --use-db test2 does not exist." ]] || false

    run dolt --use-db test3 sql -q "select * from a"
    [ $status -eq 0 ]
    [[ $output =~ " 5 " ]] || false
    [[ $output =~ " 6 " ]] || false
}

@test "sql-server: connect to databases case insensitive" {
    skiponwindows "Missing dependencies"

    mkdir no_dolt && cd no_dolt
    start_sql_server

    dolt sql -q "create database Test1"

    run dolt sql -q "show databases"
    [ $status -eq 0 ]
    [[ $output =~ "mysql" ]] || false
    [[ $output =~ "information_schema" ]] || false
    [[ $output =~ "Test1" ]] || false
    dolt sql -q "use test1; create table a(x int);"
    dolt sql -q "use TEST1; insert into a values (1), (2);"
    dolt sql -q "use test1; call dolt_add('.'); call dolt_commit('-a', '-m', 'new table a');"
    dolt sql -q "use test1; call dolt_checkout('-b', 'newbranch');"
    dolt sql -q "use \`TEST1/newbranch\`; select * from a order by x"
    dolt sql -q "use \`test1/newbranch\`; select * from a order by x"
    dolt sql -q "use \`TEST1/NEWBRANCH\`"

    run dolt sql -q "create database test2; use test2; select database();"
    [ $status -eq 0 ]
    [[ $output =~ "test2" ]] || false
    run dolt sql -q "use test2; drop database TEST2; select database();"
    [ $status -eq 0 ]
    [[ $output =~ "NULL" ]] || false
}

@test "sql-server: create and drop database with --data-dir" {
    skiponwindows "Missing dependencies"

    mkdir no_dolt && cd no_dolt
    mkdir db_dir
    start_sql_server_with_args --host 0.0.0.0 --data-dir=db_dir

    dolt --port $PORT --host 0.0.0.0 --no-tls --use-db '' sql -q "create database test1"
    run dolt --port $PORT --host 0.0.0.0 --no-tls --use-db '' sql -q "show databases"
    [ $status -eq 0 ]
    [[ $output =~ "mysql" ]] || false
    [[ $output =~ "information_schema" ]] || false
    [[ $output =~ "test1" ]] || false

    dolt --port $PORT --host 0.0.0.0 --no-tls --use-db test1 sql -q "create table a(x int)"
    dolt --port $PORT --host 0.0.0.0 --no-tls --use-db test1 sql -q "call dolt_add('.')"
    dolt --port $PORT --host 0.0.0.0 --no-tls --use-db test1 sql -q "insert into a values (1), (2)"
    dolt --port $PORT --host 0.0.0.0 --no-tls --use-db test1 sql -q "call dolt_commit('-a', '-m', 'new table a')"
    stop_sql_server 1

    [ -d db_dir/test1 ]

    cd db_dir/test1
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "new table a" ]] || false

    cd ../..

    start_sql_server_with_args --host 0.0.0.0 --data-dir=db_dir
    dolt --port $PORT --host 0.0.0.0 --no-tls --use-db '' sql -q "create database test3"
    dolt --port $PORT --host 0.0.0.0 --no-tls --use-db test3 sql -q "create table c(x int)"
    dolt --port $PORT --host 0.0.0.0 --no-tls --use-db test3 sql -q "call dolt_add('.')"
    dolt --port $PORT --host 0.0.0.0 --no-tls --use-db test3 sql -q "insert into c values (1), (2)"
    dolt --port $PORT --host 0.0.0.0 --no-tls --use-db test3 sql -q "call dolt_commit('-a', '-m', 'new table c')"
    dolt --port $PORT --host 0.0.0.0 --no-tls --use-db '' sql -q "drop database test1"
    stop_sql_server 1

    [ -d db_dir/test3 ]
    [ ! -d db_dir/test1 ]

    # make sure the databases exist on restart
    stop_sql_server
    start_sql_server_with_args --host 0.0.0.0 --data-dir=db_dir
    run dolt --port $PORT --host 0.0.0.0 --no-tls --use-db '' sql -q "show databases"
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

    dolt sql -q "create database test1"

    # Error on creation, already exists
    run dolt sql -q "create database test1"
    [ $status -ne 0 ]
    [[ $output =~ exists ]] || false

    # Files / dirs in the way
    run dolt sql -q "create database dir_exists"
    [ $status -ne 0 ]
    [[ $output =~ exists ]] || false

    run dolt sql -q "create database file_exists"
    [ $status -ne 0 ]
    [[ $output =~ exists ]] || false
}

@test "sql-server: create database with existing repo" {
    skiponwindows "Missing dependencies"

    cd repo1
    start_sql_server

    dolt sql -q "create database test1"
    run dolt sql -q "show databases"
    [ $status -eq 0 ]
    [[ $output =~ "mysql" ]] || false
    [[ $output =~ "information_schema" ]] || false
    [[ $output =~ "test1" ]] || false
    [[ $output =~ "repo1" ]] || false

    dolt --use-db test1 sql -q "create table a(x int)"
    dolt --use-db test1 sql -q "call dolt_add('.')"
    dolt --use-db test1 sql -q "insert into a values (1), (2)"
    dolt --use-db test1 sql -q "call dolt_commit('-a', '-m', 'new table a')"
    dolt --use-db repo1 sql -q "create database test2"
    dolt --use-db test2 sql -q "create table b(x int)"
    dolt --use-db test2 sql -q "call dolt_add('.')"
    dolt --use-db test2 sql -q "insert into b values (1), (2)"
    dolt --use-db test2 sql -q "call dolt_commit('-a', '-m', 'new table b')"
    stop_sql_server 1

    cd test1
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "new table a" ]] || false

    run dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "a" ]] || false

    cd ../test2
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "new table b" ]] || false

    run dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "b" ]] || false

    cd ../
    # make sure the databases exist on restart
    start_sql_server
    run dolt --use-db repo1 sql -q "show databases"
    [ $status -eq 0 ]
    [[ $output =~ "mysql" ]] || false
    [[ $output =~ "information_schema" ]] || false
    [[ $output =~ "test1" ]] || false
    [[ $output =~ "repo1" ]] || false
    [[ $output =~ "test2" ]] || false
}

@test "sql-server: fetch uses database data dir from different working directory" {
    skiponwindows "Missing dependencies"

    mkdir remote1
    cd repo2
    dolt remote add remote1 file://../remote1
    dolt push -u remote1 main

    cd ..
    rm -rf repo1
    mkdir -p dbs/repo1 && cd dbs
    dolt clone file://./../remote1 repo1
    cd repo1
    dolt remote add remote1 file://../remote1

    cd ../../repo2
    dolt sql -q "create table test (a int)"
    dolt add .
    dolt commit -am "new commit"
    dolt push -u remote1 main

    cd ../dbs
    DATA_DIR=$(pwd)
    cd ..

    echo "
data_dir: $DATA_DIR
" > server.yaml

    start_sql_server_with_config repo1 server.yaml

    dolt --port $PORT --host 0.0.0.0 --no-tls --use-db repo1 sql -q "call dolt_fetch()"
}

# bats test_tags=no_lambda
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
    start_sql_server_with_args -ltrace --no-auto-commit

    run expect $BATS_TEST_DIRNAME/sql-server-mysql.expect $PORT repo1
    [ "$status" -eq 0 ]
}

@test "sql-server: sql-server info cleanup" {
    cd repo1
    start_sql_server
    stop_sql_server
    start_sql_server
    stop_sql_server
}

@test "sql-server: sql-server info database" {
    cd repo1
    start_sql_server
    [[ -f "$PWD/.dolt/sql-server.info" ]] || false

    PORT=$( definePORT )
    run dolt sql-server -P $PORT --socket "dolt.$PORT.sock"
    [ "$status" -eq 1 ]
}

@test "sql-server: sql-server sets permissions on sql-server.info" {
    cd repo1
    ! [[ -f "$PWD/.dolt/sql-server.info" ]] || false
    start_sql_server
    [[ -f "$PWD/.dolt/sql-server.info" ]] || false


    if [[ `uname` == 'Darwin' ]]; then
      run stat -x "$PWD/.dolt/sql-server.info"
      [[ "$output" =~ "(0600/-rw-------)" ]] || false
    else
      run stat "$PWD/.dolt/sql-server.info"
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
    dolt sql -q "create database newdb"

    # Verify that we can't start a sql-server from the new database dir
    cd newdb
    PORT=$( definePORT )
    run dolt sql-server -P $PORT --socket "dolt.$PORT.sock"
    [ "$status" -eq 1 ]
}

# When the deprecated --user argument is specified to sql-server, we expect a helpful error message
# to be displayed and for the sql-server command to fail.
#
# TODO: After six months or so (July 2025), completely remove support for the user/pass
#       args, so that the sql-server will fail with an unknown args error instead.
@test "sql-server: deprecated --user arg fails with a helpful error message" {
    PORT=$( definePORT )
    run dolt sql-server --port $PORT -u temp1
    [ $status -ne 0 ]
    [[ $output =~ "--user and --password have been removed from the sql-server command." ]] || false
    [[ $output =~ "Create users explicitly with CREATE USER and GRANT statements instead." ]] || false

    # Assert that there is no root user
    run dolt sql -q "select user, host from mysql.user where user='root';"
    [ $status -eq 0 ]
    [[ $output =~ "root" ]] || false
    ! [[ $output =~ "temp1" ]] || false
}

# When the deprecated user section is included in a config.yaml file, we expect the server to
# start up, but emit a warning about the user section not being used anymore.
#
# TODO: After six months or so (July 2025), completely remove support for the user section
#       so that server startup will fail instead.
@test "sql-server: deprecated user section in config.yaml logs a warning message" {
    cd repo1
    PORT=$( definePORT )

    echo "
log_level: debug

user:
  name: dolt
  password: pass123

listener:
  host: localhost
  port: $PORT
  max_connections: 10
  socket: dolt.$PORT.sock

behavior:
  autocommit: true
  dolt_transaction_commit: true" > server.yaml

    dolt sql-server --config server.yaml > log.txt 2>&1 &
    SERVER_PID=$!
    sleep 3

    run grep 'user and password are no longer supported in sql-server configuration files' log.txt
    [ "$status" -eq 0 ]
    run grep 'Use CREATE USER and GRANT statements to manage user accounts' log.txt
    [ "$status" -eq 0 ]
}

@test "sql-server: start server without socket flag should set default socket path" {
    skiponwindows "unix socket is not available on Windows"
    cd repo2
    DEFAULT_DB="repo2"
    PORT=$( definePORT )

    dolt sql-server --port $PORT >> log.txt 2>&1 &
    SERVER_PID=$!
    wait_for_connection $PORT 8500

    cat log.txt

    run dolt sql -q "select 1 as col1"
    [ $status -eq 0 ]
    [[ $output =~ col1 ]] || false
    [[ $output =~ " 1 " ]] || false

    run grep '"/tmp/mysql.sock"' log.txt
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]

    run dolt sql -q "select 1"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1" ]] || false
}

@test "sql-server: start server with socket option undefined should set default socket path" {
    skiponwindows "unix socket is not available on Windows"
    cd repo2
    DEFAULT_DB="repo2"
    PORT=$( definePORT )

    dolt sql-server --port $PORT --socket > log.txt 2>&1 &
    SERVER_PID=$!
    wait_for_connection $PORT 8500

    run dolt sql -q "select 1 as col1"
    [ $status -eq 0 ]
    [[ $output =~ col1 ]] || false
    [[ $output =~ " 1 " ]] || false

    run grep '"/tmp/mysql.sock"' log.txt
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
    dolt sql-server --port=$secondPORT --socket="$REPO_NAME/mysql.sock" > log.txt 2>&1 &
    SECOND_SERVER_PID=$!
    run wait_for_connection $secondPORT 8500
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

listener:
  host: localhost
  port: $PORT
  max_connections: 10
  socket: dolt.$PORT.sock

behavior:
  autocommit: true
  dolt_transaction_commit: true" > server.yaml

    dolt sql-server --config server.yaml > log.txt 2>&1 &
    SERVER_PID=$!
    wait_for_connection $PORT 8500

    run dolt sql -q "select 1 as col1"
    [ $status -eq 0 ]
    [[ $output =~ col1 ]] || false
    [[ $output =~ " 1 " ]] || false

    run dolt sql -q "select @@dolt_transaction_commit"
    [ $status -eq 0 ]
    [[ $output =~ " 1 " ]] || false

    run grep "dolt.$PORT.sock" log.txt
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]
}

@test "sql-server: sigterm running server and restarting works correctly" {
    start_sql_server

    kill -9 $SERVER_PID

    run ls .dolt
    [[ "$output" =~ "sql-server.info" ]] || false

    start_sql_server
    run dolt sql -q "select 1 as col1"
    [ $status -eq 0 ]
    [[ $output =~ col1 ]] || false
    [[ $output =~ " 1 " ]] || false
}

@test "sql-server: create a database when no current database is set" {
    mkdir new_format && cd new_format
    dolt init

    PORT=$( definePORT )
    dolt sql-server --host 0.0.0.0 --port=$PORT --socket "dolt.$PORT.sock" &
    SERVER_PID=$! # will get killed by teardown_common
    wait_for_connection $PORT 8500

    dolt sql -q "create database mydb1;"
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

    dolt sql -q "CREATE DATABASE mydb1"
    dolt sql -q "CREATE DATABASE mydb2"

    [ -d mydb1 ]
    [ -d mydb2 ]

    rm -rf mydb2

    run dolt sql -q "SHOW DATABASES"
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
    dolt sql -q "CREATE DATABASE doltdb"
    run dolt sql -q "SHOW DATABASES"
    [[ "$output" =~ "mydb" ]] || false
    [[ "$output" =~ "doltdb" ]] || false

    dolt sql -q "DROP DATABASE mydb"
    stop_sql_server 1
    [ ! -d .dolt ]

    run dolt sql -q "SHOW DATABASES"
    [[ ! "$output" =~ "mydb" ]] || false
    [[ "$output" =~ "doltdb" ]] || false

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
    dolt sql -q "DROP DATABASE mydb;"

    run dolt sql -q "SHOW DATABASES"
    [[ ! "$output" =~ "mydb" ]] || false

    [ ! -d .dolt ]

    run dolt sql -q "SHOW DATABASES"
    [[ ! "$output" =~ "mydb" ]] || false
}

@test "sql-server: create database, drop it, and then create it again" {
    skiponwindows "Missing dependencies"

    mkdir mydbs
    cd mydbs

    start_sql_server >> server_log.txt 2>&1
    dolt sql -q "CREATE DATABASE mydb1;"
    [ -d mydb1 ]

    dolt sql -q "DROP DATABASE mydb1;"
    [ ! -d mydb1 ]

    dolt sql -q "CREATE DATABASE mydb1;"
    [ -d mydb1 ]

    run dolt sql -q "SHOW DATABASES;"
    [ $status -eq 0 ]
    [[ "$output" =~ "mydb1" ]] || false
}

@test "sql-server: dropping database with '-' in it but replaced with underscore" {
    skiponwindows "Missing dependencies"
    export DOLT_DBNAME_REPLACE="true"
    mkdir my-db
    cd my-db
    dolt init
    cd ..

    run dolt sql -q "SHOW DATABASES"
    [[ "$output" =~ "my_db" ]] || false

    start_sql_server >> server_log.txt 2>&1
    dolt sql -q "DROP DATABASE my_db;"

    run dolt sql -q "SHOW DATABASES"
    [[ ! "$output" =~ "my_db" ]] || false

    [ ! -d my-db ]
}

@test "sql-server: dropping database with '-' in it" {
    skiponwindows "Missing dependencies"

    mkdir my-db
    cd my-db
    dolt init
    cd ..

    run dolt sql -q "SHOW DATABASES"
    [[ "$output" =~ "my-db" ]] || false

    start_sql_server >> server_log.txt 2>&1
    dolt sql -q "DROP DATABASE \`my-db\`;"

    run dolt sql -q "SHOW DATABASES"
    [[ ! "$output" =~ "my-db" ]] || false

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
    run dolt sql --result-format csv -q "SELECT GET_LOCK('mylock', 1000); SELECT IS_FREE_LOCK('mylock');"
    [ $status -eq 0 ]
    [[ "$output" =~ "$EXPECTED" ]] || false

    EXPECTED=$(echo -e "IS_FREE_LOCK('mylock')\n1")
    run dolt sql --result-format csv -q "SELECT IS_FREE_LOCK('mylock');"
    [ $status -eq 0 ]
    [[ "$output" =~ "$EXPECTED" ]] || false
}

@test "sql-server: binary literal is printed as hex string for utf8 charset result set" {
    cd repo1
    start_sql_server
    dolt sql -q "SET character_set_results = utf8; CREATE TABLE mapping(branch_id binary(16) PRIMARY KEY, user_id binary(16) NOT NULL, company_id binary(16) NOT NULL);"

    run dolt sql -q "EXPLAIN PLAN SELECT m.* FROM mapping m WHERE user_id = uuid_to_bin('1c4c4e33-8ad7-4421-8450-9d5182816ac3');"
    [ $status -eq 0 ]
    [[ "$output" =~ "0x1C4C4E338AD7442184509D5182816AC3" ]] || false
}

@test "sql-server: CALL DOLT_BRANCH -m on session active branch (dolt sql-server)" {
    cd repo1
    dolt branch other
    start_sql_server
    run dolt sql -q "call dolt_checkout('other'); call dolt_branch('-m', 'other', 'newOther'); select active_branch();"
    [ $status -eq 0 ]
    [[ "$output" =~ "newOther" ]] || false
    run dolt branch
    [ $status -eq 0 ]
    [[ "$output" =~ "newOther" ]] || false
    [[ "$output" =~ "main" ]] || false
    [[ ! "$output" =~ "other" ]] || false
}

@test "sql-server: empty server can be connected to using sql with no args" {
    baseDir=$(mktemp -d)
    cd $baseDir

    start_sql_server

    run dolt sql -q "select current_user"
    [ $status -eq 0 ]
    [[ "$output" =~ "__dolt_local_user__@localhost" ]] || false
}

@test "sql-server: --data-dir respected when creating server lock file" {
    baseDir=$(mktemp -d)

    PORT=$( definePORT )
    dolt sql-server --data-dir=$baseDir --host 0.0.0.0 --port=$PORT &
    SERVER_PID=$!
    SQL_USER='root'
    wait_for_connection $PORT 8500

    run dolt --data-dir=$baseDir sql -q "select current_user"
    [ $status -eq 0 ]
    [[ "$output" =~ "__dolt_local_user__@localhost" ]] || false

    # We create a database here so that the server has exclusive access to this database.
    # Starting another server attempting to serve it will fail.
    dolt --data-dir=$baseDir sql -q "create database mydb"

    cd "$baseDir"
    run dolt sql-server
    [ $status -eq 1 ]

    run dolt sql -q "select current_user"
    [ $status -eq 0 ]
    [[ "$output" =~ "__dolt_local_user__@localhost" ]] || false
}

@test "sql-server: --data-dir used to load persisted system variables" {
    prevWd=$(pwd)
    baseDir=$(mktemp -d)

    # Initialize a Dolt directory and persist a global variable
    cd $baseDir
    dolt init
    dolt sql -q "SET @@PERSIST.log_bin=1;"
    run cat .dolt/config.json
    [ $status -eq 0 ]
    [[ "$output" =~ "\"sqlserver.global.log_bin\":\"1\"" ]] || false

    # Start a sql-server and make sure the persisted global was loaded
    cd $prevWd
    PORT=$( definePORT )
    dolt sql-server --data-dir=$baseDir --host 0.0.0.0 --port=$PORT &
    SERVER_PID=$!
    SQL_USER='root'
    wait_for_connection $PORT 7500

    run dolt --data-dir=$baseDir sql -q "select @@log_bin"
    [ $status -eq 0 ]
    [[ "$output" =~ "1" ]] || false
}

# Tests that when a Dolt sql-server is running from a directory that hasn't been initialized as a dolt
# database, that the CLI gives good error messages.
@test "sql-server: dolt CLI commands give good error messages in an uninitialized sql-server dir" {
    # Start a sql-server from an uninitialized directory
    PORT=$( definePORT )
    dolt sql-server --host 0.0.0.0 --port=$PORT &
    SERVER_PID=$!
    SQL_USER='root'
    wait_for_connection $PORT 7500

    # Test various commands to make sure they give a good error message
    run dolt pull
    [ $status -ne 0 ]
    [[ "$output" =~ "The current directory is not a valid dolt repository." ]] || false

    run dolt ls
    [ $status -ne 0 ]
    [[ "$output" =~ "The current directory is not a valid dolt repository." ]] || false

    run dolt rebase
    [ $status -ne 0 ]
    [[ "$output" =~ "The current directory is not a valid dolt repository." ]] || false

    run dolt stash
    [ $status -ne 0 ]
    [[ "$output" =~ "The current directory is not a valid dolt repository." ]] || false

    run dolt docs print
    [ $status -ne 0 ]
    [[ "$output" =~ "The current directory is not a valid dolt repository." ]] || false

    run dolt rebase
    [ $status -ne 0 ]
    [[ "$output" =~ "The current directory is not a valid dolt repository." ]] || false

    run dolt tag
    [ $status -ne 0 ]
    [[ "$output" =~ "The current directory is not a valid dolt repository." ]] || false

    run dolt remote
    [ $status -ne 0 ]
    [[ "$output" =~ "The current directory is not a valid dolt repository." ]] || false

    run dolt push
    [ $status -ne 0 ]
    [[ "$output" =~ "The current directory is not a valid dolt repository." ]] || false

    # dolt init has a different error message, since the sql-server won't pick up the initialized directory
    run dolt init
    [ $status -ne 0 ]
    [[ "$output" =~ "Detected that a Dolt sql-server is running from this directory." ]] || false
    [[ "$output" =~ "Stop the sql-server before initializing this directory as a Dolt database." ]] || false
}


@test "sql-server: fail to start when multiple data dirs found" {
    skiponwindows "Missing dependencies"

    mkdir datadir1
    mkdir datadir2

    # This file is legit, and would work if there was no --data-dir on the cli.
    cat > config.yml <<EOF
listener:
  host: "0.0.0.0"
  port: 4444
data_dir: ./datadir1
EOF
    run dolt --data-dir datadir2 sql-server --config ./config.yml
    [ $status -eq 1 ]
    [[ "$output" =~ "cannot specify both global --data-dir argument and --data-dir in sql-server config" ]] || false

    run dolt sql-server --data-dir datadir2 --config ./config.yml
    [ $status -eq 1 ]
    [[ "$output" =~ "--data-dir specified in both config file and command line" ]] || false

    run dolt --data-dir datadir1 sql-server --data-dir datadir2
    [ $status -eq 1 ]
    [[ "$output" =~ "cannot specify both global --data-dir argument and --data-dir in sql-server config" ]] || false
}

# This is really a test of the dolt_Branches system table, but due to needing a server with multiple dirty branches
# it was easier to test it with a sql-server.
@test "sql-server: dirty branches listed properly in dolt_branches table" {
    skiponwindows "Missing dependencies"

    cd repo1
    dolt checkout main
    dolt branch br1 # Will be a clean commit, ahead of main.
    dolt branch br2 # will be a dirty branch, on main.
    dolt branch br3 # will be a dirty branch, on br1
    start_sql_server repo1

    dolt --use-db "repo1" --branch br1 sql -q "CREATE TABLE tbl (i int primary key)"
    dolt --use-db "repo1" --branch br1 sql -q "CALL DOLT_COMMIT('-Am', 'commit it')"

    dolt --use-db "repo1" --branch br2 sql -q "CREATE TABLE tbl (j int primary key)"

    # Fast forward br3 to br1, then make it dirty.
    dolt --use-db "repo1" --branch br3 sql -q "CALL DOLT_MERGE('br1')"
    dolt --use-db "repo1" --branch br3 sql -q "CREATE TABLE othertbl (k int primary key)"

    stop_sql_server 1 && sleep 0.5

    run dolt sql -q "SELECT name,dirty FROM dolt_branches"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "br1  | false" ]] || false
    [[ "$output" =~ "br2  | true " ]] || false
    [[ "$output" =~ "br3  | true" ]] || false
    [[ "$output" =~ "main | false" ]] || false

    # Verify that the dolt_branches table show the same output, regardless of the checked out branch.
    dolt checkout br1
    run dolt sql -q "SELECT name,dirty FROM dolt_branches"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "br1  | false" ]] || false
    [[ "$output" =~ "br2  | true " ]] || false
    [[ "$output" =~ "br3  | true" ]] || false
    [[ "$output" =~ "main | false" ]] || false
}

@test "sql-server: warning log on forced __dolt_local_user__ drop after restart" {
    skiponwindows "Missing dependencies"

    cd repo1
    start_sql_server > server_log.txt 2>&1

    # use root account to avoid err on drop
    run dolt -u root -p "" sql -q "drop user if exists __dolt_local_user__@localhost"
    [ $status -eq 0 ]
    run dolt -u root -p "" sql -q "create user __dolt_local_user__@localhost"
    [ $status -eq 0 ]
    run dolt sql
    [ $status -ne 0 ]
    [[ "$output" =~ "Error 1045 (28000): Access denied for user" ]] || false

    stop_sql_server 1 && sleep 0.5
    start_sql_server > server_log.txt 2>&1 && sleep 0.5

    run grep -F "Dropping persisted '__dolt_local_user__@localhost' because this account name is reserved for Dolt" server_log.txt
    [ $status -eq 0 ]
}


@test "sql-server: client binary-as-hex behavior works with server connections" {
    skiponwindows "Missing dependencies"
    which expect > /dev/null || skip "expect not available"
    
    cd repo1
    
    # Setup: Create table with binary data and commit it
    dolt sql -q "DROP TABLE IF EXISTS binary_test; CREATE TABLE binary_test (pk INT PRIMARY KEY, vb VARBINARY(20)); INSERT INTO binary_test VALUES (1, 0x0A000000), (2, 'abc');"
    dolt add .
    dolt commit -m "Add binary test data"
    
    start_sql_server repo1
    
    # 1. Test default interactive behavior (should show hex by default)
    run expect "$BATS_TEST_DIRNAME/binary-as-hex-server-default-interactive.expect" $PORT
    [ $status -eq 0 ]
    
    # 2. Test --binary-as-hex flag in interactive mode (should show hex)
    run expect "$BATS_TEST_DIRNAME/binary-as-hex-server-interactive.expect" $PORT
    [ $status -eq 0 ]
    
    # 3. Test --skip-binary-as-hex flag in interactive mode (should show raw)
    run expect "$BATS_TEST_DIRNAME/binary-as-hex-skip-flag-interactive.expect" $PORT
    [ $status -eq 0 ]
    
    # 4. Test flag precedence: --skip-binary-as-hex overrides --binary-as-hex
    run expect "$BATS_TEST_DIRNAME/binary-as-hex-flag-precedence-interactive.expect" $PORT
    [ $status -eq 0 ]
    
    # 5. Test non-interactive server behavior with -q flag (should show raw by default)
    run dolt --host 127.0.0.1 --port $PORT --no-tls sql -q "USE repo1; SELECT vb FROM binary_test WHERE pk = 1"
    [ $status -eq 0 ]
    ! [[ "$output" =~ "0x0A000000" ]] || false
    
    # 6. Test non-interactive server behavior with --binary-as-hex flag
    run dolt --host 127.0.0.1 --port $PORT --no-tls sql --binary-as-hex -q "USE repo1; SELECT vb FROM binary_test WHERE pk = 1"
    [ $status -eq 0 ]
    [[ "$output" =~ "0x0A000000" ]] || false
    
    # 7. Test non-interactive server behavior with printable data
    run dolt --host 127.0.0.1 --port $PORT --no-tls sql -q "USE repo1; SELECT vb FROM binary_test WHERE pk = 2"
    [ $status -eq 0 ]
    [[ "$output" =~ "abc" ]] || false
    
    # 8. Test non-interactive server flag precedence
    run dolt --host 127.0.0.1 --port $PORT --no-tls sql --binary-as-hex --skip-binary-as-hex -q "USE repo1; SELECT vb FROM binary_test WHERE pk = 2"
    [ $status -eq 0 ]
    [[ "$output" =~ "abc" ]] || false
    ! [[ "$output" =~ "0x616263" ]] || false
    
    # 9. Test non-interactive server behavior with -q flag (should show raw by default)
    run dolt --host 127.0.0.1 --port $PORT --no-tls sql -q "USE repo1; SELECT vb FROM binary_test WHERE pk = 1"
    [ $status -eq 0 ]
    ! [[ "$output" =~ "0x0A000000" ]] || false
    
    # 10. Test non-interactive server behavior with -q and --binary-as-hex flags
    run dolt --host 127.0.0.1 --port $PORT --no-tls sql --binary-as-hex -q "USE repo1; SELECT vb FROM binary_test WHERE pk = 1"
    [ $status -eq 0 ]
    [[ "$output" =~ "0x0A000000" ]] || false
    
    # 11. Test non-interactive server -q flag precedence
    run dolt --host 127.0.0.1 --port $PORT --no-tls sql --binary-as-hex --skip-binary-as-hex -q "USE repo1; SELECT vb FROM binary_test WHERE pk = 2"
    [ $status -eq 0 ]
    [[ "$output" =~ "abc" ]] || false
    ! [[ "$output" =~ "0x616263" ]] || false
    
    # 12. Test non-filtered SELECT with default behavior (should show raw by default)
    run dolt --host 127.0.0.1 --port $PORT --no-tls sql -q "USE repo1; SELECT * FROM binary_test"
    [ $status -eq 0 ]
    ! [[ "$output" =~ "0x0A000000" ]] || false
    ! [[ "$output" =~ "0x616263" ]] || false
    [[ "$output" =~ "abc" ]] || false
    
    # 13. Test non-filtered SELECT with --binary-as-hex flag
    run dolt --host 127.0.0.1 --port $PORT --no-tls sql --binary-as-hex -q "USE repo1; SELECT * FROM binary_test"
    [ $status -eq 0 ]
    [[ "$output" =~ "0x0A000000" ]] || false
    [[ "$output" =~ "0x616263" ]] || false
    
    # 14. Test non-filtered SELECT with --skip-binary-as-hex flag
    run dolt --host 127.0.0.1 --port $PORT --no-tls sql --skip-binary-as-hex -q "USE repo1; SELECT * FROM binary_test"
    [ $status -eq 0 ]
    ! [[ "$output" =~ "0x0A000000" ]] || false
    ! [[ "$output" =~ "0x616263" ]] || false
    [[ "$output" =~ "abc" ]] || false
    
    stop_sql_server 1
}


