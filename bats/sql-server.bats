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

@test "multi-client" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    cd repo1
    start_sql_multi_user_server repo1

    cd $BATS_TEST_DIRNAME
    let PORT="$$ % (65536-1024) + 1024"
    python3 server_multiclient_test.py $PORT
}

@test "test autocommit" {
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

@test "test basic querying via dolt sql-server" {
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

@test "test multiple queries on the same connection" {
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

@test "test manual commit" {
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
    SET @@repo1_head=commit('test commit message');
    INSERT INTO dolt_branches (name,hash) VALUES ('test_branch', @@repo1_head);"

    # validate new branch was created
    server_query 0 "SELECT name,latest_commit_message FROM dolt_branches" "name,latest_commit_message\nmaster,Initialize data repository\ntest_branch,test commit message"

    # validate no tables on master still
    server_query 0 "SHOW tables" ""

    # validate tables and data on test_branch
    server_query 0 "SET @@repo1_head=hashof('test_branch');SHOW tables" ";Table\none_pk"
    server_query 0 "SET @@repo1_head=hashof('test_branch');SELECT * FROM one_pk ORDER by pk" ";pk,c1,c2\n0,None,None\n1,1,None\n2,2,2\n3,3,3"
}

@test "test multi db with use statements" {
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


@test "test multi db without use statements" {
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