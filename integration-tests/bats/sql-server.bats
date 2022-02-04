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

@test "sql-server: read-only flag prevents modification" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    cd repo1

    DEFAULT_DB="$1"
    let PORT="$$ % (65536-1024) + 1024"
    echo "
  read_only: true" > server.yaml
    start_sql_server_with_config repo1 server.yaml

    # No tables at the start
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "No tables in working set" ]] || false

    # attempt to create table (autocommit on), expect either some exception
    server_query repo1 1 "CREATE TABLE i_should_not_exist (
            c0 INT
        )" "" "not authorized"

    # Expect that there are still no tables
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "No tables in working set" ]] || false
}

@test "sql-server: read-only flag still allows select" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    cd repo1
    dolt sql -q "create table t(c0 int)"
    dolt sql -q "insert into t values (1)"

    DEFAULT_DB="$1"
    let PORT="$$ % (65536-1024) + 1024"
    echo "
  read_only: true" > server.yaml
    start_sql_server_with_config repo1 server.yaml

    # make a select query
    server_query repo1 1 "select * from t" "c0\n1"
}

@test "sql-server: read-only flag prevents dolt_commit" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    cd repo1

    DEFAULT_DB="$1"
    let PORT="$$ % (65536-1024) + 1024"
    echo "
  read_only: true" > server.yaml
    start_sql_server_with_config repo1 server.yaml

    # make a dolt_commit query
    skip "read-only flag does not prevent dolt_commit"
    server_query repo1 1 "select dolt_commit('--allow-empty', '-m', 'msg')" "" "not authorized: user does not have permission: write"
}

@test "sql-server: test command line modification" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    cd repo1
    start_sql_server repo1

    # No tables at the start
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "No tables in working set" ]] || false

    server_query repo1 1 "CREATE TABLE one_pk (
        pk BIGINT NOT NULL,
        c1 BIGINT,
        c2 BIGINT,
        PRIMARY KEY (pk)
    )" ""
    run dolt ls

    [ "$status" -eq 0 ]
    [[ "$output" =~ "one_pk" ]] || false

    # Add rows on the command line
    dolt sql -q "insert into one_pk values (1,1,1)"

    server_query repo1 1 "SELECT * FROM one_pk ORDER by pk" "pk,c1,c2\n1,1,1"

    # Test import as well (used by doltpy)
    echo 'pk,c1,c2' > import.csv
    echo '2,2,2' >> import.csv
    dolt table import -u one_pk import.csv
    server_query repo1 1 "SELECT * FROM one_pk ORDER by pk" "pk,c1,c2\n1,1,1\n2,2,2"
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

    # check that only main branch exists
    server_query repo1 0 "SELECT name, latest_commit_message FROM dolt_branches" "name,latest_commit_message\nmain,Initialize data repository"

    # check that new connections are set to main by default
    server_query repo1 0 "SELECT name, latest_commit_message FROM dolt_branches WHERE hash = @@repo1_head" "name,latest_commit_message\nmain,Initialize data repository"

    # check no tables on main
    server_query repo1 0 "SHOW Tables" ""

    # make some changes to main and commit to branch test_branch
    multi_query repo1 0 "
    SET @@repo1_head=hashof('main');
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
    server_query repo1 0 "SELECT name,latest_commit_message FROM dolt_branches" "name,latest_commit_message\nmain,Initialize data repository\ntest_branch,test commit message"

    # Check that the author information is correct.
    server_query repo1 0 "SELECT latest_committer,latest_committer_email FROM dolt_branches ORDER BY latest_commit_date DESC LIMIT 1" "latest_committer,latest_committer_email\nJohn Doe,john@example.com"

    # validate no tables on main still
    server_query repo1 0 "SHOW tables" ""

    # validate tables and data on test_branch
    server_query repo1 0 "SET @@repo1_head=hashof('test_branch');SHOW tables" ";Table\none_pk"
    server_query repo1 0 "SET @@repo1_head=hashof('test_branch');SELECT * FROM one_pk ORDER by pk" ";pk,c1,c2\n0,None,None\n1,1,None\n2,2,2\n3,3,3"
}

@test "sql-server: test manual merge" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    cd repo1
    start_sql_server repo1

    # check that only main branch exists
    server_query repo1 0 "SELECT name, latest_commit_message FROM dolt_branches" "name,latest_commit_message\nmain,Initialize data repository"

    # check that new connections are set to main by default
    server_query repo1 0 "SELECT name, latest_commit_message FROM dolt_branches WHERE hash = @@repo1_head" "name,latest_commit_message\nmain,Initialize data repository"

    # check no tables on main
    server_query repo1 0 "SHOW Tables" ""

    # make some changes to main and commit to branch test_branch
    multi_query repo1 0 "
    SET @@repo1_head=hashof('main');
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
    server_query repo1 0 "SELECT name,latest_commit_message FROM dolt_branches" "name,latest_commit_message\nmain,Initialize data repository\ntest_branch,test commit message"

    # validate no tables on main still
    server_query repo1 0 "SHOW tables" ""

    # Merge the test_branch into main. This should a fast forward merge.
    multi_query repo1 0 "
    SET @@repo1_head = merge('test_branch');
    INSERT INTO dolt_branches (name, hash) VALUES('main', @@repo1_head);"

    # Validate tables and data on main
    server_query repo1 0 "SET @@repo1_head=hashof('main');SHOW tables" ";Table\none_pk"
    server_query repo1 0 "SET @@repo1_head=hashof('main');SELECT * FROM one_pk ORDER by pk" ";pk,c1,c2\n0,None,None\n1,1,None\n2,2,2\n3,3,3"

    # Validate the commit main matches that of test_branch (this is a fast forward) by matching commit hashes.
    server_query repo1 0 "select COUNT(hash) from dolt_branches where hash IN (select hash from dolt_branches WHERE name = 'test_branch')" "COUNT(hash)\n2"

    # make some changes to test_branch and commit. Make some changes to main and commit. Merge.
    multi_query repo1 0 "
    SET @@repo1_head=hashof('main');
    UPDATE one_pk SET c1=10 WHERE pk=2;
    SET @@repo1_head=commit('-m', 'Change c 1 to 10');
    INSERT INTO dolt_branches (name,hash) VALUES ('main', @@repo1_head);

    SET @@repo1_head=hashof('test_branch');
    INSERT INTO one_pk (pk,c1,c2) VALUES (4,4,4);
    SET @@repo1_head=commit('-m', 'add 4');
    INSERT INTO dolt_branches (name,hash) VALUES ('test_branch', @@repo1_head);"

    multi_query repo1 0 "
    SET @@repo1_head=hashof('main');
    SET @@repo1_head=merge('test_branch');
    INSERT INTO dolt_branches (name, hash) VALUES('main', @@repo1_head);"

    # Validate tables and data on main
    server_query repo1 0 "SET @@repo1_head=hashof('main');SHOW tables" ";Table\none_pk"
    server_query repo1 0 "SET @@repo1_head=hashof('main');SELECT * FROM one_pk ORDER by pk" ";pk,c1,c2\n0,None,None\n1,1,None\n2,10,2\n3,3,3\n4,4,4"

    # Validate the a merge commit was written by making sure the hashes of the two branches don't match
    server_query repo1 0 "select COUNT(hash) from dolt_branches where hash IN (select hash from dolt_branches WHERE name = 'test_branch')" "COUNT(hash)\n1"
}


@test "sql-server: test manual squash" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    cd repo1
    start_sql_server repo1

    # check that only main branch exists
    server_query repo1 0 "SELECT name, latest_commit_message FROM dolt_branches" "name,latest_commit_message\nmain,Initialize data repository"

    # check that new connections are set to main by default
    server_query repo1 0 "SELECT name, latest_commit_message FROM dolt_branches WHERE hash = @@repo1_head" "name,latest_commit_message\nmain,Initialize data repository"

    # check no tables on main
    server_query repo1 0 "SHOW Tables" ""

    # make some changes to main and commit to branch test_branch
    multi_query repo1 0 "
    SET @@repo1_head=hashof('main');
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
    server_query repo1 0 "SELECT name,latest_commit_message FROM dolt_branches" "name,latest_commit_message\nmain,Initialize data repository\ntest_branch,second commit"

    # validate no tables on main still
    server_query repo1 0 "SHOW tables" ""

    # Squash the test_branch into main even though it is a fast-forward merge.
    multi_query repo1 0 "
    SET @@repo1_working = squash('test_branch');
    SET @@repo1_head = COMMIT('-m', 'cm1');
    UPDATE dolt_branches SET hash = @@repo1_head WHERE name= 'main';"

    # Validate tables and data on main
    server_query repo1 0 "SET @@repo1_head=hashof('main');SHOW tables" ";Table\none_pk"
    server_query repo1 0 "SET @@repo1_head=hashof('main');SELECT * FROM one_pk ORDER by pk" ";pk,c1,c2\n0,None,None\n1,1,None\n2,2,2\n3,3,3\n4,4,4\n5,5,5"

    # Validate that the squash operations resulted in one commit to main than before
    server_query repo1 0 "SET @@repo1_head=hashof('main');select COUNT(*) from dolt_log" ";COUNT(*)\n2"

    # make some changes to main and commit. Make some changes to test_branch and commit. Squash/Merge.
    multi_query repo1 0 "
    SET @@repo1_head=hashof('main');
    UPDATE one_pk SET c1=10 WHERE pk=2;
    SET @@repo1_head=commit('-m', 'Change c 1 to 10');
    UPDATE dolt_branches SET hash = @@repo1_head WHERE name= 'main';

    SET @@repo1_head=hashof('test_branch');
    INSERT INTO one_pk (pk,c1,c2) VALUES (6,6,6);
    SET @@repo1_head=commit('-m', 'add 6');
    INSERT INTO one_pk (pk,c1,c2) VALUES (7,7,7);
    SET @@repo1_head=commit('-m', 'add 7');
    INSERT INTO dolt_branches (name,hash) VALUES ('test_branch', @@repo1_head);"

    # Validate that running a squash operation without updating the working variable itself alone does not
    # change the working root value
    server_query repo1 0 "SET @@repo1_head=hashof('main');SET @junk = squash('test_branch');SELECT * FROM one_pk ORDER by pk" ";;pk,c1,c2\n0,None,None\n1,1,None\n2,10,2\n3,3,3\n4,4,4\n5,5,5"

    multi_query repo1 0 "
    SET @@repo1_head=hashof('main');
    SET @@repo1_working = squash('test_branch');
    SET @@repo1_head = COMMIT('-m', 'cm2');
    UPDATE dolt_branches SET hash = @@repo1_head WHERE name= 'main';"

    # Validate tables and data on main
    server_query repo1 0 "SET @@repo1_head=hashof('main');SHOW tables" ";Table\none_pk"
    server_query repo1 0 "SET @@repo1_head=hashof('main');SELECT * FROM one_pk ORDER by pk" ";pk,c1,c2\n0,None,None\n1,1,None\n2,10,2\n3,3,3\n4,4,4\n5,5,5\n6,6,6\n7,7,7"

    # Validate that the squash operations resulted in one commit to main than before
    server_query repo1 0 "select COUNT(*) from dolt_log" "COUNT(*)\n4"

    # Validate the a squash commit was written by making sure the hashes of the two branches don't match
    server_query repo1 0 "select COUNT(hash) from dolt_branches where hash IN (select hash from dolt_branches WHERE name = 'test_branch')" "COUNT(hash)\n1"

    # check that squash with unknown branch throws an error
    run server_query repo1 0 "SET @@repo1_working = squash('fake');" ""
    [ "$status" -eq 1 ]

    # TODO: this throws an error on COMMIT because it has conflicts on the root it's trying to commit
    multi_query repo1 0 "
    SELECT DOLT_CHECKOUT('main');
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
        REPLACE INTO dolt_branches (name,hash) VALUES ('main', @@repo1_head);"

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "working tree clean" ]] || false
    run dolt sql -q "SELECT sum(pk), sum(c0) FROM test;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "6,6" ]] || false

    multi_query repo1 1 "
        INSERT INTO test VALUES (8,8);
        SET @@repo1_head = reset('hard');
        REPLACE INTO dolt_branches (name,hash) VALUES ('main', @@repo1_head);"

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
    CREATE DATABASE test;
    USE test;
    CREATE TABLE pk(pk int primary key);
    INSERT INTO pk (pk) VALUES (0);
    "

    server_query repo1 1 "SELECT * FROM test.pk ORDER BY pk" "pk\n0"
    server_query repo1 1 "DROP DATABASE test" ""
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
     SELECT DOLT_CHECKOUT('main');
     SELECT DOLT_MERGE('feature-branch');
     "

     server_query repo1 1 "SELECT * FROM test ORDER BY pk" "pk\n1\n2\n3\n1000"

     server_query repo1 1 "SELECT COUNT(*) FROM dolt_log" "COUNT(*)\n3"
}

@test "sql-server: LOAD DATA LOCAL INFILE works" {
     skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

     cd repo1
     start_sql_server repo1

     multi_query repo1 1 "
     CREATE TABLE test(pk int primary key, c1 int, c2 int, c3 int, c4 int, c5 int);
     SET GLOBAL local_infile = 1;
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

     unselected_server_query 1 "INSERT INTO repo2.one_pk VALUES (0), (1), (2)"
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

     unselected_server_query 1 "INSERT INTO testdb.one_pk VALUES (0), (1), (2)"
     unselected_server_query 1 "SELECT * FROM testdb.one_pk" "pk\n0\n1\n2"

     unselected_update_query 1 "UPDATE testdb.one_pk SET pk=3 WHERE pk=2"
     unselected_server_query 1 "SELECT * FROM testdb.one_pk" "pk\n0\n1\n3"

     unselected_update_query 1 "DELETE FROM testdb.one_pk WHERE pk=3"
     unselected_server_query 1 "SELECT * FROM testdb.one_pk" "pk\n0\n1"

     # one last query on insert db.
     unselected_server_query 1 "INSERT INTO repo2.one_pk VALUES (4)"
     unselected_server_query 1 "SELECT * FROM repo2.one_pk" "pk\n0\n1\n4"

     # verify changes outside the session
     cd repo2
     run dolt sql -q "show tables"
     [ "$status" -eq 0 ]
     [[ "$output" =~ "one_pk" ]] || false

     run dolt sql -q "select * from one_pk"
     [ "$status" -eq 0 ]
     [[ "$output" =~ "0" ]] || false
     [[ "$output" =~ "1" ]] || false
     [[ "$output" =~ "4" ]] || false
}

@test "sql-server: create database without USE" {
     skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

     start_multi_db_server repo1

     unselected_server_query 1 "CREATE DATABASE newdb" ""
     unselected_server_query 1 "CREATE TABLE newdb.test (a int primary key)" ""

     # verify changes outside the session
     cd newdb
     run dolt sql -q "show tables"
     [ "$status" -eq 0 ]
     [[ "$output" =~ "test" ]] || false
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

    # check no tables on main
    server_query repo1 1 "SHOW Tables" ""

    # make some changes to main and commit to branch test_branch
    multi_query repo1 1 "
    SET @@repo1_head=hashof('main');
    CREATE TABLE one_pk (
        pk BIGINT NOT NULL,
        c1 BIGINT,
        c2 BIGINT,
        PRIMARY KEY (pk)
    );
    INSERT INTO one_pk (pk,c1,c2) VALUES (2,2,2),(3,3,3);
    SET @@repo1_head=commit('-m', 'test commit message', '--author', 'John Doe <john@example.com>');
    INSERT INTO dolt_branches (name,hash) VALUES ('main', @@repo1_head);"

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

    # check no tables on main
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
    dolt checkout main
    start_sql_server repo1

    server_query "repo1/feature-branch" 1 "CREATE TABLE test (
        pk int,
        c1 int,
        PRIMARY KEY (pk)
    )" ""

    server_query repo1 1 "SHOW tables" "" # no tables on main

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
    dolt checkout main
    start_sql_server repo1

    multi_query repo1 1 '
    USE `repo1/feature-branch`;
    CREATE TABLE test ( 
        pk int,
        c1 int,
        PRIMARY KEY (pk)
    )' ""

    server_query repo1 1 "SHOW tables" "" # no tables on main

    server_query "repo1/feature-branch" 1 "SHOW Tables" "Table\ntest"
}

@test "sql-server: SET GLOBAL default branch as ref" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    cd repo1
    dolt checkout -b "new"
    dolt checkout main
    start_sql_server repo1

    multi_query repo1 1 '
    select dolt_checkout("new");
    CREATE TABLE t (a int primary key, b int);
    INSERT INTO t VALUES (2,2),(3,3);' ""

    server_query repo1 1 "SHOW tables" "" # no tables on main
    server_query repo1 1 "set GLOBAL dolt_default_branch = 'refs/heads/new';" ""
    server_query repo1 1 "select @@GLOBAL.dolt_default_branch;" "@@GLOBAL.dolt_default_branch\nrefs/heads/new"
    server_query repo1 1 "select active_branch()" "active_branch()\nnew"
    server_query repo1 1 "SHOW tables" "Table\nt"
}

@test "sql-server: SET GLOBAL default branch as branch name" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    cd repo1
    dolt checkout -b "new"
    dolt checkout main
    start_sql_server repo1

    multi_query repo1 1 '
    select dolt_checkout("new");
    CREATE TABLE t (a int primary key, b int);
    INSERT INTO t VALUES (2,2),(3,3);' ""

    server_query repo1 1 "SHOW tables" "" # no tables on main
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

@test "sql-server: disable_client_multi_statements makes create trigger work" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."
    cd repo1
    dolt sql -q 'create table test (id int primary key)'
    let PORT="$$ % (65536-1024) + 1024"
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
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."
    cd repo1
    dolt sql -q 'create table test (id int primary key)'
    let PORT="$$ % (65536-1024) + 1024"
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

@test "sql-server: sql-push --set-remote within session" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    mkdir rem1
    cd repo1
    dolt remote add origin file://../rem1
    start_sql_server repo1

    dolt push origin main
    run server_query repo1 1 "select dolt_push() as p" "p\n0"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "the current branch has no upstream branch" ]] || false

    server_query repo1 1 "select dolt_push('--set-upstream', 'origin', 'main') as p" "p\n1"

    skip "In-memory branch doesn't track upstream correctly"
    server_query repo1 1 "select dolt_push() as p" "p\n1"
}

@test "sql-server: replicate to backup after sql-session commit" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    mkdir bac1
    cd repo1
    dolt remote add backup1 file://../bac1
    dolt config --local --add sqlserver.global.DOLT_REPLICATE_TO_REMOTE backup1
    start_sql_server repo1

    multi_query repo1 1 "
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
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    mkdir no_dolt && cd no_dolt
    start_sql_server

    server_query "" 1 "create database test1"
    server_query "" 1 "show databases" "Database\ninformation_schema\ntest1"
    server_query "test1" 1 "create table a(x int)"
    server_query "test1" 1 "insert into a values (1), (2)"
    # not bothering to check the results of the commit here
    run server_query "test1" 1 "select dolt_commit('-a', '-m', 'new table a')"

    server_query "" 1 "create database test2"
    server_query "test2" 1 "create table b(x int)"
    server_query "test2" 1 "insert into b values (1), (2)"
    # not bothering to check the results of the commit here
    run server_query "test2" 1 "select dolt_commit('-a', '-m', 'new table b')"

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

    server_query "" 1 "create database test3"
    server_query "test3" 1 "create table c(x int)"
    server_query "test3" 1 "insert into c values (1), (2)"
    run server_query "test3" 1 "select dolt_commit('-a', '-m', 'new table c')"

    server_query "" 1 "drop database test2"

    [ -d test3 ]
    [ ! -d test2 ]
    
    # make sure the databases exist on restart
    stop_sql_server
    start_sql_server
    server_query "" 1 "show databases" "Database\ninformation_schema\ntest1\ntest3"
}

@test "sql-server: create and drop database with --multi-db-dir" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    mkdir no_dolt && cd no_dolt
    mkdir db_dir
    start_sql_server_with_args --host 0.0.0.0 --user dolt --multi-db-dir=db_dir

    server_query "" 1 "create database test1"
    server_query "" 1 "show databases" "Database\ninformation_schema\ntest1"
    server_query "test1" 1 "create table a(x int)"
    server_query "test1" 1 "insert into a values (1), (2)"
    # not bothering to check the results of the commit here
    run server_query "test1" 1 "select dolt_commit('-a', '-m', 'new table a')"

    [ -d db_dir/test1 ]
    
    cd db_dir/test1
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "new table a" ]] || false

    cd ../..

    server_query "" 1 "create database test3"
    server_query "test3" 1 "create table c(x int)"
    server_query "test3" 1 "insert into c values (1), (2)"
    run server_query "test3" 1 "select dolt_commit('-a', '-m', 'new table c')"

    server_query "" 1 "drop database test1"

    [ -d db_dir/test3 ]
    [ ! -d db_dir/test1 ]
    
    # make sure the databases exist on restart
    stop_sql_server
    start_sql_server_with_args --host 0.0.0.0 --user dolt --multi-db-dir=db_dir
    server_query "" 1 "show databases" "Database\ninformation_schema\ntest3"
}

@test "sql-server: create database errors" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    mkdir no_dolt && cd no_dolt
    mkdir dir_exists
    touch file_exists
    start_sql_server
    
    server_query "" 1 "create database test1"

    # Error on creation, already exists
    server_query "" 1 "create database test1" "" "exists"

    # Files / dirs in the way
    server_query "" 1 "create database dir_exists" "" "exists"
    server_query "" 1 "create database file_exists" "" "exists"        
}

@test "sql-server: create database with existing repo" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    cd repo1
    start_sql_server
    
    server_query "" 1 "create database test1"
    server_query "repo1" 1 "show databases" "Database\ninformation_schema\nrepo1\ntest1"
    server_query "test1" 1 "create table a(x int)"
    server_query "test1" 1 "insert into a values (1), (2)"
    # not bothering to check the results of the commit here
    run server_query "test1" 1 "select dolt_commit('-a', '-m', 'new table a')"

    server_query "" 1 "create database test2"
    server_query "test2" 1 "create table b(x int)"
    server_query "test2" 1 "insert into b values (1), (2)"
    # not bothering to check the results of the commit here
    run server_query "test2" 1 "select dolt_commit('-a', '-m', 'new table b')"

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
    stop_sql_server
    start_sql_server
    server_query "" 1 "show databases" "Database\ninformation_schema\nrepo1\ntest1\ntest2"
}

@test "sql-server: fetch uses database tempdir from different working directory" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

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

    server_query repo1 1 "select dolt_fetch() as f" "f\n1"
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
    dolt commit -am "create two tables"

    cd ../repo2
    dolt sql -q "create table r2t_one (id1 int primary key, col1 varchar(20));"
    dolt sql -q "create table r2t_two (id2 int primary key, col2 varchar(20));"
    dolt sql -q "create table r2t_three (id3 int primary key, col3 varchar(20));"
    dolt sql -q "insert into r2t_three values (4,'dddd'), (3,'gggg'), (2,'eeee'), (1,'ffff');"
    dolt commit -am "create three tables"

    cd ..
    start_sql_server_with_args --user dolt -ltrace --no-auto-commit

    run expect $BATS_TEST_DIRNAME/sql-server-mysql.expect $PORT repo1
    [ "$status" -eq 0 ]
}
