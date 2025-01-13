#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    skiponwindows "tests are flaky on Windows"
    setup_common
}

teardown() {
    skiponwindows "tests are flaky on Windows"
    assert_feature_version
    teardown_common
    rm -rf $BATS_TMPDIR/remotes-$$
}

@test "system-tables: Show list of system tables using dolt ls --system or --all" {
    dolt sql -q "create table test (pk int, c1 int, primary key(pk))"

    run dolt ls --system
    [ $status -eq 0 ]
    [[ "$output" =~ "dolt_log" ]] || false
    [[ "$output" =~ "dolt_conflicts" ]] || false
    [[ "$output" =~ "dolt_branches" ]] || false
    [[ "$output" =~ "dolt_remote_branches" ]] || false
    [[ "$output" =~ "dolt_remotes" ]] || false
    [[ "$output" =~ "dolt_status" ]] || false
    [[ ! "$output" =~ " test" ]] || false  # spaces are impt!

    run dolt ls --all
    [ $status -eq 0 ]
    [[ "$output" =~ "dolt_log" ]] || false
    [[ "$output" =~ "dolt_commits" ]] || false
    [[ "$output" =~ "dolt_commit_ancestors" ]] || false
    [[ "$output" =~ "dolt_conflicts" ]] || false
    [[ "$output" =~ "dolt_branches" ]] || false
    [[ "$output" =~ "dolt_remote_branches" ]] || false
    [[ "$output" =~ "dolt_remotes" ]] || false
    [[ "$output" =~ "dolt_status" ]] || false
    [[ "$output" =~ "dolt_help" ]] || false
    [[ "$output" =~ "test" ]] || false

    dolt add test
    dolt commit -m "Added test table"

    run dolt ls --system
    [ $status -eq 0 ]
    [[ "$output" =~ "dolt_history_test" ]] || false
    [[ "$output" =~ "dolt_diff_test" ]] || false
    [[ "$output" =~ "dolt_commit_diff_test" ]] || false

    run dolt ls --all
    [ $status -eq 0 ]
    [[ "$output" =~ "dolt_history_test" ]] || false
    [[ "$output" =~ "dolt_diff_test" ]] || false
    [[ "$output" =~ "dolt_commit_diff_test" ]] || false
}

@test "system-tables: query dolt_log system table" {
    dolt sql -q "create table test (pk int, c1 int, primary key(pk))"
    dolt add test
    dolt commit -m "Added test table"
    run dolt sql -q "select * from dolt_log"
    [ $status -eq 0 ]
    [[ "$output" =~ "Added test table" ]] || false
    run dolt sql -q "select * from dolt_log where message !='Added test table'"
    [ $status -eq 0 ]
    [[ ! "$output" =~ "Added test table" ]] || false
}

@test "system-tables: query dolt_branches system table" {
    dolt checkout -b create-table-branch
    dolt sql -q "create table test (pk int, c1 int, primary key(pk))"
    dolt add test
    dolt commit -m "Added test table"
    run dolt sql -q "select * from dolt_branches"
    [ $status -eq 0 ]
    [[ "$output" =~ main.*Initialize\ data\ repository ]] || false
    [[ "$output" =~ create-table-branch.*Added\ test\ table ]] || false
    run dolt sql -q "select * from dolt_branches where latest_commit_message ='Initialize data repository'"
    [ $status -eq 0 ]
    [[ "$output" =~ "main" ]] || false
    [[ ! "$output" =~ "create-table-branch" ]] || false
    run dolt sql -q "select * from dolt_branches where latest_commit_message ='Added test table'"
    [ $status -eq 0 ]
    [[ ! "$output" =~ "main" ]] || false
    [[ "$output" =~ "create-table-branch" ]] || false
}

@test "system-tables: query dolt_remote_branches system table" {
    dolt checkout -b create-table-branch
    dolt sql -q "create table test (pk int, c1 int, primary key(pk))"
    dolt add test
    dolt commit -m "Added test table"
    dolt branch "b1"
    mkdir ./remote1
    dolt remote add rem1 file://./remote1
    dolt push rem1 b1
    dolt branch -d b1

    run dolt sql -q "select name, latest_commit_message from dolt_branches"
    [ $status -eq 0 ]
    [[ "$output" =~ main.*Initialize\ data\ repository ]] || false
    [[ "$output" =~ create-table-branch.*Added\ test\ table ]] || false
    [[ ! "$output" =~ b1 ]] || false

    run dolt sql -q "select name, latest_commit_message from dolt_remote_branches"
    [ $status -eq 0 ]
    [[ ! "$output" =~ main.*Initialize\ data\ repository ]] || false
    [[ ! "$output" =~ create-table-branch.*Added\ test\ table ]] || false
    [[ "$output" =~ "remotes/rem1/b1" ]] || false

    run dolt sql -q "select name from dolt_remote_branches where latest_commit_message ='Initialize data repository'"
    [ $status -eq 0 ]
    [[ ! "$output" =~ "main" ]] || false
    [[ ! "$output" =~ "create-table-branch" ]] || false
    [[ ! "$output" =~ "remotes/rem1/b1" ]] || false

    run dolt sql -q "select name from dolt_remote_branches where latest_commit_message ='Added test table'"
    [ $status -eq 0 ]
    [[ ! "$output" =~ "main" ]] || false
    [[ ! "$output" =~ "create-table-branch" ]] || false
    [[ "$output" =~ "remotes/rem1/b1" ]] || false

    run dolt sql -q "select name from dolt_branches union select name from dolt_remote_branches"
    [[ "$output" =~ "main" ]] || false
    [[ "$output" =~ "create-table-branch" ]] || false
    [[ "$output" =~ "remotes/rem1/b1" ]] || false

    # make sure table works with no remote branches
    mkdir noremotes && cd noremotes
    dolt init
    dolt sql <<SQL
create table t1(a int primary key);
SQL
    dolt commit -Am 'new table';
    dolt branch b1

    run dolt sql -q "select * from dolt_remote_branches" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]
}

@test "system-tables: query dolt_remotes system table" {
    run dolt sql -q "select count(*) from dolt_remotes" -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ 0 ]] || false

    mkdir remote
    dolt remote add origin file://remote/

    run dolt sql -q "select count(*) from dolt_remotes"
    [ $status -eq 0 ]
    [[ "$output" =~ 1 ]] || false

    regex='file://.*/remote'
    run dolt sql -q "select name, fetch_specs, params from dolt_remotes" -r csv
    [ $status -eq 0 ]
    [[ "${lines[0]}" = name,fetch_specs,params ]] || false
    [[ "${lines[1]}" =~ "origin,\"[\"\"refs/heads/*:refs/remotes/origin/*\"\"]\",{}" ]] || false
    run dolt sql -q "select url from dolt_remotes" -r csv
    [ $status -eq 0 ]
    [[ "${lines[0]}" = url ]] || false
    [[ "${lines[1]}" =~ $regex ]] || false
}

@test "system-tables: check unsupported dolt_remote behavior" {
    run dolt sql -q "insert into dolt_remotes (name, url) values ('origin', 'file://remote')"
    [ $status -ne 0 ]
    [[ ! "$output" =~ panic ]] || false
    [[ "$output" =~ "the dolt_remotes table is read-only; use the dolt_remote stored procedure to edit remotes" ]] || false

    mkdir remote
    dolt remote add origin file://remote/
    run dolt sql -q "delete from dolt_remotes where name = 'origin'"
    [ $status -ne 0 ]
    [[ "$output" =~ "the dolt_remotes table is read-only; use the dolt_remote stored procedure to edit remotes" ]] || false
}

@test "system-tables: insert into dolt_remotes system table using dolt_remote procedure" {
    mkdir remote
    dolt sql -q "CALL DOLT_REMOTE('add', 'origin1', 'file://remote')"
    dolt sql -q "CALL DOLT_REMOTE('add', 'origin2', 'aws://[dynamo_db_table:s3_bucket]/repo_name')"

    run dolt sql -q "select count(*) from dolt_remotes" -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ 2 ]] || false

    run dolt sql -q "select name, fetch_specs, params from dolt_remotes" -r csv
    [ $status -eq 0 ]
    [[ "${lines[0]}" = name,fetch_specs,params ]] || false
    [[ "$output" =~ "origin1,\"[\"\"refs/heads/*:refs/remotes/origin1/*\"\"]\",{}" ]] || false
    [[ "$output" =~ "origin2,\"[\"\"refs/heads/*:refs/remotes/origin2/*\"\"]\",{}" ]] || false

    file_regex='file://.*/remote'
    aws_regex='aws://.*/repo_name'
    run dolt sql -q "select url from dolt_remotes" -r csv
    [ $status -eq 0 ]
    [[ "${lines[0]}" = url ]] || false
    [[ "$output" =~ $file_regex ]] || false
    [[ "$output" =~ $aws_regex ]] || false
}

@test "system-tables: delete from dolt_remotes system table using dolt_remote procedure" {
    mkdir remote
    dolt remote add origin file://remote/

    run dolt sql -q "select count(*) from dolt_remotes"
    [ $status -eq 0 ]
    [[ "$output" =~ 1 ]] || false

    run dolt sql -q "select name, fetch_specs, params from dolt_remotes" -r csv
    [ $status -eq 0 ]
    [[ "${lines[0]}" =~ "name,fetch_specs,params" ]] || false
    [[ "${lines[1]}" =~ "origin,\"[\"\"refs/heads/*:refs/remotes/origin/*\"\"]\",{}" ]] || false

    regex='file://.*/remote'
    run dolt sql -q "select url from dolt_remotes" -r csv
    [ $status -eq 0 ]
    [[ "${lines[0]}" = url ]] || false
    [[ "$output" =~ $regex ]] || false

    run dolt sql -q "CALL DOLT_REMOTE('remove', 'origin1')"
    [ $status -eq 1 ]
    [[ "$output" =~ "error: unknown remote: 'origin1'" ]] || false

    run dolt sql -q "select count(*) from dolt_remotes"
    [ $status -eq 0 ]
    [[ "$output" =~ 1 ]] || false

    dolt sql -q "CALL DOLT_REMOTE('remove', 'origin')"
    run dolt sql -q "select count(*) from dolt_remotes" -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ 0 ]] || false
}

@test "system-tables: revision databases can query dolt_remotes table" {
    mkdir remote
    dolt remote add origin file://remote/
    dolt branch b1

    run dolt sql <<SQL
SELECT name FROM dolt_remotes;
SQL
    [ $status -eq 0 ]
    [[ "$output" =~ "origin" ]] || false

    DATABASE=$(echo $(basename $(pwd)))
    run dolt sql <<SQL
USE \`$DATABASE/b1\`;
SELECT name FROM dolt_remotes;
SQL
    [ $status -eq 0 ]
    [[ "$output" =~ "origin" ]] || false
}

@test "system-tables: query dolt_diff system table" {
    dolt sql -q "CREATE TABLE testStaged (pk INT, c1 INT, PRIMARY KEY(pk))"
    dolt add testStaged
    dolt sql -q "CREATE TABLE testWorking (pk INT, c1 INT, PRIMARY KEY(pk))"

    run dolt sql -r csv -q 'select * from dolt_diff'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "STAGED,testStaged,,,,,false,true" ]] || false
    [[ "$output" =~ "WORKING,testWorking,,,,,false,true" ]] || false
}

@test "system-tables: query dolt_column_diff system table" {
    dolt sql -q "CREATE TABLE testStaged (pk INT, c1 INT, PRIMARY KEY(pk))"
    dolt add testStaged
    dolt sql -q "CREATE TABLE testWorking (pk INT, c1 INT, PRIMARY KEY(pk))"

    run dolt sql -r csv -q 'select * from dolt_column_diff'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "STAGED,testStaged,pk,,,,,added" ]] || false
    [[ "$output" =~ "STAGED,testStaged,c1,,,,,added" ]] || false
    [[ "$output" =~ "WORKING,testWorking,pk,,,,,added" ]] || false
    [[ "$output" =~ "WORKING,testWorking,c1,,,,,added" ]] || false
}

@test "system-tables: query dolt_diff_ system table" {
    dolt sql -q "CREATE TABLE test (pk INT, c1 INT, PRIMARY KEY(pk))"
    dolt add test
    dolt commit -m "Added test table"
    dolt branch create_checkpoint main
    dolt sql -q "INSERT INTO test VALUES (0,0),(1,1),(2,2)"
    dolt add test
    dolt commit -m "Added rows"
    dolt branch inserted_rows main
    dolt sql -q "INSERT INTO test VALUES (3,3)"
    dolt sql -q "UPDATE test SET c1=5 WHERE pk=1"
    dolt sql -q "DELETE FROM test WHERE pk=2"

    EXPECTED=$(echo -e "to_pk,to_c1,from_pk,from_c1,diff_type\n0,0,,,added\n1,1,,,added\n2,2,,,added\n1,5,1,1,modified\n,,2,2,removed\n3,3,,,added")
    run dolt sql -r csv -q 'SELECT to_pk, to_c1, from_pk, from_c1, diff_type FROM dolt_diff_test ORDER BY from_commit_date'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "$EXPECTED" ]] || false

    EXPECTED=$(echo -e "to_pk,to_c1,from_pk,from_c1,diff_type\n1,5,1,1,modified\n,,2,2,removed\n3,3,,,added")
    run dolt sql -r csv -q 'SELECT to_pk, to_c1, from_pk, from_c1, diff_type FROM dolt_diff_test WHERE to_commit = "WORKING" ORDER BY from_commit_date'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "$EXPECTED" ]] || false

    EXPECTED=$(echo -e "to_pk,to_c1,from_pk,from_c1,diff_type\n0,0,,,added\n1,1,,,added\n2,2,,,added")
    run dolt sql -r csv -q 'SELECT to_pk, to_c1, from_pk, from_c1, diff_type FROM dolt_diff_test WHERE from_commit = HASHOF("create_checkpoint") AND to_commit = HASHOF("inserted_rows") ORDER BY from_commit_date'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "$EXPECTED" ]] || false
}

@test "system-tables: query dolt_diff_ and dolt_commit_diff_ highlighting differences" {
    dolt branch before_creation main
    dolt sql -q "CREATE TABLE test (pk INT, c1 INT, PRIMARY KEY(pk))"
    dolt add test
    dolt commit -m "Added test table"
    dolt branch start main
    dolt sql -q "INSERT INTO test (pk, c1) VALUES (1,1),(2,2),(3,3)"
    dolt add test
    dolt commit -m "add rows 1-3"
    dolt sql -q "UPDATE  test SET c1=4 WHERE pk=2"
    dolt sql -q "UPDATE test SET c1=5 WHERE pk=3"
    dolt add test
    dolt commit -m "modified 2 & 3"
    # revert row 2 on working
    dolt sql -q "UPDATE test SET c1=2 WHERE pk=2"

    #expect no rows on the dolt_diff_test table as there are commits between hashof("start") and hashof("main") so no rows meet those two criteria
    EXPECTED=$(echo -e "to_pk,to_c1,from_pk,from_c1,diff_type")
    run dolt sql -r csv -q 'SELECT to_pk, to_c1, from_pk, from_c1, diff_type FROM dolt_diff_test WHERE to_commit=hashof("main") and from_commit=hashof("start") ORDER BY to_pk'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "$EXPECTED" ]] || false

    EXPECTED=$(echo -e "to_pk,to_c1,from_pk,from_c1,diff_type\n1,1,,,added\n2,4,,,added\n3,5,,,added")
    run dolt sql -r csv -q 'SELECT to_pk, to_c1, from_pk, from_c1, diff_type FROM dolt_commit_diff_test WHERE to_commit=hashof("main") and from_commit=hashof("start") ORDER BY to_pk'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "$EXPECTED" ]] || false

    EXPECTED=$(echo -e "to_pk,to_c1,from_pk,from_c1,diff_type\n1,1,,,added\n2,2,,,added\n3,5,,,added")
    run dolt sql -r csv -q 'SELECT to_pk, to_c1, from_pk, from_c1, diff_type FROM dolt_commit_diff_test WHERE to_commit="WORKING" and from_commit=hashof("start") ORDER BY to_pk'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "$EXPECTED" ]] || false

    EXPECTED=$(echo -e "to_pk,to_c1,from_pk,from_c1,diff_type\n1,1,,,added\n2,2,,,added\n3,5,,,added")
    run dolt sql -r csv -q 'SELECT to_pk, to_c1, from_pk, from_c1, diff_type FROM dolt_commit_diff_test WHERE to_commit="WORKING" and from_commit=hashof("before_creation") ORDER BY to_pk'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "$EXPECTED" ]] || false

    EXPECTED=$(echo -e "to_pk,to_c1,from_pk,from_c1,diff_type\n2,2,,,added")
    run dolt sql -r csv -q 'SELECT to_pk, to_c1, from_pk, from_c1, diff_type FROM dolt_commit_diff_test WHERE to_commit="WORKING" and from_commit=hashof("before_creation") and to_pk = 2 ORDER BY to_pk'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "$EXPECTED" ]] || false

    #diff table and commit diff look the same when looking at the difference between a single commit
    EXPECTED=$(echo -e "to_pk,to_c1,from_pk,from_c1,diff_type\n2,2,2,4,modified")
    run dolt sql -r csv -q 'SELECT to_pk, to_c1, from_pk, from_c1, diff_type FROM dolt_diff_test WHERE to_commit="WORKING" and from_commit=hashof("main") ORDER BY to_pk'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "$EXPECTED" ]] || false

    EXPECTED=$(echo -e "to_pk,to_c1,from_pk,from_c1,diff_type\n2,2,2,4,modified")
    run dolt sql -r csv -q 'SELECT to_pk, to_c1, from_pk, from_c1, diff_type FROM dolt_commit_diff_test WHERE to_commit="WORKING" and from_commit=hashof("main") ORDER BY to_pk'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "$EXPECTED" ]] || false

    #expect no rows on the dolt_diff_test table as there are commits between hashof("start") and hashof("main") so no rows meet those two criteria
    EXPECTED=$(echo -e "to_pk,to_c1,from_pk,from_c1,diff_type\n1,1,,,added\n2,2,,,added\n3,3,,,added\n2,4,2,2,modified\n3,5,3,3,modified")
    run dolt sql -r csv -q 'SELECT to_pk, to_c1, from_pk, from_c1, diff_type FROM dolt_diff_test WHERE to_commit=hashof("main") or from_commit=hashof("start") ORDER BY to_commit_date, to_pk'
    [ "$status" -eq 0 ]
    echo $output
    [[ "$output" =~ "$EXPECTED" ]] || false

    run dolt sql -r csv -q 'SELECT to_pk, to_c1, from_pk, from_c1, diff_type FROM dolt_commit_diff_test WHERE to_commit=hashof("main") or from_commit=hashof("start") ORDER BY to_pk'
    [ ! "$status" -eq 0 ]
}

@test "system-tables: query dolt_diff_ system table without committing table" {
    dolt sql -q "create table test (pk int not null primary key);"
    dolt sql -q "insert into test values (0), (1);"

    EXPECTED=$(echo -e "to_pk,to_commit,from_pk,diff_type\n0,WORKING,,added\n1,WORKING,,added")
    run dolt sql -r csv -q 'select to_pk, to_commit, from_pk, diff_type from dolt_diff_test;'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "$EXPECTED" ]] || false
}

@test "system-tables: query dolt_diff_ system table when column types have been narrowed" {
    dolt sql -q "create table t (pk int primary key, col1 varchar(20), col2 int);"
    dolt commit -Am "new table t"
    dolt sql -q "insert into t values (1, '123456789012345', 420);"
    dolt commit -am "inserting a row"
    dolt sql -q "update t set col1='1234567890', col2=13;"
    dolt sql -q "alter table t modify column col1 varchar(10);"
    dolt sql -q "alter table t modify column col2 tinyint;"
    dolt commit -am "narrowing types"

    run dolt sql -r csv -q "select to_pk, to_col1, to_col2, from_pk, from_col1, from_col2, diff_type from dolt_diff_t order by from_commit_date ASC;"
    [ $status -eq 0 ]
    [[ $output =~ "1,,,,,,added" ]] || false
    [[ $output =~ "1,1234567890,13,1,,,modified" ]] || false
}

@test "system-tables: query dolt_history_ system table" {
    dolt sql -q "create table test (pk int, c1 int, primary key(pk))"
    dolt add test
    dolt commit -m "Added test table"
    dolt sql -q "insert into test values (0,0)"
    dolt add test
    dolt commit -m "Added (0,0) row"
    dolt sql -q "insert into test values (1,1)"
    dolt add test
    dolt commit -m "Added (1,1) row"
    run dolt sql -q "select * from dolt_history_test"
    echo $output
    echo ${#lines[@]}
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 7 ]
    run dolt sql -q "select * from dolt_history_test where pk=1"
    echo $output
    echo ${#lines[@]}
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    run dolt sql -q "select * from dolt_history_test where pk=0"
    echo $output
    echo ${#lines[@]}
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]

    # Test DOLT_HISTORY_ table works for tables with no primary keys
    dolt sql -q "create table nopks (a int, b text);"
    dolt sql -q "insert into nopks values (123, 'onetwothree'), (234, 'twothreefour');"
    dolt add nopks
    dolt commit -m "Adding table nopks"
    run dolt sql -q "select * from dolt_history_nopks;"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]
    run dolt sql -q "select * from dolt_history_nopks where a=123;"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "$output" =~ "onetwothree" ]] || false
}

@test "system-tables: query dolt_commits" {
    run dolt sql -q "SELECT count(*) FROM dolt_commits;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1" ]] || false

    dolt sql -q "CREATE TABLE test (pk int PRIMARY KEY);"
    dolt add -A && dolt commit -m "added table test"

    dolt branch other

    dolt sql -q "INSERT INTO test VALUES (0);"
    dolt add -A && dolt commit -m "added values to test on main"

    dolt checkout other
    dolt sql -q "INSERT INTO test VALUES (1);"
    dolt add -A && dolt commit -m "added values to test on other"

    run dolt sql -q "SELECT count(*) FROM dolt_commits;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "4" ]] || false
}

@test "system-tables: query dolt_ancestor_commits" {
    run dolt sql -q "SELECT count(*) FROM dolt_commit_ancestors;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1" ]] || false

    run dolt sql -q "SELECT count(*) FROM dolt_commit_ancestors;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1" ]] || false
    run dolt sql -q "SELECT parent_hash FROM dolt_commit_ancestors;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "NULL" ]] || false

    dolt sql -q "CREATE TABLE test (pk int PRIMARY KEY);"
    dolt add -A && dolt commit -m "commit A"

    dolt branch other

    dolt sql -q "INSERT INTO test VALUES (0);"
    dolt add -A && dolt commit -m "commit B"

    dolt checkout other
    dolt sql -q "INSERT INTO test VALUES (1);"
    dolt add -A && dolt commit -m "commit C"

    dolt checkout main
    dolt merge other -m "merge other (commit M)"

    #         C--M
    #        /  /
    #  --*--A--B

    run dolt sql -q "SELECT count(*) FROM dolt_commit_ancestors;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "6" ]] || false
}

@test "system-tables: join dolt_commits and dolt_commit_ancestors" {
    dolt sql -q "CREATE TABLE test (pk int PRIMARY KEY);"
    dolt add -A && dolt commit -m "commit A"

    dolt branch other

    dolt sql -q "INSERT INTO test VALUES (0);"
    dolt add -A && dolt commit -m "commit B"

    dolt checkout other
    dolt sql -q "INSERT INTO test VALUES (1);"
    dolt add -A && dolt commit -m "commit C"

    dolt checkout main
    dolt merge other -m "merge other (commit M)"

    run dolt sql -q "
        SELECT an.parent_index,cm.message
        FROM dolt_commits as cm
        JOIN dolt_commit_ancestors as an
        ON cm.commit_hash = an.parent_hash
        ORDER BY cm.date;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "parent_index,message" ]] || false
    [[ "$output" =~ "0,Initialize data repository" ]] || false
    [[ "$output" =~ "0,commit A" ]] || false
    [[ "$output" =~ "0,commit A" ]] || false
    [[ "$output" =~ "0,commit B" ]] || false
    [[ "$output" =~ "1,commit C" ]] || false
}

@test "system-tables: dolt_branches is read-only" {
    run dolt sql -q "DELETE FROM dolt_branches"
    [ "$status" -ne 0 ]
    [[ "$output" =~ "read-only" ]] || false

    run dolt sql -q "INSERT INTO dolt_branches (name,hash) VALUES ('branch1', 'main');"
    [ "$status" -ne 0 ]
    [[ "$output" =~ "read-only" ]] || false

    run dolt sql -q "UPDATE dolt_branches SET name = 'branch1' WHERE name = 'main'"
    [ "$status" -ne 0 ]
    [[ "$output" =~ "read-only" ]] || false
}

@test "system-tables: dolt diff includes changes from initial commit" {
    dolt sql -q "CREATE TABLE test(pk int primary key, val int)"
    dolt sql -q "INSERT INTO test VALUES (1,1)"
    dolt add .
    dolt commit -am "cm1"

    dolt sql -q "INSERT INTO test VALUES (2,2)"
    dolt commit -am "cm2"

    run dolt sql -q "SELECT to_val,to_pk FROM dolt_diff_test" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
}

@test "system-tables: query dolt_tags" {
    dolt sql -q "CREATE TABLE test(pk int primary key, val int)"
    dolt add .
    dolt sql -q "INSERT INTO test VALUES (1,1)"
    dolt commit -am "cm1"
    dolt tag v1 head -m "tag v1 from main"

    dolt checkout -b branch1
    dolt sql -q "INSERT INTO test VALUES (2,2)"
    dolt commit -am "cm2"
    dolt tag v2 branch1~ -m "tag v2 from branch1"
    dolt tag v3 branch1 -m "tag v3 from branch1"

    run dolt sql -q "SELECT * FROM dolt_tags" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "tag v1 from main" ]] || false
    [[ "$output" =~ "tag v2 from branch1" ]] || false
    [[ "$output" =~ "tag v3 from branch1" ]] || false
}

@test "system-tables: query dolt_schema_diff" {
			dolt sql <<SQL
call dolt_checkout('-b', 'branch1');
create table test (pk int primary key, c1 int, c2 int);
call dolt_add('.');
call dolt_commit('-am', 'commit 1');
call dolt_tag('tag1');
call dolt_checkout('-b', 'branch2');
alter table test drop column c2, add column c3 varchar(10);
call dolt_add('.');
call dolt_commit('-m', 'commit 2');
call dolt_tag('tag2');
call dolt_checkout('-b', 'branch3');
insert into test values (1, 2, 3);
call dolt_add('.');
call dolt_commit('-m', 'commit 3');
call dolt_tag('tag3');
SQL

      run dolt sql -q "select * from dolt_schema_diff('branch1', 'branch2');"
      [ "$status" -eq 0 ]
      [[ "$output" =~ "| test            | test          | CREATE TABLE \`test\` (                                             | CREATE TABLE \`test\` (                                             |" ]] || false
      [[ "$output" =~ "|                 |               |   \`c2\` int,                                                       |   \`c3\` varchar(10),                                               |" ]] || false

      run dolt sql -q "select * from dolt_schema_diff('branch2', 'branch1');"
      [ "$status" -eq 0 ]
      [[ "$output" =~ "| test            | test          | CREATE TABLE \`test\` (                                             | CREATE TABLE \`test\` (                                             |" ]] || false
      [[ "$output" =~ "|                 |               |   \`c3\` varchar(10),                                               |   \`c2\` int,                                                       |" ]] || false

      run dolt sql -q "select * from dolt_schema_diff('tag1', 'tag2');"
      [ "$status" -eq 0 ]
      [[ "$output" =~ "| test            | test          | CREATE TABLE \`test\` (                                             | CREATE TABLE \`test\` (                                             |" ]] || false
      [[ "$output" =~ "|                 |               |   \`c2\` int,                                                       |   \`c3\` varchar(10),                                               |" ]] || false

      run dolt sql -q "select * from dolt_schema_diff('tag2', 'tag1');"
      [ "$status" -eq 0 ]
      [[ "$output" =~ "| test            | test          | CREATE TABLE \`test\` (                                             | CREATE TABLE \`test\` (                                             |" ]] || false
      [[ "$output" =~ "|                 |               |   \`c3\` varchar(10),                                               |   \`c2\` int,                                                       |" ]] || false

      run dolt sql -q "select * from dolt_schema_diff('branch1', 'branch1');"
      [ "$status" -eq 0 ]
      [ "$output" = "" ]

      run dolt sql -q "select * from dolt_schema_diff('tag2', 'tag2');"
      [ "$status" -eq 0 ]
      [ "$output" = "" ]
}

@test "system-tables: query dolt_help system table" {
    run dolt sql -q "select type from dolt_help where target='dolt_rebase'"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "procedure" ]] || false

    run dolt sql -q "select short_description from dolt_help where target='dolt_commit'"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Record changes to the database" ]] || false

    run dolt sql -q "select long_description from dolt_help where target='dolt_add'"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "This command updates the list of tables using the current content found in the working root" ]] || false
    [[ "$output" =~ "This command can be performed multiple times before a commit." ]] || false

    run dolt sql -q "select arguments from dolt_help where target='dolt_pull'"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "remote".*"The name of the remote to pull from." ]] || false
    [[ "$output" =~ "remoteBranch".*"The name of a branch on the specified remote to be merged into the current working set." ]] || false
}
