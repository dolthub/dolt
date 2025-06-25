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

@test "system-tables: dolt_log system table includes commit_order column" {
    dolt sql -q "create table test (pk int, c1 int, primary key(pk))"
    dolt add test
    dolt commit -m "Added test table"
    dolt commit --allow-empty -m "Empty commit"
    
    # Test that dolt_log system table has commit_order column
    run dolt sql -q "describe dolt_log"
    [ $status -eq 0 ]
    [[ "$output" =~ "commit_order" ]] || false
    
    # Test that commit_order values are present and numeric
    run dolt sql -q "select commit_order from dolt_log where message = 'Added test table'"
    [ $status -eq 0 ]
    [[ "$output" =~ [0-9]+ ]] || false
    
    # Test that we have 3 distinct commit orders (init commit + 2 new commits)
    run dolt sql -q "select count(distinct commit_order) from dolt_log"
    [ $status -eq 0 ]
    [[ "$output" =~ "3" ]] || false
}

@test "system-tables: dolt_log() table function includes commit_order column" {
    dolt sql -q "create table test2 (pk int, c1 int, primary key(pk))"
    dolt add test2
    dolt commit -m "Added test2 table"
    
    # Test that dolt_log() function has commit_order column
    run dolt sql -q "select commit_order from dolt_log() where message = 'Added test2 table'"
    [ $status -eq 0 ]
    [[ "$output" =~ [0-9]+ ]] || false
    
    # Test that the function and system table return the same commit_order for the same commit
    run dolt sql -q "select (select commit_order from dolt_log where message = 'Added test2 table') = (select commit_order from dolt_log() where message = 'Added test2 table') as orders_match"
    [ $status -eq 0 ]
    [[ "$output" =~ "true" ]] || false
}

@test "system-tables: commit_order reflects topological order for branches" {
    if [ "$SQL_ENGINE" = "remote-engine" ]; then
      skip "needs checkout which is unsupported for remote-engine"
    fi
    
    # Create initial commit
    dolt sql -q "create table test (pk int, c1 int, primary key(pk))"
    dolt add test
    dolt commit -m "initial commit"
    
    # Create a branch
    dolt checkout -b feature
    dolt commit --allow-empty -m "feature commit"
    
    # Go back to main and make another commit
    dolt checkout main
    dolt commit --allow-empty -m "main commit"
    
    # Both feature and main commits should have the same commit_order (height)
    # since they branched from the same parent
    # Get the commit hashes and compare their heights using dolt_log() function
    run dolt sql -q "select commit_hash from dolt_commits where message = 'feature commit'"
    [ $status -eq 0 ]
    feature_hash=$(echo "$output" | tail -n 1 | tr -d ' |')
    
    run dolt sql -q "select commit_hash from dolt_commits where message = 'main commit'"
    [ $status -eq 0 ]
    main_hash=$(echo "$output" | tail -n 1 | tr -d ' |')
    
    run dolt sql -q "select (select commit_order from dolt_log('$feature_hash') limit 1) = (select commit_order from dolt_log('$main_hash') limit 1) as same_height"
    [ $status -eq 0 ]
    [[ "$output" =~ "true" ]] || false
    
    # Merge feature into main
    dolt merge feature -m "merge feature"
    
    # The merge commit should have a higher commit_order than both branch commits
    run dolt sql -q "select (select commit_order from dolt_log where message = 'merge feature') > (select commit_order from dolt_log where message = 'main commit') as merge_higher"
    [ $status -eq 0 ]
    [[ "$output" =~ "true" ]] || false
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

@test "system-tables: query dolt_history_dolt_schemas system table" {
    # Set up test data with views, triggers, and events across multiple commits
    dolt sql -q "CREATE VIEW test_view AS SELECT 1 as col1"
    dolt add .
    dolt commit -m "add test view"
    
    dolt sql -q "CREATE TABLE test_table (id INT PRIMARY KEY, name VARCHAR(50))"
    dolt sql -q "CREATE TRIGGER test_trigger BEFORE INSERT ON test_table FOR EACH ROW SET NEW.name = UPPER(NEW.name)"
    dolt add .
    dolt commit -m "add table and trigger"
    
    dolt sql -q "DROP VIEW test_view"
    dolt sql -q "CREATE VIEW test_view AS SELECT 1 as col1, 2 as col2"
    dolt add .
    dolt commit -m "modify test view"
    
    dolt sql -q "CREATE EVENT test_event ON SCHEDULE EVERY 1 DAY DO SELECT 1"
    dolt add .
    dolt commit -m "add event"
    
    # Test that the table exists and has correct schema
    run dolt sql -r csv -q 'DESCRIBE dolt_history_dolt_schemas'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "type,varchar(64)" ]] || false
    [[ "$output" =~ "name,varchar(64)" ]] || false
    [[ "$output" =~ "fragment,longtext" ]] || false
    [[ "$output" =~ "extra,json" ]] || false
    [[ "$output" =~ "sql_mode,varchar(256)" ]] || false
    [[ "$output" =~ "commit_hash,char(32)" ]] || false
    [[ "$output" =~ "committer,varchar(1024)" ]] || false
    [[ "$output" =~ "commit_date,datetime" ]] || false
    
    # Test that we have schema objects in history (view appears in all 4 commits, trigger in 3, event in 1)
    run dolt sql -q 'SELECT COUNT(*) FROM dolt_history_dolt_schemas WHERE type = "view"'
    [ "$status" -eq 0 ]
    # Should have 4 view entries (view exists in all 4 commits)
    [[ "$output" =~ "4" ]] || false
    
    # Test that we have trigger history (trigger appears in 3 commits after creation)
    run dolt sql -q 'SELECT COUNT(*) FROM dolt_history_dolt_schemas WHERE type = "trigger"'
    [ "$status" -eq 0 ]
    # Should have 3 trigger entries (trigger exists in last 3 commits)
    [[ "$output" =~ "3" ]] || false
    
    # Test that we have event history (event appears in 1 commit)
    run dolt sql -q 'SELECT COUNT(*) FROM dolt_history_dolt_schemas WHERE type = "event"'
    [ "$status" -eq 0 ]
    # Should have 1 event entry (event only in last commit)
    [[ "$output" =~ "1" ]] || false
    
    # Test filtering by schema object type works
    run dolt sql -q 'SELECT DISTINCT type FROM dolt_history_dolt_schemas ORDER BY type'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "event" ]] || false
    [[ "$output" =~ "trigger" ]] || false
    [[ "$output" =~ "view" ]] || false
    
    # Test commit metadata is present for all entries
    run dolt sql -q 'SELECT COUNT(*) FROM dolt_history_dolt_schemas WHERE commit_hash IS NOT NULL AND committer IS NOT NULL'
    [ "$status" -eq 0 ]
    # Should have 8 total entries (4 view + 3 trigger + 1 event)
    [[ "$output" =~ "8" ]] || false
}

@test "system-tables: query dolt_diff_dolt_schemas system table" {
    # dolt_diff_dolt_schemas starts empty
    run dolt sql -q 'SELECT COUNT(*) FROM dolt_diff_dolt_schemas'
    [ "$status" -eq 0 ]
    [[ "$output" =~ " 0 " ]] || false

    # Set up test data for diff scenarios
    dolt sql -q "CREATE VIEW original_view AS SELECT 1 as id"
    dolt sql -q "CREATE TABLE diff_table (id INT PRIMARY KEY)"
    dolt sql -q "CREATE TRIGGER original_trigger BEFORE INSERT ON diff_table FOR EACH ROW SET NEW.id = NEW.id + 1"

    # Before we commit our schema changes we should see two new rows in the
    # diff table where to_commit='WORKING'
    run dolt sql -q "SELECT COUNT(*) FROM dolt_diff_dolt_schemas where to_commit='WORKING'"
    [ "$status" -eq 0 ]                                                        
    [[ "$output" =~ " 2 " ]] || false 
    
    dolt add .
    dolt commit -m "base commit with original schemas"

    # After commit, this should still contain two changes, just now the from  comit and two commit should be populated with commits not working
    run dolt sql -q 'SELECT COUNT(*) FROM dolt_diff_dolt_schemas'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "2" ]] || false
    
    # Make changes for diff (working directory changes)
    dolt sql -q "DROP VIEW original_view"
    dolt sql -q "CREATE VIEW original_view AS SELECT 1 as id, 'modified' as status"  # modified
    dolt sql -q "CREATE VIEW new_view AS SELECT 'added' as status"  # added
    dolt sql -q "DROP TRIGGER original_trigger"  # removed
    dolt sql -q "CREATE EVENT new_event ON SCHEDULE EVERY 1 HOUR DO SELECT 1"  # added
    
    # Test that the table exists and has correct schema
    run dolt sql -r csv -q 'DESCRIBE dolt_diff_dolt_schemas'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "to_type,varchar(64)" ]] || false
    [[ "$output" =~ "to_name,varchar(64)" ]] || false
    [[ "$output" =~ "to_fragment,longtext" ]] || false
    [[ "$output" =~ "to_extra,json" ]] || false
    [[ "$output" =~ "to_sql_mode,varchar(256)" ]] || false
    [[ "$output" =~ "to_commit,varchar(1023)" ]] || false
    [[ "$output" =~ "to_commit_date,datetime(6)" ]] || false
    [[ "$output" =~ "from_type,varchar(64)" ]] || false
    [[ "$output" =~ "from_name,varchar(64)" ]] || false
    [[ "$output" =~ "from_fragment,longtext" ]] || false
    [[ "$output" =~ "from_extra,json" ]] || false
    [[ "$output" =~ "from_sql_mode,varchar(256)" ]] || false
    [[ "$output" =~ "from_commit,varchar(1023)" ]] || false
    [[ "$output" =~ "from_commit_date,datetime(6)" ]] || false
    [[ "$output" =~ "diff_type,varchar(1023)" ]] || false
    
    # Test actual diff functionality - should show complete history plus working changes
    # Initial commit: 2 added (original_view, original_trigger)
    # Working changes: 4 changes (original_view modified, new_view added, new_event added, original_trigger removed)
    # Total: 6 changes
    run dolt sql -q 'SELECT COUNT(*) FROM dolt_diff_dolt_schemas'
    [ "$status" -eq 0 ]
    [[ "$output" =~ " 6 " ]] || false
    
    # Test that we have changes of different types
    run dolt sql -q 'SELECT COUNT(*) FROM dolt_diff_dolt_schemas WHERE diff_type = "added"'
    [ "$status" -eq 0 ]
    [[ "$output" =~ " 4 " ]] || false  # initial: original_view, original_trigger + working: new_view, new_event
    
    run dolt sql -q 'SELECT COUNT(*) FROM dolt_diff_dolt_schemas WHERE diff_type = "modified"'
    [ "$status" -eq 0 ]
    [[ "$output" =~ " 1 " ]] || false  # original_view
    
    run dolt sql -q 'SELECT COUNT(*) FROM dolt_diff_dolt_schemas WHERE diff_type = "removed"'
    [ "$status" -eq 0 ]
    [[ "$output" =~ " 1 " ]] || false  # original_trigger
    
    # Test that we can identify specific changes
    run dolt sql -q 'SELECT to_name FROM dolt_diff_dolt_schemas WHERE diff_type = "added" ORDER BY to_name'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "new_event" ]] || false
    [[ "$output" =~ "new_view" ]] || false
    
    run dolt sql -q 'SELECT to_name FROM dolt_diff_dolt_schemas WHERE diff_type = "modified"'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "original_view" ]] || false
    
    run dolt sql -q 'SELECT from_name FROM dolt_diff_dolt_schemas WHERE diff_type = "removed"'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "original_trigger" ]] || false
    
    # Test that diff_type values are correct
    run dolt sql -q 'SELECT DISTINCT diff_type FROM dolt_diff_dolt_schemas ORDER BY diff_type'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "added" ]] || false
    [[ "$output" =~ "modified" ]] || false
    [[ "$output" =~ "removed" ]] || false
    
    # Test that from_commit is always populated (should be commit hashes)
    run dolt sql -q 'SELECT COUNT(*) FROM dolt_diff_dolt_schemas WHERE from_commit IS NOT NULL'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "6" ]] || false
    
    # Test that from_commit is a valid commit hash (not "EMPTY" or "WORKING")
    run dolt sql -q 'SELECT DISTINCT from_commit FROM dolt_diff_dolt_schemas'
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "EMPTY" ]] || false
    [[ ! "$output" =~ "WORKING" ]] || false
    # Should be a 32-character hash
    [[ "$output" =~ [a-z0-9]{32} ]] || false
    
    # Test timestamp conversion works correctly (was causing conversion errors)
    run dolt sql -q "SELECT * FROM dolt_diff_dolt_schemas LIMIT 1" -r vertical
    [ "$status" -eq 0 ]
    [[ "$output" =~ "to_commit_date:" ]] || false
    [[ "$output" =~ "from_commit_date:" ]] || false
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
    run dolt sql -q "select type from dolt_help where name='dolt_rebase'"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "procedure" ]] || false

    run dolt sql -q "select synopsis from dolt_help where name='dolt_backup'"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "dolt backup [-v | --verbose]" ]] || false
    [[ "$output" =~ "dolt backup restore [--force] <url> <name>" ]] || false

    run dolt sql -q "select short_description from dolt_help where name='dolt_commit'"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Record changes to the database" ]] || false

    run dolt sql -q "select long_description from dolt_help where name='dolt_add'"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "This command updates the list of tables using the current content found in the working root" ]] || false
    [[ "$output" =~ "This command can be performed multiple times before a commit." ]] || false

    run dolt sql -q "select arguments from dolt_help where name='dolt_pull'"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "<remote>".*"The name of the remote to pull from." ]] || false
    [[ "$output" =~ "<remoteBranch>".*"The name of a branch on the specified remote to be merged into the current working set." ]] || false
    [[ "$output" =~ "-f, --force".*"Update from the remote HEAD even if there are errors." ]] || false

    run dolt sql -q "select arguments from dolt_help where name='dolt_tag'"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "-m <msg>, --message=<msg>".*"Use the given msg as the tag message." ]] || false
}

@test "system-tables: query dolt_history_dolt_procedures system table" {
    # Set up test data with procedures across multiple commits
    dolt sql -q "CREATE PROCEDURE test_proc1(x INT) BEGIN SELECT x * 2 as result; END"
    dolt add .
    dolt commit -m "add first procedure"
    
    dolt sql -q "CREATE PROCEDURE test_proc2(name VARCHAR(50)) BEGIN SELECT CONCAT('Hello, ', name) as greeting; END"
    dolt add .
    dolt commit -m "add second procedure"
    
    dolt sql -q "DROP PROCEDURE test_proc1"
    dolt sql -q "CREATE PROCEDURE test_proc1(x INT, y INT) BEGIN SELECT x + y as sum; END"  # modified
    dolt add .
    dolt commit -m "modify first procedure"
    
    # Test that the table exists and has correct schema
    run dolt sql -r csv -q 'DESCRIBE dolt_history_dolt_procedures'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "name,varchar(64)" ]] || false
    [[ "$output" =~ "create_stmt,varchar(4096)" ]] || false
    [[ "$output" =~ "created_at,timestamp" ]] || false
    [[ "$output" =~ "modified_at,timestamp" ]] || false
    [[ "$output" =~ "sql_mode,varchar(256)" ]] || false
    [[ "$output" =~ "commit_hash,char(32)" ]] || false
    [[ "$output" =~ "committer,varchar(1024)" ]] || false
    [[ "$output" =~ "commit_date,datetime" ]] || false
    
    # Test that we have procedure history across commits
    run dolt sql -q 'SELECT COUNT(*) FROM dolt_history_dolt_procedures'
    [ "$status" -eq 0 ]
    # Should have entries for: test_proc1 (3 commits), test_proc2 (2 commits) = 5 total
    [[ "$output" =~ "5" ]] || false
    
    # Test filtering by procedure name
    run dolt sql -q 'SELECT COUNT(*) FROM dolt_history_dolt_procedures WHERE name = "test_proc1"'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "3" ]] || false
    
    run dolt sql -q 'SELECT COUNT(*) FROM dolt_history_dolt_procedures WHERE name = "test_proc2"'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "2" ]] || false
    
    # Test that procedure definitions are captured correctly
    run dolt sql -q 'SELECT name FROM dolt_history_dolt_procedures WHERE create_stmt LIKE "%x * 2%"'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test_proc1" ]] || false
    
    run dolt sql -q 'SELECT name FROM dolt_history_dolt_procedures WHERE create_stmt LIKE "%x + y%"'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test_proc1" ]] || false
    
    # Test commit metadata is present for all entries
    run dolt sql -q 'SELECT COUNT(*) FROM dolt_history_dolt_procedures WHERE commit_hash IS NOT NULL AND committer IS NOT NULL'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "5" ]] || false
    
    # Test distinct procedure names
    run dolt sql -q 'SELECT DISTINCT name FROM dolt_history_dolt_procedures ORDER BY name'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test_proc1" ]] || false
    [[ "$output" =~ "test_proc2" ]] || false
    
    # Test that all entries have created_at and modified_at timestamps
    run dolt sql -q 'SELECT COUNT(*) FROM dolt_history_dolt_procedures WHERE created_at IS NOT NULL AND modified_at IS NOT NULL'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "5" ]] || false
}
