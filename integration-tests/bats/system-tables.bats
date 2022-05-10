#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    skiponwindows "tests are flaky on Windows"
    setup_common
    skip_nbf_dolt_1

    # Needed for dolt_branches test
    cd $BATS_TMPDIR
    mkdir remotes-$$
    mkdir remotes-$$/empty
    echo remotesrv log available here $BATS_TMPDIR/remotes-$$/remotesrv.log
    remotesrv --http-port 1234 --dir ./remotes-$$ &> ./remotes-$$/remotesrv.log 3>&- &
    remotesrv_pid=$!
    cd dolt-repo-$$
    mkdir "dolt-repo-clones"
}

teardown() {
    skiponwindows "tests are flaky on Windows"
    assert_feature_version
    teardown_common
    kill $remotesrv_pid
    rm -rf $BATS_TMPDIR/remotes-$$
}

@test "system-tables: Show list of system tables using dolt ls --system or --all" {
    dolt sql -q "create table test (pk int, c1 int, primary key(pk))"
    dolt sql -q "show tables" --save "BATS query"
    dolt ls --system
    run dolt ls --system
    [ $status -eq 0 ]
    [[ "$output" =~ "dolt_log" ]] || false
    [[ "$output" =~ "dolt_conflicts" ]] || false
    [[ "$output" =~ "dolt_branches" ]] || false
    [[ "$output" =~ "dolt_remotes" ]] || false
    [[ "$output" =~ "dolt_query_catalog" ]] || false
    [[ "$output" =~ "dolt_status" ]] || false
    [[ ! "$output" =~ " test" ]] || false  # spaces are impt!
    run dolt ls --all
    [ $status -eq 0 ]
    [[ "$output" =~ "dolt_log" ]] || false
    [[ "$output" =~ "dolt_commits" ]] || false
    [[ "$output" =~ "dolt_commit_ancestors" ]] || false
    [[ "$output" =~ "dolt_conflicts" ]] || false
    [[ "$output" =~ "dolt_branches" ]] || false
    [[ "$output" =~ "dolt_remotes" ]] || false
    [[ "$output" =~ "dolt_query_catalog" ]] || false
    [[ "$output" =~ "dolt_status" ]] || false
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

@test "system-tables: dolt ls --system -v shows history and diff systems tables for deleted tables" {
    dolt sql -q "create table test (pk int, c1 int, primary key(pk))"
    dolt add test
    dolt commit -m "Added test table"
    dolt table rm test
    dolt add test
    dolt commit -m "Removed test table"
    run dolt ls --system
    [ $status -eq 0 ]
    [[ "$output" =~ "dolt_log" ]] || false
    [[ "$output" =~ "dolt_conflicts" ]] || false
    [[ "$output" =~ "dolt_branches" ]] || false
    [[ "$output" =~ "dolt_remotes" ]] || false
    [[ ! "$output" =~ "dolt_history_test" ]] || false
    [[ ! "$output" =~ "dolt_diff_test" ]] || false
    [[ ! "$output" =~ "dolt_commit_diff_test" ]] || false
    run dolt ls --system -v
    [ $status -eq 0 ]
    [[ "$output" =~ "dolt_log" ]] || false
    [[ "$output" =~ "dolt_conflicts" ]] || false
    [[ "$output" =~ "dolt_branches" ]] || false
    [[ "$output" =~ "dolt_remotes" ]] || false
    [[ "$output" =~ "dolt_history_test" ]] || false
    [[ "$output" =~ "dolt_commit_diff_test" ]] || false
}

@test "system-tables: dolt ls --system -v shows history and diff systems tables for tables on other branches" {
    dolt checkout -b add-table-branch
    dolt sql -q "create table test (pk int, c1 int, primary key(pk))"
    dolt add test
    dolt commit -m "Added test table"
    dolt checkout main
    run dolt ls --system
    [ $status -eq 0 ]
    [[ "$output" =~ "dolt_log" ]] || false
    [[ "$output" =~ "dolt_conflicts" ]] || false
    [[ "$output" =~ "dolt_branches" ]] || false
    [[ "$output" =~ "dolt_remotes" ]] || false
    [[ ! "$output" =~ "dolt_history_test" ]] || false
    [[ ! "$output" =~ "dolt_diff_test" ]] || false
    [[ ! "$output" =~ "dolt_commit_diff_test" ]] || false
    run dolt ls --system -v
    [ $status -eq 0 ]
    [[ "$output" =~ "dolt_log" ]] || false
    [[ "$output" =~ "dolt_conflicts" ]] || false
    [[ "$output" =~ "dolt_branches" ]] || false
    [[ "$output" =~ "dolt_remotes" ]] || false
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
    run dolt sql -q "select * from dolt_remotes" -r csv
    [ $status -eq 0 ]
    [[ "${lines[0]}" = name,url,fetch_specs,params ]] || false
    [[ "${lines[1]}" =~ origin,$regex,[refs/heads/*:refs/remotes/origin/*,map[] ]] || false
}

@test "system-tables: check unsupported dolt_remote behavior" {
    run dolt sql -q "insert into dolt_remotes (name, url) values ('origin1', 'file://remote')"
    [ $status -ne 0 ]
    [[ "$output" =~ "cannot insert remote in an SQL session" ]] || false

    mkdir remote
    dolt remote add origin file://remote/
    run dolt sql -q "delete from dolt_remotes where name = 'origin'"
    [ $status -ne 0 ]
    [[ "$output" =~ "cannot delete remote in an SQL session" ]] || false
}

@test "system-tables: insert into dolt_remotes system table" {
    skip "Remotes table not yet mutable in SQL session"
    run dolt sql -q "insert into dolt_remotes (name, url) values ('origin', 'file://remote')"
    [ $status -ne 0 ]
    [[ ! "$output" =~ panic ]] || false

    mkdir remote
    dolt sql -q "insert into dolt_remotes (name, url) values ('origin1', 'file://remote')"
    dolt sql -q "insert into dolt_remotes (name, url) values ('origin2', 'aws://[dynamo_db_table:s3_bucket]/repo_name')"

    run dolt sql -q "select count(*) from dolt_remotes" -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ 2 ]] || false

    file_regex='file://.*/remote'
    aws_regex='aws://.*/repo_name'
    run dolt sql -q "select * from dolt_remotes" -r csv
    [ $status -eq 0 ]
    [[ "${lines[0]}" = name,url,fetch_specs,params ]] || false
    [[ "${lines[1]}" =~ origin1,$file_regex,[refs/heads/*:refs/remotes/origin/*,map[] ]] || false
    [[ "${lines[2]}" =~ origin2,$aws_regex,[refs/heads/*:refs/remotes/origin/*,map[] ]] || false

}

@test "system-tables: delete from dolt_remotes system table" {
    skip "Remotes table not yet mutable in SQL session"
    mkdir remote
    dolt remote add origin file://remote/

    run dolt sql -q "select count(*) from dolt_remotes"
    [ $status -eq 0 ]
    [[ "$output" =~ 1 ]] || false

    regex='file://.*/remote'
    run dolt sql -q "select * from dolt_remotes" -r csv
    [ $status -eq 0 ]
    [[ "${lines[0]}" = name,url,fetch_specs,params ]] || false
    [[ "${lines[1]}" =~ origin,$regex,[refs/heads/*:refs/remotes/origin/*,map[] ]] || false

    dolt sql -q "delete from dolt_remotes where name = 'origin1'"

    run dolt sql -q "select count(*) from dolt_remotes"
    [ $status -eq 0 ]
    [[ "$output" =~ 1 ]] || false

    dolt sql -q "delete from dolt_remotes where name = 'origin'"

    run dolt sql -q "select count(*) from dolt_remotes" -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ 0 ]] || false
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
    dolt merge other
    dolt add -A && dolt commit -m "commit M"

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
    dolt merge other
    dolt add -A && dolt commit -m "commit M"

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

@test "system-tables: dolt_branches table should include remote refs as well" {
    skip "This functionality needs to be implemented"

    # Create a remote with a test branch
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    run dolt push test-remote main
    dolt checkout -b test-branch
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  c1 BIGINT,
  c2 BIGINT,
  c3 BIGINT,
  c4 BIGINT,
  c5 BIGINT,
  PRIMARY KEY (pk)
);
SQL

    dolt add test
    dolt commit -m "test commit"
    dolt push test-remote test-branch

    # Clone the branch
    cd "dolt-repo-clones"
    run dolt clone http://localhost:50051/test-org/test-repo
    [ "$status" -eq 0 ]

    cd test-repo

    # Assert we are on main
    run dolt branch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "main" ]] || false
    [[ ! "$output" =~ "test-branch" ]] || false

    # Validate that the dolt_branches table has the remote test-branch (this is the failing part)
    run dolt sql -q "SELECT COUNT(*) from dolt_branches"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "2" ]] || false

    run dolt sql -q "SELECT COUNT(*) from dolt_branches WHERE name='test-branch'"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1" ]] || false
}

@test "system-tables: cannot delete last branch in dolt_branches" {
    run dolt sql -q "DELETE FROM dolt_branches"
    [ "$status" -ne 0 ]
}

@test "system-tables: dolt diff includes changes from initial commit" {
    dolt sql -q "CREATE TABLE test(pk int primary key, val int)"
    dolt sql -q "INSERT INTO test VALUES (1,1)"
    dolt commit -am "cm1"

    dolt sql -q "INSERT INTO test VALUES (2,2)"
    dolt commit -am "cm2"

    run dolt sql -q "SELECT to_val,to_pk FROM dolt_diff_test" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
}
