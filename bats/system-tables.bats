#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}

@test "Show list of system tables using dolt ls --system or --all" {
    dolt sql -q "create table test (pk int, c1 int, primary key(pk))"
    dolt sql -q "show tables" --save "BATS query"
    dolt ls --system
    run dolt ls --system
    [ $status -eq 0 ]
    [[ "$output" =~ "dolt_log" ]] || false
    [[ "$output" =~ "dolt_branches" ]] || false
    [[ "$output" =~ "dolt_query_catalog" ]] || false
    [[ ! "$output" =~ " test" ]] || false  # spaces are impt!
    run dolt ls --all
    [ $status -eq 0 ]
    [[ "$output" =~ "dolt_log" ]] || false
    [[ "$output" =~ "dolt_branches" ]] || false
    [[ "$output" =~ "dolt_query_catalog" ]] || false
    [[ "$output" =~ "test" ]] || false
    dolt add test
    dolt commit -m "Added test table"
    run dolt ls --system
    [ $status -eq 0 ]
    [[ "$output" =~ "dolt_history_test" ]] || false
    [[ "$output" =~ "dolt_diff_test" ]] || false
    run dolt ls --all
    [ $status -eq 0 ]
    [[ "$output" =~ "dolt_history_test" ]] || false
    [[ "$output" =~ "dolt_diff_test" ]] || false
}

@test "dolt ls --system -v shows history and diff systems tables for deleted tables" {
    dolt sql -q "create table test (pk int, c1 int, primary key(pk))"
    dolt add test
    dolt commit -m "Added test table"
    dolt table rm test
    dolt add test
    dolt commit -m "Removed test table"
    run dolt ls --system
    [ $status -eq 0 ]
    [[ "$output" =~ "dolt_log" ]] || false
    [[ "$output" =~ "dolt_branches" ]] || false
    [[ ! "$output" =~ "dolt_history_test" ]] || false
    [[ ! "$output" =~ "dolt_diff_test" ]] || false
    run dolt ls --system -v
    [ $status -eq 0 ]
    [[ "$output" =~ "dolt_log" ]] || false
    [[ "$output" =~ "dolt_branches" ]] || false
    [[ "$output" =~ "dolt_history_test" ]] || false
    [[ "$output" =~ "dolt_diff_test" ]] || false
}

@test "dolt ls --system -v shows history and diff systems tables for tables on other branches" {
    dolt checkout -b add-table-branch
    dolt sql -q "create table test (pk int, c1 int, primary key(pk))"
    dolt add test
    dolt commit -m "Added test table"
    dolt checkout master
    run dolt ls --system
    [ $status -eq 0 ]
    [[ "$output" =~ "dolt_log" ]] || false
    [[ "$output" =~ "dolt_branches" ]] || false
    [[ ! "$output" =~ "dolt_history_test" ]] || false
    [[ ! "$output" =~ "dolt_diff_test" ]] || false
    run dolt ls --system -v
    [ $status -eq 0 ]
    [[ "$output" =~ "dolt_log" ]] || false
    [[ "$output" =~ "dolt_branches" ]] || false
    [[ "$output" =~ "dolt_history_test" ]] || false
    [[ "$output" =~ "dolt_diff_test" ]] || false
}

@test "query dolt_log system table" {
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

@test "query dolt_branches system table" {
    dolt checkout -b create-table-branch
    dolt sql -q "create table test (pk int, c1 int, primary key(pk))"
    dolt add test
    dolt commit -m "Added test table"
    run dolt sql -q "select * from dolt_branches"
    [ $status -eq 0 ]
    [[ "$output" =~ master.*Initialize\ data\ repository ]] || false
    [[ "$output" =~ create-table-branch.*Added\ test\ table ]] || false
    run dolt sql -q "select * from dolt_branches where latest_commit_message ='Initialize data repository'"
    [ $status -eq 0 ]
    [[ "$output" =~ "master" ]] || false
    [[ ! "$output" =~ "create-table-branch" ]] || false
    run dolt sql -q "select * from dolt_branches where latest_commit_message ='Added test table'"
    [ $status -eq 0 ]
    [[ ! "$output" =~ "master" ]] || false
    [[ "$output" =~ "create-table-branch" ]] || false
}

@test "query dolt_diff_ system table" {
    dolt sql -q "CREATE TABLE test (pk INT, c1 INT, PRIMARY KEY(pk))"
    dolt add test
    dolt commit -m "Added test table"
    dolt branch create_checkpoint master
    dolt sql -q "INSERT INTO test VALUES (0,0),(1,1),(2,2)"
    dolt add test
    dolt commit -m "Added rows"
    dolt branch inserted_rows master
    dolt sql -q "INSERT INTO test VALUES (3,3)"
    dolt sql -q "UPDATE test SET c1=5 WHERE pk=1"
    dolt sql -q "DELETE FROM test WHERE pk=2"

    EXPECTED=$(echo -e "to_pk,to_c1,from_pk,from_c1,diff_type\n0,0,,,added\n1,1,,,added\n2,2,,,added\n1,5,1,1,modified\n,,2,2,removed\n3,3,,,added")
    run dolt sql -r csv -q 'SELECT to_pk, to_c1, from_pk, from_c1, diff_type FROM dolt_diff_test ORDER BY from_commit_date'
    echo $output
    [ "$status" -eq 0 ]
    [[ "$output" =~ "$EXPECTED" ]] || false

    EXPECTED=$(echo -e "to_pk,to_c1,from_pk,from_c1,diff_type\n1,5,1,1,modified\n,,2,2,removed\n3,3,,,added")
    run dolt sql -r csv -q 'SELECT to_pk, to_c1, from_pk, from_c1, diff_type FROM dolt_diff_test WHERE to_commit = "WORKING" ORDER BY from_commit_date'
    echo $output
    [ "$status" -eq 0 ]
    [[ "$output" =~ "$EXPECTED" ]] || false


    EXPECTED=$(echo -e "to_pk,to_c1,from_pk,from_c1,diff_type\n0,0,,,added\n1,1,,,added\n2,2,,,added")
    run dolt sql -r csv -q 'SELECT to_pk, to_c1, from_pk, from_c1, diff_type FROM dolt_diff_test WHERE from_commit = HASHOF("create_checkpoint") AND to_commit = HASHOF("inserted_rows") ORDER BY from_commit_date'
    echo $output
    [ "$status" -eq 0 ]
    [[ "$output" =~ "$EXPECTED" ]] || false
}

@test "query dolt_diff_ system table without committing table" {
    dolt sql -q "create table test (pk int not null primary key);"
    dolt sql -q "insert into test values (0), (1);"

    EXPECTED=$(echo -e "to_pk,to_commit,from_pk,diff_type\n0,WORKING,,added\n1,WORKING,,added")
    run dolt sql -r csv -q 'select to_pk, to_commit, from_pk, diff_type from dolt_diff_test;'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "$EXPECTED" ]] || false
}



@test "query dolt_history_ system table" {
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
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 7 ]
    run dolt sql -q "select * from dolt_history_test where pk=1"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    run dolt sql -q "select * from dolt_history_test where pk=0"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]
}
