#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common

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
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "sql-status: status properly works with working and staged tables" {
    run dolt sql -r csv -q "select * from dolt_status"
    [ "$status" -eq 0 ]
    [[ "$output" =~ 'test,false,new table' ]] || false
    dolt add .

    # Confirm table is now marked as staged
    run dolt sql -r csv -q "select * from dolt_status"
    [ "$status" -eq 0 ]
    [[ "$output" =~ 'test,true,new table' ]] || false
}

@test "sql-status: status properly works with table rename" {
    # Test is staged
    dolt add test
    run dolt sql -r csv -q "select * from dolt_status"
    [ "$status" -eq 0 ]
    [[ "$output" =~ 'test,true,new table' ]] || false

    # Rename test to test2
    run dolt sql -r csv -q "alter table test rename to test2"
    [ "$status" -eq 0 ]

    # Confirm table is now marked as renamed, test still staged
    run dolt sql -r csv -q "select * from dolt_status"
    [ "$status" -eq 0 ]
    [[ "$output" =~ 'test,true,new table' ]] || false
    [[ "$output" =~ 'test -> test2,false,renamed' ]] || false

    # Confirm table is now marked as staged
    dolt add test2
    run dolt sql -r csv -q "select * from dolt_status"
    [ "$status" -eq 0 ]
    [[ "$output" =~ 'test2,true,new table' ]] || false
}

@test "sql-status: table that has staged and unstaged changes shows up twice" {
    # Stage one set of changes.
    dolt add test

    # Make a modification that isn't staged.
    dolt sql -q "insert into test values (1, 1, 1, 1, 1, 1)"

    run dolt sql -r csv -q "select * from dolt_status"

    [ "$status" -eq 0 ]
    [[ "$output" =~ 'test,true,new table' ]] || false
    [[ "$output" =~ 'test,false,modified' ]] || false
}

@test "sql-status: status properly works with docs" {
    echo readme-text > README.md
    dolt docs upload README.md README.md
    echo license-text > LICENSE.md
    dolt docs upload LICENSE.md LICENSE.md

    dolt sql -r csv -q "select * from dolt_status ORDER BY table_name"
    run dolt sql -r csv -q "select * from dolt_status ORDER BY table_name"
    [ "$status" -eq 0 ]
    [[ "$output" =~ 'dolt_docs,false,new table' ]] || false
}

@test "sql-status: status works property with working tables in conflict" {
    # Start by causing the conflict.
    dolt sql -q "insert into test values (0, 0, 0, 0, 0, 0)"
    dolt add test
    dolt commit -m "table created"
    dolt branch change-cell
    dolt sql -q "replace into test values (0, 1, 1, 1, 1, 1)"
    dolt add test
    dolt commit -m "changed pk=0 all cells to 1"
    dolt checkout change-cell
    dolt sql -q "replace into test values (0, 11, 11, 11, 11, 11)"
    dolt add test
    dolt commit -m "changed pk=0 all cells to 11"
    dolt checkout main
    run dolt merge change-cell -m "merge change-cell"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONFLICT" ]] || false

    run dolt sql -r csv -q "select * from dolt_status"
    [ "$status" -eq 0 ]
    [[ "$output" =~ 'test,false,conflict' ]] || false
}

@test "sql-status: status works properly with working docs in conflict" {
     echo "a readme" > README.md
     dolt docs upload README.md README.md
     dolt add .
     dolt commit -m "Committing initial docs"

     dolt branch test-a
     dolt branch test-b

     dolt checkout test-a
     echo test-a branch > README.md
     dolt docs upload README.md README.md
     dolt add .
     dolt commit -m "Changed README.md on test-a branch"

     dolt checkout test-b
     run dolt docs print README.md
     [[ $output =~ "a readme" ]] || false
     [[ ! $output =~ "test-a branch" ]] || false

     echo test-b branch > README.md
     dolt docs upload README.md README.md
     dolt add .
     dolt commit -m "Changed README.md on test-a branch"
     dolt checkout main

     # On successful FF merge, docs match the new working root
     run dolt merge test-a
     [ "$status" -eq 0 ]
     [[ $output =~ "Fast-forward" ]] || false
     run dolt docs print README.md
     [[ "$output" =~ "test-a branch" ]] || false

     # A merge with conflicts does not change the working root.
     # If the conflicts are resolved with --ours, the working root and the docs on the filesystem remain the same.
     run dolt merge test-b -m "merge test-b"
     [ "$status" -eq 1 ]
     [[ $output =~ "CONFLICT" ]] || false

     dolt status
     run dolt sql -r csv -q "select * from dolt_status ORDER BY status"
     [ "$status" -eq 0 ]
     [[ "$output" =~ 'dolt_docs,false,conflict' ]] || false
}

@test "sql-status: tables with no observable changes don't show in status but can be staged" {
dolt sql <<SQL
create table t1 (id int primary key);
insert into t1 values (1);
SQL
    dolt add -A && dolt commit -m "create table t1"
    dolt sql <<SQL
create table temp__t1 (id int primary key);
insert into temp__t1 values (1);
drop table t1;
rename table temp__t1 to t1;
SQL
    dolt sql -q "select * from dolt_status;"
    run dolt sql -q "select * from dolt_status;"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 0 ]

    run dolt diff t1
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 0 ]

    dolt add t1

    dolt sql -r csv -q "select * from dolt_status;"
    run dolt sql -r csv -q "select * from dolt_status;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "t1,true,modified" ]] || false
}
