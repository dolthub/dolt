#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common

    dolt sql <<SQL
CREATE TABLE test (
    pk int primary key
);
INSERT INTO test VALUES (0),(1),(2);
SQL
    dolt add .
    dolt commit -m "created table test"
    dolt sql <<SQL
DELETE FROM test WHERE pk = 0;
INSERT INTO test VALUES (3);
SQL
    dolt add .
    dolt commit -m "made changes"
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "commit_tags: create a tag with a explicit ref" {
    run dolt tag v1 HEAD^
    [ $status -eq 0 ]
    run dolt tag
    [ $status -eq 0 ]
    [[ "$output" =~ "v1" ]] || false
}

@test "commit_tags: create a tag with implicit head ref" {
    run dolt tag v1
    [ $status -eq 0 ]
    run dolt tag
    [ $status -eq 0 ]
    [[ "$output" =~ "v1" ]] || false
}

@test "commit_tags: create tag v1.2.3" {
    skip "Noms doesn't support '.' in dataset names"
    run dolt tag v1.2.3
    [ $status -eq 0 ]
}

@test "commit_tags: delete a tag" {
    dolt tag v1
    dolt tag -d v1
    run dolt tag
    [ $status -eq 0 ]
    [ "$output" = "" ]
}

@test "commit_tags: checkout a tag" {
    dolt branch comp HEAD^
    dolt tag v1 HEAD^
    skip "need to implelement detached head first"
    run dolt checkout v1
    [ $status -eq 0 ]
    run dolt diff comp
    [ $status -eq 0 ]
    [ "$output" = "" ]
}

@test "commit_tags: commit onto checked out tag" {
    dolt tag v1 HEAD^
    skip "need to implement detached head first"
    dolt checkout v1
    run dolt sql -q "insert into test values (8),(9)"
    [ $status -eq 0 ]
    dolt add -A
    run dolt commit -m "msg"
    [ $status -eq 0 ]
}

@test "commit_tags: use a tag as ref for diff" {
    skip_nbf_dolt_1
    dolt tag v1 HEAD^
    run dolt diff v1
    [ $status -eq 0 ]
    [[ "$output" =~ "-  | 0" ]]
    [[ "$output" =~ "+  | 3" ]]
}

@test "commit_tags: use a tag as a ref for merge" {
    dolt tag v1 HEAD
    dolt checkout -b other HEAD^
    dolt sql -q "insert into test values (8),(9)"
    dolt add -A && dolt commit -m 'made changes'
    run dolt merge v1
    [ $status -eq 0 ]
    run dolt sql -q "select * from test"
    [ $status -eq 0 ]
    [[ "$output" =~ "1" ]]
    [[ "$output" =~ "2" ]]
    [[ "$output" =~ "3" ]]
    [[ "$output" =~ "8" ]]
    [[ "$output" =~ "9" ]]
}

@test "commit_tags: push/pull tags to/from a remote" {
    # reset env
    rm -rf .dolt
    mkdir repo remote
    cd repo

    dolt init
    dolt sql -q "create table test (pk int primary key);"
    dolt sql -q "insert into test values (0),(1),(2);"
    dolt add -A && dolt commit -m "table test"
    dolt sql -q "insert into test values (7),(8),(9);"
    dolt add -A && dolt commit -m "more rows"

    dolt remote add origin file://../remote
    dolt push origin main
    cd .. && dolt clone file://remote repo_clone && cd repo

    run dolt tag v1 HEAD^
    [ $status -eq 0 ]
    run dolt tag v2 HEAD -m "SAMO"
    [ $status -eq 0 ]

    skip "todo"
    run dolt push origin master
    [ $status -eq 0 ]
    cd ../repo_clone
    run dolt pull
    [ $status -eq 0 ]
    run dolt tag
    [ $status -eq 0 ]
    [[ "$output" =~ "v1" ]] || false
    [[ "$output" =~ "v2" ]] || false
    run dolt tag -v
    [ $status -eq 0 ]
    [[ "$output" =~ "SAMO" ]] || false
}
