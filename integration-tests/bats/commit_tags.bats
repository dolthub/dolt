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

@test "commit_tags: create a tag with author arg given" {
    run dolt tag v1 --author "John Doe <john@doe.com>"
    [ $status -eq 0 ]
    run dolt tag -v
    [ $status -eq 0 ]
    [[ "$output" =~ "v1" ]] || false
    [[ "$output" =~ "Tagger: John Doe <john@doe.com>" ]] || false
}

@test "commit_tags: create tag v1.2.3" {
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
    skip "need to implement detached head first"
    run dolt checkout v1
    [ $status -eq 0 ]
    run dolt diff comp
    [ $status -eq 0 ]
    [ "$output" = "" ]
}

@test "commit_tags: commit onto checked out tag" {
    skip "need to implement detached head first"
    dolt tag v1 HEAD^
    dolt checkout v1
    run dolt sql -q "insert into test values (8),(9)"
    [ $status -eq 0 ]
    dolt add -A
    run dolt commit -m "msg"
    [ $status -eq 0 ]
}

@test "commit_tags: use a tag as ref for diff" {
    dolt tag v1 HEAD^
    run dolt diff v1
    [ $status -eq 0 ]
    [[ "$output" =~ "- | 0" ]] || false
    [[ "$output" =~ "+ | 3" ]] || false
}

@test "commit_tags: use a tag as a ref for merge" {
    dolt tag v1 HEAD
    dolt branch other HEAD~
    dolt checkout other
    dolt sql -q "insert into test values (8),(9)"
    dolt add -A && dolt commit -m 'made changes'
    run dolt merge v1 -m "merge v1"
    [ $status -eq 0 ]
    run dolt sql -q "select * from test"
    [ $status -eq 0 ]
    [[ "$output" =~ "1" ]] || false
    [[ "$output" =~ "2" ]] || false
    [[ "$output" =~ "3" ]] || false
    [[ "$output" =~ "8" ]] || false
    [[ "$output" =~ "9" ]] || false
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

    dolt remote add origin file://./../remote
    dolt push origin main

    run dolt tag v1 HEAD^
    [ $status -eq 0 ]
    run dolt tag v2 HEAD -m "SAMO"
    [ $status -eq 0 ]

    # tags are not pushed by default
    run dolt push origin main
    [ $status -eq 0 ]
    [[ "$output" =~ "Everything up-to-date" ]] || false

    cd .. && dolt clone file://./remote repo_clone && cd repo

    cd ../repo_clone
    run dolt pull --no-edit
    [ $status -eq 0 ]
    run dolt tag
    [ $status -eq 0 ]
    [[ ! "$output" =~ "v1" ]] || false
    [[ ! "$output" =~ "v2" ]] || false

    cd ../repo
    run dolt push origin v1
    [ $status -eq 0 ]
    run dolt push origin v2
    [ $status -eq 0 ]

    cd ../repo_clone
    run dolt pull --no-edit
    [ $status -eq 0 ]
    run dolt tag
    [ $status -eq 0 ]
    [[ "$output" =~ "v1" ]] || false
    [[ "$output" =~ "v2" ]] || false
    run dolt tag -v
    [ $status -eq 0 ]
    [[ "$output" =~ "SAMO" ]] || false
}

@test "commit_tags: create a tag with semver string" {
    dolt tag v1.0.0 HEAD^

    run dolt tag
    [ $status -eq 0 ]
    [[ "$output" =~ "v1.0.0" ]] || false

    dolt tag 1.0.0 HEAD
    run dolt tag
    [ $status -eq 0 ]
    [[ "$output" =~ "1.0.0" ]] || false
}
