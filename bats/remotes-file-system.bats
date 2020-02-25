#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    [ -d /var/tmp ]
    cd $BATS_TMPDIR
    cd dolt-repo-$$
    mkdir "dolt-repo-clones"
}

teardown() {
    teardown_common
}

@test "Add a file system based remote" {
    dolt remote add origin file:///var/tmp/
    run dolt remote -v
    [ $status -eq 0 ]
    [[ "$output" =~ "file:///var/tmp" ]] || false 
}

@test "Add a file system remote with a bad path" {
    skip "Bad error message"
    run dolt remote add origin file:///poop/
    [ $status -ne 0 ]
    [[ ! "$output" =~ "'' is not valid" ]] || false
}

@test "push, pull, and clone file based remotes" {
    # seed with some data
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt add test
    dolt commit -m "test commit"

    # push to a file based remote
    mkdir remotedir
    dolt remote add origin file://remotedir
    dolt push origin master

    # clone from a directory
    cd dolt-repo-clones
    dolt clone file://../remotedir test-repo
    cd test-repo

    # make modifications
    dolt sql -q "insert into test values (0, 0, 0, 0, 0, 1)"
    dolt add test
    dolt commit -m "put row"

    # push back to the other directory
    dolt push origin master
    run dolt branch --list master -v
    master_state1=$output

    # check that the remote master was updated
    cd ../..
    dolt pull
    run dolt branch --list master -v
    [[ "$output" = "$master_state1" ]] || false
}

@test "clone, fetch, and push from multiple file system remotes" {
    # seed with some data
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt add test
    dolt commit -m "test commit"

    # create the remote data storage directories
    mkdir remote1
    mkdir remote2

    # push to a file based remote
    dolt remote add remote2 file://remote2
    dolt push remote2 master

    # fetch fail for unspecified remote
    run dolt fetch
    [ "$status" -eq 1 ]

    # succeed when specifying a remote
    dolt fetch remote2

    #add origin push and fetch
    dolt remote add origin file://remote1
    dolt push master:notmaster

    #fetch should now work without a specified remote because origin exists
    dolt fetch

    # fetch master into some garbage tracking branches
    dolt fetch refs/heads/notmaster:refs/remotes/anything/master
    dolt fetch remote2 refs/heads/master:refs/remotes/something/master

    run dolt branch -a
    [[ "$output" =~ "remotes/anything/master" ]] || false
    [[ "$output" =~ "remotes/something/master" ]] || false
}
