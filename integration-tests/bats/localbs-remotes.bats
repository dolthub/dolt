#!/usr/bin/env bats

# Local Blobstore exercises the same code path used by GCS remotes using the local filesystem

load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    cd $BATS_TMPDIR
    cd dolt-repo-$$
    mkdir "dolt-repo-clones"
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "localbs-remotes: push, pull, and clone localbs remote" {
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

    # push to a localbs based remote
    mkdir remotedir
    dolt remote add origin localbs://remotedir
    dolt push origin master

    # clone from a directory
    cd dolt-repo-clones
    dolt clone localbs://../remotedir test-repo
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

@test "localbs-remotes: clone, fetch, and push from multiple localbs system remotes" {
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

    # push to a localbs based remote
    dolt remote add remote2 localbs://remote2
    dolt push remote2 master

    # fetch fail for unspecified remote
    run dolt fetch
    [ "$status" -eq 1 ]

    # succeed when specifying a remote
    dolt fetch remote2

    #add origin push and fetch
    dolt remote add origin localbs://remote1
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

@test "localbs-remotes: fetch displays and updates branch list" {
    # create a new branch
    run dolt checkout -b tester
    [ "$status" -eq 0 ]

    # demonstrate there is no table named test on tester
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "${lines[0]}" =~ "No tables in working set" ]] || false

    # seed tester with some data
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

    # push both branches to remote
    dolt remote add origin localbs://remote1
    dolt push origin tester

    dolt checkout master
    dolt push origin master

    # clone from a directory
    cd dolt-repo-clones
    dolt clone localbs://../remote1 test-repo
    cd test-repo

    run dolt branch -a
    [ "$status" -eq 0 ]
    [[ "$output" =~ "* master" ]] || false
    [[ "$output" =~ "remotes/origin/tester" ]] || false
    [[ "$output" =~ "remotes/origin/master" ]] || false
}

@test "localbs-remotes: clone from a directory that is not a dolt repo" {
    mkdir remotedir
    touch remotedir/some-junk

    cd dolt-repo-clones
    run dolt clone localbs://../remotedir test-repo
    [ "$status" -eq 1 ]
    [ ! -d test-repo ]
    cd ..
}
