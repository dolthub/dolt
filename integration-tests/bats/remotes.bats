#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

remotesrv_pid=
setup() {
    skiponwindows "tests are flaky on Windows"
    setup_common
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
    teardown_common
    kill $remotesrv_pid
    wait $remotesrv_pid || :
    remotesrv_pid=""
    rm -rf $BATS_TMPDIR/remotes-$$
}

@test "remotes: dolt remotes server is running" {
    ps -p $remotesrv_pid | grep remotesrv
}

@test "remotes: cli 'dolt checkout new_branch' without -b flag creates new branch and sets upstream if there is a remote branch with matching name" {
    mkdir remote
    mkdir repo1

    cd repo1
    dolt init
    dolt remote add origin file://../remote
    dolt push origin main
    dolt checkout -b other
    dolt push --set-upstream origin other

    cd ..
    dolt clone file://./remote repo2

    cd repo2
    dolt commit --allow-empty -m "a commit for main from repo2"
    dolt push
    run dolt branch
    [[ ! "$output" =~ "other" ]] || false

    run dolt checkout other
    [ "$status" -eq 0 ]
    [[ "$output" =~ "branch 'other' set up to track 'origin/other'." ]] || false

    run dolt status
    [[ "$output" =~ "Your branch is up to date with 'origin/other'." ]] || false
}

@test "remotes: guessing the remote branch fails if there are multiple remotes with branches with matching name" {
    mkdir remote
    mkdir repo1

    cd repo1
    dolt init
    dolt remote add origin file://../remote
    dolt push origin main
    dolt checkout -b other
    dolt push origin other

    cd ..
    dolt clone file://./remote repo2

    cd repo2
    dolt remote add test-remote file://../remote
    dolt fetch test-remote

    run dolt branch -a
    [[ "$output" =~ "remotes/origin/other" ]] || false
    [[ "$output" =~ "remotes/test-remote/other" ]] || false

    run dolt branch
    [[ ! "$output" =~ "other" ]] || false

    run dolt checkout other
    [ "$status" -eq 1 ]
    [[ "$output" =~ "'other' matched multiple (2) remote tracking branches" ]] || false
}

@test "remotes: cli 'dolt checkout -b new_branch' should not set upstream if there is a remote branch with matching name" {
    mkdir remote
    mkdir repo1

    cd repo1
    dolt init
    dolt remote add origin file://../remote
    dolt sql -q "CREATE TABLE a (pk int)"
    dolt add .
    dolt commit -am "add table a"
    dolt push --set-upstream origin main
    dolt checkout -b other
    dolt push --set-upstream origin other

    cd ..
    dolt clone file://./remote repo2

    cd repo2
    dolt branch
    [[ ! "$output" =~ "other" ]] || false

    run dolt checkout -b other
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "branch 'other' set up to track 'origin/other'." ]] || false

    run dolt status
    [[ ! "$output" =~ "Your branch is up to date with 'origin/other'." ]] || false

    cd ../repo1
    dolt checkout other
    dolt sql -q "INSERT INTO a VALUES (1), (2)"
    dolt commit -am "add table a"
    dolt push

    cd ../repo2
    dolt checkout other
    run dolt pull
    [ "$status" -eq 1 ]
    [[ "$output" =~ "There is no tracking information for the current branch." ]] || false
}

@test "remotes: call dolt_checkout('new_branch') without '-b' sets upstream if there is a remote branch with matching name" {
    mkdir remote
    mkdir repo1

    cd repo1
    dolt init
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    dolt sql -q "CREATE TABLE test (pk INT)"
    dolt add .
    dolt commit -am "main commit"
    dolt push test-remote main
    run dolt branch -a
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "remotes/test-remote/test-branch" ]] || false

    cd ..
    dolt clone --remote=test-remote http://localhost:50051/test-org/test-repo repo2
    run dolt branch -a
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "test-branch" ]] || false
    [[ ! "$output" =~ "remotes/test-remote/test-branch" ]] || false

    cd repo1
    dolt checkout -b test-branch
    dolt sql -q "INSERT INTO test VALUES (1);"
    dolt commit -am "test commit"
    dolt push test-remote test-branch

    cd ../repo2
    dolt fetch test-remote
    run dolt branch
    [[ ! "$output" =~ "test-branch" ]] || false

    run dolt sql << SQL
call dolt_checkout('test-branch');
SELECT * FROM test;
call dolt_pull();
SQL
    [ "$status" -eq 0 ]
    [[ "$output" =~ "pk" ]] || false
    [[ "$output" =~ "1" ]] || false

    run dolt branch
    [[ "$output" =~ "test-branch" ]] || false

    dolt checkout test-branch
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Your branch is up to date with 'test-remote/test-branch'." ]] || false
}

@test "remotes: select 'DOLT_CHECKOUT('-b','new_branch') should not set upstream if there is a remote branch with matching name" {
    mkdir remote
    mkdir repo1

    cd repo1
    dolt init
    dolt remote add origin file://../remote
    dolt sql -q "CREATE TABLE a (pk int)"
    dolt add .
    dolt commit -am "add table a"
    dolt push --set-upstream origin main
    dolt checkout -b other
    dolt push --set-upstream origin other

    cd ..
    dolt clone file://./remote repo2

    cd repo2
    dolt branch
    [[ ! "$output" =~ "other" ]] || false

    # Checkout with DOLT_CHECKOUT and confirm the table has the row added in the remote
    run dolt sql << SQL
call dolt_checkout('-b','other');
call dolt_pull();
SQL
    [ "$status" -eq 1 ]
    [[ "$output" =~ "There is no tracking information for the current branch." ]] || false
}

@test "remotes: add a remote using dolt remote" {
    run dolt remote add test-remote http://localhost:50051/test-org/test-repo
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt remote -v
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test-remote" ]] || false
    run dolt remote add test-remote
    [ "$status" -eq 1 ]
    [[ "$output" =~ "usage:" ]] || false
}

@test "remotes: remove a remote" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    run dolt remote remove test-remote
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt remote -v
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "test-remote" ]] || false
    run dolt remote remove poop
    [ "$status" -eq 1 ]
    [[ "$output" =~ "unknown remote: 'poop'" ]] || false
}

@test "remotes: clone a remote" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
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
    dolt push test-remote main
    cd "dolt-repo-clones"
    run dolt clone http://localhost:50051/test-org/test-repo
    [ "$status" -eq 0 ]
    [[ "$output" =~ "cloning http://localhost:50051/test-org/test-repo" ]] || false
    cd test-repo
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test commit" ]] || false
    run dolt status
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "LICENSE.md" ]] || false
    [[ ! "$output" =~ "README.md" ]] || false
    run ls
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "LICENSE.md" ]] || false
    [[ ! "$output" =~ "README.md" ]] || false
}

@test "remotes: clone a complicated remote" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    dolt sql -q "CREATE TABLE test (pk int primary key)"
    dolt sql -q "INSERT INTO test VALUES (1), (2), (3)"
    dolt add test
    dolt commit -m "test commit"
    dolt push test-remote main
    dolt push test-remote main:genesis-branch # In the beginning, there was a branch.
    dolt tag customtag main
    dolt push test-remote customtag

    ## Cloning from a remote which has tags and it's own remotes
    ## https://github.com/dolthub/dolt/issues/7043
    cd "dolt-repo-clones"
    dolt clone http://localhost:50051/test-org/test-repo clone-1
    cd clone-1

    # Assert that all the branches and tags are present
    run dolt branch -a
    [[ "$status" -eq 0 ]] || false
    [[ "$output" =~ "remotes/origin/main" ]] || false
    [[ "$output" =~ "remotes/origin/genesis-branch" ]] || false
    run dolt tag
    [[ "$status" -eq 0 ]] || false
    [[ "$output" =~ "customtag" ]] || false

    # Make a local branch, and backup to a different remote
    dolt checkout -b clone-1-branch HEAD
    dolt backup add mybackup http://localhost:50051/alternate-org/backup
    dolt backup sync mybackup

    cd ..
    dolt clone http://localhost:50051/alternate-org/backup clone-2
    cd clone-2

    # Assert that the backup creates remote branches which are correct.
    run dolt branch -a
    [[ "$status" -eq 0 ]] || false
    [[ "$output" =~ "remotes/origin/clone-1-branch" ]] || false
    [[ "$output" =~ "remotes/origin/main" ]] || false
    ! [[ "$output" =~ "remotes/origin/genesis-branch" ]] || false

    run dolt tag
    [[ "$status" -eq 0 ]] || false
    [[ "$output" =~ "customtag" ]] || false
}

@test "remotes: read tables test" {
    # create table t1 and commit
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    dolt sql <<SQL
CREATE TABLE t1 (
  pk BIGINT NOT NULL,
  PRIMARY KEY (pk)
);
SQL
    dolt add t1
    dolt commit -m "added t1"

    # create table t2 and commit
    dolt sql <<SQL
CREATE TABLE t2 (
  pk BIGINT NOT NULL,
  PRIMARY KEY (pk)
);
SQL
    dolt add t2
    dolt commit -m "added t2"

    # create table t3 and commit
    dolt sql <<SQL
CREATE TABLE t3 (
  pk BIGINT NOT NULL,
  PRIMARY KEY (pk)
);
SQL
    dolt add t3
    dolt commit -m "added t3"

    # push repo
    dolt push test-remote main
    cd "dolt-repo-clones"

    # Create a read latest tables and verify we have all the tables
    dolt read-tables http://localhost:50051/test-org/test-repo main
    cd test-repo
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "t1" ]] || false
    [[ "$output" =~ "t2" ]] || false
    [[ "$output" =~ "t3" ]] || false
    cd ..

    # Read specific table from latest with a specified directory
    dolt read-tables --dir clone_t1_t2 http://localhost:50051/test-org/test-repo main t1 t2
    cd clone_t1_t2
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "t1" ]] || false
    [[ "$output" =~ "t2" ]] || false
    [[ ! "$output" =~ "t3" ]] || false
    cd ..

    # Read tables from parent of parent of the tip of main. Should only have table t1
    dolt read-tables --dir clone_t1 http://localhost:50051/test-org/test-repo main~2
    cd clone_t1
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "t1" ]] || false
    [[ ! "$output" =~ "t2" ]] || false
    [[ ! "$output" =~ "t3" ]] || false
    cd ..
}

@test "remotes: clone a remote with docs" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    echo "license-text" > LICENSE.md
    dolt docs upload LICENSE.md LICENSE.md
    echo "readme-text" > README.md
    dolt docs upload README.md README.md
    dolt add .
    dolt commit -m "test doc commit"
    dolt push test-remote main
    cd "dolt-repo-clones"
    run dolt clone http://localhost:50051/test-org/test-repo
    [ "$status" -eq 0 ]
    [[ "$output" =~ "cloning http://localhost:50051/test-org/test-repo" ]] || false
    cd test-repo
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test doc commit" ]] || false
    run dolt status
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "LICENSE.md" ]] || false
    [[ ! "$output" =~ "README.md" ]] || false
    dolt docs print LICENSE.md > LICENSE.md
    dolt docs print README.md > README.md
    run ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "LICENSE.md" ]] || false
    [[ "$output" =~ "README.md" ]] || false
    run cat LICENSE.md
    [ "$status" -eq 0 ]
    [[ "$output" =~ "license-text" ]] || false
    run cat README.md
    [ "$status" -eq 0 ]
    [[ "$output" =~ "readme-text" ]] || false
}

@test "remotes: clone an empty remote" {
    run dolt clone http://localhost:50051/test-org/empty
    [ "$status" -eq 1 ]
    [[ "$output" =~ "clone failed" ]] || false
    [[ "$output" =~ "remote at that url contains no Dolt data" ]] || false
}

@test "remotes: clone a non-existent remote" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    cd "dolt-repo-clones"
    run dolt clone http://localhost:50051/foo/bar
    [ "$status" -eq 1 ]
    [[ "$output" =~ "clone failed" ]] || false
    [[ "$output" =~ "remote at that url contains no Dolt data" ]] || false
}

@test "remotes: clone a different branch than main" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    dolt checkout -b test-branch
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
    dolt push test-remote test-branch
    cd "dolt-repo-clones"
    run dolt clone -b test-branch http://localhost:50051/test-org/test-repo
    [ "$status" -eq 0 ]
    [[ "$output" =~ "cloning http://localhost:50051/test-org/test-repo" ]] || false
    cd test-repo
    run dolt branch
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "main" ]] || false
    [[ "$output" =~ "test-branch" ]] || false
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test commit" ]] || false
}

@test "remotes: call a clone's remote something other than origin" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
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
    dolt push test-remote main
    cd "dolt-repo-clones"
    run dolt clone --remote test-remote http://localhost:50051/test-org/test-repo
    [ "$status" -eq 0 ]
    [[ "$output" =~ "cloning http://localhost:50051/test-org/test-repo" ]] || false
    cd test-repo
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test commit" ]] || false
    run dolt remote -v
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test-remote" ]] || false
    [[ ! "$output" =~ "origin" ]] || false
}

@test "remotes: dolt fetch" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    dolt push test-remote main
    run dolt fetch test-remote
    [ "$status" -eq 0 ]
    [ "$output" != "" ]  # spinner output
    run dolt fetch test-remote refs/heads/main:refs/remotes/test-remote/main
    [ "$status" -eq 0 ]
    [ "$output" != "" ]  # spinner output
    run dolt fetch poop refs/heads/main:refs/remotes/poop/main
    [ "$status" -eq 1 ]
    [[ "$output" =~ "unknown remote" ]] || false
    run dolt fetch test-remote refs/heads/main:refs/remotes/test-remote/poop
    [ "$status" -eq 0 ]
    [ "$output" != "" ]  # spinner output
    run dolt branch -v -a
    [ "$status" -eq 0 ]
    [[ "$output" =~ "remotes/test-remote/poop" ]] || false
}

@test "remotes: fetch output" {
    # create main remote branch
    dolt remote add origin http://localhost:50051/test-org/test-repo
    dolt sql -q 'create table test (id int primary key);'
    dolt add .
    dolt commit -m 'create test table.'
    dolt push origin main:main

    # create remote branch "branch1"
    dolt checkout -b branch1
    dolt sql -q 'insert into test (id) values (1), (2), (3);'
    dolt add .
    dolt commit -m 'add some values to branch 1.'
    dolt push --set-upstream origin branch1

    # create remote branch "branch2"
    dolt checkout -b branch2
    dolt sql -q 'insert into test (id) values (4), (5), (6);'
    dolt add .
    dolt commit -m 'add some values to branch 2.'
    dolt push --set-upstream origin branch2

    # create first clone
    cd dolt-repo-clones
    dolt clone http://localhost:50051/test-org/test-repo
    cd test-repo
    dolt status
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false

    cd ../..

    # create second clone
    cd "dolt-repo-clones"
    dolt clone http://localhost:50051/test-org/test-repo test-repo2
    cd test-repo2
    dolt status
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false

    # CHANGE 1: add more data to branch1
    dolt checkout -b branch1 remotes/origin/branch1
    dolt sql -q 'insert into test (id) values (100), (101), (102);'
    dolt add .
    dolt commit -m 'add more values to branch 1.'
    dolt push --set-upstream origin branch1

    # CHANGE 2: add more data to branch2
    dolt checkout -b branch2 remotes/origin/branch2
    dolt sql -q 'insert into test (id) values (103), (104), (105);'
    dolt add .
    dolt commit -m 'add more values to branch 2.'
    dolt push --set-upstream origin branch2

    # CHANGE 3: create remote branch "branch3"
    dolt checkout -b branch3
    dolt sql -q 'insert into test (id) values (7), (8), (9);'
    dolt add .
    dolt commit -m 'add some values to branch 3.'
    dolt push --set-upstream origin branch3

    # CHANGE 4: create remote branch "branch4"
    dolt checkout -b branch4
    dolt sql -q 'insert into test (id) values (10), (11), (12);'
    dolt add .
    dolt commit -m 'add some values to  branch 4.'
    dolt push --set-upstream origin branch4

    cd ..
    cd test-repo
    run dolt fetch
    [ "$status" -eq 0 ]
    # The number of $lines and $output printed is non-deterministic
    # due to EphemeralPrinter. We can't test for their length here.
    [ "$output" != "" ]
}

@test "remotes: dolt fetch with docs" {
    # Initial commit of docs on remote
    echo "initial-license" > LICENSE.md
    dolt docs upload LICENSE.md LICENSE.md
    echo "initial-readme" > README.md
    dolt docs upload README.md README.md
    dolt add .
    dolt commit -m "initial doc commit"
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    dolt push test-remote main
    run dolt fetch test-remote
    [ "$status" -eq 0 ]
    [ "$output" != "" ]  # spinner output
    run cat README.md
    [ "$status" -eq 0 ]
    [[ "$output" =~ "initial-readme" ]] || false
    run cat LICENSE.md
    [ "$status" -eq 0 ]
    [[ "$output" =~ "initial-license" ]] || false

    # Clone the initial docs/repo into dolt-repo-clones/test-repo
    cd "dolt-repo-clones"
    run dolt clone http://localhost:50051/test-org/test-repo
    cd test-repo
    dolt docs print LICENSE.md > LICENSE.md
    dolt docs print README.md > README.md
    run cat LICENSE.md
    [ "$status" -eq 0 ]
    [[ "$output" =~ "initial-license" ]] || false
    run cat README.md
    [ "$status" -eq 0 ]
    [[ "$output" =~ "initial-readme" ]] || false
    # Change the docs
    echo "dolt-repo-clones-license" > LICENSE.md
    dolt docs upload LICENSE.md LICENSE.md
    echo "dolt-repo-clones-readme" > README.md
    dolt docs upload README.md README.md
    dolt add .
    dolt commit -m "dolt-repo-clones updated docs"

    # Go back to original repo, and change the docs again
    cd ../../
    echo "initial-license-updated" > LICENSE.md
    dolt docs upload LICENSE.md LICENSE.md
    echo "initial-readme-updated" > README.md
    dolt docs upload README.md README.md
    dolt add .
    dolt commit -m "update initial doc values in test-org/test-repo"

    # Go back to dolt-repo-clones/test-repo and fetch the test-remote
    cd dolt-repo-clones/test-repo
    run dolt fetch test-remote
    run cat LICENSE.md
    [ "$status" -eq 0 ]
    [[ "$output" =~ "dolt-repo-clones-license" ]] || false
    run cat README.md
    [ "$status" -eq 0 ]
    [[ "$output" =~ "dolt-repo-clones-readme" ]] || false
}

@test "remotes: dolt merge with origin/main syntax." {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    dolt push test-remote main
    dolt fetch test-remote
    cd "dolt-repo-clones"
    dolt clone http://localhost:50051/test-org/test-repo
    cd ..
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
    dolt push test-remote main
    cd "dolt-repo-clones/test-repo"
    run dolt log
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "test commit" ]] || false
    run dolt merge origin/main
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Everything up-to-date" ]] || false
    run dolt fetch
    [ "$status" -eq 0 ]
    run dolt merge origin/main
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Fast-forward" ]] || false
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test commit" ]] || false
}

@test "remotes: dolt fetch and merge with remotes/origin/main syntax" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    dolt push test-remote main
    cd "dolt-repo-clones"
    dolt clone http://localhost:50051/test-org/test-repo
    cd ..
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
    dolt push test-remote main
    cd "dolt-repo-clones/test-repo"
    run dolt merge remotes/origin/main
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Everything up-to-date" ]] || false
    run dolt fetch origin main
    [ "$status" -eq 0 ]
    run dolt merge remotes/origin/main
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Fast-forward" ]] || false
}

@test "remotes: generate a merge with no conflict with a remote branch" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    dolt push test-remote main
    cd "dolt-repo-clones"
    dolt clone http://localhost:50051/test-org/test-repo
    cd ..
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
    dolt push test-remote main
    cd "dolt-repo-clones/test-repo"
    dolt sql <<SQL
CREATE TABLE test2 (
  pk BIGINT NOT NULL COMMENT 'tag:10',
  c1 BIGINT COMMENT 'tag:11',
  c2 BIGINT COMMENT 'tag:12',
  c3 BIGINT COMMENT 'tag:13',
  c4 BIGINT COMMENT 'tag:14',
  c5 BIGINT COMMENT 'tag:15',
  PRIMARY KEY (pk)
);
SQL
    dolt add test2
    dolt commit -m "another test commit"
    run dolt pull origin --no-edit
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Updating" ]] || false

    run dolt log --oneline -n 1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Merge branch 'main' of" ]] || false
    [[ ! "$output" =~ "test commit" ]] || false
    [[ ! "$output" =~ "another test commit" ]] || false
}

@test "remotes: merge with divergent head" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    dolt push test-remote main
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
    dolt push test-remote main

    cd "dolt-repo-clones"
    dolt clone http://localhost:50051/test-org/test-repo
    cd ..

    dolt commit --amend -m 'new message'
    dolt push -f test-remote main
    
    cd "dolt-repo-clones/test-repo"

    run dolt pull origin
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "Fast-forward" ]] || false

    run dolt log --oneline -n 1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Merge branch 'main' of" ]] || false
    [[ ! "$output" =~ "new message" ]] || false
}

@test "remotes: generate a merge with a conflict with a remote branch" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
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
    dolt commit -m "created table"
    dolt push --set-upstream test-remote main
    cd "dolt-repo-clones"
    dolt clone http://localhost:50051/test-org/test-repo
    cd ..
    dolt sql -q "insert into test values (0, 0, 0, 0, 0, 0)"
    dolt add test
    dolt commit -m "row to generate conflict"
    dolt push test-remote main
    cd "dolt-repo-clones/test-repo"
    dolt sql -q "insert into test values (0, 1, 1, 1, 1, 1)"
    dolt add test
    dolt commit -m "conflicting row"
    run dolt pull origin
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONFLICT" ]] || false
    dolt conflicts resolve test --ours
    dolt add test
    dolt commit -m "Fixed conflicts"
    run dolt push origin main
    cd ../../
    dolt pull test-remote
    run dolt log
    [[ "$output" =~ "Fixed conflicts" ]] || false
}

@test "remotes: clone sets your current branch appropriately" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
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
    dolt checkout -b aaa
    dolt checkout -b zzz
    dolt push test-remote aaa
    dolt push test-remote zzz
    cd "dolt-repo-clones"
    dolt clone http://localhost:50051/test-org/test-repo
    cd test-repo
    dolt branch

    # main hasn't been pushed so expect aaa to be the current branch and the string main should not be present
    # branches should be sorted lexicographically
    run dolt branch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "* aaa" ]] || false
    [[ ! "$output" =~ "main" ]] || false
    cd ../..
    dolt push test-remote main
    cd "dolt-repo-clones"
    dolt clone http://localhost:50051/test-org/test-repo test-repo2
    cd test-repo2

    # main pushed so it should be the current branch.
    run dolt branch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "* main" ]] || false
}

@test "remotes: force fetch from main" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    dolt push --set-upstream test-remote main

    cd "dolt-repo-clones"
    dolt clone http://localhost:50051/test-org/test-repo
    cd ..

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
    dolt push test-remote main
    
    cd "dolt-repo-clones/test-repo"
    dolt sql <<SQL
CREATE TABLE other (
  pk BIGINT NOT NULL COMMENT 'tag:10',
  c1 BIGINT COMMENT 'tag:11',
  c2 BIGINT COMMENT 'tag:12',
  c3 BIGINT COMMENT 'tag:13',
  c4 BIGINT COMMENT 'tag:14',
  c5 BIGINT COMMENT 'tag:15',
  PRIMARY KEY (pk)
);
SQL
    dolt add other
    dolt commit -m "added other table"
    dolt fetch
    dolt push -f origin main
    cd ../../
    
    run dolt pull --no-edit
    [ "$status" -eq 0 ]    
}

create_main_remote_branch() {
    dolt remote add origin http://localhost:50051/test-org/test-repo
    dolt sql -q 'create table test (id int primary key);'
    dolt add .
    dolt commit -m 'create test table.'
    dolt push origin main:main
}

create_two_more_remote_branches() {
    dolt commit --allow-empty -m 'another commit.'
    dolt push origin main:branch-one
    dolt sql -q 'insert into test (id) values (1), (2), (3);'
    dolt add .
    dolt commit -m 'add some values.'
    dolt push origin main:branch-two
}

create_three_remote_branches() {
    create_main_remote_branch
    create_two_more_remote_branches
}

create_remote_branch() {
    local b="$1"
    dolt push origin main:"$b"
}

create_five_remote_branches_main_only() {
  dolt remote add origin http://localhost:50051/test-org/test-repo
  create_remote_branch "zzz"
  create_remote_branch "dev"
  create_remote_branch "prod"
  create_remote_branch "main"
  create_remote_branch "aaa"
}

create_five_remote_branches_master_only() {
  dolt remote add origin http://localhost:50051/test-org/test-repo
  create_remote_branch "111"
  create_remote_branch "dev"
  create_remote_branch "master"
  create_remote_branch "prod"
  create_remote_branch "aaa"
}

create_five_remote_branches_no_main_no_master() {
  dolt remote add origin http://localhost:50051/test-org/test-repo
  create_remote_branch "123"
  create_remote_branch "dev"
  create_remote_branch "456"
  create_remote_branch "prod"
  create_remote_branch "aaa"
}

create_five_remote_branches_main_and_master() {
  dolt remote add origin http://localhost:50051/test-org/test-repo
  create_remote_branch "master"
  create_remote_branch "dev"
  create_remote_branch "456"
  create_remote_branch "main"
  create_remote_branch "aaa"
}

@test "remotes: clone checks out main, if found" {
    create_five_remote_branches_main_only
    cd dolt-repo-clones
    dolt clone http://localhost:50051/test-org/test-repo
    cd test-repo
    dolt status
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false
}

@test "remotes: clone checks out master, if main not found" {
    create_five_remote_branches_master_only
    cd dolt-repo-clones
    dolt clone http://localhost:50051/test-org/test-repo
    cd test-repo
    dolt status
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch master" ]] || false
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false
}

@test "remotes: clone checks out main, if both main and master found" {
    create_five_remote_branches_main_and_master
    cd dolt-repo-clones
    dolt clone http://localhost:50051/test-org/test-repo
    cd test-repo
    dolt status
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false
}

@test "remotes: clone checks out first lexicographical branch if neither main nor master found" {
    create_five_remote_branches_no_main_no_master
    cd dolt-repo-clones
    dolt clone http://localhost:50051/test-org/test-repo
    cd test-repo
    dolt status
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch 123" ]] || false
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false
}

@test "remotes: clone creates remotes refs for all remote branches" {
    create_three_remote_branches
    cd dolt-repo-clones
    dolt clone http://localhost:50051/test-org/test-repo
    cd test-repo
    run dolt branch -a
    [ "$status" -eq 0 ]
    [[ "$output" =~ "* main" ]] || false
    [[ ! "$output" =~ " branch-one" ]] || false
    [[ ! "$output" =~ " branch-two" ]] || false
    [[ "$output" =~ "remotes/origin/main" ]] || false
    [[ "$output" =~ "remotes/origin/branch-one" ]] || false
    [[ "$output" =~ "remotes/origin/branch-two" ]] || false
}

@test "remotes: clone --single-branch does not create remote refs for all remote branches" {
    create_three_remote_branches
    cd dolt-repo-clones
    dolt clone --single-branch http://localhost:50051/test-org/test-repo
    cd test-repo
    run dolt branch -a
    [ "$status" -eq 0 ]
    [[ "$output" =~ "* main" ]] || false
    [[ ! "$output" =~ " branch-one" ]] || false
    [[ ! "$output" =~ " branch-two" ]] || false
    [[ "$output" =~ "remotes/origin/main" ]] || false
    [[ ! "$output" =~ "remotes/origin/branch-one" ]] || false
    [[ ! "$output" =~ "remotes/origin/branch-two" ]] || false
}

@test "remotes: clone --branch specifies which branch to clone" {
    create_three_remote_branches
    cd dolt-repo-clones
    dolt clone --branch branch-one http://localhost:50051/test-org/test-repo
    cd test-repo
    run dolt branch -a
    [ "$status" -eq 0 ]
    [[ "$output" =~ "* branch-one" ]] || false
    [[ ! "$output" =~ " main" ]] || false
    [[ ! "$output" =~ " branch-two" ]] || false
    [[ "$output" =~ "remotes/origin/main" ]] || false
    [[ "$output" =~ "remotes/origin/branch-one" ]] || false
    [[ "$output" =~ "remotes/origin/branch-two" ]] || false
}

@test "remotes: clone --single-branch --branch does not create all remote refs" {
    create_three_remote_branches
    cd dolt-repo-clones
    dolt clone --branch branch-one --single-branch http://localhost:50051/test-org/test-repo
    cd test-repo
    run dolt branch -a
    [ "$status" -eq 0 ]
    [[ "$output" =~ "* branch-one" ]] || false
    [[ ! "$output" =~ " main" ]] || false
    [[ ! "$output" =~ " branch-two" ]] || false
    [[ ! "$output" =~ "remotes/origin/main" ]] || false
    [[ "$output" =~ "remotes/origin/branch-one" ]] || false
    [[ ! "$output" =~ "remotes/origin/branch-two" ]] || false
}

@test "remotes: fetch creates new remote refs for new remote branches" {
    create_main_remote_branch

    cd dolt-repo-clones
    dolt clone http://localhost:50051/test-org/test-repo
    cd test-repo
    dolt branch -a
    run dolt branch -a
    [ "$status" -eq 0 ]
    [[ "$output" =~ "* main" ]] || false
    [[ "$output" =~ "remotes/origin/main" ]] || false

    cd ../../
    create_two_more_remote_branches

    cd dolt-repo-clones/test-repo
    dolt fetch
    run dolt branch -a
    [ "$status" -eq 0 ]
    [[ "$output" =~ "* main" ]] || false
    [[ "$output" =~ "remotes/origin/main" ]] || false
    [[ "$output" =~ "remotes/origin/branch-one" ]] || false
    [[ "$output" =~ "remotes/origin/branch-two" ]] || false
}

setup_ref_test() {
    create_main_remote_branch

    cd dolt-repo-clones
    dolt clone http://localhost:50051/test-org/test-repo
    cd test-repo
}

@test "remotes: can use refs/remotes/origin/... as commit reference for log" {
    setup_ref_test
    dolt log refs/remotes/origin/main
}

@test "remotes: can use refs/remotes/origin/... as commit reference for diff" {
    setup_ref_test
    dolt diff HEAD refs/remotes/origin/main
    dolt diff refs/remotes/origin/main HEAD
}

@test "remotes: can use refs/remotes/origin/... as commit reference for merge" {
    setup_ref_test
    dolt merge refs/remotes/origin/main -m "merge"
}

@test "remotes: can use remotes/origin/... as commit reference for log" {
    setup_ref_test
    dolt log remotes/origin/main
}

@test "remotes: can use remotes/origin/... as commit reference for diff" {
    setup_ref_test
    dolt diff HEAD remotes/origin/main
    dolt diff remotes/origin/main HEAD
}

@test "remotes: can use remotes/origin/... as commit reference for merge" {
    setup_ref_test
    dolt merge remotes/origin/main -m "merge"
}

@test "remotes: can use origin/... as commit reference for log" {
    setup_ref_test
    dolt log origin/main
}

@test "remotes: can use origin/... as commit reference for diff" {
    setup_ref_test
    dolt diff HEAD origin/main
    dolt diff origin/main HEAD
}

@test "remotes: can use origin/... as commit reference for merge" {
    setup_ref_test
    dolt merge origin/main -m "merge"
}

@test "remotes: can delete remote reference branch as origin/..." {
    setup_ref_test
    cd ../../
    create_two_more_remote_branches
    cd dolt-repo-clones/test-repo
    dolt fetch # TODO: Remove this fetch once clone works

    dolt branch -r -d origin/main
    run dolt branch -a
    [ "$status" -eq 0 ]
    [[ "$output" =~ "* main" ]] || false
    [[ ! "$output" =~ "remotes/origin/main" ]] || false
    [[ "$output" =~ "remotes/origin/branch-one" ]] || false
    [[ "$output" =~ "remotes/origin/branch-two" ]] || false
    dolt branch -r -d origin/branch-one origin/branch-two
    run dolt branch -a
    [ "$status" -eq 0 ]
    [[ "$output" =~ "* main" ]] || false
    [[ ! "$output" =~ "remotes/origin/main" ]] || false
    [[ ! "$output" =~ "remotes/origin/branch-one" ]] || false
    [[ ! "$output" =~ "remotes/origin/branch-two" ]] || false
}

@test "remotes: can list remote reference branches with -r" {
    setup_ref_test
    cd ../../
    create_two_more_remote_branches
    cd dolt-repo-clones/test-repo
    dolt fetch # TODO: Remove this fetch once clone works

    run dolt branch -r
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "* main" ]] || false
    [[ "$output" =~ "remotes/origin/main" ]] || false
    [[ "$output" =~ "remotes/origin/branch-one" ]] || false
    [[ "$output" =~ "remotes/origin/branch-two" ]] || false
    run dolt branch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "* main" ]] || false
    [[ ! "$output" =~ "remotes/origin/main" ]] || false
    [[ ! "$output" =~ "remotes/origin/branch-one" ]] || false
    [[ ! "$output" =~ "remotes/origin/branch-two" ]] || false
    run dolt branch -a
    [ "$status" -eq 0 ]
    [[ "$output" =~ "* main" ]] || false
    [[ "$output" =~ "remotes/origin/main" ]] || false
    [[ "$output" =~ "remotes/origin/branch-one" ]] || false
    [[ "$output" =~ "remotes/origin/branch-two" ]] || false
}

@test "remotes: no remote is set" {
    run dolt push
    [ "$status" -eq 1 ]
    [[ "$output" =~ "fatal: No configured push destination." ]] || false
}

@test "remotes: existing parent directory is not wiped when clone fails" {
    # Create the new testdir and save it
    mkdir testdir && cd testdir
    run pwd
    testdir=$output

    # Create a clone operation that purposely fails on a valid remote
    mkdir clone_root
    mkdir dest && cd dest

    run dolt clone "file://../clone_root" .
    [ "$status" -eq 1 ]
    [[ "$output" =~ "clone failed" ]] || false

    # Validates that the directory exists
    run ls $testdir
    [ "$status" -eq 0 ]
    [[ "$output" =~ "clone_root" ]] || false
    [[ "$output" =~ "dest" ]] || false

    # Check that .dolt was deleted
    run ls -a $testdir/dest
    ! [[ "$output" =~ ".dolt" ]] || false

    # try again and now make sure that /dest/.dolt is correctly deleted instead of dest/
    cd ..
    run dolt clone "file://./clone_root" dest/
    [ "$status" -eq 1 ]
    [[ "$output" =~ "clone failed" ]] || false

    run ls $testdir
    [ "$status" -eq 0 ]
    [[ "$output" =~ "clone_root" ]] || false
    [[ "$output" =~ "dest" ]] || false

    run ls -a $testdir/dest
    ! [[ "$output" =~ ".dolt" ]] || false
}

@test "remotes: fetching unknown remotes should error" {
    setup_ref_test
    cd ../../
    cd dolt-repo-clones/test-repo
    run dolt fetch remotes/dasdas
    [ "$status" -eq 1 ]
    [[ ! "$output" =~ "panic" ]] || false
    [[ "$output" =~ "'remotes/dasdas' does not appear to be a dolt database" ]] || false
}

@test "remotes: fetching added invalid remote correctly errors" {
    setup_ref_test
    cd ../../
    cd dolt-repo-clones/test-repo
    dolt remote add myremote dolthub/fake

    run dolt fetch myremote
    [ "$status" -eq 1 ]
    [[ ! "$output" =~ "panic" ]] || false
    [[ "$output" =~ "permission denied" ]] || false
}

@test "remotes: fetching unknown remote ref errors accordingly" {
   setup_ref_test
   cd ../../
   cd dolt-repo-clones/test-repo

   run dolt fetch origin dadasdfasdfa
   [ "$status" -eq 1 ]
   [[ "$output" =~ "invalid ref spec: 'dadasdfasdfa'" ]] || false
}

@test "remotes: checkout with -f flag without conflict" {
    # create main remote branch
    dolt remote add origin http://localhost:50051/test-org/test-repo
    dolt sql -q 'create table test (id int primary key);'
    dolt sql -q 'insert into test (id) values (8);'
    dolt add .
    dolt commit -m 'create test table.'
    dolt push origin main:main

    # create remote branch "branch1"
    dolt checkout -b branch1
    dolt sql -q 'insert into test (id) values (1), (2), (3);'
    dolt add .
    dolt commit -m 'add some values to branch 1.'
    dolt push --set-upstream origin branch1

    run dolt checkout -f main
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Switched to branch 'main'" ]] || false

    run dolt sql -q "select * from test;"
    [[ "$output" =~ "8" ]] || false
    [[ ! "$output" =~ "1" ]] || false
    [[ ! "$output" =~ "2" ]] || false
    [[ ! "$output" =~ "3" ]] || false

    dolt checkout branch1
    run dolt sql -q "select * from test;"
    [[ "$output" =~ "1" ]] || false
    [[ "$output" =~ "2" ]] || false
    [[ "$output" =~ "3" ]] || false
    [[ "$output" =~ "8" ]] || false
}

@test "remotes: checkout with -f flag with conflict" {
    # create main remote branch
    dolt remote add origin http://localhost:50051/test-org/test-repo
    dolt sql -q 'create table test (id int primary key);'
    dolt sql -q 'insert into test (id) values (8);'
    dolt add .
    dolt commit -m 'create test table.'
    dolt push origin main:main

    # create remote branch "branch1"
    dolt checkout -b branch1
    dolt sql -q 'insert into test (id) values (1), (2), (3);'
    dolt add .
    dolt commit -m 'add some values to branch 1.'
    dolt push --set-upstream origin branch1

    dolt sql -q 'insert into test (id) values (4);'
    run dolt checkout main
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Please commit your changes or stash them before you switch branches." ]] || false

    run dolt checkout -f main
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Switched to branch 'main'" ]] || false

    run dolt sql -q "select * from test;"
    [[ "$output" =~ "8" ]] || false
    [[ ! "$output" =~ "4" ]] || false

    dolt checkout branch1
    run dolt sql -q "select * from test;"
    [[ "$output" =~ "1" ]] || false
    [[ "$output" =~ "2" ]] || false
    [[ "$output" =~ "3" ]] || false
    [[ "$output" =~ "8" ]] || false
    [[ ! "$output" =~ "4" ]] || false
}

@test "remotes: clone sets default upstream for main" {
    dolt remote add origin http://localhost:50051/test-org/test-repo
    dolt push origin main
    cd dolt-repo-clones
    dolt clone http://localhost:50051/test-org/test-repo
    cd test-repo
    dolt push
}

@test "remotes: local clone does not contain working set changes" {
    mkdir repo1
    mkdir rem1
    cd repo1
    dolt init
    dolt sql -q "create table t (i int)"
    run dolt status
    [[ "$output" =~ "new table:" ]] || false
    dolt commit -Am "new table"
    dolt sql -q "create table t2 (i int)"
    dolt remote add rem1 file://../rem1
    dolt push rem1 main
    cd ..

    dolt clone file://./rem1/ repo2
    cd repo2

    run dolt status
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false
}

@test "remotes: dolt_remote uses the right db directory in a multidb env" {
    tempDir=$(mktemp -d)

    cd $tempDir
    mkdir db1
    cd db1
    dolt init
    cd ..

    run dolt sql -q "use db1; call dolt_remote('add', 'test1', 'foo/bar');"
    [ "$status" -eq 0 ]

    run grep "test1" db1/.dolt/repo_state.json
    [ "$status" -eq 0 ]
    [[ "$output" =~ '"name": "test1"' ]] || false
    [ ! -d ".dolt" ]

    run dolt sql -q "use db1; call dolt_remote('remove', 'test1');"
    [ "$status" -eq 0 ]
    [ ! -d ".dolt" ]
}

@test "remotes: dolt_remote add and remove works with other commands" {
    mkdir remote
    mkdir repo1

    cd repo1
    dolt init
    run dolt sql <<SQL
CALL dolt_remote('add', 'origin', 'http://localhost:50051/test-org/test-repo');
CALL dolt_push('origin', 'main');
SQL
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "must provide a GRPCDialProvider param through GRPCDialProviderParam" ]] || false

    cd ..
    dolt clone http://localhost:50051/test-org/test-repo repo2

    cd repo2
    run dolt branch -va
    [[ "$output" =~ "main" ]] || false
    [[ ! "$output" =~ "other" ]] || false

    cd ../repo1
    dolt checkout -b other
    dolt push origin other

    cd ../repo2
    dolt pull
    run dolt branch -va
    [[ "$output" =~ "main" ]] || false
    [[ "$output" =~ "other" ]] || false

    dolt checkout main
    dolt sql -q "CREATE TABLE a(pk int primary key)"
    dolt add .
    dolt commit -am "add table a"
    dolt push

    cd ../repo1
    dolt sql -q "CALL dolt_remote('remove', 'origin')"
    run dolt pull
    [ "$status" -eq 1 ]
    [[ "$output" =~ "no remote" ]] || false
}

@test "remotes: dolt status on local repo compares with remote tracking" {
    mkdir remote
    mkdir repo1

    cd repo1
    dolt init
    dolt remote add origin file://../remote
    dolt push --set-upstream origin main

    cd ..
    dolt clone file://./remote repo2

    cd repo2
    dolt sql -q "CREATE TABLE test (id int primary key)"
    dolt add .
    dolt commit -am "create table"
    run dolt push
    [ "$status" -eq "0" ]
    dolt log

    cd ../repo1
    run dolt status
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false

    dolt fetch
    run dolt status
    [[ "$output" =~ "behind" ]] || false
    [[ "$output" =~ "1 commit" ]] || false

    dolt sql -q "CREATE TABLE different (id int primary key)"
    dolt add .
    dolt commit -am "create different table"

    run dolt status
    [[ "$output" =~ "diverged" ]] || false
    [[ "$output" =~ "1 and 1" ]] || false

    dolt pull --no-edit
    run dolt status
    [[ "$output" =~ "ahead" ]] || false
    [[ "$output" =~ "2 commit" ]] || false
}

@test "remotes: dolt checkout --track origin/feature checks out new local branch 'feature' with upstream set" {
    mkdir remote
    mkdir repo1

    cd repo1
    dolt init
    dolt remote add origin file://../remote
    dolt push --set-upstream origin main
    dolt checkout -b feature
    dolt push --set-upstream origin feature

    cd ..
    dolt clone file://./remote repo2

    cd repo2
    run dolt branch
    [[ ! "$output" =~ "feature" ]] || false

    run dolt checkout --track origin/feature
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Switched to branch 'feature'" ]] || false
    [[ "$output" =~ "branch 'feature' set up to track 'origin/feature'." ]] || false

    run dolt pull
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Everything up-to-date" ]] || false
}

@test "remotes: call dolt_checkout track flag sets upstream" {
    mkdir remote
    mkdir repo1

    cd repo1
    dolt init
    dolt remote add origin file://../remote
    dolt sql -q "CREATE TABLE a (pk int)"
    dolt commit -Am "add table a"
    dolt push --set-upstream origin main
    dolt checkout -b other
    dolt push --set-upstream origin other

    cd ..
    dolt clone file://./remote repo2

    cd repo2
    dolt branch
    [[ ! "$output" =~ "other" ]] || false

    run dolt sql << SQL
    call dolt_checkout('--track', 'origin/other');
    select active_branch();
SQL
    [ "$status" -eq 0 ]
    [[ "$output" =~ "other" ]] || false

    run dolt checkout other
    [ "$status" -eq 0 ]

    run dolt status
    [[ "$output" =~ "Your branch is up to date with 'origin/other'." ]] || false

    run dolt pull
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Everything up-to-date" ]] || false
}

@test "remotes: call dolt_checkout with --track and no arg returns error" {
    run dolt sql -q "call dolt_checkout('--track')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "no value for option" ]] || false
}

@test "remotes: call dolt_checkout with local branch name" {
    run dolt sql -q "call dolt_checkout('--track', 'newbranch')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "invalid ref spec" ]] || false
}

@test "remotes: dolt checkout -b newbranch --track origin/feature checks out new local branch 'newbranch' with upstream set" {
    mkdir remote
    mkdir repo1

    cd repo1
    dolt init
    dolt remote add origin file://../remote
    dolt push --set-upstream origin main
    dolt checkout -b feature
    dolt push --set-upstream origin feature

    cd ..
    dolt clone file://./remote repo2

    cd repo2
    run dolt branch
    [[ ! "$output" =~ "feature" ]] || false

    run dolt checkout -b newbranch --track origin/feature
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Switched to branch 'newbranch'" ]] || false
    [[ "$output" =~ "branch 'newbranch' set up to track 'origin/feature'." ]] || false

    run dolt pull
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Everything up-to-date" ]] || false

    dolt checkout main
    dolt branch -D newbranch

    # branch.autosetupmerge configuration defaults to --track, so the upstream is set
    run dolt checkout -b newbranch origin/feature
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Switched to branch 'newbranch'" ]] || false
    [[ "$output" =~ "branch 'newbranch' set up to track 'origin/feature'." ]] || false

    run dolt pull
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Everything up-to-date" ]] || false
}

@test "remotes: call dolt_checkout('-b', 'newbranch', '--track', 'origin/feature') checks out new local branch 'newbranch' with upstream set" {
    mkdir remote
    mkdir repo1

    cd repo1
    dolt init
    dolt remote add origin file://../remote
    dolt push --set-upstream origin main
    dolt checkout -b feature
    dolt push --set-upstream origin feature

    cd ..
    dolt clone file://./remote repo2

    cd repo2
    run dolt branch
    [[ ! "$output" =~ "feature" ]] || false

    dolt sql -q 'call dolt_checkout("-b", "newbranch", "--track", "origin/feature");'

    run dolt sql -q 'select * from dolt_branches;'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| origin | feature" ]] || false

    run dolt pull
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Everything up-to-date" ]] || false

    dolt checkout main
    dolt branch -D newbranch

    # branch.autosetupmerge configuration defaults to --track, so the upstream is set
    run dolt sql -q 'call dolt_checkout("-b", "newbranch2", "origin/feature");'
    [ "$status" -eq 0 ]
    run dolt sql -q 'select * from dolt_branches;'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| origin | feature" ]] || false


    run dolt pull
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Everything up-to-date" ]] || false
}

@test "remotes: dolt_clone failure cleanup" {
    repoDir="$BATS_TMPDIR/dolt-repo-$$"

    # try to clone a remote that doesn't exist
    cd $repoDir
    run dolt sql -q 'call dolt_clone("file:///tmp/sanity/remote");'
    [ "$status" -eq 1 ]

    # Make sure there's nothing remaining from the failed clone
    [ ! -d "$repoDir/remote" ]
}

@test "remotes: dolt_clone procedure" {
    repoDir="$BATS_TMPDIR/dolt-repo-$$"

    # make directories outside of the dolt repo
    tempDir=$(mktemp -d)
    cd $tempDir
    mkdir remote
    mkdir repo1

    cd repo1
    dolt init
    dolt remote add origin file://../remote
    dolt push origin main
    dolt checkout -b other
    dolt push --set-upstream origin other

    cd $repoDir

    run dolt sql -q "call dolt_clone()"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "error: invalid number of arguments" ]] || false

    run dolt sql -q "call dolt_clone('file://$tempDir/remote', 'foo', 'bar')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "error: clone has too many positional arguments." ]] || false

    # Clone a local database and check for all the branches
    run dolt sql -q "call dolt_clone('file://$tempDir/remote');"
    [ "$status" -eq 0 ]
    cd remote
    run dolt branch
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "other" ]] || false
    [[ "$output" =~ "main" ]] || false
    run dolt branch --remote
    [[ "$output" =~ "origin/other" ]] || false
    [[ "$output" =~ "origin/main" ]] || false
    cd ..

    # Ensure we can't clone it again
    run dolt sql -q "call dolt_clone('file://$tempDir/remote');"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "can't create database remote; database exists" ]] || false
    run dolt sql -q "show databases"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "remote" ]] || false

    # Drop the new database and re-clone it with a different name
    dolt sql -q "drop database remote"
    run dolt sql -q "show databases"
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "repo2" ]] || false
    dolt sql -q "call dolt_clone('file://$tempDir/remote', 'repo2');"

    # Sanity check that we can use the new database
    dolt sql << SQL
use repo2;
create table new_table(a int primary key);
insert into new_table values (1), (2);
SQL
    cd repo2
    dolt add .
    dolt commit -am "a commit for main from repo2"
    dolt push origin main
    cd ..

    # Test -remote option to customize the origin remote name for the cloned DB
    run dolt sql -q "call dolt_clone('-remote', 'custom', 'file://$tempDir/remote', 'custom_remote');"
    [ "$status" -eq 0 ]
    run dolt sql -q "use custom_remote; select name from dolt_remotes;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "custom" ]] || false

    # Test -branch option to only clone a single branch
    run dolt sql -q "call dolt_clone('-branch', 'other', 'file://$tempDir/remote', 'single_branch');"
    [ "$status" -eq 0 ]
    run dolt sql -q "use single_branch; select name from dolt_branches;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "other" ]] || false
    [[ ! "$output" =~ "main" ]] || false
    run dolt sql -q "use single_branch; select active_branch();"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "other" ]] || false
    # TODO: To match Git's semantics, clone for a single branch should NOT create any other
    #       remote tracking branches (https://github.com/dolthub/dolt/issues/3873)
    # run dolt checkout main
    # [ "$status" -eq 1 ]

    # Set up a test repo in the remote server
    cd repo2
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    dolt sql -q "CREATE TABLE test_table (pk INT)"
    dolt add .
    dolt commit -am "main commit"
    dolt push test-remote main
    cd ..

    # Test cloning from a server remote
    run dolt sql -q "call dolt_clone('http://localhost:50051/test-org/test-repo');"
    [ "$status" -eq 0 ]
    run dolt sql -q "use \`test-repo\`; show tables;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test_table" ]] || false
}

@test "remotes: fetch --prune deletes remote refs not on remote" {
    mkdir remote
    mkdir repo1

    cd repo1
    dolt init
    dolt remote add origin file://../remote1
    dolt remote add remote2 file://../remote2
    dolt branch b1
    dolt branch b2
    dolt push origin main
    dolt push remote2 main
    dolt push origin b1
    dolt push remote2 b2

    cd ..
    dolt clone file://./remote1 repo2

    cd repo2
    run dolt branch -va
    [[ "$output" =~ "main" ]] || false

    dolt remote add remote2 file://../remote2
    dolt fetch
    dolt fetch remote2

    run dolt branch -r
    [ "$status" -eq 0 ]
    [[ "$output" =~ "origin/b1" ]] || false
    [[ "$output" =~ "remote2/b2" ]] || false

    # delete the branches on the remote
    cd ../repo1
    dolt push origin :b1
    dolt push remote2 :b2

    cd ../repo2
    dolt fetch --prune

    # prune should have deleted the origin/b1 branch, but not the one on the other remote
    run dolt branch -r
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "origin/b1" ]] || false
    [[ "$output" =~ "remote2/b2" ]] || false

    # now the other remote
    dolt fetch --prune remote2
    run dolt branch -r
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "origin/b1" ]] || false
    [[ ! "$output" =~ "remote2/b2" ]] || false

    run dolt fetch --prune remote2 'refs/heads/main:refs/remotes/remote2/othermain'
    [ "$status" -ne 0 ]
    [[ "$output" =~ "--prune option cannot be provided with a ref spec" ]] || false
}

@test "remotes: push ignores local tracking branch" {
    # https://github.com/dolthub/dolt/issues/7448
    mkdir remote
    mkdir cloneA

    cd cloneA
    dolt init
    dolt remote add origin file://../remote
    dolt push origin main:main
    dolt checkout -b alt
    dolt commit -m new --allow-empty
    dolt push origin alt:alt

    cd ..
    dolt clone file://remote cloneB
    cd cloneB
    dolt merge origin/alt
    dolt push origin main:main
    dolt push origin :alt

    cd ../cloneA
    dolt checkout main
    dolt checkout -B alt
    dolt commit -m another --allow-empty

    run dolt push origin alt:alt
    [ "$status" -eq 0 ]
    [[ "$output" =~ "new branch" ]] || false
}

@test "remotes: can clone to . when local temp files are being used" {
    mkdir toclone

    mkdir topush
    cd topush
    dolt init
    dolt remote add origin file://../toclone
    dolt push origin main:main
    cd ..

    mkdir dest
    cd dest
    env DOLT_FORCE_LOCAL_TEMP_FILES=1 dolt clone file://../toclone .
}
