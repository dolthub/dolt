#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

remotesrv_pid=
setup() {
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
    rm -rf $BATS_TMPDIR/remotes-$$
}

@test "dolt remotes server is running" {
    ps -p $remotesrv_pid | grep remotesrv
}

@test "add a remote using dolt remote" {
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

@test "remove a remote" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    run dolt remote remove test-remote
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt remote -v
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "test-remote" ]] || false
    run dolt remote remove poop
    [ "$status" -eq 1 ]
    [[ "$output" =~ "unknown remote poop" ]] || false
}

@test "push and pull an unknown remote" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    run dolt push poop master
    [ "$status" -eq 1 ]
    [[ "$output" =~ "unknown remote" ]] || false
    run dolt pull poop
    [ "$status" -eq 1 ]
    [[ "$output" =~ "unknown remote" ]] || false
}

@test "push with only one argument" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    run dolt push test-remote
    [ "$status" -eq 1 ]
    [[ "$output" =~ "fatal: The current branch master has no upstream branch." ]] || false
    [[ "$output" =~ "To push the current branch and set the remote as upstream, use" ]] || false
    [[ "$output" =~ "dolt push --set-upstream test-remote master" ]] || false
}

@test "push and pull master branch from a remote" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    run dolt push test-remote master
    [ "$status" -eq 0 ]
    [ -d "$BATS_TMPDIR/remotes-$$/test-org/test-repo" ]
    run dolt pull test-remote
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Everything up-to-date" ]] || false
}

@test "push and pull non-master branch from remote" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    dolt checkout -b test-branch
    run dolt push test-remote test-branch
    [ "$status" -eq 0 ]
    run dolt pull test-remote
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Everything up-to-date" ]] || false
}

@test "push and pull from non-master branch and use --set-upstream" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    dolt checkout -b test-branch
    run dolt push --set-upstream test-remote test-branch
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "panic:" ]] || false
    dolt sql -q "create table test (pk int, c1 int, primary key(pk))"
    dolt add .
    dolt commit -m "Added test table"
    run dolt push
    [ "$status" -eq 0 ]
}

@test "push and pull with docs from remote" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    echo "license-text" > LICENSE.md
    echo "readme-text" > README.md
    dolt add .
    dolt commit -m "test doc commit"
    dolt push test-remote master
    cd "dolt-repo-clones"
    run dolt clone http://localhost:50051/test-org/test-repo
    [ "$status" -eq 0 ]
    
    cd test-repo
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test doc commit" ]] || false

    cd ../../
    echo "updated-license" > LICENSE.md
    dolt add .
    dolt commit -m "updated license"
    dolt push test-remote master

    cd dolt-repo-clones/test-repo
    echo "this text should remain after pull :p" > README.md
    run dolt pull
    [[ "$output" =~ "Updating" ]] || false
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "updated license" ]] || false
    run cat LICENSE.md
    [ "$status" -eq 0 ]
    [[ "$output" =~ "updated-license" ]] || false
    run cat README.md
    [ "$status" -eq 0 ]
    [[ "$output" =~ "this text should remain after pull :p" ]] || false
}

@test "push and pull tags to/from remote" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    dolt sql <<SQL
CREATE TABLE test (pk int PRIMARY KEY);
INSERT INTO  test VALUES (1),(2),(3);
SQL
    dolt add . && dolt commit -m "added table test"
    dolt push test-remote master
    cd "dolt-repo-clones"
    run dolt clone http://localhost:50051/test-org/test-repo
    [ "$status" -eq 0 ]

    cd ../
    dolt tag v1 head
    dolt push test-remote v1

    cd dolt-repo-clones/test-repo
    run dolt pull
    [[ "$output" =~ "Successfully" ]] || false
    run dolt tag
    [ "$status" -eq 0 ]
    [[ "$output" =~ "v1" ]] || false
}

@test "tags are only pulled if their commit is pulled" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    dolt sql <<SQL
CREATE TABLE test (pk int PRIMARY KEY);
INSERT INTO  test VALUES (1),(2),(3);
SQL
    dolt add . && dolt commit -m "added table test"
    dolt push test-remote master
    cd "dolt-repo-clones"
    run dolt clone http://localhost:50051/test-org/test-repo
    [ "$status" -eq 0 ]

     cd ../
    dolt tag v1 head
    dolt push test-remote v1
    dolt checkout -b other
    dolt sql -q "INSERT INTO test VALUES (8),(9),(10)"
    dolt add . && dolt commit -m "added values on branch other"
    dolt push -u test-remote other
    dolt tag other_tag head
    dolt push test-remote other_tag

    cd dolt-repo-clones/test-repo
    run dolt pull
    [[ "$output" =~ "Successfully" ]] || false
    run dolt tag
    [ "$status" -eq 0 ]
    [[ "$output" =~ "v1" ]] || false
    [[ ! "$output" =~ "other_tag" ]] || false
    dolt fetch
    dolt checkout other
    run dolt pull
    [ "$status" -eq 0 ]
    run dolt tag
    [ "$status" -eq 0 ]
    [[ "$output" =~ "v1" ]] || false
    [[ "$output" =~ "other_tag" ]] || false
}

@test "clone a remote" {
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
    dolt push test-remote master
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

@test "read tables test" {
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
    dolt push test-remote master
    cd "dolt-repo-clones"

    # Create a read latest tables and verify we have all the tables
    dolt read-tables http://localhost:50051/test-org/test-repo master
    cd test-repo
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "t1" ]] || false
    [[ "$output" =~ "t2" ]] || false
    [[ "$output" =~ "t3" ]] || false
    cd ..

    # Read specific table from latest with a specified directory
    dolt read-tables --dir clone_t1_t2 http://localhost:50051/test-org/test-repo master t1 t2
    cd clone_t1_t2
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "t1" ]] || false
    [[ "$output" =~ "t2" ]] || false
    [[ ! "$output" =~ "t3" ]] || false
    cd ..

    # Read tables from parent of parent of the tip of master. Should only have table t1
    dolt read-tables --dir clone_t1 http://localhost:50051/test-org/test-repo master~2
    cd clone_t1
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "t1" ]] || false
    [[ ! "$output" =~ "t2" ]] || false
    [[ ! "$output" =~ "t3" ]] || false
    cd ..
}

@test "clone a remote with docs" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    echo "license-text" > LICENSE.md
    echo "readme-text" > README.md
    dolt add .
    dolt commit -m "test doc commit"
    dolt push test-remote master
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

@test "clone an empty remote" {
    run dolt clone http://localhost:50051/test-org/empty
    [ "$status" -eq 1 ]
    [[ "$output" =~ "error: clone failed" ]] || false
    [[ "$output" =~ "cause: remote at that url contains no Dolt data" ]] || false
}

@test "clone a non-existent remote" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    cd "dolt-repo-clones"
    run dolt clone http://localhost:50051/foo/bar
    [ "$status" -eq 1 ]
    [[ "$output" =~ "error: clone failed" ]] || false
    [[ "$output" =~ "cause: remote at that url contains no Dolt data" ]] || false
}

@test "clone a different branch than master" {
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
    [[ ! "$output" =~ "master" ]] || false
    [[ "$output" =~ "test-branch" ]] || false
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test commit" ]] || false
}

@test "call a clone's remote something other than origin" {
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
    dolt push test-remote master
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

@test "dolt fetch" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    dolt push test-remote master
    run dolt fetch test-remote
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt fetch test-remote refs/heads/master:refs/remotes/test-remote/master
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt fetch poop refs/heads/master:refs/remotes/poop/master
    [ "$status" -eq 1 ]
    [[ "$output" =~ "unknown remote" ]] || false
    run dolt fetch test-remote refs/heads/master:refs/remotes/test-remote/poop
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt branch -v -a
    [ "$status" -eq 0 ]
    [[ "$output" =~ "remotes/test-remote/poop" ]] || false
}

@test "dolt fetch with docs" {
    # Initial commit of docs on remote
    echo "initial-license" > LICENSE.md
    echo "initial-readme" > README.md
    dolt add .
    dolt commit -m "initial doc commit"
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    dolt push test-remote master
    run dolt fetch test-remote
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
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
    run cat LICENSE.md
    [ "$status" -eq 0 ]
    [[ "$output" =~ "initial-license" ]] || false
    run cat README.md
    [ "$status" -eq 0 ]
    [[ "$output" =~ "initial-readme" ]] || false
    # Change the docs 
    echo "dolt-repo-clones-license" > LICENSE.md
    echo "dolt-repo-clones-readme" > README.md
    dolt add .
    dolt commit -m "dolt-repo-clones updated docs"

    # Go back to original repo, and change the docs again
    cd ../../
    echo "initial-license-updated" > LICENSE.md
    echo "initial-readme-updated" > README.md
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

@test "dolt merge with origin/master syntax." {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    dolt push test-remote master
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
    dolt push test-remote master
    cd "dolt-repo-clones/test-repo"
    run dolt log
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "test commit" ]] || false
    run dolt merge origin/master
    [ "$status" -eq 0 ]
    # This needs to say up-to-date like the skipped test above
    # [[ "$output" =~ "up to date" ]]
    run dolt fetch
    [ "$status" -eq 0 ]
    run dolt merge origin/master
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Fast-forward" ]]
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test commit" ]] || false
}

@test "dolt fetch and merge with remotes/origin/master syntax" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    dolt push test-remote master
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
    dolt push test-remote master
    cd "dolt-repo-clones/test-repo"
    run dolt merge remotes/origin/master
    [ "$status" -eq 0 ]
    # This needs to say up-to-date like the skipped test above
    [[ "$output" =~ "Everything up-to-date" ]]
    run dolt fetch origin master
    [ "$status" -eq 0 ]
    run dolt merge remotes/origin/master
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Fast-forward" ]]
}

@test "try to push a remote that is behind tip" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    dolt push test-remote master
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
    dolt push test-remote master
    cd "dolt-repo-clones/test-repo"
    run dolt push origin master
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Everything up-to-date" ]] || false
    dolt fetch
    run dolt push origin master
    [ "$status" -eq 1 ]
    [[ "$output" =~ "tip of your current branch is behind" ]] || false
}

@test "generate a merge with no conflict with a remote branch" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    dolt push test-remote master
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
    dolt push test-remote master
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
    run dolt pull origin
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Updating" ]] || false
}

@test "generate a merge with a conflict with a remote branch" {
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
    dolt push test-remote master
    cd "dolt-repo-clones"
    dolt clone http://localhost:50051/test-org/test-repo
    cd ..
    dolt sql -q "insert into test values (0, 0, 0, 0, 0, 0)"
    dolt add test
    dolt commit -m "row to generate conflict"
    dolt push test-remote master
    cd "dolt-repo-clones/test-repo"
    dolt sql -q "insert into test values (0, 1, 1, 1, 1, 1)"
    dolt add test
    dolt commit -m "conflicting row"
    run dolt pull origin
    [ "$status" -eq 0 ]
    [[ "$output" =~ "CONFLICT" ]]
    dolt conflicts resolve test --ours
    dolt add test
    dolt commit -m "Fixed conflicts"
    run dolt push origin master
    cd ../../
    dolt pull test-remote
    run dolt log
    [[ "$output" =~ "Fixed conflicts" ]] || false
}

@test "clone sets your current branch appropriately" {
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

    # master hasn't been pushed so expect zzz to be the current branch and the string master should not be present
    run dolt branch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "* zzz" ]] || false
    [[ ! "$output" =~ "master" ]] || false
    cd ../..
    dolt push test-remote master
    cd "dolt-repo-clones"
    dolt clone http://localhost:50051/test-org/test-repo test-repo2
    cd test-repo2

    # master pushed so it should be the current branch.
    run dolt branch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "* master" ]] || false
}

@test "dolt pull onto a dirty working set fails" {
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
    dolt push test-remote master
    cd "dolt-repo-clones"
    dolt clone http://localhost:50051/test-org/test-repo
    cd ..
    dolt sql -q "insert into test values (0, 0, 0, 0, 0, 0)"
    dolt add test
    dolt commit -m "row to generate conflict"
    dolt push test-remote master
    cd "dolt-repo-clones/test-repo"
    dolt sql -q "insert into test values (0, 1, 1, 1, 1, 1)"
    run dolt pull origin
    [ "$status" -ne 0 ]
    [[ "$output" =~ "error: Your local changes to the following tables would be overwritten by merge:" ]] || false
    [[ "$output" =~ "test" ]] || false
    [[ "$output" =~ "Please commit your changes before you merge." ]] || false
}

@test "force push to master" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    dolt push test-remote master

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
    dolt push test-remote master
    cd "dolt-repo-clones/test-repo"
    dolt sql <<SQL
CREATE TABLE other (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt add other
    dolt commit -m "added other table"
    dolt fetch
    run dolt push origin master
    [ "$status" -eq 1 ]
    [[ "$output" =~ "tip of your current branch is behind" ]] || false
    dolt push -f master
    run dolt push -f origin master
    [ "$status" -eq 0 ]
}


@test "force fetch from master" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    dolt push test-remote master

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
    dolt push test-remote master
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
    dolt push -f origin master
    cd ../../
    run dolt fetch test-remote
    [ "$status" -ne 0 ]
    run dolt pull
    [ "$status" -ne 0 ]
    run dolt fetch -f test-remote
    [ "$status" -eq 0 ]
    dolt pull
    [ "$status" -eq 0 ]
}

create_master_remote_branch() {
    dolt remote add origin http://localhost:50051/test-org/test-repo
    dolt sql -q 'create table test (id int primary key);'
    dolt add .
    dolt commit -m 'create test table.'
    dolt push origin master:master
}

create_two_more_remote_branches() {
    dolt commit --allow-empty -m 'another commit.'
    dolt push origin master:branch-one
    dolt sql -q 'insert into test (id) values (1), (2), (3);'
    dolt add .
    dolt commit -m 'add some values.'
    dolt push origin master:branch-two
}

create_three_remote_branches() {
    create_master_remote_branch
    create_two_more_remote_branches
}

@test "clone creates remotes refs for all remote branches" {
    create_three_remote_branches
    cd dolt-repo-clones
    dolt clone http://localhost:50051/test-org/test-repo
    cd test-repo
    run dolt branch -a
    [ "$status" -eq 0 ]
    [[ "$output" =~ "* master" ]] || false
    [[ ! "$output" =~ " branch-one" ]] || false
    [[ ! "$output" =~ " branch-two" ]] || false
    [[ "$output" =~ "remotes/origin/master" ]] || false
    [[ "$output" =~ "remotes/origin/branch-one" ]] || false
    [[ "$output" =~ "remotes/origin/branch-two" ]] || false
}

@test "fetch creates new remote refs for new remote branches" {
    create_master_remote_branch

    cd dolt-repo-clones
    dolt clone http://localhost:50051/test-org/test-repo
    cd test-repo
    dolt branch -a
    run dolt branch -a
    [ "$status" -eq 0 ]
    [[ "$output" =~ "* master" ]] || false
    [[ "$output" =~ "remotes/origin/master" ]] || false

    cd ../../
    create_two_more_remote_branches

    cd dolt-repo-clones/test-repo
    dolt fetch
    run dolt branch -a
    [ "$status" -eq 0 ]
    [[ "$output" =~ "* master" ]] || false
    [[ "$output" =~ "remotes/origin/master" ]] || false
    [[ "$output" =~ "remotes/origin/branch-one" ]] || false
    [[ "$output" =~ "remotes/origin/branch-two" ]] || false
}

setup_ref_test() {
    create_master_remote_branch

    cd dolt-repo-clones
    dolt clone http://localhost:50051/test-org/test-repo
    cd test-repo
}

@test "can use refs/remotes/origin/... as commit reference for log" {
    setup_ref_test
    dolt log refs/remotes/origin/master
}

@test "can use refs/remotes/origin/... as commit reference for diff" {
    setup_ref_test
    dolt diff HEAD refs/remotes/origin/master
    dolt diff refs/remotes/origin/master HEAD
}

@test "can use refs/remotes/origin/... as commit reference for merge" {
    setup_ref_test
    dolt merge refs/remotes/origin/master
}

@test "can use remotes/origin/... as commit reference for log" {
    setup_ref_test
    dolt log remotes/origin/master
}

@test "can use remotes/origin/... as commit reference for diff" {
    setup_ref_test
    dolt diff HEAD remotes/origin/master
    dolt diff remotes/origin/master HEAD
}

@test "can use remotes/origin/... as commit reference for merge" {
    setup_ref_test
    dolt merge remotes/origin/master
}

@test "can use origin/... as commit reference for log" {
    setup_ref_test
    dolt log origin/master
}

@test "can use origin/... as commit reference for diff" {
    setup_ref_test
    dolt diff HEAD origin/master
    dolt diff origin/master HEAD
}

@test "can use origin/... as commit reference for merge" {
    setup_ref_test
    dolt merge origin/master
}

@test "can delete remote reference branch as origin/..." {
    setup_ref_test
    cd ../../
    create_two_more_remote_branches
    cd dolt-repo-clones/test-repo
    dolt fetch # TODO: Remove this fetch once clone works

    dolt branch -r -d origin/master
    run dolt branch -a
    [ "$status" -eq 0 ]
    [[ "$output" =~ "* master" ]] || false
    [[ ! "$output" =~ "remotes/origin/master" ]] || false
    [[ "$output" =~ "remotes/origin/branch-one" ]] || false
    [[ "$output" =~ "remotes/origin/branch-two" ]] || false
    dolt branch -r -d origin/branch-one origin/branch-two
    run dolt branch -a
    [ "$status" -eq 0 ]
    [[ "$output" =~ "* master" ]] || false
    [[ ! "$output" =~ "remotes/origin/master" ]] || false
    [[ ! "$output" =~ "remotes/origin/branch-one" ]] || false
    [[ ! "$output" =~ "remotes/origin/branch-two" ]] || false
}

@test "can list remote reference branches with -r" {
    setup_ref_test
    cd ../../
    create_two_more_remote_branches
    cd dolt-repo-clones/test-repo
    dolt fetch # TODO: Remove this fetch once clone works

    run dolt branch -r
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "* master" ]] || false
    [[ "$output" =~ "remotes/origin/master" ]] || false
    [[ "$output" =~ "remotes/origin/branch-one" ]] || false
    [[ "$output" =~ "remotes/origin/branch-two" ]] || false
    run dolt branch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "* master" ]] || false
    [[ ! "$output" =~ "remotes/origin/master" ]] || false
    [[ ! "$output" =~ "remotes/origin/branch-one" ]] || false
    [[ ! "$output" =~ "remotes/origin/branch-two" ]] || false
    run dolt branch -a
    [ "$status" -eq 0 ]
    [[ "$output" =~ "* master" ]] || false
    [[ "$output" =~ "remotes/origin/master" ]] || false
    [[ "$output" =~ "remotes/origin/branch-one" ]] || false
    [[ "$output" =~ "remotes/origin/branch-two" ]] || false
}
