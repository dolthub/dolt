#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    cd $BATS_TMPDIR
    mkdir remotes-$$
    mkdir remotes-$$/empty
    echo remotesrv log available here $BATS_TMPDIR/remotes-$$/remotesrv.log
    remotesrv --http-port 1234 --dir ./remotes-$$ &> ./remotes-$$/remotesrv.log 3>&- &
    cd dolt-repo-$$
    mkdir "dolt-repo-clones"
}

teardown() {
    teardown_common
    pgrep remotesrv | xargs kill
    rm -rf $BATS_TMPDIR/remotes-$$
}

@test "dolt remotes server is running" {
    pgrep remotesrv
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

@test "push and pull master branch from a remote" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    run dolt push test-remote master
    [ "$status" -eq 0 ]
    [ -d "$BATS_TMPDIR/remotes-$$/test-org/test-repo" ]
    run dolt pull test-remote
    [ "$status" -eq 0 ]
    skip "Should say Already up to date not fast forward"
    [[ "$output" = "up to date" ]] || false
}

@test "push and pull non-master branch from remote" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    dolt checkout -b test-branch
    run dolt push test-remote test-branch
    [ "$status" -eq 0 ]
    run dolt pull test-remote
    [ "$status" -eq 0 ]
    skip "Should say up to date not fast forward"
    [[ "$output" = "up to date" ]] || false
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
    [[ "$output" =~ "readme-text" ]] || false
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
    [ "$status" -eq 0 ]
    cd empty
    run dolt status
    [ "$status" -eq 0 ]
    run ls
    [[ "$output" =~ "LICENSE.md" ]] || false
    [[ "$output" =~ "README.md" ]] || false
    run dolt remote -v
    [ "$status" -eq 0 ]
    [[ "$output" =~ "origin" ]] || false
}

@test "clone a non-existent remote" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    cd "dolt-repo-clones"
    run dolt clone foo/bar
    [ "$status" -eq 1 ]
    skip "Cloning a non-existant repository fails weirdly and leaves trash"
    [ "$output" = "fatal: repository 'foo/bar' does not exist" ]
    [[ ! "$output" =~ "permission denied" ]] || false
    [ ! -d bar ]
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
    echo $output
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
    skip "This remotes/origin/master syntax no longer works but works on git"
    run dolt merge remotes/origin/master
    [ "$status" -eq 0 ]
    # This needs to say up-to-date like the skipped test above
    # [[ "$output" =~ "up to date" ]]
    dolt fetch origin master
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
    skip "dolt push when behind returns a 0 exit code now. should be 1"
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
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
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
    [[ "$output" =~ "aaa" ]] || false
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
    [[ "$output" =~ "aaa" ]] || false
    [[ "$output" =~ "zzz" ]] || false
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
}
