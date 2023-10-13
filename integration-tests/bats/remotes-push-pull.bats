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
    rm -rf $BATS_TMPDIR/remotes-$$
}

@test "remotes-push-pull: pull also fetches" {
    mkdir remote
    mkdir repo1

    cd repo1
    dolt init
    dolt remote add origin file://../remote
    dolt push origin main

    cd ..
    dolt clone file://./remote repo2

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
}

@test "remotes-push-pull: pull also fetches, but does not merge other branches" {
    mkdir remote
    mkdir repo1

    cd repo1
    dolt init
    dolt remote add origin file://../remote
    dolt push --set-upstream origin main
    dolt checkout -b other
    dolt commit --allow-empty -m "first commit on other"
    dolt push --set-upstream origin other

    cd ..
    dolt clone file://./remote repo2

    cd repo2
    dolt pull


    dolt commit --allow-empty -m "a commit for main from repo2"
    dolt push

    run dolt checkout other
    [ "$status" -eq 0 ]
    [[ "$output" =~ "branch 'other' set up to track 'origin/other'." ]] || false

    run dolt log --oneline -n 1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "first commit on other" ]] || false

    run dolt status
    [[ "$output" =~ "Your branch is up to date with 'origin/other'." ]] || false

    dolt commit --allow-empty -m "second commit on other from repo2"
    dolt push

    cd ../repo1
    dolt checkout other
    run dolt pull
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Updating" ]] || false

    run dolt log --oneline -n 1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "second commit on other from repo2" ]] || false

    dolt checkout main
    run dolt status
    [[ "$output" =~ "behind 'origin/main' by 1 commit" ]] || false

    run dolt log --oneline -n 1
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "a commit for main from repo2" ]] || false

    run dolt pull
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Updating" ]] || false

    run dolt log --oneline -n 1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "a commit for main from repo2" ]] || false
}

@test "remotes-push-pull: push and pull an unknown remote" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    run dolt push poop main
    [ "$status" -eq 1 ]
    [[ "$output" =~ "remote 'poop' not found" ]] || false
    run dolt pull poop
    [ "$status" -eq 1 ]
    [[ "$output" =~ "remote 'poop' not found" ]] || false
}

@test "remotes-push-pull: push with only one argument" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    run dolt push test-remote
    [ "$status" -eq 1 ]
    [[ "$output" =~ "fatal: The current branch main has no upstream branch." ]] || false
    [[ "$output" =~ "To push the current branch and set the remote as upstream, use" ]] || false
    [[ "$output" =~ "dolt push --set-upstream test-remote main" ]] || false
    [[ "$output" =~ "To have this happen automatically for branches without a tracking" ]] || false
    [[ "$output" =~ "upstream, see 'push.autoSetupRemote' in 'dolt config --help'" ]] || false
}

@test "remotes-push-pull: push without set-upstream works with autoSetUpRemote set to true" {
    dolt config --local --add push.autoSetUpRemote true
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    run dolt push test-remote main
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Uploading" ]] || false

    run dolt push test-remote main
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Everything up-to-date" ]] || false
}

@test "remotes-push-pull: push and pull main branch from a remote" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    run dolt push --set-upstream test-remote main
    [ "$status" -eq 0 ]
    [ -d "$BATS_TMPDIR/remotes-$$/test-org/test-repo" ]
    run dolt pull test-remote
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Everything up-to-date" ]] || false
}

@test "remotes-push-pull: push and pull non-main branch from remote" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    dolt checkout -b test-branch
    run dolt push --set-upstream test-remote test-branch
    [ "$status" -eq 0 ]
    run dolt pull test-remote
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Everything up-to-date" ]] || false
}

@test "remotes-push-pull: pull with explicit remote and branch" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    dolt checkout -b test-branch
    dolt sql -q "create table t1(c0 varchar(100));"
    dolt add .
    dolt commit -am "adding table t1"
    run dolt push test-remote test-branch
    [ "$status" -eq 0 ]
    dolt checkout main
    run dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "t1" ]] || false

    # Specifying a non-existent remote branch returns an error
    run dolt pull test-remote doesnotexist
    [ "$status" -eq 1 ]
    [[ "$output" =~ 'branch "doesnotexist" not found on remote' ]] || false

    # Explicitly specifying the remote and branch will merge in that branch
    run dolt pull test-remote test-branch
    [ "$status" -eq 0 ]
    run dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "t1" ]] || false

    # Make a conflicting working set change and test that pull complains
    dolt reset --hard HEAD^1
    dolt sql -q "create table t1 (pk int primary key);"
    run dolt pull test-remote test-branch
    [ "$status" -eq 1 ]
    [[ "$output" =~ 'cannot merge with uncommitted changes' ]] || false

    # Commit changes and test that a merge conflict fails the pull
    dolt add .
    dolt commit -am "adding new t1 table"
    run dolt pull test-remote test-branch
    [ "$status" -eq 1 ]
    [[ "$output" =~ "table with same name 't1' added in 2 commits can't be merged" ]] || false
}

@test "remotes-push-pull: push and pull from non-main branch and use --set-upstream" {
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

@test "remotes-push-pull: push output" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    dolt checkout -b test-branch
    dolt sql -q "create table test (pk int, c1 int, primary key(pk))"
    dolt add .
    dolt commit -m "Added test table"
    dolt push --set-upstream test-remote test-branch | tr "\n" "*" > output.txt
    run tail -c 1 output.txt
    [[ "$output" != "" ]] || false   # should have a spinner
}

@test "remotes-push-pull: push and pull with docs from remote" {
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

    cd test-repo
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test doc commit" ]] || false

    cd ../../
    echo "updated-license" > LICENSE.md
    dolt docs upload LICENSE.md LICENSE.md
    dolt add .
    dolt commit -m "updated license"
    dolt push test-remote main

    cd dolt-repo-clones/test-repo
    run dolt pull
    [[ "$output" =~ "Updating" ]] || false
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "updated license" ]] || false
    dolt docs print LICENSE.md > LICENSE.md
    run cat LICENSE.md
    [ "$status" -eq 0 ]
    [[ "$output" =~ "updated-license" ]] || false
}

@test "remotes-push-pull: push and pull tags to/from remote" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    dolt sql <<SQL
CREATE TABLE test (pk int PRIMARY KEY);
INSERT INTO  test VALUES (1),(2),(3);
SQL
    dolt add . && dolt commit -m "added table test"
    dolt push test-remote main
    cd "dolt-repo-clones"
    run dolt clone http://localhost:50051/test-org/test-repo
    [ "$status" -eq 0 ]

    cd ../
    dolt tag v1 head
    dolt push test-remote v1

    cd dolt-repo-clones/test-repo
    dolt pull
    [ "$status" -eq 0 ]
    run dolt tag
    [ "$status" -eq 0 ]
    [[ "$output" =~ "v1" ]] || false
}

@test "remotes-push-pull: tags are fetched when pulling" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    dolt sql <<SQL
CREATE TABLE test (pk int PRIMARY KEY);
INSERT INTO  test VALUES (1),(2),(3);
SQL
    dolt add . && dolt commit -m "added table test"
    dolt push test-remote main
    cd "dolt-repo-clones"
    run dolt clone http://localhost:50051/test-org/test-repo
    [ "$status" -eq 0 ]

     cd ../
    dolt tag v1 head -m "tag message"
    dolt push test-remote v1
    dolt checkout -b other
    dolt sql -q "INSERT INTO test VALUES (8),(9),(10)"
    dolt add . && dolt commit -m "added values on branch other"
    dolt push -u test-remote other
    dolt tag other_tag head  -m "other message"
    dolt push test-remote other_tag

    cd dolt-repo-clones/test-repo
    run dolt pull
    [ "$status" -eq 0 ]
    run dolt tag -v
    [ "$status" -eq 0 ]
    [[ "$output" =~ "v1" ]] || false
    [[ "$output" =~ "tag message" ]] || false
    [[ "$output" =~ "other_tag" ]] || false
    [[ "$output" =~ "other message" ]] || false
}

@test "remotes-push-pull: try to push a remote that is behind tip" {
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
    run dolt push origin main
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Everything up-to-date" ]] || false
    dolt fetch
    run dolt push origin main
    [ "$status" -eq 1 ]
    [[ "$output" =~ " ! [rejected]            main -> main" ]] || false
    [[ "$output" =~ "tip of your current branch is behind" ]] || false
}

@test "remotes-push-pull: dolt_pull() with divergent head" {
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

    run dolt sql -q "call dolt_pull('origin')" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ '0,0' ]] || false

    run dolt log --oneline -n 1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Merge branch 'main' of" ]] || false
    [[ ! "$output" =~ "new message" ]] || false
}

@test "remotes-push-pull: dolt pull onto a dirty working set fails" {
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
    dolt push test-remote main
    cd "dolt-repo-clones"
    dolt clone http://localhost:50051/test-org/test-repo
    cd ..
    dolt sql -q "insert into test values (0, 0, 0, 0, 0, 0)"
    dolt add test
    dolt commit -m "row to generate conflict"
    dolt push test-remote main
    cd "dolt-repo-clones/test-repo"
    dolt sql -q "insert into test values (0, 1, 1, 1, 1, 1)"
    run dolt pull origin
    [ "$status" -ne 0 ]
    [[ "$output" =~ "cannot merge with uncommitted changes" ]] || false
}

@test "remotes-push-pull: force push to main" {
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
    run dolt push origin main
    [ "$status" -eq 1 ]
    [[ "$output" =~ "tip of your current branch is behind" ]] || false
    dolt push -f origin main
    run dolt push -f origin main
    [ "$status" -eq 0 ]
}

@test "remotes-push-pull: fetch after force push" {
    mkdir remote clone1
    cd clone1
    dolt init
    dolt sql -q "create table t (pk int primary key);"
    dolt commit -Am "commit1"

    dolt remote add origin file://../remote
    dolt push origin main

    cd ..
    dolt clone file://./remote clone2

    cd clone1
    dolt commit --amend -m "commit1 edited"
    dolt push origin main -f

    cd ../clone2
    run dolt fetch
    [ "$status" -eq 0 ]
}

@test "remotes: validate that a config is needed for a pull." {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    dolt push test-remote main
    dolt fetch test-remote
    cd "dolt-repo-clones"
    dolt clone http://localhost:50051/test-org/test-repo
    cd ..
    dolt sql <<SQL
CREATE TABLE test (
  pk int,
  val int,
  PRIMARY KEY (pk)
);
SQL
    dolt add test
    dolt commit -m "test commit"
    dolt push test-remote main

    # cd to the other directory and execute a pull without a config
    cd "dolt-repo-clones/test-repo"
    dolt config --global --unset user.name
    dolt config --global --unset user.email
    run dolt pull
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Could not determine name and/or email." ]] || false

    dolt config --global --add user.name mysql-test-runner
    dolt config --global --add user.email mysql-test-runner@liquidata.co
    dolt pull
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test commit" ]] || false

    # test pull with workspace up to date
    dolt config --global --unset user.name
    dolt config --global --unset user.email
    run dolt pull
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Could not determine name and/or email." ]] || false

    # turn back on the configs and make a change in the remote
    dolt config --global --add user.name mysql-test-runner
    dolt config --global --add user.email mysql-test-runner@liquidata.co

    cd ../../
    dolt sql -q "insert into test values (1,1)"
    dolt commit -am "commit from main repo"
    dolt push test-remote main

    # Try a --no-ff merge and make sure it fails
    cd "dolt-repo-clones/test-repo"
    # turn configs off again
    dolt config --global --unset user.name
    dolt config --global --unset user.email

    run dolt pull --no-ff
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Could not determine name and/or email." ]] || false

    # Now do a two sided merge
    dolt config --global --add user.name mysql-test-runner
    dolt config --global --add user.email mysql-test-runner@liquidata.co

    dolt sql -q "insert into test values (2,1)"
    dolt commit -am "commit from test repo"

    cd ../../
    dolt sql -q "insert into test values (2,2)"
    dolt commit -am "commit from main repo"
    dolt push test-remote main

    cd "dolt-repo-clones/test-repo"
    dolt config --global --unset user.name
    dolt config --global --unset user.email

    run dolt pull
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Could not determine name and/or email." ]] || false
}

@test "remotes-push-pull: push not specifying a branch throws error on default remote" {
    dolt remote add origin http://localhost:50051/test-org/test-repo
    run dolt remote -v
    [[ "$output" =~  origin ]] || false
    run dolt push origin
    [ "$status" -eq 1 ]
    [[ "$output" =~ "fatal: The current branch main has no upstream branch." ]] || false
}

@test "remotes-push-pull: push not specifying a branch create new remote branch without setting upstream on non-default remote" {
    dolt remote add origin http://localhost:50051/test-org/test-repo
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    run dolt remote -v
    [[ "$output" =~  origin ]] || false
    [[ "$output" =~  test-remote ]] || false

    dolt checkout -b test-branch
    dolt branch -a
    run dolt push test-remote
    [ "$status" -eq 0 ]
    [[ "$output" =~ "* [new branch]          test-branch -> test-branch" ]] || false
}

@test "remotes-push-pull: push not specifying a branch creates new remote branch and sets upstream on non-default remote when -u is used" {
    dolt remote add origin http://localhost:50051/test-org/test-repo
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    run dolt remote -v
    [[ "$output" =~  origin ]] || false
    [[ "$output" =~  test-remote ]] || false

    dolt checkout -b test-branch
    run dolt push -u test-remote
    [ "$status" -eq 0 ]
    [[ "$output" =~ "* [new branch]          test-branch -> test-branch" ]] || false
    [[ "$output" =~ "branch 'test-branch' set up to track 'test-remote/test-branch'." ]] || false
}

@test "remotes-push-pull: push origin throws an error when no remote is set" {
    run dolt remote -v
    [ "$output" = "" ]
    run dolt push origin
    [ "$status" -eq 1 ]
    [[ "$output" =~ "fatal: remote 'origin' not found" ]] || false
}

@test "remotes-push-pull: push --all with no remote or refspec specified" {
    dolt remote add origin http://localhost:50051/test-org/test-repo
    run dolt remote -v
    [[ "$output" =~  origin ]] || false

    dolt checkout -b new-branch
    run dolt push --all
    [ "$status" -eq 0 ]
    [[ "$output" =~ "* [new branch]          main -> main" ]] || false
    [[ "$output" =~ "* [new branch]          new-branch -> new-branch" ]] || false
}

@test "remotes-push-pull: push --all with remote specified" {
    mkdir remote
    mkdir repo1

    cd repo1
    dolt init
    dolt remote add origin file://../remote
    dolt push origin main

    cd ..
    dolt clone file://./remote repo2
    cd repo2

    dolt sql -q "CREATE TABLE test (pk INT PRIMARY KEY, col1 VARCHAR(10))"
    dolt add .
    dolt commit -am "create table"
    dolt checkout -b branch1
    dolt sql -q "INSERT INTO test VALUES (1, '1')"
    dolt commit -am "add 1s"
    run dolt push --all origin  # should not set upstream for new branches
    [ "$status" -eq 0 ]
    [[ "$output" =~ " * [new branch]          branch1 -> branch1" ]] || false

    # on branch1
    run dolt push
    [ "$status" -eq 1 ]
    [[ "$output" =~ "fatal: The current branch branch1 has no upstream branch." ]] || false

    dolt sql -q "INSERT INTO test VALUES (2, '2')"
    dolt commit -am "add 2s"
    dolt checkout -b branch2
    dolt sql -q "INSERT INTO test VALUES (3, '3')"
    dolt commit -am "add 3s"
    run dolt push --all -u origin   # should set upstream for all branches
    [ "$status" -eq 0 ]
    [[ "$output" =~ " * [new branch]          branch1 -> branch1" ]] || false
    [[ "$output" =~ "branch 'branch1' set up to track 'origin/branch1'." ]] || false
    [[ "$output" =~ "branch 'branch2' set up to track 'origin/branch2'." ]] || false
}

@test "remotes-push-pull: push --all with multiple remotes will push all local branches to default remote, regardless of their upstream" {
    mkdir remote1
    mkdir repo1
    cd repo1
    dolt init
    dolt remote add origin file://../remote1
    dolt push origin main

    cd ..
    dolt clone file://./remote1 remote2
    cd repo1
    dolt remote add test-remote file://../remote2

    dolt sql -q "CREATE TABLE test (pk INT PRIMARY KEY, col1 VARCHAR(10))"
    dolt add .
    dolt commit -am "create table"
    dolt checkout -b branch1
    dolt sql -q "INSERT INTO test VALUES (1, '1')"
    dolt commit -am "add 1s"
    run dolt push --all -u origin
    [ "$status" -eq 0 ]
    [[ "$output" =~ " * [new branch]          branch1 -> branch1" ]] || false
    [[ "$output" =~ "branch 'branch1' set up to track 'origin/branch1'." ]] || false

    dolt sql -q "INSERT INTO test VALUES (2, '2')"
    dolt commit -am "add 2s"
    dolt checkout -b branch2
    dolt sql -q "INSERT INTO test VALUES (3, '3')"
    dolt commit -am "add 3s"
    run dolt push -u test-remote branch2
    [ "$status" -eq 0 ]
    [[ "$output" =~ " * [new branch]          branch2 -> branch2" ]] || false
    [[ "$output" =~ "branch 'branch2' set up to track 'test-remote/branch2'." ]] || false

    run dolt branch -a
    [ "$status" -eq 0 ]
    [[ "$output" =~ "remotes/origin/branch1" ]] || false
    [[ "$output" =~ "remotes/origin/main" ]] || false
    [[ "$output" =~ "remotes/test-remote/branch2" ]] || false
    [[ ! "$output" =~ "remotes/origin/branch2" ]] || false

    run dolt push --all   # should push all branches to origin including branch2
    [ "$status" -eq 0 ]
    [[ "$output" =~ " * [new branch]          branch2 -> branch2" ]] || false

    run dolt branch -a
    [ "$status" -eq 0 ]
    [[ "$output" =~ "remotes/origin/branch2" ]] || false
}

@test "remotes-push-pull: push --all with local branch that has conflict" {
    mkdir remote
    mkdir repo1

    cd repo1
    dolt init
    dolt remote add origin file://../remote
    dolt push origin main

    cd ..
    dolt clone file://./remote repo2
    cd repo2

    dolt sql -q "CREATE TABLE test (pk INT PRIMARY KEY, col1 VARCHAR(10))"
    dolt add .
    dolt commit -am "create table"
    dolt checkout -b branch1
    dolt sql -q "INSERT INTO test VALUES (1, '1')"
    dolt commit -am "add 1s"
    run dolt push --all -u   # should not set upstream for new branches
    [ "$status" -eq 0 ]
    [[ "$output" =~ " * [new branch]          branch1 -> branch1" ]] || false
    [[ "$output" =~ "branch 'branch1' set up to track 'origin/branch1'." ]] || false

    cd ../repo1
    dolt fetch
    dolt pull origin main
    dolt checkout branch1
    dolt sql -q "INSERT INTO test VALUES (2, '2')"
    dolt commit -am "add 2s"
    dolt push

    cd ../repo2
    dolt sql -q "INSERT INTO test VALUES (2, '2')"
    dolt commit -am "add 2s"

    dolt checkout -b branch2
    dolt sql -q "INSERT INTO test VALUES (3, '3')"
    dolt commit -am "add 3s"
    run dolt push --all -u   # should set upstream for all branches
    [ "$status" -eq 1 ]
    [[ "$output" =~ " * [new branch]          branch2 -> branch2" ]] || false
    [[ "$output" =~ " ! [rejected]            branch1 -> branch1 (non-fast-forward)" ]] || false
    [[ "$output" =~ "branch 'branch2' set up to track 'origin/branch2'." ]] || false
    [[ "$output" =~ "Updates were rejected because the tip of your current branch is behind" ]] || false
}

@test "remotes-push-pull: pushing empty branch does not panic" {
    run dolt push origin ''
    [ "$status" -eq 1 ]
    [[ "$output" =~ "invalid ref spec: ''" ]] || false
}

@test "remotes-push-pull: push set upstream succeeds even if up to date" {
    dolt remote add origin http://localhost:50051/test-org/test-repo
    dolt push origin main
    dolt checkout -b feature
    dolt push --set-upstream origin feature

    cd dolt-repo-clones
    dolt clone http://localhost:50051/test-org/test-repo
    cd test-repo
    dolt checkout -b feature
    run dolt push
    [ "$status" -eq 1 ]
    [[ "$output" =~ "The current branch feature has no upstream branch." ]] || false
    dolt push --set-upstream origin feature
    dolt push
}

@test "remotes-push-pull: local clone pushes to other branch" {
    mkdir repo1
    mkdir rem1
    cd repo1
    dolt init
    dolt sql -q "create table t (i int)"
    run dolt status
    [[ "$output" =~ "new table:" ]] || false
    dolt commit -Am "new table"
    dolt remote add rem1 file://../rem1
    dolt push rem1 main
    cd ..

    dolt clone file://./rem1/ repo2
    cd repo2
    dolt checkout -b other
    dolt sql -q "create table t2 (i int)"
    dolt add .
    dolt commit -am "adding table from other"
    dolt remote add rem1 file://../rem1
    dolt push rem1 other

    cd ../repo1
    dolt fetch rem1
    dolt checkout other
    run dolt log
    [[ "$output" =~ "adding table from other" ]] || false
}

@test "remotes-push-pull: pull with DOLT_AUTHOR_DATE and DOLT_COMMITER_DATE doesn't overwrite commit timestamps" {
    mkdir repo1

    cd repo1
    dolt init
    dolt sql -q "create table t1(a int)"
    dolt commit -Am "new table"
    dolt branch b1
    dolt remote add origin file://../remote1
    dolt push origin main
    dolt push origin b1

    cd ..
    dolt clone file://./remote1 repo2

    cd repo2
    TZ=PST+8 DOLT_COMMITTER_DATE='2023-09-26T12:34:56' DOLT_AUTHOR_DATE='2023-09-26T01:23:45' dolt fetch
    TZ=PST+8 DOLT_COMMITTER_DATE='2023-09-26T12:34:56' DOLT_AUTHOR_DATE='2023-09-26T01:23:45' dolt pull

    run dolt_log_in_PST
    [[ ! "$output" =~ 'Tue Sep 26 01:23:45' ]] || false

    TZ=PST+8 DOLT_COMMITTER_DATE='2023-09-26T12:34:56' DOLT_AUTHOR_DATE='2023-09-26T01:23:45' dolt checkout b1
    run dolt_log_in_PST
    [[ ! "$output" =~ 'Tue Sep 26 01:23:45' ]] || false

    cd ../repo1
    dolt checkout b1
    dolt commit --allow-empty -m 'empty commit'
    dolt push origin b1

    cd ../repo2
    TZ=PST+8 DOLT_COMMITTER_DATE='2023-09-26T12:34:56' DOLT_AUTHOR_DATE='2023-09-26T01:23:45' dolt pull

    run dolt_log_in_PST
    [[ ! "$output" =~ 'Tue Sep 26 01:23:45' ]] || false
}

@test "remotes-push-pull: validate that a config is needed for a pull." {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    dolt push test-remote main
    dolt fetch test-remote
    cd "dolt-repo-clones"
    dolt clone http://localhost:50051/test-org/test-repo
    cd ..
    dolt sql <<SQL
CREATE TABLE test (
  pk int,
  val int,
  PRIMARY KEY (pk)
);
SQL
    dolt add test
    dolt commit -m "test commit"
    dolt push test-remote main

    # cd to the other directory and execute a pull without a config
    cd "dolt-repo-clones/test-repo"
    dolt config --global --unset user.name
    dolt config --global --unset user.email
    run dolt pull
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Could not determine name and/or email." ]] || false

    dolt config --global --add user.name mysql-test-runner
    dolt config --global --add user.email mysql-test-runner@liquidata.co
    dolt pull
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test commit" ]] || false

    # test pull with workspace up to date
    dolt config --global --unset user.name
    dolt config --global --unset user.email
    run dolt pull
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Could not determine name and/or email." ]] || false

    # turn back on the configs and make a change in the remote
    dolt config --global --add user.name mysql-test-runner
    dolt config --global --add user.email mysql-test-runner@liquidata.co

    cd ../../
    dolt sql -q "insert into test values (1,1)"
    dolt commit -am "commit from main repo"
    dolt push test-remote main

    # Try a --no-ff merge and make sure it fails
    cd "dolt-repo-clones/test-repo"
    # turn configs off again
    dolt config --global --unset user.name
    dolt config --global --unset user.email

    run dolt pull --no-ff
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Could not determine name and/or email." ]] || false

    # Now do a two sided merge
    dolt config --global --add user.name mysql-test-runner
    dolt config --global --add user.email mysql-test-runner@liquidata.co

    dolt sql -q "insert into test values (2,1)"
    dolt commit -am "commit from test repo"

    cd ../../
    dolt sql -q "insert into test values (2,2)"
    dolt commit -am "commit from main repo"
    dolt push test-remote main

    cd "dolt-repo-clones/test-repo"
    dolt config --global --unset user.name
    dolt config --global --unset user.email

    run dolt pull
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Could not determine name and/or email." ]] || false
}
