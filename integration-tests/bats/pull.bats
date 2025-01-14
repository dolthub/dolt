#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    # We don't use setup_common because we don't want the remote server to run in the top level test directory.
    # Using multiple databases is kind of inherant to these tests.
    # Instead, we manualy setup the remote server in the individual tests.
    setup_no_dolt_init

    TESTDIRS=$(pwd)/testdirs
    mkdir -p $TESTDIRS/{rem1,repo1}

    # repo1 -> rem1 -> repo2
    cd $TESTDIRS/repo1
    dolt init
    dolt branch feature
    dolt remote add origin file://../rem1
    dolt remote add test-remote file://../rem1
    dolt push origin main

    cd $TESTDIRS
    dolt clone file://rem1 repo2
    cd $TESTDIRS/repo2
    dolt branch feature
    dolt remote add test-remote file://../rem1

    # table and commits only present on repo1, rem1 at start
    cd $TESTDIRS/repo1
    dolt sql -q "create table t1 (a int primary key, b int)"
    dolt add .
    dolt commit -am "First commit"
    dolt sql -q "insert into t1 values (0,0)"
    dolt commit -am "Second commit"
    dolt push origin main
    cd $TESTDIRS
}

teardown() {
    teardown_common
    rm -rf $TESTDIRS
}

@test "pull: pull main" {
    cd repo2

    setup_remote_server

    dolt pull origin
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "First commit" ]] || false
    [[ "$output" =~ "Second commit" ]] || false
}

@test "pull: pull custom remote" {
    cd repo2

    setup_remote_server

    dolt pull test-remote
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "First commit" ]] || false
    [[ "$output" =~ "Second commit" ]] || false
}

@test "pull: pull default origin" {
    cd repo2
    dolt remote remove test-remote

    setup_remote_server

    dolt pull
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "First commit" ]] || false
    [[ "$output" =~ "Second commit" ]] || false
}

@test "pull: pull default custom remote" {
    cd repo2
    dolt remote remove origin

    setup_remote_server

    dolt pull
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "First commit" ]] || false
    [[ "$output" =~ "Second commit" ]] || false
}

@test "pull: pull up to date does not error" {
    cd repo2

    setup_remote_server

    dolt pull origin
    run dolt pull origin
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Everything up-to-date" ]] || false

    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "pull: pull unknown remote fails" {
    cd repo2

    setup_remote_server

    run dolt pull unknown
    [ "$status" -eq 1 ]
    [[ "$output" =~ "fatal: remote 'unknown' not found" ]] || false
    [[ ! "$output" =~ "panic" ]] || false
}

@test "pull: pull unknown feature branch fails" {
    cd repo2
    dolt checkout feature

    setup_remote_server

    run dolt pull origin
    [ "$status" -eq 1 ]
    [[ "$output" =~ "You asked to pull from the remote 'origin', but did not specify a branch" ]] || false
    [[ ! "$output" =~ "panic" ]] || false
}

@test "pull: pull feature branch" {
    cd repo1
    dolt checkout feature
    dolt push --set-upstream origin feature

    cd ../repo2
    dolt checkout feature
    dolt push --set-upstream origin feature

    cd ../repo1
    dolt merge main
    dolt push

    cd ../repo2

    setup_remote_server

    dolt pull origin
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "First commit" ]] || false
    [[ "$output" =~ "Second commit" ]] || false
}

@test "pull: checkout after fetch a new feature branch" {
    cd repo1
    dolt checkout -b feature2
    dolt sql -q "create table t2 (i int primary key);"
    dolt sql -q "call dolt_add('.');"
    dolt sql -q "call dolt_commit('-am', 'create t2')"
    dolt push --set-upstream origin feature2

    cd ../repo2
    dolt fetch origin feature2
    dolt checkout feature2
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
    [[ "$output" =~ "t2" ]] || false
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "First commit" ]] || false
    [[ "$output" =~ "Second commit" ]] || false
    [[ "$output" =~ "create t2" ]] || false
}

@test "pull: pull force" {
    cd repo1
    # disable foreign key checks to create merge conflicts
    dolt sql <<SQL
SET FOREIGN_KEY_CHECKS=0;
CREATE TABLE colors (
    id INT NOT NULL,
    color VARCHAR(32) NOT NULL,

    PRIMARY KEY (id),
    INDEX color_index(color)
);
CREATE TABLE objects (
    id INT NOT NULL,
    name VARCHAR(64) NOT NULL,
    color VARCHAR(32)
);
SQL
    dolt commit -A -m "Commit1"
    dolt push origin main

    cd ../repo2
#    setup_remote_server

    dolt pull
    dolt sql -q "alter table objects add constraint color FOREIGN KEY (color) REFERENCES colors(color)"
    dolt commit -A -m "Commit2"

    cd ../repo1
    dolt sql -q "INSERT INTO objects (id,name,color) VALUES (1,'truck','red'),(2,'ball','green'),(3,'shoe','blue')"
    dolt commit -A -m "Commit3"
    dolt push origin main

    cd ../repo2

    setup_remote_server

    run dolt pull

    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONSTRAINT VIOLATION" ]] || false

    dolt sql -q "call dolt_merge('--abort')"

    run dolt pull --force
    [ "$status" -eq 0 ]
    run dolt sql -q "select * from objects"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "truck" ]] || false
    [[ "$output" =~ "ball" ]] || false
    [[ "$output" =~ "shoe" ]] || false
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "First commit" ]] || false
    [[ "$output" =~ "Second commit" ]] || false
    [[ "$output" =~ "Commit1" ]] || false
    [[ "$output" =~ "Commit3" ]] || false
}

@test "pull: pull squash" {
    cd repo2
    dolt sql -q "create table t2 (i int primary key);"
    dolt commit -Am "commit 1"

    setup_remote_server

    dolt pull --squash origin
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
    [[ "$output" =~ "t2" ]] || false

    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Merge branch" ]] || false
    [[ ! "$output" =~ "Second commit" ]] || false
    [[ ! "$output" =~ "First commit" ]] || false
}

@test "pull: pull --noff flag" {
    cd repo2

    setup_remote_server

    dolt pull --no-ff origin
    dolt status

    run dolt log -n 1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Merge branch 'main'" ]] || false

    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "pull: pull dirty working set fails" {
    cd repo2
    dolt pull

    cd ../repo1
    dolt sql -q "insert into t1 values (3, 3)"
    dolt commit -am "dirty commit"
    dolt push origin main

    cd ../repo2
    dolt sql -q "insert into t1 values (2, 2)"

    setup_remote_server

    run dolt pull origin
    [ "$status" -eq 1 ]
    [[ "$output" =~ "cannot merge with uncommitted changes" ]] || false
}

@test "pull: pull tag" {
    cd repo1
    dolt tag v1
    dolt push origin v1
    dolt tag

    cd ../repo2

    setup_remote_server

    dolt pull origin
    run dolt tag
    [ "$status" -eq 0 ]
    [[ "$output" =~ "v1" ]] || false
}

@test "pull: pull tags only for resolved commits" {
    cd repo1
    dolt tag v1 head
    dolt tag v2 head^
    dolt push origin v1
    dolt push origin v2

    dolt checkout feature
    dolt sql -q "create table t2 (a int)"
    dolt add .
    dolt commit -am "feature commit"
    dolt tag v3
    dolt push origin v3

    cd ../repo2

    setup_remote_server

    dolt pull origin
    run dolt tag
    [ "$status" -eq 0 ]
    [[ "$output" =~ "v1" ]] || false
    [[ "$output" =~ "v2" ]] || false
    [[ ! "$output" =~ "v3" ]] || false
}

@test "pull: pull with remote and remote ref" {
    cd repo1
    dolt checkout feature
    dolt checkout -b newbranch
    run dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "t1" ]] || false

    setup_remote_server

    # Specifying a non-existent remote branch returns an error
    run dolt pull origin doesnotexist
    [ "$status" -eq 1 ]
    [[ "$output" =~ 'branch "doesnotexist" not found on remote' ]] || false

    # Explicitly specifying the remote and branch will merge in that branch
    run dolt pull origin main
    [ "$status" -eq 0 ]
    run dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "t1" ]] || false
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "working tree clean" ]] || false

    # Make a conflicting working set change and test that pull complains
    dolt reset --hard HEAD^1
    dolt sql -q "insert into t1 values (0, 100);"
    run dolt pull origin main
    [ "$status" -eq 1 ]
    [[ "$output" =~ 'cannot merge with uncommitted changes' ]] || false

    # Commit changes and test that a merge conflict fails the pull
    dolt commit -am "adding new t1 table"
    run dolt pull origin main
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Auto-merging t1" ]] || false
    [[ "$output" =~ "CONFLICT (content): Merge conflict in t1" ]] || false
    [[ "$output" =~ "Automatic merge failed; 1 table(s) are unmerged." ]] || false
    [[ "$output" =~ "Use 'dolt conflicts' to investigate and resolve conflicts." ]] || false

    run dolt show head
    [ "$status" -eq 0 ]
    [[ "$output" =~ "adding new t1 table" ]] || false
}

@test "pull: pull also fetches, but does not merge other branches" {
    cd repo1
    dolt checkout -b other
    dolt push --set-upstream origin other
    dolt checkout feature
    dolt push origin feature

    cd ../repo2
    dolt fetch
    # this checkout will set upstream because 'other' branch is a new branch that matches one of remote tracking branch
    dolt checkout other
    # this checkout will not set upstream because this 'feature' branch existed before matching remote tracking branch was created
    dolt checkout feature
    dolt push --set-upstream origin feature

    cd ../repo1
    dolt merge main
    dolt push origin feature
    dolt checkout other
    dolt commit --allow-empty -m "new commit on other"
    dolt push

    cd ../repo2
    dolt pull
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false

    dolt checkout other
    run dolt log --oneline -n 1
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "new commit on other" ]] || false

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "behind 'origin/other' by 1 commit" ]] || false
}

@test "pull: pull commits successful merge on current branch" {
    cd repo1
    dolt checkout -b other
    dolt push --set-upstream origin other

    cd ../repo2
    dolt fetch
    # this checkout will set upstream because 'other' branch is a new branch that matches one of remote tracking branch
    dolt checkout other

    cd ../repo1
    dolt sql -q "insert into t1 values (1, 2)"
    dolt commit -am "add (1,2) to t1"
    dolt push

    cd ../repo2
    run dolt sql -q "select * from t1" -r csv
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "1,2" ]] || false

    dolt sql -q "insert into t1 values (2, 3)"
    dolt commit -am "add (2,3) to t1"
    run dolt pull
    [ "$status" -eq 0 ]

    run dolt log --oneline -n 1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Merge branch 'other' of" ]] || false
    [[ ! "$output" =~ "add (1,2) to t1" ]] || false
    [[ ! "$output" =~ "add (2,3) to t1" ]] || false
}

@test "pull: --no-ff and --no-commit" {
    cd repo2
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]

    run dolt pull --no-ff --no-commit origin
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Automatic merge went well; stopped before committing as requested" ]] || false

    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "t1" ]] || false

    dolt commit -m "merge from origin"
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "merge from origin" ]] || false
}

@test "pull: --silent suppresses progress message" {
    cd repo2
    run dolt pull origin --silent
    [ "$status" -eq 0 ]
    ! [[ "$output" =~ "Pulling..." ]] || false
}

@test "pull: pull when there are changes to ignored tables" {
    cd repo1

    dolt sql -q "create table ignore_this (id int primary key, words varchar(100))"
    dolt sql -q "INSERT INTO dolt_ignore VALUES ('ignore_*', true)"

    dolt commit -Am "Added ignore_this table and added a rule in dolt_ignore"

    dolt push origin main

    cd ../repo2

    dolt pull origin main

    dolt sql -q "create table testTable (id int primary key, words varchar(100))"
    dolt sql -q "INSERT INTO testTable VALUES (1, 'hello')"

    dolt commit -Am "Added test table"

    dolt push origin main

    cd ../repo1

    dolt sql -q "INSERT INTO ignore_this VALUES (2, 'world')"

    dolt pull origin main

    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "testTable" ]] || false
}

@test "pull: clean up branches with --prune" {
    cd repo1
    dolt checkout -b other
    dolt commit --allow-empty -m "new commit on other"
    dolt push origin HEAD:new-branch

    run dolt branch -a
    [ "$status" -eq 0 ]
    [[ "$output" =~ "origin/main" ]] || false
    [[ "$output" =~ "origin/new-branch" ]] || false

    cd ../repo2
    dolt pull origin

    run dolt branch -a
    [ "$status" -eq 0 ]
    [[ "$output" =~ "origin/main" ]] || false
    [[ "$output" =~ "origin/new-branch" ]] || false

    dolt merge origin/new-branch
    dolt push origin HEAD:main
    # Delete the remote branch.
    dolt push origin :new-branch

    cd ../repo1
    dolt branch -d main
    dolt checkout -b main origin/main # Ensure we are tracking the remote branch.

    dolt branch
    run dolt pull origin --prune
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Fast-forward" ]] || false

    run dolt branch -a
    [ "$status" -eq 0 ]
    [[ "$output" =~ "origin/main" ]] || false
    # Verify that the remote branch was deleted.
    [[ ! "$output" =~ "origin/new-branch" ]] || false
}
