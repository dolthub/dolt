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

@test "pull: --ff-only succeeds when fast-forward is possible" {
    cd repo2
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]  # Only header, no tables

    run dolt pull --ff-only origin
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Fast-forward" ]] || false

    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "t1" ]] || false
}

@test "pull: --ff-only fails when fast-forward is not possible" {
    cd repo2

    # Make a local commit that diverges from remote
    dolt sql -q "create table divergent (id int primary key);"
    dolt commit -Am "local divergent commit"
    
    run dolt pull --ff-only origin
    [ "$status" -eq 1 ]
    [[ "$output" =~ "fatal: Not possible to fast-forward, aborting" ]] || false
}

@test "pull: --ff-only conflicts with --no-ff" {
    cd repo2
    run dolt pull --ff-only --no-ff origin
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Flags '--ff-only' and '--no-ff' cannot be used together" ]] || false
}

@test "pull: --ff-only conflicts with --squash" {
    cd repo2
    run dolt pull --ff-only --squash origin  
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Flags '--ff-only' and '--squash' cannot be used together" ]] || false
}

@test "pull: --ff-only with already up-to-date branch" {
    cd repo2
    # First pull normally to get up to date
    dolt pull origin
    
    # Now pull again with --ff-only - should succeed with "up-to-date" message
    run dolt pull --ff-only origin
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Everything up-to-date" ]] || false
}

@test "pull: --ff-only works with --no-commit" {
    cd repo2

    run dolt pull --ff-only --no-commit origin
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Fast-forward" ]] || false
    
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "t1" ]] || false
}

@test "pull: pull --rebase with divergent history produces linear history" {
    skip_if_remote
    cd repo2
    dolt pull origin

    # Make a commit on repo1 and push
    cd ../repo1
    dolt sql -q "insert into t1 values (1, 1)"
    dolt commit -am "remote commit"
    dolt push origin main

    # Make a different commit on repo2 (divergent)
    cd ../repo2
    dolt sql -q "insert into t1 values (2, 2)"
    dolt commit -am "local commit"

    run dolt pull --rebase origin
    [ "$status" -eq 0 ]

    # Verify linear history (no merge commits)
    run dolt log --oneline
    [ "$status" -eq 0 ]
    [[ "$output" =~ "local commit" ]] || false
    [[ "$output" =~ "remote commit" ]] || false
    ! [[ "$output" =~ "Merge" ]] || false

    # Verify the data is correct
    run dolt sql -q "select * from t1 order by a" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "0,0" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
}

@test "pull: pull --rebase with no local commits fast-forwards" {
    skip_if_remote
    cd repo2

    run dolt pull --rebase origin
    [ "$status" -eq 0 ]

    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "t1" ]] || false

    run dolt sql -q "select * from t1" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "0,0" ]] || false

    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "First commit" ]] || false
    [[ "$output" =~ "Second commit" ]] || false
}

@test "pull: pull --rebase when already up-to-date" {
    skip_if_remote
    cd repo2
    dolt pull origin

    run dolt pull --rebase origin
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Everything up-to-date" ]] || false
}

@test "pull: pull --rebase with data conflict pauses rebase" {
    skip_if_remote
    cd repo2
    dolt pull origin

    # Make a conflicting commit on repo1 and push
    cd ../repo1
    dolt sql -q "insert into t1 values (1, 1)"
    dolt commit -am "remote commit"
    dolt push origin main

    # Make a conflicting commit on repo2 (same pk, different value)
    cd ../repo2
    dolt sql -q "insert into t1 values (1, 99)"
    dolt commit -am "local conflicting commit"

    # Pull with rebase should fail with conflict
    run dolt pull --rebase origin
    [ "$status" -eq 1 ]
    [[ "$output" =~ "data conflict detected while rebasing commit" ]] || false

    # Resolve conflicts
    dolt conflicts resolve --theirs t1
    dolt add t1

    # Continue the rebase
    run dolt rebase --continue
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully rebased" ]] || false

    # Verify linear history
    run dolt log --oneline
    [ "$status" -eq 0 ]
    [[ "$output" =~ "local conflicting commit" ]] || false
    [[ "$output" =~ "remote commit" ]] || false
    ! [[ "$output" =~ "Merge" ]] || false
}

@test "pull: pull --rebase conflicts with --squash" {
    cd repo2
    run dolt pull --rebase --squash origin
    [ "$status" -eq 1 ]
    [[ "$output" =~ "cannot be used together" ]] || false
}

@test "pull: pull --rebase conflicts with --no-ff" {
    cd repo2
    run dolt pull --rebase --no-ff origin
    [ "$status" -eq 1 ]
    [[ "$output" =~ "cannot be used together" ]] || false
}

@test "pull: pull --rebase conflicts with --ff-only" {
    cd repo2
    run dolt pull --rebase --ff-only origin
    [ "$status" -eq 1 ]
    [[ "$output" =~ "cannot be used together" ]] || false
}

@test "pull: pull --rebase conflicts with --no-commit" {
    cd repo2
    run dolt pull --rebase --no-commit origin
    [ "$status" -eq 1 ]
    [[ "$output" =~ "cannot be used together" ]] || false
}

@test "pull: pull -r shorthand works" {
    skip_if_remote
    cd repo2
    dolt pull origin

    # Make a commit on repo1 and push
    cd ../repo1
    dolt sql -q "insert into t1 values (1, 1)"
    dolt commit -am "remote commit"
    dolt push origin main

    # Make a different commit on repo2 (divergent)
    cd ../repo2
    dolt sql -q "insert into t1 values (2, 2)"
    dolt commit -am "local commit"

    run dolt pull -r origin
    [ "$status" -eq 0 ]

    # Verify linear history (no merge commits)
    run dolt log --oneline
    [ "$status" -eq 0 ]
    [[ "$output" =~ "local commit" ]] || false
    [[ "$output" =~ "remote commit" ]] || false
    ! [[ "$output" =~ "Merge" ]] || false
}

@test "pull: pull --rebase with multiple local commits preserves order" {
    skip_if_remote
    cd repo2
    dolt pull origin

    # Make a commit on repo1 and push
    cd ../repo1
    dolt sql -q "insert into t1 values (10, 10)"
    dolt commit -am "remote commit"
    dolt push origin main

    # Make 3 local commits on repo2
    cd ../repo2
    dolt sql -q "insert into t1 values (3, 3)"
    dolt commit -am "local commit 1"
    dolt sql -q "insert into t1 values (4, 4)"
    dolt commit -am "local commit 2"
    dolt sql -q "insert into t1 values (5, 5)"
    dolt commit -am "local commit 3"

    run dolt pull --rebase origin
    [ "$status" -eq 0 ]

    # Verify linear history (no merge commits)
    run dolt log --oneline
    [ "$status" -eq 0 ]
    [[ "$output" =~ "local commit 3" ]] || false
    [[ "$output" =~ "local commit 2" ]] || false
    [[ "$output" =~ "local commit 1" ]] || false
    [[ "$output" =~ "remote commit" ]] || false
    ! [[ "$output" =~ "Merge" ]] || false

    # Verify commit ordering: local commit 3 on top
    run dolt show HEAD
    [ "$status" -eq 0 ]
    [[ "$output" =~ "local commit 3" ]] || false

    run dolt show HEAD~1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "local commit 2" ]] || false

    run dolt show HEAD~2
    [ "$status" -eq 0 ]
    [[ "$output" =~ "local commit 1" ]] || false

    run dolt show HEAD~3
    [ "$status" -eq 0 ]
    [[ "$output" =~ "remote commit" ]] || false

    # Verify all data present
    run dolt sql -q "select * from t1 order by a" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "0,0" ]] || false
    [[ "$output" =~ "3,3" ]] || false
    [[ "$output" =~ "4,4" ]] || false
    [[ "$output" =~ "5,5" ]] || false
    [[ "$output" =~ "10,10" ]] || false
}

@test "pull: pull --rebase conflict resolved with --theirs verifies data" {
    skip_if_remote
    cd repo2
    dolt pull origin

    # Make a conflicting commit on repo1 and push
    cd ../repo1
    dolt sql -q "insert into t1 values (1, 1)"
    dolt commit -am "remote commit"
    dolt push origin main

    # Make a conflicting commit on repo2 (same pk, different value)
    cd ../repo2
    dolt sql -q "insert into t1 values (1, 99)"
    dolt commit -am "local conflicting commit"

    # Pull with rebase should fail with conflict
    run dolt pull --rebase origin
    [ "$status" -eq 1 ]
    [[ "$output" =~ "data conflict detected while rebasing commit" ]] || false

    # Resolve conflicts with --theirs (in rebase, theirs = the local commit being replayed)
    dolt conflicts resolve --theirs t1
    dolt add t1

    # Continue the rebase
    run dolt rebase --continue
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully rebased" ]] || false

    # Verify the data: --theirs keeps the local commit's value (1,99)
    run dolt sql -q "select * from t1 order by a" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "0,0" ]] || false
    [[ "$output" =~ "1,99" ]] || false
}

@test "pull: pull --rebase conflict resolved with --ours keeps upstream data" {
    skip_if_remote
    cd repo2
    dolt pull origin

    # Make a conflicting commit on repo1 and push
    cd ../repo1
    dolt sql -q "insert into t1 values (1, 1)"
    dolt commit -am "remote commit"
    dolt push origin main

    # Make a conflicting commit on repo2 (same pk, different value)
    cd ../repo2
    dolt sql -q "insert into t1 values (1, 99)"
    dolt commit -am "local conflicting commit"

    # Pull with rebase should fail with conflict
    run dolt pull --rebase origin
    [ "$status" -eq 1 ]
    [[ "$output" =~ "data conflict detected while rebasing commit" ]] || false

    # Resolve conflicts with --ours (in rebase, ours = upstream/remote base)
    dolt conflicts resolve --ours t1
    dolt add t1

    # Continue the rebase
    run dolt rebase --continue
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully rebased" ]] || false

    # Verify the data: --ours keeps the upstream (remote) value (1,1)
    run dolt sql -q "select b from t1 where a = 1" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" = "1" ]] || false

    # Verify linear history
    run dolt log --oneline
    [ "$status" -eq 0 ]
    ! [[ "$output" =~ "Merge" ]] || false
}

@test "pull: pull --rebase with multiple conflicts across multiple commits" {
    skip_if_remote
    cd repo2
    dolt pull origin

    # Make conflicting commits on repo1 and push
    cd ../repo1
    dolt sql -q "insert into t1 values (1, 1)"
    dolt sql -q "insert into t1 values (2, 2)"
    dolt commit -am "remote commit with rows 1 and 2"
    dolt push origin main

    # Make 2 local commits on repo2 that will each conflict
    cd ../repo2
    dolt sql -q "insert into t1 values (1, 99)"
    dolt commit -am "local commit 1 conflicts on pk 1"
    dolt sql -q "insert into t1 values (2, 99)"
    dolt commit -am "local commit 2 conflicts on pk 2"

    # Pull with rebase should fail on first conflict
    run dolt pull --rebase origin
    [ "$status" -eq 1 ]
    [[ "$output" =~ "data conflict detected while rebasing commit" ]] || false
    [[ "$output" =~ "local commit 1" ]] || false

    # Resolve first conflict
    dolt conflicts resolve --theirs t1
    dolt add t1

    # Continue rebase, should hit second conflict
    run dolt rebase --continue
    [ "$status" -eq 1 ]
    [[ "$output" =~ "data conflict detected while rebasing commit" ]] || false
    [[ "$output" =~ "local commit 2" ]] || false

    # Resolve second conflict
    dolt conflicts resolve --theirs t1
    dolt add t1

    # Continue rebase, should succeed
    run dolt rebase --continue
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully rebased" ]] || false

    # Verify linear history
    run dolt log --oneline
    [ "$status" -eq 0 ]
    [[ "$output" =~ "local commit 2" ]] || false
    [[ "$output" =~ "local commit 1" ]] || false
    [[ "$output" =~ "remote commit" ]] || false
    ! [[ "$output" =~ "Merge" ]] || false

    # Verify the rebase working branch was cleaned up
    run dolt branch
    [ "$status" -eq 0 ]
    ! [[ "$output" =~ "dolt_rebase_main" ]] || false
}

@test "pull: pull --rebase with schema conflict auto-aborts" {
    skip_if_remote
    cd repo2
    dolt pull origin

    # Make a schema change on repo1: modify column b to varchar
    cd ../repo1
    dolt sql -q "ALTER TABLE t1 MODIFY COLUMN b varchar(100)"
    dolt commit -am "remote schema change: modify b to varchar"
    dolt push origin main

    # Make a conflicting schema change on repo2: modify column b to bigint
    cd ../repo2
    dolt sql -q "ALTER TABLE t1 MODIFY COLUMN b bigint"
    dolt commit -am "local schema change: modify b to bigint"

    # Pull with rebase should fail with schema conflict and auto-abort
    run dolt pull --rebase origin
    [ "$status" -eq 1 ]
    [[ "$output" =~ "schema conflict detected while rebasing commit" ]] || false
    [[ "$output" =~ "the rebase has been automatically aborted" ]] || false

    # Verify no rebase is in progress
    run dolt rebase --continue
    [ "$status" -eq 1 ]
    [[ "$output" =~ "no rebase in progress" ]] || false

    # Verify the rebase working branch was cleaned up
    run dolt branch
    [ "$status" -eq 0 ]
    ! [[ "$output" =~ "dolt_rebase_main" ]] || false
}

@test "pull: pull --rebase with uncommitted changes fails" {
    skip_if_remote
    cd repo2
    dolt pull origin

    # Make a commit on repo1 and push to create divergence
    cd ../repo1
    dolt sql -q "insert into t1 values (3, 3)"
    dolt commit -am "remote commit"
    dolt push origin main

    # Make a committed divergent change and an uncommitted change on repo2
    cd ../repo2
    dolt sql -q "insert into t1 values (4, 4)"
    dolt commit -am "local committed change"
    dolt sql -q "insert into t1 values (5, 5)"

    run dolt pull --rebase origin
    [ "$status" -eq 1 ]
    [[ "$output" =~ "cannot start a rebase with uncommitted changes" ]] || false

    # Verify working set is unchanged
    run dolt sql -q "select * from t1 where a = 5" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "5,5" ]] || false
}

@test "pull: pull --rebase when local is ahead returns up-to-date" {
    skip_if_remote
    cd repo2
    dolt pull origin

    # Make a local commit but do NOT push
    dolt sql -q "insert into t1 values (7, 7)"
    dolt commit -am "local only commit"

    run dolt pull --rebase origin
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Everything up-to-date" ]] || false

    # Verify local commit is still there
    run dolt log --oneline
    [ "$status" -eq 0 ]
    [[ "$output" =~ "local only commit" ]] || false
}

@test "pull: pull --rebase with multiple remote commits and one local" {
    skip_if_remote
    cd repo2
    dolt pull origin

    # Make 3 commits on repo1 and push
    cd ../repo1
    dolt sql -q "insert into t1 values (10, 10)"
    dolt commit -am "remote commit 1"
    dolt sql -q "insert into t1 values (11, 11)"
    dolt commit -am "remote commit 2"
    dolt sql -q "insert into t1 values (12, 12)"
    dolt commit -am "remote commit 3"
    dolt push origin main

    # Make 1 local commit on repo2
    cd ../repo2
    dolt sql -q "insert into t1 values (20, 20)"
    dolt commit -am "local commit"

    run dolt pull --rebase origin
    [ "$status" -eq 0 ]

    # Verify linear history: local commit on top of all 3 remote commits
    run dolt log --oneline
    [ "$status" -eq 0 ]
    [[ "$output" =~ "local commit" ]] || false
    [[ "$output" =~ "remote commit 3" ]] || false
    [[ "$output" =~ "remote commit 2" ]] || false
    [[ "$output" =~ "remote commit 1" ]] || false
    ! [[ "$output" =~ "Merge" ]] || false

    # Verify local commit is most recent
    run dolt show HEAD
    [ "$status" -eq 0 ]
    [[ "$output" =~ "local commit" ]] || false

    # Verify all data correct
    run dolt sql -q "select * from t1 order by a" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "0,0" ]] || false
    [[ "$output" =~ "10,10" ]] || false
    [[ "$output" =~ "11,11" ]] || false
    [[ "$output" =~ "12,12" ]] || false
    [[ "$output" =~ "20,20" ]] || false
}

@test "pull: pull --rebase with non-conflicting new tables on both sides" {
    skip_if_remote
    cd repo2
    dolt pull origin

    # Make a new table on repo1 and push
    cd ../repo1
    dolt sql -q "create table t2 (a int primary key, b int)"
    dolt sql -q "insert into t2 values (1, 1)"
    dolt commit -Am "remote: create table t2"
    dolt push origin main

    # Make a different new table on repo2
    cd ../repo2
    dolt sql -q "create table t3 (a int primary key, b int)"
    dolt sql -q "insert into t3 values (1, 1)"
    dolt commit -Am "local: create table t3"

    run dolt pull --rebase origin
    [ "$status" -eq 0 ]

    # Verify both tables exist
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "t1" ]] || false
    [[ "$output" =~ "t2" ]] || false
    [[ "$output" =~ "t3" ]] || false

    # Verify linear history
    run dolt log --oneline
    [ "$status" -eq 0 ]
    [[ "$output" =~ "local: create table t3" ]] || false
    [[ "$output" =~ "remote: create table t2" ]] || false
    ! [[ "$output" =~ "Merge" ]] || false
}

@test "pull: pull --rebase abort then retry" {
    skip_if_remote
    cd repo2
    dolt pull origin

    # Create conflict scenario
    cd ../repo1
    dolt sql -q "insert into t1 values (1, 1)"
    dolt commit -am "remote commit"
    dolt push origin main

    cd ../repo2
    dolt sql -q "insert into t1 values (1, 99)"
    dolt commit -am "local conflicting commit"

    # Pull with rebase hits conflict
    run dolt pull --rebase origin
    [ "$status" -eq 1 ]
    [[ "$output" =~ "data conflict detected while rebasing commit" ]] || false

    # Abort the rebase
    run dolt rebase --abort
    [ "$status" -eq 0 ]

    # Verify clean state, back on main
    run dolt branch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "* main" ]] || false
    ! [[ "$output" =~ "dolt_rebase_main" ]] || false

    # Retry pull --rebase, still conflicts
    run dolt pull --rebase origin
    [ "$status" -eq 1 ]
    [[ "$output" =~ "data conflict detected while rebasing commit" ]] || false

    # This time resolve and continue
    dolt conflicts resolve --theirs t1
    dolt add t1
    run dolt rebase --continue
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully rebased" ]] || false

    # Verify linear history
    run dolt log --oneline
    [ "$status" -eq 0 ]
    ! [[ "$output" =~ "Merge" ]] || false

    # Verify rebase branch cleaned up
    run dolt branch
    [ "$status" -eq 0 ]
    ! [[ "$output" =~ "dolt_rebase_main" ]] || false
}

@test "pull: pull --rebase fails when dolt_rebase_main branch already exists" {
    skip_if_remote
    cd repo2
    dolt pull origin

    # Manually create the rebase working branch
    dolt branch dolt_rebase_main

    # Create divergent history
    cd ../repo1
    dolt sql -q "insert into t1 values (1, 1)"
    dolt commit -am "remote commit"
    dolt push origin main

    cd ../repo2
    dolt sql -q "insert into t1 values (2, 2)"
    dolt commit -am "local commit"

    run dolt pull --rebase origin
    [ "$status" -eq 1 ]
    [[ "$output" =~ "A branch named 'dolt_rebase_main' already exists" ]] || false

    # Verify original branch is still intact
    run dolt log --oneline
    [ "$status" -eq 0 ]
    [[ "$output" =~ "local commit" ]] || false
}

@test "pull: pull --rebase shows conflicts during paused rebase" {
    skip_if_remote
    cd repo2
    dolt pull origin

    # Create conflict scenario
    cd ../repo1
    dolt sql -q "insert into t1 values (1, 1)"
    dolt commit -am "remote commit"
    dolt push origin main

    cd ../repo2
    dolt sql -q "insert into t1 values (1, 99)"
    dolt commit -am "local conflicting commit"

    # Pull with rebase should pause on conflict
    run dolt pull --rebase origin
    [ "$status" -eq 1 ]
    [[ "$output" =~ "data conflict detected while rebasing commit" ]] || false

    # Verify we are on the rebase working branch
    run dolt sql -q "select active_branch()" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "dolt_rebase_main" ]] || false

    # Verify dolt_conflicts shows the conflict
    run dolt sql -q "select * from dolt_conflicts" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "t1" ]] || false

    # Verify dolt conflicts cat shows conflict details
    run dolt conflicts cat .
    [ "$status" -eq 0 ]
    [[ "$output" =~ "ours" ]] || false
    [[ "$output" =~ "theirs" ]] || false

    # Clean up: abort the rebase
    run dolt rebase --abort
    [ "$status" -eq 0 ]

    # Verify back on main
    run dolt branch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "* main" ]] || false
    ! [[ "$output" =~ "dolt_rebase_main" ]] || false
}

@test "pull: pull --rebase with explicit remote and branch args" {
    skip_if_remote
    cd repo2
    dolt pull origin main

    # Make a commit on repo1 and push
    cd ../repo1
    dolt sql -q "insert into t1 values (10, 10)"
    dolt commit -am "remote commit"
    dolt push origin main

    # Make a local commit on repo2
    cd ../repo2
    dolt sql -q "insert into t1 values (20, 20)"
    dolt commit -am "local commit"

    # Use explicit remote and branch args
    run dolt pull --rebase origin main
    [ "$status" -eq 0 ]

    # Verify linear history
    run dolt log --oneline
    [ "$status" -eq 0 ]
    [[ "$output" =~ "local commit" ]] || false
    [[ "$output" =~ "remote commit" ]] || false
    ! [[ "$output" =~ "Merge" ]] || false

    # Verify all data
    run dolt sql -q "select * from t1 order by a" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "0,0" ]] || false
    [[ "$output" =~ "10,10" ]] || false
    [[ "$output" =~ "20,20" ]] || false
}

@test "pull: pull --rebase is unsupported against a running server" {
    if [ "$SQL_ENGINE" != "remote-engine" ]; then
        skip "test only applicable in remote-engine mode"
    fi
    cd repo2
    dolt pull origin

    setup_remote_server

    run dolt pull --rebase origin
    [ "$status" -eq 1 ]
    [[ "$output" =~ "can not currently be used when there is a local server running" ]] || false
}
