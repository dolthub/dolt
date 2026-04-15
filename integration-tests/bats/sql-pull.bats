#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common

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
    dolt log
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

@test "sql-pull: dolt_pull main" {
    cd repo2
    dolt sql -q "call dolt_pull('origin')"
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-pull: dolt_pull custom remote" {
    cd repo2
    dolt sql -q "call dolt_pull('test-remote')"
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-pull: dolt_pull default origin" {
    cd repo2
    dolt remote remove test-remote
    dolt sql -q "call dolt_pull()"
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-pull: dolt_pull default custom remote" {
    cd repo2
    dolt remote remove origin
    dolt sql -q "call dolt_pull()"
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-pull: dolt_pull up to date does not error" {
    cd repo2
    dolt sql -q "call dolt_pull('origin')"
    dolt sql -q "call dolt_pull('origin')"
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-pull: dolt_pull unknown remote fails" {
    cd repo2
    run dolt sql -q "call dolt_pull('unknown')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "fatal: remote 'unknown' not found" ]] || false
    [[ ! "$output" =~ "panic" ]] || false
}

@test "sql-pull: dolt_pull unknown feature branch fails" {
    cd repo2
    dolt checkout feature
    run dolt sql -q "call dolt_pull('origin')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "You asked to pull from the remote 'origin', but did not specify a branch" ]] || false
    [[ ! "$output" =~ "panic" ]] || false
}

@test "sql-pull: dolt_pull feature branch" {
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
    dolt sql -q "call dolt_pull('origin')"
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-pull: dolt_checkout after dolt_fetch a new feature branch" {
    cd repo1
    dolt checkout -b feature2
    dolt sql -q "create table t2 (i int primary key);"
    dolt sql -q "call dolt_add('.');"
    dolt sql -q "call dolt_commit('-am', 'create t2')"
    dolt push --set-upstream origin feature2

    cd ../repo2
    dolt sql -q "CALL dolt_fetch('origin', 'feature2')"
    run dolt sql -q "call dolt_checkout('feature2'); show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
    [[ "$output" =~ "t2" ]] || false
}

@test "sql-pull: dolt_pull force" {
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
    dolt pull
    dolt sql -q "alter table objects add constraint color FOREIGN KEY (color) REFERENCES colors(color)"
    dolt commit -A -m "Commit2"

    cd ../repo1
    dolt sql -q "INSERT INTO objects (id,name,color) VALUES (1,'truck','red'),(2,'ball','green'),(3,'shoe','blue')"
    dolt commit -A -m "Commit3"
    dolt push origin main

    cd ../repo2
    run dolt sql -q "call dolt_pull()"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Constraint violations" ]] || false

    run dolt sql -q "call dolt_pull('--force')"
    [ "$status" -eq 0 ]
    run dolt sql -q "select * from objects"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "truck" ]] || false
    [[ "$output" =~ "ball" ]] || false
    [[ "$output" =~ "shoe" ]] || false
}

@test "sql-pull: dolt_pull squash" {
    cd repo2
    dolt sql -q "create table t2 (i int primary key);"
    dolt commit -Am "commit 1"

    dolt sql -q "CALL dolt_pull('--squash', 'origin')"
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

@test "sql-pull: dolt_pull --noff flag" {
    cd repo2
    dolt sql -q "CALL dolt_pull('--no-ff', 'origin')"
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

@test "sql-pull: empty remote name does not panic" {
    cd repo2
    dolt sql -q "call dolt_pull('')"
}

@test "sql-pull: dolt_pull dirty working set fails" {
    cd repo2
    dolt sql -q "create table t2 (a int)"
    run dolt sql -q "call dolt_pull('origin')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "cannot merge with uncommitted changes" ]] || false
}

@test "sql-pull: dolt_pull tag" {
    cd repo1
    dolt tag v1
    dolt push origin v1
    dolt tag

    cd ../repo2
    dolt sql -q "call dolt_pull('origin')"
    run dolt tag
    [ "$status" -eq 0 ]
    [[ "$output" =~ "v1" ]] || false
}

@test "sql-pull: dolt_pull tags only for resolved commits" {
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
    dolt sql -q "call dolt_pull('origin')"
    run dolt tag
    [ "$status" -eq 0 ]
    [[ "$output" =~ "v1" ]] || false
    [[ "$output" =~ "v2" ]] || false
    [[ ! "$output" =~ "v3" ]] || false
}

@test "sql-pull: dolt_pull with remote and remote ref" {
    cd repo1
    dolt checkout feature
    dolt checkout -b newbranch
    run dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "t1" ]] || false

    # Specifying a non-existent remote branch returns an error
    run dolt sql -q "call dolt_pull('origin', 'doesnotexist');"
    [ "$status" -eq 1 ]
    [[ "$output" =~ 'branch "doesnotexist" not found on remote' ]] || false

    # Explicitly specifying the remote and branch will merge in that branch
    run dolt sql -q "call dolt_pull('origin', 'main');"
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
    run dolt sql -q "call dolt_pull('origin', 'main');"
    [ "$status" -eq 1 ]
    [[ "$output" =~ 'cannot merge with uncommitted changes' ]] || false

    # Commit changes and test that a merge conflict fails the pull
    dolt commit -am "adding new t1 table"
    run dolt sql -q "call dolt_pull('origin', 'main');"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "| fast_forward | conflicts |" ]] || false
    [[ "$output" =~ "| 0            | 1         |" ]] || false
}

@test "sql-pull: dolt_pull also fetches, but does not merge other branches" {
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
    dolt sql -q "call dolt_pull()"
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

@test "sql-pull: dolt_pull commits successful merge on current branch" {
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
    run dolt sql -q "call dolt_pull()"
    [ "$status" -eq 0 ]

    run dolt log --oneline -n 1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Merge branch 'other' of" ]] || false
    [[ ! "$output" =~ "add (1,2) to t1" ]] || false
    [[ ! "$output" =~ "add (2,3) to t1" ]] || false
}

@test "sql-pull: --no-ff and --no-commit" {
    cd repo2
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]

    dolt sql -q "call dolt_pull('--no-ff', '--no-commit')"
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "t1" ]] || false

    dolt commit -m "merge from origin"
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "merge from origin" ]] || false
}

@test "sql-pull: pull two different branches in the same session" {
    cd repo2
    dolt pull
    
    dolt sql <<SQL
    call dolt_checkout('main');
    insert into t1 values (1,1), (2,2);
    call dolt_commit('-Am', 'new rows in t1');
    call dolt_checkout('-b', 'b1');
    insert into t1 values (3,3);
    call dolt_commit('-Am', 'new row on b1');
SQL

    dolt push origin main
    dolt checkout b1
    dolt push origin b1
    dolt checkout main

    cd ../repo1
    dolt pull origin main
    dolt checkout b1
    dolt pull origin b1

    cd ../repo2
    dolt sql <<SQL
    call dolt_checkout('main');
    insert into t1 values (4,4);
    call dolt_commit('-Am', 'new row in t1');
    call dolt_checkout('b1');
    insert into t1 values (5,5);
    call dolt_commit('-Am', 'new row on b1');
SQL

    dolt push origin main
    dolt checkout b1
    dolt push origin b1
    dolt checkout main
    
    cd ../repo1

    dolt sql <<SQL
    call dolt_checkout('main');
    insert into t1 values (6,6);
    call dolt_commit('-Am', 'new row in t1');
    call dolt_checkout('b1');
    insert into t1 values (7,7);
    call dolt_commit('-Am', 'new row on b1');
SQL

    # Now pull from both branches and make sure we can commit the result in a single tx
    dolt sql <<SQL
    set autocommit = 0;
    call dolt_checkout('main');
    call dolt_pull('origin', 'main');
    call dolt_checkout('b1');
    call dolt_pull('origin', 'b1');
    commit;
SQL
}

@test "sql-pull: pull two different branches same session, already up to date" {
    cd repo2
    dolt pull
    
    dolt sql <<SQL
    call dolt_checkout('main');
    insert into t1 values (1,1), (2,2);
    call dolt_commit('-Am', 'new rows in t1');
    call dolt_checkout('-b', 'b1');
    insert into t1 values (3,3);
    call dolt_commit('-Am', 'new row on b1');
SQL

    dolt push origin main
    dolt checkout b1
    dolt push origin b1
    dolt checkout main

    cd ../repo1
    dolt pull origin main
    dolt checkout b1
    dolt pull origin b1

    # Make sure we can commit the result after a no-op pull on two branches
    dolt sql <<SQL
    set autocommit=off;
    call dolt_checkout('main');
    call dolt_pull('origin', 'main');
    call dolt_checkout('b1');
    call dolt_pull('origin', 'b1');
    commit;
SQL
}

@test "sql-pull: dolt_pull with --rebase and divergent history" {
    cd repo2
    dolt sql -q "call dolt_pull('origin')"

    # Make a commit on repo1 and push
    cd ../repo1
    dolt sql -q "insert into t1 values (1, 1)"
    dolt commit -am "remote commit"
    dolt push origin main

    # Make a different commit on repo2 (divergent)
    cd ../repo2
    dolt sql -q "insert into t1 values (2, 2)"
    dolt commit -am "local commit"

    dolt sql -q "CALL dolt_pull('--rebase')"

    # Verify linear history via SQL (no merge commits)
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

@test "sql-pull: dolt_pull with --rebase fast-forwards" {
    cd repo2

    dolt sql -q "CALL dolt_pull('--rebase')"

    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "t1" ]] || false

    run dolt sql -q "select * from t1" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "0,0" ]] || false
}

@test "sql-pull: dolt_pull with --rebase already up-to-date" {
    cd repo2
    dolt sql -q "call dolt_pull('origin')"

    run dolt sql -q "CALL dolt_pull('--rebase')"
    [ "$status" -eq 0 ]
}

@test "sql-pull: dolt_pull with --rebase conflicting flags" {
    cd repo2
    run dolt sql -q "CALL dolt_pull('--rebase', '--squash')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "cannot be used together" ]] || false

    run dolt sql -q "CALL dolt_pull('--rebase', '--no-ff')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "cannot be used together" ]] || false

    run dolt sql -q "CALL dolt_pull('--rebase', '--ff-only')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "cannot be used together" ]] || false

    run dolt sql -q "CALL dolt_pull('--rebase', '--no-commit')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "cannot be used together" ]] || false
}

@test "sql-pull: dolt_pull with --rebase conflict auto-aborts without allow_commit_conflicts" {
    cd repo2
    dolt sql -q "call dolt_pull('origin')"

    # Make a conflicting commit on repo1 and push
    cd ../repo1
    dolt sql -q "insert into t1 values (1, 1)"
    dolt commit -am "remote commit"
    dolt push origin main

    # Make a conflicting commit on repo2 (same pk, different value)
    cd ../repo2
    dolt sql -q "insert into t1 values (1, 99)"
    dolt commit -am "local conflicting commit"

    # Without @@dolt_allow_commit_conflicts, the rebase auto-aborts on conflict
    run dolt sql -q "CALL dolt_pull('--rebase')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "data conflicts from rebase" ]] || false
    [[ "$output" =~ "rebase has been aborted" ]] || false

    # Verify no rebase is in progress (it was auto-aborted)
    run dolt sql -q "CALL dolt_rebase('--continue')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "no rebase in progress" ]] || false

    # Verify we are still on main with our local commit intact
    run dolt sql -q "select active_branch()" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "main" ]] || false

    run dolt log --oneline
    [ "$status" -eq 0 ]
    [[ "$output" =~ "local conflicting commit" ]] || false
}

@test "sql-pull: dolt_pull with --rebase multiple local commits" {
    cd repo2
    dolt sql -q "call dolt_pull('origin')"

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

    dolt sql -q "CALL dolt_pull('--rebase')"

    # Verify linear history
    run dolt log --oneline
    [ "$status" -eq 0 ]
    [[ "$output" =~ "local commit 3" ]] || false
    [[ "$output" =~ "local commit 2" ]] || false
    [[ "$output" =~ "local commit 1" ]] || false
    [[ "$output" =~ "remote commit" ]] || false
    ! [[ "$output" =~ "Merge" ]] || false

    # Verify all data present
    run dolt sql -q "select * from t1 order by a" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "0,0" ]] || false
    [[ "$output" =~ "3,3" ]] || false
    [[ "$output" =~ "4,4" ]] || false
    [[ "$output" =~ "5,5" ]] || false
    [[ "$output" =~ "10,10" ]] || false
}

@test "sql-pull: dolt_pull with --rebase drops commit that becomes empty" {
    cd repo2
    dolt sql -q "call dolt_pull('origin')"

    # Make a commit on repo1 inserting (5,5) and push
    cd ../repo1
    dolt sql -q "insert into t1 values (5, 5)"
    dolt commit -am "remote: insert 5,5"
    dolt push origin main

    # Make an identical commit on repo2 (same insert)
    cd ../repo2
    dolt sql -q "insert into t1 values (5, 5)"
    dolt commit -am "local: insert 5,5"

    dolt sql -q "CALL dolt_pull('--rebase')"

    # Verify success and the local commit that became empty is dropped
    run dolt log --oneline
    [ "$status" -eq 0 ]
    [[ "$output" =~ "remote: insert 5,5" ]] || false
    ! [[ "$output" =~ "local: insert 5,5" ]] || false

    # Verify data is correct
    run dolt sql -q "select * from t1 order by a" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "0,0" ]] || false
    [[ "$output" =~ "5,5" ]] || false
}

@test "sql-pull: dolt_pull with --rebase conflict resolved with --theirs" {
    cd repo2
    dolt sql -q "call dolt_pull('origin')"

    # Make a conflicting commit on repo1 and push
    cd ../repo1
    dolt sql -q "insert into t1 values (1, 1)"
    dolt commit -am "remote commit"
    dolt push origin main

    # Make a conflicting commit on repo2 (same pk, different value)
    cd ../repo2
    dolt sql -q "insert into t1 values (1, 99)"
    dolt commit -am "local conflicting commit"

    # Use --continue to keep session alive after conflict error
    run dolt sql --continue << SQL
SET @@dolt_allow_commit_conflicts = 1;
CALL dolt_pull('--rebase');
CALL dolt_conflicts_resolve('--theirs', 't1');
CALL dolt_add('t1');
CALL dolt_rebase('--continue');
SQL
    [ "$status" -eq 0 ]

    # In rebase, --theirs = local commit being replayed, so value should be 99
    run dolt sql -q "select b from t1 where a = 1" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "99" ]] || false

    # Verify linear history
    run dolt log --oneline
    [ "$status" -eq 0 ]
    [[ "$output" =~ "local conflicting commit" ]] || false
    ! [[ "$output" =~ "Merge" ]] || false
}

@test "sql-pull: dolt_pull with --rebase conflict resolved with --ours" {
    cd repo2
    dolt sql -q "call dolt_pull('origin')"

    # Make a conflicting commit on repo1 and push
    cd ../repo1
    dolt sql -q "insert into t1 values (1, 1)"
    dolt commit -am "remote commit"
    dolt push origin main

    # Make a conflicting commit on repo2 (same pk, different value)
    cd ../repo2
    dolt sql -q "insert into t1 values (1, 99)"
    dolt commit -am "local conflicting commit"

    # Use --continue; resolve with --ours (ours = upstream in rebase context)
    run dolt sql --continue << SQL
SET @@dolt_allow_commit_conflicts = 1;
CALL dolt_pull('--rebase');
CALL dolt_conflicts_resolve('--ours', 't1');
CALL dolt_add('t1');
CALL dolt_rebase('--continue');
SQL
    [ "$status" -eq 0 ]

    # In rebase, --ours = upstream (remote), so value should be 1
    run dolt sql -q "select b from t1 where a = 1" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1" ]] || false
    ! [[ "$output" =~ "99" ]] || false

    # Verify linear history
    run dolt log --oneline
    [ "$status" -eq 0 ]
    ! [[ "$output" =~ "Merge" ]] || false
}

@test "sql-pull: dolt_pull with --rebase multiple conflicts across commits" {
    cd repo2
    dolt sql -q "call dolt_pull('origin')"

    # Make conflicting rows on repo1 and push
    cd ../repo1
    dolt sql -q "insert into t1 values (1, 1)"
    dolt sql -q "insert into t1 values (2, 2)"
    dolt commit -am "remote commit"
    dolt push origin main

    # Make 2 conflicting local commits on repo2
    cd ../repo2
    dolt sql -q "insert into t1 values (1, 99)"
    dolt commit -am "local conflict 1"
    dolt sql -q "insert into t1 values (2, 99)"
    dolt commit -am "local conflict 2"

    # Use --continue to resolve both conflicts in sequence
    run dolt sql --continue << SQL
SET @@dolt_allow_commit_conflicts = 1;
CALL dolt_pull('--rebase');
CALL dolt_conflicts_resolve('--theirs', 't1');
CALL dolt_add('t1');
CALL dolt_rebase('--continue');
CALL dolt_conflicts_resolve('--theirs', 't1');
CALL dolt_add('t1');
CALL dolt_rebase('--continue');
SQL
    [ "$status" -eq 0 ]

    # Verify linear history with both local commits
    run dolt log --oneline
    [ "$status" -eq 0 ]
    [[ "$output" =~ "local conflict 2" ]] || false
    [[ "$output" =~ "local conflict 1" ]] || false
    [[ "$output" =~ "remote commit" ]] || false
    ! [[ "$output" =~ "Merge" ]] || false

    # Verify data -- theirs = local values
    run dolt sql -q "select * from t1 order by a" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,99" ]] || false
    [[ "$output" =~ "2,99" ]] || false

    # Verify rebase branch cleaned up
    run dolt branch
    [ "$status" -eq 0 ]
    ! [[ "$output" =~ "dolt_rebase_main" ]] || false
}

@test "sql-pull: dolt_pull with --rebase schema conflict auto-aborts" {
    cd repo2
    dolt sql -q "call dolt_pull('origin')"

    # Change column type on repo1 and push
    cd ../repo1
    dolt sql -q "ALTER TABLE t1 MODIFY COLUMN b varchar(100)"
    dolt commit -am "remote schema change"
    dolt push origin main

    # Change same column differently on repo2
    cd ../repo2
    dolt sql -q "ALTER TABLE t1 MODIFY COLUMN b bigint"
    dolt commit -am "local schema change"

    run dolt sql -q "CALL dolt_pull('--rebase')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "schema conflict" ]] || false
    [[ "$output" =~ "automatically aborted" ]] || false

    # Verify no rebase in progress
    run dolt sql -q "CALL dolt_rebase('--continue')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "no rebase in progress" ]] || false

    # Verify still on main
    run dolt sql -q "select active_branch()" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "main" ]] || false
}

@test "sql-pull: dolt_pull with --rebase uncommitted changes fails" {
    cd repo2
    dolt sql -q "call dolt_pull('origin')"

    # Push a new commit from repo1
    cd ../repo1
    dolt sql -q "insert into t1 values (10, 10)"
    dolt commit -am "remote commit"
    dolt push origin main

    # Make a committed change AND an uncommitted change on repo2
    cd ../repo2
    dolt sql -q "insert into t1 values (20, 20)"
    dolt commit -am "local committed"
    dolt sql -q "insert into t1 values (30, 30)"

    run dolt sql -q "CALL dolt_pull('--rebase')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "cannot start a rebase with uncommitted changes" ]] || false

    # Verify uncommitted data is still present
    run dolt sql -q "select * from t1 where a = 30" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "30,30" ]] || false
}

@test "sql-pull: dolt_pull with --rebase when local is ahead" {
    cd repo2
    dolt sql -q "call dolt_pull('origin')"

    # Make a local-only commit (no remote changes)
    dolt sql -q "insert into t1 values (7, 7)"
    dolt commit -am "local only commit"

    run dolt sql -q "CALL dolt_pull('--rebase')"
    [ "$status" -eq 0 ]

    # Verify local commit is still there
    run dolt log --oneline
    [ "$status" -eq 0 ]
    [[ "$output" =~ "local only commit" ]] || false
}

@test "sql-pull: dolt_pull with --rebase multiple remote commits and one local" {
    cd repo2
    dolt sql -q "call dolt_pull('origin')"

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

    dolt sql -q "CALL dolt_pull('--rebase')"

    # Verify linear history with local commit on top
    run dolt log --oneline
    [ "$status" -eq 0 ]
    [[ "$output" =~ "local commit" ]] || false
    [[ "$output" =~ "remote commit 3" ]] || false
    [[ "$output" =~ "remote commit 2" ]] || false
    [[ "$output" =~ "remote commit 1" ]] || false
    ! [[ "$output" =~ "Merge" ]] || false

    # Verify all data present
    run dolt sql -q "select * from t1 order by a" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "0,0" ]] || false
    [[ "$output" =~ "10,10" ]] || false
    [[ "$output" =~ "11,11" ]] || false
    [[ "$output" =~ "12,12" ]] || false
    [[ "$output" =~ "20,20" ]] || false
}

@test "sql-pull: dolt_pull with --rebase non-conflicting new tables" {
    cd repo2
    dolt sql -q "call dolt_pull('origin')"

    # Create t2 on repo1 and push
    cd ../repo1
    dolt sql -q "create table t2 (a int primary key, b int)"
    dolt sql -q "insert into t2 values (1, 1)"
    dolt add .
    dolt commit -am "remote: add t2"
    dolt push origin main

    # Create t3 on repo2
    cd ../repo2
    dolt sql -q "create table t3 (a int primary key, b int)"
    dolt sql -q "insert into t3 values (2, 2)"
    dolt add .
    dolt commit -am "local: add t3"

    dolt sql -q "CALL dolt_pull('--rebase')"

    # Verify all 3 tables exist
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "t1" ]] || false
    [[ "$output" =~ "t2" ]] || false
    [[ "$output" =~ "t3" ]] || false

    # Verify linear history
    run dolt log --oneline
    [ "$status" -eq 0 ]
    ! [[ "$output" =~ "Merge" ]] || false
}

@test "sql-pull: dolt_pull with --rebase abort then retry" {
    cd repo2
    dolt sql -q "call dolt_pull('origin')"

    # Make a conflicting commit on repo1 and push
    cd ../repo1
    dolt sql -q "insert into t1 values (1, 1)"
    dolt commit -am "remote commit"
    dolt push origin main

    # Make a conflicting commit on repo2
    cd ../repo2
    dolt sql -q "insert into t1 values (1, 99)"
    dolt commit -am "local conflicting commit"

    # First attempt: hit conflict and abort
    run dolt sql --continue << SQL
SET @@dolt_allow_commit_conflicts = 1;
CALL dolt_pull('--rebase');
CALL dolt_rebase('--abort');
SQL
    [ "$status" -eq 0 ]

    # Verify clean state after abort
    run dolt sql -q "select active_branch()" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "main" ]] || false

    run dolt branch
    [ "$status" -eq 0 ]
    ! [[ "$output" =~ "dolt_rebase_main" ]] || false

    # Second attempt: resolve and continue
    run dolt sql --continue << SQL
SET @@dolt_allow_commit_conflicts = 1;
CALL dolt_pull('--rebase');
CALL dolt_conflicts_resolve('--theirs', 't1');
CALL dolt_add('t1');
CALL dolt_rebase('--continue');
SQL
    [ "$status" -eq 0 ]

    # Verify success
    run dolt log --oneline
    [ "$status" -eq 0 ]
    [[ "$output" =~ "local conflicting commit" ]] || false
    ! [[ "$output" =~ "Merge" ]] || false

    run dolt branch
    [ "$status" -eq 0 ]
    ! [[ "$output" =~ "dolt_rebase_main" ]] || false
}

@test "sql-pull: dolt_pull with --rebase shows conflicts during paused rebase" {
    cd repo2
    dolt sql -q "call dolt_pull('origin')"

    # Make a conflicting commit on repo1 and push
    cd ../repo1
    dolt sql -q "insert into t1 values (1, 1)"
    dolt commit -am "remote commit"
    dolt push origin main

    # Make a conflicting commit on repo2
    cd ../repo2
    dolt sql -q "insert into t1 values (1, 99)"
    dolt commit -am "local conflicting commit"

    # Pull with rebase, inspect conflict state, then abort
    run dolt sql --continue -r csv << SQL
SET @@dolt_allow_commit_conflicts = 1;
CALL dolt_pull('--rebase');
SELECT active_branch();
SELECT * FROM dolt_conflicts;
CALL dolt_rebase('--abort');
SQL
    [ "$status" -eq 0 ]

    # Verify we were on dolt_rebase_main during the conflict
    [[ "$output" =~ "dolt_rebase_main" ]] || false

    # Verify dolt_conflicts showed our table
    [[ "$output" =~ "t1" ]] || false

    # Verify we're back on main after abort
    run dolt sql -q "select active_branch()" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "main" ]] || false
}

@test "sql-pull: dolt_pull with --rebase explicit remote and branch args" {
    cd repo2
    dolt sql -q "call dolt_pull('origin')"

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
    dolt sql -q "CALL dolt_pull('--rebase', 'origin', 'main')"

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

@test "sql-pull: dolt_pull with --rebase fails when dolt_rebase_main branch exists" {
    cd repo2
    dolt sql -q "call dolt_pull('origin')"

    # Make a commit on repo1 and push
    cd ../repo1
    dolt sql -q "insert into t1 values (10, 10)"
    dolt commit -am "remote commit"
    dolt push origin main

    # Make a local commit on repo2 and create blocking branch
    cd ../repo2
    dolt sql -q "insert into t1 values (20, 20)"
    dolt commit -am "local commit"
    dolt branch dolt_rebase_main

    run dolt sql -q "CALL dolt_pull('--rebase')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "dolt_rebase_main" ]] || false

    # Verify original data intact
    run dolt sql -q "select * from t1 order by a" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "0,0" ]] || false
    [[ "$output" =~ "20,20" ]] || false
}

# https://github.com/dolthub/dolt/issues/10839
@test "sql-pull: dolt_pull returns a corrective error when the branch tracking merge ref is unset" {
    cd repo1

    # Push to the remote without setting upstream tracking, matching the original issue repro.
    dolt sql -q "call dolt_push('origin', 'main')"

    # Write a tracking entry that has a remote but no merge ref, equivalent to running
    # git config branch.main.remote origin without a corresponding merge line.
    # The JSON "head" key is absent, so the Merge field has no ref when the file is loaded.
    sed -i 's/"branches": {}/"branches": {"main": {"remote": "origin"}}/' .dolt/repo_state.json

    run dolt sql -q "call dolt_pull('origin')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "there is no tracking information for the current branch" ]] || false
    [[ "$output" =~ "dolt push --set-upstream origin main" ]] || false

    run dolt pull origin
    [ "$status" -eq 1 ]
    [[ "$output" =~ "there is no tracking information for the current branch" ]] || false
    [[ "$output" =~ "dolt push --set-upstream origin main" ]] || false

    # repo1 is at the same commit as the remote here, so the push succeeds in setting
    # the upstream before repo2 advances the remote.
    dolt push --set-upstream origin main

    # Push a new commit from repo2 so repo1 has something to pull.
    cd ../repo2
    dolt sql -q "call dolt_pull('origin')"
    dolt sql -q "insert into t1 values (99, 99)"
    dolt commit -am "new commit for pull test"
    dolt push origin main

    cd ../repo1
    run dolt sql -q "call dolt_pull('origin')"
    [ "$status" -eq 0 ]
    run dolt sql -q "select * from t1 where a = 99" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "99,99" ]] || false
}
