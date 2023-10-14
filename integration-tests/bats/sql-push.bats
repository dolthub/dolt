#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common

    TESTDIRS=$(pwd)/testdirs
    mkdir -p $TESTDIRS/{rem1,repo1}

    # repo1 -> rem1 -> repo2
    cd $TESTDIRS/repo1
    dolt init
    dolt remote add origin file://../rem1
    dolt remote add test-remote file://../rem1
    dolt push origin main

    cd $TESTDIRS
    dolt clone file://rem1 repo2
    cd $TESTDIRS/repo2
    dolt log
    dolt remote add test-remote file://../rem1

    # table and comits only present on repo1, rem1 at start
    cd $TESTDIRS/repo1
    dolt sql -q "create table t1 (a int primary key, b int)"
    dolt add .
    dolt commit -am "First commit"
    dolt sql -q "insert into t1 values (0,0)"
    dolt commit -am "Second commit"
    cd $TESTDIRS
}

teardown() {
    teardown_common
    rm -rf $TESTDIRS
}

@test "sql-push: dolt_push origin" {
    cd repo1
    dolt sql -q "call dolt_push('origin', 'main')"

    cd ../repo2
    dolt pull origin
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-push: CALL dolt_push origin" {
    cd repo1
    dolt sql -q "CALL dolt_push('origin', 'main')"

    cd ../repo2
    dolt pull origin
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-push: CALL dolt_push origin in stored procedure" {
    cd repo1
    dolt sql <<SQL
delimiter //
create procedure merge_push_branch(branchName varchar(255))
begin
	call dolt_checkout(branchName);
	call dolt_merge('--no-ff', 'main');
  call dolt_push('origin', branchName);
end;
//
SQL

    dolt sql <<SQL
call dolt_checkout('-b', 'branch1');
insert into t1 values (5,500);
call dolt_commit('-am', 'new row on branch1');
SQL

    dolt sql <<SQL
insert into t1 values (10,100);
call dolt_commit('-am', 'new row on main');
SQL
    
    dolt sql -q "CALL merge_push_branch('branch1')"

    cd ../repo2
    dolt fetch origin
    dolt checkout branch1

    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false

    run dolt sql -q "select * from t1 order by 1" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 4 ]
    [[ "$output" =~ "10,100" ]] || false
    [[ "$output" =~ "5,500" ]] || false
}

@test "sql-push: CALL dpush origin" {
    cd repo1
    dolt sql -q "CALL dpush('origin', 'main')"

    cd ../repo2
    dolt pull origin
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-push: dolt_push custom remote" {
    cd repo1
    dolt sql -q "call dolt_push('test-remote', 'main')"

    cd ../repo2
    dolt pull origin
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-push: CALL dolt_push custom remote" {
    cd repo1
    dolt sql -q "CALL dolt_push('test-remote', 'main')"

    cd ../repo2
    dolt pull origin
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-push: dolt_push active branch" {
    skip "upstream state lost between sessions"
    cd repo1
    dolt sql -q "call dolt_push('origin')"

    cd ../repo2
    dolt pull origin
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-push: CALL dolt_push active branch" {
    skip "upstream state lost between sessions"
    cd repo1
    dolt sql -q "CALL dolt_push('origin')"

    cd ../repo2
    dolt pull origin
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-push: dolt_push feature branch" {
    cd repo1
    dolt checkout -b feature
    dolt sql -q "call dolt_push('origin', 'feature')"

    cd ../repo2
    dolt fetch origin feature
    dolt checkout feature
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-push: CALL dolt_push feature branch" {
    cd repo1
    dolt checkout -b feature
    dolt sql -q "CALL dolt_push('origin', 'feature')"

    cd ../repo2
    dolt fetch origin feature
    dolt checkout feature
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-push: dolt_push --set-upstream persists outside of session" {
    cd repo1
    dolt checkout -b other
    run dolt push
    [ "$status" -eq 1 ]
    [[ "$output" =~ "The current branch other has no upstream branch." ]] || false

    dolt sql -q "call dolt_push('-u', 'origin', 'other')"
    # upstream should be set still
    run dolt push
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Everything up-to-date" ]] || false
    [[ ! "$output" =~ "The current branch main has no upstream branch." ]] || false
}

@test "sql-push: dolt_push without --set-upstream persists outside of session when push.autoSetupRemote is set to true" {
    cd repo1
    dolt checkout -b other
    run dolt push
    [ "$status" -eq 1 ]
    [[ "$output" =~ "The current branch other has no upstream branch." ]] || false

    dolt config --local --add push.autoSetUpRemote true
    dolt sql -q "call dolt_push()"
    # upstream should be set still
    run dolt push
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Everything up-to-date" ]] || false
    [[ ! "$output" =~ "The current branch main has no upstream branch." ]] || false
}

@test "sql-push: dolt_push without --set-upstream persists outside of session when push.autoSetupRemote is set to all capital TRUE" {
    cd repo1
    dolt checkout -b other
    run dolt push
    [ "$status" -eq 1 ]
    [[ "$output" =~ "The current branch other has no upstream branch." ]] || false

    dolt config --local --add push.autoSetUpRemote TRUE
    dolt sql -q "call dolt_push()"
    # upstream should be set still
    run dolt push
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Everything up-to-date" ]] || false
    [[ ! "$output" =~ "The current branch main has no upstream branch." ]] || false
}

@test "sql-push: CALL dolt_push --set-upstream persists outside of session" {
    cd repo1
    dolt checkout -b other
    run dolt push
    [ "$status" -eq 1 ]
    [[ "$output" =~ "The current branch other has no upstream branch." ]] || false

    dolt sql -q "call dolt_push('-u', 'origin', 'other')"
    # upstream should be set still
    run dolt push
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "The current branch main has no upstream branch." ]] || false
}

@test "sql-push: dolt_push --force flag" {
    cd repo2
    dolt sql -q "create table t2 (a int)"
    dolt add .
    dolt commit -am "commit to override"
    dolt push origin main

    cd ../repo1
    run dolt sql -q "call dolt_push('origin', 'main')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "the tip of your current branch is behind its remote counterpart" ]] || false

    dolt sql -q "call dolt_push('--force', 'origin', 'main')"
}

@test "sql-push: CALL dolt_push --force flag" {
    cd repo2
    dolt sql -q "create table t2 (a int)"
    dolt add .
    dolt commit -am "commit to override"
    dolt push origin main

    cd ../repo1
    run dolt sql -q "CALL dolt_push('origin', 'main')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "the tip of your current branch is behind its remote counterpart" ]] || false

    dolt sql -q "CALL dolt_push('--force', 'origin', 'main')"
}

@test "sql-push: push to unknown remote" {
    cd repo1
    run dolt sql -q "call dolt_push('unknown', 'main')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "unknown remote: 'unknown'" ]] || false
}

@test "sql-push: push to unknown remote on CALL" {
    cd repo1
    run dolt sql -q "CALL dolt_push('unknown', 'main')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "unknown remote: 'unknown'" ]] || false
}

@test "sql-push: push unknown branch" {
    cd repo1
    run dolt sql -q "call dolt_push('origin', 'unknown')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "refspec not found: 'unknown'" ]] || false
}

@test "sql-push: push unknown branch on CALL" {
    cd repo1
    run dolt sql -q "CALL dolt_push('origin', 'unknown')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "refspec not found: 'unknown'" ]] || false
}

@test "sql-push: not specifying a branch throws an error" {
    cd repo1
    run dolt sql -q "call dolt_push('-u', 'origin')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "invalid set-upstream arguments" ]] || false
}

@test "sql-push: not specifying a branch throws an error on CALL" {
    cd repo1
    run dolt sql -q "CALL dolt_push('-u', 'origin')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "invalid set-upstream arguments" ]] || false
}

@test "sql-push: pushing empty branch does not panic" {
    cd repo1
    run dolt sql -q "call dolt_push('origin', '')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "invalid ref spec: ''" ]] || false
}

@test "sql-push: pushing empty branch does not panic on CALL" {
    cd repo1
    run dolt sql -q "CALL dolt_push('origin', '')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "invalid ref spec: ''" ]] || false
}

@test "sql-push: up to date push returns message" {
    cd repo1
    dolt sql -q "call dolt_push('origin', 'main')"
    run dolt sql -q "call dolt_push('origin', 'main')"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Everything up-to-date" ]] || false
}
