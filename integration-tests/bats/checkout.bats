#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}

export NO_COLOR=1

@test "checkout: dolt checkout takes working set changes with you" {
    dolt sql <<SQL
create table test(a int primary key);
insert into test values (1);
SQL
    dolt add .

    dolt commit -am "Initial table with one row"
    dolt branch feature

    dolt sql -q "insert into test values (2)"
    dolt checkout feature

    run dolt sql -q "select count(*) from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "2" ]] || false

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "modified" ]] || false

    dolt checkout main

    run dolt sql -q "select count(*) from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "2" ]] || false

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "modified" ]] || false

    # Making additional changes to main, should carry them to feature without any problem
    dolt sql -q "insert into test values (3)"
    dolt checkout feature

    run dolt sql -q "select count(*) from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "3" ]] || false

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "modified" ]] || false
}

@test "checkout: dolt checkout takes working set changes with you on new table" {
    dolt sql <<SQL
create table test(a int primary key);
insert into test values (1);
SQL
    dolt add . && dolt commit -am "Initial table with one row"
    dolt branch feature

    dolt sql -q "create table t2(b int primary key)"
    dolt sql -q "insert into t2 values (1);"

    # This is fine for an untracked table, takes it to the new branch with you
    dolt checkout feature

    run dolt sql -q "select count(*) from t2"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1" ]] || false

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "new table" ]] || false

    dolt checkout main

    run dolt sql -q "select count(*) from t2"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1" ]] || false

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "new table" ]] || false

    # Now check the table into main and make additonal changes
    dolt add . && dolt commit -m "new table"
    dolt sql -q "insert into t2 values (2);"

    # This is an error, matching git (cannot check out a branch that lacks a
    # file you have modified)
    run dolt checkout feature
    [ "$status" -ne 0 ]
    [[ "$output" =~ "Your local changes to the following tables would be overwritten by checkout" ]] || false
    [[ "$output" =~ "t2" ]] || false 
}

@test "checkout: checkout would overwrite local changes" {
    dolt sql <<SQL
create table test(a int primary key);
insert into test values (1);
SQL
    dolt add .

    dolt commit -am "Initial table with one row"
    dolt checkout -b feature

    dolt sql -q "insert into test values (2)"
    dolt commit -am "inserted a value"
    dolt checkout main

    dolt sql -q "insert into test values (3)"
    run dolt checkout feature

    [ "$status" -ne 0 ]
    [[ "$output" =~ "Your local changes to the following tables would be overwritten by checkout" ]] || false
    [[ "$output" =~ "test" ]] || false 
}

@test "checkout: dolt checkout doesn't stomp working set changes on other branch" {
    dolt sql <<SQL
create table test(a int primary key);
insert into test values (1);
SQL

    dolt add .
    dolt commit -am "Initial table with one row"
    dolt branch feature

    dolt sql  <<SQL
call dolt_checkout('feature');
insert into test values (2);
SQL

    # With no uncommitted working set changes, this works fine (no
    # working set comes with us, we get the working set of the feature
    # branch instead)
    run dolt checkout feature
    [ "$status" -eq 0 ]
 
    run dolt sql -q "select count(*) from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "2" ]] || false

    # These working set changes come with us when we change back to main
    dolt checkout main
    run dolt sql -q "select count(*) from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "2" ]] || false

    # Reset our test setup
    dolt sql  <<SQL
call dolt_checkout('feature');
call dolt_reset('--hard');
insert into test values (3);
SQL

    # With a dirty working set on the other branch, dolt checkout should fail
    run dolt checkout feature
    [ "$status" -eq 1 ]
    [[ "$output" =~ "checkout would overwrite uncommitted changes" ]] || false

    # Same as above, but changes are staged, not in working
    dolt sql  <<SQL
call dolt_checkout('feature');
call dolt_reset('--hard');
insert into test values (3);
call dolt_add('.');
SQL

    run dolt checkout feature
    [ "$status" -eq 1 ]
    [[ "$output" =~ "checkout would overwrite uncommitted changes" ]] || false

    # Same as above, but changes are staged and working
    dolt add .
    dolt sql  <<SQL
call dolt_checkout('feature');
call dolt_reset('--hard');
insert into test values (3);
call dolt_add('.');
insert into test values (4);
SQL

    run dolt checkout feature
    [ "$status" -eq 1 ]
    [[ "$output" =~ "checkout would overwrite uncommitted changes" ]] || false
    
    dolt reset --hard
    dolt sql -q "insert into test values (3)"
    dolt add .
    dolt sql -q "insert into test values (4)"
    
    # with staged changes matching on both branches, permit the checkout
    dolt checkout feature
    run dolt sql -q "select count(*) from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "3" ]] || false
}

@test "checkout: dolt checkout table to restore working tree tables with add and drop foreign key" {
    dolt sql -q "create table t (c1 int primary key, c2 int, check(c2 > 0))"
    dolt sql -q "create table z (c1 int primary key, c2 int)"
    dolt commit -Am "create tables t and z"

    dolt sql -q "ALTER TABLE z ADD CONSTRAINT foreign_key1 FOREIGN KEY (c1) references t(c1)"
    run dolt status
    [[ "$output" =~ "Changes not staged for commit:" ]] || false
    [[ "$output" =~ "modified:         z" ]] || false

    run dolt schema show z
    [ "$status" -eq 0 ]
    [[ "$output" =~ "foreign_key1" ]] || false

    dolt checkout z

    run dolt status
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false

    run dolt schema show z
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "foreign_key1" ]] || false

    dolt sql -q "ALTER TABLE z ADD CONSTRAINT foreign_key1 FOREIGN KEY (c1) references t(c1)"
    dolt commit -am "add fkey"

    dolt sql -q "alter table z drop constraint foreign_key1"
    run dolt status
    [[ "$output" =~ "Changes not staged for commit:" ]] || false
    [[ "$output" =~ "modified:         z" ]] || false

    run dolt schema show z
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "foreign_key1" ]] || false

    dolt checkout z

    run dolt status
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false

    run dolt schema show z
    [ "$status" -eq 0 ]
    [[ "$output" =~ "foreign_key1" ]] || false
}

@test "checkout: with -f flag without conflict" {
    dolt sql -q 'create table test (id int primary key);'
    dolt sql -q 'insert into test (id) values (8);'
    dolt add .
    dolt commit -m 'create test table.'

    dolt checkout -b branch1
    dolt sql -q 'insert into test (id) values (1), (2), (3);'
    dolt add .
    dolt commit -m 'add some values to branch 1.'

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

@test "checkout: with -f flag with conflict" {
    dolt sql -q 'create table test (id int primary key);'
    dolt sql -q 'insert into test (id) values (8);'
    dolt add .
    dolt commit -m 'create test table.'

    dolt checkout -b branch1
    dolt sql -q 'insert into test (id) values (1), (2), (3);'
    dolt add .
    dolt commit -m 'add some values to branch 1.'

    dolt sql -q 'insert into test (id) values (4);'
    run dolt checkout main
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Please commit your changes or stash them before you switch branches." ]] || false

    # Still on main
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "branch1" ]] || false

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

@test "checkout: -B flag will forcefully reset an existing branch" {
    dolt sql -q 'create table test (id int primary key);'
    dolt sql -q 'insert into test (id) values (89012);'
    dolt commit -Am 'first change.'
    dolt sql -q 'insert into test (id) values (76543);'
    dolt commit -Am 'second change.'

    dolt checkout -b testbr main~1
    run dolt sql -q "select * from test;"
    [[ "$output" =~ "89012" ]] || false
    [[ ! "$output" =~ "76543" ]] || false

    # make a change to the branch which we'll lose
    dolt sql -q 'insert into test (id) values (19283);'
    dolt commit -Am 'change to testbr.'

    dolt checkout main
    dolt checkout -B testbr main
    run dolt sql -q "select * from test;"
    [[ "$output" =~ "89012" ]] || false
    [[ "$output" =~ "76543" ]] || false
    [[ ! "$output" =~ "19283" ]] || false
}

@test "checkout: -B will create a branch that does not exist" {
    dolt sql -q 'create table test (id int primary key);'
    dolt sql -q 'insert into test (id) values (89012);'
    dolt commit -Am 'first change.'
    dolt sql -q 'insert into test (id) values (76543);'
    dolt commit -Am 'second change.'

    dolt checkout -B testbr main~1
    run dolt sql -q "select * from test;"
    [[ "$output" =~ "89012" ]] || false
    [[ ! "$output" =~ "76543" ]] || false
}




@test "checkout: attempting to checkout a detached head shows a suggestion instead" {
  dolt sql -q "create table test (id int primary key);"
  dolt add .
  dolt commit -m "create test table."
  sha=$(dolt log --oneline --decorate=no | head -n 1 | cut -d ' ' -f 1)

  # remove special characters (color)
  sha=$(echo $sha | sed -E "s/[[:cntrl:]]\[[0-9]{1,3}m//g")

  run dolt checkout "$sha"
  [ "$status" -ne 0 ]
  cmd=$(echo "${lines[1]}" | cut -d ' ' -f 1,2,3)
  [[ $cmd =~ "dolt checkout $sha" ]] || false
}

@test "checkout: commit --amend only changes commit message" {
  dolt sql -q "create table test (id int primary key);"
  dolt sql -q 'insert into test (id) values (8);'
  dolt add .
  dolt commit -m "original commit message"

  dolt commit --amend -m "modified_commit_message"

  commitmsg=$(dolt log --oneline | head -n 1)
  [[ $commitmsg =~ "modified_commit_message" ]] || false

  numcommits=$(dolt log --oneline | wc -l)
  [[ $numcommits =~ "2" ]] || false

  run dolt sql -q 'select * from test;'
  [ "$status" -eq 0 ]
  [[ "$output" =~ "8" ]] || false
}

@test "checkout: commit --amend adds new changes to existing commit" {
  dolt sql -q "create table test (id int primary key);"
  dolt sql -q 'insert into test (id) values (8);'
  dolt add .
  dolt commit -m "original commit message"

  dolt sql -q 'insert into test (id) values (9);'
  dolt add .
  dolt commit --amend -m "modified_commit_message"

  commitmsg=$(dolt log --oneline | head -n 1)
  [[ $commitmsg =~ "modified_commit_message" ]] || false

  numcommits=$(dolt log --oneline | wc -l)
  [[ $numcommits =~ "2" ]] || false

  run dolt sql -q 'select * from test;'
  [ "$status" -eq 0 ]
  [[ "$output" =~ "8" ]] || false
  [[ "$output" =~ "9" ]] || false
}

@test "checkout: commit --amend on merge commits does not modify metadata of merged parents" {
  dolt sql -q "create table test (id int primary key, id2 int);"
  dolt add .
  dolt commit -m "original table"

  dolt checkout -b test-branch
  dolt sql -q 'insert into test (id, id2) values (0, 2);'
  dolt add .
  dolt commit -m "conclicting commit message"

  shaparent1=$(dolt log --oneline --decorate=no | head -n 1 | cut -d ' ' -f 1)
  # remove special characters (color)
  shaparent1=$(echo $shaparent1 | sed -E "s/[[:cntrl:]]\[[0-9]{1,3}m//g")

  dolt checkout main
  dolt sql -q 'insert into test (id, id2) values (0, 1);'
  dolt add .
  dolt commit -m "original commit message"
  shaparent2=$(dolt log --oneline --decorate=no | head -n 1 | cut -d ' ' -f 1)
  # remove special characters (color)
  shaparent2=$(echo $shaparent2 | sed -E "s/[[:cntrl:]]\[[0-9]{1,3}m//g")

  run dolt merge test-branch
  [ "$status" -eq 1 ]
  [[ "$output" =~ "CONFLICT (content):" ]] || false
  dolt conflicts resolve --theirs .
  dolt commit -m "final merge"

  dolt commit --amend -m "new merge"
  commitmeta=$(dolt log --oneline --parents | head -n 1)
  echo "debug commitmeta $commitmeta"
  echo "debug shaparent1 $shaparent1"
  [[ "$commitmeta" =~ "$shaparent1" ]] || false
  [[ "$commitmeta" =~ "$shaparent2" ]] || false
}

@test "checkout: dolt_commit --amend on merge commits does not modify metadata of merged parents" {
  dolt sql -q "create table test (id int primary key, id2 int);"
  dolt add .
  dolt commit -m "original table"

  dolt checkout -b test-branch
  dolt sql -q 'insert into test (id, id2) values (0, 2);'
  dolt add .
  dolt commit -m "conclicting commit message"

  shaparent1=$(dolt log --oneline --decorate=no | head -n 1 | cut -d ' ' -f 1)
  # remove special characters (color)
  shaparent1=$(echo $shaparent1 | sed -E "s/[[:cntrl:]]\[[0-9]{1,3}m//g")

  dolt checkout main
  dolt sql -q 'insert into test (id, id2) values (0, 1);'
  dolt add .
  dolt commit -m "original commit message"
  shaparent2=$(dolt log --oneline --decorate=no | head -n 1 | cut -d ' ' -f 1)
  # remove special characters (color)
  shaparent2=$(echo $shaparent2 | sed -E "s/[[:cntrl:]]\[[0-9]{1,3}m//g")

  run dolt merge test-branch
  [ "$status" -eq 1 ]
  echo "$output"
  [[ "$output" =~ "CONFLICT (content):" ]] || false
  dolt conflicts resolve --theirs .
  dolt commit -m "final merge"

  dolt sql -q "call dolt_commit('--amend', '-m', 'new merge');"
  commitmeta=$(dolt log --oneline --parents | head -n 1)
  [[ "$commitmeta" =~ "$shaparent1" ]] || false
  [[ "$commitmeta" =~ "$shaparent2" ]] || false
}


@test "checkout: dolt_checkout brings in changes from main to feature branch that has no working set" {
  # original setup
  dolt sql -q "create table users (id int primary key, name varchar(32));"
  dolt add .
  dolt commit -m "original users table"

  # create feature branch
  dolt branch -c main feature

  # make changes on main and verify
  dolt sql -q 'insert into users (id, name) values (1, "main-change");'
  run dolt sql -q "select name from users"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "main-change" ]] || false

  # checkout feature branch and bring over main changes
  dolt checkout feature

  # verify working set changes are brought in from main
  run dolt sql << SQL
call dolt_checkout('feature');
select name from users
SQL
  [ "$status" -eq 0 ]
  [[ "$output" =~ "main-change" ]] || false

  # verify working set changes are not on main
  run dolt sql << SQL
call dolt_checkout('main');
select count(*) from users
SQL
  [ "$status" -eq 0 ]
  [[ "$output" =~ "0" ]] || false

  # revert working set changes on feature branch
  dolt reset --hard HEAD
  run dolt sql -q "select count(*) from users"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "0" ]] || false

  # switch to main and verify working set changes are not present
  dolt checkout main
  run dolt sql -q "select count(*) from users"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "0" ]] || false
}

@test "checkout: dolt_checkout switches from clean main to feature branch that has changes" {
  # original setup
  dolt sql -q "create table users (id int primary key, name varchar(32));"
  dolt add .
  dolt commit -m "original users table"

  # create feature branch
  dolt branch -c main feature

  # make changes on feature (through SQL)
  dolt sql << SQL
call dolt_checkout('feature');
insert into users (id, name) values (1, "feature-change");
SQL

  # verify feature branch changes are present
  run dolt sql << SQL
call dolt_checkout('feature');
select name from users;
SQL
  echo "output = $output"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "feature-change" ]] || false

  # checkout feature branch
  dolt checkout feature

  # verify feature's working set changes are gone
  run dolt sql << SQL
call dolt_checkout('feature');
select count(*) from users
SQL
  [ "$status" -eq 0 ]
  [[ "$output" =~ "0" ]] || false

  # verify working set changes are not on main
  run dolt sql << SQL
call dolt_checkout('main');
select count(*) from users
SQL
  [ "$status" -eq 0 ]
  [[ "$output" =~ "0" ]] || false
}

@test "checkout: dolt_checkout brings in changes from main to feature branch that has identical changes" {
  # original setup
  dolt sql -q "create table users (id int primary key, name varchar(32));"
  dolt add .
  dolt commit -m "original users table"

  # create feature branch
  dolt branch -c main feature

  # make changes on main and verify
  dolt sql -q 'insert into users (id, name) values (1, "main-change");'
  run dolt sql -q "select name from users"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "main-change" ]] || false

  # make identical changes on feature (through SQL)
  dolt sql << SQL
call dolt_checkout('feature');
insert into users (id, name) values (1, "main-change");
SQL

  # verify feature branch changes are present
  run dolt sql << SQL
call dolt_checkout('feature');
select name from users;
SQL
  echo "output = $output"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "main-change" ]] || false

  # checkout feature branch
  dolt checkout feature

  # verify working set changes are still the same on feature branch
  run dolt sql << SQL
call dolt_checkout('feature');
select name from users
SQL
  [ "$status" -eq 0 ]
  [[ "$output" =~ "main-change" ]] || false

  # verify working set changes are not on main
  run dolt sql << SQL
call dolt_checkout('main');
select count(*) from users
SQL
  [ "$status" -eq 0 ]
  [[ "$output" =~ "0" ]] || false

  # revert working set changes on feature branch
  dolt reset --hard HEAD

  # verify working set changes are not on feature branch
  run dolt sql << SQL
call dolt_checkout('feature');
select count(*) from users
SQL
  [ "$status" -eq 0 ]
  [[ "$output" =~ "0" ]] || false

  # switch to main and verify working set changes are not present
  dolt checkout main
  run dolt sql << SQL
call dolt_checkout('main');
select count(*) from users
SQL
  [ "$status" -eq 0 ]
  [[ "$output" =~ "0" ]] || false
}

@test "checkout: dolt_checkout needs -f to bring in changes from main to feature branch that has different changes" {
  # original setup
  dolt sql -q "create table users (id int primary key, name varchar(32));"
  dolt add .
  dolt commit -m "original users table"

  # create feature branch from main
  dolt branch feature

  # make changes on main and verify
  dolt sql -q 'insert into users (id, name) values (1, "main-change");'
  run dolt sql -q "select name from users"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "main-change" ]] || false

  # make different changes on feature (through SQL)
  dolt sql << SQL
call dolt_checkout('feature');
insert into users (id, name) values (2, "feature-change");
SQL

  # verify feature branch changes are present
  run dolt sql << SQL
call dolt_checkout('feature');
select name from users;
SQL
  echo "output = $output"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "feature-change" ]] || false

  # checkout feature branch: should fail due to working set changes
  run dolt checkout feature
  echo "output = $output"
  [ "$status" -eq 1 ]
  [[ "$output" =~ "checkout would overwrite uncommitted changes on target branch" ]] || false

  # force checkout feature branch
  dolt checkout -f feature

  # verify working set changes on feature are from main
  run dolt sql << SQL
call dolt_checkout('feature');
select name from users
SQL
  [ "$status" -eq 0 ]
  [[ "$output" =~ "main-change" ]] || false

  # verify working set changes are not on main
  run dolt sql << SQL
call dolt_checkout('main');
select count(*) from users
SQL
  [ "$status" -eq 0 ]
  [[ "$output" =~ "0" ]] || false
}

@test "checkout: dolt_checkout brings changes from main to multiple feature branches and back to main" {
  # original setup
  dolt sql -q "create table users (id int primary key, name varchar(32));"
  dolt add .
  dolt commit -m "original users table"


  # make changes on main and verify
  dolt sql -q 'insert into users (id, name) values (0, "main-change");'
  run dolt sql << SQL
call dolt_checkout('main');
select name from users
SQL
  [ "$status" -eq 0 ]
  [[ "$output" =~ "main-change" ]] || false


  # create feature1 branch and bring changes to the new feature branch
  dolt checkout -b feature1

  # verify the changes are brought to feature1
  run dolt sql << SQL
call dolt_checkout('feature1');
select name from users
SQL
  [ "$status" -eq 0 ]
  [[ "$output" =~ "main-change" ]] || false


  # make changes on feature1 and verify
  dolt sql -q 'insert into users (id, name) values (1, "feature1-change");'
  run dolt sql << SQL
call dolt_checkout('feature1');
select name from users
SQL
  [ "$status" -eq 0 ]
  [[ "$output" =~ "main-change" ]] || false
  [[ "$output" =~ "feature1-change" ]] || false

  # create feature2 branch and bring changes to next feature branch
  dolt checkout -b feature2

  # verify the changes are brought to feature1
  run dolt sql << SQL
call dolt_checkout('feature2');
select name from users
SQL
  [ "$status" -eq 0 ]
  [[ "$output" =~ "main-change" ]] || false
  [[ "$output" =~ "feature1-change" ]] || false

  # make changes on feature2 and verify
  dolt sql -q 'insert into users (id, name) values (2, "feature2-change");'
  run dolt sql << SQL
call dolt_checkout('feature2');
select name from users
SQL
  [ "$status" -eq 0 ]
  [[ "$output" =~ "main-change" ]] || false
  [[ "$output" =~ "feature1-change" ]] || false
  [[ "$output" =~ "feature2-change" ]] || false


  # bring changes back to main
  dolt checkout main

  # verify the changes are brought to main
  run dolt sql << SQL
call dolt_checkout('main');
select name from users
SQL
  [ "$status" -eq 0 ]
  [[ "$output" =~ "main-change" ]] || false
  [[ "$output" =~ "feature1-change" ]] || false
  [[ "$output" =~ "feature2-change" ]] || false

}
