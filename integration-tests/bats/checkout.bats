#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}

# Create a single primary key table and do stuff
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
    dolt checkout -f feature

    run dolt sql -q "select count(*) from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "3" ]] || false

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "modified" ]] || false
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
select dolt_checkout('feature');
insert into test values (2), (3), (4);
commit;
SQL

    skip "checkout stomps working set changes made on the feature branch via SQL. Needs to be prevented."
    skip "See https://github.com/dolthub/dolt/issues/2246"

    # With no uncommitted working set changes, this works fine (no
    # working set comes with us, we get the working set of the feature
    # branch instead)
    run dolt checkout feature
    [ "$status" -eq 0 ]

    run dolt sql -q "select count(*) from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "4" ]] || false

    dolt checkout main
    run dolt sql -q "select count(*) from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "4" ]] || false

    # Reset our test setup
    dolt sql  <<SQL
select dolt_checkout('feature');
select dolt_reset('--hard');
insert into test values (2), (3), (4);
commit;
SQL

    # With a dirty working set, dolt checkout should fail
    dolt sql -q "insert into test values (5)"
    run dolt checkout feature

    [ "$status" -eq 1 ]
    [[ "$output" =~ "some error" ]] || false
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
  [[ $cmd =~ "dolt checkout $sha" ]]
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

  dolt merge test-branch
  dolt conflicts resolve --theirs .
  dolt commit -m "final merge"

  dolt commit --amend -m "new merge"
  commitmeta=$(dolt log --oneline --parents | head -n 1)
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

  dolt merge test-branch
  dolt conflicts resolve --theirs .
  dolt commit -m "final merge"

  dolt sql -q "call dolt_commit('--amend', '-m', 'new merge');"
  commitmeta=$(dolt log --oneline --parents | head -n 1)
  [[ "$commitmeta" =~ "$shaparent1" ]] || false
  [[ "$commitmeta" =~ "$shaparent2" ]] || false
}


# @test "checkout: block checkout when current and target branches have working set changes" {
#   dolt sql -q "create table users (id int primary key, name varchar(32));"
#   dolt add .
#   dolt commit -m "original users table"
#   dolt branch -c main feature

#   # make changes on main
#   dolt sql -q 'insert into users (id, name) values (1, "main-change");'
#   # make sure changes are present
#   run dolt sql -q "select count(*) from users"
#   [ "$status" -eq 0 ]
#   [[ "$output" =~ "1" ]] || false

#   # checkout feature
#   dolt checkout feature
#   # make sure changes are pulled in from main
#   run dolt sql -q "select count(*) from users"
#   [ "$status" -eq 0 ]
#   [[ "$output" =~ "1" ]] || false

#   # make changes on feature
#   dolt sql -q 'insert into users (id, name) values (2, "feature-change");'
#   # make sure new changes are present
#   run dolt sql -q "select count(*) from users"
#   [ "$status" -eq 0 ]
#   [[ "$output" =~ "2" ]] || false

#   # try to checkout main, but fail due to working set changes
#   run dolt checkout main
#   [ "$status" -eq 1 ]
#   [[ "$output" =~ "working sets exist on both branches" ]] || false

#   # try to checkout main, but succeed due to force flag
#   dolt checkout -f main

#   # make sure changes on both branches were applied
#   run dolt sql -q "select count(*) from users"
#   [ "$status" -eq 0 ]
#   [[ "$output" =~ "2" ]] || false
# }

# @test "checkout: allow checkout when current and target branches have identical working set changes" {
#   dolt sql -q "create table users (id int primary key, name varchar(32));"
#   dolt add .
#   dolt commit -m "original users table"

#   dolt branch -c main feature
#   dolt sql -q 'insert into users (id, name) values (1, "main-change");'

#   run dolt sql -q "select count(*) from users"
#   [ "$status" -eq 0 ]
#   [[ "$output" =~ "1" ]] || false

#   dolt checkout feature
#   run dolt sql -q "select count(*) from users"
#   [ "$status" -eq 0 ]
#   [[ "$output" =~ "1" ]] || false

#   dolt checkout main
#   run dolt sql -q "select count(*) from users"
#   [ "$status" -eq 0 ]
#   [[ "$output" =~ "1" ]] || false
# }


@test "checkout: 4941 ScenarioA" {
  # original setup
  dolt sql -q "create table users (id int primary key, name varchar(32));"
  dolt add .
  dolt commit -m "original users table"

  # create feature branch
  dolt branch -c main feature

  # make changes on main and verify
  dolt sql -q 'insert into users (id, name) values (1, "main-change");'
  run dolt sql -q "select count(*) from users"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "1" ]] || false

  # checkout feature branch
  dolt checkout feature

  # verify working set changes are pulled in from main
  run dolt sql -q "select count(*) from users"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "1" ]] || false

  # revert working set changes on feature branch
  dolt reset --hard HEAD

  # switch to main and verify working set changes are not present
  dolt checkout main
  run dolt sql -q "select count(*) from users"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "0" ]] || false
}

@test "checkout: 4941 ScenarioB" {
  # original setup
  dolt sql -q "create table users (id int primary key, name varchar(32));"
  dolt add .
  dolt commit -m "original users table"

  # create feature branch
  dolt branch -c main feature

  # make changes on main and verify
  dolt sql -q 'insert into users (id, name) values (1, "main-change");'
  run dolt sql -q "select count(*) from users"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "1" ]] || false

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

  # verify working set changes are pulled in from main
  run dolt sql -q "select name from users"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "main-change" ]] || false

  # revert working set changes on feature branch
  dolt reset --hard HEAD

  # switch to main and verify working set changes are not present
  dolt checkout main
  run dolt sql -q "select count(*) from users"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "0" ]] || false
}

@test "checkout: 4941 ScenarioC" {
  # original setup
  dolt sql -q "create table users (id int primary key, name varchar(32));"
  dolt add .
  dolt commit -m "original users table"

  # create feature branch
  dolt branch -c main feature

  # make changes on main and verify
  dolt sql -q 'insert into users (id, name) values (1, "main-change");'
  run dolt sql -q "select count(*) from users"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "1" ]] || false

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
  [ "$status" -eq 1 ]

  # force checkout feature branch
  dolt checkout -f feature

  run dolt sql << SQL
call dolt_checkout('feature');
select count(*) from users;
select name from users;
SQL
  echo "output: $output, status: $status"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "main-change" ]] || false
  [[ "$output" =~ "feature-change" ]] || false
}
