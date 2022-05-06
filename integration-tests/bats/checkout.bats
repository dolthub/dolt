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

@test "checkout: dolt checkout doesn't stomp working set changes on other branch" {
    dolt sql <<SQL
create table test(a int primary key);
insert into test values (1);
SQL

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
