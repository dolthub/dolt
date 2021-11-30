#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

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

    dolt sql --disable-batch <<SQL
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
    dolt sql --disable-batch <<SQL
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

@test "checkout: dolt checkout with -f flag without conflict" {
    # create main remote branch
    dolt remote add origin http://localhost:50051/test-org/test-repo
    dolt sql -q 'create table test (id int primary key);'
    dolt sql -q 'insert into test (id) values (10);'
    dolt add .
    dolt commit -m 'create test table.'
    dolt push origin main:main

    # create remote branch "branch1"
    dolt checkout -b branch1
    dolt sql -q 'insert into test (id) values (1), (2), (3);'
    dolt add .
    dolt commit -m 'add some values to branch 1.'
    dolt push --set-upstream origin branch1

    run dolt checkout -f main
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Switched to branch 'main'" ]] || false

    run dolt table export test test1.sql
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully exported data." ]] || false
    [ -f test1.sql ]

    run grep INSERT test1.sql
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]

    dolt checkout branch1
    run dolt table export test test2.sql
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully exported data." ]] || false
    [ -f test2.sql ]

    run grep INSERT test2.sql
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 4 ]
}

@test "checkout: dolt checkout with -f flag with conflict" {
    # create main remote branch
    dolt remote add origin http://localhost:50051/test-org/test-repo
    dolt sql -q 'create table test (id int primary key);'
    dolt sql -q 'insert into test (id) values (10);'
    dolt add .
    dolt commit -m 'create test table.'
    dolt push origin main:main

    # create remote branch "branch1"
    dolt checkout -b branch1
    dolt sql -q 'insert into test (id) values (1), (2), (3);'
    dolt add .
    dolt commit -m 'add some values to branch 1.'
    dolt push --set-upstream origin branch1

    dolt sql -q 'insert into test (id) values (4), (5), (6);'
    run dolt checkout main
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Please commit your changes or stash them before you switch branches." ]] || false

    run dolt checkout -f main
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Switched to branch 'main'" ]] || false

    run dolt table export test test1.sql
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully exported data." ]] || false
    [ -f test1.sql ]

    run grep INSERT test1.sql
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]

    dolt checkout branch1
    run dolt table export test test2.sql
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully exported data." ]] || false
    [ -f test2.sql ]

    run grep INSERT test2.sql
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 4 ]
}
