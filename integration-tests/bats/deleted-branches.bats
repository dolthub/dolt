#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash
load $BATS_TEST_DIRNAME/helper/query-server-common.bash

setup() {
    skiponwindows "Missing dependencies"

    setup_common
}

teardown() {
    teardown_common
    stop_sql_server
}

make_it() {
    dolt sql -q 'create table test (id int primary key);'
    dolt add .
    dolt commit -m 'initial commit'

    dolt branch -c main to_keep
}

@test "deleted-branches: can checkout existing branch after checked out branch on CLI is deleted" {
    make_it

    run dolt status
    [[ "$output" =~ "On branch main" ]] || false

    run dolt sql -q 'call dolt_branch("-D", "main");'
    [ $status -eq 1 ]
    [[ "$output" =~ "attempted to delete checked out branch" ]] || false

    dolt sql -q 'call dolt_checkout("to_keep"); call dolt_branch("-D", "main");'

    dolt branch -av

    dolt checkout to_keep
}

@test "deleted-branches: attempt to delete the last branch when currently on no branch" {
    make_it

    dolt sql -q 'call dolt_checkout("to_keep"); call dolt_branch("-D", "main");'

    dolt branch -av

    run dolt branch -D to_keep
    [[ "$output" =~ "cannot delete the last branch" ]] || false
}

@test "deleted-branches: renaming current branch on CLI deletes that branch and sets the current branch to the new branch on CLI" {
    make_it

    dolt sql -q 'call dolt_checkout("to_keep"); call dolt_branch("-m", "main", "master");'

    dolt branch -av

    run dolt status
    [[ "$output" =~ "On branch master" ]] || false
}

@test "deleted-branches: can SQL connect with dolt_default_branch set to existing branch when checked out branch is deleted" {
    make_it

    start_sql_server "dolt_repo_$$"

    dolt sql-client --use-db "dolt_repo_$$" -u dolt -P $PORT -q "SET @@GLOBAL.dolt_repo_$$_default_branch = 'to_keep'"

    dolt sql-client --use-db "dolt_repo_$$" -u dolt -P $PORT -q "call dolt_checkout('to_keep')"
    dolt sql-client --use-db "dolt_repo_$$" -u dolt -P $PORT -q "call dolt_branch('-D', 'main');"

    run dolt sql-client --use-db "dolt_repo_$$" -u dolt -P $PORT -q "describe test"
    [ $status -eq 0 ]
    [[ "$output" =~ "id" ]] || false
}

@test "deleted-branches: can SQL connect with existing branch revision specifier when checked out branch is deleted" {
    make_it

    start_sql_server "dolt_repo_$$"

    dolt sql-client --use-db "dolt_repo_$$" -u dolt -P $PORT -q 'call dolt_checkout("to_keep"); call dolt_branch("-D", "main");'
    
    # Against the default branch it fails
    run dolt sql-client --use-db "dolt_repo_$$" -u dolt -P $PORT -q "SELECT * FROM test"
    [ $status -ne 0 ] 

    # Against to_keep it succeeds
    dolt sql-client --use-db "dolt_repo_$$/to_keep" -u dolt -P $PORT -q "SELECT * FROM test"
}

@test "deleted-branches: can SQL connect with existing branch revision specifier when dolt_default_branch is invalid" {
    make_it

    start_sql_server "dolt_repo_$$"

    dolt sql-client --use-db "dolt_repo_$$" -u dolt -P $PORT -q "SET @@GLOBAL.dolt_repo_$$_default_branch = 'this_branch_does_not_exist'"

    # Against the default branch it fails
    run dolt sql-client --use-db "dolt_repo_$$" -u dolt -P $PORT -q "SELECT * FROM test" ""
    [ $status -ne 0 ]

    # Against main, which exists it succeeds
    dolt sql-client --use-db "dolt_repo_$$/main" -u dolt -P $PORT -q "SELECT * FROM test"
}

@test "deleted-branches: calling DOLT_CHECKOUT on SQL connection with existing branch revision specifier when dolt_default_branch is invalid does not panic" {
    skip "Will fix in a future PR"
    make_it

    start_sql_server "dolt_repo_$$"

    dolt sql-client --use-db "dolt_repo_$$" -u dolt -P $PORT -q "SET @@GLOBAL.dolt_repo_$$_default_branch = 'this_branch_does_not_exist'" ""

    # We are able to use a database branch revision in the connection string
    dolt sql-client --use-db "dolt_repo_$$/main" -u dolt -P $PORT -q "SELECT * FROM test;"

    # Trying to checkout a new branch throws an error, but doesn't panic
    run dolt sql-client --use-db "dolt_repo_$$/main" -u dolt -P $PORT -q"CALL DOLT_CHECKOUT('to_keep');"
    [ $status -ne 0 ]
    [[ "$output" =~ "branch not found" ]] || false
}

@test "deleted-branches: calling DOLT_CHECKOUT on SQL connection with existing branch revision specifier set to existing branch when default branch is deleted does not panic" {
    skip "Will fix in a future PR"
    make_it

    dolt branch -c to_keep to_checkout

    start_sql_server "dolt_repo_$$"

    dolt sql-client --use-db "dolt_repo_$$" -u dolt -P $PORT -q "call dolt_checkout('to_keep'); call dolt_branch('-D', 'main');"

    # We are able to use a database branch revision in the connection string
    dolt sql-client --use-db "dolt_repo_$$/to_keep" -u dolt -P $PORT -q "SELECT * FROM test;"

    # Trying to checkout a new branch throws an error, but doesn't panic
    run dolt sql-client --use-db "dolt_repo_$$/to_keep" -u dolt -P $PORT -q "CALL DOLT_CHECKOUT('to_checkout');"
    [ $status -ne 0 ]
    [[ "$output" =~ "branch not found" ]] || false
}

@test "deleted-branches: can DOLT_CHECKOUT on SQL connection with dolt_default_branch set to existing branch when checked out branch is deleted" {
    make_it

    dolt branch -c to_keep to_checkout

    start_sql_server "dolt_repo_$$"

    dolt sql-client --use-db "dolt_repo_$$" -u dolt -P $PORT -q "SET @@GLOBAL.dolt_repo_$$_default_branch = 'to_keep'" ""

    dolt sql-client --use-db "dolt_repo_$$" -u dolt -P $PORT -q "call dolt_checkout('to_keep'); call dolt_branch('-D', 'main');"

    dolt sql-client --use-db "dolt_repo_$$" -u dolt -P $PORT -q "SELECT * FROM test"
    
    dolt sql-client --use-db "dolt_repo_$$" -u dolt -P $PORT -q "CALL DOLT_CHECKOUT('to_checkout')"
}
