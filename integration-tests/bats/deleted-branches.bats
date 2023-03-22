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

# force_delete_main_branch_on_sqlserver connects to a dolt sql-server and deletes the default
# branch. It does this by using the hidden variable @dolt_allow_default_branch_deletion to bypass
# the check for deleting a db's default branch.
force_delete_main_branch_on_sqlserver() {
    dolt sql-client --use-db "dolt_repo_$$" -u dolt -P $PORT \
      -q "set @dolt_allow_default_branch_deletion=true; call dolt_checkout('to_keep'); call dolt_branch('-D', 'main');"
}

@test "deleted-branches: dolt checkout from CLI works when the db's default branch doesn't exist" {
    make_it
    run dolt status
    [[ "$output" =~ "On branch main" ]] || false

    run dolt sql -q 'call dolt_branch("-D", "main");'
    [ $status -eq 1 ]
    [[ "$output" =~ "attempted to delete checked out branch" ]] || false

    dolt sql -q 'call dolt_checkout("to_keep"); call dolt_branch("-D", "main");'
    run dolt branch -av
    [ $status -eq 0 ]
    [[ ! "$output" =~ "main" ]] || false

    # Checkout the branch and verify that we can run commands on the branch
    dolt checkout to_keep
    run dolt status
    [ $status -eq 0 ]
    [[ "$output" =~ "On branch to_keep" ]] || false
}

@test "deleted-branches: dolt_checkout() from sql-server doesn't panic when the db's default branch doesn't exist" {
    make_it
    start_sql_server "dolt_repo_$$"

    run dolt sql-client --use-db "dolt_repo_$$" -u dolt -P $PORT \
        -q "call dolt_checkout('to_keep'); show tables;"

    force_delete_main_branch_on_sqlserver

    run dolt sql-client --use-db "dolt_repo_$$" -u dolt -P $PORT \
        -q "call dolt_checkout('to_keep');"
    [ $status -ne 0 ]
    [[ "$output" =~ "branch not found" ]] || false
}

@test "deleted-branches: dolt branch from the CLI does not allow deleting the last branch" {
    make_it

    dolt sql -q 'call dolt_checkout("to_keep"); call dolt_branch("-D", "main");'
    run dolt branch -av
    [ $status -eq 0 ]
    [[ ! "$output" =~ "main" ]] || false

    run dolt branch -D to_keep
    [[ "$output" =~ "cannot delete the last branch" ]] || false
}

@test "deleted-branches: dolt_branch() from SQL correctly renames the db's default branch" {
    make_it

    dolt sql -q 'call dolt_checkout("to_keep"); call dolt_branch("-m", "main", "master");'

    run dolt branch -av
    [ $status -eq 0 ]
    [[ ! "$output" =~ "main" ]] || false
    [[ "$output" =~ "master" ]] || false

    run dolt status
    [ $status -eq 0 ]
    [[ "$output" =~ "On branch master" ]] || false
}

@test "deleted-branches: clients can use sql-server when the default branch doesn't exist, but the global default_branch var is set" {
    make_it
    start_sql_server "dolt_repo_$$"

    dolt sql-client --use-db "dolt_repo_$$" -u dolt -P $PORT \
      -q "SET @@GLOBAL.dolt_repo_$$_default_branch = 'to_keep'"

    force_delete_main_branch_on_sqlserver

    run dolt sql-client --use-db "dolt_repo_$$" -u dolt -P $PORT -q "describe test"
    [ $status -eq 0 ]
    [[ "$output" =~ "id" ]] || false
}

@test "deleted-branches: clients can use revision dbs in sql-server when the db's default branch doesn't exist" {
    make_it
    start_sql_server "dolt_repo_$$"

    force_delete_main_branch_on_sqlserver
    
    # Against the default branch it fails
    run dolt sql-client --use-db "dolt_repo_$$" -u dolt -P $PORT -q "SELECT * FROM test"
    [ $status -ne 0 ] 

    # Against to_keep it succeeds
    dolt sql-client --use-db "dolt_repo_$$/to_keep" -u dolt -P $PORT -q "SELECT * FROM test"
}

@test "deleted-branches: clients can use revision dbs in sql-server when the global default_branch var is set to an invalid branch" {
    make_it
    start_sql_server "dolt_repo_$$"

    dolt sql-client --use-db "dolt_repo_$$" -u dolt -P $PORT \
      -q "SET @@GLOBAL.dolt_repo_$$_default_branch = 'this_branch_does_not_exist'"

    # Against the default branch it fails
    run dolt sql-client --use-db "dolt_repo_$$" -u dolt -P $PORT -q "SELECT * FROM test" ""
    [ $status -ne 0 ]

    # Against main, which exists it succeeds
    dolt sql-client --use-db "dolt_repo_$$/main" -u dolt -P $PORT -q "SELECT * FROM test"
}

@test "deleted-branches: dolt_checkout() from sql-server works when connected to a revision db and the global default_branch var is set to an invalid branch" {
    make_it
    start_sql_server "dolt_repo_$$"

    dolt sql-client --use-db "dolt_repo_$$" -u dolt -P $PORT \
      -q "SET @@GLOBAL.dolt_repo_$$_default_branch = 'this_branch_does_not_exist'" ""

    # We are able to use a database branch revision in the connection string
    dolt sql-client --use-db "dolt_repo_$$/main" -u dolt -P $PORT -q "SELECT * FROM test;"

    # Trying to checkout a new branch throws an error, but doesn't panic
    run dolt sql-client --use-db "dolt_repo_$$/main" -u dolt -P $PORT -q "CALL DOLT_CHECKOUT('to_keep');"
    [ $status -ne 0 ]
    [[ "$output" =~ "branch not found" ]] || false
}

@test "deleted-branches: dolt_checkout() from sql-server works when connected to a revision db and the db's default branch is invalid" {
    make_it
    dolt branch -c to_keep to_checkout
    start_sql_server "dolt_repo_$$"
    force_delete_main_branch_on_sqlserver

    # We are able to use a database branch revision in the connection string
    dolt sql-client --use-db "dolt_repo_$$/to_keep" -u dolt -P $PORT -q "SELECT * FROM test;"

    # Trying to checkout a new branch throws an error, but doesn't panic
    run dolt sql-client --use-db "dolt_repo_$$/to_keep" -u dolt -P $PORT -q "CALL DOLT_CHECKOUT('to_checkout');"
    [ $status -ne 0 ]
    [[ "$output" =~ "branch not found" ]] || false
}

@test "deleted-branches: dolt_checkout() from sql-server works when the db's default branch is invalid, but the global default_branch var is valid" {
    make_it
    dolt branch -c to_keep to_checkout
    start_sql_server "dolt_repo_$$"

    dolt sql-client --use-db "dolt_repo_$$" -u dolt -P $PORT -q "SET @@GLOBAL.dolt_repo_$$_default_branch = 'to_keep'" ""

    force_delete_main_branch_on_sqlserver

    dolt sql-client --use-db "dolt_repo_$$" -u dolt -P $PORT -q "SELECT * FROM test"
    
    dolt sql-client --use-db "dolt_repo_$$" -u dolt -P $PORT -q "CALL DOLT_CHECKOUT('to_checkout')"
}
