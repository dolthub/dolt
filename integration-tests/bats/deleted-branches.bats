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

@test "deleted-branches: can checkout existing branch after checked out branch is deleted" {
    make_it

    dolt sql -q 'delete from dolt_branches where name = "main"'

    dolt branch -av

    dolt checkout to_keep
}

@test "deleted-branches: can SQL connect with dolt_default_branch set to existing branch when checked out branch is deleted" {
    make_it

    start_sql_server "dolt_repo_$$"

    server_query "dolt_repo_$$" 1 dolt "" "SET @@GLOBAL.dolt_repo_$$_default_branch = 'to_keep'" ""

    server_query "dolt_repo_$$"  1 dolt "" 'delete from dolt_branches where name = "main"' ""

    server_query "dolt_repo_$$" 1 dolt "" "SELECT * FROM test" "id\n" ""
}

@test "deleted-branches: can SQL connect with existing branch revision specifier when checked out branch is deleted" {
    make_it

    start_sql_server "dolt_repo_$$"

    server_query "dolt_repo_$$" 1 dolt "" 'delete from dolt_branches where name = "main"' ""

    # Against the default branch it fails
    run server_query "dolt_repo_$$" 1 "" dolt "" "SELECT * FROM test" "id\n" ""
    [ "$status" -eq 1 ] || fail "expected query against the default branch, which was deleted, to fail"

    # Against to_keep it succeeds
    server_query "dolt_repo_$$/to_keep" 1 dolt "" "SELECT * FROM test" "id\n" ""
}

@test "deleted-branches: can SQL connect with existing branch revision specifier when dolt_default_branch is invalid" {
    make_it

    start_sql_server "dolt_repo_$$"

    server_query "dolt_repo_$$" 1 dolt "" "SET @@GLOBAL.dolt_repo_$$_default_branch = 'this_branch_does_not_exist'" ""

    # Against the default branch it fails
    server_query "dolt_repo_$$" 1 dolt "" "SELECT * FROM test" "" 1

    # Against main, which exists it succeeds
    server_query "dolt_repo_$$/main" 1 dolt "" "SELECT * FROM test" "id\n" ""
}

@test "deleted-branches: can DOLT_CHECKOUT on SQL connection with existing branch revision specifier when dolt_default_branch is invalid" {
    make_it

    start_sql_server "dolt_repo_$$"

    server_query "dolt_repo_$$" 1 dolt "" "SET @@GLOBAL.dolt_repo_$$_default_branch = 'this_branch_does_not_exist'" ""

    server_query "dolt_repo_$$/main" 1 dolt "" "SELECT * from test" ""
    server_query "dolt_repo_$$/main" 1 dolt "" "CALL DOLT_CHECKOUT('to_keep');" "DOLT_CHECKOUT('to_keep')\n0"
    server_query "dolt_repo_$$/to_keep" 1 dolt "" "SELECT * from test" ""
}

@test "deleted-branches: can DOLT_CHECKOUT on SQL connection with existing branch revision specifier set to existing branch when checked out branch is deleted" {
    make_it

    dolt branch -c to_keep to_checkout

    start_sql_server "dolt_repo_$$"

    server_query "dolt_repo_$$"  1 dolt "" 'delete from dolt_branches where name = "main"' ""
    server_query "dolt_repo_$$/to_checkout" 1 dolt "" "CALL DOLT_CHECKOUT('to_checkout')" "DOLT_CHECKOUT('to_checkout')\n0"
    server_query "dolt_repo_$$/to_checkout" 1 dolt "" "SELECT * from test" ""
}

@test "deleted-branches: can DOLT_CHECKOUT on SQL connection with dolt_default_branch set to existing branch when checked out branch is deleted" {
    make_it

    dolt branch -c to_keep to_checkout

    start_sql_server "dolt_repo_$$"

    server_query "dolt_repo_$$" 1 dolt "" "SET @@GLOBAL.dolt_repo_$$_default_branch = 'to_keep'" ""

    server_query "dolt_repo_$$"  1 dolt "" 'delete from dolt_branches where name = "main"' ""

    server_query "dolt_repo_$$" 1 dolt "" "SELECT * FROM test" ""
    
    server_query "dolt_repo_$$" 1 dolt "" "DOLT_CHECKOUT('to_checkout');" ""
}
