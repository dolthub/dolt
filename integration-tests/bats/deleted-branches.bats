#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash
load $BATS_TEST_DIRNAME/helper/query-server-common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
    stop_sql_server
}

@test "deleted-branches: can checkout existing branch after checked out branch is deleted" {
    dolt branch -c main to_keep
    dolt sql -q 'delete from dolt_branches where name = "main"'
    dolt checkout to_keep
}

@test "deleted-branches: can SQL connect with dolt_default_branch set to existing branch when checked out branch is deleted" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    dolt branch -c main to_keep

    start_sql_server "dolt_repo_$$"

    server_query "dolt_repo_$$" 1 "SET @@GLOBAL.dolt_default_branch = 'to_keep'"

    server_query "dolt_repo_$$"  1 'delete from dolt_branches where name = "main"' ""

    server_query "dolt_repo_$$" 1 "SELECT 2+2 FROM dual" "2+2\n4"
}

@test "deleted-branches: can SQL connect with existing branch revision specifier when checked out branch is deleted" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    dolt branch -c main to_keep

    start_sql_server "dolt_repo_$$"

    server_query "dolt_repo_$$"  1 'delete from dolt_branches where name = "main"' ""

    # Against the default branch it fails
    run server_query "dolt_repo_$$" 1 "SELECT 2+2 FROM dual" "2+2\n4"
    [ "$status" -eq 1 ] || fail "expected query against the default branch, which was deleted, to fail"

    # Against to_keep it succeeds
    server_query "dolt_repo_$$/to_keep" 1 "SELECT 2+2 FROM dual" "2+2\n4"
}
