#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash
load $BATS_TEST_DIRNAME/helper/criss-cross.bash

setup() {
    setup_common

    dolt sql -q "CREATE TABLE test (pk int primary key);"
    dolt add -A && dolt commit -m "commit A"
    dolt branch zero

    dolt sql -q "INSERT INTO test VALUES (0);"
    dolt commit -am "commit B"
    dolt branch one
    dolt branch two

    dolt sql -q "INSERT INTO test VALUES (1);"
    dolt commit -am "commit C"

    dolt checkout two
    dolt sql -q "INSERT INTO test VALUES (2);"
    dolt commit -am "commit D"
    dolt checkout main

    # # # # # # # # # # # # # # # # # # # # # # #
    #                                           #
    #                 <-- (zero)                #
    #               /                           #
    #              /       <-- (one)            #
    #             /      /                      #
    # (init) -- (A) -- (B) -- (C) <-- (main)    #
    #                    \                      #
    #                      -- (D) <-- (two)     #
    #                                           #
    # # # # # # # # # # # # # # # # # # # # # # #
}

teardown() {
    teardown_common
}

@test "merge-base: cli" {
    run dolt merge-base main two
    [ "$status" -eq 0 ]
    MERGE_BASE="$output"

    run dolt merge-base main one
    [ "$status" -eq 0 ]
    [ "$output" = "$MERGE_BASE" ]

    run dolt merge-base one two
    [ "$status" -eq 0 ]
    [ "$output" = "$MERGE_BASE" ]

    dolt checkout main
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "$MERGE_BASE" ]] || false

    dolt checkout two
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "$MERGE_BASE" ]] || false

    dolt checkout zero
    run dolt log
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "$MERGE_BASE" ]] || false
}

@test "merge-base: sql" {
    run dolt sql -q "SELECT message FROM dolt_log WHERE commit_hash = dolt_merge_base('main', 'zero');" -r csv
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = "commit A" ]

    run dolt sql -q "SELECT message FROM dolt_log WHERE commit_hash = dolt_merge_base('main', 'one');" -r csv
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = "commit B" ]

    run dolt sql -q "SELECT message FROM dolt_log WHERE commit_hash = dolt_merge_base('main', 'two');" -r csv
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = "commit B" ]

    run dolt sql -q "SELECT message FROM dolt_log WHERE commit_hash = dolt_merge_base('main', 'main');" -r csv
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = "commit C" ]

    # dolt_merge_base() resolves commit hashes
    run dolt sql -q "select dolt_merge_base('main', hashof('one')) = dolt_merge_base(hashof('main'),'one') FROM dual;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ ^1$ ]] || false
}

@test "merge-base: --all" {
    run dolt merge-base --all main two
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]
    [ "$output" = "$(dolt merge-base main two)" ]

    criss_cross

    # Like git, the merge base with the newest commit date comes first: baseB was committed after baseA.
    run dolt merge-base --all branchA branchB
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [ "${lines[0]}" = "$BASE_B" ]
    [ "${lines[1]}" = "$BASE_A" ]
}
