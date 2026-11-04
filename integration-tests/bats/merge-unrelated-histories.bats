#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

# https://github.com/dolthub/dolt/issues/4468
#
# Two repositories are initialized independently, so their commit graphs share no ancestor. The
# |other| history is published to a file:// remote and fetched into |local|.
#
#   local:  (init_L) -- (L1) <-- main
#   other:  (init_O) -- (O1) <-- other/main
#
# Both histories create a table named |common| so that, once unrelated merges are supported, the
# empty virtual base produces an add/add on the same table.

setup() {
    setup_no_dolt_init

    TESTDIRS=$(pwd)/testdirs
    mkdir -p $TESTDIRS/{other_remote,other,local}

    cd $TESTDIRS/other
    dolt init
    dolt sql -q "CREATE TABLE other_only (pk int primary key, v int);"
    dolt sql -q "CREATE TABLE common (pk int primary key, v int);"
    dolt sql -q "INSERT INTO other_only VALUES (1, 1);"
    dolt sql -q "INSERT INTO common VALUES (2, 2);"
    dolt add -A
    dolt commit -m "other: O1"
    dolt remote add origin file://../other_remote
    dolt push origin main

    cd $TESTDIRS/local
    dolt init
    dolt sql -q "CREATE TABLE local_only (pk int primary key, v int);"
    dolt sql -q "CREATE TABLE common (pk int primary key, v int);"
    dolt sql -q "INSERT INTO local_only VALUES (1, 1);"
    dolt sql -q "INSERT INTO common VALUES (1, 1);"
    dolt add -A
    dolt commit -m "local: L1"
    dolt remote add other file://../other_remote
    dolt fetch other
}

teardown() {
    teardown_common
    rm -rf $TESTDIRS
}

assert_local_main_unchanged() {
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false

    run dolt log --oneline -n 1
    [[ "$output" =~ "local: L1" ]] || false

    run dolt sql -q "SHOW TABLES" -r csv
    [[ ! "$output" =~ "other_only" ]] || false
}

@test "merge-unrelated-histories: fetch brings in the unrelated history" {
    run dolt branch -a
    [ "$status" -eq 0 ]
    [[ "$output" =~ "remotes/other/main" ]] || false

    run dolt log --oneline other/main
    [ "$status" -eq 0 ]
    [[ "$output" =~ "other: O1" ]] || false
    [[ ! "$output" =~ "local: L1" ]] || false

    local local_root=$(dolt log --oneline main | tail -n 1 | awk '{print $1}')
    local other_root=$(dolt log --oneline other/main | tail -n 1 | awk '{print $1}')
    [ "$local_root" != "$other_root" ]
}

@test "merge-unrelated-histories: merge-base reports no common ancestor" {
    run dolt merge-base main other/main
    [ "$status" -eq 1 ]
    [[ "$output" =~ "no common ancestor" ]] || false

    run dolt merge-base --all main other/main
    [ "$status" -eq 1 ]
    [[ "$output" =~ "no common ancestor" ]] || false

    run dolt sql -q "SELECT * FROM dolt_merge_bases('main', 'other/main')" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]
}

@test "merge-unrelated-histories: dolt merge is refused and leaves main untouched" {
    run dolt merge other/main
    [ "$status" -eq 1 ]
    [[ "$output" =~ "refusing to merge unrelated histories" ]] || false
    assert_local_main_unchanged

    run dolt merge --no-ff -m "merge other" other/main
    [ "$status" -eq 1 ]
    [[ "$output" =~ "refusing to merge unrelated histories" ]] || false
    assert_local_main_unchanged
}

@test "merge-unrelated-histories: dolt_merge procedure is refused" {
    run dolt sql -q "CALL dolt_merge('other/main')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "refusing to merge unrelated histories" ]] || false
    assert_local_main_unchanged
}

@test "merge-unrelated-histories: merge from the other side is also refused" {
    dolt checkout -b from_other other/main

    run dolt merge main
    [ "$status" -eq 1 ]
    [[ "$output" =~ "refusing to merge unrelated histories" ]] || false

    run dolt log --oneline -n 1
    [[ "$output" =~ "other: O1" ]] || false
}

@test "merge-unrelated-histories: preview merge conflicts fails" {
    run dolt sql -q "SELECT * FROM dolt_preview_merge_conflicts_summary('main', 'other/main')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "no common ancestor" ]] || false
}

@test "merge-unrelated-histories: pull is refused and leaves main untouched" {
    run dolt pull other main
    [ "$status" -eq 1 ]
    [[ "$output" =~ "refusing to merge unrelated histories" ]] || false
    assert_local_main_unchanged
}

@test "merge-unrelated-histories: push over an unrelated branch is rejected" {
    run dolt push other main:main
    [ "$status" -eq 1 ]
    # TODO: git reports this as a rejected non-fast-forward push rather than an unknown error.
    [[ "$output" =~ "unknown push error; no common ancestor" ]] || false

    run dolt push other main:local_main
    [ "$status" -eq 0 ]
}
