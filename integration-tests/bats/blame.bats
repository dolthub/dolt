#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    setup_repository
}

teardown() {
    assert_feature_version
    teardown_common
}

setup_repository() {
    stash_current_dolt_user

    set_dolt_user "Thomas Foolery", "bats-1@email.fake"
    dolt sql <<SQL
CREATE TABLE blame_test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  name LONGTEXT COMMENT 'tag:1',
  PRIMARY KEY (pk)
);
SQL
    dolt sql -q "insert into blame_test (pk,name) values (1, \"Tom\")"
    dolt add blame_test
    dolt commit -m "create blame_test table"

    set_dolt_user "Richard Tracy", "bats-2@email.fake"
    dolt sql -q "insert into blame_test (pk,name) values (2, \"Richard\")"
    dolt add blame_test
    dolt commit -m "add richard to blame_test"

    set_dolt_user "Harry Wombat", "bats-3@email.fake"
    dolt sql -q "update blame_test set name = \"Harry\" where pk = 2"
    dolt add blame_test
    dolt commit -m "replace richard with harry"

    set_dolt_user "Johnny Moolah", "bats-4@email.fake"
    dolt sql -q "insert into blame_test (pk,name) values (3, \"Alan\"), (4, \"Betty\")"
    dolt add blame_test
    dolt commit -m "add more people to blame_test"

    restore_stashed_dolt_user
}

@test "blame: no arguments shows usage" {
    run dolt blame
    [ "$status" -eq 1 ]
    [[ "$output" =~ "usage" ]] || false
}

@test "blame: annotates a small table with simple history" {
    # should be the same as dolt blame HEAD blame_test
    run dolt blame -- blame_test
    [ "$status" -eq 0 ]

    # TODO: Make these assertions better
    [[ "$output" =~ "Thomas Foolery" ]] || false
    [[ "$output" =~ "create blame_test table" ]] || false
    [[ ! "$output" =~ "Richard Tracy" ]] || false
    [[ ! "$output" =~ "add richard to blame_test" ]] || false
    [[ "$output" =~ "Harry Wombat" ]] || false
    [[ "$output" =~ "replace richard" ]] || false
    [[ "$output" =~ "Johnny Moolah" ]] || false
    [[ "$output" =~ "add more people" ]] || false
}

@test "blame: blames HEAD when commit ref omitted" {
    run dolt blame blame_test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Thomas Foolery" ]] || false
    [[ "$output" =~ "create blame_test table" ]] || false
    [[ ! "$output" =~ "Richard Tracy" ]] || false
    [[ ! "$output" =~ "add richard to blame_test" ]] || false
    [[ "$output" =~ "Harry Wombat" ]] || false
    [[ "$output" =~ "replace richard" ]] || false
    [[ "$output" =~ "Johnny Moolah" ]] || false
    [[ "$output" =~ "add more people" ]] || false
}

@test "blame: works with HEAD as the commit ref" {
    run dolt blame HEAD blame_test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Thomas Foolery" ]] || false
    [[ "$output" =~ "create blame_test table" ]] || false
    [[ ! "$output" =~ "Richard Tracy" ]] || false
    [[ ! "$output" =~ "add richard to blame_test" ]] || false
    [[ "$output" =~ "Harry Wombat" ]] || false
    [[ "$output" =~ "replace richard" ]] || false
    [[ "$output" =~ "Johnny Moolah" ]] || false
    [[ "$output" =~ "add more people" ]] || false
}

@test "blame: works with HEAD~1 as the commit ref" {
    run dolt blame HEAD~1 blame_test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Thomas Foolery" ]] || false
    [[ "$output" =~ "create blame_test table" ]] || false
    [[ ! "$output" =~ "Richard Tracy" ]] || false
    [[ ! "$output" =~ "add richard to blame_test" ]] || false
    [[ "$output" =~ "Harry Wombat" ]] || false
    [[ "$output" =~ "replace richard" ]] || false
    [[ ! "$output" =~ "Johnny Moolah" ]] || false
    [[ ! "$output" =~ "add more people" ]] || false
}

@test "blame: works with HEAD~2 as the commit ref" {
    run dolt blame HEAD~2 blame_test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Thomas Foolery" ]] || false
    [[ "$output" =~ "create blame_test table" ]] || false
    [[ "$output" =~ "Richard Tracy" ]] || false
    [[ "$output" =~ "add richard to blame_test" ]] || false
    [[ ! "$output" =~ "Harry Wombat" ]] || false
    [[ ! "$output" =~ "replace richard" ]] || false
    [[ ! "$output" =~ "Johnny Moolah" ]] || false
    [[ ! "$output" =~ "add more people" ]] || false
}

@test "blame: works with HEAD~3 as the commit ref" {
    run dolt blame HEAD~3 blame_test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Thomas Foolery" ]] || false
    [[ "$output" =~ "create blame_test table" ]] || false
    [[ ! "$output" =~ "Richard Tracy" ]] || false
    [[ ! "$output" =~ "add richard to blame_test" ]] || false
    [[ ! "$output" =~ "Harry Wombat" ]] || false
    [[ ! "$output" =~ "replace richard" ]] || false
    [[ ! "$output" =~ "Johnny Moolah" ]] || false
    [[ ! "$output" =~ "add more people" ]] || false
}

@test "blame: returns an error when the table is not found in the given revision" {
    run dolt blame HEAD~4 blame_test
    [ "$status" -eq 1 ]
    [[ "$output" =~ "no table named blame_test found" ]] || false
}

@test "blame: pk ordered output" {
    run dolt blame blame_test
    [ "$status" -eq 0 ]
    [[ "${lines[3]}" =~ "| 1  | create blame_test table       | Thomas Foolery, |" ]] || false
    [[ "${lines[4]}" =~ "| 2  | replace richard with harry    | Harry Wombat,   |" ]] || false
    [[ "${lines[5]}" =~ "| 3  | add more people to blame_test | Johnny Moolah,  |" ]] || false
    [[ "${lines[6]}" =~ "| 4  | add more people to blame_test | Johnny Moolah,  |" ]] || false
}
