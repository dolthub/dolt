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

@test "blame: too many arguments shows usage" {
    run dolt blame HEAD blame_test foo
    [ "$status" -eq 1 ]
    [[ "$output" =~ "blame has too many positional arguments" ]] || false
}

@test "blame: annotates a small table with simple history" {
    # should be the same as dolt blame HEAD blame_test
    run dolt blame blame_test
    [ "$status" -eq 0 ]

    [[ "${lines[1]}" =~ "pk".*"commit".*"commit_date".*"committer".*"email".*"message" ]] || false
    [[ "$output" =~ "1  |".+"|".+"| Thomas Foolery, | bats-1@email.fake | create blame_test table       |" ]] || false
    [[ ! "$output" =~ "Richard Tracy" ]] || false
    [[ ! "$output" =~ "add richard to blame_test" ]] || false
    [[ "$output" =~ "2  |".+"|".+"| Harry Wombat,   | bats-3@email.fake | replace richard with harry    |" ]] || false
    [[ "$output" =~ "3  |".+"|".+"| Johnny Moolah,  | bats-4@email.fake | add more people to blame_test |" ]] || false
    [[ "$output" =~ "4  |".+"|".+"| Johnny Moolah,  | bats-4@email.fake | add more people to blame_test |" ]] || false
}

@test "blame: blames HEAD when commit ref omitted" {
    run dolt blame blame_test
    [ "$status" -eq 0 ]

    [[ "$output" =~ "1".*"Thomas Foolery".*"create blame_test table" ]] || false
    [[ ! "$output" =~ "Richard Tracy" ]] || false
    [[ ! "$output" =~ "add richard to blame_test" ]] || false
    [[ "$output" =~ "2".*"Harry Wombat".*"replace richard" ]] || false
    [[ "$output" =~ "3".*"Johnny Moolah".*"add more people" ]] || false
    [[ "$output" =~ "4".*"Johnny Moolah".*"add more people" ]] || false
}

@test "blame: works with HEAD as the commit ref" {
    run dolt blame HEAD blame_test
    [ "$status" -eq 0 ]

    [[ "$output" =~ "1".*"Thomas Foolery".*"create blame_test table" ]] || false
    [[ ! "$output" =~ "Richard Tracy" ]] || false
    [[ ! "$output" =~ "add richard to blame_test" ]] || false
    [[ "$output" =~ "2".*"Harry Wombat".*"replace richard" ]] || false
    [[ "$output" =~ "3".*"Johnny Moolah".*"add more people" ]] || false
    [[ "$output" =~ "4".*"Johnny Moolah".*"add more people" ]] || false
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
    skip "SQL views return incorrect data when using AS OF with commits that modify existing data"

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
    [[ "$output" =~ "View 'dolt_repo_$$.dolt_blame_blame_test' references invalid table(s) or column(s) or function(s) or definer/invoker of view lack rights to use them" ]] || false
}

@test "blame: pk ordered output" {
    dolt blame blame_test
    run dolt blame blame_test
    [ "$status" -eq 0 ]
    [[ "${lines[3]}" =~ "| 1  |" ]] || false
    [[ "${lines[3]}" =~ "| Thomas Foolery, | bats-1@email.fake | create blame_test table       |" ]] || false
    [[ "${lines[4]}" =~ "| 2  |" ]] || false
    [[ "${lines[4]}" =~ "| Harry Wombat,   | bats-3@email.fake | replace richard with harry    |" ]] || false
    [[ "${lines[5]}" =~ "| 3  |" ]] || false
    [[ "${lines[5]}" =~ "| Johnny Moolah,  | bats-4@email.fake | add more people to blame_test |" ]] || false
    [[ "${lines[6]}" =~ "| 4  |" ]] || false
    [[ "${lines[6]}" =~ "| Johnny Moolah,  | bats-4@email.fake | add more people to blame_test |" ]] || false
}

@test "blame: composite pk ordered output with correct header" {
    dolt sql -q "create table t(pk varchar(20), val int)"
    run dolt sql -q "ALTER TABLE t ADD PRIMARY KEY (pk, val)"
    [ "$status" -eq 0 ]

    dolt sql -q "INSERT INTO t VALUES ('zzz',4),('mult',1),('sub',2),('add',5)"
    dolt add .
    dolt commit -m "add rows"

    dolt sql -q "INSERT INTO t VALUES ('dolt',0),('alt',12),('del',8),('ctl',3)"
    dolt add .
    dolt commit -m "add more rows"

    dolt blame t
    run dolt blame t
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "| pk   | val |" ]] || false
    [[ "${lines[3]}" =~ "| add  | 5   |" ]] || false
    [[ "${lines[4]}" =~ "| alt  | 12  |" ]] || false
    [[ "${lines[5]}" =~ "| ctl  | 3   |" ]] || false
    [[ "${lines[6]}" =~ "| del  | 8   |" ]] || false
    [[ "${lines[7]}" =~ "| dolt | 0   |" ]] || false
    [[ "${lines[8]}" =~ "| mult | 1   |" ]] || false
    [[ "${lines[9]}" =~ "| sub  | 2   |" ]] || false
    [[ "${lines[10]}" =~ "| zzz  | 4   |" ]] || false
}
