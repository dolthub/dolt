#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    skip_nbf_dolt_1
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
  pk1 BIGINT NOT NULL COMMENT 'tag:0',
  pk2 TEXT,
  name LONGTEXT COMMENT 'tag:1',
  PRIMARY KEY (pk1, pk2)
);
SQL
    dolt sql -q "insert into blame_test values (1, 'one', 'Tom')"
    dolt add blame_test
    dolt commit -m "create blame_test table"

    set_dolt_user "Richard Tracy", "bats-2@email.fake"
    dolt sql -q "insert into blame_test values (2, 'two', 'Richard')"
    dolt add blame_test
    dolt commit -m "add richard to blame_test"

    set_dolt_user "Harry Wombat", "bats-3@email.fake"
    dolt sql -q "update blame_test set name = 'Harry' where pk1 = 2"
    dolt add blame_test
    dolt commit -m "replace richard with harry"

    set_dolt_user "Johnny Moolah", "bats-4@email.fake"
    dolt sql -q "insert into blame_test values (3, 'three', 'Alan'), (4, 'four', 'Betty')"
    dolt add blame_test
    dolt commit -m "add more people to blame_test"

    restore_stashed_dolt_user
}

@test "blame-system-view: view works for table with single element primary key" {
    dolt sql -q "alter table blame_test drop primary key"
    dolt commit -am "dropped primary key"

    dolt sql -q "alter table blame_test add primary key (pk1)"
    dolt commit -am "added a new primary key"

    # BUG: Altering the primary key on a table was leaving behind metadata. SuperSchema was pulling the schema
    #      from the last commit, then adding the updated columns, but never took account of any primary key changes,
    #      so, the col primary key metadata persisted even after dropping and adding a different primary key.
    #      After changing SuperSchema to account for primary key changes the column prikey metadata changes correctly,
    #      but dolt_diff_<TABLE> is emptied out and loses all data.
    #
    # TODO: After fixing that bug to keep the data around, we can re-enable this test.
    skip "BUG: Altering primary key drops all data from dolt_diff_<table>"

    run dolt sql -q "select * from dolt_blame_blame_test;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ pk1[[:space:]]*|[[:space:]]commit[[:space:]]*|[[:space:]]commit_date[[:space:]]*|[[:space:]]committer[[:space:]]*|[[:space:]]email[[:space:]]*|[[:space:]]message ]] || false

    [[ "$output" =~ "Thomas Foolery" ]] || false
    [[ "$output" =~ "Harry Wombat" ]] || false
    [[ "$output" =~ "Johnny Moolah" ]] || false
    [[ ! "$output" =~ "Richard Tracy" ]] || false
}

@test "blame-system-view: view works for table with compound primary key" {
    run dolt sql -q "select * from dolt_blame_blame_test;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ pk1[[:space:]]*|[[:space:]]pk2[[:space:]]*|[[:space:]]commit[[:space:]]*|[[:space:]]commit_date[[:space:]]*|[[:space:]]committer[[:space:]]*|[[:space:]]email[[:space:]]*|[[:space:]]message ]] || false

    [[ "$output" =~ "Thomas Foolery" ]] || false
    [[ "$output" =~ "Harry Wombat" ]] || false
    [[ "$output" =~ "Johnny Moolah" ]] || false
    [[ ! "$output" =~ "Richard Tracy" ]] || false
}

@test "blame-system-view: correct error message for table with no primary key" {
    dolt sql -q "create table no_pks (a int, b text, c datetime);"
    dolt sql -q "insert into no_pks values (1, 'one', null), (2, 'two', NOW());"

    run dolt sql -q "select * from dolt_blame_no_pks;"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "unable to generate blame view" ]] || false
}

@test "blame-system-view: view can be described" {
    run dolt sql -q "describe dolt_blame_blame_test;"
    [ "$status" -eq 0 ]

    # TODO: go-mysql-server doesn't currently support describing views:
    #       https://github.com/dolthub/go-mysql-server/issues/787
    #       Enable this test when that issue is fixed
    skip "BUG: views can't currently be described"

    [[ "$output" =~ "| pk1          | bigint         | NO   | PRI |         |       |" ]]
    [[ "$output" =~ "| pk1          | longtext       | NO   | PRI |         |       |" ]]
    [[ "$output" =~ "| name         | longtext       | NO   |     |         |       |" ]]
    [[ "$output" =~ "| commit       | varchar(16383) | NO   |     |         |       |" ]]
    [[ "$output" =~ "| commit_date  | datetime       | NO   |     |         |       |" ]]
    [[ "$output" =~ "| committer    | text           | NO   |     |         |       |" ]]
    [[ "$output" =~ "| email        | text           | NO   |     |         |       |" ]]
    [[ "$output" =~ "| message      | text           | NO   |     |         |       |" ]]
}

@test "blame-system-view: view is not included in show tables output" {
    run dolt sql -q "show tables;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "blame_test" ]] || false
    [[ ! "$output" =~ "dolt_blame_blame_test" ]] || false

    # Check again after using the dolt_blame table
    dolt sql -q "select * from dolt_blame_blame_test;"
    run dolt sql -q "show tables;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "blame_test" ]] || false
    [[ ! "$output" =~ "dolt_blame_blame_test" ]] || false
}

@test "blame-system-view: view has a deterministic order" {
    for i in {0..5}
    do
        run dolt sql -q "select * from dolt_blame_blame_test;"
        [ "$status" -eq 0 ]

        lines=()
        while read -r line; do
           lines+=("$line")
        done <<< "$output"

        [ "${#lines[@]}" -eq 8 ] || false
        [[ "${lines[3]}" =~ ^\|[[:space:]]1[[:space:]]+\|[[:space:]]one ]] || false
        [[ "${lines[4]}" =~ ^\|[[:space:]]2[[:space:]]+\|[[:space:]]two ]] || false
        [[ "${lines[5]}" =~ ^\|[[:space:]]3[[:space:]]+\|[[:space:]]three ]] || false
        [[ "${lines[6]}" =~ ^\|[[:space:]]4[[:space:]]+\|[[:space:]]four ]] || false
    done
}

@test "blame-system-view: case insensitive dolt_blame view names" {
    run dolt sql -q "select * from DOLT_BLAME_BLAME_TEST"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Thomas Foolery" ]] || false

    run dolt sql -q "select * from DOLT_blame_BLAME_TEST"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Thomas Foolery" ]] || false

    run dolt sql -q "select * from DOLT_BLAME_BLAME_test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Thomas Foolery" ]] || false

    run dolt sql -q "select * from dOlT_bLaMe_BlAmE_tEsT"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Thomas Foolery" ]] || false
}

@test "blame-system-view: invalid dolt_blame view names" {
    run dolt sql -q "select * from dolt_blame_"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "table not found" ]] || false

    run dolt sql -q "select * from dolt_blame_not_a_table"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "table not found" ]] || false
}

@test "blame-system-view: blame does not show working set changes" {
    dolt sql -q "update blame_test set pk2='working set changes...'"

    run dolt sql -q "select * from dolt_blame_blame_test"
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "working set changes..." ]] || false
}

@test "blame-system-view: view works when a table is deleted and recreated" {
    # BUG: This test is affected by the same bug mentioned above in the single element primary key test.
    #      When a table is dropped and recreated, dolt_diff_<table> loses its data so we can't see history anymore.
    #
    # TODO: Enable this test when the bug above is fixed.
    skip "BUG: Dropping and recreating a table causes all existing data in dolt_diff_<table> to "

    stash_current_dolt_user
    set_dolt_user "Danny Deleter", "dropper@email.fake"
    dolt sql -q "drop table blame_test"
    dolt commit -am "dropped the blame_test table"

    set_dolt_user "Carl Creator", "creator@email.fake"
    dolt sql -q "create table blame_test (pk1 int primary key, name text, code int)"
    dolt sql -q "insert into blame_test values (100, 'oliver', 1)"
    dolt commit -am "added a new blame_test table"

    set_dolt_user "Ingrid Inserter", "inserter@email.fake"
    dolt sql -q "insert into blame_test values (101, 'spencer', 0), (103, 'jules', 1)"
    dolt commit -am "added more blame_test data"
    restore_stashed_dolt_user

    run dolt sql -q "select * from dolt_blame_blame_test"
    [ "$status" -eq 0 ]
    echo -e "OUTPUT:\n $output"
    false
}