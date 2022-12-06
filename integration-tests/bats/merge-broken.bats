#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

# These tests describe how merge is broken today. New format only.

setup() {
    setup_common
    skip_nbf_not_dolt
    dolt sql <<SQL
CREATE TABLE t (
  pk int PRIMARY KEY
);
SQL
    dolt commit -Am "added table t"
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "merge-broken: dropping columns" {
    dolt sql -q "alter table t add column col1 int;"
    dolt sql -q "alter table t add column col2 int;"

    dolt sql -q "insert into t values (1, 10, 100), (2, 20, 200);"
    dolt commit -am "added column with data"

    dolt checkout -b other
    dolt sql -q "alter table t drop column col1;"
    dolt sql -q "insert into t (pk, col2) values (3, 300), (4, 400);"
    dolt commit -am "added more data"

    dolt checkout main
    dolt sql -q "insert into t (pk, col1, col2) values (5, 50, 500), (6, 60, 600);"
    dolt commit -am "dropped column and added data"

    dolt merge other
    run dolt sql -r csv -q "select pk, col2 from t;"
    [ $status -eq 0 ]
    [[ $output =~ "1,100" ]] || false
    [[ $output =~ "2,200" ]] || false
    [[ $output =~ "3,300" ]] || false
    [[ $output =~ "4,400" ]] || false

    skip "outputs 5,50 and 6,60"
    [[ $output =~ "5,500" ]] || false
    [[ $output =~ "6,600" ]] || false
}

@test "merge-broken: adding different columns to both sides" {
    dolt sql -q "insert into t values (1), (2);"
    dolt commit -Am "added data"

    dolt checkout -b other
    dolt sql -q "alter table t add column col2 int;"
    dolt sql -q "insert into t values (3, 300), (4, 400);"
    dolt commit -Am "added column with data on right"

    dolt checkout main
    dolt sql -q "alter table t add column col1 int;"
    dolt sql -q "insert into t values (5, 50), (6, 60);"
    dolt commit -Am "added column with data on left"

    dolt merge other

    run dolt sql -r csv -q "select pk, col1, col2 from t;"
    [ $status -eq 0 ]
    [[ $output =~ "1,," ]] || false
    [[ $output =~ "2,," ]] || false
    [[ $output =~ "5,50," ]] || false
    [[ $output =~ "6,60," ]] || false

    skip "output incorrect: 300 is in the wrong column"
    [[ $output =~ "3,,300" ]] || false
    [[ $output =~ "4,,400" ]] || false
}

@test "merge-broken: re-ordering columns" {
    dolt sql -q "alter table t add column col1 int;"
    dolt sql -q "alter table t add column col2 int;"
    dolt sql -q "insert into t values (1, 10, 100), (2, 20, 200);"
    dolt commit -am "added columns with data"

    dolt checkout -b other
    dolt sql -q "alter table t drop column col1;"
    dolt sql -q "alter table t add column col1 int;"
    dolt sql -q "insert into t (pk, col2, col1) values (3, 300, 30), (4, 400, 40);"
    dolt commit -am "move column one to end"

    dolt checkout main
    dolt sql -q "insert into t (pk, col1, col2) values (5, 50, 500), (6, 60, 600);"
    dolt commit -am "reordered columns and added data"

    dolt merge other

    run dolt sql -r csv -q "select pk, col1, col2 from t;"
    [ $status -eq 0 ]
    [[ $output =~ "1,,100" ]] || false
    [[ $output =~ "2,,200" ]] || false
    [[ $output =~ "3,30,300" ]] || false
    [[ $output =~ "4,40,400" ]] || false

    skip "outputs 5,500,50 and 6,600,60"
    [[ $output =~ "5,50,500" ]]
    [[ $output =~ "6,60,600" ]]
}

@test "merge-broken: changing the type of a column" {
    dolt sql -q "alter table t add column col1 int;"
    dolt sql -q "insert into t values (1, 10), (2, 20);"
    dolt commit -am "initial"

    dolt checkout -b other
    dolt sql -q "alter table t modify column col1 varchar(100);"
    dolt sql -q "insert into t values (3, 'thirty'), (4, 'forty');"
    dolt commit -am "changed type"

    dolt checkout main
    dolt sql -q "insert into t values (5, 50), (6, 60);"
    dolt commit -am "left"

    dolt merge other

    run dolt sql -r csv -q "select pk, col1 from t;"
    [ $status -eq 0 ]
    [[ $output =~ "1,10" ]] || false
    [[ $output =~ "2,20" ]] || false
    [[ $output =~ "3,thirty" ]] || false
    [[ $output =~ "4,forty" ]] || false

    skip "outputs garbled text"
    [[ $output =~ "5,50" ]] || false
    [[ $output =~ "6,60" ]] || false
}

@test "merge-broken: adding a not-null constraint with default to a column" {
    dolt sql -q "alter table t add column col1 int;"
    dolt sql -q "insert into t values (1, null), (2, null);"
    dolt commit -am "initial"

    dolt checkout -b other
    dolt sql -q "update t set col1 = 9999 where col1 is null;"
    dolt sql -q "alter table t modify column col1 int not null default 9999;"
    dolt sql -q "insert into t values (3, 30), (4, 40);"
    dolt commit -am "added not-null constraint with default"

    dolt checkout main
    dolt sql -q "insert into t values (5, null), (6, null);"
    dolt commit -am "added data"

    dolt merge other

    run dolt sql -r csv -q "select pk, col1 from t;"
    [ $status -eq 0 ]
    [[ $output =~ "1,9999" ]] || false
    [[ $output =~ "2,9999" ]] || false
    [[ $output =~ "3,30" ]] || false
    [[ $output =~ "4,40" ]] || false

    skip "garbled"
    [[ $output =~ "5,9999" ]] || false
    [[ $output =~ "6,9999" ]] || false
}

