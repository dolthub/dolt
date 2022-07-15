#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}

@test "conflict-cat: smoke test print output" {
    dolt sql <<SQL
CREATE table t (pk int PRIMARY KEY, col1 int);
INSERT INTO t VALUES (1, 1);
INSERT INTO t VALUES (2, 2);
INSERT INTO t VALUES (3, 3);
SQL
    dolt commit -am 'create table with rows'

    dolt checkout -b other
    dolt sql <<SQL
UPDATE t set col1 = 3 where pk = 1;
UPDATE t set col1 = 0 where pk = 2;
DELETE FROM t where pk = 3;
INSERT INTO t VALUES (4, -4);
SQL
    dolt commit -am 'right edit'

    dolt checkout main
    dolt sql <<SQL
UPDATE t set col1 = 2 where pk = 1;
DELETE FROM t where pk = 2;
UPDATE t set col1 = 0 where pk = 3;
INSERT INTO t VALUES (4, 4);
SQL
    dolt commit -am 'left edit'
    dolt merge other

    # trick to disable colors
    dolt conflicts cat . > output.txt
    run cat output.txt
    [[ $output =~ "|     | base   | 1  | 1    |" ]]
    [[ $output =~ "|  *  | ours   | 1  | 2    |" ]]
    [[ $output =~ "|  *  | theirs | 1  | 3    |" ]]
    [[ $output =~ "|     | base   | 2  | 2    |" ]]
    [[ $output =~ "|  -  | ours   | 2  | 2    |" ]]
    [[ $output =~ "|  *  | theirs | 2  | 0    |" ]]
    [[ $output =~ "|     | base   | 3  | 3    |" ]]
    [[ $output =~ "|  *  | ours   | 3  | 0    |" ]]
    [[ $output =~ "|  -  | theirs | 3  | 3    |" ]]
    [[ $output =~ "|  +  | ours   | 4  | 4    |" ]]
    [[ $output =~ "|  +  | theirs | 4  | -4   |" ]]
}

@test "conflict-cat: conflicts should show using the union-schema (new schema on right)" {
    dolt sql -q "CREATE TABLE t (a INT PRIMARY KEY, b INT);"
    dolt commit -am "base"

    dolt checkout -b right
    dolt sql <<SQL
ALTER TABLE t ADD c INT;
INSERT INTO t VALUES (1, 2, 1);
SQL
    dolt commit -am "right"

    dolt checkout main
    dolt sql -q "INSERT INTO t values (1, 3);"
    dolt commit -am "left"

    dolt merge right

    run dolt conflicts cat .
    [[ "$output" =~ "| a" ]]
    [[ "$output" =~ "| b" ]]
    [[ "$output" =~ "| c" ]]
}

@test "conflict-cat: conflicts should show using the union-schema (new schema on left)" {
    dolt sql -q "CREATE TABLE t (a INT PRIMARY KEY, b INT);"
    dolt commit -am "base"

    dolt checkout -b right
    dolt sql -q "INSERT INTO t values (1, 2);"
    dolt commit -am "right"

    dolt checkout main
    dolt sql <<SQL
ALTER TABLE t ADD c INT;
INSERT INTO t VALUES (1, 3, 1);
SQL
    dolt commit -am "left"
    dolt merge right

    run dolt conflicts cat .
    [[ "$output" =~ "| a" ]]
    [[ "$output" =~ "| b" ]]
    [[ "$output" =~ "| c" ]]
}
