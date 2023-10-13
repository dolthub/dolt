#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}

@test "conflict-cat: smoke test print schema output" {
    dolt sql << SQL
CREATE TABLE people (
  id INT NOT NULL,
  last_name VARCHAR(120),
  first_name VARCHAR(120),
  birthday DATETIME(6),
  age INT DEFAULT '0',
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;
SQL
    dolt add .
    dolt commit -am "base"

    dolt checkout -b right
    dolt sql <<SQL
ALTER TABLE people
MODIFY COLUMN age FLOAT;
SQL
    dolt commit -am "right"

    dolt checkout main
    dolt sql <<SQL
ALTER TABLE people
MODIFY COLUMN age BIGINT;
SQL
    dolt commit -am "left"

    run dolt merge right -m "merge right"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONFLICT (schema):" ]] || false

    run dolt conflicts cat .
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| our_schema" ]] || false
    [[ "$output" =~ "| their_schema" ]] || false
    [[ "$output" =~ "| base_schema" ]] || false
    [[ "$output" =~ "| description" ]] || false
    [[ "$output" =~ "different column definitions for our column age and their column age" ]] || false
    [[ "$output" =~ "\`age\` bigint," ]] || false
    [[ "$output" =~ "\`age\` float," ]] || false
    [[ "$output" =~ "\`age\` int DEFAULT '0'," ]] || false
}

@test "conflict-cat: smoke test print data output" {
    dolt sql <<SQL
CREATE table t (pk int PRIMARY KEY, col1 int);
INSERT INTO t VALUES (1, 1);
INSERT INTO t VALUES (2, 2);
INSERT INTO t VALUES (3, 3);
SQL
    dolt add .
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
    run dolt merge other -m "merge other"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONFLICT (content):" ]] || false

    # trick to disable colors
    dolt conflicts cat . > output.txt
    run cat output.txt
    [[ $output =~ "|     | base   | 1  | 1    |" ]] || false
    [[ $output =~ "|  *  | ours   | 1  | 2    |" ]] || false
    [[ $output =~ "|  *  | theirs | 1  | 3    |" ]] || false
    [[ $output =~ "|     | base   | 2  | 2    |" ]] || false
    [[ $output =~ "|  -  | ours   | 2  | 2    |" ]] || false
    [[ $output =~ "|  *  | theirs | 2  | 0    |" ]] || false
    [[ $output =~ "|     | base   | 3  | 3    |" ]] || false
    [[ $output =~ "|  *  | ours   | 3  | 0    |" ]] || false
    [[ $output =~ "|  -  | theirs | 3  | 3    |" ]] || false
    [[ $output =~ "|  +  | ours   | 4  | 4    |" ]] || false
    [[ $output =~ "|  +  | theirs | 4  | -4   |" ]] || false
}

@test "conflict-cat: conflicts should show using the union-schema (new schema on right)" {
    dolt sql -q "CREATE TABLE t (a INT PRIMARY KEY, b INT);"
    dolt add .
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

    run dolt merge right -m "merge right"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONFLICT (content):" ]] || false

    run dolt conflicts cat .
    [[ "$output" =~ "| a" ]] || false
    [[ "$output" =~ "| b" ]] || false
    [[ "$output" =~ "| c" ]] || false
}

@test "conflict-cat: conflicts should show using the union-schema (new schema on left)" {
    dolt sql -q "CREATE TABLE t (a INT PRIMARY KEY, b INT);"
    dolt add .
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
    run dolt merge right -m "merge left"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONFLICT (content):" ]] || false

    run dolt conflicts cat .
    [[ "$output" =~ "| a" ]] || false
    [[ "$output" =~ "| b" ]] || false
    [[ "$output" =~ "| c" ]] || false
}
