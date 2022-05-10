#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    skip_nbf_dolt_1
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "primary-key-changes: add primary key using null values" {
    dolt sql -q "create table t(pk int, val int)"
    dolt sql -q "INSERT INTO t (val) VALUES (1)"
    run dolt sql -q "ALTER TABLE t ADD PRIMARY KEY (pk)"
    [ "$status" -eq 1 ]
}

@test "primary-key-changes: add single primary key" {
    dolt sql -q "create table t(pk int, val int)"
    run dolt sql -q "ALTER TABLE t ADD PRIMARY KEY (pk)"
    [ "$status" -eq 0 ]

    dolt sql -q "INSERT INTO t VALUES (1,1),(2,2),(3,3)"
    run dolt sql -q "SELECT * FROM t" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,3" ]] || false

    run dolt sql -q "INSERT INTO t VALUES (2,4)"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "duplicate primary key" ]] || false
}

@test "primary-key-changes: add composite primary key" {
    dolt sql -q "create table t(pk int, val int)"
    run dolt sql -q "ALTER TABLE t ADD PRIMARY KEY (pk, val)"
    [ "$status" -eq 0 ]

    dolt sql -q "INSERT INTO t VALUES (1,1),(2,2),(3,3)"
    run dolt sql -q "SELECT * FROM t" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,3" ]] || false

    run dolt sql -q "INSERT INTO t VALUES (2, 2)"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "duplicate primary key" ]] || false

    run dolt sql -q "INSERT INTO t VALUES (2, 3)"
    [ "$status" -eq 0 ]

    run dolt sql -q "SELECT * FROM t" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "2,3" ]] || false
    [[ "$output" =~ "3,3" ]] || false

    run dolt sql -q "SELECT COUNT(*) FROM t" -r csv
    [[ "$output" =~ "4" ]] || false
}

@test "primary-key-changes: can delete single primary key" {
    dolt sql -q "create table t(pk int, val int, PRIMARY KEY(pk))"
    run dolt sql -q "ALTER TABLE t DROP PRIMARY KEY"
    [ "$status" -eq 0 ]

    dolt sql -q "INSERT INTO t VALUES (1,1),(2,2),(2,2)"
    run dolt sql -q "SELECT * FROM t" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "1,1" ]] || false
    [[ "${lines[2]}" =~ '2,2' ]] || false
    [[ "${lines[3]}" =~ '2,2' ]] || false

    run dolt sql -q "INSERT INTO t VALUES (2, 2)"
    [ "$status" -eq 0 ]

    dolt sql -q "drop table t"
    dolt sql -q "create table t(pk int, val int, PRIMARY KEY(pk))"
    dolt sql -q "insert into t values (1, 1)"
    dolt sql -q "alter table t drop primary key"

    run dolt sql -q "SELECT * FROM t" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,1" ]] || false

    run dolt sql -q "describe t;"
    ! [[ "$output" =~ "PRI" ]] || false
}

@test "primary-key-changes: can delete composite primary key" {
    dolt sql -q "create table t(pk int, val int, PRIMARY KEY(pk, val))"
    run dolt sql -q "ALTER TABLE t DROP PRIMARY KEY"
    [ "$status" -eq 0 ]

    dolt sql -q "INSERT INTO t VALUES (1,1),(2,2),(2,2)"
    run dolt sql -q "SELECT * FROM t ORDER BY pk" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "1,1" ]] || false
    [[ "${lines[2]}" =~ "2,2" ]] || false
    [[ "${lines[3]}" =~ "2,2" ]] || false

    run dolt sql -q "describe t;"
    ! [[ "$output" =~ "PRI" ]] || false
}

@test "primary-key-changes: run through some add and drop primary key operations" {
    dolt sql -q "create table t(pk int, val int, PRIMARY KEY(pk, val))"
    run dolt sql -q "ALTER TABLE t DROP PRIMARY KEY"
    [ "$status" -eq 0 ]

    dolt sql -q "INSERT INTO t VALUES (1,1),(2,2)"
    run dolt sql -q "SELECT * FROM t" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false

    run dolt sql -q "ALTER TABLE t ADD PRIMARY KEY (pk)"
    [ "$status" -eq 0 ]

    run dolt sql -q "INSERT INTO t values (1, 5)"
    [ "$status" -eq 1 ]

    dolt sql -q "INSERT INTO t VALUES (3,3)"
    run dolt sql -q "SELECT * FROM t" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,3" ]] || false
}

@test "primary-key-changes: add an index after dropping a key, and then recreate the key" {
   dolt sql -q "create table t(pk int, val int, primary key (pk, val));"
   dolt sql -q "insert into t values (1,1);"
   run dolt sql -q "alter table t drop primary key;"
   [ "$status" -eq 0 ]

   run dolt sql -q "select * from t;" -r csv
   [ "$status" -eq 0 ]
   [[ "$output" =~ "1,1" ]] || false

   run dolt sql -q "alter table t add primary key (pk, val);"
   [ "$status" -eq 0 ]

   run dolt sql -q "select * from t;" -r csv
   [ "$status" -eq 0 ]
   [[ "$output" =~ "1,1" ]] || false

   run dolt sql -q "alter table t drop primary key;"
   [ "$status" -eq 0 ]

   run dolt sql -q "select * from t;" -r csv
   [ "$status" -eq 0 ]
   [[ "$output" =~ "1,1" ]] || false

   run dolt sql -q "alter table t add index (val);"
   [ "$status" -eq 0 ]

   run dolt sql -q "alter table t add primary key (pk);"
   [  "$status" -eq 0 ]
}

@test "primary-key-changes: alter table on keyless column with duplicates throws an error" {
    dolt sql -q "create table t(pk int, val int)"
    dolt sql -q "insert into t values (1,1),(1,1)"

    run dolt sql -q "ALTER TABLE t ADD PRIMARY KEY (pk, val)"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "duplicate primary key given: [1,1]" ]] || false

    run dolt sql -q "ALTER TABLE t ADD PRIMARY KEY (pk)"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "duplicate primary key given: [1]" ]] || false
}

@test "primary-key-changes: dropping a primary key still preserves secondary indexes" {
    dolt sql -q "create table t(pk int PRIMARY KEY, val1 int, val2 int);"
    dolt sql -q "alter table t add index (val2)"

    dolt sql -q "insert into t values (1,1,1), (2,2,2)"
    run dolt sql -q "ALTER TABLE t DROP PRIMARY KEY"
    [ "$status" -eq 0 ]

    run dolt sql -q "SELECT * FROM t ORDER BY pk" -r csv
    [[ "$output" =~ "1,1,1" ]] || false
    [[ "$output" =~ "2,2,2" ]] || false

    run dolt sql -q "INSERT INTO t VALUES (3, 3, 3)"
    [ "$status" -eq 0 ]

    run dolt sql -q "SELECT * FROM t ORDER BY pk" -r csv
    [[ "$output" =~ "1,1,1" ]] || false
    [[ "$output" =~ "2,2,2" ]] || false
    [[ "$output" =~ "3,3,3" ]] || false

    run dolt sql -q "describe t"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "MUL" ]] || false
}

@test "primary-key-changes: drop on table with no primary key correctly errors" {
     dolt sql -q "create table t(pk int, val1 int, val2 int);"

     run dolt sql -q "ALTER TABLE t DROP PRIMARY KEY"
     [ "$status" -eq 1 ]
     [[ "$output" =~ "error: can't drop 'PRIMARY'; check that column/key exists" ]] || false
}

@test "primary-key-changes: drop primary key with auto increment throws an error" {
    dolt sql -q "create table t(pk int PRIMARY KEY AUTO_INCREMENT, val1 int, val2 int)"
    run dolt sql -q "alter table t drop primary key"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "error: incorrect table definition: there can be only one auto column and it must be defined as a key" ]] || false
}

@test "primary-key-changes: ff merge with primary key schema differences correctly works" {
    dolt sql -q "create table t(pk int PRIMARY KEY, val1 int, val2 int)"
    dolt sql -q "INSERT INTO t values (1,1,1)"

    dolt commit -am "cm1"
    dolt checkout -b test
    dolt sql -q "ALTER TABLE t drop PRIMARY key"
    dolt add .
    dolt commit -am "cm2"
    dolt checkout main

    run dolt merge test
    [ "$status" -eq 0 ]

    run dolt sql -q "describe t;"
    ! [[ "$output" =~ 'PRI' ]] || false
}

@test "primary-key-changes: merge on branch with primary key dropped throws an error" {
    dolt sql -q "create table t(pk int PRIMARY KEY, val1 int, val2 int)"
    dolt sql -q "INSERT INTO t values (1,1,1)"
    dolt commit -am "cm1"
    dolt checkout -b test

    dolt sql -q "ALTER TABLE t drop PRIMARY key"
    dolt add .

    run dolt status
    [[ "$output" =~ 'Changes to be committed' ]] || false
    [[ "$output" =~ ([[:space:]]*modified:[[:space:]]*t) ]] || false
    ! [[ "$output" =~ 'deleted' ]] || false
    ! [[ "$output" =~ 'new table' ]] || false

    dolt commit -m "cm2"
    dolt checkout main

    dolt sql -q "INSERT INTO t values (2,2,2)"
    dolt commit -am "cm3"

    run dolt merge test
    [ "$status" -eq 1 ]
    [[ "$output" =~ 'error: cannot merge two tables with different primary key sets' ]] || false
}

@test "primary-key-changes: merge on branch with primary key added throws an error" {
    dolt sql -q "create table t(pk int, val1 int, val2 int)"
    dolt sql -q "INSERT INTO t values (1,1,1)"
    dolt commit -am "cm1"
    dolt checkout -b test

    dolt sql -q "ALTER TABLE t add PRIMARY key (pk)"
    dolt add .

    run dolt status
    [[ "$output" =~ 'Changes to be committed' ]] || false
    [[ "$output" =~ ([[:space:]]*modified:[[:space:]]*t) ]] || false
    ! [[ "$output" =~ 'deleted' ]] || false
    ! [[ "$output" =~ 'new table' ]] || false

    dolt commit -m "cm2"
    dolt checkout main

    dolt sql -q "INSERT INTO t values (2,2,2)"
    dolt commit -am "cm3"

    run dolt merge test
    [ "$status" -eq 1 ]
    [[ "$output" =~ 'error: cannot merge two tables with different primary key sets' ]] || false
}

@test "primary-key-changes: diff on primary key schema change shows schema level diff but does not show row level diff" {
    dolt sql -q "CREATE TABLE t (pk int PRIMARY KEY, val int)"
    dolt sql -q "INSERT INTO t VALUES (1, 1)"
    dolt commit -am "cm1"

    run dolt sql -q "ALTER TABLE t DROP PRIMARY key"
    [ "$status" -eq 0 ]

    dolt diff
    run dolt diff
    [ "$status" -eq 0 ]
    [[ "$output" =~ '-  `val` int,' ]] || false
    [[ "$output" =~ '-  PRIMARY KEY (`pk`)' ]] || false
    [[ "$output" =~ '+  `val` int' ]] || false

    # Make sure there is no data diff
    dolt diff --data
    run dolt diff --data
    [ "$status" -eq 0 ]
    [[ "$output" =~ "warning: skipping data diff due to primary key set change" ]] || false
}

@test "primary-key-changes: diff on composite schema" {
    dolt sql -q "CREATE TABLE t (pk int PRIMARY KEY, val int)"
    dolt sql -q "INSERT INTO t VALUES (1, 1)"
    dolt commit -am "cm1"

    run dolt sql -q "ALTER TABLE t DROP PRIMARY key"
    [ "$status" -eq 0 ]

    run dolt sql -q "ALTER TABLE t ADD PRIMARY KEY (pk, val)"
    [ "$status" -eq 0 ]

    dolt diff
    run dolt diff
    [ "$status" -eq 0 ]
    [[ "$output" =~ '-  `val` int,' ]] || false
    [[ "$output" =~ '-  PRIMARY KEY (`pk`)' ]] || false
    [[ "$output" =~ '+  `val` int NOT NULL,' ]] || false
    [[ "$output" =~ '+  PRIMARY KEY (`pk`,`val`)' ]] || false

    # Make sure there is not a data diff or summary diff
    dolt diff --data
    run dolt diff --data
    [ "$status" -eq 0 ]
    [[ "$output" =~ "warning: skipping data diff due to primary key set change" ]] || false

    run dolt diff --summary
    [ "$status" -eq 1 ]
    [[ "$output" =~ "diff summary will not compute due to primary key set change with table t" ]] || false

    dolt add .

    run dolt status
    [[ "$output" =~ 'Changes to be committed' ]] || false
    [[ "$output" =~ ([[:space:]]*modified:[[:space:]]*t) ]] || false
    ! [[ "$output" =~ 'deleted' ]] || false
    ! [[ "$output" =~ 'new table' ]] || false

    dolt commit -am "add changes"
    run dolt status
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false
}

@test "primary-key-changes: dolt diff on working set shows correct status diff" {
    dolt sql -q "CREATE TABLE t (pk int PRIMARY KEY, val int)"
    dolt sql -q "INSERT INTO t VALUES (1, 1)"
    run dolt sql -q "ALTER TABLE t DROP PRIMARY key"
    [ "$status" -eq 0 ]

    run dolt status
    [[ "$output" =~ 'Untracked files' ]] || false
    [[ "$output" =~ ([[:space:]]*new table:[[:space:]]*t) ]] || false
    ! [[ "$output" =~ 'deleted' ]] || false
    ! [[ "$output" =~ 'modified' ]] || false

    dolt diff
    run dolt diff
    [[ "$output" =~ 'added table' ]] || false
}

@test "primary-key-changes: dolt diff table returns top-down diff until schema change" {
    dolt sql -q "CREATE TABLE t (pk int PRIMARY KEY, val int)"
    dolt sql -q "INSERT INTO t VALUES (1, 1)"

    dolt add .
    dolt commit -m "cm1"

    dolt sql -q "ALTER TABLE t DROP PRIMARY KEY"
    dolt sql -q "INSERT INTO t values (2,2)"

    dolt add .
    dolt commit -m "cm2"

    # run the diff command and validate the appropriate warning is there
    run dolt sql << SQL
SELECT COUNT(*) from dolt_diff_t;
SHOW WARNINGS;
SQL
    [ "$status" -eq 0 ]
    [[ "$output" =~ '| 0' ]] || false
    [[ "$output" =~ 'cannot render full diff between commits' ]] || false

    dolt sql -q "INSERT INTO t values (3,3)"
    dolt commit -am "cm3"

    dolt sql -q "INSERT INTO t values (4,4)"
    dolt commit -am "cm4"

    run dolt sql << SQL
SELECT COUNT(DISTINCT to_commit) from dolt_diff_t;
SHOW WARNINGS;
SQL
    [ "$status" -eq 0 ]
    [[ "$output" =~ '| 2' ]] || false
    [[ "$output" =~ 'cannot render full diff between commits' ]] || false

    run dolt sql -q "SELECT to_val,to_pk,from_val,from_pk FROM dolt_diff_t" -r csv
    [[ "$output" =~ '3,3,' ]] || false
    [[ "$output" =~ '4,4,' ]] || false
}

@test "primary-key-changes: test whether dolt_commit_diff correctly returns a diff whether there is or isn't a schema change" {
    dolt sql -q "CREATE TABLE t (pk int PRIMARY KEY, val int)"
    dolt commit -am "cm0"

    dolt sql -q "INSERT INTO t VALUES (1, 1)"
    dolt commit -am "cm1"

    dolt sql -q "ALTER TABLE t DROP PRIMARY KEY"
    dolt sql -q "INSERT INTO t values (2,2)"
    dolt commit -am "cm2"

    dolt sql -q "INSERT INTO t values (3,3)"
    dolt commit -am "cm3"

    dolt sql -q "INSERT INTO t values (4,4)"
    dolt commit -am "cm4"

    # run the diff command and validate the appropriate warning is there
    run dolt sql << SQL
SELECT COUNT(DISTINCT to_commit) from dolt_commit_diff_t where from_commit=HASHOF('HEAD~3') and to_commit=HASHOF('HEAD');
SHOW WARNINGS;
SQL
    [ "$status" -eq 0 ]
    [[ "$output" =~ '| 0' ]] || false
    [[ "$output" =~ 'cannot render full diff between commits' ]] || false

    run dolt sql -q "SELECT to_val,to_pk,from_val,from_pk from dolt_commit_diff_t where from_commit=HASHOF('HEAD~2') and to_commit=HASHOF('HEAD');" -r csv
    [[ "$output" =~ '3,3,,' ]] || false
    [[ "$output" =~ '4,4,,' ]] || false
}

@test "primary-key-changes: error dropping foreign key when used as a child in Fk relationship" {
    dolt sql -q "CREATE TABLE child(pk int primary key)"
    dolt sql -q "CREATE TABLE parent(pk int primary key, val int);"
    dolt sql -q "ALTER TABLE parent ADD CONSTRAINT myfk FOREIGN KEY (val) REFERENCES child (pk);"

    run dolt sql -q "ALTER TABLE child DROP PRIMARY KEY"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "error: can't drop index 'PRIMARY': needed in a foreign key constraint" ]] || false
}

@test "primary-key-changes: dolt constraints verify works gracefully with schema violations" {
    dolt sql -q "CREATE table t (pk int primary key, val int)"
    dolt commit -am "cm1"

    dolt sql -q "alter table t drop primary key"
    dolt sql -q "alter table t add index myidx (val)"
    dolt sql -q "create table parent(pk int primary key)"
    dolt sql -q "alter table parent add constraint fk FOREIGN KEY (pk) REFERENCES t (val);"

    run dolt constraints verify
    [ "$status" -eq 0 ]
    [[ "$output" = "" ]] || false
}

@test "primary-key-changes: add/drop primary key in different order" {
    dolt sql -q "CREATE table t (pk int primary key, val int)"
    dolt commit -am "cm1"

    dolt sql -q "alter table t drop primary key"
    run dolt sql -q "ALTER TABLE t ADD PRIMARY KEY (val, pk)"
    [ "$status" -eq 0 ]

    # Try a different table
    dolt sql -q "CREATE table t2 (pk int primary key, val1 int, val2 int)"
    dolt sql -q "INSERT INTO t2 VALUES (1, 2, 2), (2, 2, 2)"
    dolt sql -q "alter table t2 drop primary key"
    run dolt sql -q "ALTER TABLE t2 ADD PRIMARY KEY (val2, val1)"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "duplicate primary key given: [2,2]" ]] || false
}

@test "primary-key-changes: add primary key on column that doesn't exist errors appropriately" {
    dolt sql -q "CREATE table t (pk int, val int)"
    run dolt sql -q "ALTER TABLE t ADD PRIMARY KEY (pk1)"

    [ "$status" -eq 1 ]
    [[ "$output" =~ "error: key column 'pk1' doesn't exist in table" ]] || false
}

@test "primary-key-changes: same primary key set in different order is detected and blocked on merge" {
    dolt sql -q "CREATE table t (pk int, val int, primary key (pk, val))"
    dolt commit -am "cm1"

    dolt checkout -b test
    dolt sql -q "ALTER TABLE t DROP PRIMARY KEY"
    run dolt sql -q "ALTER TABLE t ADD PRIMARY KEY (val, pk);"
    [ "$status" -eq 0 ]

    dolt commit -am "cm2"
    run dolt status
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false

    dolt checkout main

    dolt diff
    run dolt diff test
    # TODO: dolt doesn't correctly store primary key order, we can't check this
#    [[ "$output" =~ '<    PRIMARY KEY (val, pk)' ]] || false
#    [[ "$output" =~ '>    PRIMARY KEY (pk, val)' ]] || false

    dolt sql -q "INSERT INTO t VALUES (1,1)"
    dolt commit -am "insert"

    run dolt merge test
    [ "$status" -eq 1 ]
    [[ "$output" =~ 'error: cannot merge two tables with different primary key sets' ]] || false

    run dolt sql -q "SELECT DOLT_MERGE('test')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ 'error: cannot merge two tables with different primary key sets' ]] || false

    skip "Dolt doesn't correctly store primary key order if it doesn't match the column order"
}

@test "primary-key-changes: correct diff is returned even with a new added column" {
    skip "TODO implement PK ordering for SHOW CREATE TABLE"

    dolt sql -q "CREATE table t (pk int, val int, primary key (pk, val))"
    dolt commit -am "cm1"

    dolt checkout -b test
    dolt sql -q "ALTER TABLE t ADD val2 int"
    dolt sql -q "ALTER TABLE t DROP PRIMARY KEY"
    run dolt sql -q "ALTER TABLE t ADD PRIMARY KEY (val2, pk);"
    [ "$status" -eq 0 ]

    dolt commit -am "cm2"
    run dolt status
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false

    dolt checkout main

    dolt diff test
    run dolt diff test
    [[ "$output" =~ '-  PRIMARY KEY (`val2`,`pk`)' ]] || false
    [[ "$output" =~ '+  PRIMARY KEY (`pk`,`val`)' ]] || false
}

@test "primary-key-changes: column with duplicates throws an error when added as pk" {
    dolt sql -q "CREATE table t (pk int, val int)"
    dolt sql -q "INSERT INTO t VALUES (1,1),(2,1)"
    dolt commit -am "cm1"

    run dolt sql -q "ALTER TABLE t ADD PRIMARY KEY (val);"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "duplicate primary key given: [1]" ]] || false
}

@test "primary-key-changes: can drop pk with supporting backup index" {
    dolt sql -q "CREATE table t1 (pk int, val int, primary key (pk, val), key `backup` (pk, val))"
    dolt sql -q "CREATE table c1 (pk int, val int, foreign key (pk, val) references t1 (pk, val))"
    dolt sql -q "alter table t1 drop primary key"

    dolt sql -q "CREATE table t2 (pk int, val int, primary key (pk, val), key `backup` (val))"
    dolt sql -q "CREATE table c2 (pk int, val int, foreign key (val) references t2 (val))"
    dolt sql -q "alter table t2 drop primary key"

    dolt sql -q "CREATE table t3 (pk int, val int, primary key (val, pk), key `backup` (pk))"
    dolt sql -q "CREATE table c3 (pk int, val int, foreign key (val) references t3 (val))"
    run dolt sql -q "alter table t3 drop primary key"
    [ "$status" -eq 1 ]
}

@test "primary-key-changes: foreign key lookup switch to backup index" {
    dolt sql -q "CREATE table t (pk int, val int, primary key (pk, val), key `backup` (val))"
    dolt sql -q "CREATE table c (pk int, val int, foreign key (val) references t (val))"
    dolt sql -q "insert into t values (1,1), (2,2)"
    dolt sql -q "insert into c values (1,1), (2,2)"
    dolt sql -q "alter table t drop primary key"

    run dolt sql -q "select * from c where val = 2" -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ "2,2" ]] || false
}

@test "primary-key-changes: can't add a primary key on a column containing NULLs" {
    dolt sql -q "create table t (pk int, c1 int)"
    dolt sql -q "insert into t values (NULL, NULL)"
    run dolt sql -q "alter table t add primary key(pk)"
    [ $status -eq 1 ]
}

@test "primary-key-changes: create table with primary key adds not null constraint" {
    dolt sql -q "create table t (pk int primary key)"
    run dolt sql -q "show create table t"
    [ $status -eq 0 ]
    [[ "$output" =~ "NOT NULL" ]] || false
}

@test "primary-key-changes: adding primary key also adds not null constraint" {
    dolt sql -q "create table t (pk int)"
    dolt sql -q "alter table t add primary key (pk)"
    run dolt sql -q "show create table t"
    [ $status -eq 0 ]
    [[ "$output" =~ "NOT NULL" ]] || false
}

@test "primary-key-changes: dropping primary key retains not null constraint" {
    dolt sql -q "create table t (pk1 int, pk2 int, c1 int)"
    dolt sql -q "alter table t add primary key (pk1, pk2)"
    dolt sql -q "alter table t drop primary key"
    run dolt sql -q "show create table t"
    [ $status -eq 0 ]
    [[ "$output" =~ "`pk1` int NOT NULL" ]] || false
    [[ "$output" =~ "`pk2` int NOT NULL" ]] || false
    [[ ! "$output" =~ "PRIMARY KEY" ]] || false

    dolt sql -q "show create table t" > res.txt
    run grep 'NOT NULL' res.txt
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
}

@test "primary-key-changes: creating table with null and primary key column throws error" {
    run dolt sql -q "create table t (pk int null primary key)"
    [ $status -eq 1 ]
}

@test "primary-key-changes: creating table with null and primary key column throws error again" {
    run dolt sql -q "create table t (pk int null, primary key(pk))"
    [ $status -eq 1 ]
}

@test "primary-key-changes: can't modify column with conflicting constraints" {
    dolt sql -q "create table t (pk int)"
    run dolt sql -q "alter table t modify (pk int null primary key)"
    [ $status -eq 1 ]
}

@test "primary-key-changes: can't add column with conflicting constraints" {
    dolt sql -q "create table t (c0 int)"
    run dolt sql -q "alter table t add (pk int null primary key)"
    [ $status -eq 1 ]
}

@test "primary-key-changes: can add primary keys on db.table named tables" {
    dolt sql <<SQL
create database mydb;
create table mydb.test(pk int, c1 int);
alter table mydb.test add primary key(pk);
SQL
    run dolt sql -q "show create table mydb.test"
    [ $status -eq 0 ]
    [[ "$output" =~ "PRIMARY KEY" ]]
}

@test "primary-key-changes: can add and drop primary keys on keyless db.table named tables" {
    dolt sql -q "CREATE DATABASE mydb"
    dolt sql -q "CREATE TABLE mydb.test(pk INT, c1 LONGTEXT, c2 BIGINT, c3 INT)"
    dolt sql -q "ALTER TABLE mydb.test ADD PRIMARY KEY(pk, c1)"
    run dolt sql -q "SHOW CREATE TABLE mydb.test"
    [ $status -eq 0 ]
    [[ "$output" =~ "PRIMARY KEY (\`pk\`,\`c1\`)" ]] || false

    dolt sql -q "ALTER TABLE mydb.test DROP PRIMARY KEY"
    run dolt sql -q "SHOW CREATE TABLE mydb.test"
    [ $status -eq 0 ]
    [[ ! "$output" =~ "PRIMARY KEY" ]]

    dolt sql -q "SHOW CREATE TABLE mydb.test" > output.txt
    run grep 'NOT NULL' output.txt
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
}

@test "primary-key-changes: can drop and add multiple primary keys on db.table named tables" {
    dolt sql -q "CREATE DATABASE mydb"
    dolt sql -q "CREATE TABLE mydb.test(pk INT PRIMARY KEY, c1 INT, c2 BIGINT, c3 INT)"
    dolt sql -q "ALTER TABLE mydb.test DROP PRIMARY KEY"
    dolt sql -q "SHOW CREATE TABLE mydb.test" > output.txt

    run grep 'NOT NULL' output.txt
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]

    dolt sql -q "ALTER TABLE mydb.test ADD PRIMARY KEY(c1, c2)"
    run dolt sql -q "SHOW CREATE TABLE mydb.test"
    [ $status -eq 0 ]
    [[ "$output" =~ "PRIMARY KEY (\`c1\`,\`c2\`)" ]] || false

    dolt sql -q "SHOW CREATE TABLE mydb.test" > output.txt
    run grep 'NOT NULL' output.txt
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
}
