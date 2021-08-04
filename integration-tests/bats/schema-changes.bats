#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common

    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "schema-changes: changing column types should not produce a data diff error" {
    dolt sql -q 'insert into test values (0,0,0,0,0,0)'
    dolt add test
    dolt commit -m 'made table'
    dolt sql -q 'alter table test drop column c1'
    dolt sql -q 'alter table test add column c1 longtext'
    run dolt diff
    [ "$status" -eq 0 ]
    [[ "$output" =~ "BIGINT" ]] || false
    [[ "$output" =~ "LONGTEXT" ]] || false
    [[ ! "$ouput" =~ "Merge failed" ]] || false
}

@test "schema-changes: dolt schema rename column" {
    dolt sql -q 'insert into test values (1,1,1,1,1,1)'
    run dolt sql -q "alter table test rename column c1 to c0"
    [ "$status" -eq 0 ]
    run dolt schema show test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test @ working" ]] || false
    [[ "$output" =~ "CREATE TABLE \`test\`" ]] || false
    [[ "$output" =~ "\`pk\` bigint NOT NULL" ]] || false
    [[ "$output" =~ "\`c2\` bigint" ]] || false
    [[ "$output" =~ "\`c3\` bigint" ]] || false
    [[ "$output" =~ "\`c4\` bigint" ]] || false
    [[ "$output" =~ "\`c5\` bigint" ]] || false
    [[ "$output" =~ "PRIMARY KEY (\`pk\`)" ]] || false
    [[ "$output" =~ "\`c0\` bigint" ]] || false
    [[ ! "$output" =~ "\`c1\` bigint" ]] || false
    dolt sql -q "select * from test"
}

@test "schema-changes: dolt schema delete column" {
    dolt sql -q 'insert into test values (1,1,1,1,1,1)'
    run dolt sql -q "alter table test drop column c1"
    [ "$status" -eq 0 ]
    run dolt schema show test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test @ working" ]] || false
    [[ "$output" =~ "CREATE TABLE \`test\`" ]] || false
    [[ "$output" =~ "\`pk\` bigint NOT NULL" ]] || false
    [[ "$output" =~ "\`c2\` bigint" ]] || false
    [[ "$output" =~ "\`c3\` bigint" ]] || false
    [[ "$output" =~ "\`c4\` bigint" ]] || false
    [[ "$output" =~ "\`c5\` bigint" ]] || false
    [[ "$output" =~ "PRIMARY KEY (\`pk\`)" ]] || false
    [[ ! "$output" =~ "\`c1\` bigint" ]] || false
    dolt sql -q "select * from test"
}

@test "schema-changes: dolt diff on schema changes" {
    dolt add test
    dolt commit -m "committed table so we can see diffs"
    dolt sql -q "alter table test add c0 bigint"
    run dolt diff
    [ "$status" -eq 0 ]
    [[ "$output" =~ \+[[:space:]]+\`c0\` ]] || false
    [[ "$output" =~ "| c0 |" ]] || false
    run dolt diff --schema
    [ "$status" -eq 0 ]
    [[ "$output" =~ \+[[:space:]]+\`c0\` ]] || false
    [[ ! "$output" =~ "| c0 |" ]] || false
    run dolt diff --data
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ \+[[:space:]]+\`c0\` ]] || false
    [[ "$output" =~ "| c0 |" ]] || false
    [[ "$output" =~ ">" ]] || false
    [[ "$output" =~ "<" ]] || false
    # Check for a blank column in the diff output
    [[ "$output" =~ \|[[:space:]]+\| ]] || false
    dolt sql -q "insert into test (pk,c0,c1,c2,c3,c4,c5) values (0,0,0,0,0,0,0)"
    run dolt diff
    [ "$status" -eq 0 ]
    [[ "$output" =~ \|[[:space:]]+c0[[:space:]]+\| ]] || false
    [[ "$output" =~ \+[[:space:]]+[[:space:]]+\|[[:space:]]+0 ]] || false
    dolt sql -q "alter table test drop column c0"
    dolt diff
}

@test "schema-changes: dolt diff on schema changes rename primary key" {
    dolt add test
    dolt commit -m "committed table so we can see diffs"
    dolt sql -q "alter table test rename column pk to pk1"
    run dolt diff
    [ "$status" -eq 0 ]
    [[ "$output" =~ "pk1" ]] || false
}

@test "schema-changes: adding and dropping column should produce no diff" {
    dolt add test
    dolt commit -m "committed table so we can see diffs"
    dolt sql -q "alter table test add c0 bigint"
    dolt sql -q "alter table test drop column c0"
    run dolt diff
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
}

@test "schema-changes: schema diff should show primary keys in output" {
    dolt add test
    dolt commit -m "committed table so we can see diffs"
    dolt sql -q 'alter table test rename column c2 to column2'
    run dolt diff --schema
    [ "$status" -eq 0 ]
    [[ "$output" =~ "PRIMARY KEY" ]] || false
}

@test "schema-changes: changing column does not allow nulls in primary key" {
    dolt sql <<SQL
CREATE TABLE test2(
  pk1 BIGINT,
  pk2 BIGINT,
  v1 BIGINT,
  PRIMARY KEY(pk1, pk2)
);
SQL
    run dolt sql -q "INSERT INTO test2 (pk1, pk2) VALUES (1, null)"
    [ "$status" -eq 1 ]
    dolt sql -q "ALTER TABLE test2 CHANGE pk2 pk2new BIGINT"
    run dolt sql -q "INSERT INTO test2 (pk1, pk2new) VALUES (1, null)"
    [ "$status" -eq 1 ]
}

@test "schema-changes: changing column types in place works" {
    dolt sql <<SQL
CREATE TABLE test2(
  pk1 BIGINT,
  pk2 BIGINT,
  v1 VARCHAR(100) NOT NULL,
  v2 VARCHar(120) NULL,
  PRIMARY KEY(pk1, pk2)
);
SQL
    run dolt schema tags -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,column,tag" ]] || false
    [[ "$output" =~ "test2,pk1,6801" ]] || false
    [[ "$output" =~ "test2,pk2,4776" ]] || false
    [[ "$output" =~ "test2,v1,10579" ]] || false
    [[ "$output" =~ "test2,v2,7704" ]] || false

    run dolt sql -q "INSERT INTO test2 (pk1, pk2, v1, v2) VALUES (1, 1, 'abc', 'def')"
    [ "$status" -eq 0 ]
    dolt add .
    dolt commit -m "Created table with one row"

    dolt branch original

    # push to a file based remote, clone a copy to pull to later
    mkdir remotedir
    dolt remote add origin file://remotedir
    dolt push origin master
    dolt clone file://remotedir original
    
    dolt sql -q "ALTER TABLE Test2 MODIFY V1 varchar(300) not null"
    dolt sql -q "ALTER TABLE TEST2 MODIFY PK2 tinyint not null"
    dolt sql -q "ALTER TABLE Test2 MODIFY V2 varchar(1024) not null"

    # verify that the tags have not changed
    run dolt schema tags -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,column,tag" ]] || false
    [[ "$output" =~ "test2,pk1,6801" ]] || false
    [[ "$output" =~ "test2,pk2,4776" ]] || false
    [[ "$output" =~ "test2,v1,10579" ]] || false
    [[ "$output" =~ "test2,v2,7704" ]] || false

    run dolt diff
    [ "$status" -eq 0 ]
    [[ "$output" =~ '<   `pk2`  BIGINT NOT NULL' ]] || false
    [[ "$output" =~ '>   `pk2` TINYINT NOT NULL' ]] || false
    [[ "$output" =~ '<   `v1` VARCHAR(100) NOT NULL' ]] || false
    [[ "$output" =~ '>   `v1` VARCHAR(300) NOT NULL' ]] || false
    [[ "$output" =~ '<   `v2`  VARCHAR(120)' ]] || false
    [[ "$output" =~ '>   `v2` VARCHAR(1024)' ]] || false
    [[ "$output" =~ 'PRIMARY KEY' ]] || false

    dolt add .
    dolt commit -m "Changed types"

    dolt checkout original
    run dolt sql -q "INSERT INTO test2 (pk1, pk2, v1, v2) VALUES (2, 2, 'abc', 'def')"
    dolt diff
    dolt add .
    dolt commit -m "Created table with one row"

    dolt merge master

    run dolt sql -q 'show create table test2'
    [ "$status" -eq 0 ]
    [[ "$output" =~ '`pk2` tinyint NOT NULL' ]] || false
    [[ "$output" =~ '`v1` varchar(300) NOT NULL' ]] || false

    run dolt sql -q 'select * from test2' -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
    [[ "$output" =~ '1,1,abc,def' ]] || false
    [[ "$output" =~ '2,2,abc,def' ]] || false

    dolt add .
    dolt commit -m "merge master"

    # push to remote
    dolt checkout master
    dolt merge original
    dolt push origin master

    # pull from the remote and make sure there's no issue
    cd original
    dolt pull
    run dolt sql -q 'show create table test2'
    [ "$status" -eq 0 ]
    [[ "$output" =~ '`pk2` tinyint NOT NULL' ]] || false
    [[ "$output" =~ '`v1` varchar(300) NOT NULL' ]] || false

    run dolt sql -q 'select * from test2' -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
    [[ "$output" =~ '1,1,abc,def' ]] || false
    [[ "$output" =~ '2,2,abc,def' ]] || false

    # make sure diff works as expected for schema change on clone
    run dolt diff HEAD~2
    [ "$status" -eq 0 ]
    [[ "$output" =~ '<   `pk2`  BIGINT NOT NULL' ]] || false
    [[ "$output" =~ '>   `pk2` TINYINT NOT NULL' ]] || false
    [[ "$output" =~ '<   `v1` VARCHAR(100) NOT NULL' ]] || false
    [[ "$output" =~ '>   `v1` VARCHAR(300) NOT NULL' ]] || false
    [[ "$output" =~ '<   `v2`  VARCHAR(120)' ]] || false
    [[ "$output" =~ '>   `v2` VARCHAR(1024)' ]] || false
    [[ "$output" =~ 'PRIMARY KEY' ]] || false
}

@test "schema-changes: add single primary key" {
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

@test "schema-changes: add composite primary key" {
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

@test "schema-changes: can delete single primary key" {
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

@test "schema-changes: can delete composite primary key" {
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

@test "schema-changes: run through some add and drop primary key operations" {
    dolt sql -q "create table t(pk int, val int, PRIMARY KEY(pk, val))"
    run dolt sql -q "ALTER TABLE t DROP PRIMARY KEY"
    [ "$status" -eq 0 ]

    dolt sql -q "INSERT INTO t VALUES (1,1),(2,2)"
    run dolt sql -q "SELECT * FROM t"

    run dolt sql -q "SELECT * FROM t" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false

    run dolt sql -q "ALTER TABLE t ADD PRIMARY KEY (pk)"
    echo $output
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

@test "schema-changes: add an index after dropping a key, and then recreate the key" {
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
   echo $output
   [  "$status" -eq 0 ]
}

@test "schema-changes: alter table on keyless column with duplicates throws an error" {
    skip "unimplemented"
    dolt sql -q "create table t(pk int, val int)"
    dolt sql -q "insert into t values (1,1),(1,1)"

    run dolt sql -q "ALTER TABLE t ADD PRIMARY KEY (pk, val)"
    [ "$status" -eq 1 ]
}


@test "schema-changes: dropping a primary key still preserves secondary indexes" {
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
    [[ "$output" =~ "MUL" ]] || false
}

@test "schema-changes: drop primary key with auto increment throws an error" {
    skip "unimplemented"
    dolt sql -q "create table t(pk int PRIMARY KEY AUTO_INCREMENT, val1 int, val2 int)"
    run dolt sql -q "alter table t drop primary key"
    [ "$status" -eq 1 ]
}

@test "schema-changes: merge on primary key schema differences throws an error" {
    skip "unimplemented"
    dolt sql -q "create table t(pk int PRIMARY KEY, val1 int, val2 int)"
    dolt sql -q "INSERT INTO t values (1,1,1)"

    dolt commit -am "cm1"
    dolt checkout -b test
    dolt sql -q "ALTER TABLE t drop PRIMARY key"
    dolt commit -am "cm2"
    dolt checkout master

    run dolt merge test
    [ "$status" -eq 1 ]
    [[ "$output" =~ 'cannot merge branches with different primary key schema' ]] || false
}

@test "schema-changes: diff on primary key schema change shows schema level diff but does not show row level diff" {
    skip "unimplemented"
    dolt sql -q "CREATE TABLE t (pk int PRIMARY KEY, val int)"
    dolt sql -q "INSERT INTO t VALUES (1, 1)"

    dolt commit -am "cm1"

    run dolt sql -q "ALTER TABLE T DROP PRIMARY key"
    [ "$status" -eq 0 ]

    run dolt diff HEAD
    [ "$status" -eq 0 ]
    # TODO: Schema level dif
}

@test "schema-changes: dolt diff table return an empty table" {
    skip "unimplemented"
    dolt sql -q "CREATE TABLE t (pk int PRIMARY KEY, val int)"
    dolt sql -q "INSERT INTO t VALUES (1, 1)"

    dolt sql -q "ALTER TABLE t DROP PRIMARY KEY"
    dolt sql -q "INSERT INTO t values (2,2)"

    dolt add .

    run dolt sql -q "SELECT COUNT(*) from dolt_diff_t"
    [ "$status" -eq 0 ]
    [[ "$output" =~ '0' ]] || false
}

# TODO: FKs checks