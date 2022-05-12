#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    skip_nbf_dolt_1

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

    dolt diff
    run dolt diff
    [ "$status" -eq 0 ]
    [[ "$output" =~ "bigint" ]] || false
    [[ "$output" =~ "longtext" ]] || false
    [[ ! "$ouput" =~ "Merge failed" ]] || false
}

@test "schema-changes: dolt schema alter column preserves table checks" {
    dolt sql -q "alter table test add constraint test_check CHECK (c2 < 12345);"
    dolt sql -q "alter table test rename column c1 to c0;"
    run dolt schema show test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "CONSTRAINT \`test_check\` CHECK ((\`c2\` < 12345))" ]] || false
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

@test "schema-changes: dolt schema rename column fails when column is used in table check" {
    dolt sql -q "alter table test add constraint test_check CHECK (c2 < 12345);"
    run dolt sql -q "alter table test rename column c2 to c0"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "because it would invalidate check constraint" ]] || false

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
    dolt push origin main
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

    dolt diff
    run dolt diff
    [ "$status" -eq 0 ]
    [[ "$output" =~ '-  `pk2` bigint NOT NULL,' ]] || false
    [[ "$output" =~ '-  `v1` varchar(100) NOT NULL,' ]] || false
    [[ "$output" =~ '-  `v2` varchar(120),' ]] || false
    [[ "$output" =~ '+  `pk2` tinyint NOT NULL,' ]] || false
    [[ "$output" =~ '+  `v1` varchar(300) NOT NULL,' ]] || false
    [[ "$output" =~ '+  `v2` varchar(1024) NOT NULL,' ]] || false
    [[ "$output" =~ 'PRIMARY KEY' ]] || false

    dolt add .
    dolt commit -m "Changed types"

    dolt checkout original
    run dolt sql -q "INSERT INTO test2 (pk1, pk2, v1, v2) VALUES (2, 2, 'abc', 'def')"
    dolt diff
    dolt add .
    dolt commit -m "Created table with one row"

    dolt merge main

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
    dolt commit -m "merge main"

    # push to remote
    dolt checkout main
    dolt merge original
    dolt push origin main

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
    dolt diff HEAD~2
    run dolt diff HEAD~2
    [ "$status" -eq 0 ]
    [[ "$output" =~ '-  `pk2` bigint NOT NULL,' ]] || false
    [[ "$output" =~ '-  `v1` varchar(100) NOT NULL,' ]] || false
    [[ "$output" =~ '-  `v2` varchar(120),' ]] || false
    [[ "$output" =~ '+  `pk2` tinyint NOT NULL,' ]] || false
    [[ "$output" =~ '+  `v1` varchar(300) NOT NULL,' ]] || false
    [[ "$output" =~ '+  `v2` varchar(1024) NOT NULL,' ]] || false
    [[ "$output" =~ 'PRIMARY KEY' ]] || false
}

@test "schema-changes: drop then add column" {
    dolt sql <<SQL
CREATE TABLE test2(
  pk1 BIGINT,
  pk2 BIGINT,
  v1 BIGINT,
  PRIMARY KEY(pk1, pk2)
);
insert into test2 values (1, 1, 1), (2, 2, 2);
SQL

    # Commit is important here because we are testing column reuse on
    # drop / add, we want to be sure that we don't re-use any old
    # values from before the column was dropped
    dolt commit -am "Committing test table"

    dolt sql -q "alter table test2 drop column v1"
    dolt sql -q "alter table test2 add column v1 bigint"

    dolt status
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ modified:[[:space:]]+test2 ]] || false
    
    dolt sql -q "select * from test2 where pk1 = 1"
    run dolt sql -q "select * from test2 where pk1 = 1"
    [ "$status" -eq 0 ]
    [[  "$output" =~ "| 1   | 1   | NULL |" ]] || false
}

@test "schema-changes: drop then add column show no schema diff" {
    dolt sql <<SQL
CREATE TABLE test2(
  pk1 BIGINT,
  pk2 BIGINT,
  v1 BIGINT,
  PRIMARY KEY(pk1, pk2)
);
insert into test2 values (1, 1, 1), (2, 2, 2);
SQL

    # Commit is important here because we are testing column reuse on
    # drop / add, we want to be sure that we don't re-use any old
    # values from before the column was dropped
    dolt commit -am "Committing test table"

    dolt sql -q "alter table test2 drop column v1"
    dolt sql -q "alter table test2 add column v1 bigint"

    dolt status
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ modified:[[:space:]]+test2 ]] || false
    
    dolt sql -q "select * from test2 where pk1 = 1"
    run dolt sql -q "select * from test2 where pk1 = 1"
    [ "$status" -eq 0 ]
    [[  "$output" =~ "| 1   | 1   | NULL |" ]] || false

    dolt diff --data
    run dolt diff --data
    [ "$status" -eq 0 ]

    skip "dolt incorrectly considers there to be two different columns named v1"
    skip "output should have a single column named v1 https://github.com/dolthub/dolt/issues/2430"
    
    [[ ! "$output" =~ '|  <  | pk1 | pk2 |      | v1   |' ]] || false
    [[ ! "$output" =~ '|  >  | pk1 | pk2 | v1   |      |' ]] || false
}
