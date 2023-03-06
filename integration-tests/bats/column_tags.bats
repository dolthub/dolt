#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "column_tags: Renaming a column should preserve the tag number" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  c1 BIGINT,
  PRIMARY KEY (pk));
SQL
    run dolt schema tags -r=csv
    [ $status -eq 0 ]
    [[ "$output" =~ "test,c1,8201" ]] || false
    dolt sql -q "alter table test rename column c1 to c0"
    run dolt schema tags -r=csv
    [ $status -eq 0 ]
    [[ "$output" =~ "test,c0,8201" ]] || false
}

@test "column_tags: Renaming a table should preserve the tag number" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  c1 BIGINT,
  PRIMARY KEY (pk));
SQL
    run dolt schema tags -r=csv
    [ $status -eq 0 ]
    [[ "$output" =~ "test,pk,3228" ]] || false
    [[ "$output" =~ "test,c1,8201" ]] || false
    dolt sql -q "alter table test rename to new_name"
    run dolt schema tags -r=csv
    [ $status -eq 0 ]
    [[ "$output" =~ "new_name,pk,3228" ]] || false
    [[ "$output" =~ "new_name,c1,8201" ]] || false
}

@test "column_tags: Schema tags should be case insensitive to tables" {
    dolt sql <<SQL
CREATE TABLE TeSt (
  pk BIGINT NOT NULL,
  c1 BIGINT,
  PRIMARY KEY (pk));
SQL
    run dolt schema tags test -r=csv
    [ $status -eq 0 ]
    [[ "$output" =~ "TeSt,pk,3228" ]] || false
    [[ "$output" =~ "TeSt,c1,8201" ]] || false
}

@test "column_tags: Merging two branches that added same tag, name, type, and constraints" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  c1 BIGINT,
  PRIMARY KEY (pk));
SQL
    dolt add test
    dolt commit -m "Committed test table"
    dolt branch branch1
    dolt branch branch2
    dolt checkout branch1
    dolt sql -q "alter table test add column c2 bigint"
    dolt add test
    dolt commit -m "Added column c2 bigint"
    dolt checkout branch2
    dolt sql -q "alter table test add column c2 bigint"
    dolt add test
    dolt commit	-m "Added column c2 bigint"
    dolt checkout main
    dolt merge branch1 -m "merge branch1"
    run dolt merge branch2 -m "merge branch2"
    [ $status -eq 0 ]
}

@test "column_tags: Merging branches that use the same tag referring to different schema fails" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:1234',
  c1 BIGINT COMMENT 'tag:5678',
  PRIMARY KEY (pk));
SQL
    dolt add test
    dolt commit	-m "Committed test table"
    dolt branch	branch1
    dolt branch	branch2
    dolt checkout branch1
    dolt sql -q "alter table test add column c2 bigint"
    dolt add test
    dolt commit	-m "Added column c2 bigint"
    dolt checkout branch2
    dolt sql -q "alter table test add column c2 longtext"
    dolt add test
    dolt commit -m "Added column c2 longtext"
    dolt checkout main
    dolt merge branch1
    run dolt merge branch2
    [ $status -ne 0 ]
}

@test "column_tags: Merging branches that use the same tag referring to different column names fails" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:1234',
  c1 BIGINT COMMENT 'tag:5678',
  PRIMARY KEY (pk));
SQL
    dolt add test
    dolt commit -m "Committed test table"
    dolt branch branch1
    dolt branch branch2
    dolt checkout branch1
    dolt sql -q "alter table test add column c2 bigint"
    dolt add test
    dolt commit -m "Added column c2 bigint"
    dolt checkout branch2
    dolt sql -q "alter table test add column c2 bigint"
    dolt sql -q "alter table test rename column c2 to c0"
    dolt add test
    dolt commit -m "Added column c0 bigint"
    dolt checkout main
    dolt merge branch1
    run dolt merge branch2
    [ $status -eq 1 ]
}

@test "column_tags: Merging branches that both created the same column succeeds" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  PRIMARY KEY (pk));
SQL
    dolt add test
    dolt commit -m "Committed test table"
    dolt branch branch1
    dolt branch branch2
    dolt checkout branch1
    dolt sql -q "alter table test add column c2 bigint"
    dolt sql -q "alter table test add column c3 double"
    dolt add test
    dolt commit -m "Added columns c2 bigint and c3 double to branch1"
    dolt checkout branch2
    dolt sql -q "alter table test add column c2 bigint"
    # column c3 will have the same tag on both branches due to deterministic tag generation
    dolt sql -q "alter table test add column c3 double"
    dolt add test
    dolt commit -m "Added columns c2 bigint and c3 double to branch2"
    dolt checkout main
    dolt merge branch1 -m "merge branch1"
    run dolt merge branch2 -m "merge branch2"
    [ $status -eq 0 ]
    run dolt schema show
    [[ "${lines[2]}" =~ "\`pk\` bigint NOT NULL" ]] || false
    [[ "${lines[3]}" =~ "\`c1\` bigint" ]] || false
    [[ "${lines[4]}" =~ "\`c2\` bigint" ]] || false
    [[ "${lines[5]}" =~ "\`c3\` double" ]] || false
}

@test "column_tags: Merging branches that both created the same table succeeds" {
    dolt branch branch1
    dolt branch branch2
    dolt checkout branch1
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  c1 BIGINT,
  PRIMARY KEY (pk));
SQL
    dolt add test
    dolt commit -m "Committed test table"

    dolt checkout branch2
dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  c1 BIGINT,
  PRIMARY KEY (pk));
SQL
    dolt add test
    # pk and c1 will have the same tags on both branches due to deterministic tag generation
    dolt commit -m "Committed test table"
    dolt checkout main
    dolt merge branch1 -m "merge branch1"
    run dolt merge branch2 -m "merge branch2"
    [ $status -eq 0 ]
    run dolt schema show
    [[ "${lines[2]}" =~ "\`pk\` bigint NOT NULL" ]] || false
    [[ "${lines[3]}" =~ "\`c1\` bigint" ]] || false
}

@test "column_tags: Deterministic tag generation produces consistent results" {
    dolt branch other
    dolt sql <<SQL
CREATE TABLE test1 (
  pk1 BIGINT NOT NULL,
  c1 BIGINT,
  c2 DOUBLE,
  c3 LONGTEXT,
  PRIMARY KEY (pk1));
SQL
    dolt add test1
    dolt commit -m "Committed test table"

    # If anything changes to deterministic tag generation, this will break
    run dolt schema tags -r=csv
    [ $status -eq 0 ]
    [[ "$output" =~ "test1,pk1,10458" ]] || false
    [[ "$output" =~ "test1,c1,5951" ]] || false
    [[ "$output" =~ "test1,c2,10358" ]] || false
    [[ "$output" =~ "test1,c3,16293" ]] || false
}

@test "column_tags: dolt table import -c uses deterministic tag generation" {
    cat <<DELIM > data.csv
pk,c1,c2,c3,c4,c5
0,1,2,3,4,5
a,b,c,d,e,f
DELIM
    run dolt table import -c -pk=pk ints_table data.csv
    [ $status -eq 0 ]
    run dolt schema tags -r=csv
    [ $status -eq 0 ]
    [[ "$output" =~ "ints_table,pk,6302" ]] || false
    [[ "$output" =~ "ints_table,c1,12880" ]] || false
    [[ "$output" =~ "ints_table,c2,15463" ]] || false
    [[ "$output" =~ "ints_table,c3,14526" ]] || false
    [[ "$output" =~ "ints_table,c4,5634" ]] || false
    [[ "$output" =~ "ints_table,c5,12796" ]] || false
}

@test "column_tags: Round-tripping a column type through different NomsKinds restores original tag" {
    dolt sql -q "CREATE TABLE t (pk INT PRIMARY KEY, col1 int);"
    run dolt schema tags
    [ $status -eq 0 ]
    [[ $output =~ "col1   | 10878" ]] || false

    dolt sql -q "ALTER TABLE t MODIFY COLUMN col1 VARCHAR(100);"
    run dolt schema tags
    [ $status -eq 0 ]
    [[ $output =~ "col1   | 16050" ]] || false

    dolt sql -q "ALTER TABLE t MODIFY COLUMN col1 int;"
    run dolt schema tags
    [ $status -eq 0 ]
    [[ $output =~ "col1   | 10878" ]] || false
}

@test "column_tags: Round-tripping a column type through same NomsKinds keeps original tag" {
    dolt sql -q "CREATE TABLE t (pk INT PRIMARY KEY, col1 VARCHAR(100));"
    run dolt schema tags
    [ $status -eq 0 ]
    [[ $output =~ "col1   | 16050" ]] || false

    dolt sql -q "ALTER TABLE t MODIFY COLUMN col1 VARCHAR(101);"
    run dolt schema tags
    [ $status -eq 0 ]
    [[ $output =~ "col1   | 16050" ]] || false

     dolt sql -q "ALTER TABLE t MODIFY COLUMN col1 VARCHAR(100);"
    run dolt schema tags
    [ $status -eq 0 ]
    [[ $output =~ "col1   | 16050" ]] || false
}

@test "column_tags: Round-tripping a column type after some other column has been altered" {
    dolt sql -q "CREATE TABLE t (pk INT PRIMARY KEY, col1 int);"
    run dolt schema tags
    [ $status -eq 0 ]
    [[ $output =~ "col1   | 10878" ]] || false

    dolt sql -q "ALTER TABLE t ADD COLUMN col2 int;"

    dolt sql -q "ALTER TABLE t MODIFY COLUMN col1 VARCHAR(100);"
    run dolt schema tags
    [ $status -eq 0 ]
    [[ $output =~ "col1   | 11127" ]] || false

    dolt sql -q "ALTER TABLE t MODIFY COLUMN col1 int;"
    run dolt schema tags
    [ $status -eq 0 ]
    [[ $output =~ "col1   | 10186" ]] || false
}

@test "column_tags: update-tag only available on __DOLT__" {
    mkdir ld
    mkdir dev

    cd ld
    DOLT_DEFAULT_BIN_FORMAT=__LD_1__ dolt init
    run dolt schema update-tag t col 5
    [ $status -ne 0 ]
    echo $output
    [[ $output =~ "update-tag is only available in storage format __DOLT__" ]] || false
}

@test "column_tags: update-tag updates a columns tag" {
    skip_nbf_not_dolt

    dolt sql -q "CREATE TABLE t (pk INT PRIMARY KEY, col1 int);"
    run dolt schema tags
    [ $status -eq 0 ]
    [[ $output =~ "pk     | 15476" ]] || false
    [[ $output =~ "col1   | 10878" ]] || false

    dolt schema update-tag t pk 5
    run dolt schema tags
    [ $status -eq 0 ]
    [[ $output =~ "pk     | 5" ]] || false
    [[ $output =~ "col1   | 10878" ]] || false

    dolt schema update-tag t col1 6
    run dolt schema tags
    [ $status -eq 0 ]
    [[ $output =~ "pk     | 5" ]] || false
    [[ $output =~ "col1   | 6" ]] || false
}

@test "column_tags: create table on two separate branches, merge them together by updating tags" {
    skip_nbf_not_dolt

    dolt branch other
    dolt sql -q "CREATE TABLE t (pk int PRIMARY KEY, col1 int);"
    dolt sql -q "INSERT INTO t VALUES (1, 1);"
    dolt commit -Am "unrelated table"

    dolt sql -q "CREATE table target (pk int PRIMARY KEY, col1 int);"
    dolt sql -q "INSERT into target VALUES (1, 1);"
    dolt commit -Am "table target on main branch"

    dolt checkout other
    dolt sql -q "CREATE table target (pk int PRIMARY KEY, badCol int, col1 int);"
    dolt sql -q "INSERT INTO target VALUES (2, 2, 2);"
    dolt commit -Am "table target on other branch"
    dolt sql -q "ALTER TABLE target DROP COLUMN badCol;"
    dolt commit -Am "fixup"

    run dolt schema tags
    [[ $output =~ "| target | col1   | 14690 |" ]] || false

    dolt checkout main

    run dolt schema tags
    [ $status -eq 0 ]
    [[ $output =~ "| target | col1   | 14649 |" ]] || false

    run dolt merge other
    [ $status -ne 0 ]
    [[ $output =~ "table with same name added in 2 commits can't be merged" ]] || false
    dolt reset --hard

    dolt schema update-tag target col1 14690
    dolt commit -am "update tag of col1 of target"

    run dolt merge other -m "merge other into main"
    [ $status -eq 0 ]
    [[ $output =~ "1 tables changed, 1 rows added(+)" ]] || false

    run dolt sql -r csv -q "select * from target;"
    [ $status -eq 0 ]
    [[ $output =~ "1,1" ]] || false
    [[ $output =~ "2,2" ]] || false
}
