#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}

@test "changing column types should not produce a data diff error" {
    cat <<SQL > 1pk5col-ints-schema.sql
CREATE TABLE test (
  pk BIGINT NOT NULL,
  c1 BIGINT,
  c2 BIGINT,
  c3 BIGINT,
  c4 BIGINT,
  c5 BIGINT,
  PRIMARY KEY (pk)
);
SQL
    dolt table import -c --pk=pk test `batshelper 1pk5col-ints.csv`
    run dolt schema show
    [[ "$output" =~ "INT" ]] || false
    dolt add test
    dolt commit -m "Added test table"
    dolt table import -c -f -s=1pk5col-ints-schema.sql test `batshelper 1pk5col-ints.csv`
    run dolt diff
    skip "This produces a failed to merge schemas error message right now"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "BIGINT" ]] || false
    [[ ! "$output" =~ "TEXT" ]] || false
    [[ ! "$ouput" =~ "Failed to merge schemas" ]] || false
}

@test "dolt schema rename column" {
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
    dolt sql -q 'insert into test values (1,1,1,1,1,1)'
    run dolt sql -q "alter table test rename column c1 to c0"
    [ "$status" -eq 0 ]
    run dolt schema show test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test @ working" ]] || false
    [[ "$output" =~ "CREATE TABLE \`test\`" ]] || false
    [[ "$output" =~ "\`pk\` BIGINT NOT NULL COMMENT 'tag:0'" ]] || false
    [[ "$output" =~ "\`c2\` BIGINT COMMENT 'tag:2'" ]] || false
    [[ "$output" =~ "\`c3\` BIGINT COMMENT 'tag:3'" ]] || false
    [[ "$output" =~ "\`c4\` BIGINT COMMENT 'tag:4'" ]] || false
    [[ "$output" =~ "\`c5\` BIGINT COMMENT 'tag:5'" ]] || false
    [[ "$output" =~ "PRIMARY KEY (\`pk\`)" ]] || false
    [[ "$output" =~ "\`c0\` BIGINT COMMENT 'tag:1'" ]] || false
    [[ ! "$output" =~ "\`c1\` BIGINT COMMENT 'tag:1'" ]] || false
    dolt sql -q "select * from test"
}

@test "dolt schema delete column" {
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
    dolt sql -q 'insert into test values (1,1,1,1,1,1)'
    run dolt sql -q "alter table test drop column c1"
    [ "$status" -eq 0 ]
    run dolt schema show test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test @ working" ]] || false
    [[ "$output" =~ "CREATE TABLE \`test\`" ]] || false
    [[ "$output" =~ "\`pk\` BIGINT NOT NULL COMMENT 'tag:0'" ]] || false
    [[ "$output" =~ "\`c2\` BIGINT COMMENT 'tag:2'" ]] || false
    [[ "$output" =~ "\`c3\` BIGINT COMMENT 'tag:3'" ]] || false
    [[ "$output" =~ "\`c4\` BIGINT COMMENT 'tag:4'" ]] || false
    [[ "$output" =~ "\`c5\` BIGINT COMMENT 'tag:5'" ]] || false
    [[ "$output" =~ "PRIMARY KEY (\`pk\`)" ]] || false
    [[ ! "$output" =~ "\`c1\` BIGINT COMMENT 'tag:1'" ]] || false
    dolt sql -q "select * from test"
}

@test "dolt diff on schema changes" {
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

@test "change the primary key. view the schema diff" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  c1 BIGINT,
  c2 BIGINT,
  c3 BIGINT,
  c4 BIGINT,
  c5 BIGINT,
  PRIMARY KEY (pk)
);
SQL
    dolt add test
    dolt commit -m "committed table so we can see diffs"
    dolt table rm test
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  c1 BIGINT,
  c2 BIGINT,
  c3 BIGINT,
  c4 BIGINT,
  c5 BIGINT,
  PRIMARY KEY (c1)
);
SQL
    run dolt diff --schema
    [ "$status" -eq 0 ]
    [[ "$output" =~ "<    PRIMARY KEY (\`pk\`)" ]] || false
    [[ "$output" =~ ">    PRIMARY KEY (\`c1\`)" ]] || false
}

@test "add another primary key. view the schema diff" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  c1 BIGINT,
  c2 BIGINT,
  c3 BIGINT,
  c4 BIGINT,
  c5 BIGINT,
  PRIMARY KEY (pk)
);
SQL
    dolt add test
    dolt commit -m "committed table so we can see diffs"
    dolt table rm test
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  c1 BIGINT,
  c2 BIGINT,
  c3 BIGINT,
  c4 BIGINT,
  c5 BIGINT NOT NULL,
  PRIMARY KEY (c1,c5)
);
SQL
    run dolt diff --schema
    [ "$status" -eq 0 ]
    [[ "$output" =~ "<    PRIMARY KEY (\`pk\`)" ]] || false
    [[ "$output" =~ ">    PRIMARY KEY (\`c1\`, \`c5\`)" ]] || false
}

@test "adding and dropping column should produce no diff" {
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
    dolt add test
    dolt commit -m "committed table so we can see diffs"
    dolt sql -q "alter table test add c0 bigint"
    dolt sql -q "alter table test drop column c0"
    run dolt diff
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
}

@test "schema diff should show primary keys in output" {
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
    dolt add test
    dolt commit -m "committed table so we can see diffs"
    dolt sql -q 'alter table test rename column c2 to column2'
    run dolt diff --schema
    [ "$status" -eq 0 ]
    [[ "$output" =~ "PRIMARY KEY" ]] || false
}

@test "add another new primary key column. view the schema diff" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  c1 BIGINT,
  c2 BIGINT,
  c3 BIGINT,
  c4 BIGINT,
  c5 BIGINT,
  PRIMARY KEY (pk)
);
SQL
    dolt add test
    dolt commit -m "committed table so we can see diffs"
    dolt table rm test
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  c1 BIGINT,
  c2 BIGINT,
  c3 BIGINT,
  c4 BIGINT,
  c5 BIGINT,
  c6 BIGINT,
  PRIMARY KEY (pk,c6)
);
SQL
    run dolt diff --schema
    [ "$status" -eq 0 ]
    [[ "$output" =~ "<    PRIMARY KEY (\`pk\`)" ]] || false
    [[ "$output" =~ ">    PRIMARY KEY (\`pk\`, \`c6\`)" ]] || false
}

@test "remove a primary key column. view the schema diff" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  c1 BIGINT,
  c2 BIGINT,
  c3 BIGINT,
  c4 BIGINT,
  c5 BIGINT,
  PRIMARY KEY (pk)
);
SQL
    dolt add test
    dolt commit -m "committed table so we can see diffs"
    dolt table rm test
    dolt sql <<SQL
CREATE TABLE test (
  c1 BIGINT,
  c2 BIGINT,
  c3 BIGINT,
  c4 BIGINT,
  c5 BIGINT,
  PRIMARY KEY (c3)
);
SQL
    run dolt diff --schema
    [ "$status" -eq 0 ]
    [[ "$output" =~ "<    PRIMARY KEY (\`pk\`)" ]] || false
    [[ "$output" =~ ">    PRIMARY KEY (\`c3\`)" ]] || false
}
