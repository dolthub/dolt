#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}

@test "create a single primary key table" {
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
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false
}

@test "create a two primary key table" {
    dolt sql <<SQL
CREATE TABLE test (
  pk1 BIGINT NOT NULL,
  pk2 BIGINT NOT NULL,
  c1 BIGINT,
  c2 BIGINT,
  c3 BIGINT,
  c4 BIGINT,
  c5 BIGINT,
  PRIMARY KEY (pk1,pk2)
);
SQL
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false
}

@test "create a table that uses all supported types" {
    dolt sql <<SQL
CREATE TABLE test (
  \`pk\` BIGINT NOT NULL,
  \`int\` BIGINT,
  \`string\` LONGTEXT,
  \`boolean\` BOOLEAN,
  \`float\` DOUBLE,
  \`uint\` BIGINT UNSIGNED,
  \`uuid\` CHAR(36) CHARACTER SET ascii COLLATE ascii_bin,
  PRIMARY KEY (pk)
);
SQL
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false
}

@test "create a table that uses unsupported poop type" {
    run dolt sql <<SQL
CREATE TABLE test (
  \`pk\` BIGINT NOT NULL,
  \`int\` BIGINT,
  \`string\` LONGTEXT,
  \`boolean\` BOOLEAN,
  \`float\` DOUBLE,
  \`uint\` BIGINT UNSIGNED,
  \`uuid\` CHAR(36) CHARACTER SET ascii COLLATE ascii_bin,
  \`blob\` LONGBLOB,
  \`poop\` POOP,
  PRIMARY KEY (pk)
);
SQL
    [ "$status" -eq 1 ]
}

@test "create a repo with two tables" {
    dolt sql <<SQL
CREATE TABLE test1 (
  pk BIGINT NOT NULL,
  c1 BIGINT,
  c2 BIGINT,
  c3 BIGINT,
  c4 BIGINT,
  c5 BIGINT,
  PRIMARY KEY (pk)
);
SQL
    dolt sql <<SQL
CREATE TABLE test2 (
  pk1 BIGINT NOT NULL,
  pk2 BIGINT NOT NULL,
  c1 BIGINT,
  c2 BIGINT,
  c3 BIGINT,
  c4 BIGINT,
  c5 BIGINT,
  PRIMARY KEY (pk1,pk2)
);
SQL
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test1" ]] || false
    [[ "$output" =~ "test2" ]] || false
    [ "${#lines[@]}" -eq 3 ]
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test1" ]] || false
    [[ "$output" =~ "test2" ]] || false
}

@test "create a basic table (int types) using sql" {
    run dolt sql <<SQL
CREATE TABLE test (
    pk BIGINT,
    c1 BIGINT,
    c2 BIGINT,
    c3 BIGINT,
    c4 BIGINT,
    c5 BIGINT,
    PRIMARY KEY (pk)
);
SQL
    [ "$status" -eq 0 ]
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false
    run dolt schema show
    [ "$status" -eq 0 ]
    [[ "$output" =~ "CREATE TABLE \`test\`" ]] || false
    [[ "$output" =~ "\`pk\` BIGINT NOT NULL" ]] || false
    [[ "$output" =~ "\`c1\` BIGINT" ]] || false
    [[ "$output" =~ "\`c2\` BIGINT" ]] || false
    [[ "$output" =~ "\`c3\` BIGINT" ]] || false
    [[ "$output" =~ "\`c4\` BIGINT" ]] || false
    [[ "$output" =~ "\`c5\` BIGINT" ]] || false
    [[ "$output" =~ "PRIMARY KEY (\`pk\`)" ]] || false
}

@test "create a table with sql with multiple primary keys" {
    run dolt sql <<SQL
CREATE TABLE test (
    pk1 BIGINT,
    pk2 BIGINT,
    c1 BIGINT,
    c2 BIGINT,
    c3 BIGINT,
    c4 BIGINT,
    c5 BIGINT,
    PRIMARY KEY (pk1),
    PRIMARY KEY (pk2)
);
SQL
    [ "$status" -eq 0 ]
    run dolt schema show
    [ "$status" -eq 0 ]
    [[ "$output" =~ "CREATE TABLE \`test\`" ]] || false
    [[ "$output" =~ "\`pk1\` BIGINT NOT NULL" ]] || false
    [[ "$output" =~ "\`pk2\` BIGINT NOT NULL" ]] || false
    [[ "$output" =~ "\`c1\` BIGINT" ]] || false
    [[ "$output" =~ "\`c2\` BIGINT" ]] || false
    [[ "$output" =~ "\`c3\` BIGINT" ]] || false
    [[ "$output" =~ "\`c4\` BIGINT" ]] || false
    [[ "$output" =~ "\`c5\` BIGINT" ]] || false
    [[ "$output" =~ "PRIMARY KEY (\`pk1\`,\`pk2\`)" ]] || false
}

@test "create a table using sql with not null constraint" {
    run dolt sql <<SQL
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
    [ "$status" -eq 0 ]
    run dolt schema show test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "CREATE TABLE \`test\`" ]] || false
    [[ "$output" =~ "\`pk\` BIGINT NOT NULL" ]] || false
    [[ "$output" =~ "\`c1\` BIGINT" ]] || false
    [[ "$output" =~ "\`c2\` BIGINT" ]] || false
    [[ "$output" =~ "\`c3\` BIGINT" ]] || false
    [[ "$output" =~ "\`c4\` BIGINT" ]] || false
    [[ "$output" =~ "\`c5\` BIGINT" ]] || false
    [[ "$output" =~ "PRIMARY KEY (\`pk\`)" ]] || false
}

@test "create a table using sql with a float" {
    run dolt sql <<SQL
CREATE TABLE test (
    pk BIGINT NOT NULL,
    c1 DOUBLE,
    PRIMARY KEY (pk)
);
SQL
    [ "$status" -eq 0 ]
    run dolt schema show test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "CREATE TABLE \`test\` " ]] || false
    [[ "$output" =~ "\`pk\` BIGINT NOT NULL" ]] || false
    [[ "$output" =~ "\`c1\` DOUBLE" ]] || false
    [[ "$output" =~ "PRIMARY KEY (\`pk\`)" ]] || false
}


@test "create a table using sql with a string" {
    run dolt sql <<SQL
CREATE TABLE test (
    pk BIGINT NOT NULL,
    c1 LONGTEXT,
    PRIMARY KEY (pk)
);
SQL
    [ "$status" -eq 0 ]
    run dolt schema show test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "CREATE TABLE \`test\`" ]] || false
    [[ "$output" =~ "\`pk\` BIGINT NOT NULL" ]] || false
    [[ "$output" =~ "\`c1\` LONGTEXT" ]] || false
    [[ "$output" =~ "PRIMARY KEY (\`pk\`)" ]] || false
}


@test "create a table using sql with an unsigned int" {
    run dolt sql -q "CREATE TABLE test (pk BIGINT NOT NULL, c1 BIGINT UNSIGNED, PRIMARY KEY (pk))"
    [ "$status" -eq 0 ]
    [ -z "$output" ]
    run dolt schema show test
    [[ "$output" =~ "BIGINT UNSIGNED" ]] || false
}

@test "create a table using sql with a boolean" {
    run dolt sql -q "CREATE TABLE test (pk BIGINT NOT NULL, c1 BOOLEAN, PRIMARY KEY (pk))"
    [ "$status" -eq 0 ]
    [ -z "$output" ]
}

@test "create a table with a mispelled primary key" {
    run dolt sql -q "CREATE TABLE test (pk BIGINT, c1 BIGINT, c2 BIGINT, PRIMARY KEY
(pk,noexist))"
    [ "$status" -eq 1 ]
}

@test "create a table with a SQL reserved word" {
    dolt sql <<SQL
CREATE TABLE test (
    pk INT NOT NULL,
    \`all\` INT,
    \`select\` INT,
    PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq 0 ]
    [[ "$output" =~ "all" ]] || false
    [[ "$output" =~ "select" ]] || false
    run dolt sql <<SQL
CREATE TABLE test (
    pk INT NOT NULL,
    all INT,
    select INT,
    PRIMARY KEY (pk)
);
SQL
    [ "$status" -ne 0 ]
}

@test "create a table with a SQL keyword that is not reserved" {
    dolt sql <<SQL
CREATE TABLE test (
    pk INT NOT NULL,
    \`comment\` INT,
    \`date\` INT,
    PRIMARY KEY (pk)
);
SQL
    run dolt schema show
    [ "$status" -eq 0 ]
    [[ "$output" =~ "comment" ]] || false
    [[ "$output" =~ "date" ]] || false
    run dolt sql <<SQL
CREATE TABLE test (
    pk INT NOT NULL,
    comment INT,
    date INT,
    PRIMARY KEY (pk)
);
SQL
    skip "Current SQL parser requires backticks around keywords, not just reserved words"
    [ "$status" -eq 0 ]
    run dolt schema show
    [ "$status" -eq 0 ]
    [[ "$output" =~ "comment" ]] || false
    [[ "$output" =~ "date" ]] || false
}


@test "create two table with the same name" {
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
    run dolt sql <<SQL
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
    [ "$status" -ne 0 ]
    [[ "$output" =~ "already exists" ]] || false
}