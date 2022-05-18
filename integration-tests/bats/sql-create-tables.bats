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

@test "sql-create-tables: create a single primary key table" {
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

@test "sql-create-tables: create a two primary key table" {
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

@test "sql-create-tables: create a table that uses all supported types" {
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

@test "sql-create-tables: create a table that uses unsupported poop type" {
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

@test "sql-create-tables: create a repo with two tables" {
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

@test "sql-create-tables: create a basic table (int types) using sql" {
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
    [[ "$output" =~ "\`pk\` bigint NOT NULL" ]] || false
    [[ "$output" =~ "\`c1\` bigint" ]] || false
    [[ "$output" =~ "\`c2\` bigint" ]] || false
    [[ "$output" =~ "\`c3\` bigint" ]] || false
    [[ "$output" =~ "\`c4\` bigint" ]] || false
    [[ "$output" =~ "\`c5\` bigint" ]] || false
    [[ "$output" =~ "PRIMARY KEY (\`pk\`)" ]] || false
}

@test "sql-create-tables: create a table with sql with multiple primary keys" {
    run dolt sql <<SQL
CREATE TABLE test (
    pk1 BIGINT,
    pk2 BIGINT,
    c1 BIGINT,
    c2 BIGINT,
    c3 BIGINT,
    c4 BIGINT,
    c5 BIGINT,
    PRIMARY KEY (pk1, pk2)
);
SQL
    [ "$status" -eq 0 ]
    run dolt schema show
    [ "$status" -eq 0 ]
    [[ "$output" =~ "CREATE TABLE \`test\`" ]] || false
    [[ "$output" =~ "\`pk1\` bigint NOT NULL" ]] || false
    [[ "$output" =~ "\`pk2\` bigint NOT NULL" ]] || false
    [[ "$output" =~ "\`c1\` bigint" ]] || false
    [[ "$output" =~ "\`c2\` bigint" ]] || false
    [[ "$output" =~ "\`c3\` bigint" ]] || false
    [[ "$output" =~ "\`c4\` bigint" ]] || false
    [[ "$output" =~ "\`c5\` bigint" ]] || false
    [[ "$output" =~ "PRIMARY KEY (\`pk1\`,\`pk2\`)" ]] || false
}

@test "sql-create-tables: create a table using sql with not null constraint" {
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
    [[ "$output" =~ "\`pk\` bigint NOT NULL" ]] || false
    [[ "$output" =~ "\`c1\` bigint" ]] || false
    [[ "$output" =~ "\`c2\` bigint" ]] || false
    [[ "$output" =~ "\`c3\` bigint" ]] || false
    [[ "$output" =~ "\`c4\` bigint" ]] || false
    [[ "$output" =~ "\`c5\` bigint" ]] || false
    [[ "$output" =~ "PRIMARY KEY (\`pk\`)" ]] || false
}

@test "sql-create-tables: create a table using sql with a float" {
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
    [[ "$output" =~ "\`pk\` bigint NOT NULL" ]] || false
    [[ "$output" =~ "\`c1\` double" ]] || false
    [[ "$output" =~ "PRIMARY KEY (\`pk\`)" ]] || false
}


@test "sql-create-tables: create a table using sql with a string" {
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
    [[ "$output" =~ "\`pk\` bigint NOT NULL" ]] || false
    [[ "$output" =~ "\`c1\` longtext" ]] || false
    [[ "$output" =~ "PRIMARY KEY (\`pk\`)" ]] || false
}


@test "sql-create-tables: create a table using sql with an unsigned int" {
    run dolt sql -q "CREATE TABLE test (pk BIGINT NOT NULL, c1 BIGINT UNSIGNED, PRIMARY KEY (pk))"
    [ "$status" -eq 0 ]
    [ -z "$output" ]
    run dolt schema show test
    [[ "$output" =~ "bigint unsigned" ]] || false
}

@test "sql-create-tables: create a table using sql with a boolean" {
    run dolt sql -q "CREATE TABLE test (pk BIGINT NOT NULL, c1 BOOLEAN, PRIMARY KEY (pk))"
    [ "$status" -eq 0 ]
    [ -z "$output" ]
}

@test "sql-create-tables: create a table with a mispelled primary key" {
    run dolt sql -q "CREATE TABLE test (pk BIGINT, c1 BIGINT, c2 BIGINT, PRIMARY KEY
(pk,noexist))"
    [ "$status" -eq 1 ]
}

@test "sql-create-tables: create a table with a SQL reserved word" {
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

@test "sql-create-tables: create a table with a SQL keyword that is not reserved" {
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
CREATE TABLE test2 (
    pk INT NOT NULL,
    comment INT,
    date INT,
    PRIMARY KEY (pk)
);
SQL

    [ "$status" -eq 0 ]
    run dolt schema show
    [ "$status" -eq 0 ]
    [[ "$output" =~ "comment" ]] || false
    [[ "$output" =~ "date" ]] || false
}


@test "sql-create-tables: create two table with the same name" {
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

@test "sql-create-tables: create like" {
    dolt sql <<SQL
CREATE TABLE test1 (
  pk bigint primary key,
  c1 bigint default 5 comment 'hi'
);
CREATE TABLE test2 LIKE test1;
SQL
    run dolt schema show test2
    [ "$status" -eq 0 ]
    [[ "$output" =~ "CREATE TABLE \`test2\`" ]] || false
    [[ "$output" =~ "\`pk\` bigint NOT NULL" ]] || false
    [[ "$output" =~ "\`c1\` bigint DEFAULT 5 COMMENT 'hi'" ]] || false
    [[ "$output" =~ "PRIMARY KEY (\`pk\`)" ]] || false
}

@test "sql-create-tables: create like from other database" {
    mkdir otherdb
    cd otherdb
    dolt init
    dolt sql -q "CREATE TABLE othertable(pk bigint primary key, v1 bigint comment 'other')"
    dolt add -A
    dolt commit -m "some commit"
    cd ..
    mkdir workspace
    cd workspace
    dolt init
    cd ..
    dolt sql --multi-db-dir ./ -b -q "USE workspace;CREATE TABLE mytable LIKE otherdb.othertable;"
    cd workspace
    run dolt schema show mytable
    [ "$status" -eq 0 ]
    [[ "$output" =~ "CREATE TABLE \`mytable\`" ]] || false
    [[ "$output" =~ "\`pk\` bigint NOT NULL" ]] || false
    [[ "$output" =~ "\`v1\` bigint COMMENT 'other'" ]] || false
    [[ "$output" =~ "PRIMARY KEY (\`pk\`)" ]] || false
}

@test "sql-create-tables: duplicate column errors" {
    run dolt sql <<SQL
CREATE TABLE test1 (
  a bigint primary key,
  A bigint
);
SQL
    [ "$status" -eq 1 ]
    [[ "$output" =~ "same name" ]] || false

    run dolt sql <<SQL
CREATE TABLE test1 (
  a bigint primary key,
  a bigint
);
SQL
    [ "$status" -eq 1 ]
    [[ "$output" =~ "same name" ]] || false

    dolt sql <<SQL
CREATE TABLE test1 (
  a bigint primary key,
  b bigint
);
SQL

    run dolt sql -q "alter table test1 rename column b to a"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "already exists" ]] || false
    
    run dolt sql -q "alter table test1 add column A int"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "already exists" ]] || false

    run dolt sql -q "alter table test1 change column b A bigint"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "name" ]] || false
}

@test "sql-create-tables: Can create a temporary table that lasts the length of a session" {
    run dolt sql -q "CREATE TEMPORARY TABLE mytemptable(pk int PRIMARY KEY)"
    [ "$status" -eq 0 ]

    run dolt ls
    [ "$status" -eq 0 ]
    ! [[ "$output" =~ "mytemptable" ]] || false

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false
}

@test "sql-create-tables: Temporary tables can have data inserted and retrieved" {
    run dolt sql <<SQL
CREATE TEMPORARY TABLE colors (
    id INT NOT NULL,
    color VARCHAR(32) NOT NULL,

    PRIMARY KEY (id)
);

INSERT INTO colors VALUES (1,'red'),(2,'green'),(3,'blue');
SELECT * FROM colors;
SQL
    [[ "$output" =~ "| id | color |" ]] || false
    [[ "$output" =~ "1  | red" ]] || false
    [[ "$output" =~ "2  | green" ]] || false
    [[ "$output" =~ "3  | blue" ]] || false

    run dolt ls
    [ "$status" -eq 0 ]
    ! [[ "$output" =~ "colors" ]] || false

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false
}

@test "sql-create-tables: Temporary tables with indexes can have data inserted and retrieved" {
    run dolt sql <<SQL
CREATE TEMPORARY TABLE colors (
    id INT NOT NULL,
    color VARCHAR(32) NOT NULL,
    PRIMARY KEY (id),
    INDEX color_index(color)
);

INSERT INTO colors VALUES (1,'red'),(2,'green'),(3,'blue');
SELECT * FROM colors;
SQL
    [[ "$output" =~ "| id | color |" ]] || false
    [[ "$output" =~ "1  | red" ]] || false
    [[ "$output" =~ "2  | green" ]] || false
    [[ "$output" =~ "3  | blue" ]] || false

    run dolt ls
    [ "$status" -eq 0 ]
    ! [[ "$output" =~ "colors" ]] || false

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false
}

@test "sql-create-tables: Create temporary table select from another table works" {
    run dolt sql <<SQL
CREATE TABLE colors (
    id INT NOT NULL,
    color VARCHAR(32) NOT NULL,

    PRIMARY KEY (id),
    INDEX color_index(color)
);

INSERT INTO colors VALUES (1,'red'),(2,'green'),(3,'blue');
CREATE TEMPORARY TABLE mytemptable SELECT * FROM colors;
SELECT * from mytemptable;
SQL
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| id | color |" ]] || false
    [[ "$output" =~ "1  | red" ]] || false
    [[ "$output" =~ "2  | green" ]] || false
    [[ "$output" =~ "3  | blue" ]] || false

    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "colors" ]] || false
    ! [[ "$output" =~ "mytemptable" ]] || false
}

@test "sql-create-tables: You can drop temp tables" {
    run dolt sql <<SQL
CREATE TEMPORARY TABLE mytemptable(pk int PRIMARY KEY);
DROP TABLE mytemptable
SQL
    [ "$status" -eq 0 ]

    run dolt ls
    [ "$status" -eq 0 ]
    ! [[ "$output" =~ "colors" ]] || false

    # Double check with an additional query
    run dolt sql <<SQL
CREATE TEMPORARY TABLE mytemptable(pk int PRIMARY KEY);
DROP TABLE mytemptable;
SELECT * FROM mytemptable;
SQL
    [ "$status" -eq 1 ]
    [[ "$output" =~ "table not found: mytemptable" ]] || false

    run dolt ls
    [ "$status" -eq 0 ]
    ! [[ "$output" =~ "colors" ]] || false

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false
}


@test "sql-create-tables: You can create a temp table of the same name as a normal table. Run it through operations" {
    run dolt sql <<SQL
CREATE TABLE goodtable(pk int PRIMARY KEY);

CREATE TEMPORARY TABLE goodtable(pk int PRIMARY KEY);
INSERT INTO goodtable VALUES (1);

SELECT * FROM goodtable;
DROP TABLE goodtable;
SQL
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| pk |" ]] || false
    [[ "$output" =~ "| 1  |" ]] || false

    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "goodtable" ]] || false

    run dolt sql -q "SELECT COUNT(*) FROM goodtable;"
    [[ "$output" =~ "0" ]] || false
}

@test "sql-create-tables: You can create a normal table even if a temporary table exists with the same name" {
    run dolt sql <<SQL
CREATE TEMPORARY TABLE goodtable(pk int PRIMARY KEY);
INSERT INTO goodtable VALUES (1);

CREATE TABLE goodtable(pk int PRIMARY KEY);

SELECT * FROM goodtable;
DROP TABLE goodtable;
SQL
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| pk |" ]] || false
    [[ "$output" =~ "| 1  |" ]] || false

    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "goodtable" ]] || false

    run dolt sql -q "SELECT COUNT(*) FROM goodtable;"
    [[ "$output" =~ "0" ]] || false
}

@test "sql-create-tables: Alter on a temporary table" {
    skip "unskip once DDL operations are moved to the SQL engine"

    run dolt sql <<SQL
CREATE TEMPORARY TABLE goodtable(pk int PRIMARY KEY);
ALTER TABLE goodtable ADD COLUMN val int;

INSERT INTO goodtable VALUES (1, 1);

SELECT * FROM goodtable;
SQL
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| pk | val |" ]] || false
    [[ "$output" =~ "+----+-----+" ]] || false
    [[ "$output" =~ "| 1  | 1   |" ]] || false

    run dolt ls
    [ "$status" -eq 0 ]
    ! [[ "$output" =~ "goodtable" ]] || false

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false
}

@test "sql-create-tables: Creating a foreign key on a temporary table throws an error" {
    run dolt sql <<SQL
CREATE TABLE colors (
    id INT NOT NULL,
    color VARCHAR(32) NOT NULL,

    PRIMARY KEY (id),
    INDEX color_index(color)
);

CREATE TEMPORARY TABLE objects (
    id INT NOT NULL,
    name VARCHAR(64) NOT NULL,
    color VARCHAR(32),

    PRIMARY KEY(id),
    FOREIGN KEY (color) REFERENCES colors(color)
);

SQL
    [ "$status" -eq 1 ]
    [[ "$output" =~ "temporary tables do not support foreign key" ]] || false

    # Now try with an alter
    run dolt sql <<SQL
CREATE TEMPORARY TABLE objects (
    id INT NOT NULL,
    name VARCHAR(64) NOT NULL,
    color VARCHAR(32),

    PRIMARY KEY(id)
);

ALTER TABLE objects ADD FOREIGN KEY (color) REFERENCES colors(color);
SQL
    [ "$status" -eq 1 ]
    [[ "$output" =~ "temporary tables do not support foreign key" ]] || false

    run dolt ls
    [ "$status" -eq 0 ]
    ! [[ "$output" =~ "objects" ]] || false
    [[ "$output" =~ "colors" ]] || false
}

@test "sql-create-tables: Temporary table supports UNIQUE Index" {
     run dolt sql <<SQL
CREATE TEMPORARY TABLE mytemptable (
    pk INT NOT NULL,
    val INT,

    PRIMARY KEY (pk),
    UNIQUE INDEX (val)
);

INSERT IGNORE INTO mytemptable VALUES (1,1);
INSERT IGNORE INTO mytemptable VALUES (2,1);
SELECT * FROM mytemptable;
SQL
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| pk | val |" ]] || false
    [[ "$output" =~ "+----+-----+" ]] || false
    [[ "$output" =~ "| 1  | 1   |" ]] || false
    ! [[ "$output" =~ "| 2  | 1   |" ]] ||

    run dolt ls
    [ "$status" -eq 0 ]
    ! [[ "$output" =~ "mytemptable" ]] || false

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false
}

@test "sql-create-tables: create like with temporary tables" {
      run dolt sql <<SQL
CREATE TABLE mytable (
    pk int PRIMARY KEY,
    val int
);

CREATE TEMPORARY TABLE mytemptable like mytable;
INSERT INTO mytemptable VALUES (1,1),(2,1);
SELECT * from mytemptable;
SQL

    [ "$status" -eq 0 ]
    [[ "$output" =~ "| pk | val |" ]] || false
    [[ "$output" =~ "+----+-----+" ]] || false
    [[ "$output" =~ "| 1  | 1   |" ]] || false
    [[ "$output" =~ "| 2  | 1   |" ]] || false
}

@test "sql-create-tables: create temporary table like from other database" {
    mkdir repo1
    cd repo1
    dolt init
    dolt sql -q "CREATE TABLE tableone(pk bigint primary key, v1 int)"
    cd ..
    mkdir repo2
    cd repo2
    dolt init
    cd ..
    run dolt sql --multi-db-dir ./ -b -q "USE repo2;CREATE TEMPORARY TABLE temp2 LIKE repo1.tableone;"
    [ "$status" -eq 0 ]
    cd repo2

    run dolt ls
    [ "$status" -eq 0 ]
    ! [[ "$output" =~ "temp2" ]] || false
}

@test "sql-create-tables: verify that temporary tables appear in the innodb_temp_table_info table" {
      run dolt sql <<SQL
CREATE TEMPORARY TABLE mytemptable (
    pk int PRIMARY KEY,
    val int
);

SELECT name FROM information_schema.innodb_temp_table_info;
SQL
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| mytemptable |" ]] || false

    run dolt ls
    [ "$status" -eq 0 ]
    ! [[ "$output" =~ "mytemptable" ]] || false

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false
}

@test "sql-create-tables: temporary tables can be queried in a case insensitive way" {
    run dolt sql <<SQL
CREATE TEMPORARY TABLE myTempTable (
    pk int PRIMARY KEY,
    val int
);

INSERT INTO myTempTABLE VALUES (1,1),(2,2),(3,3);
DELETE FROM MyTempTable where pk = 3;
SELECT * FROM myTEMPTable;
SQL

    [ "$status" -eq 0 ]
    [[ "$output" =~ "| pk | val |" ]] || false
    [[ "$output" =~ "+----+-----+" ]] || false
    [[ "$output" =~ "| 1  | 1   |" ]] || false
    [[ "$output" =~ "| 2  | 2   |" ]] || false

    run dolt sql <<SQL
CREATE TEMPORARY TABLE myTempTable (
    pk int PRIMARY KEY,
    val int
);

INSERT INTO myTempTABLE VALUES (1,1),(2,2),(3,3);
DROP TABLE myTempTABLE;
SELECT * FROM myTempTable;
SQL
    [ "$status" -eq 1 ]
    [[ "$output" =~ "table not found: myTempTable" ]] || false
}
