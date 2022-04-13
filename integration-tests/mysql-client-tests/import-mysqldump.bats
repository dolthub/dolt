#!/usr/bin/env bats

setup() {
    REPO_NAME="dolt_repo_$$"
    mkdir $REPO_NAME
    cd $REPO_NAME

    mysql.server start
    # Give the server a chance to start
    sleep 1

    export MYSQL_PWD=""
}

teardown() {
    mysql <<SQL
DROP DATABASE testdb;
SQL
    sleep 1
    mysql.server stop
    cd ..
    rm -rf $REPO_NAME
}

@test "import mysqldump: empty database dump" {
    mysql <<SQL
CREATE DATABASE testdb;
SQL

    mysqldump -B 'testdb' --result-file=dump.sql

    run dolt sql < dump.sql
    [ "$status" -eq 0 ]

    run dolt sql -q "show databases"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "testdb" ]] || false
}

@test "import mysqldump: a simple table dump" {
    mysql <<SQL
CREATE DATABASE testdb;
USE testdb;
CREATE TABLE mytable (pk int NOT NULL PRIMARY KEY, c1 varchar(25) DEFAULT NULL);
INSERT INTO mytable VALUES (0, 'one'), (1, 'two');
SQL

    mysqldump -B 'testdb' --result-file=dump.sql
    run dolt sql < dump.sql
    [ "$status" -eq 0 ]

    run dolt sql -r csv <<SQL
USE testdb;
SELECT * FROM mytable;
SQL
    [[ "$output" =~ "pk,c1
0,one
1,two" ]] || false
}

@test "import mysqldump: database with view" {
    mysql <<SQL
CREATE DATABASE testdb;
USE testdb;
CREATE TABLE mytable (id bigint NOT NULL PRIMARY KEY, col2 bigint DEFAULT '999', col3 datetime DEFAULT CURRENT_TIMESTAMP);
CREATE VIEW myview AS SELECT * FROM mytable;
SQL

    mysqldump -B 'testdb' --result-file=dump.sql
    run dolt sql < dump.sql
    [ "$status" -eq 0 ]

    run dolt sql -r csv <<SQL
USE testdb;
INSERT INTO mytable (id, col3) VALUES (1, TIMESTAMP('2003-12-31'));
SELECT * FROM myview;
SQL
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,999,2003-12-31 00:00:00 +0000 UTC" ]] || false

    run dolt sql -r csv <<SQL
USE testdb;
SHOW CREATE VIEW myview;
SQL
    [ "$status" -eq 0 ]
    [[ "$output" =~ "CREATE VIEW \`myview\` AS select \`mytable\`.\`id\` AS \`id\`,\`mytable\`.\`col2\` AS \`col2\`,\`mytable\`.\`col3\` AS \`col3\` from \`mytable\`" ]] || false
}

@test "import mysqldump: database with trigger" {
    mysql <<SQL
CREATE DATABASE testdb;
USE testdb;
CREATE TABLE mytable (id bigint NOT NULL PRIMARY KEY, v1 bigint DEFAULT NULL);
CREATE TRIGGER tt BEFORE INSERT ON mytable FOR EACH ROW SET NEW.v1 = NEW.v1 * 11;
SQL

    mysqldump -B 'testdb' --result-file=dump.sql
    run dolt sql < dump.sql
    [ "$status" -eq 0 ]

    run dolt sql -r csv <<SQL
USE testdb;
INSERT INTO mytable VALUES (6,8);
SELECT * FROM mytable;
SQL
    [ "$status" -eq 0 ]
    [[ "$output" =~ "6,88" ]] || false

    run dolt sql -r csv <<SQL
USE testdb;
SELECT trigger_name, event_object_table, action_statement, definer FROM information_schema.triggers;
SQL
    [ "$status" -eq 0 ]
    [[ "$output" =~ "tt,mytable,SET NEW.v1 = NEW.v1 * 11,\`root\`@\`localhost\`" ]] || false
}

@test "import mysqldump: database with procedure dumped with --routines flag" {
    mysql <<SQL
CREATE DATABASE testdb;
USE testdb;
CREATE TABLE mytable (id bigint NOT NULL PRIMARY KEY, v1 bigint DEFAULT NULL);
CREATE PROCEDURE new_proc(x DOUBLE, y DOUBLE) SELECT x*y;
SQL

    mysqldump -B 'testdb' --routines --result-file=dump.sql
    run dolt sql < dump.sql
    [ "$status" -eq 0 ]

    run dolt sql -r csv <<SQL
USE testdb;
CALL new_proc(2, 3);
SQL
    [ "$status" -eq 0 ]
    [[ "$output" =~ "6" ]] || false

    run dolt sql -r csv <<SQL
USE testdb;
SHOW PROCEDURE STATUS;
SQL
    [ "$status" -eq 0 ]
    [[ "$output" =~ "new_proc,PROCEDURE,\`root\`@\`localhost\`" ]] || false
}

@test "import mysqldump: a table with string literal representation in column definition" {
    skip "charset introducer needs to be supported in LIKE filter"
    mysql <<SQL
CREATE DATABASE testdb;
USE testdb;
CREATE TABLE mytable (
  pk int NOT NULL,
  col2 int DEFAULT (date_format(now(),_utf8mb4'%Y')),
  col3 varchar(20) NOT NULL DEFAULT 'sometext',
  PRIMARY KEY (pk),
  CONSTRAINT status CHECK ((col3 like _utf8mb4'%sometext%'))
);
SQL

    mysqldump -B 'testdb' --result-file=dump.sql
    run dolt sql < dump.sql
    [ "$status" -eq 0 ]

    run dolt sql -r csv <<SQL
USE testdb;
INSERT INTO mytable VALUES (1, 2003, 'first_sometext');
SELECT * FROM mytable;
SQL
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,2003,first_sometext" ]] || false
}

@test "import mysqldump: charset introducer in tables from mysql db" {
    skip "charset introducer needs to be supported in LIKE filter"

    mysqldump mysql engine_cost --result-file=dump.sql
    run dolt sql < dump.sql
    [ "$status" -eq 0 ]

    skip "generated always as functionality is not supported"
    run dolt sql -q "SHOW CREATE TABLE engine_cost"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "GENERATED ALWAYS AS" ]] || false
}
