#!/bin/bash

dolt init

dolt sql <<SQL
CREATE TABLE abc (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  a LONGTEXT COMMENT 'tag:100',
  b DOUBLE COMMENT 'tag:101',
  w BIGINT COMMENT 'tag:102',
  x BIGINT COMMENT 'tag:103',
  PRIMARY KEY (pk)
);
INSERT INTO abc VALUES (0, 'asdf', 1.1, 0, 0);
INSERT INTO abc VALUES (1, 'asdf', 1.1, 0, 0);
INSERT INTO abc VALUES (2, 'asdf', 1.1, 0, 0);
CREATE TABLE foo (
  pk BIGINT NOT NULL COMMENT 'tag:1',
  PRIMARY KEY (pk)
);
INSERT INTO foo VALUES (0);
CREATE TABLE bar (
  pk BIGINT NOT NULL COMMENT 'tag:2',
  PRIMARY KEY (pk)
);
INSERT INTO bar VALUES (0);
SQL
dolt add .
dolt commit -m "initialized data"
dolt branch init


# dolt branch other
# dolt sql <<SQL
# DELETE FROM abc WHERE pk=1;
# INSERT INTO abc VALUES (3, 'data');
# ALTER TABLE abc DROP COLUMN w;
# ALTER TABLE abc ADD COLUMN y BIGINT COMMENT 'tag:104';
# DROP TABLE foo;
# CREATE TABLE baz (
#   pk BIGINT NOT NULL COMMENT 'tag:3',
#   PRIMARY KEY (pk)
# );
# INSERT INTO baz VALUES (0);
# SQL
# dolt add .
# dolt commit -m "made changes to master"
# dolt branch pre-merge
#
# dolt checkout second
# dolt sql <<SQL
# DELETE FROM abc WHERE pk=2;
# INSERT INTO abc VALUES (4, 'data');
# ALTER TABLE abc DROP COLUMN x;
# ALTER TABLE abc ADD COLUMN z BIGINT COMMENT 'tag:105';
# DROP TABLE bar;
# CREATE TABLE qux (
#   pk BIGINT NOT NULL COMMENT 'tag:4',
#   PRIMARY KEY (pk)
# );
# INSERT INTO qux VALUES (0);
# SQL
# dolt add .
# dolt commit -m "made changes to other"
#
# dolt checkout master
# dolt merge other
# dolt add .
# dolt commit -m "merge other into master"
#
# dolt sql -q "INSERT INTO abc VALUES (2, 'something', '2020-01-14 20:48:37.13061')"
# dolt add .
# dolt commit -m "Added something row"
# dolt checkout master
# dolt checkout -b newcolumn
# dolt sql -q "ALTER TABLE abc ADD COLUMN c BIGINT UNSIGNED COMMENT 'tag:4657'"
# dolt sql -q "UPDATE abc SET c = 2133 WHERE c IS NULL"
# dolt sql -q "INSERT INTO abc VALUES (2, 'something', '2020-01-13 20:48:37.13061', 1132020)"
# dolt add .
# dolt commit -m "Added something row and column c"
# dolt checkout master