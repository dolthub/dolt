#!/bin/bash

set -eo pipefail

mkdir "$1"
cd "$1"

dolt init

dolt branch no-data

dolt sql <<SQL
CREATE TABLE abc (
  pk BIGINT NOT NULL,
  a LONGTEXT,
  b DOUBLE,
  w BIGINT,
  x BIGINT,
  PRIMARY KEY (pk)
);
INSERT INTO abc VALUES (0, 'asdf', 1.1, 0, 0);
INSERT INTO abc VALUES (1, 'asdf', 1.1, 0, 0);
INSERT INTO abc VALUES (2, 'asdf', 1.1, 0, 0);
CREATE VIEW view1 AS SELECT 2+2 FROM dual;
CREATE TABLE big (
  pk int PRIMARY KEY,
  str longtext
);
SQL
dolt sql < "../../test_files/big_table.sql"  # inserts 1K rows to `big`
dolt add .
dolt commit -m "initialized data"
dolt branch init


dolt branch other
dolt sql <<SQL
DELETE FROM abc WHERE pk=1;
INSERT INTO abc VALUES (3, 'data', 1.1, 0, 0);
ALTER TABLE abc DROP COLUMN w;
ALTER TABLE abc ADD COLUMN y BIGINT;
UPDATE abc SET y = 121;
SQL
dolt add .
dolt commit -m "made changes to master"

dolt checkout other
dolt sql <<SQL
DELETE FROM abc WHERE pk=2;
INSERT INTO abc VALUES (4, 'data', 1.1, 0, 0);
ALTER TABLE abc DROP COLUMN x;
ALTER TABLE abc ADD COLUMN z BIGINT;
UPDATE abc SET z = 122;
SQL
dolt add .
dolt commit -m "made changes to other"

dolt checkout master
dolt table export abc abc.csv
dolt schema export abc abc_schema.json

# add info to the log
echo
echo "dolt status"
dolt status

echo
echo "dolt branch"
dolt branch

echo
echo "dolt schema show"
dolt schema show

echo
echo "dolt sql -q 'select * from abc;'"
dolt sql -q 'select * from abc;'

echo
echo "dolt_schemas"
dolt sql -q "select * from dolt_schemas"
