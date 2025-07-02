#!/bin/bash

set -eo pipefail

mkdir "$1"
cd "$1"

dolt init

DEFAULT_BRANCH="master"

branches=`dolt branch`
branches=`echo "$branches" | xargs`
echo "$branches"

if [ "$branches" == "* main" ]; then
  DEFAULT_BRANCH="main"
fi

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

CREATE TABLE def (
  i INT check (i > 0)
);
INSERT INTO def VALUES (1), (2), (3);
SQL
dolt sql < "../../test_files/big_table.sql"  # inserts 1K rows to `big`
dolt add .
dolt commit -m "initialized data"
dolt branch init

dolt branch other
dolt sql <<SQL
DELETE FROM abc WHERE pk=1;
UPDATE abc SET x = 1 WHERE pk = 0;
INSERT INTO abc VALUES (3, 'data', 1.1, 0, 0);
ALTER TABLE abc DROP COLUMN w;
ALTER TABLE abc ADD COLUMN y BIGINT;
UPDATE abc SET y = 121;
SQL
dolt add .
dolt commit -m "made changes to $DEFAULT_BRANCH"

dolt checkout other
dolt sql <<SQL
DELETE FROM abc WHERE pk=2;
UPDATE abc SET w = 1 WHERE pk = 0;
INSERT INTO abc VALUES (4, 'data', 1.1, 0, 0);
ALTER TABLE abc DROP COLUMN x;
ALTER TABLE abc ADD COLUMN z BIGINT;
UPDATE abc SET z = 122;
SQL
dolt add .
dolt commit -m "made changes to other"

dolt checkout -b check_merge
dolt sql <<SQL
INSERT INTO def VALUES (5), (6), (7);
SQL
dolt add .
dolt commit -m "made changes to check_merge"

dolt checkout "$DEFAULT_BRANCH"
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

# write default branch to use in ../runner.sh and ./bats/compatibility.bats
echo  "$DEFAULT_BRANCH" > ./default_branch.var
