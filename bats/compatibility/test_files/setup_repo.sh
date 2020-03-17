#!/bin/bash

dolt init || exit

dolt sql <<SQL || exit
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
SQL
dolt add . || exit
dolt commit -m "initialized data" || exit
dolt branch init || exit


dolt branch other || exit
dolt sql <<SQL || exit
DELETE FROM abc WHERE pk=1;
INSERT INTO abc VALUES (3, 'data', 1.1, 0, 0);
ALTER TABLE abc DROP COLUMN w;
ALTER TABLE abc ADD COLUMN y BIGINT COMMENT 'tag:104';
SQL
dolt add . || exit
dolt commit -m "made changes to master" || exit

dolt checkout other || exit
dolt sql <<SQL || exit
DELETE FROM abc WHERE pk=2;
INSERT INTO abc VALUES (4, 'data', 1.1, 0, 0);
ALTER TABLE abc DROP COLUMN x;
ALTER TABLE abc ADD COLUMN z BIGINT COMMENT 'tag:105';
SQL
dolt add . || exit
dolt commit -m "made changes to other" || exit

dolt checkout master || exit
dolt table export abc abc.csv || exit
dolt schema export abc abc_schema.json || exit

# add info to the log
echo
echo "dolt status" && dolt status || exit

echo
echo "dolt branch" && dolt branch || exit

echo
echo "dolt schema show" && dolt schema show || exit

echo
echo "dolt sql -q 'select * from abc;'" && dolt sql -q 'select * from abc;' || exit
