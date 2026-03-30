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

CREATE TABLE all_types (
  pk INT NOT NULL PRIMARY KEY,
  c_tinyint TINYINT,
  c_smallint SMALLINT,
  c_mediumint MEDIUMINT,
  c_int INT,
  c_bigint BIGINT,
  c_bigint_u BIGINT UNSIGNED,
  c_float FLOAT,
  c_double DOUBLE,
  c_decimal DECIMAL(10,2),
  c_char CHAR(10),
  c_varchar VARCHAR(255),
  c_tinytext TINYTEXT,
  c_text TEXT,
  c_mediumtext MEDIUMTEXT,
  c_longtext LONGTEXT,
  c_varbinary VARBINARY(255),
  c_tinyblob TINYBLOB,
  c_blob BLOB,
  c_mediumblob MEDIUMBLOB,
  c_longblob LONGBLOB,
  c_date DATE,
  c_time TIME,
  c_datetime DATETIME,
  c_timestamp TIMESTAMP NULL DEFAULT NULL,
  c_year YEAR,
  c_json JSON,
  c_enum ENUM('val1','val2','val3'),
  c_set SET('a','b','c','d')
);
INSERT INTO all_types (pk, c_tinyint, c_smallint, c_mediumint, c_int, c_bigint, c_bigint_u,
  c_float, c_double, c_decimal, c_char, c_varchar,
  c_tinytext, c_text, c_mediumtext, c_longtext,
  c_varbinary, c_tinyblob, c_blob, c_mediumblob, c_longblob,
  c_date, c_time, c_datetime, c_timestamp, c_year,
  c_json, c_enum, c_set) VALUES (
  1,
  100, 1000, 100000, 2000000, 9223372036854775807,
  18446744073709551615,
  1.5, 2.5, 12345.67,
  'hello', 'hello world',
  'tinytext val', 'text val', 'mediumtext val', 'longtext val',
  'varbinary val', 'tinyblob val', 'blob val', 'mediumblob val', 'longblob val',
  '2024-01-15', '13:30:45', '2024-01-15 13:30:45', '2024-01-15 13:30:45',
  2024,
  '{"k":"v"}',
  'val2', 'a');
INSERT INTO all_types (pk, c_tinyint, c_smallint, c_mediumint, c_int, c_bigint, c_bigint_u,
  c_float, c_double, c_decimal, c_char, c_varchar,
  c_tinytext, c_text, c_mediumtext, c_longtext,
  c_varbinary, c_tinyblob, c_blob, c_mediumblob, c_longblob,
  c_date, c_time, c_datetime, c_timestamp, c_year,
  c_json, c_enum, c_set) VALUES (
  2,
  -100, -1000, -100000, -2000000, -9223372036854775807,
  0,
  -1.5, -2.5, -12345.67,
  'hi', 'hi there',
  'tinytext2', 'text val2', 'mediumtext2', 'longtext2',
  'varbinary2', 'tinyblob2', 'blob val2', 'mediumblob2', 'longblob2',
  '2023-12-31', '23:59:59', '2023-12-31 23:59:59', NULL,
  2023,
  '[1,2,3]',
  'val1', 'b');
INSERT INTO all_types (pk, c_text, c_mediumtext, c_longtext, c_blob, c_mediumblob, c_longblob) VALUES (
  3,
  REPEAT('t', 500), REPEAT('m', 500), REPEAT('l', 500),
  REPEAT('b', 500), REPEAT('x', 500), REPEAT('y', 500));

CREATE TABLE geom_types (
  pk INT NOT NULL PRIMARY KEY,
  c_point POINT,
  c_linestring LINESTRING,
  c_polygon POLYGON,
  c_geometry GEOMETRY,
  c_multipoint MULTIPOINT,
  c_multilinestring MULTILINESTRING,
  c_multipolygon MULTIPOLYGON,
  c_geometrycollection GEOMETRYCOLLECTION
);
INSERT INTO geom_types VALUES (
  1,
  ST_GeomFromText('POINT(1 2)'),
  ST_GeomFromText('LINESTRING(0 0,1 1,2 2)'),
  ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))'),
  ST_GeomFromText('POINT(3 4)'),
  ST_GeomFromText('MULTIPOINT(0 0,1 2)'),
  ST_GeomFromText('MULTILINESTRING((0 0,1 1),(2 2,3 3))'),
  ST_GeomFromText('MULTIPOLYGON(((0 0,1 0,1 1,0 1,0 0)))'),
  ST_GeomFromText('GEOMETRYCOLLECTION(POINT(0 0),LINESTRING(1 1,2 2))')
);
INSERT INTO geom_types (pk, c_point, c_geometry) VALUES (
  2,
  ST_GeomFromText('POINT(10 20)'),
  ST_GeomFromText('LINESTRING(0 0,5 5)')
);
CREATE VIEW all_types_view AS SELECT * FROM all_types;
CREATE VIEW geom_view AS SELECT * FROM geom_types;
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

dolt branch check_merge

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

dolt checkout check_merge
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
