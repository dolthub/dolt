#!/bin/bash

# Setup script for descending index compatibility tests.
# Creates a repo using the current dolt containing a table whose secondary index has a descending
# column, which older clients cannot read, next to a table whose index only uses the default order.

set -eo pipefail

mkdir "$1"
cd "$1"

dolt init

dolt sql <<SQL
CREATE TABLE desc_index (
  pk INT NOT NULL PRIMARY KEY,
  c_int INT,
  c_varchar VARCHAR(255),
  INDEX c_int_desc (c_int DESC, c_varchar)
);
INSERT INTO desc_index VALUES
  (1, 42, 'hello'),
  (2, 99, 'world'),
  (3, NULL, 'unknown');

CREATE TABLE default_index (
  pk INT NOT NULL PRIMARY KEY,
  c_int INT,
  c_varchar VARCHAR(255),
  INDEX c_int_asc (c_int ASC, c_varchar)
);
INSERT INTO default_index VALUES
  (1, 42, 'hello'),
  (2, 99, 'world'),
  (3, NULL, 'unknown');
SQL

dolt add .
dolt commit -m "tables with descending and default indexes"

dolt sql <<SQL
INSERT INTO desc_index VALUES (4, 7, 'added later');
INSERT INTO default_index VALUES (4, 7, 'added later');
SQL
dolt add .
dolt commit -m "added more rows"
