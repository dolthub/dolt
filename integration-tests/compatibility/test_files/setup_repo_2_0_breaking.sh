#!/bin/bash

# Setup script for 2.0 breaking compatibility tests.
# Creates a repo using the current dolt with DOLT_USE_ADAPTIVE_ENCODING=true,
# containing TEXT and BLOB columns that older clients cannot read.

set -eo pipefail

mkdir "$1"
cd "$1"

dolt init

dolt sql <<SQL
CREATE TABLE text_types (
  pk INT NOT NULL PRIMARY KEY,
  c_tinytext TINYTEXT,
  c_text TEXT,
  c_mediumtext MEDIUMTEXT,
  c_longtext LONGTEXT
);
INSERT INTO text_types VALUES
  (1, 'tiny1', 'text value 1', 'medium text value 1', 'long text value 1'),
  (2, 'tiny2', 'text value 2', 'medium text value 2', 'long text value 2'),
  (3, 'tiny3', REPEAT('t', 500), REPEAT('m', 500), REPEAT('l', 500));

CREATE TABLE blob_types (
  pk INT NOT NULL PRIMARY KEY,
  c_tinyblob TINYBLOB,
  c_blob BLOB,
  c_mediumblob MEDIUMBLOB,
  c_longblob LONGBLOB
);
INSERT INTO blob_types VALUES
  (1, 'tinyblob1', 'blob value 1', 'mediumblob value 1', 'longblob value 1'),
  (2, 'tinyblob2', 'blob value 2', 'mediumblob value 2', 'longblob value 2'),
  (3, 'tinyblob3', REPEAT('b', 500), REPEAT('x', 500), REPEAT('y', 500));

CREATE TABLE mixed_types (
  pk INT NOT NULL PRIMARY KEY,
  c_text TEXT,
  c_blob BLOB,
  c_varchar VARCHAR(255),
  c_int INT
);
INSERT INTO mixed_types VALUES
  (1, 'text val', 'blob val', 'varchar val', 42),
  (2, 'another text', 'another blob', 'another varchar', 99);

CREATE TABLE no_text_blob (
  pk INT NOT NULL PRIMARY KEY,
  c_int INT,
  c_varchar VARCHAR(255)
);
INSERT INTO no_text_blob VALUES
  (1, 42, 'hello'),
  (2, 99, 'world');
SQL

dolt add .
dolt commit -m "initial data with adaptive encoding"

dolt sql <<SQL
INSERT INTO text_types VALUES (4, 'tiny4', 'added later', 'medium added', 'long added');
SQL
dolt add .
dolt commit -m "added more text data"
