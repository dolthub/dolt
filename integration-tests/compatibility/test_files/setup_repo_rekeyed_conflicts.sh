#!/bin/bash

# Setup script for rekeyed conflict compatibility tests.
# Creates a repo using the current dolt with two merges left in progress with conflicts:
#   - rekeyed_left merged rekeyed_right, which both changed the primary key of table rekeyed the same way. The merge
#     rekeys the common ancestor to that primary key, and its conflicts hold the address of the rekeyed ancestor,
#     which older clients cannot read.
#   - plain_left merged plain_right, which conflict on table plain as any merge does.

set -eo pipefail

mkdir "$1"
cd "$1"

dolt init

dolt sql <<SQL
CREATE TABLE rekeyed (a INT NOT NULL, b INT NOT NULL, v INT, PRIMARY KEY (a), KEY vidx (v));
CREATE TABLE plain (pk INT PRIMARY KEY, v INT);
INSERT INTO rekeyed VALUES (1, 1, 10), (2, 2, 20), (3, 3, 30);
INSERT INTO plain VALUES (1, 10), (2, 20);
CALL DOLT_COMMIT('-Am', 'base');
CALL DOLT_BRANCH('rekeyed_left');
CALL DOLT_BRANCH('rekeyed_right');
CALL DOLT_BRANCH('plain_left');
CALL DOLT_BRANCH('plain_right');

CALL DOLT_CHECKOUT('rekeyed_left');
ALTER TABLE rekeyed DROP PRIMARY KEY, ADD PRIMARY KEY (a, b);
UPDATE rekeyed SET v = 100 WHERE a = 1;
CALL DOLT_COMMIT('-am', 'rekeyed_left: change pk');

CALL DOLT_CHECKOUT('rekeyed_right');
ALTER TABLE rekeyed DROP PRIMARY KEY, ADD PRIMARY KEY (a, b);
UPDATE rekeyed SET v = 11 WHERE a = 1;
UPDATE rekeyed SET v = 22 WHERE a = 2;
CALL DOLT_COMMIT('-am', 'rekeyed_right: change pk');

CALL DOLT_CHECKOUT('plain_left');
UPDATE plain SET v = 100 WHERE pk = 1;
CALL DOLT_COMMIT('-am', 'plain_left');

CALL DOLT_CHECKOUT('plain_right');
UPDATE plain SET v = 11 WHERE pk = 1;
CALL DOLT_COMMIT('-am', 'plain_right');
SQL

# Each merge runs in its own session, so that the conflicts stay in the working set of its branch.
dolt sql -q "CALL DOLT_CHECKOUT('rekeyed_left'); SET @@dolt_allow_commit_conflicts = 1; CALL DOLT_MERGE('rekeyed_right');"
dolt sql -q "CALL DOLT_CHECKOUT('plain_left'); SET @@dolt_allow_commit_conflicts = 1; CALL DOLT_MERGE('plain_right');"
