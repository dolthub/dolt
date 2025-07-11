# Helper functions for generating test data efficiently
#
# PURPOSE:
# This file provides utilities for quickly generating substantial amounts of test data
# to trigger garbage collection and create storage files in oldgen. The functions create
# large SQL statements that perform many operations in batches, which is much faster
# than individual commits in loops.
#
# TABLE SCHEMA:
# The functions expect a table named 'tbl' with the following schema:
#   CREATE TABLE tbl (
#     i int auto_increment primary key,
#     guid char(36)
#   )
#
# USAGE PATTERN:
# 1. Call create_test_table() to set up the initial table
# 2. Call mutations_and_gc_statement() one or more times to generate data and trigger GC
# 3. Since GC is used, all data ends up in oldgen tablefiles.
#
# The mutations_and_gc_statement() function performs:
# - 10 cycles of update operations (10 updates per cycle) with commits
# - 10 cycles of insert operations (25 inserts per cycle) with commits  
# - A final garbage collection call

# Inserts 25 new rows and commits them.
insert_statement() {
  res="INSERT INTO tbl (guid) VALUES (UUID());"
  for ((i=1; i<=24; i++))
  do
    res="$res INSERT INTO tbl (guid) VALUES (UUID());"
  done
  res="$res call dolt_commit(\"-A\", \"--allow-empty\", \"-m\", \"Add 25 values\");"
  echo "$res"
}

# Updates 10 random rows and commits the changes.
update_statement() {
  res="SET @max_id = (SELECT MAX(i) FROM tbl);
SET @random_id = FLOOR(1 + RAND() * @max_id);
UPDATE tbl SET guid = UUID() WHERE i >= @random_id LIMIT 1;"
  for ((i=1; i<=9; i++))
  do
    res="$res
SET @max_id = (SELECT MAX(i) FROM tbl);
SET @random_id = FLOOR(1 + RAND() * @max_id);
UPDATE tbl SET guid = UUID() WHERE i >= @random_id LIMIT 1;"
  done
  res="$res call dolt_commit(\"-A\", \"--allow-empty\", \"-m\", \"Update 10 values\");"
  echo "$res"
}

# A series of 10 update-and-commit-then-insert-and-commit pairs, followed by a dolt_gc call
#
# This is useful because we need at least 25 retained chunks to create an archive.
mutations_and_gc_statement() {
  query=`update_statement`
  for ((j=1; j<=9; j++))
  do
    query="$query $(insert_statement)"
    query="$query $(update_statement)"
  done
  query="$query $(insert_statement)"
  query="$query call dolt_gc();"
  echo "$query"
}

# Creates the initial table structure expected by the above functions
create_test_table() {
  dolt sql -q "create table tbl (i int auto_increment primary key, guid char(36))"
  dolt commit -A -m "create tbl"
}