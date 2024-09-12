#! /usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
  skiponwindows "Need to install expect and make this script work on windows."
  setup_common

  dolt sql <<SQL
CREATE TABLE names (pk int primary key, name varchar(8));
CREATE TABLE colors (pk int primary key, name varchar(8), red int, green int, blue int);
CREATE TABLE coordinates (pk int primary key, x float, y float);

INSERT INTO names VALUES (1, 'neil');
INSERT INTO names VALUES (2, 'sami');
INSERT INTO names VALUES (3, 'jane');
INSERT INTO colors VALUES (1, 'Red', 255, 0, 0);
INSERT INTO colors VALUES (2, 'Green', 0, 255, 0);
INSERT INTO colors VALUES (3, 'Blue', 0, 0, 255);
INSERT INTO coordinates VALUES (1, 1.1, 2.2);
INSERT INTO coordinates VALUES (2, 3.3, 4.4);
INSERT INTO coordinates VALUES (3, 5.5, 6.6);

CALL dolt_commit('-Am', 'main commit 1');

UPDATE names SET name = 'joey' WHERE pk = 1;
UPDATE colors SET name = 'SkyBlue', red = 0, green = 128, blue = 255 WHERE pk = 3;
UPDATE coordinates SET y = 100.001 WHERE pk = 3;

INSERT INTO names VALUES (4,'john');
INSERT INTO colors VALUES (0, 'Yellow', 255, 255, 0); // use 0 pk to ensure ordering is different from other tables.
INSERT INTO coordinates VALUES (4, 42.24, 23.32);

DELETE FROM names WHERE pk = 2;
DELETE FROM colors WHERE pk = 2;
DELETE FROM coordinates WHERE pk = 2;
SQL

# The default setup has three tables, at this stage there have the a single update, insert, and delete
# on each table. In addition, each contains an unmodified row. Finally, the diff shows that the updates
# to each table are in different orders based on the primary key.
#
# diff --dolt a/colors b/colors
# --- a/colors
# +++ b/colors
# +---+----+---------+-----+-------+------+
# |   | pk | name    | red | green | blue |
# +---+----+---------+-----+-------+------+
# | + | 0  | Yellow  | 255 | 255   | 0    |
# | - | 2  | Green   | 0   | 255   | 0    |
# | < | 3  | Blue    | 0   | 0     | 255  |
# | > | 3  | SkyBlue | 0   | 128   | 255  |
# +---+----+---------+-----+-------+------+
# diff --dolt a/coordinates b/coordinates
# --- a/coordinates
# +++ b/coordinates
# +---+----+-------+---------+
# |   | pk | x     | y       |
# +---+----+-------+---------+
# | - | 2  | 3.3   | 4.4     |
# | < | 3  | 5.5   | 6.6     |
# | > | 3  | 5.5   | 100.001 |
# | + | 4  | 42.24 | 23.32   |
# +---+----+-------+---------+
# diff --dolt a/names b/names
# --- a/names
# +++ b/names
# +---+----+------+
# |   | pk | name |
# +---+----+------+
# | < | 1  | neil |
# | > | 1  | joey |
# | - | 2  | sami |
# | + | 4  | john |
# +---+----+------+
#

}

teardown() {
    teardown_common
}

# bats test_tags=no_lambda
@test "add-patch: clean workspace" {
  dolt reset --hard

  run dolt add --patch

  [ "$status" -eq 0 ]
  [[ "$output" =~ "No changes." ]] || false
}

# bats test_tags=no_lambda
@test "add-patch: all changes staged" {
  dolt add .

  run dolt add --patch

  [ "$status" -eq 0 ]
  [[ "$output" =~ "No changes." ]] || false
}

# bats test_tags=no_lambda
@test "add-patch: help and quit" {
  run dolt sql -r csv -q "select dolt_hashof_db()"
  [ $status -eq 0 ]
  ORIG_DB_HASH=$(echo "$output" | awk 'NR==2')

  run $BATS_TEST_DIRNAME/add-patch-expect/help_quit.expect

  [ $status -eq 0 ]

  run dolt sql -r csv -q "select dolt_hashof_db()"
  [ $status -eq 0 ]
  DB_HASH=$(echo "$output" | awk 'NR==2')

  # Verify that the state of the database hasn't changed.
  [[ "$DB_HASH" == "$ORIG_DB_HASH" ]] || false
}

# bats test_tags=no_lambda
@test "add-patch: a then d for two tables" {
  # This test does: `add -p coordinates colors` -> 'a' -> 'd'
  run $BATS_TEST_DIRNAME/add-patch-expect/all_none.expect
  [ $status -eq 0 ]

  run dolt sql -q "select name from colors AS OF STAGED"
  [ $status -eq 0 ]
  [[ "$output" =~ "Red"     ]] || false
  [[ "$output" =~ "SkyBlue" ]] || false
  [[ "$output" =~ "Yellow"  ]] || false
  [[ ! "$output" =~ "Green" ]] || false

  # Should be no changes on coordinates.
  run dolt sql -q "select pk, y from coordinates AS OF STAGED"
  [ $status -eq 0 ]
  [[ "$output" =~ "| 1  | 2.2 |"   ]] || false
  [[ "$output" =~ "| 2  | 4.4 |"   ]] || false
  [[ "$output" =~ "| 3  | 6.6 |"   ]] || false
  [[ ! "$output" =~ "23.32" ]] || false # Value for inserted row - should not be there.

  # Should be no changes on names.
  run dolt sql -q "select pk, name from names AS OF STAGED"
  [ $status -eq 0 ]
  [[ "$output" =~ "| 1  | neil |" ]] || false
  [[ "$output" =~ "| 2  | sami |" ]] || false
  [[ "$output" =~ "| 3  | jane |" ]] || false
  [[ ! "$output" =~ "john" ]] || false # Value for inserted row - should not be there.
}

# bats test_tags=no_lambda
@test "add-patch: y/n repeatedly with restarts" {
  # This test repeatedly does 'y/n/y/s' until the program exits.
  run $BATS_TEST_DIRNAME/add-patch-expect/restart_multiple_times.expect
  [ $status -eq 0 ]

  run dolt sql -q "select name from colors AS OF STAGED"
  [ $status -eq 0 ]
  [[ "$output" =~ "Red"     ]] || false
  [[ "$output" =~ "SkyBlue" ]] || false
  [[ "$output" =~ "Yellow"  ]] || false
  [[ ! "$output" =~ "Green" ]] || false

  run dolt sql -q "select pk, y from coordinates AS OF STAGED"
  [ $status -eq 0 ]
  [[ "$output" =~ "| 1  | 2.2     |"   ]] || false
  [[ "$output" =~ "| 3  | 100.001 |"   ]] || false
  [[ "$output" =~ "| 4  | 23.32   |"   ]] || false

  run dolt sql -q "select pk, name from names AS OF STAGED"
  [ $status -eq 0 ]
  [[ "$output" =~ "| 1  | neil |" ]] || false
  [[ "$output" =~ "| 3  | jane |" ]] || false
  [[ ! "$output" =~ "john" ]] || false # last promoted row - should not be staged.
}

# bats test_tags=no_lambda
@test "add-patch: y then d" {
  # Accept the first change for each table, then skip the rest.
  run $BATS_TEST_DIRNAME/add-patch-expect/yes_then_d.expect
  [ $status -eq 0 ]

  run dolt sql -q "select name from colors AS OF STAGED"
  [ $status -eq 0 ]
  [[ "$output" =~ "Yellow"  ]] || false
  [[ "$output" =~ "Red"     ]] || false
  [[ "$output" =~ "Green"   ]] || false
  [[ "$output" =~ "Blue"    ]] || false
  # verify no extra rows in table we didn't look for.
  run dolt sql -q "select sum(pk) as s from colors AS OF STAGED"
  [ $status -eq 0 ]
  [[ "$output" =~ "| 6 |" ]] || false # Yellow added as pk=0, so 0+1+2+3

  run dolt sql -q "select pk, y from coordinates AS OF STAGED"
  [ $status -eq 0 ]
  [[ "$output" =~ "| 1  | 2.2 |"   ]] || false
  [[ "$output" =~ "| 3  | 6.6 |"   ]] || false
  # verify no extra rows in table we didn't look for.
  run dolt sql -q "select sum(pk) as s from coordinates AS OF STAGED"
  [ $status -eq 0 ]
  [[ "$output" =~ "| 4 |" ]] || false

  run dolt sql -q "select pk, name from names AS OF STAGED"
  [ $status -eq 0 ]
  [[ "$output" =~ "| 1  | joey |" ]] || false
  [[ "$output" =~ "| 2  | sami |" ]] || false
  [[ "$output" =~ "| 3  | jane |" ]] || false
  run dolt sql -q "select sum(pk) as s from names AS OF STAGED"
  [ $status -eq 0 ]
  [[ "$output" =~ "| 6 |" ]] || false
}

# bats test_tags=no_lambda
@test "add-patch: n then a" {
  # Accept the reject fir change, then accept the rest.
  run $BATS_TEST_DIRNAME/add-patch-expect/no_then_a.expect
  [ $status -eq 0 ]

  run dolt sql -q "select pk,name from colors AS OF STAGED"
  [ $status -eq 0 ]
  [[ "$output" =~ "| 1  | Red     |" ]] || false
  [[ "$output" =~ "| 3  | SkyBlue |" ]] || false
  # verify no extra rows in table we didn't look for.
  run dolt sql -q "select sum(pk) as s from colors AS OF STAGED"
  [ $status -eq 0 ]
  [[ "$output" =~ "| 4 |" ]] || false

  run dolt sql -q "select pk, y from coordinates AS OF STAGED"
  [ $status -eq 0 ]
  [[ "$output" =~ "| 1  | 2.2     |"   ]] || false
  [[ "$output" =~ "| 2  | 4.4     |"   ]] || false
  [[ "$output" =~ "| 3  | 100.001 |"   ]] || false
  [[ "$output" =~ "| 4  | 23.32   |"   ]] || false
  # verify no extra rows in table we didn't look for.
  run dolt sql -q "select sum(pk) as s from coordinates AS OF STAGED"
  [ $status -eq 0 ]
  [[ "$output" =~ "| 10 |" ]] || false

  run dolt sql -q "select pk, name from names AS OF STAGED"
  [ $status -eq 0 ]
  [[ "$output" =~ "| 1  | neil |" ]] || false
  [[ "$output" =~ "| 2  | sami |" ]] || false
  [[ "$output" =~ "| 3  | jane |" ]] || false
  run dolt sql -q "select sum(pk) as s from names AS OF STAGED"
  [ $status -eq 0 ]
  [[ "$output" =~ "| 6 |" ]] || false
}

# Test needed:
# add keyless table tests.
