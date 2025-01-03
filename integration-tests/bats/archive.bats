#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common

    dolt sql -q "create table tbl (i int auto_increment primary key, guid char(36))"
    dolt commit -A -m "create tbl"

    dolt sql -q "$(insert_statement)"
}

teardown() {
    assert_feature_version
    teardown_common
}

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
# This is useful because we need at least 25 retained chunks to create a commit.
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

@test "archive: too few chunks" {
  dolt sql -q "$(update_statement)"
  dolt gc

  run dolt archive
  [ "$status" -eq 1 ]
  [[ "$output" =~ "Not enough samples to build default dictionary" ]] || false
}

@test "archive: require gc first" {
  run dolt archive
  [ "$status" -eq 1 ]
  [[ "$output" =~ "Run 'dolt gc' first" ]] || false
}

@test "archive: single archive" {
  dolt sql -q "$(mutations_and_gc_statement)"
  dolt archive

  files=$(find . -name "*darc" | wc -l | sed 's/[ \t]//g')
  [ "$files" -eq "1" ]

  # Ensure updates continue to work.
  dolt sql -q "$(update_statement)"
}

@test "archive: multiple archives" {
  dolt sql -q "$(mutations_and_gc_statement)"
  dolt sql -q "$(mutations_and_gc_statement)"
  dolt sql -q "$(mutations_and_gc_statement)"

  dolt archive

  files=$(find . -name "*darc" | wc -l | sed 's/[ \t]//g')
  [ "$files" -eq "3" ]

  # dolt log --stat will load every single chunk.
  commits=$(dolt log --stat --oneline | wc -l | sed 's/[ \t]//g')
  [ "$commits" -eq "186" ]
}

@test "archive: archive multiple times" {
  dolt sql -q "$(mutations_and_gc_statement)"
  dolt archive

  dolt sql -q "$(mutations_and_gc_statement)"
  dolt archive

  files=$(find . -name "*darc" | wc -l | sed 's/[ \t]//g')
  [ "$files" -eq "2" ]
}

@test "archive: archive with remotesrv no go" {
  dolt sql -q "$(mutations_and_gc_statement)"
  dolt archive

  run dolt sql-server --remotesapi-port=12321
  [ "$status" -eq 1 ]
  [[ "$output" =~ "archive files present" ]] || false

  run remotesrv --repo-mode
  [ "$status" -eq 1 ]
  [[ "$output" =~ "archive files present" ]] || false
}

@test "archive: archive --revert (fast)" {
  dolt sql -q "$(mutations_and_gc_statement)"
  dolt archive
  dolt archive --revert

  # dolt log --stat will load every single chunk. 66 manually verified.
  commits=$(dolt log --stat --oneline | wc -l | sed 's/[ \t]//g')
  [ "$commits" -eq "66" ]
}

@test "archive: archive --revert (rebuild)" {
  dolt sql -q "$(mutations_and_gc_statement)"
  dolt archive
  dolt gc                         # This will delete the unused table files.
  dolt archive --revert

  # dolt log --stat will load every single chunk. 66 manually verified.
  commits=$(dolt log --stat --oneline | wc -l | sed 's/[ \t]//g')
  [ "$commits" -eq "66" ]
}

@test "archive: archive backup no go" {
  dolt sql -q "$(mutations_and_gc_statement)"
  dolt archive

  dolt backup add bac1 file://../bac1
  run dolt backup sync bac1

  [ "$status" -eq 1 ]
  [[ "$output" =~ "archive files present" ]] || false

  # currently the cli and stored procedures are different code paths.
  run dolt sql -q "call dolt_backup('sync', 'bac1')"
  [ "$status" -eq 1 ]
  # NM4 - TODO. This message is cryptic, but plumbing the error through is awkward.
  [[ "$output" =~ "Archive chunk source" ]] || false
}
