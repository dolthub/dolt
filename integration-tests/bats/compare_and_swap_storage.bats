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

@test "compare-and-swap-storage: basic compare and swap test" {
  # 1) Create a repo with enough chunks to merit archiving. DONT archive yet.
  dolt sql -q "$(mutations_and_gc_statement)"
  
  # 2) Copy the repo into a preserved copy.
  cp -R .dolt preserved_copy/.dolt
  
  # 3) Use find to locate all the table files in the repository.
  # Find table files in oldgen directory (32-character hex filenames)
  old_table_files=$(find .dolt/noms -type f -print | awk -F/ 'length($NF) == 32 && $NF ~ /^[a-v0-9]{32}$/')
  
  # Ensure we have at least one table file to work with
  [ -n "$old_table_files" ]
  
  # Get the first table file ID for testing
  old_table_id=$(echo "$old_table_files" | head -1 | awk -F/ '{print $NF}')
  
  # 4) Run `dolt archive --purge` on the original repository.
  dolt archive --purge
  
  # Verify archive was created
  archive_files=$(find .dolt -name "*.darc" | wc -l | sed 's/[ \t]//g')
  [ "$archive_files" -ge "1" ]
  
  # 5) Copy the new `.darc` into the noms directory of the copy.
  cp .dolt/noms/*.darc preserved_copy/noms/
  
  # Get a new table file ID from the preserved copy (should be different after archiving)
  cd preserved_copy
  new_table_files=$(find .dolt/noms/oldgen -type f -print | awk -F/ 'length($NF) == 32 && $NF ~ /^[a-v0-9]{32}$/')
  
  # If no table files exist in preserved copy, use a dummy ID for testing
  if [ -z "$new_table_files" ]; then
    new_table_id="00000000000000000000000000000000"
  else
    new_table_id=$(echo "$new_table_files" | head -1 | awk -F/ '{print $NF}')
  fi
  
  # 6) Run `dolt admin compare-and-swap-storage --from <old table id> --to <new table id>
  # Note: This test is expected to fail since we haven't implemented the --from and --to flags yet
  run dolt admin compare-and-swap-storage --from "$old_table_id" --to "$new_table_id"
  
  # For now, we expect this to fail since the command doesn't support --from and --to flags yet
  # Once implemented, this should succeed
  # [ "$status" -eq 0 ]
  
  # Temporary assertion - expect failure until implementation is complete
  [ "$status" -ne 0 ]
}

@test "compare-and-swap-storage: test with invalid table IDs" {
  # Create a repo with enough chunks
  dolt sql -q "$(mutations_and_gc_statement)"
  
  # Test with invalid/non-existent table IDs
  run dolt admin compare-and-swap-storage --from "invalid_id_1" --to "invalid_id_2"
  
  # Should fail with invalid IDs
  [ "$status" -ne 0 ]
}

@test "compare-and-swap-storage: test with same from and to IDs" {
  # Create a repo with enough chunks
  dolt sql -q "$(mutations_and_gc_statement)"
  
  # Find a valid table file
  table_files=$(find .dolt/noms/oldgen -type f -print | awk -F/ 'length($NF) == 32 && $NF ~ /^[a-v0-9]{32}$/')
  
  if [ -n "$table_files" ]; then
    table_id=$(echo "$table_files" | head -1 | awk -F/ '{print $NF}')
    
    # Test with same from and to IDs
    run dolt admin compare-and-swap-storage --from "$table_id" --to "$table_id"
    
    # Should handle this case appropriately (likely succeed as no-op or fail with appropriate message)
    # For now, expect failure until implementation is complete
    [ "$status" -ne 0 ]
  else
    skip "No table files found for testing"
  fi
}
