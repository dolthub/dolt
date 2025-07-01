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
  dolt sql -q "$(mutations_and_gc_statement)"
  
  mkdir -p preserved_copy/.dolt
  cp -R .dolt/* preserved_copy/.dolt

  dolt archive --purge
  
  # Verify one darc file was create
  archive_files=$(find .dolt -name "*.darc" | wc -l | sed 's/[ \t]//g')
  [ "$archive_files" -eq "1" ]

  # Find file in .dolt not in preserved_copy/.dolt
  extra_in_dolt=$(cd .dolt && find . -type f ! -exec test -e "../preserved_copy/.dolt/{}" \; -print)

  # Find file in preserved_copy/.dolt not in .dolt
  extra_in_preserved=$(cd preserved_copy/.dolt && find . -type f ! -exec test -e "../../.dolt/{}" \; -print)

  # convert paths to 32 char file ids.
  extra_in_dolt=${extra_in_dolt##*/}
  extra_in_dolt=${extra_in_dolt%.darc}
  extra_in_preserved=${extra_in_preserved##*/}

  # The extra file in .dolt should be darc file.
  cp ".dolt/$extra_in_dolt" preserved_copy/.dolt/noms/oldgen

  cd preserved_copy

  # 6) Run `dolt admin compare-and-swap-storage --from <old table id> --to <new table id>
  # Note: This test is expected to fail since we haven't implemented the --from and --to flags yet
  run dolt admin compare-and-swap-storage --from "$old_table_id" --to "$new_table_id"
  [ "$status" -eq 0 ]

  # Verify that the preserved copy has the new file as a storage artifact, and that the old file
  # is not used for storage. The file will still exist, but it won't be in the manifest.
  run dolt admin storage
  [[ ! "$output" =~ "$extra_in_preserved" ]] || false
  [[ "$output" =~ "$extra_in_dolt" ]] || false
}

@test "compare-and-swap-storage: test with invalid table IDs" {
  # Create a repo with enough chunks
  dolt sql -q "$(mutations_and_gc_statement)"
  
  # Test with invalid/non-existent table IDs
  run dolt admin compare-and-swap-storage --from "invalid_id_1" --to "invalid_id_2"
  
  # Should fail with invalid IDs
  [ "$status" -ne 0 ]
}

