#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "chunk-journal: assert chunk journal index is correctly bootstrapped" {
    dolt sql -q "create table t (pk int primary key, c0 text);"
    dolt commit -Am "new table t"

    # chunk journal index is only populated after a sufficient number of chunk
    # records have been written to the journal, see go/store/nbs/journal_writer.go
    echo "insert into t values" > import.sql
    for i in {1..16384}
    do
        echo "  ($i,'$i')," >> import.sql
    done
    echo "  (16385,'16385');" >> import.sql

    dolt sql < import.sql

    # read the database
    dolt status
    [ -s ".dolt/noms/vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv" ]
    [ -s ".dolt/noms/journal.idx" ]

    # write the database
    dolt checkout -b newbranch
    [ -s ".dolt/noms/vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv" ]
    [ -s ".dolt/noms/journal.idx" ]
}

file_size() {
  # mac and linux differ on stat args.
  if stat -c%s "$1" >/dev/null 2>&1; then
    stat -c%s "$1"
  else
    stat -f%z "$1"
  fi
}


@test "chunk-journal: verify null padding on journal file is truncated" {
  local journal=".dolt/noms/vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv"
  [ -f "$journal" ]

  # capture original size
  local orig_size=$(file_size "$journal")

  # append 4K of null bytes. Historically, null bytes were found in journal files all the time.
  dd if=/dev/zero bs=4096 count=1 2>/dev/null >>"$journal"

  # sanity check size actually grew
  local grown_size=$(file_size "$journal")
  [ "$grown_size" -gt "$orig_size" ]

  # This should truncate the journal.
  dolt status

  local final_size=$(file_size "$journal")

  [ "$final_size" -eq "$orig_size" ]
}

@test "chunk-journal: verify garbage padding on journal file is truncated, reported." {
  local journal=".dolt/noms/vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv"
  [ -f "$journal" ]

  local orig_size=$(file_size "$journal")

  # append some bytes which are going to be parsed as an invalid chunk size of 5242881 (one byte too large)
  printf '\x00\x50\x00\x01' >>"$journal"

  # append 4K of null bytes. This will be ignored, but ensures that we have more than a root hash's worth of garbage.
  dd if=/dev/zero bs=4096 count=1 2>/dev/null >>"$journal"

  # This should truncate the journal.
  run dolt status
  [ "$status" -eq 0 ]
  [[ "$output" =~ "invalid journal record length: 5242881 exceeds max allowed size of 5242880" ]] || false

  local final_size=$(file_size "$journal")

  [ "$final_size" -eq "$orig_size" ]
}

@test "chunk-journal: verify dataloss detection does not truncate the file." {
  local journal=".dolt/noms/vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv"
  [ -f "$journal" ]

  # append some bytes which are going to be parsed as an invalid chunk size of 5242881 (one byte too large)
  printf '\x00\x50\x00\x01' >>"$journal"

  cat $BATS_CWD/corrupt_dbs/journal_data.bin >>"$journal"

  local grown_size=$(file_size "$journal")

  run dolt status
  [ "$status" -eq 1 ]
  [[ "$output" =~ "invalid journal record length: 5242881 exceeds max allowed size of 5242880" ]] || false

  local final_size=$(file_size "$journal")

  # file should be unchanged after mangling it to have data loss.
  [ "$final_size" -eq "$grown_size" ]
}
