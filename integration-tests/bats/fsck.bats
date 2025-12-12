#! /usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_no_dolt_init
}

teardown() {
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

@test "fsck: bad commit" {
    mkdir .dolt
    cp -R $BATS_CWD/corrupt_dbs/bad_commit/* .dolt/

    # validate that cp worked.
    dolt status

    run dolt fsck
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Chunk: rlmgv0komq0oj7qu4osdo759vs4c5pvg content hash mismatch: gpphmuvegiedtjtbfku4ru8jalfdk21u" ]] || false
    [[ "$output" =~ "hacky@hackypants.com" ]] || false
}

@test "fsck: good archive" {
    dolt init
    dolt sql -q "create table tbl (i int auto_increment primary key, guid char(36))"
    dolt commit -A -m "create tbl"

    stmt=""
    for ((j=1; j<=10; j++))
    do
        stmt="$stmt $(insert_statement)"
        stmt="$stmt $(update_statement)"
    done
    dolt sql -q "$stmt"

    dolt gc
    dolt archive

    dolt fsck
}

@test "fsck: good journal" {
    dolt init
    dolt sql -q "create table tbl (i int auto_increment primary key, guid char(36))"
    dolt commit -Am "Create table tbl"

    dolt sql -q "$(insert_statement)"

    # Objects are in the journal. Don't gc.
    dolt fsck
}

@test "fsck: bad journal crc" {
    mkdir .dolt
    cp -R $BATS_CWD/corrupt_dbs/bad_journal_crc/* .dolt/

    run dolt fsck
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Chunk: 7i48kt4h41hcjniri7scv5m8a69cdn13 content hash mismatch: hitg0bb0hsakip96qvu2hts0hkrrla9o" ]] || false
}

@test "fsck: bad journal crc, suggests data loss recovery" {
  mkdir .dolt
  cp -R $BATS_CWD/corrupt_dbs/bad_journal_crc_2/* .dolt/

  run dolt fsck
  [ "$status" -eq 1 ]
  [[ "$output" =~ "WARNING: Chunk journal is corrupted and some data may be lost." ]] || false
  [[ "$output" =~ "Run \`dolt fsck --revive-journal-with-data-loss\`" ]] || false
}

@test "fsck: recover from broken journal" {
  dolt init
  dolt sql -q "create table recovermeplease (i int auto_increment primary key, guid char(36))"
  dolt commit -Am "Create table recovermeplease"

  local journal=".dolt/noms/vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv"
  # append some bytes which are going to stop the journal reading process, then append a couple records.
  printf '\x00\x00\x00\x00' >> "$journal"
  cat $BATS_CWD/corrupt_dbs/journal_data.bin >> "$journal"

  run dolt status
  [ "$status" -eq 1 ]
  [[ "$output" =~ "please run 'dolt fsck' to assess the damage and attempt repairs" ]] || false

  run dolt fsck
  [ "$status" -eq 1 ]
  [[ "$output" =~ "dolt fsck --revive-journal-with-data-loss" ]] || false

  run dolt fsck --revive-journal-with-data-loss
  [ "$status" -eq 0 ]
  [[ "$output" =~ "please file a ticket: https://github.com/dolthub/dolt/issues" ]] || false

  dolt fsck

  run dolt show
  [ "$status" -eq 0 ]
  [[ "$output" =~ "recovermeplease" ]] || false

  # Look for the save file.
  ls .dolt/noms/vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv_save_* >/dev/null 2>&1
}

@test "fsck: do nothing when nothing is wrong with journal" {
  dolt init
  dolt sql -q "create table recovermenoloss (i int auto_increment primary key, guid char(36))"
  dolt commit -Am "Create table recovermenoloss"

  run dolt fsck --revive-journal-with-data-loss
  [ "$status" -eq 1 ]
  [[ "$output" =~ "no data loss detected in chunk journal file; no recovery performed" ]] || false

  run dolt show
  [ "$status" -eq 0 ]
  [[ "$output" =~ "recovermenoloss" ]] || false
}

@test "fsck: missing closure object" {
    mkdir .dolt
    cp -R $BATS_CWD/corrupt_dbs/missing_closure_object/* .dolt/

    run dolt fsck
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Commit 00apprui1m4mtcs8umenpt8e2lkjmihc is missing data. Failed to read commit closure d92u2dpnhocp5pv4pn7vgm9fs30vdv94" ]] || false
}

@test "fsck: missing schema object" {
  mkdir .dolt
  cp -R $BATS_CWD/corrupt_dbs/missing_schema_object/* .dolt/

  run dolt fsck
  [ "$status" -eq 1 ]
  [[ "$output" =~ "commit::gub2hagj8cp2mcdlp95l90sisp37iupd: tree 2dr5okftnc3velmrt0191j479jiel7de -> (missing) 8tsjiu5fcsvchoo4re8bgftuuogl7ko1" ]] || false
}

