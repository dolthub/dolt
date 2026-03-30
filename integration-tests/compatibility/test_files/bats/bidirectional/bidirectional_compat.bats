#!/usr/bin/env bats
# Bi-directional compatibility tests: HEAD dolt and an older dolt version interleave reads and
# writes against the same repository across multiple rounds, verifying that each version can always
# read what the other has written.
#
# Each test creates an isolated repository, alternating between old_dolt and new_dolt. The terms old
# and new are kind of arbitrary, since we run tests in both directions (swapping DOLT_LEGACY_BIN and
# DOLT_NEW_BIN).
#
# Note that the setup files in |helper| can't be used here because they use relative paths and we
# are in a subdirectory. So the key pieces of functionality are duplicated in this file.

dolt config --global --add metrics.disabled true > /dev/null 2>&1

setup() {
    cp -Rpf $REPO_DIR bats_repo
    cd bats_repo
}

teardown() {
    cd ..
    rm -rf bats_repo
}

# When DOLT_LEGACY_BIN is set, old_dolt runst that binary; otherwise runs dolt.
old_dolt() {
  if [ -n "$DOLT_LEGACY_BIN" ]; then
    "$DOLT_LEGACY_BIN" "$@"
  else
    dolt "$@"
  fi
}

# When DOLT_NEW_BIN is set, new_dolt runs that binary; otherwise runs dolt.
new_dolt() {
  if [ -n "$DOLT_NEW_BIN" ]; then
    "$DOLT_NEW_BIN" "$@"
  else
    dolt "$@"
  fi
}

# We made a forwards-incompatible change to branch control serialization that causes a panic in
# older dolt clients when reading from a db that was written by a modern client. We want to ignore
# this failure for these tests since it prevents us from finding any other issues.
clear_branch_control() {
    rm -f .doltcfg/branch_control.db
}

# ---------------------------------------------------------------------------
# Test 1: Scalar types DML — INT, VARCHAR, DECIMAL, DATETIME round-trip.
# Four rounds: old → HEAD → old → HEAD, verifying state at each step.
# ---------------------------------------------------------------------------

@test "bidirectional_compat: scalar types round-trip across versions" {
    [ -n "$DOLT_LEGACY_BIN" ] || skip "requires DOLT_LEGACY_BIN"
    [ -n "$DOLT_NEW_BIN" ] || skip "requires DOLT_NEW_BIN"
    
    # Setup: old dolt creates schema and seeds two rows
    old_dolt init
    old_dolt sql <<SQL
CREATE TABLE scalars (
  pk        INT NOT NULL PRIMARY KEY,
  c_int     INT,
  c_varchar VARCHAR(255),
  c_decimal DECIMAL(10,2),
  c_datetime DATETIME
);
INSERT INTO scalars VALUES
  (1, 100, 'old-row-1', 10.50, '2024-01-01 10:00:00'),
  (2, 200, 'old-row-2', 20.75, '2024-06-15 12:30:00');
SQL
    old_dolt add .
    old_dolt commit -m "old: initial data"

    clear_branch_control

    # Round 1: HEAD reads old's rows, inserts its own
    run new_dolt sql -q "SELECT pk, c_varchar, c_decimal FROM scalars WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "1,old-row-1,10.50" ]] || false

    run new_dolt sql -q "SELECT count(*) FROM scalars;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "2" ]] || false

    new_dolt sql -q "INSERT INTO scalars VALUES (3, 300, 'head-row-3', 30.25, '2025-01-15 08:00:00');"
    new_dolt sql -q "UPDATE scalars SET c_varchar='head-updated-1' WHERE pk=1;"
    new_dolt add .
    new_dolt commit -m "head: insert row 3, update row 1"

    clear_branch_control

    # Round 2: old reads HEAD's changes, inserts its own, deletes a row
    run old_dolt sql -q "SELECT pk, c_varchar FROM scalars WHERE pk=3;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "3,head-row-3" ]] || false

    run old_dolt sql -q "SELECT pk, c_varchar FROM scalars WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "1,head-updated-1" ]] || false

    old_dolt sql -q "INSERT INTO scalars VALUES (4, 400, 'old-row-4', 40.00, '2025-03-01 09:00:00');"
    old_dolt sql -q "DELETE FROM scalars WHERE pk=2;"
    old_dolt add .
    old_dolt commit -m "old: insert row 4, delete row 2"

    clear_branch_control

    # Round 3: HEAD reads old's changes
    run new_dolt sql -q "SELECT pk, c_varchar, c_decimal FROM scalars WHERE pk=4;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "4,old-row-4,40.00" ]] || false

    run new_dolt sql -q "SELECT pk FROM scalars WHERE pk=2;" -r csv
    [ "$status" -eq 0 ]
    [[ "${#lines[@]}" -eq 1 ]] || false  # only header, row 2 deleted

    new_dolt sql -q "INSERT INTO scalars VALUES (5, 500, 'head-row-5', 50.50, '2025-06-01 14:00:00');"
    new_dolt sql -q "UPDATE scalars SET c_decimal=99.99 WHERE pk=4;"
    new_dolt add .
    new_dolt commit -m "head: insert row 5, update row 4 decimal"

    clear_branch_control

    # Round 4: old reads final state — verify full picture
    run old_dolt sql -q "SELECT count(*) FROM scalars;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "4" ]] || false  # pks 1,3,4,5

    run old_dolt sql -q "SELECT pk, c_decimal FROM scalars WHERE pk=4;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "4,99.99" ]] || false

    run old_dolt sql -q "SELECT pk, c_varchar FROM scalars WHERE pk=5;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "5,head-row-5" ]] || false
}

# ---------------------------------------------------------------------------
# Test 2: TEXT and BLOB large values — out-of-band storage round-trip.
# Verifies that large values written by one version are intact after the
# other version reads and modifies them.
# ---------------------------------------------------------------------------

@test "bidirectional_compat: large text and blob values round-trip" {
    [ -n "$DOLT_LEGACY_BIN" ] || skip "requires DOLT_LEGACY_BIN"
    [ -n "$DOLT_NEW_BIN" ] || skip "requires DOLT_NEW_BIN"

    # Setup: old dolt creates table with text/blob columns and small initial values
    old_dolt init
    old_dolt sql <<SQL
CREATE TABLE blobs (
  pk       INT NOT NULL PRIMARY KEY,
  c_text   TEXT,
  c_mtext  MEDIUMTEXT,
  c_ltext  LONGTEXT,
  c_blob   BLOB,
  c_lblob  LONGBLOB
);
INSERT INTO blobs (pk, c_text, c_blob) VALUES (1, 'old-small-text', 'old-small-blob');
SQL
    old_dolt add .
    old_dolt commit -m "old: small initial values"

    clear_branch_control

    # Round 1: HEAD reads small value, inserts large values
    run new_dolt sql -q "SELECT pk, c_text FROM blobs WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "1,old-small-text" ]] || false

    new_dolt sql -q "INSERT INTO blobs (pk, c_text, c_mtext, c_ltext, c_blob, c_lblob)
      VALUES (2,
        REPEAT('H', 1000), REPEAT('M', 2000), REPEAT('L', 3000),
        REPEAT('B', 1000), REPEAT('Z', 3000));"
    new_dolt add .
    new_dolt commit -m "head: insert large values"

    clear_branch_control

    # Round 2: old reads HEAD's large values and verifies lengths
    run old_dolt sql -q "SELECT pk, LENGTH(c_text), LENGTH(c_mtext), LENGTH(c_ltext) FROM blobs WHERE pk=2;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "2,1000,2000,3000" ]] || false

    run old_dolt sql -q "SELECT pk, LENGTH(c_blob), LENGTH(c_lblob) FROM blobs WHERE pk=2;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "2,1000,3000" ]] || false

    # Old dolt inserts its own large values and updates row 1
    old_dolt sql -q "INSERT INTO blobs (pk, c_text, c_mtext, c_ltext, c_blob, c_lblob)
      VALUES (3,
        REPEAT('O', 1500), REPEAT('P', 2500), REPEAT('Q', 4000),
        REPEAT('X', 1500), REPEAT('Y', 4000));"
    old_dolt sql -q "UPDATE blobs SET c_text=REPEAT('u', 800), c_blob=REPEAT('v', 800) WHERE pk=1;"
    old_dolt add .
    old_dolt commit -m "old: insert large row 3, update row 1"

    clear_branch_control

    # Round 3: HEAD reads old's large values
    run new_dolt sql -q "SELECT pk, LENGTH(c_text), LENGTH(c_mtext), LENGTH(c_ltext) FROM blobs WHERE pk=3;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "3,1500,2500,4000" ]] || false

    run new_dolt sql -q "SELECT pk, LENGTH(c_text), LENGTH(c_blob) FROM blobs WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "1,800,800" ]] || false

    # HEAD updates row 2's large values in-place
    new_dolt sql -q "UPDATE blobs SET c_ltext=REPEAT('N', 5000), c_lblob=REPEAT('W', 5000) WHERE pk=2;"
    new_dolt add .
    new_dolt commit -m "head: update row 2 to even larger values"

    clear_branch_control

    # Round 4: old reads HEAD's in-place update
    run old_dolt sql -q "SELECT pk, LENGTH(c_ltext), LENGTH(c_lblob) FROM blobs WHERE pk=2;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "2,5000,5000" ]] || false

    run old_dolt sql -q "SELECT count(*) FROM blobs;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "3" ]] || false
}

# ---------------------------------------------------------------------------
# Test 3: Geometry types DML round-trip.
# ---------------------------------------------------------------------------

@test "bidirectional_compat: geometry types round-trip across versions" {
    [ -n "$DOLT_LEGACY_BIN" ] || skip "requires DOLT_LEGACY_BIN"
    [ -n "$DOLT_NEW_BIN" ] || skip "requires DOLT_NEW_BIN"

    # Setup: old dolt creates geometry table
    old_dolt init
    old_dolt sql <<SQL
CREATE TABLE geoms (
  pk         INT NOT NULL PRIMARY KEY,
  c_point    POINT,
  c_line     LINESTRING,
  c_poly     POLYGON,
  c_geometry GEOMETRY
);
INSERT INTO geoms VALUES (
  1,
  ST_GeomFromText('POINT(1 2)'),
  ST_GeomFromText('LINESTRING(0 0,1 1,2 2)'),
  ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))'),
  ST_GeomFromText('POINT(3 4)')
);
SQL
    old_dolt add .
    old_dolt commit -m "old: initial geometry data"

    clear_branch_control

    # Round 1: HEAD reads old's geometry, inserts more
    run new_dolt sql -q "SELECT pk, ST_X(c_point), ST_Y(c_point) FROM geoms WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "1,1,2" ]] || false

    run new_dolt sql -q "SELECT pk, ST_AsText(c_line) FROM geoms WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "LINESTRING" ]] || false

    new_dolt sql -q "INSERT INTO geoms VALUES (
      2,
      ST_GeomFromText('POINT(10 20)'),
      ST_GeomFromText('LINESTRING(0 0,5 5)'),
      ST_GeomFromText('POLYGON((0 0,3 0,3 3,0 3,0 0))'),
      ST_GeomFromText('LINESTRING(1 1,9 9)')
    );"
    new_dolt add .
    new_dolt commit -m "head: insert geometry row 2"

    clear_branch_control

    # Round 2: old reads HEAD's geometry, updates row 1, inserts row 3
    run old_dolt sql -q "SELECT pk, ST_X(c_point), ST_Y(c_point) FROM geoms WHERE pk=2;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "2,10,20" ]] || false

    old_dolt sql -q "UPDATE geoms SET c_point=ST_GeomFromText('POINT(99 88)') WHERE pk=1;"
    old_dolt sql -q "INSERT INTO geoms (pk, c_point, c_geometry)
      VALUES (3, ST_GeomFromText('POINT(30 40)'), ST_GeomFromText('POINT(50 60)'));"
    old_dolt add .
    old_dolt commit -m "old: update row 1 point, insert row 3"

    clear_branch_control

    # Round 3: HEAD reads old's changes
    run new_dolt sql -q "SELECT pk, ST_X(c_point), ST_Y(c_point) FROM geoms WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "1,99,88" ]] || false

    run new_dolt sql -q "SELECT pk, ST_X(c_point), ST_Y(c_point) FROM geoms WHERE pk=3;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "3,30,40" ]] || false

    new_dolt sql -q "UPDATE geoms SET c_geometry=ST_GeomFromText('POLYGON((0 0,10 0,10 10,0 10,0 0))') WHERE pk=2;"
    new_dolt add .
    new_dolt commit -m "head: update row 2 geometry to polygon"

    clear_branch_control

    # Round 4: old reads HEAD's geometry update
    run old_dolt sql -q "SELECT pk, ST_AsText(c_geometry) FROM geoms WHERE pk=2;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "POLYGON" ]] || false

    run old_dolt sql -q "SELECT count(*) FROM geoms;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "3" ]] || false
}

# ---------------------------------------------------------------------------
# Test 4: ADD COLUMN DDL — both versions evolve the schema, each adding
# columns that the other then reads and writes.
# ---------------------------------------------------------------------------

@test "bidirectional_compat: add columns from both versions" {
    [ -n "$DOLT_LEGACY_BIN" ] || skip "requires DOLT_LEGACY_BIN"
    [ -n "$DOLT_NEW_BIN" ] || skip "requires DOLT_NEW_BIN"

    # Setup: old dolt creates a minimal base table
    old_dolt init
    old_dolt sql <<SQL
CREATE TABLE evolving (
  pk     INT NOT NULL PRIMARY KEY,
  c_base INT
);
INSERT INTO evolving VALUES (1, 10), (2, 20), (3, 30);
SQL
    old_dolt add .
    old_dolt commit -m "old: base schema"

    clear_branch_control

    # Round 1: HEAD adds TEXT and DATE columns, populates them
    new_dolt sql -q "ALTER TABLE evolving ADD COLUMN c_text TEXT;"
    new_dolt sql -q "ALTER TABLE evolving ADD COLUMN c_date DATE;"
    new_dolt sql -q "UPDATE evolving SET c_text=CONCAT('text-', pk), c_date='2025-01-01';"
    new_dolt sql -q "INSERT INTO evolving VALUES (4, 40, 'text-4', '2025-02-01');"
    new_dolt add .
    new_dolt commit -m "head: add text and date columns"

    clear_branch_control

    # Round 2: old reads HEAD's new columns
    run old_dolt sql -q "SELECT pk, c_text, c_date FROM evolving WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "1,text-1,2025-01-01" ]] || false

    run old_dolt sql -q "SELECT pk, c_text FROM evolving WHERE pk=4;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "4,text-4" ]] || false

    # Old dolt adds its own columns and inserts a row using all columns
    old_dolt sql -q "ALTER TABLE evolving ADD COLUMN c_int2 INT;"
    old_dolt sql -q "ALTER TABLE evolving ADD COLUMN c_decimal DECIMAL(8,2);"
    old_dolt sql -q "UPDATE evolving SET c_int2=pk*100, c_decimal=pk*1.5;"
    old_dolt sql -q "INSERT INTO evolving VALUES (5, 50, 'text-5', '2025-03-01', 500, 7.50);"
    old_dolt add .
    old_dolt commit -m "old: add int2 and decimal columns"

    clear_branch_control

    # Round 3: HEAD reads old's new columns — all 4 added columns now visible
    run new_dolt sql -q "SELECT pk, c_text, c_date, c_int2, c_decimal FROM evolving WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "1,text-1,2025-01-01,100,1.50" ]] || false

    run new_dolt sql -q "SELECT pk, c_text, c_int2, c_decimal FROM evolving WHERE pk=5;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "5,text-5,500,7.50" ]] || false

    # HEAD adds a geometry column
    new_dolt sql -q "ALTER TABLE evolving ADD COLUMN c_point POINT;"
    new_dolt sql -q "UPDATE evolving SET c_point=ST_GeomFromText(CONCAT('POINT(', pk, ' ', pk, ')'));"
    new_dolt sql -q "INSERT INTO evolving VALUES (6, 60, 'text-6', '2025-04-01', 600, 9.00, ST_GeomFromText('POINT(6 6)'));"
    new_dolt add .
    new_dolt commit -m "head: add point column"

    clear_branch_control

    # Round 4: old reads HEAD's geometry column additions
    run old_dolt sql -q "SELECT pk, c_text, c_int2 FROM evolving WHERE pk=6;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "6,text-6,600" ]] || false

    run old_dolt sql -q "SELECT pk, ST_X(c_point), ST_Y(c_point) FROM evolving WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "1,1,1" ]] || false

    # Old inserts a row using all columns including HEAD's geometry column
    old_dolt sql -q "INSERT INTO evolving VALUES (7, 70, 'text-7', '2025-05-01', 700, 10.50, ST_GeomFromText('POINT(7 7)'));"
    old_dolt add .
    old_dolt commit -m "old: insert using all columns including geometry"

    clear_branch_control

    # Round 5: HEAD reads old's insert with geometry
    run new_dolt sql -q "SELECT pk, c_text, c_decimal, ST_X(c_point) FROM evolving WHERE pk=7;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "7,text-7,10.50,7" ]] || false

    run new_dolt sql -q "SELECT count(*) FROM evolving;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "7" ]] || false
}

# ---------------------------------------------------------------------------
# Test 5: Branch and merge — both versions create branches, commit to them,
# and merge across version boundaries.
# ---------------------------------------------------------------------------

@test "bidirectional_compat: branch and merge across versions" {
    [ -n "$DOLT_LEGACY_BIN" ] || skip "requires DOLT_LEGACY_BIN"
    [ -n "$DOLT_NEW_BIN" ] || skip "requires DOLT_NEW_BIN"

    # Setup: old dolt creates repo with base table and data
    old_dolt init
    MAIN=$(old_dolt branch | sed 's/^\* //' | sed 's/[[:space:]]*$//')
    old_dolt sql <<SQL
CREATE TABLE t_shared (
  pk  INT NOT NULL PRIMARY KEY,
  val VARCHAR(100),
  src VARCHAR(20)
);
INSERT INTO t_shared VALUES (1, 'base-1', 'old'), (2, 'base-2', 'old');
SQL
    old_dolt add .
    old_dolt commit -m "old: base data on $MAIN"

    clear_branch_control

    # Round 1: HEAD creates a feature branch and commits changes to it
    new_dolt checkout -b head_feature
    new_dolt sql -q "INSERT INTO t_shared VALUES (10, 'head-feature-10', 'head');"
    new_dolt sql -q "INSERT INTO t_shared VALUES (11, 'head-feature-11', 'head');"
    new_dolt add .
    new_dolt commit -m "head: feature branch inserts"
    new_dolt checkout "$MAIN"

    clear_branch_control

    # Round 2: old dolt creates its own branch, commits, merges HEAD's feature
    old_dolt checkout -b old_branch
    old_dolt sql -q "INSERT INTO t_shared VALUES (20, 'old-branch-20', 'old');"
    old_dolt add .
    old_dolt commit -m "old: old_branch insert"
    old_dolt checkout "$MAIN"
    old_dolt sql -q "INSERT INTO t_shared VALUES (3, 'base-3', 'old');"
    old_dolt add .
    old_dolt commit -m "old: main insert"

    # Merge HEAD's feature branch into main (old dolt performs the merge)
    old_dolt merge head_feature
    run old_dolt sql -q "SELECT count(*) FROM t_shared;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "5" ]] || false  # 1,2,3,10,11

    run old_dolt sql -q "SELECT pk, val FROM t_shared WHERE pk=10;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "10,head-feature-10" ]] || false

    clear_branch_control

    # Round 3: HEAD reads merged result, merges old_branch too
    run new_dolt sql -q "SELECT count(*) FROM t_shared;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "5" ]] || false

    run new_dolt sql -q "SELECT pk, val FROM t_shared WHERE pk=3;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "3,base-3" ]] || false

    # HEAD merges old's branch (row 20)
    new_dolt merge old_branch
    run new_dolt sql -q "SELECT count(*) FROM t_shared;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "6" ]] || false  # 1,2,3,10,11,20

    run new_dolt sql -q "SELECT pk, val FROM t_shared WHERE pk=20;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "20,old-branch-20" ]] || false

    # HEAD creates another branch, makes a DDL change, commits
    new_dolt checkout -b head_schema_change
    new_dolt sql -q "ALTER TABLE t_shared ADD COLUMN extra INT;"
    new_dolt sql -q "UPDATE t_shared SET extra=pk*10;"
    new_dolt add .
    new_dolt commit -m "head: add extra column on schema_change branch"
    new_dolt checkout "$MAIN"
    new_dolt merge head_schema_change

    clear_branch_control

    # Round 4: old reads HEAD's DDL merge — new column visible
    run old_dolt sql -q "SELECT pk, val, extra FROM t_shared WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "1,base-1,10" ]] || false

    run old_dolt sql -q "SELECT count(*) FROM t_shared;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "6" ]] || false

    # Old inserts using the new column added by HEAD
    old_dolt sql -q "INSERT INTO t_shared VALUES (30, 'old-final-30', 'old', 300);"
    old_dolt add .
    old_dolt commit -m "old: insert using HEAD's new column"

    clear_branch_control

    # Round 5: HEAD reads old's final insert
    run new_dolt sql -q "SELECT pk, val, extra FROM t_shared WHERE pk=30;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "30,old-final-30,300" ]] || false

    run new_dolt sql -q "SELECT count(*) FROM t_shared;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "7" ]] || false
}

# ---------------------------------------------------------------------------
# Test 6: Comprehensive schema evolution — all major type categories.
# Both versions add columns of different type families across rounds,
# exercising the full encoding path for each type.
# ---------------------------------------------------------------------------

@test "bidirectional_compat: comprehensive type coverage across versions" {
    [ -n "$DOLT_LEGACY_BIN" ] || skip "requires DOLT_LEGACY_BIN"
    [ -n "$DOLT_NEW_BIN" ] || skip "requires DOLT_NEW_BIN"

    # Setup: old dolt creates a minimal table
    old_dolt init
    old_dolt sql <<SQL
CREATE TABLE typed (
  pk INT NOT NULL PRIMARY KEY
);
INSERT INTO typed (pk) VALUES (1), (2), (3);
SQL
    old_dolt add .
    old_dolt commit -m "old: pk-only base"

    clear_branch_control

    # Round 1: HEAD adds integer and floating-point columns
    new_dolt sql -q "ALTER TABLE typed ADD COLUMN c_tinyint TINYINT;"
    new_dolt sql -q "ALTER TABLE typed ADD COLUMN c_bigint BIGINT;"
    new_dolt sql -q "ALTER TABLE typed ADD COLUMN c_float FLOAT;"
    new_dolt sql -q "ALTER TABLE typed ADD COLUMN c_double DOUBLE;"
    new_dolt sql -q "UPDATE typed SET c_tinyint=pk*10, c_bigint=pk*1000000, c_float=pk*1.5, c_double=pk*2.5;"
    new_dolt sql -q "INSERT INTO typed (pk, c_tinyint, c_bigint, c_float, c_double) VALUES (4, 40, 4000000, 6.0, 10.0);"
    new_dolt add .
    new_dolt commit -m "head: add numeric columns"

    clear_branch_control

    # Round 2: old reads HEAD's numeric columns
    run old_dolt sql -q "SELECT pk, c_tinyint, c_bigint, c_float, c_double FROM typed WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "1,10,1000000" ]] || false

    # Old adds string and binary columns
    old_dolt sql -q "ALTER TABLE typed ADD COLUMN c_varchar VARCHAR(255);"
    old_dolt sql -q "ALTER TABLE typed ADD COLUMN c_char CHAR(10);"
    old_dolt sql -q "ALTER TABLE typed ADD COLUMN c_varbinary VARBINARY(255);"
    old_dolt sql -q "UPDATE typed SET c_varchar=CONCAT('varchar-',pk), c_char=CONCAT('ch-',pk), c_varbinary=CONCAT('bin-',pk);"
    old_dolt sql -q "INSERT INTO typed (pk, c_varchar, c_char, c_varbinary) VALUES (5, 'varchar-5', 'ch-5', 'bin-5');"
    old_dolt add .
    old_dolt commit -m "old: add string and binary columns"

    clear_branch_control

    # Round 3: HEAD reads old's string columns
    run new_dolt sql -q "SELECT pk, c_varchar, c_char, c_varbinary FROM typed WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "1,varchar-1,ch-1,bin-1" ]] || false

    run new_dolt sql -q "SELECT pk, c_varchar FROM typed WHERE pk=5;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "5,varchar-5" ]] || false

    # HEAD adds temporal and decimal columns
    new_dolt sql -q "ALTER TABLE typed ADD COLUMN c_date DATE;"
    new_dolt sql -q "ALTER TABLE typed ADD COLUMN c_datetime DATETIME;"
    new_dolt sql -q "ALTER TABLE typed ADD COLUMN c_decimal DECIMAL(10,3);"
    new_dolt sql -q "UPDATE typed SET c_date='2025-01-01', c_datetime='2025-01-01 12:00:00', c_decimal=pk*3.141;"
    new_dolt add .
    new_dolt commit -m "head: add temporal and decimal columns"

    clear_branch_control

    # Round 4: old reads HEAD's temporal/decimal columns
    run old_dolt sql -q "SELECT pk, c_date, c_decimal FROM typed WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "1,2025-01-01,3.141" ]] || false

    # Old adds enum and set columns
    old_dolt sql -q "ALTER TABLE typed ADD COLUMN c_enum ENUM('a','b','c');"
    old_dolt sql -q "ALTER TABLE typed ADD COLUMN c_set SET('x','y','z');"
    old_dolt sql -q "UPDATE typed SET c_enum='a', c_set='x';"
    old_dolt sql -q "UPDATE typed SET c_enum='b', c_set='x,y' WHERE pk=2;"
    old_dolt sql -q "INSERT INTO typed (pk, c_enum, c_set, c_varchar, c_date, c_decimal)
      VALUES (6, 'c', 'x,y,z', 'varchar-6', '2025-06-01', 18.847);"
    old_dolt add .
    old_dolt commit -m "old: add enum and set columns"

    clear_branch_control

    # Round 5: HEAD reads old's enum/set columns, does a final insert using everything
    run new_dolt sql -q "SELECT pk, c_enum, c_set FROM typed WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "1,a,x" ]] || false

    run new_dolt sql -q "SELECT pk, c_enum, c_set FROM typed WHERE pk=2;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ '2,b,"x,y"' ]] || false

    run new_dolt sql -q "SELECT pk, c_enum, c_set, c_varchar, c_decimal FROM typed WHERE pk=6;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ '6,c,"x,y,z",varchar-6,18.847' ]] || false

    new_dolt sql -q "INSERT INTO typed (pk, c_tinyint, c_bigint, c_varchar, c_date, c_decimal, c_enum, c_set)
      VALUES (7, 70, 7000000, 'varchar-7', '2025-07-01', 21.988, 'c', 'y,z');"
    new_dolt add .
    new_dolt commit -m "head: final insert using all columns"

    clear_branch_control

    # Round 6: old reads HEAD's final insert — all columns from both versions
    run old_dolt sql -q "SELECT pk, c_tinyint, c_bigint, c_varchar, c_decimal, c_enum, c_set FROM typed WHERE pk=7;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ '7,70,7000000,varchar-7,21.988,c,"y,z"' ]] || false

    run old_dolt sql -q "SELECT count(*) FROM typed;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "7" ]] || false
}

# ---------------------------------------------------------------------------
# Test 7: TEXT types — all TEXT variants (TINYTEXT/TEXT/MEDIUMTEXT/LONGTEXT)
# exercise value correctness across version boundaries, including values that
# trigger adaptive encoding:
#   - TINYTEXT/TEXT are always inline (max 65535 bytes < 64KB threshold)
#   - MEDIUMTEXT/LONGTEXT values >= 64KB are stored out of band
# Each round includes rows with mixed inline and out-of-band columns.
# ---------------------------------------------------------------------------

@test "bidirectional_compat: text types round-trip across versions" {
    [ -n "$DOLT_LEGACY_BIN" ] || skip "requires DOLT_LEGACY_BIN"
    [ -n "$DOLT_NEW_BIN" ] || skip "requires DOLT_NEW_BIN"

    # Setup: old dolt creates table with all TEXT variants.
    # Row 1: all inline (small values).
    # Row 2: c_text inline (60000 < 65536), c_medtext out-of-band (70000 > 65536),
    #         c_longtext out-of-band (90000). Mixed inline/out-of-band in same row.
    old_dolt init
    old_dolt sql <<SQL
CREATE TABLE texts (
  pk         INT NOT NULL PRIMARY KEY,
  c_tinytext TINYTEXT,
  c_text     TEXT,
  c_medtext  MEDIUMTEXT,
  c_longtext LONGTEXT
);
INSERT INTO texts VALUES
  (1, 'tiny-old-1', 'text-old-1', 'med-old-1', 'long-old-1'),
  (2, 'tiny-old-2', REPEAT('t', 60000), REPEAT('m', 70000), REPEAT('l', 90000));
SQL
    old_dolt add .
    old_dolt commit -m "old: row 1 all-inline, row 2 mixed inline/out-of-band"

    clear_branch_control

    # Round 1: HEAD reads old's rows (inline and out-of-band), then inserts more.
    run new_dolt sql -q "SELECT pk, c_tinytext, c_text FROM texts WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "1,tiny-old-1,text-old-1" ]] || false

    # Verify row 2: c_text inline (60000), c_medtext/c_longtext out-of-band
    run new_dolt sql -q "SELECT pk, LENGTH(c_text), LENGTH(c_medtext), LENGTH(c_longtext) FROM texts WHERE pk=2;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "2,60000,70000,90000" ]] || false

    # Row 3: c_text inline (50000), c_medtext out-of-band (68000), c_longtext out-of-band (75000)
    # Row 4: all inline (small values)
    new_dolt sql -q "INSERT INTO texts VALUES
      (3, REPEAT('a', 200), REPEAT('e', 50000), REPEAT('m', 68000), REPEAT('l', 75000)),
      (4, 'tiny-head-4', 'text-head-4', 'med-head-4', 'long-head-4');"
    # Update row 1: promote c_medtext from inline to out-of-band
    new_dolt sql -q "UPDATE texts SET c_medtext=REPEAT('U', 70000) WHERE pk=1;"
    new_dolt add .
    new_dolt commit -m "head: rows 3/4, row 1 c_medtext inline->out-of-band"

    clear_branch_control

    # Round 2: old reads HEAD's rows (including newly out-of-band columns), then inserts more.
    # Verify row 3 mixed encoding
    run old_dolt sql -q "SELECT pk, LENGTH(c_tinytext), LENGTH(c_text), LENGTH(c_medtext), LENGTH(c_longtext) FROM texts WHERE pk=3;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "3,200,50000,68000,75000" ]] || false

    # Verify row 1 c_medtext is now out-of-band
    run old_dolt sql -q "SELECT pk, LENGTH(c_medtext) FROM texts WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "1,70000" ]] || false

    run old_dolt sql -q "SELECT pk, c_tinytext, c_text FROM texts WHERE pk=4;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "4,tiny-head-4,text-head-4" ]] || false

    # Row 5: c_text inline (55000), c_medtext out-of-band (72000), c_longtext inline (small)
    old_dolt sql -q "INSERT INTO texts VALUES
      (5, REPEAT('o', 150), REPEAT('p', 55000), REPEAT('q', 72000), 'long-old-5');"
    # Update row 4: promote c_longtext from small to out-of-band
    old_dolt sql -q "UPDATE texts SET c_longtext=REPEAT('X', 80000) WHERE pk=4;"
    old_dolt add .
    old_dolt commit -m "old: row 5 mixed, row 4 c_longtext inline->out-of-band"

    clear_branch_control

    # Round 3: HEAD reads old's new rows including mixed encodings.
    run new_dolt sql -q "SELECT pk, LENGTH(c_tinytext), LENGTH(c_text), LENGTH(c_medtext), LENGTH(c_longtext) FROM texts WHERE pk=5;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "5,150,55000,72000,9" ]] || false  # 'long-old-5' = 9 chars

    run new_dolt sql -q "SELECT pk, LENGTH(c_longtext) FROM texts WHERE pk=4;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "4,80000" ]] || false

    # Update row 3: demote c_medtext and c_longtext from out-of-band back to inline
    new_dolt sql -q "UPDATE texts SET c_medtext='med-head-upd', c_longtext='long-head-upd' WHERE pk=3;"
    new_dolt add .
    new_dolt commit -m "head: row 3 c_medtext/c_longtext out-of-band->inline"

    clear_branch_control

    # Round 4: old reads HEAD's demotion and verifies full table state.
    run old_dolt sql -q "SELECT pk, c_tinytext, c_text, c_medtext, c_longtext FROM texts WHERE pk=3;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "3" ]] || false
    [[ "$output" =~ "med-head-upd" ]] || false
    [[ "$output" =~ "long-head-upd" ]] || false

    run old_dolt sql -q "SELECT count(*) FROM texts;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "5" ]] || false
}

# ---------------------------------------------------------------------------
# Test 8: BLOB types — all BLOB variants (TINYBLOB/BLOB/MEDIUMBLOB/LONGBLOB)
# plus VARBINARY exercise value correctness across version boundaries, including
# values that trigger adaptive encoding:
#   - VARBINARY(255)/TINYBLOB are always inline (max 255 bytes)
#   - BLOB is always inline (max 65535 bytes < 64KB threshold)
#   - MEDIUMBLOB/LONGBLOB values >= 64KB are stored out of band
# Each round includes rows with mixed inline and out-of-band columns.
# ---------------------------------------------------------------------------

@test "bidirectional_compat: blob types round-trip across versions" {
    [ -n "$DOLT_LEGACY_BIN" ] || skip "requires DOLT_LEGACY_BIN"
    [ -n "$DOLT_NEW_BIN" ] || skip "requires DOLT_NEW_BIN"

    # Setup: old dolt creates table with all BLOB variants and VARBINARY.
    # Row 1: all inline (small values).
    # Row 2: c_blob inline (60000 < 65536), c_medblob out-of-band (70000 > 65536),
    #         c_longblob out-of-band (90000). Mixed inline/out-of-band in same row.
    old_dolt init
    old_dolt sql <<SQL
CREATE TABLE blobdata (
  pk          INT NOT NULL PRIMARY KEY,
  c_varbinary VARBINARY(255),
  c_tinyblob  TINYBLOB,
  c_blob      BLOB,
  c_medblob   MEDIUMBLOB,
  c_longblob  LONGBLOB
);
INSERT INTO blobdata VALUES
  (1, 'varbin-old-1', 'tiny-old-1', 'blob-old-1', 'med-old-1', 'long-old-1'),
  (2, 'varbin-old-2', 'tiny-old-2', REPEAT('b', 60000), REPEAT('m', 70000), REPEAT('l', 90000));
SQL
    old_dolt add .
    old_dolt commit -m "old: row 1 all-inline, row 2 mixed inline/out-of-band"

    clear_branch_control

    # Round 1: HEAD reads old's rows (inline and out-of-band), then inserts more.
    run new_dolt sql -q "SELECT pk, c_varbinary, c_tinyblob FROM blobdata WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "1,varbin-old-1,tiny-old-1" ]] || false

    # Verify row 2: c_blob inline (60000), c_medblob/c_longblob out-of-band
    run new_dolt sql -q "SELECT pk, LENGTH(c_blob), LENGTH(c_medblob), LENGTH(c_longblob) FROM blobdata WHERE pk=2;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "2,60000,70000,90000" ]] || false

    # Row 3: c_blob inline (50000), c_medblob out-of-band (68000), c_longblob out-of-band (75000)
    # Row 4: all inline (small values)
    new_dolt sql -q "INSERT INTO blobdata VALUES
      (3, REPEAT('v', 200), REPEAT('t', 200), REPEAT('b', 50000), REPEAT('m', 68000), REPEAT('l', 75000)),
      (4, 'varbin-head-4', 'tiny-head-4', 'blob-head-4', 'med-head-4', 'long-head-4');"
    # Update row 1: promote c_medblob from inline to out-of-band
    new_dolt sql -q "UPDATE blobdata SET c_medblob=REPEAT('U', 70000) WHERE pk=1;"
    new_dolt add .
    new_dolt commit -m "head: rows 3/4, row 1 c_medblob inline->out-of-band"

    clear_branch_control

    # Round 2: old reads HEAD's rows (including newly out-of-band columns), then inserts more.
    # Verify row 3 mixed encoding
    run old_dolt sql -q "SELECT pk, LENGTH(c_varbinary), LENGTH(c_tinyblob), LENGTH(c_blob), LENGTH(c_medblob), LENGTH(c_longblob) FROM blobdata WHERE pk=3;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "3,200,200,50000,68000,75000" ]] || false

    # Verify row 1 c_medblob is now out-of-band
    run old_dolt sql -q "SELECT pk, LENGTH(c_medblob) FROM blobdata WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "1,70000" ]] || false

    run old_dolt sql -q "SELECT pk, c_varbinary, c_tinyblob FROM blobdata WHERE pk=4;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "4,varbin-head-4,tiny-head-4" ]] || false

    # Row 5: c_blob inline (55000), c_medblob out-of-band (72000), c_longblob inline (small)
    old_dolt sql -q "INSERT INTO blobdata VALUES
      (5, REPEAT('o', 150), REPEAT('p', 150), REPEAT('q', 55000), REPEAT('r', 72000), 'long-old-5');"
    # Update row 4: promote c_longblob from small to out-of-band
    old_dolt sql -q "UPDATE blobdata SET c_longblob=REPEAT('X', 80000) WHERE pk=4;"
    old_dolt add .
    old_dolt commit -m "old: row 5 mixed, row 4 c_longblob inline->out-of-band"

    clear_branch_control

    # Round 3: HEAD reads old's new rows including mixed encodings.
    run new_dolt sql -q "SELECT pk, LENGTH(c_varbinary), LENGTH(c_tinyblob), LENGTH(c_blob), LENGTH(c_medblob), LENGTH(c_longblob) FROM blobdata WHERE pk=5;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "5,150,150,55000,72000,10" ]] || false  # 'long-old-5' = 10 chars

    run new_dolt sql -q "SELECT pk, LENGTH(c_longblob) FROM blobdata WHERE pk=4;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "4,80000" ]] || false

    # Update row 3: demote c_medblob and c_longblob from out-of-band back to inline
    new_dolt sql -q "UPDATE blobdata SET c_medblob='mb-upd', c_longblob='lb-upd' WHERE pk=3;"
    new_dolt add .
    new_dolt commit -m "head: row 3 c_medblob/c_longblob out-of-band->inline"

    clear_branch_control

    # Round 4: old reads HEAD's demotion and verifies full table state.
    run old_dolt sql -q "SELECT pk, c_medblob, c_longblob FROM blobdata WHERE pk=3;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "mb-upd" ]] || false
    [[ "$output" =~ "lb-upd" ]] || false

    run old_dolt sql -q "SELECT count(*) FROM blobdata;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "5" ]] || false
}

# ---------------------------------------------------------------------------
# Test 9: JSON type — two JSON columns exercise both inline (small docs) and
# out-of-band (large docs > 64KB) storage across version boundaries.
# The table has c_json (always small, always inline) and c_json2 (varies: small
# inline rows and large out-of-band rows). Each round includes rows where c_json
# is inline while c_json2 is out-of-band, ensuring mixed encoding within a row.
# ---------------------------------------------------------------------------

@test "bidirectional_compat: json round-trip across versions" {
    [ -n "$DOLT_LEGACY_BIN" ] || skip "requires DOLT_LEGACY_BIN"
    [ -n "$DOLT_NEW_BIN" ] || skip "requires DOLT_NEW_BIN"

    # Setup: old dolt creates table with two JSON columns.
    # Row 1: both c_json and c_json2 inline (small docs).
    # Row 2: c_json inline (small), c_json2 out-of-band (large string > 64KB).
    # Row 3: both c_json and c_json2 out-of-band (both > 64KB).
    old_dolt init
    old_dolt sql <<SQL
CREATE TABLE jsondocs (
  pk     INT NOT NULL PRIMARY KEY,
  c_json  JSON,
  c_json2 JSON
);
INSERT INTO jsondocs VALUES
  (1, '{"key":"val1","num":100}', '{"meta":"small"}'),
  (2, '{"key":"val2","arr":[1,2,3]}', CONCAT('{"big":"', REPEAT('x', 70000), '"}')),
  (3, CONCAT('{"src":"old","pad":"', REPEAT('a', 70000), '"}'), CONCAT('{"src2":"old","pad2":"', REPEAT('b', 70000), '"}'));
SQL
    old_dolt add .
    old_dolt commit -m "old: row 1 both-inline, row 2 mixed, row 3 both-out-of-band"

    clear_branch_control

    # Round 1: HEAD reads old's rows and verifies inline vs out-of-band content.
    # Row 1: both inline
    run new_dolt sql -q "SELECT pk, JSON_EXTRACT(c_json, '$.key'), JSON_EXTRACT(c_json2, '$.meta') FROM jsondocs WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "val1" ]] || false
    [[ "$output" =~ "small" ]] || false

    # Row 2: c_json inline, c_json2 out-of-band — verify large value length
    run new_dolt sql -q "SELECT pk, JSON_EXTRACT(c_json, '$.key'), LENGTH(JSON_UNQUOTE(JSON_EXTRACT(c_json2, '$.big'))) FROM jsondocs WHERE pk=2;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "val2" ]] || false
    [[ "$output" =~ "70000" ]] || false

    # Row 3: both out-of-band — verify both large value lengths
    run new_dolt sql -q "SELECT pk, LENGTH(JSON_UNQUOTE(JSON_EXTRACT(c_json, '$.pad'))), LENGTH(JSON_UNQUOTE(JSON_EXTRACT(c_json2, '$.pad2'))) FROM jsondocs WHERE pk=3;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "3,70000,70000" ]] || false

    # Insert row 4 (both inline), row 5 (c_json inline, c_json2 out-of-band)
    new_dolt sql -q "INSERT INTO jsondocs VALUES
      (4, '{\"head\":true,\"count\":42}', '{\"meta2\":\"inline\"}'),
      (5, '{\"head\":true,\"seq\":5}', CONCAT('{\"bighead\":\"', REPEAT('H', 70000), '\"}'));"
    # Update row 1: promote c_json2 from inline to out-of-band
    new_dolt sql -q "UPDATE jsondocs SET c_json2=CONCAT('{\"promoted\":\"', REPEAT('P', 70000), '\"}') WHERE pk=1;"
    new_dolt add .
    new_dolt commit -m "head: rows 4/5, row 1 c_json2 inline->out-of-band"

    clear_branch_control

    # Round 2: old reads HEAD's changes.
    # Verify row 4 both inline
    run old_dolt sql -q "SELECT pk, JSON_EXTRACT(c_json, '$.head'), JSON_EXTRACT(c_json2, '$.meta2') FROM jsondocs WHERE pk=4;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "true" ]] || false
    [[ "$output" =~ "inline" ]] || false

    # Verify row 5: c_json inline, c_json2 out-of-band
    run old_dolt sql -q "SELECT pk, JSON_EXTRACT(c_json, '$.seq'), LENGTH(JSON_UNQUOTE(JSON_EXTRACT(c_json2, '$.bighead'))) FROM jsondocs WHERE pk=5;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "5" ]] || false
    [[ "$output" =~ "70000" ]] || false

    # Verify row 1 c_json2 is now out-of-band
    run old_dolt sql -q "SELECT pk, LENGTH(JSON_UNQUOTE(JSON_EXTRACT(c_json2, '$.promoted'))) FROM jsondocs WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "70000" ]] || false

    # Old inserts row 6 (c_json inline, c_json2 out-of-band) and row 7 (both inline)
    old_dolt sql -q "INSERT INTO jsondocs VALUES
      (6, '{\"old\":true,\"tags\":[\"x\",\"y\"]}', CONCAT('{\"oldpad\":\"', REPEAT('O', 70000), '\"}')),
      (7, '{\"old\":true,\"small\":1}', '{\"also\":\"small\"}');"
    # Update row 4: promote c_json2 from inline to out-of-band
    old_dolt sql -q "UPDATE jsondocs SET c_json2=CONCAT('{\"oldpromote\":\"', REPEAT('Q', 70000), '\"}') WHERE pk=4;"
    old_dolt add .
    old_dolt commit -m "old: rows 6/7, row 4 c_json2 inline->out-of-band"

    clear_branch_control

    # Round 3: HEAD reads old's changes.
    # Verify row 6: c_json inline, c_json2 out-of-band
    run new_dolt sql -q "SELECT pk, JSON_EXTRACT(c_json, '$.old'), LENGTH(JSON_UNQUOTE(JSON_EXTRACT(c_json2, '$.oldpad'))) FROM jsondocs WHERE pk=6;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "true" ]] || false
    [[ "$output" =~ "70000" ]] || false

    # Verify row 7 both inline
    run new_dolt sql -q "SELECT pk, JSON_EXTRACT(c_json, '$.small'), JSON_EXTRACT(c_json2, '$.also') FROM jsondocs WHERE pk=7;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1" ]] || false
    [[ "$output" =~ "small" ]] || false

    # Verify row 4 c_json2 is now out-of-band
    run new_dolt sql -q "SELECT pk, LENGTH(JSON_UNQUOTE(JSON_EXTRACT(c_json2, '$.oldpromote'))) FROM jsondocs WHERE pk=4;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "70000" ]] || false

    # Demote row 3 c_json2 from out-of-band to inline, and update row 2 c_json
    new_dolt sql -q "UPDATE jsondocs SET c_json2='{\"demoted\":true}' WHERE pk=3;"
    new_dolt sql -q "UPDATE jsondocs SET c_json=JSON_SET(c_json, '$.updated', true) WHERE pk=2;"
    new_dolt add .
    new_dolt commit -m "head: row 3 c_json2 out-of-band->inline, row 2 c_json updated"

    clear_branch_control

    # Round 4: old reads HEAD's final state and verifies full table.
    # Row 3 c_json2 now inline
    run old_dolt sql -q "SELECT pk, JSON_EXTRACT(c_json2, '$.demoted') FROM jsondocs WHERE pk=3;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "true" ]] || false

    # Row 2 c_json updated
    run old_dolt sql -q "SELECT pk, JSON_EXTRACT(c_json, '$.updated') FROM jsondocs WHERE pk=2;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "true" ]] || false

    run old_dolt sql -q "SELECT count(*) FROM jsondocs;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "7" ]] || false
}
