#!/usr/bin/env bats
# Bi-directional compatibility tests: HEAD dolt and an older dolt version
# interleave reads and writes against the same repository across multiple rounds,
# verifying that each version can always read what the other has written.
#
# Each test creates an isolated repository, alternating between old_dolt
# (the version under test, via DOLT_LEGACY_BIN) and dolt (HEAD build).
# Tests are skipped when DOLT_LEGACY_BIN is not set.
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}

# We made a forwards-incompatible change to branch control serialization that causes a panic in
# older dolt clients when reading from a db that was written by a modern client. We want to ignore
# this failure for these tests since it prevents us from finding any other issues.
clear_branch_control() {
    rm -f .doltcfg/branch_control.db
}

# ---------------------------------------------------------------------------
# Helper: detect the default branch of a freshly-inited dolt repo.
# ---------------------------------------------------------------------------
get_main_branch() {
    dolt branch | sed 's/^\* //'
}

get_old_main_branch() {
    old_dolt branch | sed 's/^\* //'
}

# ---------------------------------------------------------------------------
# Test 1: Scalar types DML — INT, VARCHAR, DECIMAL, DATETIME round-trip.
# Four rounds: old → HEAD → old → HEAD, verifying state at each step.
# ---------------------------------------------------------------------------

@test "bidirectional dml: scalar types round-trip across versions" {
    [ -n "$DOLT_LEGACY_BIN" ] || skip "requires DOLT_LEGACY_BIN"

    repo="$BATS_TEST_TMPDIR/bidir_scalars_$$"
    mkdir -p "$repo" && cd "$repo"

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

    # Round 1: HEAD reads old's rows, inserts its own
    run dolt sql -q "SELECT pk, c_varchar, c_decimal FROM scalars WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "1,old-row-1,10.50" ]] || false

    run dolt sql -q "SELECT count(*) FROM scalars;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "2" ]] || false

    dolt sql -q "INSERT INTO scalars VALUES (3, 300, 'head-row-3', 30.25, '2025-01-15 08:00:00');"
    dolt sql -q "UPDATE scalars SET c_varchar='head-updated-1' WHERE pk=1;"
    dolt add .
    dolt commit -m "head: insert row 3, update row 1"

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

    # Round 3: HEAD reads old's changes
    run dolt sql -q "SELECT pk, c_varchar, c_decimal FROM scalars WHERE pk=4;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "4,old-row-4,40.00" ]] || false

    run dolt sql -q "SELECT pk FROM scalars WHERE pk=2;" -r csv
    [ "$status" -eq 0 ]
    [[ "${#lines[@]}" -eq 1 ]] || false  # only header, row 2 deleted

    dolt sql -q "INSERT INTO scalars VALUES (5, 500, 'head-row-5', 50.50, '2025-06-01 14:00:00');"
    dolt sql -q "UPDATE scalars SET c_decimal=99.99 WHERE pk=4;"
    dolt add .
    dolt commit -m "head: insert row 5, update row 4 decimal"

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

@test "bidirectional dml: large text and blob values round-trip" {
    [ -n "$DOLT_LEGACY_BIN" ] || skip "requires DOLT_LEGACY_BIN"

    repo="$BATS_TEST_TMPDIR/bidir_blobs_$$"
    mkdir -p "$repo" && cd "$repo"

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

    # Round 1: HEAD reads small value, inserts large values
    run dolt sql -q "SELECT pk, c_text FROM blobs WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "1,old-small-text" ]] || false

    dolt sql -q "INSERT INTO blobs (pk, c_text, c_mtext, c_ltext, c_blob, c_lblob)
      VALUES (2,
        REPEAT('H', 1000), REPEAT('M', 2000), REPEAT('L', 3000),
        REPEAT('B', 1000), REPEAT('Z', 3000));"
    dolt add .
    dolt commit -m "head: insert large values"

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

    # Round 3: HEAD reads old's large values
    run dolt sql -q "SELECT pk, LENGTH(c_text), LENGTH(c_mtext), LENGTH(c_ltext) FROM blobs WHERE pk=3;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "3,1500,2500,4000" ]] || false

    run dolt sql -q "SELECT pk, LENGTH(c_text), LENGTH(c_blob) FROM blobs WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "1,800,800" ]] || false

    # HEAD updates row 2's large values in-place
    dolt sql -q "UPDATE blobs SET c_ltext=REPEAT('N', 5000), c_lblob=REPEAT('W', 5000) WHERE pk=2;"
    dolt add .
    dolt commit -m "head: update row 2 to even larger values"

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

@test "bidirectional dml: geometry types round-trip across versions" {
    [ -n "$DOLT_LEGACY_BIN" ] || skip "requires DOLT_LEGACY_BIN"
    [[ "$DOLT_VERSION" =~ 0\.50 ]] && skip "geometry not tested against Dolt version 0.50"

    repo="$BATS_TEST_TMPDIR/bidir_geom_$$"
    mkdir -p "$repo" && cd "$repo"

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

    # Round 1: HEAD reads old's geometry, inserts more
    run dolt sql -q "SELECT pk, ST_X(c_point), ST_Y(c_point) FROM geoms WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "1,1,2" ]] || false

    run dolt sql -q "SELECT pk, ST_AsText(c_line) FROM geoms WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "LINESTRING" ]] || false

    dolt sql -q "INSERT INTO geoms VALUES (
      2,
      ST_GeomFromText('POINT(10 20)'),
      ST_GeomFromText('LINESTRING(0 0,5 5)'),
      ST_GeomFromText('POLYGON((0 0,3 0,3 3,0 3,0 0))'),
      ST_GeomFromText('LINESTRING(1 1,9 9)')
    );"
    dolt add .
    dolt commit -m "head: insert geometry row 2"

    # Round 2: old reads HEAD's geometry, updates row 1, inserts row 3
    run old_dolt sql -q "SELECT pk, ST_X(c_point), ST_Y(c_point) FROM geoms WHERE pk=2;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "2,10,20" ]] || false

    old_dolt sql -q "UPDATE geoms SET c_point=ST_GeomFromText('POINT(99 88)') WHERE pk=1;"
    old_dolt sql -q "INSERT INTO geoms (pk, c_point, c_geometry)
      VALUES (3, ST_GeomFromText('POINT(30 40)'), ST_GeomFromText('POINT(50 60)'));"
    old_dolt add .
    old_dolt commit -m "old: update row 1 point, insert row 3"

    # Round 3: HEAD reads old's changes
    run dolt sql -q "SELECT pk, ST_X(c_point), ST_Y(c_point) FROM geoms WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "1,99,88" ]] || false

    run dolt sql -q "SELECT pk, ST_X(c_point), ST_Y(c_point) FROM geoms WHERE pk=3;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "3,30,40" ]] || false

    dolt sql -q "UPDATE geoms SET c_geometry=ST_GeomFromText('POLYGON((0 0,10 0,10 10,0 10,0 0))') WHERE pk=2;"
    dolt add .
    dolt commit -m "head: update row 2 geometry to polygon"

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

@test "bidirectional ddl: add columns from both versions" {
    [ -n "$DOLT_LEGACY_BIN" ] || skip "requires DOLT_LEGACY_BIN"

    repo="$BATS_TEST_TMPDIR/bidir_ddl_$$"
    mkdir -p "$repo" && cd "$repo"

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

    # Round 1: HEAD adds TEXT and DATE columns, populates them
    dolt sql -q "ALTER TABLE evolving ADD COLUMN c_text TEXT;"
    dolt sql -q "ALTER TABLE evolving ADD COLUMN c_date DATE;"
    dolt sql -q "UPDATE evolving SET c_text=CONCAT('text-', pk), c_date='2025-01-01';"
    dolt sql -q "INSERT INTO evolving VALUES (4, 40, 'text-4', '2025-02-01');"
    dolt add .
    dolt commit -m "head: add text and date columns"

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

    # Round 3: HEAD reads old's new columns — all 4 added columns now visible
    run dolt sql -q "SELECT pk, c_text, c_date, c_int2, c_decimal FROM evolving WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "1,text-1,2025-01-01,100,1.50" ]] || false

    run dolt sql -q "SELECT pk, c_text, c_int2, c_decimal FROM evolving WHERE pk=5;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "5,text-5,500,7.50" ]] || false

    # HEAD adds a geometry column
    dolt sql -q "ALTER TABLE evolving ADD COLUMN c_point POINT;"
    dolt sql -q "UPDATE evolving SET c_point=ST_GeomFromText(CONCAT('POINT(', pk, ' ', pk, ')'));"
    dolt sql -q "INSERT INTO evolving VALUES (6, 60, 'text-6', '2025-04-01', 600, 9.00, ST_GeomFromText('POINT(6 6)'));"
    dolt add .
    dolt commit -m "head: add point column"

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

    # Round 5: HEAD reads old's insert with geometry
    run dolt sql -q "SELECT pk, c_text, c_decimal, ST_X(c_point) FROM evolving WHERE pk=7;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "7,text-7,10.50,7" ]] || false

    run dolt sql -q "SELECT count(*) FROM evolving;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "7" ]] || false
}

# ---------------------------------------------------------------------------
# Test 5: Branch and merge — both versions create branches, commit to them,
# and merge across version boundaries.
# ---------------------------------------------------------------------------

@test "bidirectional vcs: branch and merge across versions" {
    [ -n "$DOLT_LEGACY_BIN" ] || skip "requires DOLT_LEGACY_BIN"

    repo="$BATS_TEST_TMPDIR/bidir_merge_$$"
    mkdir -p "$repo" && cd "$repo"

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

    # Round 1: HEAD creates a feature branch and commits changes to it
    dolt checkout -b head_feature
    dolt sql -q "INSERT INTO t_shared VALUES (10, 'head-feature-10', 'head');"
    dolt sql -q "INSERT INTO t_shared VALUES (11, 'head-feature-11', 'head');"
    dolt add .
    dolt commit -m "head: feature branch inserts"
    dolt checkout "$MAIN"

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

    # Round 3: HEAD reads merged result, merges old_branch too
    run dolt sql -q "SELECT count(*) FROM t_shared;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "5" ]] || false

    run dolt sql -q "SELECT pk, val FROM t_shared WHERE pk=3;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "3,base-3" ]] || false

    # HEAD merges old's branch (row 20)
    dolt merge old_branch
    run dolt sql -q "SELECT count(*) FROM t_shared;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "6" ]] || false  # 1,2,3,10,11,20

    run dolt sql -q "SELECT pk, val FROM t_shared WHERE pk=20;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "20,old-branch-20" ]] || false

    # HEAD creates another branch, makes a DDL change, commits
    dolt checkout -b head_schema_change
    dolt sql -q "ALTER TABLE t_shared ADD COLUMN extra INT;"
    dolt sql -q "UPDATE t_shared SET extra=pk*10;"
    dolt add .
    dolt commit -m "head: add extra column on schema_change branch"
    dolt checkout "$MAIN"
    dolt merge head_schema_change

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

    # Round 5: HEAD reads old's final insert
    run dolt sql -q "SELECT pk, val, extra FROM t_shared WHERE pk=30;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "30,old-final-30,300" ]] || false

    run dolt sql -q "SELECT count(*) FROM t_shared;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "7" ]] || false
}

# ---------------------------------------------------------------------------
# Test 6: Comprehensive schema evolution — all major type categories.
# Both versions add columns of different type families across rounds,
# exercising the full encoding path for each type.
# ---------------------------------------------------------------------------

@test "bidirectional ddl: comprehensive type coverage across versions" {
    [ -n "$DOLT_LEGACY_BIN" ] || skip "requires DOLT_LEGACY_BIN"

    repo="$BATS_TEST_TMPDIR/bidir_types_$$"
    mkdir -p "$repo" && cd "$repo"

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

    # Round 1: HEAD adds integer and floating-point columns
    dolt sql -q "ALTER TABLE typed ADD COLUMN c_tinyint TINYINT;"
    dolt sql -q "ALTER TABLE typed ADD COLUMN c_bigint BIGINT;"
    dolt sql -q "ALTER TABLE typed ADD COLUMN c_float FLOAT;"
    dolt sql -q "ALTER TABLE typed ADD COLUMN c_double DOUBLE;"
    dolt sql -q "UPDATE typed SET c_tinyint=pk*10, c_bigint=pk*1000000, c_float=pk*1.5, c_double=pk*2.5;"
    dolt sql -q "INSERT INTO typed (pk, c_tinyint, c_bigint, c_float, c_double) VALUES (4, 40, 4000000, 6.0, 10.0);"
    dolt add .
    dolt commit -m "head: add numeric columns"

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

    # Round 3: HEAD reads old's string columns
    run dolt sql -q "SELECT pk, c_varchar, c_char, c_varbinary FROM typed WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "1,varchar-1,ch-1,bin-1" ]] || false

    run dolt sql -q "SELECT pk, c_varchar FROM typed WHERE pk=5;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "5,varchar-5" ]] || false

    # HEAD adds temporal and decimal columns
    dolt sql -q "ALTER TABLE typed ADD COLUMN c_date DATE;"
    dolt sql -q "ALTER TABLE typed ADD COLUMN c_datetime DATETIME;"
    dolt sql -q "ALTER TABLE typed ADD COLUMN c_decimal DECIMAL(10,3);"
    dolt sql -q "UPDATE typed SET c_date='2025-01-01', c_datetime='2025-01-01 12:00:00', c_decimal=pk*3.141;"
    dolt add .
    dolt commit -m "head: add temporal and decimal columns"

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

    # Round 5: HEAD reads old's enum/set columns, does a final insert using everything
    run dolt sql -q "SELECT pk, c_enum, c_set FROM typed WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "1,a,x" ]] || false

    run dolt sql -q "SELECT pk, c_enum, c_set FROM typed WHERE pk=2;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ '2,b,"x,y"' ]] || false

    run dolt sql -q "SELECT pk, c_enum, c_set, c_varchar, c_decimal FROM typed WHERE pk=6;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ '6,c,"x,y,z",varchar-6,18.847' ]] || false

    dolt sql -q "INSERT INTO typed (pk, c_tinyint, c_bigint, c_varchar, c_date, c_decimal, c_enum, c_set)
      VALUES (7, 70, 7000000, 'varchar-7', '2025-07-01', 21.988, 'c', 'y,z');"
    dolt add .
    dolt commit -m "head: final insert using all columns"

    # Round 6: old reads HEAD's final insert — all columns from both versions
    run old_dolt sql -q "SELECT pk, c_tinyint, c_bigint, c_varchar, c_decimal, c_enum, c_set FROM typed WHERE pk=7;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ '7,70,7000000,varchar-7,21.988,c,"y,z"' ]] || false

    run old_dolt sql -q "SELECT count(*) FROM typed;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "7" ]] || false
}
