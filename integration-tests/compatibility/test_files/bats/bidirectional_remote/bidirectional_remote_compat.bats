#!/usr/bin/env bats
# Bi-directional compatibility via a shared file remote.
#
# Workflow (per column type T):
#   1) Version A initializes a repo, creates a table, inserts base rows, and
#      pushes to a file remote. Then A adds a column `c_col` of type T
#      (uncommitted for now).
#   2) Version B clones from the same remote, adds `c_col` of type T with the
#      same name, inserts disjoint rows, commits.
#   3) Version A inserts its own disjoint rows using `c_col`, commits, pushes.
#   4) Version B pulls. The pull must succeed — both versions independently
#      added the same column — and the resulting table must contain every row
#      from both versions with the correct `c_col` values.
#
# A runs DOLT_LEGACY_BIN; B runs DOLT_NEW_BIN. The runner invokes this file
# twice per release, swapping the two binaries to cover both directions.
#
# Note: the helpers in ../helper can't be used here because of relative paths,
# so the common pieces (old_dolt/new_dolt, clear_branch_control) are duplicated.

dolt config --global --add metrics.disabled true > /dev/null 2>&1

startdir=""

setup() {
    cp -Rpf $REPO_DIR bats_repo
    startdir="$(pwd)"
    cd bats_repo
}

teardown() {
    cd $startdir
    rm -rf bats_repo
}

old_dolt() {
  if [ -n "$DOLT_LEGACY_BIN" ]; then
    "$DOLT_LEGACY_BIN" "$@"
  else
    dolt "$@"
  fi
}

new_dolt() {
  if [ -n "$DOLT_NEW_BIN" ]; then
    "$DOLT_NEW_BIN" "$@"
  else
    dolt "$@"
  fi
}

# A forwards-incompatible change to branch control serialization can make older
# dolt clients panic when reading a db touched by a newer client. Strip the
# file whenever we hand a repo from one version to the other.
clear_branch_control() {
    rm -f "$1/.doltcfg/branch_control.db"
}

# run_workflow COL_TYPE A_VAL1 A_VAL2 B_VAL1 B_VAL2
#
# Runs the full A-push / B-clone-and-write / A-commit-and-push / B-pull
# sequence against `c_col` of the given type. Leaves the caller in
# $bats_repo/repo_b after a successful pull, ready for verification.
#
# The *_VAL args are SQL expressions (e.g. "'a-text-20'", "42",
# "ST_GeomFromText('POINT(1 2)')").
run_workflow() {
    local col_type="$1"
    local a1="$2" a2="$3" b1="$4" b2="$5"
    local root main

    root="$(pwd)"
    mkdir "$root/remote"

    # Step 1: Version A creates the repo, pushes base data, adds the column.
    mkdir "$root/repo_a"
    cd "$root/repo_a"
    old_dolt init
    main=$(old_dolt branch | sed 's/^\* //' | sed 's/[[:space:]]*$//')
    old_dolt sql <<SQL
CREATE TABLE tbl (
  pk INT NOT NULL PRIMARY KEY,
  c_base INT
);
INSERT INTO tbl VALUES (1, 10), (2, 20);
SQL
    old_dolt add .
    old_dolt commit -m "a: initial schema and data"
    old_dolt remote add origin "file://$root/remote"
    old_dolt push origin "$main"

    old_dolt sql -q "ALTER TABLE tbl ADD COLUMN c_col ${col_type};"
    clear_branch_control "$root/repo_a"

    # Step 2: Version B clones, adds the same column, writes disjoint rows, commits.
    cd "$root"
    new_dolt clone "file://$root/remote" repo_b
    cd "$root/repo_b"
    clear_branch_control "$root/repo_b"

    new_dolt sql -q "ALTER TABLE tbl ADD COLUMN c_col ${col_type};"
    new_dolt sql -q "INSERT INTO tbl (pk, c_base, c_col) VALUES (10, 100, ${b1}), (11, 110, ${b2});"
    new_dolt add .
    new_dolt commit -m "b: add column and data"

    # Step 3: Version A writes disjoint rows using the column, commits, pushes.
    cd "$root/repo_a"
    old_dolt sql -q "INSERT INTO tbl (pk, c_base, c_col) VALUES (20, 200, ${a1}), (21, 210, ${a2});"
    old_dolt add .
    old_dolt commit -m "a: add column and data"
    old_dolt push origin "$main"
    clear_branch_control "$root/repo_a"

    # Step 4: Version B pulls; the merge must succeed.
    cd "$root/repo_b"
    run new_dolt pull origin "$main"
    [ "$status" -eq 0 ] || {
        echo "pull failed (type=${col_type}):" >&2
        echo "$output" >&2
        return 1
    }
}

# verify_col SELECT_EXPR PK EXPECTED_REGEX
verify_col() {
    local select_expr="$1" pk="$2" expected="$3"
    run new_dolt sql -q "SELECT ${select_expr} FROM tbl WHERE pk=${pk};" -r csv
    [ "$status" -eq 0 ] || {
        echo "select failed for pk=${pk}: $output" >&2
        return 1
    }
    [[ "${lines[1]}" =~ ${expected} ]] || {
        echo "pk=${pk} expected '${expected}' got '${lines[1]}'" >&2
        return 1
    }
}

verify_count() {
    run new_dolt sql -q "SELECT count(*) FROM tbl;" -r csv
    [ "$status" -eq 0 ] || return 1
    [[ "${lines[1]}" =~ "6" ]] || {
        echo "expected 6 rows, got '${lines[1]}'" >&2
        return 1
    }
}

# verify_all SELECT_EXPR EXP_PK10 EXP_PK11 EXP_PK20 EXP_PK21
verify_all() {
    verify_count || return 1
    verify_col "$1" 10 "$2" || return 1
    verify_col "$1" 11 "$3" || return 1
    verify_col "$1" 20 "$4" || return 1
    verify_col "$1" 21 "$5" || return 1
}

require_binaries() {
    [ -n "$DOLT_LEGACY_BIN" ] || skip "requires DOLT_LEGACY_BIN"
    [ -n "$DOLT_NEW_BIN" ] || skip "requires DOLT_NEW_BIN"
}

# ---------------------------------------------------------------------------
# Integer types
# ---------------------------------------------------------------------------

@test "remote_add_column: INT" {
    require_binaries
    run_workflow "INT" "2000000" "2000001" "-1000000" "-1000001"
    verify_all "c_col" "-1000000" "-1000001" "2000000" "2000001"
}

@test "remote_add_column: BIGINT" {
    require_binaries
    run_workflow "BIGINT" \
      "9223372036854775000" "9223372036854775001" \
      "-1234567890123" "-1234567890124"
    verify_all "c_col" \
      "-1234567890123" "-1234567890124" \
      "9223372036854775000" "9223372036854775001"
}

@test "remote_add_column: BIGINT UNSIGNED" {
    require_binaries
    run_workflow "BIGINT UNSIGNED" \
      "18446744073709551000" "18446744073709551001" \
      "1234567890123" "1234567890124"
    verify_all "c_col" \
      "1234567890123" "1234567890124" \
      "18446744073709551000" "18446744073709551001"
}

# ---------------------------------------------------------------------------
# Floating-point and decimal types
# ---------------------------------------------------------------------------

@test "remote_add_column: FLOAT" {
    require_binaries
    run_workflow "FLOAT" "20.5" "21.5" "10.5" "11.5"
    verify_all "c_col" "10.5" "11.5" "20.5" "21.5"
}

@test "remote_add_column: DECIMAL" {
    require_binaries
    run_workflow "DECIMAL(10,2)" "20.50" "21.75" "10.25" "11.00"
    verify_all "c_col" "10.25" "11.00" "20.50" "21.75"
}

# ---------------------------------------------------------------------------
# String and text types
# ---------------------------------------------------------------------------

@test "remote_add_column: VARCHAR" {
    require_binaries
    run_workflow "VARCHAR(255)" \
      "'a-varchar-20'" "'a-varchar-21'" "'b-varchar-10'" "'b-varchar-11'"
    verify_all "c_col" "b-varchar-10" "b-varchar-11" "a-varchar-20" "a-varchar-21"
}

@test "remote_add_column: TEXT" {
    require_binaries
    run_workflow "TEXT" \
      "'a-text-20'" "'a-text-21'" "'b-text-10'" "'b-text-11'"
    verify_all "c_col" "b-text-10" "b-text-11" "a-text-20" "a-text-21"
}

# ---------------------------------------------------------------------------
# Binary types
# ---------------------------------------------------------------------------

@test "remote_add_column: VARBINARY" {
    require_binaries
    run_workflow "VARBINARY(255)" \
      "'a-vb-20'" "'a-vb-21'" "'b-vb-10'" "'b-vb-11'"
    verify_all "c_col" "b-vb-10" "b-vb-11" "a-vb-20" "a-vb-21"
}

@test "remote_add_column: BLOB" {
    require_binaries
    run_workflow "BLOB" \
      "'a-blob-20'" "'a-blob-21'" "'b-blob-10'" "'b-blob-11'"
    verify_all "c_col" "b-blob-10" "b-blob-11" "a-blob-20" "a-blob-21"
}

# ---------------------------------------------------------------------------
# Temporal types
# ---------------------------------------------------------------------------

@test "remote_add_column: DATETIME" {
    require_binaries
    run_workflow "DATETIME" \
      "'2025-03-20 20:00:00'" "'2025-03-21 21:00:00'" \
      "'2025-01-10 10:00:00'" "'2025-01-11 11:00:00'"
    verify_all "c_col" \
      "2025-01-10 10:00:00" "2025-01-11 11:00:00" \
      "2025-03-20 20:00:00" "2025-03-21 21:00:00"
}

@test "remote_add_column: TIMESTAMP" {
    require_binaries
    run_workflow "TIMESTAMP NULL DEFAULT NULL" \
      "'2025-03-20 20:00:00'" "'2025-03-21 21:00:00'" \
      "'2025-01-10 10:00:00'" "'2025-01-11 11:00:00'"
    verify_all "c_col" \
      "2025-01-10 10:00:00" "2025-01-11 11:00:00" \
      "2025-03-20 20:00:00" "2025-03-21 21:00:00"
}

# ---------------------------------------------------------------------------
# JSON, ENUM, SET
# ---------------------------------------------------------------------------

@test "remote_add_column: JSON" {
    require_binaries
    run_workflow "JSON" \
      "'{\"k\":\"a-20\"}'" "'{\"k\":\"a-21\"}'" \
      "'{\"k\":\"b-10\"}'" "'{\"k\":\"b-11\"}'"
    verify_count
    verify_col "JSON_UNQUOTE(JSON_EXTRACT(c_col, '\$.k'))" 10 "b-10"
    verify_col "JSON_UNQUOTE(JSON_EXTRACT(c_col, '\$.k'))" 11 "b-11"
    verify_col "JSON_UNQUOTE(JSON_EXTRACT(c_col, '\$.k'))" 20 "a-20"
    verify_col "JSON_UNQUOTE(JSON_EXTRACT(c_col, '\$.k'))" 21 "a-21"
}

@test "remote_add_column: ENUM" {
    require_binaries
    run_workflow "ENUM('v10','v11','v20','v21')" \
      "'v20'" "'v21'" "'v10'" "'v11'"
    verify_all "c_col" "v10" "v11" "v20" "v21"
}

@test "remote_add_column: SET" {
    require_binaries
    run_workflow "SET('v10','v11','v20','v21')" \
      "'v20'" "'v21'" "'v10'" "'v11'"
    verify_all "c_col" "v10" "v11" "v20" "v21"
}

# ---------------------------------------------------------------------------
# Geometry types
# ---------------------------------------------------------------------------

@test "remote_add_column: POINT" {
    require_binaries
    run_workflow "POINT" \
      "ST_GeomFromText('POINT(20 20)')" "ST_GeomFromText('POINT(21 21)')" \
      "ST_GeomFromText('POINT(10 10)')" "ST_GeomFromText('POINT(11 11)')"
    verify_count
    verify_col "ST_X(c_col)" 10 "10"
    verify_col "ST_X(c_col)" 11 "11"
    verify_col "ST_X(c_col)" 20 "20"
    verify_col "ST_X(c_col)" 21 "21"
}

@test "remote_add_column: LINESTRING" {
    require_binaries
    run_workflow "LINESTRING" \
      "ST_GeomFromText('LINESTRING(0 0,20 20)')" \
      "ST_GeomFromText('LINESTRING(0 0,21 21)')" \
      "ST_GeomFromText('LINESTRING(0 0,10 10)')" \
      "ST_GeomFromText('LINESTRING(0 0,11 11)')"
    verify_count
    verify_col "ST_AsText(c_col)" 10 "LINESTRING"
    verify_col "ST_AsText(c_col)" 20 "LINESTRING"
}

@test "remote_add_column: GEOMETRYCOLLECTION" {
    require_binaries
    run_workflow "GEOMETRYCOLLECTION" \
      "ST_GeomFromText('GEOMETRYCOLLECTION(POINT(20 20),LINESTRING(0 0,1 1))')" \
      "ST_GeomFromText('GEOMETRYCOLLECTION(POINT(21 21))')" \
      "ST_GeomFromText('GEOMETRYCOLLECTION(POINT(10 10))')" \
      "ST_GeomFromText('GEOMETRYCOLLECTION(POINT(11 11))')"
    verify_count
    verify_col "ST_AsText(c_col)" 10 "GEOMETRYCOLLECTION"
    verify_col "ST_AsText(c_col)" 20 "GEOMETRYCOLLECTION"
}
