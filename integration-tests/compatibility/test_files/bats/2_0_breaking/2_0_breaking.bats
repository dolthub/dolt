#!/usr/bin/env bats
# 2.0 breaking compatibility tests: verify that older dolt clients fail with a clear error
# when reading tables that use adaptive encoding (written with DOLT_USE_ADAPTIVE_ENCODING=true).
#
# These tests use the current dolt (with adaptive encoding enabled) to create repositories
# containing TEXT and BLOB columns, then verify that an older dolt client (pre-2.0) fails
# with "table has unknown fields" when trying to read them.

dolt config --global --add metrics.disabled true > /dev/null 2>&1

setup() {
    cp -Rpf $REPO_DIR bats_repo
    cd bats_repo
}

teardown() {
    cd ..
    rm -rf bats_repo
}

# When DOLT_LEGACY_BIN is set, old_dolt runs that binary; otherwise runs dolt.
old_dolt() {
  if [ -n "$DOLT_LEGACY_BIN" ]; then
    "$DOLT_LEGACY_BIN" "$@"
  else
    dolt "$@"
  fi
}

@test "2_0_breaking: old client fails reading TEXT column with adaptive encoding" {
    [ -n "$DOLT_LEGACY_BIN" ] || skip "requires DOLT_LEGACY_BIN"

    run old_dolt sql -q "SELECT * FROM text_types;"
    [ "$status" -ne 0 ]
    [[ "$output" =~ "table has unknown fields" ]] || false
}

@test "2_0_breaking: old client fails reading BLOB column with adaptive encoding" {
    [ -n "$DOLT_LEGACY_BIN" ] || skip "requires DOLT_LEGACY_BIN"

    run old_dolt sql -q "SELECT * FROM blob_types;"
    [ "$status" -ne 0 ]
    [[ "$output" =~ "table has unknown fields" ]] || false
}

@test "2_0_breaking: old client fails reading mixed TEXT and BLOB table with adaptive encoding" {
    [ -n "$DOLT_LEGACY_BIN" ] || skip "requires DOLT_LEGACY_BIN"

    run old_dolt sql -q "SELECT * FROM mixed_types;"
    [ "$status" -ne 0 ]
    [[ "$output" =~ "table has unknown fields" ]] || false
}

@test "2_0_breaking: old client can still read table with no TEXT or BLOB columns" {
    [ -n "$DOLT_LEGACY_BIN" ] || skip "requires DOLT_LEGACY_BIN"

    run old_dolt sql -q "SELECT * FROM no_text_blob ORDER BY pk;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[0]}" == "pk,c_int,c_varchar" ]] || false
    [[ "${lines[1]}" == "1,42,hello" ]] || false
    [[ "${lines[2]}" == "2,99,world" ]] || false
}

@test "2_0_breaking: old client fails on dolt diff for adaptive-encoded table" {
    [ -n "$DOLT_LEGACY_BIN" ] || skip "requires DOLT_LEGACY_BIN"

    run old_dolt diff HEAD~1 HEAD -- text_types
    [ "$status" -ne 0 ]
    [[ "$output" =~ "table has unknown fields" ]] || false
}

@test "2_0_breaking: old client fails on schema show for adaptive-encoded table" {
    [ -n "$DOLT_LEGACY_BIN" ] || skip "requires DOLT_LEGACY_BIN"

    run old_dolt schema show text_types
    [ "$status" -ne 0 ]
    [[ "$output" =~ "table has unknown fields" ]] || false
}
