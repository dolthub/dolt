#!/usr/bin/env bats
# Descending index compatibility tests: verify that older dolt clients fail with a clear error when
# reading tables whose indexes have descending columns, and still read tables whose indexes use the
# default order.
#
# Descending columns are written as new fields of the index schema message, and older clients reject
# schemas with fields they don't know with "table has unknown fields". Indexes without a descending
# column don't write the fields.

setup() {
    bats_load_library common.bash
    bats_load_library compat-common.bash
    cp -Rpf $REPO_DIR bats_repo
    cd bats_repo
}

teardown() {
    cd ..
    rm -rf bats_repo
}

@test "desc_index_breaking: old client fails reading a table with a descending index" {
    [ -n "$DOLT_OLD_BIN" ] || skip "requires DOLT_OLD_BIN"

    run old_dolt sql -q "SELECT * FROM desc_index;"
    [ "$status" -ne 0 ]
    [[ "$output" =~ "table has unknown fields" ]] || false
}

@test "desc_index_breaking: old client fails on schema show for a table with a descending index" {
    [ -n "$DOLT_OLD_BIN" ] || skip "requires DOLT_OLD_BIN"

    run old_dolt schema show desc_index
    [ "$status" -ne 0 ]
    [[ "$output" =~ "table has unknown fields" ]] || false
}

@test "desc_index_breaking: old client fails on dolt diff for a table with a descending index" {
    [ -n "$DOLT_OLD_BIN" ] || skip "requires DOLT_OLD_BIN"

    run old_dolt diff HEAD~1 HEAD -- desc_index
    [ "$status" -ne 0 ]
    [[ "$output" =~ "table has unknown fields" ]] || false
}

@test "desc_index_breaking: old client can still read a table whose index uses the default order" {
    [ -n "$DOLT_OLD_BIN" ] || skip "requires DOLT_OLD_BIN"

    run old_dolt sql -q "SELECT * FROM default_index WHERE c_int > 10 ORDER BY pk;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[0]}" == "pk,c_int,c_varchar" ]] || false
    [[ "${lines[1]}" == "1,42,hello" ]] || false
    [[ "${lines[2]}" == "2,99,world" ]] || false

    run old_dolt schema show default_index
    [ "$status" -eq 0 ]
    [[ "$output" =~ "KEY \`c_int_asc\` (\`c_int\`,\`c_varchar\`)" ]] || false
}

@test "desc_index_breaking: current client reads both tables" {
    run new_dolt sql -q "SELECT pk FROM desc_index WHERE c_int > 10 ORDER BY c_int DESC;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" == "2" ]] || false
    [[ "${lines[2]}" == "1" ]] || false

    run new_dolt sql -q "SELECT pk FROM default_index WHERE c_int > 10 ORDER BY c_int;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" == "1" ]] || false
    [[ "${lines[2]}" == "2" ]] || false

    run new_dolt schema show desc_index
    [ "$status" -eq 0 ]
    [[ "$output" =~ "KEY \`c_int_desc\` (\`c_int\` DESC,\`c_varchar\`)" ]] || false
}

@test "desc_index_breaking: old client reads a default order index after the current client changes its column type" {
    [ -n "$DOLT_OLD_BIN" ] || skip "requires DOLT_OLD_BIN"

    new_dolt sql -q "ALTER TABLE default_index MODIFY COLUMN c_int BIGINT; INSERT INTO default_index VALUES (5, 5000000000, 'big');"

    run old_dolt sql -q "SELECT pk FROM default_index WHERE c_int > 50 ORDER BY c_int;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" == "2" ]] || false
    [[ "${lines[2]}" == "5" ]] || false
}
