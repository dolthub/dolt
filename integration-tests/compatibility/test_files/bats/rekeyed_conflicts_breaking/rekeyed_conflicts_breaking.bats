#!/usr/bin/env bats
# Rekeyed conflict compatibility tests: verify that older dolt clients fail with a clear error when reading the
# conflicts of a merge whose sides changed a primary key the same way, and still read the conflicts of other merges.
#
# Such a merge rekeys the common ancestor to the new primary key, and its conflicts hold the address of the rekeyed
# ancestor in a new field of the merge artifacts message. Older clients reject messages with fields they don't know
# with "table has unknown fields". Merge artifacts without such an address don't write the field.

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

rekeyed_conflicts_query="SELECT base_a, base_b, base_v, our_v, their_v FROM dolt_conflicts_rekeyed ORDER BY base_a;"

# check_rekeyed_conflicts checks that the current client reads the conflicts of the merge of rekeyed_right into
# rekeyed_left, with their base keyed by the new primary key.
check_rekeyed_conflicts() {
    run new_dolt sql -r csv -q "CALL DOLT_CHECKOUT('rekeyed_left'); $rekeyed_conflicts_query"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "base_a,base_b,base_v,our_v,their_v" ]] || false
    [[ "$output" =~ "1,1,10,100,11" ]] || false
    # Only rekeyed_right changed row 2, so it merged cleanly.
    [[ ! "$output" =~ "2,2,20" ]] || false
}

@test "rekeyed_conflicts_breaking: old client fails reading conflicts with a rekeyed base" {
    [ -n "$DOLT_OLD_BIN" ] || skip "requires DOLT_OLD_BIN"

    run old_dolt sql -q "CALL DOLT_CHECKOUT('rekeyed_left'); SELECT * FROM dolt_conflicts_rekeyed;"
    [ "$status" -ne 0 ]
    [[ "$output" =~ "table has unknown fields" ]] || false

    run old_dolt sql -q "CALL DOLT_CHECKOUT('rekeyed_left'); SELECT * FROM dolt_status;"
    [ "$status" -ne 0 ]
    [[ "$output" =~ "table has unknown fields" ]] || false
}

@test "rekeyed_conflicts_breaking: old client reads conflicts of other merges" {
    [ -n "$DOLT_OLD_BIN" ] || skip "requires DOLT_OLD_BIN"

    run old_dolt sql -r csv -q "CALL DOLT_CHECKOUT('plain_left'); SELECT base_pk, base_v, our_v, their_v FROM dolt_conflicts_plain;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "base_pk,base_v,our_v,their_v" ]] || false
    [[ "$output" =~ "1,10,100,11" ]] || false

    run old_dolt sql -r csv -q "CALL DOLT_CHECKOUT('plain_left'); CALL DOLT_CONFLICTS_RESOLVE('--theirs', 'plain'); CALL DOLT_COMMIT('-am', 'old: resolve plain'); SELECT * FROM plain ORDER BY pk;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,11" ]] || false
    [[ "$output" =~ "2,20" ]] || false
}

@test "rekeyed_conflicts_breaking: current client reads conflicts with a rekeyed base" {
    check_rekeyed_conflicts

    run new_dolt sql -r csv -q "CALL DOLT_CHECKOUT('plain_left'); SELECT base_pk, base_v, our_v, their_v FROM dolt_conflicts_plain;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,10,100,11" ]] || false
}

@test "rekeyed_conflicts_breaking: current client keeps the rekeyed base through garbage collection" {
    run new_dolt gc
    [ "$status" -eq 0 ]
    check_rekeyed_conflicts

    new_dolt gc --full
    check_rekeyed_conflicts
}

@test "rekeyed_conflicts_breaking: current client rebuilds a rekeyed base that an old client garbage collected" {
    [ -n "$DOLT_OLD_BIN" ] || skip "requires DOLT_OLD_BIN"

    # Older clients don't know that the conflicts reference the rekeyed base, so their garbage collection removes it.
    run old_dolt gc
    [ "$status" -eq 0 ]
    check_rekeyed_conflicts

    run new_dolt sql -r csv -q "CALL DOLT_CHECKOUT('rekeyed_left'); CALL DOLT_CONFLICTS_RESOLVE('--theirs', 'rekeyed'); CALL DOLT_COMMIT('-am', 'resolve rekeyed'); SELECT * FROM rekeyed ORDER BY a;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,1,11" ]] || false
    [[ "$output" =~ "2,2,22" ]] || false
}

@test "rekeyed_conflicts_breaking: old client reads the branch after the current client resolves the conflicts" {
    [ -n "$DOLT_OLD_BIN" ] || skip "requires DOLT_OLD_BIN"

    new_dolt sql -q "CALL DOLT_CHECKOUT('rekeyed_left'); CALL DOLT_CONFLICTS_RESOLVE('--ours', 'rekeyed'); CALL DOLT_COMMIT('-am', 'resolve rekeyed');"

    run old_dolt sql -r csv -q "CALL DOLT_CHECKOUT('rekeyed_left'); SELECT * FROM rekeyed ORDER BY a;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,1,100" ]] || false
    [[ "$output" =~ "2,2,22" ]] || false
    [[ "$output" =~ "3,3,30" ]] || false

    run old_dolt sql -r csv -q "CALL DOLT_CHECKOUT('rekeyed_left'); SELECT COUNT(*) AS changes FROM dolt_status;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "changes"$'\n'"0" ]] || false
}
