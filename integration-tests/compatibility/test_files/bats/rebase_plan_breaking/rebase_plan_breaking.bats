#!/usr/bin/env bats
# Rebase plan forward compatibility tests: verify that older dolt clients fail with a clear error when reading a plan
# from the current client, which stores the commit hashes of the dolt_rebase table as commit addresses.
#
# Those columns are written with a new field of the column schema message, and older clients reject schemas with fields
# they don't know with "table has unknown fields". Only the plan is affected, and it only exists during a rebase.
#
# Each test creates an isolated repository, so REPO_DIR just needs to be a clean scratch space.

setup() {
    bats_load_library common.bash
    bats_load_library compat-common.bash
    [ -n "$DOLT_OLD_BIN" ] || skip "requires DOLT_OLD_BIN"
    [ -n "$DOLT_NEW_BIN" ] || skip "requires DOLT_NEW_BIN"
    cp -Rpf $REPO_DIR bats_repo
    cd bats_repo
}

teardown() {
    cd ..
    rm -rf bats_repo
}

# start_rebase_with_new_dolt leaves a rebase of branch feat, which adds commits b and c, onto main in progress.
start_rebase_with_new_dolt() {
    new_dolt init
    new_dolt sql <<SQL
CREATE TABLE t (pk INT NOT NULL PRIMARY KEY, v INT);
INSERT INTO t VALUES (0, 0);
CALL dolt_commit('-Am', 'a');
CALL dolt_checkout('-b', 'feat');
INSERT INTO t VALUES (1, 1);
CALL dolt_commit('-am', 'b');
INSERT INTO t VALUES (2, 2);
CALL dolt_commit('-am', 'c');
CALL dolt_checkout('main');
INSERT INTO t VALUES (3, 3);
CALL dolt_commit('-am', 'd');
CALL dolt_checkout('feat');
CALL dolt_rebase('-i', 'main');
SQL
}

@test "rebase_plan_breaking: old client fails reading a plan from the current client" {
    start_rebase_with_new_dolt

    run old_dolt sql -q "CALL dolt_checkout('dolt_rebase_feat'); SELECT * FROM dolt_rebase;"
    [ "$status" -ne 0 ]
    [[ "$output" =~ "table has unknown fields" ]] || false
    [[ ! "$output" =~ "panic" ]] || false
    [[ ! "$output" =~ "goroutine" ]] || false
}

@test "rebase_plan_breaking: old client fails continuing a rebase, which the current client can still finish" {
    start_rebase_with_new_dolt

    run old_dolt sql -q "CALL dolt_checkout('dolt_rebase_feat'); CALL dolt_rebase('--continue');"
    [ "$status" -ne 0 ]
    [[ "$output" =~ "table has unknown fields" ]] || false
    [[ ! "$output" =~ "goroutine" ]] || false

    run new_dolt sql -q "CALL dolt_checkout('dolt_rebase_feat'); CALL dolt_rebase('--continue');"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully rebased" ]] || false
}

@test "rebase_plan_breaking: old client can still use other branches during a rebase" {
    start_rebase_with_new_dolt

    run old_dolt sql -r csv -q "SELECT count(*) FROM t;"
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "goroutine" ]] || false
    [[ "$output" =~ "2" ]] || false

    run old_dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch main" ]] || false

    run old_dolt sql -q "INSERT INTO t VALUES (5, 5); CALL dolt_commit('-am', 'e');"
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "goroutine" ]] || false
}

@test "rebase_plan_breaking: old client garbage collection keeps the commits of a plan" {
    start_rebase_with_new_dolt

    # Rewriting the branch leaves the commit of the last plan step reachable only through the plan.
    run new_dolt sql -q "CALL dolt_checkout('feat'); CALL dolt_commit('--amend', '-m', 'c2');"
    [ "$status" -eq 0 ]

    run old_dolt gc
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "goroutine" ]] || false

    run new_dolt sql -q "CALL dolt_checkout('dolt_rebase_feat'); CALL dolt_rebase('--continue');"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully rebased" ]] || false
}

@test "rebase_plan_breaking: old client reads the repository once the rebase is finished" {
    start_rebase_with_new_dolt

    run new_dolt sql -q "CALL dolt_checkout('dolt_rebase_feat'); CALL dolt_rebase('--continue');"
    [ "$status" -eq 0 ]

    run old_dolt sql -r csv -q "CALL dolt_checkout('feat'); SELECT count(*) FROM t;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "4" ]] || false
}
