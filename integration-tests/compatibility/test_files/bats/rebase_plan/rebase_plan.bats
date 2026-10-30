#!/usr/bin/env bats
# Rebase plan backward compatibility tests: an in-progress rebase started by an older dolt client must work in the
# current client.
#
# Older clients store the commit hashes of the dolt_rebase table as text. The current client stores new plans as commit
# addresses, but a plan is never migrated, so these tests cover the current client using a plan in the older format.
#
# Each test creates an isolated repository, so REPO_DIR just needs to be a clean scratch space.

setup() {
    bats_load_library common.bash
    bats_load_library compat-common.bash
    [ -n "$DOLT_OLD_BIN" ] || skip "requires DOLT_OLD_BIN"
    [ -n "$DOLT_NEW_BIN" ] || skip "requires DOLT_NEW_BIN"
    skip_if_old_lte "1.20.0" "dolt_rebase is not available"
    cp -Rpf $REPO_DIR bats_repo
    cd bats_repo
}

teardown() {
    cd ..
    rm -rf bats_repo
}

# start_rebase_with_old_dolt leaves a rebase of branch feat, which adds commits b and c, onto main in progress.
start_rebase_with_old_dolt() {
    old_dolt init
    old_dolt sql <<SQL
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

@test "rebase_plan: current client reads a plan from an older client" {
    start_rebase_with_old_dolt
    expected=$(old_dolt sql -r csv -q "CALL dolt_checkout('dolt_rebase_feat'); SELECT * FROM dolt_rebase ORDER BY rebase_order;")

    run new_dolt sql -r csv -q "CALL dolt_checkout('dolt_rebase_feat'); SELECT * FROM dolt_rebase ORDER BY rebase_order;"
    [ "$status" -eq 0 ]
    assert_no_panic_shape "$output"
    [ "$output" = "$expected" ]
    [[ "$output" =~ "pick" ]] || false

    # A plan is never migrated, so this is still a plan in the older format.
    run new_dolt sql -q "CALL dolt_checkout('dolt_rebase_feat'); SHOW CREATE TABLE dolt_rebase;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ '`commit_hash` text' ]] || false

    run new_dolt sql -r csv -q "CALL dolt_checkout('dolt_rebase_feat'); SELECT count(*) FROM dolt_status;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "0" ]] || false
}

@test "rebase_plan: current client finishes a rebase started by an older client" {
    start_rebase_with_old_dolt

    run new_dolt sql -q "CALL dolt_checkout('dolt_rebase_feat'); CALL dolt_rebase('--continue');"
    [ "$status" -eq 0 ]
    assert_no_panic_shape "$output"
    [[ "$output" =~ "Successfully rebased" ]] || false

    run new_dolt sql -r csv -q "CALL dolt_checkout('feat'); SELECT message FROM dolt_log LIMIT 3;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ c.*b.*d ]] || false
    run new_dolt sql -r csv -q "CALL dolt_checkout('feat'); SELECT count(*) FROM t;"
    [[ "$output" =~ "4" ]] || false
    run new_dolt branch
    [[ ! "$output" =~ "dolt_rebase_feat" ]] || false
}

@test "rebase_plan: current client edits a plan from an older client" {
    start_rebase_with_old_dolt

    run new_dolt sql -q "CALL dolt_checkout('dolt_rebase_feat'); UPDATE dolt_rebase SET action = 'drop' WHERE commit_message = 'b'; UPDATE dolt_rebase SET action = 'reword', commit_message = 'c2' WHERE commit_message = 'c'; CALL dolt_rebase('--continue');"
    [ "$status" -eq 0 ]
    assert_no_panic_shape "$output"
    [[ "$output" =~ "Successfully rebased" ]] || false

    run new_dolt sql -r csv -q "CALL dolt_checkout('feat'); SELECT message FROM dolt_log LIMIT 2;"
    [[ "$output" =~ c2.*d ]] || false
    run new_dolt sql -r csv -q "CALL dolt_checkout('feat'); SELECT count(*) FROM t WHERE pk = 1;"
    [[ "$output" =~ "0" ]] || false
}

@test "rebase_plan: current client aborts a rebase started by an older client" {
    start_rebase_with_old_dolt

    run new_dolt sql -q "CALL dolt_checkout('dolt_rebase_feat'); CALL dolt_rebase('--abort');"
    [ "$status" -eq 0 ]
    assert_no_panic_shape "$output"
    [[ "$output" =~ "Interactive rebase aborted" ]] || false

    run new_dolt sql -r csv -q "CALL dolt_checkout('feat'); SELECT message FROM dolt_log LIMIT 2;"
    [[ "$output" =~ c.*b ]] || false
    run new_dolt branch
    [[ ! "$output" =~ "dolt_rebase_feat" ]] || false
}

@test "rebase_plan: current client finishes a rebase that an older client paused on a conflict" {
    old_dolt init
    old_dolt sql <<SQL
CREATE TABLE t (pk INT NOT NULL PRIMARY KEY, v INT);
INSERT INTO t VALUES (0, 0);
CALL dolt_commit('-Am', 'a');
CALL dolt_checkout('-b', 'feat');
UPDATE t SET v = 10 WHERE pk = 0;
CALL dolt_commit('-am', 'b');
CALL dolt_checkout('main');
UPDATE t SET v = 20 WHERE pk = 0;
CALL dolt_commit('-am', 'c');
CALL dolt_checkout('feat');
CALL dolt_rebase('-i', 'main');
SQL
    run old_dolt sql -q "SET @@dolt_allow_commit_conflicts = 1; CALL dolt_checkout('dolt_rebase_feat'); CALL dolt_rebase('--continue');"
    [ "$status" -ne 0 ]
    [[ "$output" =~ "conflict" ]] || false

    run new_dolt sql -q "SET @@dolt_allow_commit_conflicts = 1; CALL dolt_checkout('dolt_rebase_feat'); CALL dolt_conflicts_resolve('--theirs', 't'); CALL dolt_add('t'); CALL dolt_rebase('--continue');"
    [ "$status" -eq 0 ]
    assert_no_panic_shape "$output"
    [[ "$output" =~ "Successfully rebased" ]] || false

    run new_dolt sql -r csv -q "CALL dolt_checkout('feat'); SELECT v FROM t WHERE pk = 0;"
    [[ "$output" =~ "10" ]] || false
}

@test "rebase_plan: current client garbage collects a plan from an older client" {
    start_rebase_with_old_dolt

    run new_dolt gc
    [ "$status" -eq 0 ]
    assert_no_panic_shape "$output"

    run new_dolt sql -q "CALL dolt_checkout('dolt_rebase_feat'); CALL dolt_rebase('--continue');"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully rebased" ]] || false
}
