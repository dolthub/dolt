#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

# History from https://github.com/dolthub/dolt/issues/12050. A data-free schema branch is merged into main and into
# feature, so main and feature have two best common ancestors: the fork point and the schema tip.

setup() {
    setup_common
}

teardown() {
    teardown_common
}

# $1: number of extra seed commits on main before feature forks. 0 makes the schema tip taller than the fork point,
# 1 makes them equally tall, 2 makes the fork point taller.
setup_schema_branch_criss_cross() {
    dolt sql -q "CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(20));"
    dolt commit -Am "c1: create table"
    dolt branch schema
    dolt sql -q "INSERT INTO t VALUES (1, 'seed'), (2, 'seed');"
    dolt commit -am "c2: seed rows on main"
    for ((i = 0; i < $1; i++)); do
        dolt sql -q "INSERT INTO t VALUES ($((i + 3)), 'seed');"
        dolt commit -am "c2-$((i + 3)): more seed rows"
    done
    dolt branch feature
    dolt sql -q "UPDATE t SET v = 'edited on main' WHERE id = 1; DELETE FROM t WHERE id = 2;"
    dolt commit -am "c3: main edits row 1 and deletes row 2"

    dolt checkout feature
    dolt sql -q "INSERT INTO t VALUES (50, 'feature');"
    dolt commit -am "f1: feature adds row 50"

    dolt checkout schema
    dolt sql -q "ALTER TABLE t ADD COLUMN n INT NULL;"
    dolt commit -am "s1: add column n"
    dolt sql -q "ALTER TABLE t ADD COLUMN m INT NULL;"
    dolt commit -am "s2: add column m"

    dolt checkout main
    dolt merge schema -m "merge schema into main"
    dolt checkout feature
    dolt merge schema -m "merge schema into feature"
}

@test "merge-criss-cross: merge-base --all lists every merge base" {
    setup_schema_branch_criss_cross 0
    fork_point=$(dolt sql -q "SELECT commit_hash FROM dolt_log('--all') WHERE message = 'c2: seed rows on main';" -r csv | tail -n 1)
    schema_tip=$(get_head_commit schema)

    run dolt merge-base --all main feature
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "$fork_point" ]] || false
    [[ "$output" =~ "$schema_tip" ]] || false

    run dolt merge-base --all feature main
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]

    run dolt merge-base --all main schema
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]
    [ "$output" = "$schema_tip" ]

    run dolt merge-base main feature
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]
}

@test "merge-criss-cross: merge-base --all lists the newest merge base first, like git" {
    # The fork point is taller than the schema tip, but the schema tip is newer.
    setup_schema_branch_criss_cross 2
    fork_point=$(dolt sql -q "SELECT commit_hash FROM dolt_log('--all') WHERE message = 'c2-4: more seed rows';" -r csv | tail -n 1)
    schema_tip=$(get_head_commit schema)

    run dolt merge-base --all main feature
    [ "$status" -eq 0 ]
    [ "${lines[0]}" = "$schema_tip" ]
    [ "${lines[1]}" = "$fork_point" ]
}
