#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

# History from https://github.com/dolthub/dolt/issues/12050. A data-free schema branch is merged into main and into
# feature, so main and feature have two best common ancestors: the fork point and the schema tip. Expected results
# match git's default merge strategy on an equivalent history.

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

@test "merge-criss-cross: cli merge of main into feature applies main's edits and deletes" {
    setup_schema_branch_criss_cross 0

    run dolt merge main -m "merge main into feature"
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "CONFLICT" ]] || false

    run dolt sql -q "SELECT id, v, n, m FROM t ORDER BY id;" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
    [ "${lines[1]}" = "1,edited on main,," ]
    [ "${lines[2]}" = "50,feature,," ]
}

@test "merge-criss-cross: sql merge of main into feature applies main's edits and deletes" {
    setup_schema_branch_criss_cross 0

    run dolt sql -r csv <<SQL
SET autocommit = 0;
CALL DOLT_MERGE('main', '-m', 'merge main into feature');
SELECT count(*) AS conflicts FROM dolt_conflicts;
SELECT id, v FROM t ORDER BY id;
SQL
    [ "$status" -eq 0 ]
    [[ "$output" =~ ",0,0,merge successful" ]] || false
    [[ "$output" =~ "conflicts"$'\n'"0" ]] || false
    [[ "$output" =~ "id,v"$'\n'"1,edited on main"$'\n'"50,feature" ]] || false
}

@test "merge-criss-cross: equally tall merge bases merge the same way every time" {
    # Equally tall bases were chosen by commit hash, which depends on commit timestamps, so check several histories.
    for i in 1 2 3 4 5; do
        mkdir "repo$i" && cd "repo$i"
        dolt init
        setup_schema_branch_criss_cross 1

        run dolt merge main -m "merge main into feature"
        [ "$status" -eq 0 ]
        [[ ! "$output" =~ "CONFLICT" ]] || false

        run dolt sql -q "SELECT id, v FROM t ORDER BY id;" -r csv
        [ "$status" -eq 0 ]
        [ "${lines[1]}" = "1,edited on main" ]
        [ "${lines[2]}" = "3,seed" ]
        [ "${lines[3]}" = "50,feature" ]
        cd ..
    done
}

@test "merge-criss-cross: fork point taller than the schema tip" {
    setup_schema_branch_criss_cross 2

    run dolt merge main -m "merge main into feature"
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "CONFLICT" ]] || false

    run dolt sql -q "SELECT id, v FROM t ORDER BY id;" -r csv
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = "1,edited on main" ]
    [ "${lines[2]}" = "3,seed" ]
    [ "${lines[3]}" = "4,seed" ]
    [ "${lines[4]}" = "50,feature" ]
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

# x1 and y1 both change row 1 and are merged into each other's branches, which keep different values. Merging y into
# x then conflicts against a virtual merge base, which no branch references.
setup_conflicting_merge_bases() {
    dolt sql -q "CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(20)); INSERT INTO t VALUES (1, 'orig');"
    dolt commit -Am "c1"
    dolt branch x
    dolt branch y
    dolt checkout x
    dolt sql -q "UPDATE t SET v = 'x' WHERE id = 1;"
    dolt commit -am "x1"
    dolt checkout y
    dolt sql -q "UPDATE t SET v = 'y' WHERE id = 1;"
    dolt commit -am "y1"
    dolt checkout x
    dolt merge y || true
    dolt conflicts resolve --ours t
    dolt commit -am "x2"
    dolt checkout y
    dolt merge x~1 || true
    dolt conflicts resolve --ours t
    dolt commit -am "y2"
    dolt checkout x
}

@test "merge-criss-cross: conflicts keep their virtual merge base values after gc" {
    setup_conflicting_merge_bases

    run dolt merge y
    [ "$status" -ne 0 ]
    [[ "$output" =~ "CONFLICT" ]] || false

    dolt gc

    run dolt sql -q "SELECT base_v, our_v, their_v FROM dolt_conflicts_t;" -r csv
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = "orig,x,y" ]
}

@test "merge-criss-cross: committed conflicts keep their virtual merge base values after gc" {
    setup_conflicting_merge_bases

    run dolt merge y
    [ "$status" -ne 0 ]
    dolt commit --force -am "commit with conflicts"
    dolt gc

    run dolt sql -q "SELECT base_v, our_v, their_v FROM dolt_conflicts_t;" -r csv
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = "orig,x,y" ]
}

@test "merge-criss-cross: merge-base returns the newest merge base, like git" {
    # The fork point is taller than the schema tip, but the schema tip is newer.
    setup_schema_branch_criss_cross 2
    schema_tip=$(get_head_commit schema)

    run dolt merge-base main feature
    [ "$status" -eq 0 ]
    [ "$output" = "$schema_tip" ]

    run dolt merge-base --all main feature
    [ "$status" -eq 0 ]
    [ "${lines[0]}" = "$schema_tip" ]
}

@test "merge-criss-cross: three-dot diff warns about several merge bases, like git" {
    setup_schema_branch_criss_cross 2
    schema_tip=$(get_head_commit schema)

    run dolt diff main...feature
    [ "$status" -eq 0 ]
    [[ "$output" =~ "warning: main...feature: multiple merge bases, using $schema_tip" ]] || false

    run dolt diff main...schema
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "multiple merge bases" ]] || false
}

# bats test_tags=no_lambda
@test "merge-criss-cross: sql shell shows the three-dot diff warning" {
    skiponwindows "Need to install expect and make this script work on windows."
    setup_schema_branch_criss_cross 2

    run $BATS_TEST_DIRNAME/merge-criss-cross-diff-warning.expect
    [ "$status" -eq 0 ]
}

@test "merge-criss-cross: three-dot log excludes every merge base, like git" {
    setup_schema_branch_criss_cross 2

    run dolt log --oneline main...feature
    [ "$status" -eq 0 ]
    [[ "$output" =~ "f1: feature adds row 50" ]] || false
    [[ "$output" =~ "c3: main edits row 1 and deletes row 2" ]] || false
    [[ ! "$output" =~ "c2: seed rows on main" ]] || false
    [[ ! "$output" =~ "multiple merge bases" ]] || false
}

@test "merge-criss-cross: status counts commits ahead of upstream after a merge, like git" {
    mkdir remote
    dolt remote add origin file://remote
    dolt sql -q "CREATE TABLE t (id INT PRIMARY KEY);"
    dolt commit -Am "c1"
    dolt branch feature
    dolt checkout feature
    dolt sql -q "INSERT INTO t VALUES (1);"
    dolt commit -am "f1"
    dolt checkout main
    dolt sql -q "INSERT INTO t VALUES (2);"
    dolt commit -am "c2"
    dolt push -u origin main
    dolt merge feature -m "merge feature"

    # f1 and the merge commit are not in origin/main; git status reports 2.
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Your branch is ahead of 'origin/main' by 2 commits." ]] || false
}
