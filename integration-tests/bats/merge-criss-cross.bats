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
    skip "merge uses a single merge base: https://github.com/dolthub/dolt/issues/12050"
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
    skip "merge uses a single merge base: https://github.com/dolthub/dolt/issues/12050"
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
    skip "merge uses a single merge base: https://github.com/dolthub/dolt/issues/12050"
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
