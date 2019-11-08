#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}

@test "diff sql output reconciles INSERT query" {
    dolt checkout -b firstbranch
    dolt table create -s=`batshelper 1pk5col-ints.schema` test
    dolt sql -q 'INSERT INTO test (pk, c1, c2, c3, c4, c5) VALUES (0, 0, 0, 0, 0, 0)'
    dolt add test
    dolt commit -m "Added one initial row"

    dolt checkout -b newbranch
    dolt sql -q 'INSERT INTO test (pk, c1, c2, c3, c4, c5) VALUES (1, 1, 1, 1, 1, 1)'
    dolt sql -q 'INSERT INTO test (pk, c1, c2, c3, c4, c5) VALUES (2, 11, 0, 0, 0, 0)'
    dolt sql -q 'INSERT INTO test (pk, c1, c2, c3, c4, c5) VALUES (3, 11, 0, 0, 0, 0)'
    dolt add test
    dolt commit -m "Added three rows"

    # confirm a difference exists
    run dolt diff newbranch firstbranch
    [ "$status" -eq 0 ]
    [[ "$output" != "" ]] || false

    dolt diff --sql newbranch firstbranch > query
    dolt checkout firstbranch
    dolt sql < query
    rm query
    dolt add test
    dolt commit -m "Reconciled with newbranch"

    # confirm difference was reconciled
    run dolt diff newbranch firstbranch
    [ "$status" -eq 0 ]
    [[ "$output" = "" ]] || false
}

@test "diff sql output reconciles UPDATE query" {
    dolt checkout -b firstbranch
    dolt table create -s=`batshelper 1pk5col-ints.schema` test
    dolt sql -q 'INSERT INTO test (pk, c1, c2, c3, c4, c5) VALUES (0, 0, 0, 0, 0, 0)'
    dolt sql -q 'INSERT INTO test (pk, c1, c2, c3, c4, c5) VALUES (1, 1, 1, 1, 1, 1)'
    dolt sql -q 'INSERT INTO test (pk, c1, c2, c3, c4, c5) VALUES (2, 11, 0, 0, 0, 0)'
    dolt sql -q 'INSERT INTO test (pk, c1, c2, c3, c4, c5) VALUES (3, 11, 0, 0, 0, 0)'
    dolt add test
    dolt commit -m "Added four initial rows"

    dolt checkout -b newbranch
    dolt sql -q 'UPDATE test SET c1=11, c5=6 WHERE pk=0'
    dolt add test
    dolt commit -m "modified first row"

    # confirm a difference exists
    run dolt diff newbranch firstbranch
    [ "$status" -eq 0 ]
    [[ "$output" != "" ]] || false

    dolt diff --sql newbranch firstbranch > query
    dolt checkout firstbranch
    dolt sql < query
    rm query
    dolt add test
    dolt commit -m "Reconciled with newbranch"

    # confirm difference was reconciled
    run dolt diff newbranch firstbranch
    [ "$status" -eq 0 ]
    [[ "$output" = "" ]] || false
}


@test "diff sql output reconciles DELETE query" {
    dolt checkout -b firstbranch
    dolt table create -s=`batshelper 1pk5col-ints.schema` test
    dolt sql -q 'INSERT INTO test (pk, c1, c2, c3, c4, c5) VALUES (0, 0, 0, 0, 0, 0)'
    dolt sql -q 'INSERT INTO test (pk, c1, c2, c3, c4, c5) VALUES (1, 1, 1, 1, 1, 1)'
    dolt sql -q 'INSERT INTO test (pk, c1, c2, c3, c4, c5) VALUES (2, 11, 0, 0, 0, 0)'
    dolt sql -q 'INSERT INTO test (pk, c1, c2, c3, c4, c5) VALUES (3, 11, 0, 0, 0, 0)'
    dolt add test
    dolt commit -m "Added four initial rows"

    dolt checkout -b newbranch
    dolt sql -q 'DELETE FROM test WHERE pk=0'
    dolt add test
    dolt commit -m "deleted first row"

    # confirm a difference exists
    run dolt diff newbranch firstbranch
    [ "$status" -eq 0 ]
    [[ "$output" != "" ]] || false

    dolt diff --sql newbranch firstbranch > query
    dolt checkout firstbranch
    dolt sql < query
    rm query
    dolt add test
    dolt commit -m "Reconciled with newbranch"

    # confirm difference was reconciled
    run dolt diff newbranch firstbranch
    [ "$status" -eq 0 ]
    [[ "$output" = "" ]] || false
}