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
    dolt table import -u test `batshelper 1pk5col-ints.csv`
    dolt add test
    dolt commit -m "Added one initial row"

    dolt checkout -b newbranch
    dolt sql -q 'INSERT INTO test (pk, c1, c2, c3, c4, c5) VALUES (2, 11, 0, 0, 0, 0)'
    dolt add test
    dolt commit -m "Added three rows"

    # confirm a difference exists
    run dolt diff --sql newbranch firstbranch
    [ "$status" -eq 0 ]
    [[ "$output" != "" ]] || false

    dolt diff --sql newbranch firstbranch > query
    dolt checkout firstbranch
    dolt sql < query
    rm query
    dolt add test
    dolt commit -m "Reconciled with newbranch"

    # confirm that both branches have the same content
    run dolt diff --sql newbranch firstbranch
    [ "$status" -eq 0 ]
    [[ "$output" = "" ]] || false
}

@test "diff sql output reconciles UPDATE query" {
    dolt checkout -b firstbranch
    dolt table create -s=`batshelper 1pk5col-ints.schema` test
    dolt table import -u test `batshelper 1pk5col-ints.csv`
    dolt add test
    dolt commit -m "Added one initial row"

    dolt checkout -b newbranch
    dolt sql -q 'UPDATE test SET c1=11, c5=6 WHERE pk=0'
    dolt add test
    dolt commit -m "modified first row"

    # confirm a difference exists
    run dolt diff --sql newbranch firstbranch
    [ "$status" -eq 0 ]
    [[ "$output" != "" ]] || false

    dolt diff --sql newbranch firstbranch > query
    dolt checkout firstbranch
    dolt sql < query
    rm query
    dolt add test
    dolt commit -m "Reconciled with newbranch"

    # confirm that both branches have the same content
    run dolt diff --sql newbranch firstbranch
    [ "$status" -eq 0 ]
    [[ "$output" = "" ]] || false
}

@test "diff sql output reconciles DELETE query" {
    dolt checkout -b firstbranch
    dolt table create -s=`batshelper 1pk5col-ints.schema` test
    dolt table import -u test `batshelper 1pk5col-ints.csv`
    dolt add test
    dolt commit -m "Added one initial row"

    dolt checkout -b newbranch
    dolt sql -q 'DELETE FROM test WHERE pk=0'
    dolt add test
    dolt commit -m "deleted first row"

    # confirm a difference exists
    run dolt diff --sql newbranch firstbranch
    [ "$status" -eq 0 ]
    [[ "$output" != "" ]] || false

    dolt diff --sql newbranch firstbranch > query
    dolt checkout firstbranch
    dolt sql < query
    rm query
    dolt add test
    dolt commit -m "Reconciled with newbranch"

    # confirm that both branches have the same content
    run dolt diff --sql newbranch firstbranch
    [ "$status" -eq 0 ]
    [[ "$output" = "" ]] || false
}

@test "diff sql output reconciles primary key change" {
    dolt checkout -b firstbranch
    dolt table create -s=`batshelper 1pk5col-ints.schema` test
    dolt table import -u test `batshelper 1pk5col-ints.csv`
    dolt add test
    dolt commit -m "Added one initial row"

    dolt checkout -b newbranch
        dolt sql -q 'UPDATE test SET pk=2 WHERE pk=1'
    dolt add test
    dolt commit -m "modified first row"

    # confirm a difference exists
    run dolt diff --sql newbranch firstbranch
    [ "$status" -eq 0 ]
    [[ "$output" != "" ]] || false

    dolt diff --sql newbranch firstbranch > query
    dolt checkout firstbranch
    dolt sql < query
    rm query
    dolt add test
    dolt commit -m "Reconciled with newbranch"

    # confirm that both branches have the same content
    run dolt diff --sql newbranch firstbranch
    [ "$status" -eq 0 ]
    [[ "$output" = "" ]] || false
}

@test "sql diff supports all types" {
    dolt checkout -b firstbranch
    dolt table create -s=`batshelper 1pksupportedtypes.schema` test
    dolt table import -u test `batshelper 1pksupportedtypes.csv`
    dolt add .
    dolt commit -m "create/init table test"

    # for each query file in helper/queries/1pksuppportedtypes/
    # run query on db, create sql diff patch, confirm they're equivalent
    dolt branch newbranch
    for query in delete add update
    do
        dolt checkout newbranch
        dolt sql < $BATS_TEST_DIRNAME/helper/queries/1pksupportedtypes/$query.sql
        dolt add test
        dolt commit -m "applied $query query"

        # confirm a difference exists
        run dolt diff --sql newbranch firstbranch
        [ "$status" -eq 0 ]
        [[ "$output" != "" ]] || false

        dolt diff --sql newbranch firstbranch > patch.sql
        dolt checkout firstbranch
        dolt sql < patch.sql
        rm patch.sql
        dolt add test
        dolt commit -m "Reconciled with newbranch"

        # confirm that both branches have the same content
        run dolt diff --sql newbranch firstbranch
        [ "$status" -eq 0 ]
        [[ "$output" = "" ]] || false
    done
}

@test "sql diff supports multiple primary keys" {
    dolt checkout -b firstbranch
    dolt table create -s=`batshelper 2pk5col-ints.schema` test
    dolt table import -u test `batshelper 2pk5col-ints.csv`
    dolt add .
    dolt commit -m "create/init table test"

    # for each query file in helper/queries/2pk5col-ints/
    # run query on db, create sql diff patch, confirm they're equivalent
    dolt branch newbranch
    for query in delete add update single_pk_update all_pk_update
    do
        dolt checkout newbranch
        dolt sql < $BATS_TEST_DIRNAME/helper/queries/2pk5col-ints/$query.sql
        dolt add test
        dolt diff --sql
        dolt commit -m "applied $query query "

        # confirm a difference exists

        run dolt diff --sql newbranch firstbranch
        [ "$status" -eq 0 ]
        [[ "$output" != "" ]] || false

        dolt diff --sql newbranch firstbranch > patch.sql
        dolt checkout firstbranch
        dolt sql < patch.sql
        rm patch.sql
        dolt add test
        dolt commit -m "Reconciled with newbranch"

        # confirm that both branches have the same content
        run dolt diff --sql newbranch firstbranch
        [ "$status" -eq 0 ]
        [[ "$output" = "" ]] || false
    done
}

@test "sql diff correctly exits on schema change" {
    dolt table import -c -s=`batshelper employees-sch.json` employees `batshelper employees-tbl.json`
    dolt add employees
    dolt commit -m "Added employees table with data"
    dolt schema add-column employees city string
    dolt table put-row employees id:3 "first name":taylor "last name":bantle title:"software engineer" "start date":"" "end date":"" city:"Santa Monica"
    run dolt diff --sql
    [ "$status" -eq 1 ]
    [[ "$output" =~ "SQL output of schema diffs is not yet supported" ]] || false
}