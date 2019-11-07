#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}

@test "diff summary comparing working table to last commit" {
    dolt table create -s=`batshelper 1pk5col-ints.schema` test
    dolt table put-row test pk:0 c1:0 c2:0 c3:0 c4:0 c5:0
    dolt table put-row test pk:1 c1:1 c2:1 c3:1 c4:1 c5:1
    dolt add test
    dolt commit -m "table created"
    dolt table put-row test pk:2 c1:11 c2:0 c3:0 c4:0 c5:0
    dolt table put-row test pk:3 c1:11 c2:0 c3:0 c4:0 c5:0
    run dolt diff --summary
    [ "$status" -eq 0 ]
    [[ "$output" =~ "2 Rows Unmodified (100.00%)" ]] || false
    [[ "$output" =~ "2 Rows Added (100.00%)" ]] || false
    [[ "$output" =~ "0 Rows Deleted (0.00%)" ]] || false
    [[ "$output" =~ "0 Rows Modified (0.00%)" ]] || false
    [[ "$output" =~ "0 Cells Modified (0.00%)" ]] || false
    [[ "$output" =~ "(2 Entries vs 4 Entries)" ]] || false

    dolt add test
    dolt commit -m "added two rows"
    dolt table put-row test pk:0 c1:11 c2:0 c3:0 c4:0 c5:6
    run dolt diff --summary
    [ "$status" -eq 0 ]
    [[ "$output" =~ "3 Rows Unmodified (75.00%)" ]] || false
    [[ "$output" =~ "0 Rows Added (0.00%)" ]] || false
    [[ "$output" =~ "0 Rows Deleted (0.00%)" ]] || false
    [[ "$output" =~ "1 Row Modified (25.00%)" ]] || false
    [[ "$output" =~ "2 Cells Modified (8.33%)" ]] || false
    [[ "$output" =~ "(4 Entries vs 4 Entries)" ]] || false

    dolt add test
    dolt commit -m "modified first row"
    dolt table rm-row test 0
    run dolt diff --summary
    [ "$status" -eq 0 ]
    [[ "$output" =~ "3 Rows Unmodified (75.00%)" ]] || false
    [[ "$output" =~ "0 Rows Added (0.00%)" ]] || false
    [[ "$output" =~ "1 Row Deleted (25.00%)" ]] || false
    [[ "$output" =~ "0 Rows Modified (0.00%)" ]] || false
    [[ "$output" =~ "0 Cells Modified (0.00%)" ]] || false
    [[ "$output" =~ "(4 Entries vs 3 Entries)" ]] || false
}

@test "diff summary comparing row with a deleted column and an added column" {
    dolt table create -s=`batshelper 1pk5col-ints.schema` test
    dolt add test
    dolt commit -m "create table"
    dolt table put-row test pk:0 c1:1 c2:2 c3:3 c4:4 c5:5
    dolt add test
    dolt commit -m "put row"
    dolt table put-row test pk:0 c1:1 c3:3 c4:4 c5:5
    run dolt diff --summary
    [ "$status" -eq 0 ]
    [[ "$output" =~ "0 Rows Unmodified (0.00%)" ]] || false
    [[ "$output" =~ "0 Rows Added (0.00%)" ]] || false
    [[ "$output" =~ "0 Rows Deleted (0.00%)" ]] || false
    [[ "$output" =~ "1 Row Modified (100.00%)" ]] || false
    [[ "$output" =~ "1 Cell Modified (16.67%)" ]] || false
    [[ "$output" =~ "(1 Entry vs 1 Entry)" ]] || false
    dolt add test
    dolt commit -m "row modified"
    dolt table put-row test pk:0 c1:1 c2:2 c3:3 c4:4 c5:5
    run dolt diff --summary
    [ "$status" -eq 0 ]
    [[ "$output" =~ "0 Rows Unmodified (0.00%)" ]] || false
    [[ "$output" =~ "0 Rows Added (0.00%)" ]] || false
    [[ "$output" =~ "0 Rows Deleted (0.00%)" ]] || false
    [[ "$output" =~ "1 Row Modified (100.00%)" ]] || false
    [[ "$output" =~ "1 Cell Modified (16.67%)" ]] || false
    [[ "$output" =~ "(1 Entry vs 1 Entry)" ]] || false
}

@test "diff summary comparing two branches" {
    dolt checkout -b firstbranch
    dolt table create -s=`batshelper 1pk5col-ints.schema` test
    dolt table put-row test pk:0 c1:0 c2:0 c3:0 c4:0 c5:0
    dolt add test
    dolt commit -m "Added one row"
    dolt checkout -b newbranch
    dolt table put-row test pk:1 c1:1 c2:1 c3:1 c4:1 c5:1
    dolt add test
    dolt commit -m "Added another row"
    run dolt diff --summary newbranch firstbranch 
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1 Row Unmodified (100.00%)" ]] || false
    [[ "$output" =~ "1 Row Added (100.00%)" ]] || false
    [[ "$output" =~ "0 Rows Deleted (0.00%)" ]] || false
    [[ "$output" =~ "0 Rows Modified (0.00%)" ]] || false
    [[ "$output" =~ "0 Cells Modified (0.00%)" ]] || false
    [[ "$output" =~ "(1 Entry vs 2 Entries)" ]] || false
}

@test "diff summary shows correct changes after schema change" {
    dolt table import -c -s=`batshelper employees-sch.json` employees `batshelper employees-tbl.json`
    dolt add employees
    dolt commit -m "Added employees table with data"
    dolt schema add-column employees city string
    dolt table put-row employees id:3 "first name":taylor "last name":bantle title:"software engineer" "start date":"" "end date":"" city:"Santa Monica"
    run dolt diff --summary
    [ "$status" -eq 0 ]
    [[ "$output" =~ "3 Rows Unmodified (100.00%)" ]] || false
    [[ "$output" =~ "1 Row Added (33.33%)" ]] || false
    [[ "$output" =~ "0 Rows Deleted (0.00%)" ]] || false
    [[ "$output" =~ "0 Rows Modified (0.00%)" ]] || false
    [[ "$output" =~ "0 Cells Modified (0.00%)" ]] || false
    [[ "$output" =~ "(3 Entries vs 4 Entries)" ]] || false
    dolt table put-row employees id:0 "first name":tim "last name":sehn title:ceo "start date":"2 years ago" "end date":"" city:"Santa Monica"
    run dolt diff --summary
    [ "$status" -eq 0 ]
    [[ "$output" =~ "2 Rows Unmodified (66.67%)" ]] || false
    [[ "$output" =~ "1 Row Added (33.33%)" ]] || false
    [[ "$output" =~ "0 Rows Deleted (0.00%)" ]] || false
    [[ "$output" =~ "1 Row Modified (33.33%)" ]] || false
    [[ "$output" =~ "2 Cells Modified (11.11%)" ]] || false
    [[ "$output" =~ "(3 Entries vs 4 Entries)" ]] || false
}

@test "diff summary gets summaries for all tables with changes" {
    dolt table create -s=`batshelper 1pk5col-ints.schema` test
    dolt table put-row test pk:0 c1:0 c2:0 c3:0 c4:0 c5:0
    dolt table put-row test pk:1 c1:1 c2:1 c3:1 c4:1 c5:1
    dolt table create -s=`batshelper employees-sch.json` employees
    dolt table put-row employees id:0 "first name":tim "last name":sehn title:ceo "start date":"" "end date":""
    dolt add test employees
    dolt commit -m "test tables created"
    dolt table put-row test pk:2 c1:11 c2:0 c3:0 c4:0 c5:0
    dolt table put-row employees id:1 "first name":brian "last name":hendriks title:founder "start date":"" "end date":""
    run dolt diff --summary
    [ "$status" -eq 0 ]
    [[ "$output" =~ "diff --dolt a/test b/test" ]] || false
    [[ "$output" =~ "--- a/test @" ]] || false
    [[ "$output" =~ "+++ b/test @" ]] || false
    [[ "$output" =~ "diff --dolt a/employees b/employees" ]] || false
    [[ "$output" =~ "--- a/employees @" ]] || false
    [[ "$output" =~ "+++ b/employees @" ]] || false
}

@test "full diff comparing working table to last commit" {
    dolt table create -s=`batshelper 1pk5col-ints.schema` test
    dolt table put-row test pk:0 c1:0 c2:0 c3:0 c4:0 c5:0
    dolt table put-row test pk:1 c1:1 c2:1 c3:1 c4:1 c5:1
    dolt add test
    dolt commit -m "table created"
    dolt table put-row test pk:2 c1:11 c2:0 c3:0 c4:0 c5:0
    dolt table put-row test pk:3 c1:11 c2:0 c3:0 c4:0 c5:0
    run dolt diff
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+-----+----+----+----+----+----+----+" ]] || false
    [[ "$output" =~ "|     | pk | c1 | c2 | c3 | c4 | c5 |" ]] || false
    [[ "$output" =~ "+-----+----+----+----+----+----+----+" ]] || false
    [[ "$output" =~ "|  +  | 2  | 11 | 0  | 0  | 0  | 0  |" ]] || false
    [[ "$output" =~ "|  +  | 3  | 11 | 0  | 0  | 0  | 0  |" ]] || false
    [[ "$output" =~ "+-----+----+----+----+----+----+----+" ]] || false

    dolt add test
    dolt commit -m "added two rows"
    dolt table put-row test pk:0 c1:11 c2:0 c3:0 c4:0 c5:6
    run dolt diff
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+-----+----+----+----+----+----+----+" ]] || false
    [[ "$output" =~ "|     | pk | c1 | c2 | c3 | c4 | c5 |" ]] || false
    [[ "$output" =~ "+-----+----+----+----+----+----+----+" ]] || false
    [[ "$output" =~ "|  <  | 0  | 0  | 0  | 0  | 0  | 0  |" ]] || false
    [[ "$output" =~ "|  >  | 0  | 11 | 0  | 0  | 0  | 6  |" ]] || false
    [[ "$output" =~ "+-----+----+----+----+----+----+----+" ]] || false

    dolt add test
    dolt commit -m "modified first row"
    dolt table rm-row test 0
    run dolt diff
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+-----+----+----+----+----+----+----+" ]] || false
    [[ "$output" =~ "|     | pk | c1 | c2 | c3 | c4 | c5 |" ]] || false
    [[ "$output" =~ "+-----+----+----+----+----+----+----+" ]] || false
    [[ "$output" =~ "|  -  | 0  | 11 | 0  | 0  | 0  | 6  |" ]] || false
    [[ "$output" =~ "+-----+----+----+----+----+----+----+" ]] || false
}

@test "diff sql query comparing working table to last commit" {
    skip

    dolt table create -s=`batshelper 1pk5col-ints.schema` test
    dolt table put-row test pk:0 c1:0 c2:0 c3:0 c4:0 c5:0
    dolt table put-row test pk:1 c1:1 c2:1 c3:1 c4:1 c5:1
    dolt add test
    dolt commit -m "table created"
    dolt table put-row test pk:2 c1:11 c2:0 c3:0 c4:0 c5:0
    dolt table put-row test pk:3 c1:11 c2:0 c3:0 c4:0 c5:0
    run dolt diff --sql
    [ "$status" -eq 0 ]
    [[ "$output" =~ "2 Rows Unmodified (100.00%)" ]] || false
    [[ "$output" =~ "2 Rows Added (100.00%)" ]] || false
    [[ "$output" =~ "0 Rows Deleted (0.00%)" ]] || false
    [[ "$output" =~ "0 Rows Modified (0.00%)" ]] || false
    [[ "$output" =~ "0 Cells Modified (0.00%)" ]] || false
    [[ "$output" =~ "(2 Entries vs 4 Entries)" ]] || false

    dolt add test
    dolt commit -m "added two rows"
    dolt table put-row test pk:0 c1:11 c2:0 c3:0 c4:0 c5:6
    run dolt diff --sql
    [ "$status" -eq 0 ]
    [[ "$output" =~ "3 Rows Unmodified (75.00%)" ]] || false
    [[ "$output" =~ "0 Rows Added (0.00%)" ]] || false
    [[ "$output" =~ "0 Rows Deleted (0.00%)" ]] || false
    [[ "$output" =~ "1 Row Modified (25.00%)" ]] || false
    [[ "$output" =~ "2 Cells Modified (8.33%)" ]] || false
    [[ "$output" =~ "(4 Entries vs 4 Entries)" ]] || false

    dolt add test
    dolt commit -m "modified first row"
    dolt table rm-row test 0
    run dolt diff --sql
    [ "$status" -eq 0 ]
    [[ "$output" =~ "3 Rows Unmodified (75.00%)" ]] || false
    [[ "$output" =~ "0 Rows Added (0.00%)" ]] || false
    [[ "$output" =~ "1 Row Deleted (25.00%)" ]] || false
    [[ "$output" =~ "0 Rows Modified (0.00%)" ]] || false
    [[ "$output" =~ "0 Cells Modified (0.00%)" ]] || false
    [[ "$output" =~ "(4 Entries vs 3 Entries)" ]] || false
}

@test "diff sql output reconciles branches" {


    dolt checkout -b firstbranch
    dolt table create -s=`batshelper 1pk5col-ints.schema` test
    dolt table put-row test pk:0 c1:0 c2:0 c3:0 c4:0 c5:0
    dolt add test
    dolt commit -m "Added one row"

    dolt checkout -b newbranch
    dolt table put-row test pk:1 c1:1 c2:1 c3:1 c4:1 c5:1
    dolt table put-row test pk:2 c1:11 c2:0 c3:0 c4:0 c5:0
    dolt table put-row test pk:3 c1:11 c2:0 c3:0 c4:0 c5:0
    dolt add test
    dolt commit -m "Added three rows"

    dolt diff --sql newbranch firstbranch > query
    dolt checkout firstbranch
    dolt sql < query
    rm query
    # dolt sql -q 'INSERT INTO test (pk, c1, c2, c3, c4, c5) VALUES (1, 1, 1, 1, 1, 1)'
    # dolt sql -q 'INSERT INTO test (pk, c1, c2, c3, c4, c5) VALUES (2, 11, 0, 0, 0, 0)'
    # dolt sql -q 'INSERT INTO test (pk, c1, c2, c3, c4, c5) VALUES (3, 11, 0, 0, 0, 0)'
    dolt add test
    dolt commit -m "Reconciled with newbranch"
    run dolt diff newbranch firstbranch
    [ "$status" -eq 0 ]
    [[ "$output" = "" ]] || false


    dolt checkout newbranch
    dolt table put-row test pk:0 c1:11 c2:0 c3:0 c4:0 c5:6
    dolt add test
    dolt commit -m "modified first row"

    dolt diff --sql newbranch firstbranch > query
    dolt checkout firstbranch
    dolt sql < query
    rm query
    # dolt sql -q 'UPDATE test SET c1=11, c5=6 WHERE pk=0'
    dolt add test
    dolt commit -m "Reconciled with newbranch"
    run dolt diff
    [ "$status" -eq 0 ]
    [[ "$output" = "" ]] || false


    dolt checkout newbranch
    dolt table rm-row test 0
    dolt add test
    dolt commit -m "modified first row"

    dolt diff --sql newbranch firstbranch > query
    dolt checkout firstbranch
    dolt sql < query
    rm query
    # dolt sql -q 'DELETE FROM test WHERE pk=0'
    dolt add test
    dolt commit -m "Reconciled with newbranch"
    run dolt diff
    [ "$status" -eq 0 ]
    [[ "$output" = "" ]] || false
}

@test "full diff comparing two branches" {
    dolt checkout -b firstbranch
    dolt table create -s=`batshelper 1pk5col-ints.schema` test
    dolt table put-row test pk:0 c1:0 c2:0 c3:0 c4:0 c5:0
    dolt add test;
    dolt commit -m "Added one row"
    dolt checkout -b newbranch
    dolt table put-row test pk:1 c1:1 c2:1 c3:1 c4:1 c5:1
    dolt add test
    dolt commit -m "Added another row"
    run dolt diff newbranch firstbranch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+-----+----+----+----+----+----+----+" ]] || false
    [[ "$output" =~ "|     | pk | c1 | c2 | c3 | c4 | c5 |" ]] || false
    [[ "$output" =~ "+-----+----+----+----+----+----+----+" ]] || false
    [[ "$output" =~ "|  +  | 1  | 1  | 1  | 1  | 1  | 1  |" ]] || false
    [[ "$output" =~ "+-----+----+----+----+----+----+----+" ]] || false
}

@test "diff sql query comparing two branches" {
    skip

    dolt checkout -b firstbranch
    dolt table create -s=`batshelper 1pk5col-ints.schema` test
    dolt table put-row test pk:0 c1:0 c2:0 c3:0 c4:0 c5:0
    dolt add test;
    dolt commit -m "Added one row"
    dolt checkout -b newbranch
    dolt table put-row test pk:1 c1:1 c2:1 c3:1 c4:1 c5:1
    dolt add test
    dolt commit -m "Added another row"
    run dolt diff --sql newbranch firstbranch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1 Row Unmodified (100.00%)" ]] || false
    [[ "$output" =~ "1 Row Added (100.00%)" ]] || false
    [[ "$output" =~ "0 Rows Deleted (0.00%)" ]] || false
    [[ "$output" =~ "0 Rows Modified (0.00%)" ]] || false
    [[ "$output" =~ "0 Cells Modified (0.00%)" ]] || false
    [[ "$output" =~ "(1 Entry vs 2 Entries)" ]] || false
}