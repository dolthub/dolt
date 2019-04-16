#!/usr/bin/env bats

setup() {
    export PATH=$PATH:~/go/bin
    export NOMS_VERSION_NEXT=1
    cd $BATS_TMPDIR
    mkdir "dolt-repo-$$"
    cd "dolt-repo-$$"
    dolt init
    dolt table create -s=$BATS_TEST_DIRNAME/helper/1pk5col-strings.schema test
}

teardown() {
    rm -rf "$BATS_TMPDIR/dolt-repo-$$"
}

@test "export a table with a string with commas to csv" {
    run dolt table put-row test pk:tim c1:is c2:super c3:duper c4:rad c5:"a,b,c,d,e"
    [ "$status" -eq 0 ]
    [ "$output" = "Successfully put row." ]
    run dolt table export test export.csv
    [ "$status" -eq 0 ]
    [ "$output" = "Successfully exported data." ]
    skip "dolt doesn't quote strings with the comma delimiter in them"
    grep -E \"a,b,c,d,e\" export.csv
}

@test "dolt sql with string comparison operators" {
    dolt table put-row test pk:tim c1:is c2:super c3:duper c4:rad c5:"fo sho"
    dolt table put-row test pk:zach c1:is c2:super c3:duper c4:not c5:rad
    dolt table put-row test pk:this c1:test c2:is c3:a c4:good c5:test
    run dolt sql -q "select * from test"
    [ "$status" -eq 0 ]
    # All row counts are offset by 3 to account for table printing
    [ "${#lines[@]}" -eq 7 ] 
    run dolt sql -q "select * from test where pk='tim'"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    run dolt sql -q "select * from test where pk>'tim'"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    run dolt sql -q "select * from test where pk>='tim'"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]
    run dolt sql -q "select * from test where pk<>'tim'"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]
    run dolt sql -q "select * from test where pk='bob'"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 4 ]
}

@test "interact with a strings type table with sql" {
    run dolt sql -q "insert into test (pk,c1,c2,c3,c4,c5) values ('tim','is','super','duper','rad','fo sho')"
    [ "$status" -eq 0 ]
    [ "$output" = "Rows inserted: 1" ]
    run dolt sql -q "select * from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "c5" ]] || false
    [[ "$output" =~ "tim" ]] || false
    run dolt sql -q "select pk,c1,c4 from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "c4" ]] || false
    [[ "$output" =~ "tim" ]] || false
    [[ ! "$output" =~ "super" ]] || false
}

@test "insert must use quoted strings" {
    run dolt sql -q "insert into test (pk,c1,c2,c3,c4,c5) values (tim,is,super,duper,rad,'fo sho')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Error parsing SQL" ]] || false
}