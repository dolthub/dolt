#!/usr/bin/env bats

setup() {
    load $BATS_TEST_DIRNAME/helper/common.bash
    export PATH=$PATH:~/go/bin
    export NOMS_VERSION_NEXT=1
    cd $BATS_TMPDIR
    mkdir "dolt-repo-$$"
    cd "dolt-repo-$$"
    dolt init
    dolt table create -s=`batshelper 2pk5col-ints.schema` test
}

teardown() {
    rm -rf "$BATS_TMPDIR/dolt-repo-$$"
}

@test "create a table with a schema file and examine repo" {
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "test" ]] || false
    run dolt table select test
    [ "$status" -eq 0 ]
    [[ "$output" =~ pk1[[:space:]]+\|[[:space:]]+pk2[[:space:]]+\|[[:space:]]+c1[[:space:]]+\|[[:space:]]+c2[[:space:]]+\|[[:space:]]+c3[[:space:]]+\|[[:space:]]+c4[[:space:]]+\|[[:space:]]+c5 ]] || false
    run dolt diff
    [ "$status" -eq 0 ]
    [ "${lines[0]}" = "diff --dolt a/test b/test" ]
    [ "${lines[1]}" = "added table" ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Untracked files" ]] || false
    [[ "$output" =~ "new table:" ]] || false
}

@test "add a row to a two primary table using dolt table put-row" {
    dolt add test
    dolt commit -m "added test table"
    run dolt table put-row test pk1:0 pk2:0 c1:1 c2:2 c3:3 c4:4 c5:5
    [ "$status" -eq 0 ]
    [ "$output" = "Successfully put row." ]
    run dolt diff
    [ "$status" -eq 0 ]
    [[ "$output" =~ \+[[:space:]]+\|[[:space:]]+0[[:space:]]+\|[[:space:]]+0 ]] || false
}

@test "add a row where one of the primary keys is different, not both" {
    dolt table put-row test pk1:0 pk2:0 c1:1 c2:2 c3:3 c4:4 c5:5
    run dolt table put-row test pk1:0 pk2:1 c1:1 c2:2 c3:3 c4:4 c5:10
    [ "$status" -eq 0 ]
    [ "$output" = "Successfully put row." ]
    run dolt table select test
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]
    [[ "$output" =~ \|[[:space:]]+5 ]] || false
    [[ "$output" =~ \|[[:space:]]+10 ]] || false
}

@test "overwrite a row with two primary keys" {
    dolt table put-row test pk1:0 pk2:0 c1:1 c2:2 c3:3 c4:4 c5:5
    run dolt table put-row test pk1:0 pk2:0 c1:1 c2:2 c3:3 c4:4 c5:10
    [ "$status" -eq 0 ]
    [ "$output" = "Successfully put row." ]
    run dolt table select test
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ ! "$output" =~ \|[[:space:]]+5 ]] || false
    [[ "$output" =~ \|[[:space:]]+10 ]] || false
}

@test "interact with a multiple primary key table with sql" {
    run dolt sql -q "insert into test (pk1,pk2,c1,c2,c3,c4,c5) values (0,0,6,6,6,6,6)"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| 1       |" ]] || false
    run dolt sql -q "select * from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "c5" ]] || false
    [[ "$output" =~ "6" ]] || false
    run dolt sql -q "insert into test (pk1,pk2,c1,c2,c3,c4,c5) values (0,1,7,7,7,7,7),(1,0,8,8,8,8,8)"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| 2       |" ]] || false
    run dolt sql -q "select * from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "c5" ]] || false
    [[ "$output" =~ "7" ]] || false
    [[ "$output" =~ "8" ]] || false
    run dolt sql -q "select * from test where pk1=1"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "c5" ]] || false
    [[ "$output" =~ "8" ]] || false
    [[ ! "$output" =~ "6" ]] || false
    run dolt sql -q "insert into test (pk1,pk2,c1,c2,c3,c4,c5) values (0,1,7,7,7,7,7)"
    [ "$status" -eq 1 ]
    [ "$output" = "duplicate primary key given" ] || false
    run dolt sql -q "insert into test (pk1,c1,c2,c3,c4,c5) values (0,6,6,6,6,6)"
    [ "$status" -eq 1 ]
    [ "$output" = "column name 'pk2' is non-nullable but attempted to set default value of null" ] || false
    run dolt sql -q "insert into test (c1,c2,c3,c4,c5) values (6,6,6,6,6)"
    [ "$status" -eq 1 ]
    [ "$output" = "column name 'pk1' is non-nullable but attempted to set default value of null" ] || false
}