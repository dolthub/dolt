#!/usr/bin/env bats

setup() {
    export PATH=$PATH:~/go/bin
    export NOMS_VERSION_NEXT=1
    cd $BATS_TMPDIR
    mkdir dolt-repo
    cd dolt-repo
    dolt init
    dolt table create -s=$BATS_TEST_DIRNAME/helper/2pk5col.schema test
}

teardown() {
    rm -rf $BATS_TMPDIR/dolt-repo
}

@test "create a table with a schema file and examine repo" {
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "test" ]] || false
    run dolt table select test
    [ "$status" -eq 0 ]
    [ "$output" = "pk1|pk2|c1|c2|c3|c4|c5" ]
    run dolt diff
    [ "$status" -eq 0 ]
    [ "${lines[0]}" = "diff --dolt a/test b/test" ]
    [ "${lines[1]}" = "added table" ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Untracked files" ]] || false
    [[ "$output" =~ new[[:space:]] || falsetable:[[:space:]] || false+test ]] || false
}

@test "add a row to a two primary table using dolt table put-row" {
    dolt add test
    dolt commit -m "added test table"
    run dolt table put-row test pk1:0 pk2:0 c1:1 c2:2 c3:3 c4:4 c5:5
    [ "$status" -eq 0 ]
    [ "$output" = "Successfully put row." ]
    run dolt diff
    [ "$status" -eq 0 ]
    [[ "$output" =~ \+[[:space:]] || false+0[[:space:]] || false+\|[[:space:]] || false+0 ]] || false
}

@test "add a row where one of the primary keys is different, not both" {
    dolt table put-row test pk1:0 pk2:0 c1:1 c2:2 c3:3 c4:4 c5:5
    run dolt table put-row test pk1:0 pk2:1 c1:1 c2:2 c3:3 c4:4 c5:10
    [ "$status" -eq 0 ]
    [ "$output" = "Successfully put row." ]
    run dolt table select test
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
    [[ "$output" =~ "|5" ]] || false
    [[ "$output" =~ "|10" ]] || false
}

@test "overwrite a row with two primary keys" {
    dolt table put-row test pk1:0 pk2:0 c1:1 c2:2 c3:3 c4:4 c5:5
    run dolt table put-row test pk1:0 pk2:0 c1:1 c2:2 c3:3 c4:4 c5:10
    [ "$status" -eq 0 ]
    [ "$output" = "Successfully put row." ]
    run dolt table select test
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ ! "$output" =~ "|5" ]] || false
    [[ "$output" =~ "|10" ]] || false
}
