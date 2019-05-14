#!/usr/bin/env bats

setup() {
    export PATH=$PATH:~/go/bin
    export NOMS_VERSION_NEXT=1
    cd $BATS_TMPDIR
    mkdir "dolt-repo-$$"
    cd "dolt-repo-$$"
    dolt init
}

teardown() {
    rm -rf "$BATS_TMPDIR/dolt-repo-$$"
}

@test "changing column types should not produce a data diff error" {
    dolt table import -c --pk=pk test $BATS_TEST_DIRNAME/helper/1pk5col-ints.csv
    run dolt schema
    [[ "$output" =~ "varchar" ]] || false
    dolt add test
    dolt commit -m "Added test table"
    dolt table import -c -f -pk=pk -s=$BATS_TEST_DIRNAME/helper/1pk5col-ints.schema test $BATS_TEST_DIRNAME/helper/1pk5col-ints.csv
    run dolt diff
    skip "This produces a failed to merge schemas error message right now"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "int" ]] || false
    [[ ! "$output" =~ "varchar" ]] || false
    [[ ! "$ouput" =~ "Failed to merge schemas" ]] || false
}

@test "dolt schema rename column" {
    dolt table create -s=$BATS_TEST_DIRNAME/helper/1pk5col-ints.schema test
    run dolt sql -q 'insert into test values (1,1,1,1,1,1)'
    [ "$status" -eq 0 ]
    run dolt schema --rename-column test c1 c0
    [ "$status" -eq 0 ]
    run dolt schema test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test @ working" ]] || false
    [[ "$output" =~ "CREATE TABLE test" ]] || false
    [[ "$output" =~ "pk int not null comment 'tag:0'" ]] || false
    [[ "$output" =~ "c2 int comment 'tag:2'" ]] || false
    [[ "$output" =~ "c3 int comment 'tag:3'" ]] || false
    [[ "$output" =~ "c4 int comment 'tag:4'" ]] || false
    [[ "$output" =~ "c5 int comment 'tag:5'" ]] || false
    [[ "$output" =~ "primary key (pk)" ]] || false
    [[ "$output" =~ "c0 int comment 'tag:1'" ]] || false
    [[ ! "$output" =~ "c1 int comment 'tag:1'" ]] || false
    run dolt table select test
    [ "$status" -eq 0 ]
}

@test "dolt schema delete column" {
    dolt table create -s=$BATS_TEST_DIRNAME/helper/1pk5col-ints.schema test
    run dolt sql -q 'insert into test values (1,1,1,1,1,1)'
    [ "$status" -eq 0 ]
    run dolt schema --drop-column test c1
    [ "$status" -eq 0 ]
    run dolt schema test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test @ working" ]] || false
    [[ "$output" =~ "CREATE TABLE test" ]] || false
    [[ "$output" =~ "pk int not null comment 'tag:0'" ]] || false
    [[ "$output" =~ "c2 int comment 'tag:2'" ]] || false
    [[ "$output" =~ "c3 int comment 'tag:3'" ]] || false
    [[ "$output" =~ "c4 int comment 'tag:4'" ]] || false
    [[ "$output" =~ "c5 int comment 'tag:5'" ]] || false
    [[ "$output" =~ "primary key (pk)" ]] || false
    [[ ! "$output" =~ "c1 int comment 'tag:1'" ]] || false
    run dolt table select test
    skip "This panics right now."
    [ "$status" -eq 0 ]
}

@test "dolt diff on schema changes" {
    dolt table create -s=$BATS_TEST_DIRNAME/helper/1pk5col-ints.schema test
    dolt add test
    dolt commit -m "committed table so we can see diffs"
    dolt schema --add-column test c0 int
    run dolt diff
    [ "$status" -eq 0 ]
    [[ "$output" =~ \+[[:space:]]+c0 ]] || false
    [[ "$output" =~ "| c0 |" ]] || false
    run dolt diff --schema
    [ "$status" -eq 0 ]
    [[ "$output" =~ \+[[:space:]]+c0 ]] || false
    [[ ! "$output" =~ "| c0 |" ]] || false
    run dolt diff --data
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ \+[[:space:]]+c0 ]] || false
    [[ "$output" =~ "| c0 |" ]] || false
    [[ "$output" =~ ">" ]] || false
    [[ "$output" =~ "<" ]] || false
    # Check for a blank column in the diff output
    [[ "$output" =~ \|[[:space:]]+\| ]] || false
    dolt sql -q "insert into test (pk,c0,c1,c2,c3,c4,c5) values (0,0,0,0,0,0,0)"
    run dolt diff
    [ "$status" -eq 0 ]
    [[ "$output" =~ \|[[:space:]]+c0[[:space:]]+\| ]] || false
    [[ "$output" =~ \+[[:space:]]+[[:space:]]+\|[[:space:]]+0 ]] || false
    dolt schema --drop-column test c0
    run dolt diff
    skip "This panics right now."
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
}

@test "change the primary key. view the schema diff" {
    dolt table create -s=$BATS_TEST_DIRNAME/helper/1pk5col-ints.schema test
    dolt add test
    dolt commit -m "committed table so we can see diffs"
    dolt table create -f -s=$BATS_TEST_DIRNAME/helper/1pk5col-ints-diff-pk.schema test
    run dolt diff --schema
    [ "$status" -eq 0 ]
    skip "Schema diff output does not handle changing primary keys"
    [[ "$output" =~ "primary key" ]] || false
}

@test "adding and dropping column should produce no diff" {
    dolt table create -s=$BATS_TEST_DIRNAME/helper/1pk5col-ints.schema test
    dolt add test
    dolt commit -m "committed table so we can see diffs"
    dolt schema --add-column test c0 int
    dolt schema --drop-column test c0
    run dolt diff
    skip "This produces a diff when it should not"
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
}