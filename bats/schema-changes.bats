#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}

@test "changing column types should not produce a data diff error" {
    dolt table import -c --pk=pk test `batshelper 1pk5col-ints.csv`
    run dolt schema show
    [[ "$output" =~ "TEXT" ]] || false
    dolt add test
    dolt commit -m "Added test table"
    dolt table import -c -f -pk=pk -s=`batshelper 1pk5col-ints.schema` test `batshelper 1pk5col-ints.csv`
    run dolt diff
    skip "This produces a failed to merge schemas error message right now"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "BIGINT" ]] || false
    [[ ! "$output" =~ "TEXT" ]] || false
    [[ ! "$ouput" =~ "Failed to merge schemas" ]] || false
}

@test "dolt schema rename column" {
    dolt table create -s=`batshelper 1pk5col-ints.schema` test
    dolt sql -q 'insert into test values (1,1,1,1,1,1)'
    run dolt schema rename-column test c1 c0
    [ "$status" -eq 0 ]
    run dolt schema show test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test @ working" ]] || false
    [[ "$output" =~ "CREATE TABLE \`test\`" ]] || false
    [[ "$output" =~ "\`pk\` BIGINT NOT NULL COMMENT 'tag:0'" ]] || false
    [[ "$output" =~ "\`c2\` BIGINT COMMENT 'tag:2'" ]] || false
    [[ "$output" =~ "\`c3\` BIGINT COMMENT 'tag:3'" ]] || false
    [[ "$output" =~ "\`c4\` BIGINT COMMENT 'tag:4'" ]] || false
    [[ "$output" =~ "\`c5\` BIGINT COMMENT 'tag:5'" ]] || false
    [[ "$output" =~ "PRIMARY KEY (\`pk\`)" ]] || false
    [[ "$output" =~ "\`c0\` BIGINT COMMENT 'tag:1'" ]] || false
    [[ ! "$output" =~ "\`c1\` BIGINT COMMENT 'tag:1'" ]] || false
    dolt table select test
}

@test "dolt schema delete column" {
    dolt table create -s=`batshelper 1pk5col-ints.schema` test
    dolt sql -q 'insert into test values (1,1,1,1,1,1)'
    run dolt schema drop-column test c1
    [ "$status" -eq 0 ]
    run dolt schema show test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test @ working" ]] || false
    [[ "$output" =~ "CREATE TABLE \`test\`" ]] || false
    [[ "$output" =~ "\`pk\` BIGINT NOT NULL COMMENT 'tag:0'" ]] || false
    [[ "$output" =~ "\`c2\` BIGINT COMMENT 'tag:2'" ]] || false
    [[ "$output" =~ "\`c3\` BIGINT COMMENT 'tag:3'" ]] || false
    [[ "$output" =~ "\`c4\` BIGINT COMMENT 'tag:4'" ]] || false
    [[ "$output" =~ "\`c5\` BIGINT COMMENT 'tag:5'" ]] || false
    [[ "$output" =~ "PRIMARY KEY (\`pk\`)" ]] || false
    [[ ! "$output" =~ "\`c1\` BIGINT COMMENT 'tag:1'" ]] || false
    dolt table select test
}

@test "dolt diff on schema changes" {
    dolt table create -s=`batshelper 1pk5col-ints.schema` test
    dolt add test
    dolt commit -m "committed table so we can see diffs"
    dolt schema add-column test c0 int
    run dolt diff
    [ "$status" -eq 0 ]
    [[ "$output" =~ \+[[:space:]]+\`c0\` ]] || false
    [[ "$output" =~ "| c0 |" ]] || false
    run dolt diff --schema
    [ "$status" -eq 0 ]
    [[ "$output" =~ \+[[:space:]]+\`c0\` ]] || false
    [[ ! "$output" =~ "| c0 |" ]] || false
    run dolt diff --data
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ \+[[:space:]]+\`c0\` ]] || false
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
    dolt schema drop-column test c0
    dolt diff
}

@test "change the primary key. view the schema diff" {
    dolt table create -s=`batshelper 1pk5col-ints.schema` test
    dolt add test
    dolt commit -m "committed table so we can see diffs"
    dolt table create -f -s=`batshelper 1pk5col-ints-diff-pk.schema` test
    run dolt diff --schema
    [ "$status" -eq 0 ]
    [[ "$output" =~ "<    PRIMARY KEY (\`pk\`)" ]] || false
    [[ "$output" =~ ">    PRIMARY KEY (\`c1\`)" ]] || false
}

@test "add another primary key. view the schema diff" {
    dolt table create -s=`batshelper 1pk5col-ints.schema` test
    dolt add test
    dolt commit -m "committed table so we can see diffs"
    dolt table create -f -s=`batshelper 1pk5col-ints-add-pk.schema` test
    run dolt diff --schema
    [ "$status" -eq 0 ]
    [[ "$output" =~ "<    PRIMARY KEY (\`pk\`)" ]] || false
    [[ "$output" =~ ">    PRIMARY KEY (\`c1\`, \`c5\`)" ]] || false
}

@test "adding and dropping column should produce no diff" {
    dolt table create -s=`batshelper 1pk5col-ints.schema` test
    dolt add test
    dolt commit -m "committed table so we can see diffs"
    dolt schema add-column test c0 int
    dolt schema drop-column test c0
    run dolt diff
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
}

@test "schema diff should show primary keys in output" {
    dolt table create -s=`batshelper 1pk5col-ints.schema` test
    dolt add test
    dolt commit -m "committed table so we can see diffs"
    dolt table create -f -s=`batshelper 1pk5col-ints-change-col-name.schema` test
    run dolt diff --schema
    [ "$status" -eq 0 ]
    [[ "$output" =~ "PRIMARY KEY" ]] || false
}

@test "add another new primary key column. view the schema diff" {
    dolt table create -s=`batshelper 1pk5col-ints.schema` test
    dolt add test
    dolt commit -m "committed table so we can see diffs"
    dolt table create -f -s=`batshelper 1pk5col-ints-add-col-pk.schema` test
    run dolt diff --schema
    [ "$status" -eq 0 ]
    [[ "$output" =~ "<    PRIMARY KEY (\`pk\`)" ]] || false
    [[ "$output" =~ ">    PRIMARY KEY (\`pk\`, \`c6\`)" ]] || false
}

@test "remove a primary key column. view the schema diff" {
    dolt table create -s=`batshelper 1pk5col-ints.schema` test
    dolt add test
    dolt commit -m "committed table so we can see diffs"
    dolt table create -f -s=`batshelper 1pk5col-ints-rm-pk-col.schema` test
    run dolt diff --schema
    [ "$status" -eq 0 ]
    [[ "$output" =~ "<    PRIMARY KEY (\`pk\`)" ]] || false
    [[ "$output" =~ ">    PRIMARY KEY (\`c3\`)" ]] || false
}
