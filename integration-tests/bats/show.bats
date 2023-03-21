#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "show: on initialized repo" {
    run dolt show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "Initialize data repository" ]] || false
}

@test "show: log zero refs" {
    dolt commit --allow-empty -m "Commit One"
    dolt tag v1
    run dolt show
    [ $status -eq 0 ]
    [[ "$output" =~ "Commit One" ]] || false
    [[ "$output" =~ "tag: v1" ]] || false

    dolt commit --allow-empty -m "Commit Two"
    dolt tag v2
    run dolt show
    [ $status -eq 0 ]
    [[ "$output" =~ "Commit Two" ]] || false
    [[ "$output" =~ "tag: v2" ]] || false
}

@test "show: log one ref" {
    dolt commit --allow-empty -m "Commit One"
    dolt tag v1

    dolt commit --allow-empty -m "Commit Two"
    dolt tag v2

    run dolt show v1
    [ $status -eq 0 ]
    [[ "$output" =~ "Commit One" ]] || false
    [[ "$output" =~ "tag: v1" ]] || false
}

@test "show: log two refs" {
    dolt commit --allow-empty -m "Commit One"
    dolt tag v1

    dolt commit --allow-empty -m "Commit Two"
    dolt tag v2

    run dolt show v1 v2
    [ $status -eq 0 ]
    [[ "$output" =~ "Commit One" ]] || false
    [[ "$output" =~ "tag: v1" ]] || false
    [[ "$output" =~ "Commit Two" ]] || false
    [[ "$output" =~ "tag: v2" ]] || false
}

@test "show: log and diff" {
    dolt sql -q "create table testtable (pk int PRIMARY KEY)"
    dolt add .
    dolt commit -m "commit: add table"
    
    run dolt show
    [ $status -eq 0 ]
    [[ "$output" =~ "commit: add table" ]] || false
    [[ "$output" =~ "diff --dolt a/testtable b/testtable" ]] || false
    [[ "$output" =~ "added table" ]] || false
    [[ "$output" =~ "+CREATE TABLE \`testtable\` (" ]] || false
    [[ "$output" =~ "+  \`pk\` int NOT NULL," ]] || false
    [[ "$output" =~ "+  PRIMARY KEY (\`pk\`)" ]] || false
    
    dolt sql -q 'insert into testtable values (4)'
    dolt add .
    dolt commit -m "commit: add values"
    
	run dolt show
	[ $status -eq 0 ]
	[[ "$output" =~ "commit: add values" ]] || false
	[[ "$output" =~ "|   | pk |" ]] || false
	[[ "$output" =~ "| + | 4  |" ]] || false
}