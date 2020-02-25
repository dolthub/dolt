#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}

@test "dolt schema show" {
    dolt schema show
    run dolt schema show abc
    [ "$status" -eq 0 ]
    [[ "${lines[0]}" =~ "abc @ working" ]] || false
    [[ "${lines[1]}" =~ "CREATE TABLE \`abc\` (" ]] || false
    [[ "${lines[2]}" =~ "  \`pk\` BIGINT NOT NULL COMMENT 'tag:0'," ]] || false
    [[ "${lines[3]}" =~ "  \`a\` LONGTEXT COMMENT 'tag:100'," ]] || false
    [[ "${lines[4]}" =~ "  \`b\` DOUBLE COMMENT 'tag:101'," ]] || false
    [[ "${lines[5]}" =~ "  \`w\` BIGINT COMMENT 'tag:102'," ]] || false
    [[ "${lines[6]}" =~ "  \`x\` BIGINT COMMENT 'tag:103'," ]] || false
    [[ "${lines[7]}" =~ "  PRIMARY KEY (\`pk\`)" ]] || false
    [[ "${lines[8]}" =~ ");" ]] || false

    run dolt schema show bar
    [ "$status" -eq 0 ]
    [[ "${lines[0]}" =~ "bar @ working" ]] || false
    [[ "${lines[1]}" =~ "CREATE TABLE \`bar\` (" ]] || false
    [[ "${lines[2]}" =~ "  \`pk\` BIGINT NOT NULL COMMENT 'tag:2'," ]] || false
    [[ "${lines[3]}" =~ "  PRIMARY KEY (\`pk\`)" ]] || false
    [[ "${lines[4]}" =~ ");" ]] || false

    run dolt schema show foo
    [ "$status" -eq 0 ]
    [[ "${lines[0]}" =~ "foo @ working" ]] || false
    [[ "${lines[1]}" =~ "CREATE TABLE \`foo\` (" ]] || false
    [[ "${lines[2]}" =~ "  \`pk\` BIGINT NOT NULL COMMENT 'tag:1'," ]] || false
    [[ "${lines[3]}" =~ "  PRIMARY KEY (\`pk\`)" ]] || false
    [[ "${lines[4]}" =~ ");" ]] || false
}