#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "branch: deleting a branch deletes its working set" {
    dolt gc
    sz=`du -s | awk '{print $1}'`
    dolt checkout -b to_delete
    dolt sql -q 'create table test (id int primary key);'
    values=""
    for i in `seq 0 100`; do
      values="$values""${values:+,}""($i)"
    done
    dolt sql -q 'insert into test values '"$values"';'
    dolt add .
    dolt commit -m 'making a new commit'
    dolt gc
    new_sz=`du -s | awk '{print $1}'`
    dolt checkout master
    dolt branch -d -f to_delete
    num_branches=`dolt branch | wc -l`
    [[ "$num_branches" -eq 1 ]] || fail "expected num_branches to be 1"
    dolt gc
    post_delete_sz=`du -s | awk '{print $1}'`
    echo "$sz $new_sz $post_delete_sz"
    [[ "$post_delete_sz" -eq "$sz" ]] || false
    [[ "$post_delete_sz" -lt "$new_sz" ]] || false
}

@test "branch: moving current working branch takes its working set" {
    dolt sql -q 'create table test (id int primary key);'
    dolt branch -m master new_master
    show_tables=`dolt sql -q 'show tables' | wc -l`
    [[ "$show_tables" -eq 5 ]] || false
}
