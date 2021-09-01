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
    dolt checkout -b to_delete

    root=$(noms root .dolt/noms)
    run noms show .dolt/noms::#$root
    [[ "$show_tables" -eq 0 ]] || false
    echo $output
    [[ "$output" =~ "workingSets/heads/master" ]] || false
    [[ "$output" =~ "workingSets/heads/to_delete" ]] || false

    dolt checkout master
    dolt branch -d -f to_delete

    root=$(noms root .dolt/noms)
    run noms show .dolt/noms::#$root
    [[ "$show_tables" -eq 0 ]] || false
    [[ ! "$output" =~ "to_delete" ]] || false
}

@test "branch: moving current working branch takes its working set" {
    dolt sql -q 'create table test (id int primary key);'
    dolt branch -m master new_master
    run dolt sql -r csv <<SQL
set dolt_hide_system_tables = 1;
show tables;
SQL

    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
}
