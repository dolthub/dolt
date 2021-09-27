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
    [[ "$output" =~ "workingSets/heads/main" ]] || false
    [[ "$output" =~ "workingSets/heads/to_delete" ]] || false

    dolt checkout main
    dolt branch -d -f to_delete

    root=$(noms root .dolt/noms)
    run noms show .dolt/noms::#$root
    [[ "$show_tables" -eq 0 ]] || false
    [[ ! "$output" =~ "to_delete" ]] || false
}

@test "branch: moving current working branch takes its working set" {
    dolt sql -q 'create table test (id int primary key);'
    dolt branch -m main new_main
    show_tables=`dolt sql -q 'show tables' | wc -l`
    [[ "$show_tables" -eq 5 ]] || false
}
