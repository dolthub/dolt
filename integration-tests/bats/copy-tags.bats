#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    dolt sql <<SQL
CREATE TABLE test1 (
  pk BIGINT NOT NULL,
  c1 BIGINT,
  PRIMARY KEY (pk)
);
INSERT INTO test1 VALUES(1,1);
CREATE TABLE test2 (
  pk BIGINT NOT NULL,
  c1 BIGINT,
  PRIMARY KEY (pk)
);
INSERT INTO test2 VALUES(2,2);
SQL
    dolt commit -Am "initial data"
    dolt branch branch1
}

teardown() {
    teardown_common
}

@test "copy-tags: basic case" {
    # Sanity check that the tags start off the same
    dolt schema tags > main.tags
    dolt checkout branch1
    dolt schema tags > branch1.tags
   diff main.tags branch1.tags

    # Change the tags on the branch1 branch
    dolt checkout branch1
    dolt schema update-tag test1 c1 12345
    dolt schema update-tag test2 c1 54321
    dolt commit -am "manually changing tags"
    dolt schema tags > branch1.tags
    grep "12345" branch1.tags
    grep "54321" branch1.tags
    run diff main.tags branch1.tags
    [ "$status" -ne 0 ]

    # Sync the tags on branch1 from main
    dolt checkout branch1
    dolt schema copy-tags main
    dolt schema tags > branch1.tags
    diff main.tags branch1.tags

    # Assert the expected log message
    run dolt log -n1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Syncing column tags from main branch" ]] || false
}

@test "copy-tags: dirty working set" {
    dolt checkout main
    dolt sql -q "insert into test1 values (3, 3);"

    dolt checkout branch1
    run dolt schema copy-tags main
    [ "$status" -ne 0 ]
    [[ "$output" =~ "current branch's working set is not clean" ]] || false
    [[ "$output" =~ "commit or discard any changes and try again" ]] || false
}

@test "copy-tags: no tag changes needed" {
    # Sanity check that the tags start off the same
    dolt schema tags > main.tags
    dolt checkout branch1
    dolt schema tags > branch1.tags
    diff main.tags branch1.tags

    # Assert that the CLI reports no tag changes are needed
    run dolt schema copy-tags main
    [ "$status" -eq 0 ]
    [[ "$output" =~ "No tag changes needed" ]] || false

    # Assert that no commit was created
    run dolt log -n1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "initial data" ]] || false
}
