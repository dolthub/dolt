#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    skiponwindows "tests are flaky on Windows"
    setup_common
    cd $BATS_TMPDIR
    cd dolt-repo-$$
    mkdir "dolt-repo-clones"
}

teardown() {
    teardown_common
}

@test "clone: dolt_clone procedure in empty dir" {
    repoDir="$BATS_TMPDIR/dolt-repo-$$"

    # make directories outside of the dolt repo
    repo1=$(mktemp -d)
    cd $repo1

    # init and populate repo 1
    dolt init
    dolt sql -q "CREATE TABLE test (pk INT PRIMARY KEY);"
    dolt sql -q "INSERT INTO test VALUES (1), (2), (3);"
    dolt sql -q "CREATE PROCEDURE test() SELECT 42;"
    dolt add -A
    dolt commit -m "initial commit"

    # verify data
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1" ]] || false
    [[ "$output" =~ "2" ]] || false
    [[ "$output" =~ "3" ]] || false

    # verify procedure
    run dolt sql -q "call test()"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "42" ]] || false

    # make repo 2 directory outside of the dolt repo
    repo2=$(mktemp -d)
    cd $repo2

    # Clone repo 1 into repo 2
    run dolt sql -q "call dolt_clone('file://$repo1/.dolt/noms', 'repo1');"
    echo "call dolt_clone >>> $output"
    [ "$status" -eq 0 ]

    # verify databases
    run dolt sql -q "show databases;"
    echo "show databases>>> $output"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "repo1" ]] || false

    run dolt sql -q "select database();"
    echo "select database()>>> $output"
    [[ "$output" =~ "repo1" ]] || false

    # verify data
    run dolt sql -q "SELECT * FROM test"
    echo "$output"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1" ]] || false
    [[ "$output" =~ "2" ]] || false
    [[ "$output" =~ "3" ]] || false

    # verify procedure
    run dolt sql -q "call test()"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "42" ]] || false
}
