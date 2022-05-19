#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
     teardown_common
}

# client FeatureVersion must be >= repo FeatureVersion to read
# read with maximum FeatureVersion for assersions
MAX=1000

OLD=10
NEW=20

@test "feature-version: set feature version with CLI flag" {
    dolt --feature-version 19 sql -q "CREATE TABLE test (pk int PRIMARY KEY)"
    run dolt --feature-version $MAX version --feature
    [[ "$output" =~ "feature version: 19" ]] || false
}

@test "feature-version: new client writes to table, locking out old client" {
    run dolt --feature-version $OLD sql <<SQL
CREATE TABLE test (pk int PRIMARY KEY);
INSERT INTO test VALUES (10),(11),(12);
SQL
    [ "$status" -eq 0 ]
    run dolt --feature-version $NEW sql -q "INSERT INTO test VALUES (20),(21),(22);"
    [ "$status" -eq 0 ]

    # old client can't read or write
    run dolt --feature-version $OLD sql -q "SELECT * FROM test"
    [ "$status" -ne 0 ]
    [[ ! "$output" =~ "panic" ]] || false
    run dolt --feature-version $OLD sql -q "INSERT INTO test VALUES (13);"
    [ "$status" -ne 0 ]
    [[ ! "$output" =~ "panic" ]] || false

    run dolt --feature-version $MAX version --feature
    [ "$status" -eq 0 ]
    [[ "$output" =~ "feature version: $NEW" ]] || false
}

setup_remote_tests() {
    skip_nbf_dolt_1
    # remote repo from top-level test directory
    rm -rf .dolt/

    # create a new repo and a remote
    mkdir remote first_repo
    pushd first_repo
    dolt --feature-version $OLD init
    dolt --feature-version $OLD remote add origin file://../remote/

    # add some data and push
    dolt --feature-version $OLD sql <<SQL
CREATE TABLE test (pk int PRIMARY KEY);
INSERT INTO test VALUES (10),(11),(12);
SQL
    dolt --feature-version $OLD add .
    dolt --feature-version $OLD commit -m "test table"
    dolt --feature-version $OLD push origin main
    popd

    # clone repo
    dolt --feature-version $OLD clone file://remote clone_repo
}

@test "feature-version: pulling newer FeatureVersion locks out old client" {
    setup_remote_tests

    pushd first_repo
    dolt --feature-version $NEW sql -q "INSERT INTO test VALUES (20);"
    dolt --feature-version $NEW commit -am "added row with new FeatureVersion on main"
    dolt --feature-version $NEW push origin main
    popd

    pushd clone_repo
    run dolt --feature-version $MAX version --feature
    [ "$status" -eq 0 ]
    [[ "$output" =~ "feature version: $OLD" ]] || false
    # because of autocommit, running this select actually does write
    # a new working root value with the new feature version, so we do this
    # after the above check
    run dolt --feature-version $NEW sql -q "SELECT count(*) FROM test;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "3" ]] || false

    # pull fails with old FeatureVersion
    run dolt --feature-version $OLD pull
    [ "$status" -ne 0 ]

    # pull succeeds with old FeatureVersion
    run dolt --feature-version $NEW pull
    [ "$status" -eq 0 ]

    run dolt --feature-version $NEW sql -q "SELECT count(*) FROM test;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "4" ]] || false
    run dolt --feature-version $MAX version --feature
    [ "$status" -eq 0 ]
    [[ "$output" =~ "feature version: $NEW" ]] || false
}

@test "feature-version: older client maintains access to feature branch" {
    setup_remote_tests

    pushd clone_repo
    dolt --feature-version $OLD checkout -b other
    dolt --feature-version $OLD sql -q "INSERT INTO test VALUES (13);"
    dolt --feature-version $OLD commit -am "made some changes on branch other"
    popd

    pushd first_repo
    dolt --feature-version $NEW sql -q "INSERT INTO test VALUES (20);"
    dolt --feature-version $NEW commit -am "added row with new FeatureVersion on main"
    dolt --feature-version $NEW push origin main
    popd

    pushd clone_repo
    dolt --feature-version $OLD fetch
    dolt --feature-version $OLD checkout main
    run dolt --feature-version $OLD pull
    [ "$status" -ne 0 ]
    [[ "$output" =~ "visit https://github.com/dolthub/dolt/releases/latest/" ]] || false
    dolt --feature-version $OLD sql -q "SELECT * FROM test"
    dolt --feature-version $OLD checkout other
    popd
}
