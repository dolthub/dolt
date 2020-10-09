# Simple smoke tests verifying the AWS remotes work as advertised.

load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}

@test "can add remote with aws url" {
    dolt remote add origin 'membs://test/repo_name'
}

@test "can fetch existing aws remote" {
    dolt remote add origin 'membs://test/repo_name'
    dolt fetch origin
}

@test "fetch with non-existant dynamo table fails" {
    dolt remote add origin 'membs://test/repo_name'
    run dolt fetch origin
    [ "$status" -eq 1 ]
}

@test "fetch with non-existant s3 bucket fails" {
    dolt remote add origin 'membs://test/repo_name'
    run dolt fetch origin
    [ "$status" -eq 1 ]
}

@test "can clone an existing aws remote" {
    rm -rf .dolt
    dolt clone 'membs://test/repo_name'
    cd repo_name
    dolt sql -q 'show tables'
}

# Matches behavior of other remote types
@test "clone empty aws remote fails" {
    rm -rf .dolt
    random_repo=`openssl rand -hex 32`
    run dolt clone 'membs://test/repo_name'
    [ "$status" -eq 1 ]
    [[ "$output" =~ "error: clone failed" ]] || false
    [[ "$output" =~ "cause: remote at that url contains no Dolt data" ]] || false
}

@test "can push to new remote" {
    random_repo=`openssl rand -hex 32`
    dolt remote add origin 'membs://test/repo_name'
    dolt sql -q 'create table a_test_table (id int primary key)'
    dolt sql -q 'insert into a_test_table values (1), (2), (47)'
    dolt add .
    dolt commit -m 'creating a test table'
    dolt push origin master:master
    dolt fetch origin
    dolt push origin :master
    dolt push origin master:another-branch
    dolt push origin :another-branch
}
