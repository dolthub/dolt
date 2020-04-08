# Simple smoke tests verifying the AWS remotes work as advertised.

load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}

skip_if_no_aws_tests() {
    if [ -z "$DOLT_BATS_AWS_TABLE" -o -z "$DOLT_BATS_AWS_BUCKET" -o -z "$DOLT_BATS_AWS_EXISTING_REPO" ]; then
      skip "skipping aws tests; set DOLT_BATS_AWS_TABLE, DOLT_BATS_AWS_BUCKET and DOLT_BATS_AWS_EXISTING_REPO to run"
    fi
}

@test "can add remote with aws url" {
    dolt remote add origin 'aws://[dynamo_db_table:s3_bucket]/repo_name'
}

@test "can fetch existing aws remote" {
    skip_if_no_aws_tests
    dolt remote add origin 'aws://['"$DOLT_BATS_AWS_TABLE"':'"$DOLT_BATS_AWS_BUCKET"']/'"$DOLT_BATS_AWS_EXISTING_REPO"
    dolt fetch origin
}

@test "fetch with non-existant dynamo table fails" {
    skip_if_no_aws_tests
    dolt remote add origin 'aws://['"this_dynamodb_table_does_not_exist_b612c34f055f4b458"':'"$DOLT_BATS_AWS_BUCKET"']/'"$DOLT_BATS_AWS_EXISTING_REPO"
    run dolt fetch origin
    [ "$status" -eq 1 ]
}

@test "fetch with non-existant s3 bucket fails" {
    skip_if_no_aws_tests
    dolt remote add origin 'aws://['"$DOLT_BATS_AWS_TABLE"':'"this_s3_bucket_does_not_exist_5883eaaa20a4797bb"']/'"$DOLT_BATS_AWS_EXISTING_REPO"
    run dolt fetch origin
    [ "$status" -eq 1 ]
}

@test "can clone an existing aws remote" {
    skip_if_no_aws_tests
    skip "clone does not currently work against aws remotes; use init, remote add, fetch..."
    rm -rf .dolt
    dolt clone 'aws://['"$DOLT_BATS_AWS_TABLE"':'"$DOLT_BATS_AWS_BUCKET"']/'"$DOLT_BATS_AWS_EXISTING_REPO"
    cd "$DOLT_BATS_AWS_EXISTING_REPO"
    dolt sql -q 'show tables'
}

# TODO: Is this what we want?
@test "clone empty aws repository gets new initial repository with configured remote" {
    skip_if_no_aws_tests
    rm -rf .dolt
    random_repo=`openssl rand -hex 32`
    dolt clone 'aws://['"$DOLT_BATS_AWS_TABLE"':'"$DOLT_BATS_AWS_BUCKET"']/'"$random_repo"
    cd "$random_repo"
    run dolt remote -v
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]
    [ "${lines[0]}" == 'origin aws://['"$DOLT_BATS_AWS_TABLE"':'"$DOLT_BATS_AWS_BUCKET"']/'"$random_repo"' ' ]
}

@test "can push to new remote" {
    skip_if_no_aws_tests
    skip "Unskip when #531 lands."
    random_repo=`openssl rand -hex 32`
    dolt remote add origin 'aws://['"$DOLT_BATS_AWS_TABLE"':'"$DOLT_BATS_AWS_BUCKET"']/'"$random_repo"
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
