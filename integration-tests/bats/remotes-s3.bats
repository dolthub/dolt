# Smoke tests for the s3:// remote scheme, which works against any object store
# implementing the S3 API with conditional writes and needs no DynamoDB table.
#
# The s3-compatible tests start their own MinIO, and skip when the minio binary
# is not on PATH. The AWS S3 tests run against a real bucket and are gated on
# DOLT_BATS_AWS_BUCKET; unlike aws://, they need no DynamoDB table, so that
# bucket is the only thing they require.
#
# Credentials always come from the standard AWS SDK chain and are never taken
# from the url.

load $BATS_TEST_DIRNAME/helper/common.bash
load $BATS_TEST_DIRNAME/helper/s3-common.bash

setup() {
    setup_common
}

teardown() {
    teardown_minio
    teardown_common
}

skip_if_no_aws_tests() {
    if [ -z "$DOLT_BATS_AWS_BUCKET" ]; then
      skip "skipping aws s3 tests; set DOLT_BATS_AWS_BUCKET to run"
    fi
}

# Against AWS the SDK resolves the endpoint from the region, so the url carries
# nothing but the bucket and database.
aws_s3_url() {
    echo "s3://$DOLT_BATS_AWS_BUCKET/$1"
}

seed_repo() {
    dolt sql -q "create table t (a int primary key, b varchar(20))"
    dolt sql -q "insert into t values (1,'one'),(2,'two')"
    dolt add -A
    dolt commit -m "seed"
}

# Push, clone, verify, push again, pull. The second push matters on its own:
# creating a manifest and replacing one take different conditional-write paths,
# If-None-Match versus If-Match.
round_trip() {
    local url="$1"

    seed_repo
    dolt remote add origin "$url"
    dolt push origin main

    cd "$BATS_TMPDIR/dolt-repo-$$"
    dolt clone "$url" cloned
    cd cloned
    run dolt sql -q "select count(*) from t" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "2" ]] || false

    cd "$BATS_TMPDIR/dolt-repo-$$"
    dolt sql -q "insert into t values (3,'three')"
    dolt commit -a -m "third row"
    dolt push origin main

    cd cloned
    dolt pull
    run dolt sql -q "select count(*) from t" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "3" ]] || false
}

@test "remotes-s3: can add remote with s3 url" {
    dolt remote add origin 's3://a_bucket/a_database'
}

@test "remotes-s3: can add remote with a routed s3 url" {
    dolt remote add origin 's3://a_bucket/a_database?endpoint=https://example.com&region=auto&path-style=true'
}

@test "remotes-s3: rejects an unknown url parameter" {
    run dolt remote add origin 's3://a_bucket/a_database?entpoint=https://example.com'
    [ "$status" -ne 0 ]
    [[ "$output" =~ "unknown s3 url parameter" ]] || false
}

@test "remotes-s3: rejects a non-boolean path-style" {
    run dolt remote add origin 's3://a_bucket/a_database?path-style=yes'
    [ "$status" -ne 0 ]
    [[ "$output" =~ "must be true or false" ]] || false
}

@test "remotes-s3: rejects credentials embedded in the url" {
    run dolt remote add origin 's3://AKID:secret@a_bucket/a_database'
    [ "$status" -ne 0 ]
    [[ "$output" =~ "must not embed credentials" ]] || false
}

# bats test_tags=no_lambda
@test "remotes-s3: push clone and pull against minio" {
    setup_minio
    round_trip "$(minio_url `openssl rand -hex 16`)"
}

# bats test_tags=no_lambda
@test "remotes-s3: clone an empty minio remote fails" {
    setup_minio
    rm -rf .dolt
    run dolt clone "$(minio_url `openssl rand -hex 16`)"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "remote at that url contains no Dolt data" ]] || false
}

# bats test_tags=no_lambda
@test "remotes-s3: push to an unreachable endpoint fails" {
    setup_minio
    seed_repo
    dolt remote add origin "s3://$MINIO_BUCKET/`openssl rand -hex 16`?endpoint=http://127.0.0.1:1&path-style=true"
    run dolt push origin main
    [ "$status" -ne 0 ]
}

# bats test_tags=no_lambda
@test "remotes-s3: push clone and pull against aws s3" {
    skip_if_no_aws_tests
    round_trip "$(aws_s3_url `openssl rand -hex 16`)"
}

# bats test_tags=no_lambda
@test "remotes-s3: clone an empty aws s3 remote fails" {
    skip_if_no_aws_tests
    rm -rf .dolt
    run dolt clone "$(aws_s3_url `openssl rand -hex 16`)"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "remote at that url contains no Dolt data" ]] || false
}
