#!/usr/bin/env bats

load "$BATS_TEST_DIRNAME"/helper/common.bash

TEST_PREFIX="dolt-tzdata"
TEST_IMAGE="$TEST_PREFIX:bookworm-slim"

setup_file() {
    WORKSPACE_ROOT=$(cd "$BATS_TEST_DIRNAME/../../.." && pwd)
    export WORKSPACE_ROOT

    docker build -f "$BATS_TEST_DIRNAME/tzdataDockerfile" -t "$TEST_IMAGE" "$WORKSPACE_ROOT"
}

teardown_file() {
    docker ps -a --filter "name=$TEST_PREFIX" --format '{{.Names}}' | xargs -r docker rm -f >/dev/null 2>&1 || true
}

csh() {
    docker exec -i "$TEST_CONTAINER" sh -lc "$@"
}

dolt() {
    docker exec -i -w "$DOLT_REPOSITORY" "$TEST_CONTAINER" dolt "$@"
}

setup() {
    export TEST_CONTAINER="${TEST_PREFIX}-$$"
    export DOLT_REPOSITORY="/var/lib/dolt/tzdata-repo"

    docker run -d --name "$TEST_CONTAINER" "$TEST_IMAGE" sh -lc 'sleep infinity' >/dev/null

    csh "set -eu
rm -rf $DOLT_REPOSITORY
mkdir -p $DOLT_REPOSITORY
cd $DOLT_REPOSITORY
dolt config --global --add user.email 'bats@email.fake'
dolt config --global --add user.name 'Bats Tests'
dolt init"
}

#bats test_tags=no_lambda
@test "tzdata: CONVERT_TZ works without timezone database" {
    # See https://pkg.go.dev/time#LoadLocation for IANA database locations checked. Here want to see Dolt always embed
    # the IANA database for environments without it (otherwise NULL is returned).
    run dolt sql -q "SELECT CONVERT_TZ('2023-01-01 12:00:00','UTC','America/New_York') AS iana_ok;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "iana_ok" ]] || false
    [[ "$output" =~ '2023-01-01 07:00:00' ]] || false
    [[ "$output" != *NULL* ]] || false

    run dolt sql -r csv -q "SELECT
  CONVERT_TZ('2007-03-11 2:00:00','US/Eastern','US/Central') AS time1,
  CONVERT_TZ('2007-03-11 3:00:00','US/Eastern','US/Central') AS time2;"
    [[ "$output" =~ "time1,time2" ]] || false
    [[ "$output" =~ "2007-03-11 01:00:00,2007-03-11 02:00:00" ]] || false
}