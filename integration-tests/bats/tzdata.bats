#!/usr/bin/env bats

load "$BATS_TEST_DIRNAME"/helper/common.bash

TEST_PREFIX="dolt-tzdata"
TEST_IMAGE="$TEST_PREFIX:bookworm-slim"

setup_file() {
    WORKSPACE_ROOT=$(cd "$BATS_TEST_DIRNAME/../../.." && pwd)
    export WORKSPACE_ROOT

    if ! docker image inspect "$TEST_IMAGE" >/dev/null 2>&1; then
        docker build -f "$BATS_TEST_DIRNAME/TZDataDockerfile" -t "$TEST_IMAGE" "$WORKSPACE_ROOT"
    fi
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
dolt init"
}

teardown() {
    docker ps -a --filter "name=$TEST_PREFIX" --format '{{.Names}}' | xargs -r docker rm -f >/dev/null 2>&1 || true
    teardown_common
}

#bats test_tags=no_lambda
@test "dolt-tzdata: CONVERT_TZ works without timezone data directories" {
    run dolt sql -q "SELECT CONVERT_TZ('2023-01-01 12:00:00','UTC','America/New_York') AS iana_ok;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "iana_ok" ]] || false
    [[ "$output" =~ '2023-01-01 07:00:00' ]] || false
    [[ "$output" != *NULL* ]] || false
}