#!/usr/bin/env bats

# tzdata.bats requires a Docker container in order to test an environment with no Go time zone database information is
# available. This includes the Unix file system information, which we remove in our tzdataDockerfile. You can find all
# locations and properties in the tests below in the documentation links.
load "$BATS_TEST_DIRNAME"/helper/common.bash

TEST_NAME="dolt-tzdata"
TEST_IMAGE="$TEST_NAME:bookworm-slim"

setup_file() {
    WORKSPACE_ROOT=$(cd "$BATS_TEST_DIRNAME/../../.." && pwd)
    export WORKSPACE_ROOT

    docker build -f "$BATS_TEST_DIRNAME/tzdataDockerfile" -t "$TEST_IMAGE" "$WORKSPACE_ROOT"
}

# The 'c' prefixes avoid conflicts with binaries on the local bats runner machine. The normal binaries are still
# available for normal tests for when time zone tables are added.
csh() {
    docker exec -i "$TEST_CONTAINER" sh -lc "$@"
}

cdolt() {
    docker exec -i -w "$DOLT_REPOSITORY" "$TEST_CONTAINER" dolt "$@"
}

dolt1791() {
    docker exec -i -w "$DOLT_REPOSITORY" "$TEST_CONTAINER" dolt1791 "$@"
}

setup() {
    export TEST_CONTAINER="${TEST_NAME}-$$"
    export DOLT_REPOSITORY="/var/lib/dolt/tzdata"

    docker run -d --name "$TEST_CONTAINER" "$TEST_IMAGE" sh -lc 'sleep infinity' >/dev/null

    csh "set -eu
rm -rf $DOLT_REPOSITORY
mkdir -p $DOLT_REPOSITORY
cd $DOLT_REPOSITORY
dolt config --global --add user.email 'bats@email.fake'
dolt config --global --add user.name 'Bats Tests'
dolt init"
}

teardown() {
    docker rm -f "$TEST_CONTAINER" >/dev/null 2>&1
}

# bats test_tags=no_lambda
@test "tzdata: the docker environment has no time zone database" {
    run dolt1791 sql -q "SELECT CONVERT_TZ('2023-01-01 12:00:00','UTC','America/New_York') AS iana_ok;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "iana_ok" ]] || false
    [[ "$output" =~ "NULL" ]] || false
}

# bats test_tags=no_lambda
@test "tzdata: source build works without time zone database" {
    # See https://pkg.go.dev/time#LoadLocation for IANA database locations checked. Here want to see Dolt always embed
    # the IANA database for environments without it (otherwise NULL is returned).
    run cdolt sql -q "SELECT CONVERT_TZ('2023-01-01 12:00:00','UTC','America/New_York') AS iana_ok;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "iana_ok" ]] || false
    [[ "$output" =~ '2023-01-01 07:00:00' ]] || false
    [[ "$output" != *NULL* ]] || false

    run cdolt sql -r csv -q "SELECT
  CONVERT_TZ('2007-03-11 2:00:00','US/Eastern','US/Central') AS time1,
  CONVERT_TZ('2007-03-11 3:00:00','US/Eastern','US/Central') AS time2;"
    [[ "$output" =~ "time1,time2" ]] || false
    [[ "$output" =~ "2007-03-11 01:00:00,2007-03-11 02:00:00" ]] || false
}
