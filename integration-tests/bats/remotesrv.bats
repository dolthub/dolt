#!/usr/bin/env bats
#
# Tests for remotesrv itself. remotes.bats also uses remotesrv as a testing
# dependency, but it is not technically the unit under test there. This test
# uses dolt remotestorage, clone, pull, fetch, etc., as a dependency but is
# more concerned with covering remotesrv functionality.

load $BATS_TEST_DIRNAME/helper/common.bash

remotesrv_pid=
setup() {
    skiponwindows "tests are flaky on Windows"
    setup_common
}

teardown() {
    teardown_common
    stop_remotesrv
}

stop_remotesrv() {
    if [ -n "$remotesrv_pid" ]; then
        kill $remotesrv_pid || :
    fi
}

@test "remotesrv: can read from remotesrv in repo-mode" {
    mkdir remote
    cd remote
    dolt init
    dolt sql -q 'create table vals (i int);'
    dolt sql -q 'insert into vals (i) values (1), (2), (3), (4), (5);'
    dolt add vals
    dolt commit -m 'initial vals.'

    remotesrv --http-port 1234 --repo-mode &
    remotesrv_pid=$!

    cd ../
    dolt clone http://localhost:50051/test-org/test-repo repo1
    cd repo1
    run dolt ls
    [[ "$output" =~ "vals" ]] || false
    run dolt sql -q 'select count(*) from vals'
    [[ "$output" =~ "5" ]] || false

    stop_remotesrv
    cd ../remote
    dolt sql -q 'insert into vals (i) values (6), (7), (8), (9), (10);'
    dolt commit -am 'add some vals'

    remotesrv --http-port 1234 --repo-mode &
    remotesrv_pid=$!

    cd ../repo1
    dolt pull
    run dolt sql -q 'select count(*) from vals;'
    [[ "$output" =~ "10" ]] || false
}

@test "remotesrv: can write to remotesrv in repo-mode" {
    mkdir remote
    cd remote
    dolt init
    dolt sql -q 'create table vals (i int);'
    dolt add vals
    dolt commit -m 'create vals table.'

    remotesrv --http-port 1234 --repo-mode &
    remotesrv_pid=$!

    cd ../
    dolt clone http://localhost:50051/test-org/test-repo repo1
    cd repo1
    dolt sql -q 'insert into vals values (1), (2), (3), (4), (5);'
    dolt commit -am 'insert some values'
    dolt push origin main:main

    stop_remotesrv
    cd ../remote
    # Have to reset the working set, which was not updated by the push...
    dolt reset --hard
    run dolt sql -q 'select count(*) from vals;'
    [[ "$output" =~ "5" ]] || false
}

@test "remotesrv: read only server rejects writes" {
    mkdir remote
    cd remote
    dolt init
    dolt sql -q 'create table vals (i int);'
    dolt add vals
    dolt commit -m 'create vals table.'

    remotesrv --http-port 1234 --repo-mode -read-only &
    remotesrv_pid=$!

    cd ../
    dolt clone http://localhost:50051/test-org/test-repo repo1
    cd repo1
    dolt sql -q 'insert into vals values (1), (2), (3), (4), (5);'
    dolt commit -am 'insert some values'
    run dolt push origin main:main
    [[ "$status" != 0 ]] || false
}

@test "remotesrv: can run grpc and http on same port" {
    mkdir remote
    cd remote
    dolt init
    dolt sql -q 'create table vals (i int);'
    dolt add vals
    dolt commit -m 'create vals table.'

    remotesrv --grpc-port 1234 --http-port 1234 --repo-mode &
    remotesrv_pid=$!

    cd ../
    dolt clone http://localhost:1234/test-org/test-repo repo1
}
