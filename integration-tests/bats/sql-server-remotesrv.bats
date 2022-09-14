#!/usr/bin/env bats
#
# Tests for sql-server running with --remotesapi-port, and specifically the
# functionality of the remotesapi under sql-server.

load $BATS_TEST_DIRNAME/helper/common.bash

srv_pid=
setup() {
    skiponwindows "tests are flaky on Windows"
    setup_common
}

teardown() {
    teardown_common
    if [ -n "$srv_pid" ]; then
        kill $srv_pid
    fi
}

@test "sql-server-remotesrv: can read from sql-server with --remotesapi-port" {
    mkdir remote
    cd remote
    dolt init
    dolt sql -q 'create table vals (i int);'
    dolt sql -q 'insert into vals (i) values (1), (2), (3), (4), (5);'
    dolt add vals
    dolt commit -m 'initial vals.'

    dolt sql-server --remotesapi-port 50051 &
    srv_pid=$!

    cd ../
    dolt clone http://localhost:50051/ignored_named/remote repo1
    cd repo1
    run dolt ls
    [[ "$output" =~ "vals" ]] || false
    run dolt sql -q 'select count(*) from vals'
    [[ "$output" =~ "5" ]] || false

    dolt sql-client -u root <<SQL
use remote;
insert into vals (i) values (6), (7), (8), (9), (10);
call dolt_commit('-am', 'add some vals');
SQL

    dolt pull
    run dolt sql -q 'select count(*) from vals;'
    [[ "$output" =~ "10" ]] || false
}

@test "sql-server-remotesrv: can access a created database from sql-server with --remotesapi-port" {
    mkdir remote
    cd remote
    dolt init
    dolt sql-server --remotesapi-port 50051 &
    srv_pid=$!
    cd ../

    # By cloning here, we have a near-at-hand way to wait for the server to be ready.
    dolt clone http://localhost:50051/ignored_named/remote cloned_remote

    dolt sql-client -u root <<SQL
create database created;
use created;
create table vals (i int);
insert into vals (i) values (1), (2), (3), (4), (5);
call dolt_add('vals');
call dolt_commit('-m', 'add some vals');
SQL

    dolt clone http://localhost:50051/ignored_named/created cloned_created
    cd cloned_created
    run dolt ls
    [[ "$output" =~ "vals" ]] || false
    run dolt sql -q 'select count(*) from vals'
    [[ "$output" =~ "5" ]] || false
}

@test "sql-server-remotesrv: the remotesapi server rejects writes" {
    mkdir remote
    cd remote
    dolt init
    dolt sql -q 'create table vals (i int);'
    dolt add vals
    dolt commit -m 'create vals table.'

    dolt sql-server --remotesapi-port 50051 &
    srv_pid=$!
    cd ../

    dolt clone http://localhost:50051/test-org/remote remote_cloned

    cd remote_cloned
    dolt sql -q 'insert into vals values (1), (2), (3), (4), (5);'
    dolt commit -am 'insert some values'
    run dolt push origin main:main
    [[ "$status" != 0 ]] || false
}

@test "sql-server-remotesrv: remotesapi listen error stops process" {
    mkdir remote_one
    mkdir remote_two
    cd remote_one
    dolt init
    dolt sql-server --remotesapi-port 50051 &
    srv_pid=$!

    dolt clone http://localhost:50051/test-org/remote_one remote_one_cloned

    cd ../remote_two
    dolt init
    run dolt sql-server --port 3307 --remotesapi-port 50051
    [[ "$status" != 0 ]] || false
}
