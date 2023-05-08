#!/usr/bin/env bats
#
# Tests for sql-server running with --remotesapi-port, and specifically the
# functionality of the remotesapi under sql-server.

load $BATS_TEST_DIRNAME/helper/common.bash

srv_pid=
srv_two_pid=
setup() {
    skiponwindows "tests are flaky on Windows"
    setup_common
}

teardown() {
    teardown_common
    if [ -n "$srv_pid" ]; then
        kill $srv_pid
    fi
    if [ -n "$srv_two_pid" ]; then
        kill $srv_two_pid
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
    sleep 2

    cd ../
    dolt clone http://localhost:50051/remote repo1
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
    dolt clone http://localhost:50051/remote cloned_remote

    dolt sql-client -u root <<SQL
create database created;
use created;
create table vals (i int);
insert into vals (i) values (1), (2), (3), (4), (5);
call dolt_add('vals');
call dolt_commit('-m', 'add some vals');
SQL

    dolt clone http://localhost:50051/created cloned_created
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

    dolt clone http://localhost:50051/remote remote_cloned

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

    dolt clone http://localhost:50051/remote_one remote_one_cloned

    cd ../remote_two
    dolt init
    run dolt sql-server --port 3307 --remotesapi-port 50051
    [[ "$status" != 0 ]] || false
}

@test "sql-server-remotesrv: a read replica can replicate from a remotesapi running in sql-server" {
    # Set up our primary sql-server which accepts writes.
    mkdir -p primary/db
    cd primary/db
    dolt init
    dolt sql -q 'create table vals (i int);'
    dolt add vals
    dolt commit -m 'create initial vals table'

    dolt sql-server --host 127.0.0.1 --remotesapi-port 50051 &
    srv_pid=$!

    cd ../../
    mkdir -p read_replica
    cd read_replica
    dolt clone http://127.0.0.1:50051/db
    cd db
    dolt sql <<SQL
set @@persist.dolt_read_replica_remote = 'origin';
set @@persist.dolt_replicate_all_heads = 1;
SQL
    cat .dolt/config.json
    dolt sql-server --port 3307 &
    srv_two_pid=$!

    # move CWD to make sure we don't lock ".../read_replica/db"
    mkdir tmp && cd tmp

    dolt sql-client -u root <<SQL
use db;
insert into vals values (1), (2), (3), (4), (5);
call dolt_commit('-am', 'insert 1-5.');
SQL

    run dolt sql-client --port 3307 -u root <<SQL
use db;
select count(*) from vals;
SQL
    [[ "$output" =~ "| 5 " ]] || false
}

@test "sql-server-remotesrv: can read from sql-server with --remotesapi-port with clone/fetch/pull authentication" {
    mkdir remote
    cd remote
    dolt init
    dolt --privilege-file=privs.json sql -q "CREATE USER user IDENTIFIED BY 'pass0'"
    dolt sql -q 'create table vals (i int);'
    dolt sql -q 'insert into vals (i) values (1), (2), (3), (4), (5);'
    dolt add vals
    dolt commit -m 'initial vals.'
    export DOLT_REMOTE_USER="user0"
    export DOLT_REMOTE_PASSWORD="pass0"

    dolt sql-server --port 3307 -u $DOLT_REMOTE_USER  -p $DOLT_REMOTE_PASSWORD --remotesapi-port 50051 &
    srv_pid=$!
    sleep 2 # wait for server to start so we don't lock it out

    cd ../
    dolt clone http://localhost:50051/remote repo1 -u $DOLT_REMOTE_USER
    cd repo1
    run dolt ls
    [[ "$output" =~ "vals" ]] || false
    run dolt sql -q 'select count(*) from vals'
    [[ "$output" =~ "5" ]] || false

    dolt sql-client --port 3307 -u $DOLT_REMOTE_USER  -p $DOLT_REMOTE_PASSWORD <<SQL
use remote;
call dolt_checkout('-b', 'new_branch');
insert into vals (i) values (6), (7), (8), (9), (10);
call dolt_commit('-am', 'add some vals');
SQL

    run dolt branch -v -a
    [ "$status" -eq 0 ]
    [[ "$output" =~ "remotes/origin/main" ]] || false
    [[ ! "$output" =~ "remotes/origin/new_branch" ]] || false

    # No auth fetch
    run dolt fetch
    [[ "$status" != 0 ]] || false
    [[ "$output" =~ "Unauthenticated" ]] || false

    # # With auth fetch
    run dolt fetch -u $DOLT_REMOTE_USER
    [[ "$status" -eq 0 ]] || false

    run dolt branch -v -a
    [ "$status" -eq 0 ]
    [[ "$output" =~ "remotes/origin/main" ]] || false
    [[ "$output" =~ "remotes/origin/new_branch" ]] || false

    run dolt checkout new_branch
    [[ "$status" -eq 0 ]] || false

    dolt sql-client --port 3307 -u $DOLT_REMOTE_USER  -p $DOLT_REMOTE_PASSWORD <<SQL
use remote;
call dolt_checkout('new_branch');
insert into vals (i) values (11);
call dolt_commit('-am', 'add one val');
SQL

    # No auth pull
    run dolt pull
    [[ "$status" != 0 ]] || false
    [[ "$output" =~ "Unauthenticated" ]] || false

    # With auth pull
    run dolt pull -u $DOLT_REMOTE_USER
    [[ "$status" -eq 0 ]] || false
    run dolt sql -q 'select count(*) from vals;'
    [[ "$output" =~ "11" ]] || false
}

@test "sql-server-remotesrv: dolt clone without authentication errors" {
    mkdir remote
    cd remote
    dolt init
    dolt --privilege-file=privs.json sql -q "CREATE USER user0 IDENTIFIED BY 'pass0'"
    dolt sql -q 'create table vals (i int);'
    dolt sql -q 'insert into vals (i) values (1), (2), (3), (4), (5);'
    dolt add vals
    dolt commit -m 'initial vals.'
    export DOLT_REMOTE_USER="user0"
    export DOLT_REMOTE_PASSWORD="pass0"

    dolt sql-server -u $DOLT_REMOTE_USER  -p $DOLT_REMOTE_PASSWORD --remotesapi-port 50051 &
    srv_pid=$!

    cd ../
    run dolt clone http://localhost:50051/remote repo1
    [[ "$status" != 0 ]] || false
    [[ "$output" =~ "Unauthenticated" ]] || false
}

@test "sql-server-remotesrv: dolt clone with incorrect authentication errors" {
    mkdir remote
    cd remote
    dolt init
    dolt --privilege-file=privs.json sql -q "CREATE USER user0 IDENTIFIED BY 'pass0'"
    dolt sql -q 'create table vals (i int);'
    dolt sql -q 'insert into vals (i) values (1), (2), (3), (4), (5);'
    dolt add vals
    dolt commit -m 'initial vals.'
    export DOLT_REMOTE_USER="user0"
    export PASSWORD="pass0"

    dolt sql-server -u $DOLT_REMOTE_USER  -p $PASSWORD --remotesapi-port 50051 &
    srv_pid=$!

    cd ../

    run dolt clone http://localhost:50051/remote repo1 -u $DOLT_REMOTE_USER
    [[ "$status" != 0 ]] || false
    [[ "$output" =~ "must set DOLT_REMOTE_PASSWORD environment variable" ]] || false

    export DOLT_REMOTE_PASSWORD="wrong-password"
    run dolt clone http://localhost:50051/remote repo1 -u $DOLT_REMOTE_USER
    [[ "$status" != 0 ]] || false
    [[ "$output" =~ "Unauthenticated" ]] || false
}
