#!/usr/bin/env bats
#
# Tests for sql-server running with --remotesapi-port, and specifically the
# functionality of the remotesapi under sql-server.

load $BATS_TEST_DIRNAME/helper/common.bash
load $BATS_TEST_DIRNAME/helper/query-server-common.bash

srv_pid=
srv_two_pid=
setup() {
    skiponwindows "tests are flaky on Windows"
    setup_common
}

teardown() {
    stop_sql_server
    teardown_common
    if [ -n "$srv_pid" ]; then
        kill $srv_pid
    fi
    if [ -n "$srv_two_pid" ]; then
        kill $srv_two_pid
    fi
}

@test "sql-server-remotesrv: can read from sql-server with --remotesapi-port" {
    mkdir -p db/remote
    cd db/remote
    dolt init
    dolt sql -q 'create table vals (i int);'
    dolt sql -q 'insert into vals (i) values (1), (2), (3), (4), (5);'
    dolt add vals
    dolt commit -m 'initial vals.'

    dolt sql-server --remotesapi-port 50051 &
    srv_pid=$!

    cd ../../
    dolt clone http://localhost:50051/remote repo1
    cd repo1
    run dolt ls
    [[ "$output" =~ "vals" ]] || false
    run dolt sql -q 'select count(*) from vals'
    [[ "$output" =~ "5" ]] || false

    dolt -u root --port 3306 --host localhost --no-tls sql -q "
use remote;
insert into vals (i) values (6), (7), (8), (9), (10);
call dolt_commit('-am', 'add some vals');
"

    dolt pull

    run dolt sql -q 'select count(*) from vals;'
    [[ "$output" =~ "10" ]] || false
}

@test "sql-server-remotesrv: can access a created database from sql-server with --remotesapi-port" {
    mkdir -p db/remote
    cd db/remote
    dolt init
    dolt sql-server --remotesapi-port 50051 &
    srv_pid=$!
    cd ../..

    # By cloning here, we have a near-at-hand way to wait for the server to be ready.
    dolt clone http://localhost:50051/remote cloned_remote

    dolt -u root --port 3306 --host localhost --no-tls sql -q "
create database created;
use created;
create table vals (i int);
insert into vals (i) values (1), (2), (3), (4), (5);
call dolt_add('vals');
call dolt_commit('-m', 'add some vals');
"

    dolt clone http://localhost:50051/created cloned_created
    cd cloned_created
    run dolt ls
    [[ "$output" =~ "vals" ]] || false
    run dolt sql -q 'select count(*) from vals'
    [[ "$output" =~ "5" ]] || false
}

@test "sql-server-remotesrv: remotesapi listen error stops process" {
    mkdir -p db_one/remote_one
    mkdir -p db_two/remote_two
    cd db_one/remote_one
    dolt init
    dolt sql-server --remotesapi-port 50051 &
    srv_pid=$!

    cd ../../
    dolt clone http://localhost:50051/remote_one remote_one_cloned

    cd db_two/remote_two
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
    cd ../../

    dolt -u root --port 3306 --host 127.0.0.1 --no-tls sql -q "
use db;
insert into vals values (1), (2), (3), (4), (5);
call dolt_commit('-am', 'insert 1-5.');
"

    run dolt --port 3307 --host 127.0.0.1 --no-tls -u root sql -q "
use db;
select count(*) from vals;
"
    [[ "$output" =~ "| 5 " ]] || false
}

@test "sql-server-remotesrv: clone/fetch/pull from remotesapi port with authentication" {
    mkdir -p db/remote
    cd db/remote
    dolt init
    dolt --privilege-file=privs.json sql -q "CREATE USER user0 IDENTIFIED BY 'pass0'"
    dolt sql -q 'create table vals (i int);'
    dolt sql -q 'insert into vals (i) values (1), (2), (3), (4), (5);'
    dolt add vals
    dolt commit -m 'initial vals.'
    export DOLT_REMOTE_USER="user0"
    export DOLT_REMOTE_PASSWORD="pass0"
    dolt sql -q "CREATE USER user0@'%' identified by 'pass0'; GRANT ALL ON *.* to user0@'%';"

    dolt sql-server --port 3307 --remotesapi-port 50051 &
    srv_pid=$!

    cd ../../
    dolt clone http://localhost:50051/remote repo1 -u $DOLT_REMOTE_USER
    cd repo1
    run dolt ls
    [[ "$output" =~ "vals" ]] || false
    run dolt sql -q 'select count(*) from vals'
    [[ "$output" =~ "5" ]] || false

    dolt --port 3307 --host localhost --no-tls -u $DOLT_REMOTE_USER -p $DOLT_REMOTE_PASSWORD sql -q "
use remote;
call dolt_checkout('-b', 'new_branch');
insert into vals (i) values (6), (7), (8), (9), (10);
call dolt_commit('-am', 'add some vals');
"

    run dolt branch -v -a
    [ "$status" -eq 0 ]
    [[ "$output" =~ "remotes/origin/main" ]] || false
    [[ ! "$output" =~ "remotes/origin/new_branch" ]] || false

    # No auth fetch
    run dolt fetch
    [[ "$status" != 0 ]] || false
    [[ "$output" =~ "Access denied for user 'root'" ]] || false

    # With auth fetch
    run dolt fetch --user $DOLT_REMOTE_USER
    [[ "$status" -eq 0 ]] || false

    run dolt branch -v -a
    [ "$status" -eq 0 ]
    [[ "$output" =~ "remotes/origin/main" ]] || false
    [[ "$output" =~ "remotes/origin/new_branch" ]] || false

    dolt checkout new_branch

    dolt --port 3307 --host localhost --no-tls -u $DOLT_REMOTE_USER -p $DOLT_REMOTE_PASSWORD sql -q "
use remote;
call dolt_checkout('new_branch');
insert into vals (i) values (11);
call dolt_commit('-am', 'add one val');
"

    # No auth pull
    run dolt pull
    [[ "$status" != 0 ]] || false
    [[ "$output" =~ "Access denied for user 'root'" ]] || false

    # With auth pull
    dolt pull --user $DOLT_REMOTE_USER

    run dolt sql -q 'select count(*) from vals;'
    [[ "$output" =~ "11" ]] || false
}

@test "sql-server-remotesrv: dolt_clone from remotesapi port with authentication" {
    mkdir -p db/remote
    cd db/remote
    dolt init
    dolt --privilege-file=privs.json sql -q "CREATE USER user0 IDENTIFIED BY 'pass0'"
    dolt sql -q 'create table vals (i int);'
    dolt sql -q 'insert into vals (i) values (1), (2), (3), (4), (57249);'
    dolt add vals
    dolt commit -m 'initial vals.'
    export DOLT_REMOTE_USER="user0"
    export DOLT_REMOTE_PASSWORD="pass0"
    dolt sql -q "CREATE USER user0@'%' identified by 'pass0'; GRANT ALL ON *.* to user0@'%';"

    dolt sql-server --port 3307 --remotesapi-port 50051 &
    srv_pid=$!

    cd ../../
    mkdir clone
    cd clone

    run dolt sql -q "call dolt_clone(\"--user\",\"$DOLT_REMOTE_USER\",\"http://localhost:50051/remote\", \"repo1\")"

    cd repo1
    run dolt ls
    [[ "$output" =~ "vals" ]] || false
    run dolt sql -q 'select * from vals'
    [[ "$output" =~ "57249" ]] || false
}

@test "sql-server-remotesrv: clone/fetch/pull from remotesapi port with clone_admin authentication" {
    mkdir -p db/remote
    cd db/remote
    dolt init
    dolt sql -q 'create table vals (i int);'
    dolt sql -q 'insert into vals (i) values (1), (2), (3), (4), (5);'
    dolt add vals
    dolt commit -m 'initial vals.'
    dolt sql -q "CREATE USER user0@'%' identified by 'pass0'; GRANT ALL ON *.* to user0@'%';"

    dolt sql-server --port 3307 --remotesapi-port 50051 &
    srv_pid=$!
    sleep 2
    run dolt sql -q "
CREATE USER clone_admin_user@'localhost' IDENTIFIED BY 'pass1';
GRANT CLONE_ADMIN ON *.* TO clone_admin_user@'localhost';
select user from mysql.user;
"
    [ $status -eq 0 ]
    [[ $output =~ user0 ]] || false
    [[ $output =~ clone_admin_user ]] || false

    export DOLT_REMOTE_PASSWORD="pass1"
    cd ../../
    dolt clone http://localhost:50051/remote repo1 -u clone_admin_user
    cd repo1
    run dolt ls
    [[ "$output" =~ "vals" ]] || false
    run dolt sql -q 'select count(*) from vals'
    [[ "$output" =~ "5" ]] || false

    dolt --port 3307 --host localhost -u user0 -p pass0 --no-tls --use-db remote sql -q "
call dolt_checkout('-b', 'new_branch');
insert into vals (i) values (6), (7), (8), (9), (10);
call dolt_commit('-am', 'add some vals');"

    run dolt branch -v -a
    [ "$status" -eq 0 ]
    [[ "$output" =~ "remotes/origin/main" ]] || false
    [[ ! "$output" =~ "remotes/origin/new_branch" ]] || false

    # No auth fetch
    run dolt fetch
    [[ "$status" != 0 ]] || false
    [[ "$output" =~ "Access denied for user 'root'" ]] || false

    # # With auth fetch
    run dolt fetch --user clone_admin_user
    [[ "$status" -eq 0 ]] || false

    run dolt branch -v -a
    [ "$status" -eq 0 ]
    [[ "$output" =~ "remotes/origin/main" ]] || false
    [[ "$output" =~ "remotes/origin/new_branch" ]] || false

    run dolt checkout new_branch
    [[ "$status" -eq 0 ]] || false

    dolt sql -q "
call dolt_checkout('new_branch');
insert into vals (i) values (11);
call dolt_commit('-am', 'add one val');"

    # No auth pull
    run dolt pull
    [[ "$status" != 0 ]] || false
    [[ "$output" =~ "Access denied for user 'root'" ]] || false

    # With auth pull
    run dolt pull --user clone_admin_user
    [[ "$status" -eq 0 ]] || false
    run dolt sql -q 'select count(*) from vals;'
    [[ "$output" =~ "11" ]] || false
}

@test "sql-server-remotesrv: dolt clone without authentication returns error" {
    mkdir -p db/remote
    cd db/remote
    dolt init
    dolt --privilege-file=privs.json sql -q "CREATE USER user0 IDENTIFIED BY 'pass0'"
    dolt sql -q 'create table vals (i int);'
    dolt sql -q 'insert into vals (i) values (1), (2), (3), (4), (5);'
    dolt add vals
    dolt commit -m 'initial vals.'
    export DOLT_REMOTE_USER="user0"
    export DOLT_REMOTE_PASSWORD="pass0"
    dolt sql -q "CREATE USER user0@'%' identified by 'pass0'; GRANT ALL ON *.* to user0@'%';"

    dolt sql-server --remotesapi-port 50051 &
    srv_pid=$!

    cd ../../
    run dolt clone http://localhost:50051/remote repo1
    [[ "$status" != 0 ]] || false
    [[ "$output" =~ "Access denied for user 'root'" ]] || false
}

@test "sql-server-remotesrv: dolt clone with incorrect authentication returns error" {
    mkdir -p db/remote
    cd db/remote
    dolt init
    dolt --privilege-file=privs.json sql -q "CREATE USER user0 IDENTIFIED BY 'pass0'"
    dolt sql -q 'create table vals (i int);'
    dolt sql -q 'insert into vals (i) values (1), (2), (3), (4), (5);'
    dolt add vals
    dolt commit -m 'initial vals.'
    export DOLT_REMOTE_USER="user0"
    export PASSWORD="pass0"

    dolt sql-server --remotesapi-port 50051 &
    srv_pid=$!

    cd ../../

    run dolt clone http://localhost:50051/remote repo1 -u $DOLT_REMOTE_USER
    [[ "$status" != 0 ]] || false
    [[ "$output" =~ "must set DOLT_REMOTE_PASSWORD environment variable" ]] || false

    export DOLT_REMOTE_PASSWORD="wrong-password"
    run dolt clone http://localhost:50051/remote repo1 -u $DOLT_REMOTE_USER
    [[ "$status" != 0 ]] || false
    [[ "$output" =~ "Access denied for user 'user0'" ]] || false

    export DOLT_REMOTE_PASSWORD="pass0"
    run dolt clone http://localhost:50051/remote repo1 -u doesnt_exist
    [[ "$status" != 0 ]] || false
    [[ "$output" =~ "Access denied for user 'doesnt_exist'" ]] || false
}

@test "sql-server-remotesrv: push to remotesapi port as super user" {
    mkdir remote
    cd remote
    dolt init
    dolt sql -q 'create table names (name varchar(10) primary key);'
    dolt sql -q 'insert into names (name) values ("abe"), ("betsy"), ("calvin");'
    dolt add names
    dolt commit -m 'initial names.'

    APIPORT=$( definePORT )
    dolt sql -q "CREATE USER root@'%' identified by 'rootpass'; GRANT ALL ON *.* to root@'%';"
    export DOLT_REMOTE_PASSWORD="rootpass"
    export SQL_USER="root"
    start_sql_server_with_args --remotesapi-port $APIPORT

    cd ../
    dolt clone http://localhost:$APIPORT/remote cloned_db -u root
    cd cloned_db

    dolt sql -q 'insert into names values ("dave");'
    dolt commit -am 'add dave'

    run dolt push origin --user $SQL_USER main:main
    [[ "$status" -eq 0 ]] || false

    run dolt sql -q 'select * from names;'
    [[ "$output" =~ "abe" ]] || false
    [[ "$output" =~ "betsy" ]] || false
    [[ "$output" =~ "calvin" ]] || false
    [[ "$output" =~ "dave" ]] || false
}

@test "sql-server-remotesrv: push to dirty workspace as super user" {
    mkdir remote
    cd remote
    dolt init
    dolt sql -q 'create table names (name varchar(10) primary key);'
    dolt sql -q 'insert into names (name) values ("abe"), ("betsy"), ("calvin");'
    dolt add names
    dolt commit -m 'initial names.'
    dolt sql -q 'insert into names (name) values ("zeek");' # dirty the workspace. This won't be cloned

    APIPORT=$( definePORT )
    dolt sql -q "CREATE USER root@'%' identified by 'rootpass'; GRANT ALL ON *.* to root@'%';"
    export DOLT_REMOTE_PASSWORD="rootpass"
    export SQL_USER="root"
    start_sql_server_with_args --remotesapi-port $APIPORT

    cd ../
    dolt clone http://localhost:$APIPORT/remote cloned_db -u root
    cd cloned_db

    dolt sql -q 'insert into names values ("dave");'
    dolt commit -am 'add dave'

    run dolt push origin --user $SQL_USER main:main
    [[ "$status" -ne 0 ]] || false
    [[ "$output" =~ "target has uncommitted changes. --force required to overwrite" ]] || false

    cd ../remote
    run dolt sql -q 'select * from names;'
    [[ "$output" =~ "abe" ]] || false
    [[ "$output" =~ "betsy" ]] || false
    [[ "$output" =~ "calvin" ]] || false
    ! [[ "$output" =~ "dave" ]] || false
    [[ "$output" =~ "zeek" ]] || false

    ## Now try with --force
    cd ../cloned_db
    dolt push origin --force --user $SQL_USER main:main

    cd ../remote
    run dolt sql -q 'select * from names;'
    [[ "$output" =~ "abe" ]] || false
    [[ "$output" =~ "betsy" ]] || false
    [[ "$output" =~ "calvin" ]] || false
    [[ "$output" =~ "dave" ]] || false
    ! [[ "$output" =~ "zeek" ]] || false
}


@test "sql-server-remotesrv: push to remotesapi port as super user non-fast-forward" {
    mkdir remote
    cd remote
    dolt init
    dolt sql -q 'create table names (name varchar(10) primary key);'
    dolt sql -q 'insert into names (name) values ("abe"), ("betsy"), ("calvin");'
    dolt add names
    dolt commit -m 'initial names.'

    APIPORT=$( definePORT )
    dolt sql -q "CREATE USER root@'%' identified by 'rootpass'; GRANT ALL ON *.* to root@'%';"
    export DOLT_REMOTE_PASSWORD="rootpass"
    export SQL_USER="root"
    start_sql_server_with_args --remotesapi-port $APIPORT

    cd ../
    dolt clone http://localhost:$APIPORT/remote cloned_db -u root

    cd remote
    dolt sql -q 'insert into names (name) values ("zeek");' # dirty the workspace. This won't be cloned
    dolt commit -a -m 'add Zeek.'

    cd ../cloned_db
    dolt sql -q 'insert into names values ("dave");'
    dolt commit -am 'add dave'

    run dolt push origin --user $SQL_USER main:main
    [[ "$status" -ne 0 ]] || false
    [[ "$output" =~ "Updates were rejected because the tip of your current branch is behind" ]] || false

    cd ../remote
    run dolt sql -q 'select * from names;'
    [[ "$output" =~ "abe" ]] || false
    [[ "$output" =~ "betsy" ]] || false
    [[ "$output" =~ "calvin" ]] || false
    ! [[ "$output" =~ "dave" ]] || false
    [[ "$output" =~ "zeek" ]] || false

    ## Now try with --force
    cd ../cloned_db
    run dolt push origin --force --user $SQL_USER main:main
    [[ "$status" -eq 0 ]] || false

    cd ../remote
    run dolt sql -q 'select * from names;'
    [[ "$output" =~ "abe" ]] || false
    [[ "$output" =~ "betsy" ]] || false
    [[ "$output" =~ "calvin" ]] || false
    [[ "$output" =~ "dave" ]] || false
    ! [[ "$output" =~ "zeek" ]] || false
}

@test "sql-server-remotesrv: push to remoteapi port as non-super user rejected" {
    mkdir remote
    cd remote
    dolt init
    dolt sql -q 'create table names (name varchar(10) primary key);'
    dolt sql -q 'insert into names (name) values ("abe"), ("betsy"), ("calvin");'
    dolt add names
    dolt commit -m 'initial names.'

    APIPORT=$( definePORT )
    dolt sql -q "CREATE USER root@'%' identified by 'rootpass'; GRANT ALL ON *.* to root@'%';"
    export DOLT_REMOTE_PASSWORD="rootpass"
    export SQL_USER="root"
    start_sql_server_with_args --remotesapi-port $APIPORT

    dolt sql -q "
CREATE USER clone_admin_user@'localhost' IDENTIFIED BY 'pass1';
GRANT CLONE_ADMIN ON *.* TO clone_admin_user@'localhost';
"
    export DOLT_REMOTE_PASSWORD="pass1"
    unset SQL_USER

    cd ../
    dolt clone --user clone_admin_user http://localhost:$APIPORT/remote cloned_db
    cd cloned_db

    dolt sql -q 'insert into names values ("dave");'
    dolt commit -am 'add dave'

    run dolt push origin --user clone_admin_user main:main
    [[ "$status" -ne 0 ]] || false
    [[ "$output" =~ "clone_admin_user has not been granted SuperUser access" ]] || false

    # Give that user superpowers.
    cd ../remote
    dolt sql -q "GRANT ALL PRIVILEGES ON *.* TO 'clone_admin_user'@'localhost' WITH GRANT OPTION"
    cd ../cloned_db

    dolt push origin --user clone_admin_user main:main
}

@test "sql-server-remotesrv: push to remotesapi fails when server is read only" {
    mkdir remote
    cd remote
    dolt init
    dolt sql -q 'create table names (name varchar(10) primary key);'
    dolt sql -q 'insert into names (name) values ("abe"), ("betsy"), ("calvin");'
    dolt add names
    dolt commit -m 'initial names.'

    APIPORT=$( definePORT )
    dolt sql -q "CREATE USER root@'%' identified by 'rootpass'; GRANT ALL ON *.* to root@'%';"
    export DOLT_REMOTE_PASSWORD="rootpass"
    export SQL_USER="root"
    start_sql_server_with_args --remotesapi-port $APIPORT --remotesapi-readonly

    cd ../
    dolt clone http://localhost:$APIPORT/remote cloned_db -u "$SQL_USER"
    cd cloned_db

    dolt sql -q 'insert into names values ("dave");'
    dolt commit -am 'add dave'

    run dolt push origin --user "$SQL_USER" main:main
    [[ "$status" -ne 0 ]] || false
    [[ "$output" =~ "this server only provides read-only access" ]] || false
}

@test "sql-server-remotesrv: delete remote branch from remotesapi port as super user" {
    mkdir remote
    cd remote
    dolt init
    dolt sql -q 'create table names (name varchar(10) primary key);'
    dolt sql -q 'insert into names (name) values ("abe"), ("betsy"), ("calvin");'
    dolt add names
    dolt commit -m 'initial names.'
    dolt branch new_branch HEAD

    APIPORT=$( definePORT )
    dolt sql -q "CREATE USER root@'%' identified by 'rootpass'; GRANT ALL ON *.* to root@'%';"
    export DOLT_REMOTE_PASSWORD="rootpass"
    export SQL_USER="root"
    start_sql_server_with_args --remotesapi-port $APIPORT

    cd ../
    dolt clone http://localhost:$APIPORT/remote cloned_db -u $SQL_USER
    cd cloned_db

    run dolt push origin --user $SQL_USER :new_branch
    [[ "$status" -eq 0 ]] || false
    [[ "$output" =~ "- [deleted]             new_branch" ]] || false

    cd ../remote
    run dolt branch -a
    ! [[ "$output" =~ "new_branch" ]] || false
    [[ "$output" =~ "main" ]] || false
}

@test "sql-server-remotesrv: delete remote dirty branch from remotesapi requires force" {
    mkdir remote
    cd remote
    dolt init
    dolt sql -q 'create table names (name varchar(10) primary key);'
    dolt sql -q 'insert into names (name) values ("abe"), ("betsy"), ("calvin");'
    dolt add names
    dolt commit -m 'initial names.'
    dolt branch new_branch HEAD
    dolt --use-db=remote/new_branch sql -q 'insert into names (name) values ("zeek");' # dirty the workspace

    APIPORT=$(definePORT)
    dolt sql -q "CREATE USER root@'%' identified by 'rootpass'; GRANT ALL ON *.* to root@'%';"
    export DOLT_REMOTE_PASSWORD="rootpass"
    export SQL_USER="root"
    start_sql_server_with_args --remotesapi-port $APIPORT

    cd ../
    dolt clone http://localhost:$APIPORT/remote cloned_db -u $SQL_USER
    cd cloned_db

    run dolt push origin --user $SQL_USER :new_branch
    [[ "$status" -ne 0 ]] || false
    [[ "$output" =~ "target has uncommitted changes. --force required to overwrite" ]] || false

    run dolt push origin --force --user $SQL_USER :new_branch
    [[ "$status" -eq 0 ]] || false
    [[ "$output" =~ "- [deleted]             new_branch" ]] || false

    cd ../remote
    run dolt branch -a
    ! [[ "$output" =~ "new_branch" ]] || false
    [[ "$output" =~ "main" ]] || false
}

@test "sql-server-remotesrv: push to non-existent database fails" {
    mkdir remote
    cd remote
    dolt init
    dolt sql -q 'create table names (name varchar(10) primary key);'
    dolt sql -q 'insert into names (name) values ("abe"), ("betsy"), ("calvin");'
    dolt add names
    dolt commit -m 'initial names.'

    APIPORT=$(definePORT)
    dolt sql -q "CREATE USER root@'%' identified by 'rootpass'; GRANT ALL ON *.* to root@'%';"
    export DOLT_REMOTE_PASSWORD="rootpass"
    export SQL_USER="root"
    start_sql_server_with_args --remotesapi-port $APIPORT

    cd ../
    dolt clone http://localhost:$APIPORT/remote cloned_db -u $SQL_USER
    cd cloned_db

    dolt remote add nodb http://localhost:$APIPORT/nodb

    run dolt push nodb --user $SQL_USER main:new_branch
    [[ "$status" -ne 0 ]] || false
    [[ "$output" =~ "database not found: nodb" ]] || false

    run dolt push --force nodb --user $SQL_USER main:new_branch
    [[ "$status" -ne 0 ]] || false
    [[ "$output" =~ "database not found: nodb" ]] || false
}

@test "sql-server-remotesrv: create remote branch from remotesapi port as super user" {
    mkdir remote
    cd remote
    dolt init
    dolt sql -q 'create table names (name varchar(10) primary key);'
    dolt sql -q 'insert into names (name) values ("abe"), ("betsy"), ("calvin");'
    dolt add names
    dolt commit -m 'initial names.'

    APIPORT=$( definePORT )
    dolt sql -q "CREATE USER root@'%' identified by 'rootpass'; GRANT ALL ON *.* to root@'%';"
    export DOLT_REMOTE_PASSWORD="rootpass"
    export SQL_USER="root"
    start_sql_server_with_args --remotesapi-port $APIPORT

    cd ../
    dolt clone http://localhost:$APIPORT/remote cloned_db -u $SQL_USER
    cd cloned_db

    run dolt push origin --user $SQL_USER main:new_branch
    [[ "$status" -eq 0 ]] || false
    [[ "$output" =~ "* [new branch]          main -> new_branch" ]] || false

    cd ../remote
    run dolt branch -a
    [[ "$output" =~ "new_branch" ]] || false
    [[ "$output" =~ "main" ]] || false
}

