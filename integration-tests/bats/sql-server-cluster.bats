#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash
load $BATS_TEST_DIRNAME/helper/query-server-common.bash

make_repo() {
  mkdir -p "$1"
  (cd "$1"; dolt init)
}

SERVERONE_MYSQL_PORT=3309
SERVERONE_GRPC_PORT=50051

SERVERTWO_MYSQL_PORT=3310
SERVERTWO_GRPC_PORT=50052

setup() {
    skiponwindows "tests are flaky on Windows"
    setup_no_dolt_init

    make_repo serverone/repo1
    make_repo serverone/repo2

    make_repo servertwo/repo1
    make_repo servertwo/repo2
}

teardown() {
    stop_sql_server "1"
    teardown_common
}

@test "sql-server-cluster: persisted role and epoch take precedence over bootstrap values" {
    cd serverone

    echo "
user:
  name: dolt
listener:
  host: 0.0.0.0
  port: ${SERVERONE_MYSQL_PORT}
behavior:
  read_only: false
  autocommit: true
cluster:
  standby_remotes:
  - name: standby
    remote_url_template: http://localhost:${SERVERTWO_GRPC_PORT}/{database}
  bootstrap_role: standby
  bootstrap_epoch: 10
  remotesapi:
    port: ${SERVERONE_GRPC_PORT}" > server.yaml

    (cd repo1 && dolt remote add standby http://localhost:"${SERVERTWO_GRPC_PORT}"/repo1)
    (cd repo2 && dolt remote add standby http://localhost:"${SERVERTWO_GRPC_PORT}"/repo2)
    dolt sql-server --config server.yaml &
    SERVER_PID=$!
    wait_for_connection "${SERVERONE_MYSQL_PORT}" 5000

    server_query_with_port "${SERVERONE_MYSQL_PORT}" dolt_cluster 1 dolt "" "select @@GLOBAL.dolt_cluster_role, @@GLOBAL.dolt_cluster_role_epoch;select "'`database`'", standby_remote, role, epoch from dolt_cluster_status order by "'`database`'" asc" "@@GLOBAL.dolt_cluster_role,@@GLOBAL.dolt_cluster_role_epoch\nstandby,10;database,standby_remote,role,epoch\nrepo1,standby,standby,10\nrepo2,standby,standby,10"

    kill $SERVER_PID
    wait $SERVER_PID
    SERVER_PID=

    echo "
user:
  name: dolt
listener:
  host: 0.0.0.0
  port: ${SERVERONE_MYSQL_PORT}
behavior:
  read_only: false
  autocommit: true
cluster:
  standby_remotes:
  - name: standby
    remote_url_template: http://localhost:${SERVERTWO_GRPC_PORT}/{database}
  bootstrap_role: primary
  bootstrap_epoch: 0
  remotesapi:
    port: ${SERVERONE_GRPC_PORT}" > server.yaml

    dolt sql-server --config server.yaml &
    SERVER_PID=$!
    wait_for_connection "${SERVERONE_MYSQL_PORT}" 5000

    server_query_with_port "${SERVERONE_MYSQL_PORT}" repo1 1 dolt "" "select @@GLOBAL.dolt_cluster_role, @@GLOBAL.dolt_cluster_role_epoch" "@@GLOBAL.dolt_cluster_role,@@GLOBAL.dolt_cluster_role_epoch\nstandby,10"
}

@test "sql-server-cluster: dolt_assume_cluster_role" {
    cd serverone

    echo "
user:
  name: dolt
listener:
  host: 0.0.0.0
  port: ${SERVERONE_MYSQL_PORT}
behavior:
  read_only: false
  autocommit: true
cluster:
  standby_remotes:
  - name: standby
    remote_url_template: http://localhost:${SERVERTWO_GRPC_PORT}/{database}
  bootstrap_role: standby
  bootstrap_epoch: 10
  remotesapi:
    port: ${SERVERONE_GRPC_PORT}" > server.yaml

    (cd repo1 && dolt remote add standby http://localhost:"${SERVERTWO_GRPC_PORT}"/repo1)
    (cd repo2 && dolt remote add standby http://localhost:"${SERVERTWO_GRPC_PORT}"/repo2)
    dolt sql-server --config server.yaml &
    SERVER_PID=$!
    wait_for_connection "${SERVERONE_MYSQL_PORT}" 5000

    # stale epoch
    run server_query_with_port "${SERVERONE_MYSQL_PORT}" repo1 1 dolt "" "call dolt_assume_cluster_role('standby', '9');" "" 1
    [[ "$output" =~ "error assuming role" ]] || false
    # wrong role at current epoch
    run server_query_with_port "${SERVERONE_MYSQL_PORT}" repo1 1 dolt "" "call dolt_assume_cluster_role('primary', '10');" "" 1
    [[ "$output" =~ "error assuming role" ]] || false
    # wrong role name
    run server_query_with_port "${SERVERONE_MYSQL_PORT}" repo1 1 dolt "" "call dolt_assume_cluster_role('backup', '11');" "" 1
    [[ "$output" =~ "error assuming role" ]] || false

    # successes

    # same role, same epoch
    server_query_with_port "${SERVERONE_MYSQL_PORT}" repo1 1 dolt "" "call dolt_assume_cluster_role('standby', '10');" "status\n0"
    # same role, new epoch
    server_query_with_port "${SERVERONE_MYSQL_PORT}" repo1 1 dolt "" "call dolt_assume_cluster_role('standby', '12'); select @@GLOBAL.dolt_cluster_role, @@GLOBAL.dolt_cluster_role_epoch;" "status\n0;@@GLOBAL.dolt_cluster_role,@@GLOBAL.dolt_cluster_role_epoch\nstandby,12"
    # new role, new epoch (this can drop the connection, so check the results in a new connection)
    run server_query_with_port "${SERVERONE_MYSQL_PORT}" repo1 1 dolt "" "call dolt_assume_cluster_role('primary', '13');" "" 1
    server_query_with_port "${SERVERONE_MYSQL_PORT}" repo1 1 dolt "" "select @@GLOBAL.dolt_cluster_role, @@GLOBAL.dolt_cluster_role_epoch;" "@@GLOBAL.dolt_cluster_role,@@GLOBAL.dolt_cluster_role_epoch\nprimary,13"

    # Server comes back up with latest assumed role.
    kill $SERVER_PID
    wait $SERVER_PID
    SERVER_PID=
    dolt sql-server --config server.yaml &
    SERVER_PID=$!
    wait_for_connection "${SERVERONE_MYSQL_PORT}" 5000

    server_query_with_port "${SERVERONE_MYSQL_PORT}" repo1 1 dolt "" "select @@GLOBAL.dolt_cluster_role, @@GLOBAL.dolt_cluster_role_epoch;" "@@GLOBAL.dolt_cluster_role,@@GLOBAL.dolt_cluster_role_epoch\nprimary,13"
}

@test "sql-server-cluster: create database makes a new remote" {
    cd serverone

    echo "
user:
  name: dolt
listener:
  host: 0.0.0.0
  port: ${SERVERONE_MYSQL_PORT}
behavior:
  read_only: false
  autocommit: true
cluster:
  standby_remotes:
  - name: standby
    remote_url_template: http://localhost:${SERVERTWO_GRPC_PORT}/{database}
  bootstrap_role: primary
  bootstrap_epoch: 10
  remotesapi:
    port: ${SERVERONE_GRPC_PORT}" > server.yaml

    (cd repo1 && dolt remote add standby http://localhost:"${SERVERTWO_GRPC_PORT}"/repo1)
    (cd repo2 && dolt remote add standby http://localhost:"${SERVERTWO_GRPC_PORT}"/repo2)
    dolt sql-server --config server.yaml &
    SERVER_PID=$!
    wait_for_connection "${SERVERONE_MYSQL_PORT}" 5000

    server_query_with_port "${SERVERONE_MYSQL_PORT}" repo1 1 dolt "" "create database a_new_database;use a_new_database;select name, url from dolt_remotes" ";;name,url\nstandby,http://localhost:50052/a_new_database"
}

@test "sql-server-cluster: sql-server fails to start if a configured remote is missing" {
    cd serverone

    echo "
user:
  name: dolt
listener:
  host: 0.0.0.0
  port: ${SERVERONE_MYSQL_PORT}
behavior:
  read_only: false
  autocommit: true
cluster:
  standby_remotes:
  - name: standby
    remote_url_template: http://localhost:${SERVERTWO_GRPC_PORT}/{database}
  bootstrap_role: primary
  bootstrap_epoch: 10
  remotesapi:
    port: ${SERVERONE_GRPC_PORT}" > server.yaml

    (cd repo1 && dolt remote add standby http://localhost:${SERVERTWO_GRPC_PORT}/repo1)
    run dolt sql-server --config server.yaml
    [ "$status" -ne 0 ]
    [[ "$output" =~ "destination remote standby does not exist" ]] || false
}

@test "sql-server-cluster: primary comes up and replicates to standby" {
    cd serverone

    echo "
log_level: trace
user:
  name: dolt
listener:
  host: 0.0.0.0
  port: ${SERVERONE_MYSQL_PORT}
behavior:
  read_only: false
  autocommit: true
cluster:
  standby_remotes:
  - name: standby
    remote_url_template: http://localhost:${SERVERTWO_GRPC_PORT}/{database}
  bootstrap_role: standby
  bootstrap_epoch: 10
  remotesapi:
    port: ${SERVERONE_GRPC_PORT}" > server.yaml

    DOLT_ROOT_PATH=`pwd` dolt config --global --add user.email bats@email.fake
    DOLT_ROOT_PATH=`pwd` dolt config --global --add user.name "Bats Tests"
    DOLT_ROOT_PATH=`pwd` dolt config --global --add metrics.disabled true

    (cd repo1 && dolt remote add standby http://localhost:${SERVERTWO_GRPC_PORT}/repo1)
    (cd repo2 && dolt remote add standby http://localhost:${SERVERTWO_GRPC_PORT}/repo2)
    DOLT_ROOT_PATH=`pwd` dolt sql-server --config server.yaml &
    serverone_pid=$!

    wait_for_connection "${SERVERONE_MYSQL_PORT}" 5000

    cd ../servertwo

    echo "
log_level: trace
user:
  name: dolt
listener:
  host: 0.0.0.0
  port: ${SERVERTWO_MYSQL_PORT}
behavior:
  read_only: false
  autocommit: true
cluster:
  standby_remotes:
  - name: standby
    remote_url_template: http://localhost:${SERVERONE_GRPC_PORT}/{database}
  bootstrap_role: primary
  bootstrap_epoch: 10
  remotesapi:
    port: ${SERVERTWO_GRPC_PORT}" > server.yaml

    DOLT_ROOT_PATH=`pwd` dolt config --global --add user.email bats@email.fake
    DOLT_ROOT_PATH=`pwd` dolt config --global --add user.name "Bats Tests"
    DOLT_ROOT_PATH=`pwd` dolt config --global --add metrics.disabled true

    (cd repo1 && dolt remote add standby http://localhost:${SERVERONE_GRPC_PORT}/repo1)
    (cd repo2 && dolt remote add standby http://localhost:${SERVERONE_GRPC_PORT}/repo2)
    DOLT_ROOT_PATH=`pwd` dolt sql-server --config server.yaml &
    servertwo_pid=$!

    wait_for_connection "${SERVERTWO_MYSQL_PORT}" 5000

    server_query_with_port "${SERVERTWO_MYSQL_PORT}" repo1 1 dolt "" "create table vals (i int primary key);insert into vals values (1),(2),(3),(4),(5)"

    server_query_with_port "${SERVERTWO_MYSQL_PORT}" dolt_cluster 1 dolt "" "select "'`database`'", standby_remote, role, epoch, replication_lag_millis, current_error from dolt_cluster_status order by "'`database`'" asc" "database,standby_remote,role,epoch,replication_lag_millis,current_error\nrepo1,standby,primary,10,0,None\nrepo2,standby,primary,10,0,None"

    kill $servertwo_pid
    wait $servertwo_pid

    kill $serverone_pid
    wait $serverone_pid

    cd ../serverone

    run env DOLT_ROOT_PATH=`pwd` dolt sql -q 'select count(*) from vals'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| 5 " ]] || false
}

@test "sql-server-cluster: booted standby server is read only" {
    cd serverone

    cd repo1
    dolt sql -q 'create table vals (i int primary key)'
    dolt sql -q 'insert into vals values (1), (2), (3), (4), (5)'
    cd ..

    echo "
log_level: trace
user:
  name: dolt
listener:
  host: 0.0.0.0
  port: ${SERVERONE_MYSQL_PORT}
behavior:
  read_only: false
  autocommit: true
cluster:
  standby_remotes:
  - name: standby
    remote_url_template: http://localhost:${SERVERTWO_GRPC_PORT}/{database}
  bootstrap_role: standby
  bootstrap_epoch: 10
  remotesapi:
    port: ${SERVERONE_GRPC_PORT}" > server.yaml

    DOLT_ROOT_PATH=`pwd` dolt config --global --add user.email bats@email.fake
    DOLT_ROOT_PATH=`pwd` dolt config --global --add user.name "Bats Tests"
    DOLT_ROOT_PATH=`pwd` dolt config --global --add metrics.disabled true

    (cd repo1 && dolt remote add standby http://localhost:${SERVERTWO_GRPC_PORT}/repo1)
    (cd repo2 && dolt remote add standby http://localhost:${SERVERTWO_GRPC_PORT}/repo2)
    DOLT_ROOT_PATH=`pwd` dolt sql-server --config server.yaml &
    SERVER_PID=$!

    wait_for_connection "${SERVERONE_MYSQL_PORT}" 5000

    server_query_with_port "${SERVERONE_MYSQL_PORT}" repo1 1 dolt "" "select count(*) from vals" "count(*)\n5"
    run server_query_with_port "${SERVERONE_MYSQL_PORT}" repo1 1 dolt "" "insert into vals values (6),(7),(8),(9),(10)" "" 1
    [[ "$output" =~ "Database repo1 is read-only" ]] || false
    server_query_with_port "${SERVERONE_MYSQL_PORT}" repo1 1 dolt "" "select count(*) from vals" "count(*)\n5"
}

@test "sql-server-cluster: booted primary server is read write" {
    cd serverone

    cd repo1
    dolt sql -q 'create table vals (i int primary key)'
    dolt sql -q 'insert into vals values (1), (2), (3), (4), (5)'
    cd ..

    echo "
log_level: trace
user:
  name: dolt
listener:
  host: 0.0.0.0
  port: ${SERVERONE_MYSQL_PORT}
behavior:
  read_only: false
  autocommit: true
cluster:
  standby_remotes:
  - name: standby
    remote_url_template: http://localhost:${SERVERTWO_GRPC_PORT}/{database}
  bootstrap_role: primary
  bootstrap_epoch: 10
  remotesapi:
    port: ${SERVERONE_GRPC_PORT}" > server.yaml

    DOLT_ROOT_PATH=`pwd` dolt config --global --add user.email bats@email.fake
    DOLT_ROOT_PATH=`pwd` dolt config --global --add user.name "Bats Tests"
    DOLT_ROOT_PATH=`pwd` dolt config --global --add metrics.disabled true

    (cd repo1 && dolt remote add standby http://localhost:${SERVERTWO_GRPC_PORT}/repo1)
    (cd repo2 && dolt remote add standby http://localhost:${SERVERTWO_GRPC_PORT}/repo2)
    DOLT_ROOT_PATH=`pwd` dolt sql-server --config server.yaml &
    SERVER_PID=$!

    wait_for_connection "${SERVERONE_MYSQL_PORT}" 5000

    server_query_with_port "${SERVERONE_MYSQL_PORT}" repo1 1 dolt "" "select count(*) from vals" "count(*)\n5"
    server_query_with_port "${SERVERONE_MYSQL_PORT}" repo1 1 dolt "" "insert into vals values (6),(7),(8),(9),(10)" ""
    server_query_with_port "${SERVERONE_MYSQL_PORT}" repo1 1 dolt "" "select count(*) from vals" "count(*)\n10"
}

@test "sql-server-cluster: standby transitioned to primary becomes writable" {
    cd serverone

    cd repo1
    dolt sql -q 'create table vals (i int primary key)'
    dolt sql -q 'insert into vals values (1), (2), (3), (4), (5)'
    cd ..

    echo "
log_level: trace
user:
  name: dolt
listener:
  host: 0.0.0.0
  port: ${SERVERONE_MYSQL_PORT}
behavior:
  read_only: false
  autocommit: true
cluster:
  standby_remotes:
  - name: standby
    remote_url_template: http://localhost:${SERVERTWO_GRPC_PORT}/{database}
  bootstrap_role: standby
  bootstrap_epoch: 10
  remotesapi:
    port: ${SERVERONE_GRPC_PORT}" > server.yaml

    DOLT_ROOT_PATH=`pwd` dolt config --global --add user.email bats@email.fake
    DOLT_ROOT_PATH=`pwd` dolt config --global --add user.name "Bats Tests"
    DOLT_ROOT_PATH=`pwd` dolt config --global --add metrics.disabled true

    (cd repo1 && dolt remote add standby http://localhost:${SERVERTWO_GRPC_PORT}/repo1)
    (cd repo2 && dolt remote add standby http://localhost:${SERVERTWO_GRPC_PORT}/repo2)
    DOLT_ROOT_PATH=`pwd` dolt sql-server --config server.yaml &
    SERVER_PID=$!

    wait_for_connection "${SERVERONE_MYSQL_PORT}" 5000

    run server_query_with_port "${SERVERONE_MYSQL_PORT}" repo1 1 dolt "" "insert into vals values (6),(7),(8),(9),(10)" "" 1
    [[ "$output" =~ "Database repo1 is read-only" ]] || false
    run server_query_with_port "${SERVERONE_MYSQL_PORT}" repo1 1 dolt "" "call dolt_assume_cluster_role('primary', 11)" "" 1
    server_query_with_port "${SERVERONE_MYSQL_PORT}" repo1 1 dolt "" "select count(*) from vals" "count(*)\n5"
    server_query_with_port "${SERVERONE_MYSQL_PORT}" repo1 1 dolt "" "insert into vals values (6),(7),(8),(9),(10)" "" 0
    server_query_with_port "${SERVERONE_MYSQL_PORT}" repo1 1 dolt "" "select count(*) from vals" "count(*)\n10"
}

@test "sql-server-cluster: primary transitioned to standby becomes read only" {
    # In order to gracefully transition to standby, we will need the standby running.
    cd serverone

    echo "
log_level: trace
user:
  name: dolt
listener:
  host: 0.0.0.0
  port: ${SERVERONE_MYSQL_PORT}
behavior:
  read_only: false
  autocommit: true
cluster:
  standby_remotes:
  - name: standby
    remote_url_template: http://localhost:${SERVERTWO_GRPC_PORT}/{database}
  bootstrap_role: standby
  bootstrap_epoch: 10
  remotesapi:
    port: ${SERVERONE_GRPC_PORT}" > server.yaml

    DOLT_ROOT_PATH=`pwd` dolt config --global --add user.email bats@email.fake
    DOLT_ROOT_PATH=`pwd` dolt config --global --add user.name "Bats Tests"
    DOLT_ROOT_PATH=`pwd` dolt config --global --add metrics.disabled true

    (cd repo1 && dolt remote add standby http://localhost:${SERVERTWO_GRPC_PORT}/repo1)
    (cd repo2 && dolt remote add standby http://localhost:${SERVERTWO_GRPC_PORT}/repo2)
    DOLT_ROOT_PATH=`pwd` dolt sql-server --config server.yaml &
    serverone_pid=$!

    wait_for_connection "${SERVERONE_MYSQL_PORT}" 5000

    cd ../servertwo

    echo "
log_level: trace
user:
  name: dolt
listener:
  host: 0.0.0.0
  port: ${SERVERTWO_MYSQL_PORT}
behavior:
  read_only: false
  autocommit: true
cluster:
  standby_remotes:
  - name: standby
    remote_url_template: http://localhost:${SERVERONE_GRPC_PORT}/{database}
  bootstrap_role: primary
  bootstrap_epoch: 10
  remotesapi:
    port: ${SERVERTWO_GRPC_PORT}" > server.yaml

    DOLT_ROOT_PATH=`pwd` dolt config --global --add user.email bats@email.fake
    DOLT_ROOT_PATH=`pwd` dolt config --global --add user.name "Bats Tests"
    DOLT_ROOT_PATH=`pwd` dolt config --global --add metrics.disabled true

    (cd repo1 && dolt remote add standby http://localhost:${SERVERONE_GRPC_PORT}/repo1)
    (cd repo2 && dolt remote add standby http://localhost:${SERVERONE_GRPC_PORT}/repo2)
    DOLT_ROOT_PATH=`pwd` dolt sql-server --config server.yaml &
    servertwo_pid=$!

    wait_for_connection "${SERVERTWO_MYSQL_PORT}" 5000

    server_query_with_port "${SERVERTWO_MYSQL_PORT}" repo1 1 dolt "" "create table vals (i int primary key);insert into vals values (1),(2),(3),(4),(5)"
    run server_query_with_port "${SERVERTWO_MYSQL_PORT}" repo1 1 dolt "" "call dolt_assume_cluster_role('standby', 11)" "" 1
    run server_query_with_port "${SERVERTWO_MYSQL_PORT}" repo1 1 dolt "" "insert into vals values (6),(7),(8),(9),(10)" "" 1
    [[ "$output" =~ "Database repo1 is read-only" ]] || false
    server_query_with_port "${SERVERTWO_MYSQL_PORT}" repo1 1 dolt "" "select count(*) from vals" "count(*)\n5"

    kill $servertwo_pid
    wait $servertwo_pid

    kill $serverone_pid
    wait $serverone_pid
}

@test "sql-server-cluster: misconfigured cluster with primaries at same epoch, both transition to detected_broken_config" {
    cd serverone

    echo "
log_level: trace
user:
  name: dolt
listener:
  host: 0.0.0.0
  port: ${SERVERONE_MYSQL_PORT}
behavior:
  read_only: false
  autocommit: true
cluster:
  standby_remotes:
  - name: standby
    remote_url_template: http://localhost:${SERVERTWO_GRPC_PORT}/{database}
  bootstrap_role: primary
  bootstrap_epoch: 10
  remotesapi:
    port: ${SERVERONE_GRPC_PORT}" > server.yaml

    DOLT_ROOT_PATH=`pwd` dolt config --global --add user.email bats@email.fake
    DOLT_ROOT_PATH=`pwd` dolt config --global --add user.name "Bats Tests"
    DOLT_ROOT_PATH=`pwd` dolt config --global --add metrics.disabled true

    (cd repo1 && dolt remote add standby http://localhost:${SERVERTWO_GRPC_PORT}/repo1)
    (cd repo2 && dolt remote add standby http://localhost:${SERVERTWO_GRPC_PORT}/repo2)
    DOLT_ROOT_PATH=`pwd` dolt sql-server --config server.yaml &
    serverone_pid=$!

    wait_for_connection "${SERVERONE_MYSQL_PORT}" 5000

    cd ../servertwo

    echo "
log_level: trace
user:
  name: dolt
listener:
  host: 0.0.0.0
  port: ${SERVERTWO_MYSQL_PORT}
behavior:
  read_only: false
  autocommit: true
cluster:
  standby_remotes:
  - name: standby
    remote_url_template: http://localhost:${SERVERONE_GRPC_PORT}/{database}
  bootstrap_role: primary
  bootstrap_epoch: 10
  remotesapi:
    port: ${SERVERTWO_GRPC_PORT}" > server.yaml

    DOLT_ROOT_PATH=`pwd` dolt config --global --add user.email bats@email.fake
    DOLT_ROOT_PATH=`pwd` dolt config --global --add user.name "Bats Tests"
    DOLT_ROOT_PATH=`pwd` dolt config --global --add metrics.disabled true

    (cd repo1 && dolt remote add standby http://localhost:${SERVERONE_GRPC_PORT}/repo1)
    (cd repo2 && dolt remote add standby http://localhost:${SERVERONE_GRPC_PORT}/repo2)
    DOLT_ROOT_PATH=`pwd` dolt sql-server --config server.yaml &
    servertwo_pid=$!

    wait_for_connection "${SERVERTWO_MYSQL_PORT}" 5000

    # Run a query to make sure everyone sees everyone...
    run server_query_with_port "${SERVERTWO_MYSQL_PORT}" repo1 1 dolt "" "CREATE TABLE vals (i int primary key)" "" 1

    server_query_with_port "${SERVERTWO_MYSQL_PORT}" repo1 1 dolt "" "SELECT @@GLOBAL.dolt_cluster_role,@@GLOBAL.dolt_cluster_role_epoch" "@@GLOBAL.dolt_cluster_role,@@GLOBAL.dolt_cluster_role_epoch\ndetected_broken_config,10"
    server_query_with_port "${SERVERONE_MYSQL_PORT}" repo1 1 dolt "" "SELECT @@GLOBAL.dolt_cluster_role,@@GLOBAL.dolt_cluster_role_epoch" "@@GLOBAL.dolt_cluster_role,@@GLOBAL.dolt_cluster_role_epoch\ndetected_broken_config,10"

    kill $servertwo_pid
    wait $servertwo_pid

    kill $serverone_pid
    wait $serverone_pid
}

@test "sql-server-cluster: an older primary comes up, becomes a standby and does not overwrite newer primary" {
    cd serverone

    cd repo1
    dolt sql -q 'create table vals (i int primary key)'
    dolt sql -q 'insert into vals values (1),(2),(3),(4),(5),(6),(7),(8),(9),(10)'
    cd ../

    echo "
log_level: trace
user:
  name: dolt
listener:
  host: 0.0.0.0
  port: ${SERVERONE_MYSQL_PORT}
behavior:
  read_only: false
  autocommit: true
cluster:
  standby_remotes:
  - name: standby
    remote_url_template: http://localhost:${SERVERTWO_GRPC_PORT}/{database}
  bootstrap_role: primary
  bootstrap_epoch: 15
  remotesapi:
    port: ${SERVERONE_GRPC_PORT}" > server.yaml

    DOLT_ROOT_PATH=`pwd` dolt config --global --add user.email bats@email.fake
    DOLT_ROOT_PATH=`pwd` dolt config --global --add user.name "Bats Tests"
    DOLT_ROOT_PATH=`pwd` dolt config --global --add metrics.disabled true

    (cd repo1 && dolt remote add standby http://localhost:${SERVERTWO_GRPC_PORT}/repo1)
    (cd repo2 && dolt remote add standby http://localhost:${SERVERTWO_GRPC_PORT}/repo2)
    DOLT_ROOT_PATH=`pwd` dolt sql-server --config server.yaml &
    serverone_pid=$!

    wait_for_connection "${SERVERONE_MYSQL_PORT}" 5000

    cd ../servertwo

    cd repo1
    dolt sql -q 'create table vals (i int primary key)'
    dolt sql -q 'insert into vals values (1),(2),(3),(4),(5)'
    cd ../

    echo "
log_level: trace
user:
  name: dolt
listener:
  host: 0.0.0.0
  port: ${SERVERTWO_MYSQL_PORT}
behavior:
  read_only: false
  autocommit: true
cluster:
  standby_remotes:
  - name: standby
    remote_url_template: http://localhost:${SERVERONE_GRPC_PORT}/{database}
  bootstrap_role: primary
  bootstrap_epoch: 10
  remotesapi:
    port: ${SERVERTWO_GRPC_PORT}" > server.yaml

    DOLT_ROOT_PATH=`pwd` dolt config --global --add user.email bats@email.fake
    DOLT_ROOT_PATH=`pwd` dolt config --global --add user.name "Bats Tests"
    DOLT_ROOT_PATH=`pwd` dolt config --global --add metrics.disabled true

    (cd repo1 && dolt remote add standby http://localhost:${SERVERONE_GRPC_PORT}/repo1)
    (cd repo2 && dolt remote add standby http://localhost:${SERVERONE_GRPC_PORT}/repo2)
    DOLT_ROOT_PATH=`pwd` dolt sql-server --config server.yaml &
    servertwo_pid=$!

    wait_for_connection "${SERVERTWO_MYSQL_PORT}" 5000

    server_query_with_port "${SERVERONE_MYSQL_PORT}" repo1 1 dolt "" "SELECT count(*) FROM vals" "count(*)\n10"
    server_query_with_port "${SERVERTWO_MYSQL_PORT}" repo1 1 dolt "" "SELECT @@GLOBAL.dolt_cluster_role,@@GLOBAL.dolt_cluster_role_epoch" "@@GLOBAL.dolt_cluster_role,@@GLOBAL.dolt_cluster_role_epoch\nstandby,15"

    kill $servertwo_pid
    wait $servertwo_pid

    kill $serverone_pid
    wait $serverone_pid
}

@test "sql-server-cluster: a new primary comes up, old primary becomes a standby and has its state overwritten" {
    cd serverone

    cd repo1
    dolt sql -q 'create table vals (i int primary key)'
    dolt sql -q 'insert into vals values (1),(2),(3),(4),(5)'
    cd ../

    echo "
log_level: trace
user:
  name: dolt
listener:
  host: 0.0.0.0
  port: ${SERVERONE_MYSQL_PORT}
behavior:
  read_only: false
  autocommit: true
cluster:
  standby_remotes:
  - name: standby
    remote_url_template: http://localhost:${SERVERTWO_GRPC_PORT}/{database}
  bootstrap_role: primary
  bootstrap_epoch: 10
  remotesapi:
    port: ${SERVERONE_GRPC_PORT}" > server.yaml

    DOLT_ROOT_PATH=`pwd` dolt config --global --add user.email bats@email.fake
    DOLT_ROOT_PATH=`pwd` dolt config --global --add user.name "Bats Tests"
    DOLT_ROOT_PATH=`pwd` dolt config --global --add metrics.disabled true

    (cd repo1 && dolt remote add standby http://localhost:${SERVERTWO_GRPC_PORT}/repo1)
    (cd repo2 && dolt remote add standby http://localhost:${SERVERTWO_GRPC_PORT}/repo2)
    DOLT_ROOT_PATH=`pwd` dolt sql-server --config server.yaml &
    serverone_pid=$!

    wait_for_connection "${SERVERONE_MYSQL_PORT}" 5000

    cd ../servertwo

    cd repo1
    dolt sql -q 'create table vals (i int primary key)'
    dolt sql -q 'insert into vals values (1),(2),(3),(4),(5),(6),(7),(8),(9),(10)'
    cd ../

    echo "
log_level: trace
user:
  name: dolt
listener:
  host: 0.0.0.0
  port: ${SERVERTWO_MYSQL_PORT}
behavior:
  read_only: false
  autocommit: true
cluster:
  standby_remotes:
  - name: standby
    remote_url_template: http://localhost:${SERVERONE_GRPC_PORT}/{database}
  bootstrap_role: primary
  bootstrap_epoch: 15
  remotesapi:
    port: ${SERVERTWO_GRPC_PORT}" > server.yaml

    DOLT_ROOT_PATH=`pwd` dolt config --global --add user.email bats@email.fake
    DOLT_ROOT_PATH=`pwd` dolt config --global --add user.name "Bats Tests"
    DOLT_ROOT_PATH=`pwd` dolt config --global --add metrics.disabled true

    (cd repo1 && dolt remote add standby http://localhost:${SERVERONE_GRPC_PORT}/repo1)
    (cd repo2 && dolt remote add standby http://localhost:${SERVERONE_GRPC_PORT}/repo2)
    DOLT_ROOT_PATH=`pwd` dolt sql-server --config server.yaml &
    servertwo_pid=$!

    wait_for_connection "${SERVERTWO_MYSQL_PORT}" 5000

    server_query_with_port "${SERVERONE_MYSQL_PORT}" repo1 1 dolt "" "SELECT @@GLOBAL.dolt_cluster_role,@@GLOBAL.dolt_cluster_role_epoch" "@@GLOBAL.dolt_cluster_role,@@GLOBAL.dolt_cluster_role_epoch\nstandby,15"
    server_query_with_port "${SERVERONE_MYSQL_PORT}" repo1 1 dolt "" "SELECT count(*) FROM vals" "count(*)\n10"
    server_query_with_port "${SERVERTWO_MYSQL_PORT}" repo1 1 dolt "" "SELECT @@GLOBAL.dolt_cluster_role,@@GLOBAL.dolt_cluster_role_epoch" "@@GLOBAL.dolt_cluster_role,@@GLOBAL.dolt_cluster_role_epoch\nprimary,15"

    kill $servertwo_pid
    wait $servertwo_pid

    kill $serverone_pid
    wait $serverone_pid
}
